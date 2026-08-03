"""
libro_coherencia_agent: verifica coherencia entre capítulos del libro completo.

Corre UNA vez por libro, después de que todos los capítulos pasaron A+B+C,
y antes de layout_agent.

Checklist (binario):
  1. hechos_no_contradictorios: fechas, lugares y hechos compartidos sin contradicciones
  2. parentesco_consistente: relaciones de parentesco consistentes entre capítulos
  3. transiciones_coherentes: prólogo/epílogo/transiciones coherentes con el orden
  4. anecdotas_no_duplicadas: no hay dos capítulos que se atribuyan la misma anécdota

Modelo: claude-opus-4-8, effort high.
Persistencia Firestore: nivel "libro" (diferenciado de nivel "capitulo").
"""

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable

import anthropic

from pipeline.utils.retry import call_with_retry

logger = logging.getLogger(__name__)

MODEL = "claude-opus-4-8"
MAX_INTENTOS = 3

_SYSTEM = """\
Sos un editor literario senior especializado en memorias familiares colectivas.
Tu trabajo es detectar inconsistencias entre los capítulos de un libro familiar:
contradicciones de hechos, parentescos incompatibles, y anécdotas atribuidas
incorrectamente. Respondés solo con JSON válido, sin texto adicional.
"""

_PROMPT_COHERENCIA = """\
Analizás los capítulos del libro familiar "{nombre_familia}" buscando inconsistencias
entre ellos. Cada capítulo fue escrito a partir del testimonio de un integrante.

<capitulos>
{capitulos_xml}
</capitulos>

<prologo>
{prologo}
</prologo>

<epilogo>
{epilogo}
</epilogo>

<transcripciones_resumen>
{transcripciones_xml}
</transcripciones_resumen>

Evaluá el CHECKLIST DE COHERENCIA DEL LIBRO:

1. hechos_no_contradictorios: los hechos compartidos entre integrantes (fecha de boda,
   lugar donde ocurrió algo, quiénes estaban presentes en un evento) son consistentes
   entre todos los capítulos. Un hecho puede estar en un solo capítulo sin problema;
   la violación es cuando DOS capítulos mencionan el MISMO hecho con datos distintos.

2. parentesco_consistente: las relaciones de parentesco mencionadas son consistentes.
   Si el capítulo de María dice que Juan es su hijo, el capítulo de Juan no puede decir
   que María es su hermana.

3. transiciones_coherentes: el prólogo, epílogo y transiciones entre capítulos tienen
   sentido en el orden en que aparecen. No hay referencias a eventos que ocurren
   "después" cuando en realidad aparecen antes en el libro.

4. anecdotas_no_duplicadas: no hay dos capítulos que se atribuyan como protagonista
   de la misma anécdota central, a menos que la transcripción de ambos confirme
   que ambos la vivieron. Si solo uno tiene esa anécdota en su transcripción,
   el otro no puede reclamarla como propia.

Para cada violación detectada, indicá:
- qué capítulo(s) están involucrados (nombres exactos como aparecen en <capitulos>)
- una descripción concreta del conflicto
- cuál capítulo deberías reescribir para resolver el conflicto (el que tiene menor
  respaldo en las transcripciones)

Respondé EXCLUSIVAMENTE con este JSON:
{{
  "coherencia": {{
    "hechos_no_contradictorios": true,
    "parentesco_consistente": true,
    "transiciones_coherentes": true,
    "anecdotas_no_duplicadas": true
  }},
  "pasa": true,
  "capitulos_involucrados": [],
  "violaciones": [],
  "capitulos_a_reescribir": []
}}

"violaciones" es lista de objetos: {{"item": str, "capitulos": [str], "descripcion": str}}
"capitulos_a_reescribir" es lista de objetos: {{"nombre": str, "feedback": str}}
Solo JSON. Sin explicaciones. Sin markdown.
"""


@dataclass
class ViolacionCoherencia:
    item: str
    capitulos: list[str]
    descripcion: str


@dataclass
class ResultadoCoherencia:
    hechos_no_contradictorios: bool = True
    parentesco_consistente: bool = True
    transiciones_coherentes: bool = True
    anecdotas_no_duplicadas: bool = True
    capitulos_involucrados: list[str] = field(default_factory=list)
    violaciones: list[ViolacionCoherencia] = field(default_factory=list)
    capitulos_a_reescribir: list[dict] = field(default_factory=list)
    intento: int = 1
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def pasa(self) -> bool:
        return (
            self.hechos_no_contradictorios
            and self.parentesco_consistente
            and self.transiciones_coherentes
            and self.anecdotas_no_duplicadas
        )

    def as_dict(self) -> dict:
        return {
            "nivel": "libro",
            "intento": self.intento,
            "timestamp": self.timestamp,
            "pasa": self.pasa,
            "coherencia": {
                "hechos_no_contradictorios": self.hechos_no_contradictorios,
                "parentesco_consistente": self.parentesco_consistente,
                "transiciones_coherentes": self.transiciones_coherentes,
                "anecdotas_no_duplicadas": self.anecdotas_no_duplicadas,
            },
            "capitulos_involucrados": self.capitulos_involucrados,
            "violaciones": [
                {"item": v.item, "capitulos": v.capitulos, "descripcion": v.descripcion}
                for v in self.violaciones
            ],
            "capitulos_a_reescribir": self.capitulos_a_reescribir,
        }


def verificar_coherencia(
    client: anthropic.Anthropic,
    capitulos: dict[str, str],
    personas_con_transcripciones: list[dict],
    nombre_familia: str,
    prologo: str = "",
    epilogo: str = "",
    costos=None,
    intento: int = 1,
) -> ResultadoCoherencia:
    """
    Evalúa la coherencia entre todos los capítulos del libro.

    capitulos: {nombre: texto_capitulo}
    personas_con_transcripciones: [{nombre, transcripcion, ...}]
    """
    tx_by_nombre = {p["nombre"]: p.get("transcripcion", "") for p in personas_con_transcripciones}

    capitulos_xml = "\n\n".join(
        f"<capitulo nombre=\"{nombre}\">\n{texto[:6000]}\n</capitulo>"
        for nombre, texto in capitulos.items()
    )

    transcripciones_xml = "\n\n".join(
        f"<transcripcion nombre=\"{nombre}\">\n{tx_by_nombre.get(nombre, '[sin transcripción]')[:3000]}\n</transcripcion>"
        for nombre in capitulos
    )

    prompt = _PROMPT_COHERENCIA.format(
        nombre_familia=nombre_familia,
        capitulos_xml=capitulos_xml,
        prologo=(prologo or "")[:2000],
        epilogo=(epilogo or "")[:2000],
        transcripciones_xml=transcripciones_xml,
    )

    message = call_with_retry(
        client.messages.create,
        model=MODEL,
        max_tokens=2000,
        system=_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
        label=f"coherencia_libro/intento_{intento}",
        thinking={"type": "adaptive"},
        output_config={"effort": "high"},
    )
    if costos is not None:
        costos.add_claude_usage(message.usage.input_tokens, message.usage.output_tokens)

    raw = "\n".join(b.text for b in message.content if b.type == "text").strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        data = json.loads(m.group()) if m else {}

    coh = data.get("coherencia", {})
    violaciones_raw = data.get("violaciones", [])
    violaciones = [
        ViolacionCoherencia(
            item=v.get("item", ""),
            capitulos=v.get("capitulos", []),
            descripcion=v.get("descripcion", ""),
        )
        for v in violaciones_raw
        if isinstance(v, dict)
    ]

    return ResultadoCoherencia(
        hechos_no_contradictorios=bool(coh.get("hechos_no_contradictorios", True)),
        parentesco_consistente=bool(coh.get("parentesco_consistente", True)),
        transiciones_coherentes=bool(coh.get("transiciones_coherentes", True)),
        anecdotas_no_duplicadas=bool(coh.get("anecdotas_no_duplicadas", True)),
        capitulos_involucrados=data.get("capitulos_involucrados", []),
        violaciones=violaciones,
        capitulos_a_reescribir=data.get("capitulos_a_reescribir", []),
        intento=intento,
    )


def loop_coherencia_libro(
    client: anthropic.Anthropic,
    capitulos: dict[str, str],
    personas_con_transcripciones: list[dict],
    nombre_familia: str,
    prologo: str = "",
    epilogo: str = "",
    costos=None,
    familia_id: str | None = None,
    regenerar_capitulo_fn: Callable[[str, str], tuple[str | None, str | None]] | None = None,
) -> dict[str, str]:
    """
    Loop de coherencia del libro. Corre UNA vez por libro.

    Si el check falla, usa regenerar_capitulo_fn(nombre, feedback) -> (nuevo_texto, error)
    para reescribir los capítulos problemáticos. Máx MAX_INTENTOS.

    Retorna el dict de capítulos actualizado (con los reescritos si los hubo).
    """
    capitulos_actuales = dict(capitulos)

    for intento in range(1, MAX_INTENTOS + 1):
        logger.info(
            "[coherencia_libro] familia=%s intento=%d/%d verificando %d capítulos",
            familia_id, intento, MAX_INTENTOS, len(capitulos_actuales),
        )

        resultado = verificar_coherencia(
            client=client,
            capitulos=capitulos_actuales,
            personas_con_transcripciones=personas_con_transcripciones,
            nombre_familia=nombre_familia,
            prologo=prologo,
            epilogo=epilogo,
            costos=costos,
            intento=intento,
        )

        _persistir(familia_id, resultado)

        if resultado.pasa:
            logger.info("[coherencia_libro] familia=%s PASA en intento %d", familia_id, intento)
            return capitulos_actuales

        logger.warning(
            "[coherencia_libro] familia=%s intento=%d FALLA. Violaciones: %s. Capítulos a reescribir: %s",
            familia_id, intento,
            [v.descripcion for v in resultado.violaciones],
            [c["nombre"] for c in resultado.capitulos_a_reescribir],
        )

        if intento == MAX_INTENTOS or not regenerar_capitulo_fn or not resultado.capitulos_a_reescribir:
            break

        # Regenerar los capítulos identificados
        for cap_info in resultado.capitulos_a_reescribir:
            nombre = cap_info.get("nombre", "")
            feedback = cap_info.get("feedback", "")
            if not nombre or nombre not in capitulos_actuales:
                continue
            nuevo_cap, err = regenerar_capitulo_fn(nombre, feedback)
            if err:
                logger.warning("[coherencia_libro] familia=%s no se pudo regenerar %s: %s", familia_id, nombre, err)
            elif nuevo_cap:
                logger.info("[coherencia_libro] familia=%s capítulo de %s regenerado OK", familia_id, nombre)
                capitulos_actuales[nombre] = nuevo_cap

    # Agotamos intentos o no hay regeneración disponible
    logger.warning(
        "[coherencia_libro] familia=%s ESCALACIÓN HUMANA — coherencia no resuelta tras %d intentos",
        familia_id, MAX_INTENTOS,
    )
    _escalar_libro(familia_id, resultado)
    return capitulos_actuales


def _persistir(familia_id: str | None, resultado: ResultadoCoherencia) -> None:
    if not familia_id:
        return
    try:
        from pipeline.utils import firestore as fs
        fs.save_evaluacion_coherencia_libro(familia_id, resultado.as_dict(), resultado.intento)
    except Exception as e:
        logger.warning("[coherencia_libro] No se pudo persistir evaluación: %s", e)


def _escalar_libro(familia_id: str | None, resultado: ResultadoCoherencia) -> None:
    if not familia_id:
        return
    try:
        from pipeline.utils import firestore as fs
        from pipeline.utils.email import send_alerta_admin
        fs._db().collection("familias").document(familia_id).set(
            {
                "requiere_revision_humana": True,
                "escalacion_coherencia_libro": {
                    "motivo": "coherencia_libro_agotado",
                    "violaciones": [v.descripcion for v in resultado.violaciones],
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
                "tiene_escalaciones": True,
            },
            merge=True,
        )
        send_alerta_admin(
            familia_id=familia_id,
            job_id="coherencia_libro",
            etapa="libro_coherencia_agent",
            error="\n".join(v.descripcion for v in resultado.violaciones),
        )
    except Exception as e:
        logger.error("[coherencia_libro] No se pudo escalar familia=%s: %s", familia_id, e)
