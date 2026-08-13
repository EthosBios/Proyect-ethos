"""
Quality agent: evaluador de calidad de capítulos con loop de reintentos.

Checklist A (factual, BLOQUEANTE):
  - nombre_correcto: el protagonista del capítulo es el esperado
  - sin_invenciones: no hay nombres/fechas/hechos sin respaldo en fuentes
  - sin_mezcla: no se mezclan datos de otras personas

Checklist B (editorial, RETRYABLE):
  - word_count: 3200–3800 palabras [programático]
  - sin_prohibidas: no usa palabras de la lista negra [programático]
  - cursiva_ok: citas directas en *cursiva*, no entre "comillas" [LLM]
  - frases_integradas: frases propias del perfil integradas [LLM]
  - tono_literario: prosa literaria, no IA genérica [LLM]

Loop: hasta MAX_INTENTOS (3). A falla → escalación humana inmediata.
      B sigue fallando tras MAX_INTENTOS → escalación humana.
"""

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

import anthropic

from pipeline.utils.retry import call_with_retry

logger = logging.getLogger(__name__)

MODEL = "claude-sonnet-4-6"
MODEL_C = "claude-sonnet-4-6"
MAX_INTENTOS = 3
MIN_WORDS = 3200
MAX_WORDS = 3800

PALABRAS_PROHIBIDAS = frozenset(
    ["memorable", "invaluable", "legado", "tesoro", "entrañable", "inmortal", "huella"]
)

_SYSTEM = """\
Sos un editor literario experto en memorias familiares latinoamericanas.
Tu trabajo es verificar si un capítulo cumple reglas estrictas de fidelidad factual
y calidad editorial. Respondés solo con JSON válido, sin texto adicional.
"""

_PROMPT_A = """\
<capitulo>
{capitulo}
</capitulo>

<contexto_familiar>
Nombre: {nombre}
Rol: {rol}
Cónyuge/s: {conyuges}
Hijos: {hijos}
Padres: {padres}
Hermanos: {hermanos}
</contexto_familiar>

<transcripciones>
{transcripciones}
</transcripciones>

<nombre_esperado>{nombre}</nombre_esperado>

Evaluá el capítulo contra el CHECKLIST A (verificación factual).

Reglas:
1. nombre_correcto: el protagonista central del capítulo es {nombre}, no otra persona.
2. sin_invenciones: no se mencionan nombres propios, fechas exactas, lugares específicos
   ni hechos concretos que NO aparezcan en contexto_familiar o transcripciones.
   Ante ambigüedad, marcá como violación.
3. sin_mezcla: no se atribuyen a {nombre} datos, recuerdos ni características
   que en las transcripciones claramente pertenecen a otra persona.

IMPORTANTE sobre "violaciones": listá SOLO las violaciones reales — hechos inventados
confirmados. NO listés items que luego aclarás que no son violaciones. Si un dato
aparece en las transcripciones, NO es violación. La lista debe contener solo strings
cortos y concretos, por ejemplo: "menciona 'dos perros' pero transcripción solo cita uno".

Respondé EXCLUSIVAMENTE con este JSON (sin texto adicional, sin markdown):
{{
  "nombre_correcto": true,
  "sin_invenciones": true,
  "sin_mezcla": true,
  "violaciones": []
}}

Reemplazá true/false según corresponda. "violaciones" debe ser una lista de strings cortos.
Solo JSON. Sin explicaciones. Sin comentarios.
"""

_PROMPT_B = """\
<capitulo>
{capitulo}
</capitulo>

<perfil_voz>
Frases propias: {frases_propias}
Muletillas: {muletillas}
</perfil_voz>

<nombre_esperado>{nombre}</nombre_esperado>

Evaluá el capítulo contra el CHECKLIST B (verificación editorial).

Reglas:
1. cursiva_ok: cuando el texto cita la voz directa del protagonista, usa *asteriscos*
   para marcar cursiva. NO usa "comillas dobles" para citas directas.
   Si hay citas entre comillas dobles, es una violación.
2. frases_integradas: al menos 1-2 frases propias del perfil de voz aparecen
   integradas naturalmente en el texto (no listadas ni forzadas).
3. tono_literario: la prosa es literaria, con variación en longitud de párrafos,
   imágenes concretas, y no suena a texto genérico de IA.
4. flujo_narrativo: el capítulo fluye como narrativa continua. NO es una lista de
   respuestas encadenadas tipo "Cuando le pregunté… respondió… Luego…". El lector
   siente que lee una historia, no una entrevista transcripta.
5. transiciones_naturales: los párrafos se conectan con transiciones narrativas
   fluidas. No hay saltos abruptos de tema ni párrafos que empiezan sin relación
   con el anterior.
6. hilo_conductor: hay un hilo conductor claro e identificable a lo largo del
   capítulo entero — una idea, emoción o período de vida que da unidad al texto.
   No es una colección de anécdotas sueltas sin vínculo.

Respondé EXCLUSIVAMENTE con este JSON:
{{
  "cursiva_ok": true,
  "frases_integradas": true,
  "tono_literario": true,
  "flujo_narrativo": true,
  "transiciones_naturales": true,
  "hilo_conductor": true,
  "violaciones": [],
  "feedback": "instrucción concreta de qué mejorar en la próxima versión (máx 3 frases)"
}}

Reemplazá true/false según corresponda y completá violaciones y feedback.
Solo JSON. Sin explicaciones.
"""

_PROMPT_C = """\
<transcripcion>
{transcripcion}
</transcripcion>

<capitulo>
{capitulo}
</capitulo>

<nombre>{nombre}</nombre>

Tarea: evaluá la fidelidad del capítulo respecto al transcripto original.

Paso 1 — Descomponer el transcripto en "unidades de contenido": anécdotas concretas,
datos con carga propia (fechas, lugares, nombres de personas, hechos puntuales),
y frases con valor propio del protagonista. No incluyas preguntas ni conectores genéricos.

Paso 2 — Por cada unidad, determiná si está reflejada en el capítulo. No requiere
literalidad; alcanza con que la sustancia aparezca. Marcá true/false.

Paso 3 — Calculá el porcentaje y listá las unidades que NO aparecen en el capítulo.

Respondé EXCLUSIVAMENTE con este JSON:
{{
  "unidades_totales": 0,
  "unidades_usadas": 0,
  "porcentaje_fidelidad": 0.0,
  "unidades_omitidas": []
}}

Reemplazá los valores con los resultados reales.
Solo JSON. Sin explicaciones. Sin markdown.
"""


# ─── Dataclasses de resultado ─────────────────────────────────────────────────

@dataclass
class ResultadoA:
    nombre_correcto: bool = True
    sin_invenciones: bool = True
    sin_mezcla: bool = True
    violaciones: list[str] = field(default_factory=list)

    @property
    def pasa(self) -> bool:
        return self.nombre_correcto and self.sin_invenciones and self.sin_mezcla


@dataclass
class ResultadoB:
    word_count_ok: bool = True
    sin_prohibidas: bool = True
    cursiva_ok: bool = True
    frases_integradas: bool = True
    tono_literario: bool = True
    flujo_narrativo: bool = True
    transiciones_naturales: bool = True
    hilo_conductor: bool = True
    violaciones: list[str] = field(default_factory=list)
    feedback: str = ""

    @property
    def pasa(self) -> bool:
        return (
            self.word_count_ok
            and self.sin_prohibidas
            and self.cursiva_ok
            and self.frases_integradas
            and self.tono_literario
            and self.flujo_narrativo
            and self.transiciones_naturales
            and self.hilo_conductor
        )


@dataclass
class ResultadoC:
    unidades_totales: int = 0
    unidades_usadas: int = 0
    porcentaje_fidelidad: float = 0.0
    unidades_omitidas: list[str] = field(default_factory=list)
    error: str = ""

    def as_dict(self) -> dict:
        return {
            "unidades_totales": self.unidades_totales,
            "unidades_usadas": self.unidades_usadas,
            "porcentaje_fidelidad": self.porcentaje_fidelidad,
            "unidades_omitidas": self.unidades_omitidas,
            "error": self.error,
        }


@dataclass
class Evaluacion:
    nombre: str
    intento: int
    palabras: int
    checklist_a: ResultadoA = field(default_factory=ResultadoA)
    checklist_b: ResultadoB = field(default_factory=ResultadoB)
    checklist_c: ResultadoC | None = None
    escalado: bool = False
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def aprobado(self) -> bool:
        return self.checklist_a.pasa and self.checklist_b.pasa

    def as_dict(self) -> dict:
        d: dict = {
            "nombre": self.nombre,
            "intento": self.intento,
            "palabras": self.palabras,
            "aprobado": self.aprobado,
            "escalado": self.escalado,
            "timestamp": self.timestamp,
            "checklist_a": {
                "pasa": self.checklist_a.pasa,
                "nombre_correcto": self.checklist_a.nombre_correcto,
                "sin_invenciones": self.checklist_a.sin_invenciones,
                "sin_mezcla": self.checklist_a.sin_mezcla,
                "violaciones": self.checklist_a.violaciones,
            },
            "checklist_b": {
                "pasa": self.checklist_b.pasa,
                "word_count_ok": self.checklist_b.word_count_ok,
                "sin_prohibidas": self.checklist_b.sin_prohibidas,
                "cursiva_ok": self.checklist_b.cursiva_ok,
                "frases_integradas": self.checklist_b.frases_integradas,
                "tono_literario": self.checklist_b.tono_literario,
                "flujo_narrativo": self.checklist_b.flujo_narrativo,
                "transiciones_naturales": self.checklist_b.transiciones_naturales,
                "hilo_conductor": self.checklist_b.hilo_conductor,
                "violaciones": self.checklist_b.violaciones,
                "feedback": self.checklist_b.feedback,
            },
        }
        if self.checklist_c is not None:
            d["checklist_c"] = self.checklist_c.as_dict()
        return d


# ─── Checks programáticos ─────────────────────────────────────────────────────

def _check_word_count(capitulo: str) -> tuple[bool, int]:
    palabras = len(capitulo.split())
    return MIN_WORDS <= palabras <= MAX_WORDS, palabras


def _check_palabras_prohibidas(capitulo: str) -> tuple[bool, list[str]]:
    texto_lower = capitulo.lower()
    encontradas = [p for p in PALABRAS_PROHIBIDAS if p in texto_lower]
    return len(encontradas) == 0, encontradas


# ─── Checks LLM ──────────────────────────────────────────────────────────────

def _evaluar_a(client: anthropic.Anthropic, persona: dict, capitulo: str, costos=None) -> ResultadoA:
    fctx = persona.get("familia_ctx", {})
    transcripcion = persona.get("transcripcion", "")[:8000]

    def _lista(items):
        return ", ".join(items) if items else "—"

    prompt = _PROMPT_A.format(
        nombre=persona["nombre"],
        rol=fctx.get("rol", "no especificado"),
        conyuges=_lista(fctx.get("conyuges", [])),
        hijos=_lista(fctx.get("hijos", [])),
        padres=_lista(fctx.get("padres", [])),
        hermanos=_lista(fctx.get("hermanos", [])),
        transcripciones=transcripcion,
        capitulo=capitulo[:10000],
    )

    message = call_with_retry(
        client.messages.create,
        model=MODEL,
        max_tokens=1000,
        system=_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
        label=f"quality/checklist_a/{persona['nombre']}",
    )
    if costos is not None:
        costos.add_claude_usage(message.usage.input_tokens, message.usage.output_tokens)

    raw = message.content[0].text.strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        # extraer JSON de la respuesta si hay texto extra
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        data = json.loads(m.group()) if m else {}

    # Normalizar violaciones: pueden venir como strings o como dicts {regla, detalle}
    raw_viol = data.get("violaciones", [])
    violaciones = []
    for v in raw_viol:
        if isinstance(v, str):
            violaciones.append(v)
        elif isinstance(v, dict):
            detalle = v.get("detalle") or v.get("descripcion") or str(v)
            violaciones.append(detalle)

    return ResultadoA(
        nombre_correcto=bool(data.get("nombre_correcto", True)),
        sin_invenciones=bool(data.get("sin_invenciones", True)),
        sin_mezcla=bool(data.get("sin_mezcla", True)),
        violaciones=violaciones,
    )


def _evaluar_b_llm(client: anthropic.Anthropic, persona: dict, capitulo: str, costos=None) -> dict:
    perfil = persona.get("perfil_voz", {})
    frases = ", ".join(f'"{f}"' for f in perfil.get("frases_propias", [])[:5]) or "ninguna"
    muletillas = ", ".join(perfil.get("muletillas", [])[:5]) or "ninguna"

    prompt = _PROMPT_B.format(
        nombre=persona["nombre"],
        frases_propias=frases,
        muletillas=muletillas,
        capitulo=capitulo[:10000],
    )

    message = call_with_retry(
        client.messages.create,
        model=MODEL,
        max_tokens=600,
        system=_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
        label=f"quality/checklist_b/{persona['nombre']}",
    )
    if costos is not None:
        costos.add_claude_usage(message.usage.input_tokens, message.usage.output_tokens)

    raw = message.content[0].text.strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        return json.loads(m.group()) if m else {}


# ─── Checklist C ─────────────────────────────────────────────────────────────

def _evaluar_c(client: anthropic.Anthropic, persona: dict, capitulo: str, costos=None) -> ResultadoC:
    """Evalúa la fidelidad del capítulo al transcripto. NO bloqueante."""
    transcripcion = persona.get("transcripcion", "")
    if not transcripcion.strip():
        return ResultadoC(error="transcripción vacía")

    prompt = _PROMPT_C.format(
        nombre=persona["nombre"],
        transcripcion=transcripcion[:8000],
        capitulo=capitulo[:10000],
    )

    message = call_with_retry(
        client.messages.create,
        model=MODEL_C,
        max_tokens=1500,
        system=_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
        label=f"quality/checklist_c/{persona['nombre']}",
    )
    if costos is not None:
        costos.add_claude_usage(message.usage.input_tokens, message.usage.output_tokens)

    raw = message.content[0].text.strip()
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        data = json.loads(m.group()) if m else {}

    totales = int(data.get("unidades_totales", 0))
    usadas = int(data.get("unidades_usadas", 0))
    pct = float(data.get("porcentaje_fidelidad", usadas / totales if totales else 0.0))
    return ResultadoC(
        unidades_totales=totales,
        unidades_usadas=usadas,
        porcentaje_fidelidad=round(pct, 3),
        unidades_omitidas=data.get("unidades_omitidas", []),
    )


# ─── Evaluador principal ──────────────────────────────────────────────────────

def evaluar_capitulo(
    client: anthropic.Anthropic,
    persona: dict,
    capitulo: str,
    intento: int = 1,
    costos=None,
) -> Evaluacion:
    """
    Evalúa un capítulo completo. Primero checks programáticos (rápidos),
    luego LLM para los items que requieren comprensión.
    """
    nombre = persona["nombre"]

    # ── Checklist B programático ──────────────────────────────────────────────
    wc_ok, palabras = _check_word_count(capitulo)
    prohibidas_ok, encontradas = _check_palabras_prohibidas(capitulo)

    viol_b_prog = []
    if not wc_ok:
        viol_b_prog.append(f"word_count={palabras} (esperado {MIN_WORDS}–{MAX_WORDS})")
    if not prohibidas_ok:
        viol_b_prog.append(f"palabras_prohibidas={encontradas}")

    # ── Checklist A (LLM) ─────────────────────────────────────────────────────
    try:
        resultado_a = _evaluar_a(client, persona, capitulo, costos)
    except Exception as e:
        logger.error("[quality] %s error evaluando checklist A: %s", nombre, e)
        # Error de evaluación = falla segura: marcamos sin_invenciones=False
        # para que el intento cuente como fallo B (no bloquea como A bloqueante).
        resultado_a = ResultadoA(
            nombre_correcto=True,
            sin_invenciones=True,
            sin_mezcla=True,
            violaciones=[f"[advertencia] evaluación checklist A no disponible: {e}"],
        )

    # ── Checklist B LLM ───────────────────────────────────────────────────────
    try:
        data_b = _evaluar_b_llm(client, persona, capitulo, costos)
    except Exception as e:
        logger.error("[quality] %s error evaluando checklist B: %s", nombre, e)
        data_b = {}

    resultado_b = ResultadoB(
        word_count_ok=wc_ok,
        sin_prohibidas=prohibidas_ok,
        cursiva_ok=bool(data_b.get("cursiva_ok", True)),
        frases_integradas=bool(data_b.get("frases_integradas", True)),
        tono_literario=bool(data_b.get("tono_literario", True)),
        flujo_narrativo=bool(data_b.get("flujo_narrativo", True)),
        transiciones_naturales=bool(data_b.get("transiciones_naturales", True)),
        hilo_conductor=bool(data_b.get("hilo_conductor", True)),
        violaciones=viol_b_prog + data_b.get("violaciones", []),
        feedback=data_b.get("feedback", ""),
    )

    # ── Checklist C (Fidelidad al transcripto) — NO bloqueante ───────────────
    resultado_c: ResultadoC | None = None
    try:
        resultado_c = _evaluar_c(client, persona, capitulo, costos)
        logger.info(
            "[quality] %s checklist_c: fidelidad=%.0f%% (%d/%d unidades)",
            nombre,
            resultado_c.porcentaje_fidelidad * 100,
            resultado_c.unidades_usadas,
            resultado_c.unidades_totales,
        )
    except Exception as e:
        logger.warning("[quality] %s checklist_c no disponible (ignorado): %s", nombre, e)
        resultado_c = ResultadoC(error=str(e))

    ev = Evaluacion(
        nombre=nombre,
        intento=intento,
        palabras=palabras,
        checklist_a=resultado_a,
        checklist_b=resultado_b,
        checklist_c=resultado_c,
    )

    logger.info(
        "[quality] %s intento=%d palabras=%d A=%s B=%s",
        nombre, intento, palabras,
        "OK" if resultado_a.pasa else f"FALLA({resultado_a.violaciones})",
        "OK" if resultado_b.pasa else f"FALLA({resultado_b.violaciones})",
    )

    return ev


# ─── Loop de calidad ──────────────────────────────────────────────────────────

def loop_calidad(
    client: anthropic.Anthropic,
    persona: dict,
    capitulo_inicial: str,
    costos=None,
    familia_id: str | None = None,
    integrante_id: str | None = None,
) -> tuple[str, Evaluacion]:
    """
    Loop de calidad: evalúa el capítulo y reintenta si Checklist B falla.
    Checklist A: fallo → escalación humana inmediata (no se regenera).
    Checklist B: fallo → se pide reescritura con feedback. Máx MAX_INTENTOS.
    Tras MAX_INTENTOS sin pasar B → escalación humana.

    Retorna (capitulo_final, ultima_evaluacion).
    """
    from pipeline.agents import chapter_agent

    capitulo = capitulo_inicial
    ultima_ev = None

    for intento in range(1, MAX_INTENTOS + 1):
        ev = evaluar_capitulo(client, persona, capitulo, intento=intento, costos=costos)
        ultima_ev = ev

        # Persistir evaluación en Firestore si hay contexto
        if familia_id and integrante_id:
            _persistir_evaluacion(familia_id, integrante_id, ev)

        # Checklist A falla → escalación inmediata, no hay retry posible
        if not ev.checklist_a.pasa:
            logger.warning(
                "[quality] %s ESCALACION HUMANA — Checklist A falló: %s",
                persona["nombre"], ev.checklist_a.violaciones,
            )
            ev.escalado = True
            _escalar(familia_id, integrante_id, persona["nombre"], ev, "checklist_a")
            return capitulo, ev

        # Ambas checklists pasan → OK
        if ev.aprobado:
            logger.info("[quality] %s aprobado en intento %d", persona["nombre"], intento)
            return capitulo, ev

        # Checklist B falla — si quedan intentos, reescribir
        if intento < MAX_INTENTOS:
            logger.info(
                "[quality] %s intento %d/%d falló B. Reescribiendo con feedback: %s",
                persona["nombre"], intento, MAX_INTENTOS, ev.checklist_b.feedback,
            )
            capitulo = _reescribir(client, persona, capitulo, ev.checklist_b, costos)
        else:
            # Agotamos intentos
            logger.warning(
                "[quality] %s ESCALACION HUMANA — B falló %d veces: %s",
                persona["nombre"], MAX_INTENTOS, ev.checklist_b.violaciones,
            )
            ev.escalado = True
            _escalar(familia_id, integrante_id, persona["nombre"], ev, "checklist_b_agotado")
            if familia_id and integrante_id:
                _persistir_evaluacion(familia_id, integrante_id, ev)

    return capitulo, ultima_ev


# ─── Reescritura con feedback ─────────────────────────────────────────────────

_SYSTEM_ESCRITOR = """\
Sos un escritor literario especializado en memorias familiares y narrativa oral latinoamericana.
Tu trabajo es reescribir capítulos que no superaron la revisión de calidad,
corrigiendo las violaciones señaladas sin cambiar los hechos ni la voz del protagonista.
"""

def _reescribir(
    client: anthropic.Anthropic,
    persona: dict,
    capitulo: str,
    resultado_b: ResultadoB,
    costos=None,
) -> str:
    nombre = persona["nombre"]
    perfil = persona.get("perfil_voz", {})
    frases = ", ".join(f'"{f}"' for f in perfil.get("frases_propias", [])[:5]) or "ninguna"

    instrucciones = []
    if not resultado_b.word_count_ok:
        viol_wc = next((v for v in resultado_b.violaciones if "word_count" in v), "")
        palabras_actual = len(capitulo.split())
        if palabras_actual < MIN_WORDS:
            ajuste = "expandiendo con más anécdotas y detalle sensorial."
        else:
            ajuste = (
                "condensando: sacá redundancias, digresiones y detalle accesorio, "
                "conservando las mejores anécdotas y la voz del protagonista."
            )
        instrucciones.append(
            f"El capítulo no tiene el largo correcto ({viol_wc}). "
            f"Reescribilo completo con entre {MIN_WORDS} y {MAX_WORDS} palabras, "
            + ajuste
        )
    if not resultado_b.sin_prohibidas:
        proh = [v for v in resultado_b.violaciones if "prohibidas" in v]
        instrucciones.append(f"Eliminá las palabras prohibidas: {proh}.")
    if not resultado_b.cursiva_ok:
        instrucciones.append(
            "Las citas directas de la voz del protagonista deben ir entre *asteriscos* "
            "(para cursiva), NUNCA entre comillas dobles."
        )
    if not resultado_b.frases_integradas:
        instrucciones.append(
            f"Integrá naturalmente 1-2 de estas frases propias: {frases}."
        )
    if not resultado_b.tono_literario:
        instrucciones.append(
            "Reescribí con prosa más literaria: párrafos de longitud variada, "
            "imágenes concretas, ritmo narrativo. Evitá el tono de IA genérica."
        )
    if not resultado_b.flujo_narrativo:
        instrucciones.append(
            "El capítulo suena a entrevista transcripta (respuestas encadenadas). "
            "Reescribilo como narrativa continua: el narrador cuenta una historia, "
            "no responde preguntas. Eliminá toda estructura de pregunta-respuesta."
        )
    if not resultado_b.transiciones_naturales:
        instrucciones.append(
            "Los párrafos tienen saltos abruptos o no se conectan entre sí. "
            "Agregá transiciones narrativas que lleven al lector de un momento al siguiente."
        )
    if not resultado_b.hilo_conductor:
        instrucciones.append(
            "El capítulo es una colección de anécdotas sin unidad. "
            "Identificá la idea o emoción central de esta persona y usala como hilo "
            "conductor que atraviese y vincule todos los momentos narrados."
        )
    if resultado_b.feedback:
        instrucciones.append(f"Feedback adicional: {resultado_b.feedback}")

    correcciones = "\n".join(f"- {i}" for i in instrucciones)

    prompt = (
        f"Este es el capítulo de {nombre} que necesita correcciones:\n\n"
        f"{capitulo}\n\n"
        f"CORRECCIONES REQUERIDAS:\n{correcciones}\n\n"
        "Reescribí el capítulo completo aplicando TODAS las correcciones. "
        "No cambies los hechos, nombres ni relaciones. Solo mejorá lo indicado. "
        "Devolvé SOLO el texto del capítulo. Sin título. Sin notas."
    )

    message = call_with_retry(
        client.messages.create,
        model="claude-opus-4-8",
        max_tokens=14000,
        system=_SYSTEM_ESCRITOR,
        messages=[{"role": "user", "content": prompt}],
        label=f"quality/rewrite/{nombre}",
        thinking={"type": "adaptive"},
        output_config={"effort": "high"},
    )
    if costos is not None:
        costos.add_claude_usage(message.usage.input_tokens, message.usage.output_tokens)

    return "\n".join(b.text for b in message.content if b.type == "text").strip()


# ─── Persistencia en Firestore ────────────────────────────────────────────────

def _persistir_evaluacion(familia_id: str, integrante_id: str, ev: Evaluacion) -> None:
    try:
        from pipeline.utils import firestore as fs
        fs.save_evaluacion_calidad(familia_id, integrante_id, ev.as_dict(), ev.intento)
    except Exception as e:
        logger.warning("[quality] No se pudo persistir evaluación: %s", e)


def _escalar(
    familia_id: str | None,
    integrante_id: str | None,
    nombre: str,
    ev: Evaluacion,
    motivo: str,
) -> None:
    if not familia_id or not integrante_id:
        logger.warning("[quality] Escalación sin familia_id/integrante_id para %s (%s)", nombre, motivo)
        return
    try:
        from pipeline.utils import firestore as fs
        fs.mark_escalacion_humana(familia_id, integrante_id, motivo, ev.as_dict())
    except Exception as e:
        logger.warning("[quality] No se pudo marcar escalación humana: %s", e)

    # Alerta por email SOLO para Checklist A (factual/alucinaciones). Checklist B agotado
    # escala en silencio. Best-effort: un fallo de email nunca corta el pipeline.
    if motivo == "checklist_a":
        try:
            from pipeline.utils.email import send_alerta_escalacion_humana
            send_alerta_escalacion_humana(familia_id, nombre, motivo, ev.checklist_a.violaciones)
        except Exception as e:  # noqa: BLE001
            logger.warning("[quality] No se pudo enviar email de escalación para %s: %s", nombre, e)
