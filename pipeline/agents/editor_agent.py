"""
Editor agent: ordena capítulos, genera transiciones, prólogo y epílogo.
Produce un BookManuscript listo para el layout agent.
"""

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

import anthropic

from pipeline.utils.retry import call_with_retry

MODEL = "claude-opus-4-7"

PALABRAS_PROHIBIDAS = [
    "memorable", "invaluable", "legado", "tesoro",
    "entrañable", "inmortal", "huella",
]

_SYSTEM_EDITOR = """\
Sos el editor literario de un libro de memorias familiares.
Tu escritura es precisa, cálida y sin sentimentalismo fácil.
Nunca usás estas palabras: memorable, invaluable, legado, tesoro, entrañable, inmortal, huella.
"""


@dataclass
class BookManuscript:
    prologo: str = ""
    orden: list[str] = field(default_factory=list)
    capitulos: dict[str, str] = field(default_factory=dict)
    transiciones: dict[str, str] = field(default_factory=dict)
    epilogo: str = ""
    razonamiento_orden: str = ""


def _primeras_palabras(texto: str, n: int) -> str:
    words = texto.split()
    return " ".join(words[:n])


def _ultimas_palabras(texto: str, n: int) -> str:
    words = texto.split()
    return " ".join(words[-n:])


def _verificar_palabras_prohibidas(texto: str) -> list[str]:
    encontradas = []
    for p in PALABRAS_PROHIBIDAS:
        if re.search(r"\b" + re.escape(p) + r"\b", texto, re.IGNORECASE):
            encontradas.append(p)
    return encontradas


def _call_orden(client: anthropic.Anthropic, personas: list[dict], costos=None) -> dict:
    """
    Determina el orden cronológico por fecha de nacimiento.
    temperature=0 para resultados deterministas.
    Returns {"orden": [nombres], "forward_refs": {nombre: [nombre]}, "razonamiento": str}
    """
    personas_info = json.dumps(
        [{"nombre": p["nombre"], "fecha_nac": p.get("fecha_nac", "desconocida")} for p in personas],
        ensure_ascii=False,
    )

    prompt = f"""\
Tenés estos protagonistas de un libro familiar:
{personas_info}

Ordenalos cronológicamente de mayor a menor (primero el que nació antes).
Si la fecha es desconocida, ubicalos por contexto generacional inferido del nombre o relación.

Devolvé SOLO JSON válido:
{{
  "orden": ["nombre1", "nombre2", ...],
  "forward_refs": {{"nombre1": ["nombre2"], ...}},
  "razonamiento": "breve explicación del criterio"
}}

Solo JSON. Sin markdown.
"""

    message = call_with_retry(
        client.messages.create,
        model=MODEL,
        max_tokens=1024,
        system=_SYSTEM_EDITOR,
        messages=[{"role": "user", "content": prompt}],
        label="claude/editor/orden",
    )
    if costos is not None:
        costos.add_claude_usage(message.usage.input_tokens, message.usage.output_tokens)

    text = message.content[0].text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return json.loads(text)


def _call_una_transicion(
    client: anthropic.Anthropic,
    nombre_a: str,
    capitulo_a: str,
    nombre_b: str,
    capitulo_b: str,
    relacion_entre: str = "",
    costos=None,
) -> str:
    """
    Genera la transición entre dos capítulos. temperature=1.0 para variedad.
    Usa las últimas 200 palabras del capítulo A y las primeras 200 del B.
    relacion_entre: descripción de la relación (ej. "son cónyuges", "A es padre de B")
    """
    cierre_a = _ultimas_palabras(capitulo_a, 200)
    apertura_b = _primeras_palabras(capitulo_b, 200)

    relacion_hint = f"\nCONTEXTO: {relacion_entre}" if relacion_entre else ""

    prompt = f"""\
Escribí un texto de transición entre dos capítulos de un libro de memorias familiares.
{relacion_hint}
FIN DEL CAPÍTULO DE {nombre_a.upper()}:
«{cierre_a}»

INICIO DEL CAPÍTULO DE {nombre_b.upper()}:
«{apertura_b}»

La transición debe:
- Tener entre 120 y 200 palabras
- Crear un puente temático, temporal o emocional natural entre los dos relatos
- Si hay una relación familiar declarada, podés usarla como hilo conductor (sin ser explícito)
- No mencionar que son "capítulos" ni romper la ilusión narrativa
- Puede ser reflexiva, poética o anecdótica — lo que sirva mejor al tono

Solo el texto de la transición. Sin títulos ni notas.
"""

    message = call_with_retry(
        client.messages.create,
        model=MODEL,
        max_tokens=512,
        system=_SYSTEM_EDITOR,
        messages=[{"role": "user", "content": prompt}],
        label=f"claude/editor/transicion/{nombre_a}-{nombre_b}",
    )
    if costos is not None:
        costos.add_claude_usage(message.usage.input_tokens, message.usage.output_tokens)

    return message.content[0].text.strip()


def _relacion_entre(nombre_a: str, nombre_b: str, relaciones: list[dict]) -> str:
    """Return a human-readable description of the relationship between two people."""
    a_lower = nombre_a.lower()
    b_lower = nombre_b.lower()
    for r in relaciones:
        pa, rel, pb = r["persona_a"].lower(), r["relacion"], r["persona_b"].lower()
        if pa == a_lower and pb == b_lower:
            return f"{nombre_a} es {rel} de {nombre_b}"
        if pa == b_lower and pb == a_lower:
            return f"{nombre_b} es {rel} de {nombre_a}"
    return ""


def _call_transiciones(
    client: anthropic.Anthropic,
    orden: list[str],
    capitulos: dict[str, str],
    relaciones: list[dict] | None = None,
    costos=None,
) -> dict[str, str]:
    """Genera transiciones en paralelo. Returns {"{A}→{B}": texto}."""
    pares = list(zip(orden[:-1], orden[1:]))
    relaciones = relaciones or []
    results = {}

    def _tarea(par):
        a, b = par
        rel_hint = _relacion_entre(a, b, relaciones)
        return f"{a}→{b}", _call_una_transicion(client, a, capitulos[a], b, capitulos[b], rel_hint, costos=costos)

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(_tarea, par): par for par in pares}
        for future in as_completed(futures):
            try:
                key, texto = future.result()
                results[key] = texto
            except Exception as e:
                par = futures[future]
                print(f"[editor] Error en transición {par}: {e}")
                results[f"{par[0]}→{par[1]}"] = ""

    return results


def _call_prologo_epilogo(
    client: anthropic.Anthropic,
    orden: list[str],
    capitulos: dict[str, str],
    perfiles: dict[str, dict],
    fallecidos: list[dict] | None = None,
    costos=None,
) -> tuple[str, str]:
    """
    Prólogo: usa las primeras 80 palabras de cada capítulo.
    Epílogo: usa las últimas 120 palabras de cada capítulo + citas_directas.
    """
    aperturas = "\n\n".join(
        f"{nombre}: «{_primeras_palabras(capitulos[nombre], 80)}»"
        for nombre in orden
        if nombre in capitulos
    )

    cierres = "\n\n".join(
        f"{nombre}: «{_ultimas_palabras(capitulos[nombre], 120)}»"
        for nombre in orden
        if nombre in capitulos
    )

    citas = []
    for nombre in orden:
        perfil = perfiles.get(nombre, {}).get("perfil_voz", {})
        for c in perfil.get("citas_directas", [])[:2]:
            citas.append(f"— {nombre}: «{c}»")
    citas_text = "\n".join(citas)

    # Prólogo
    prologo_prompt = f"""\
Escribí el prólogo de un libro de memorias familiares.

Estos son los inicios de cada capítulo:
{aperturas}

El prólogo debe:
- Tener entre 400 y 600 palabras
- Presentar el libro sin revelar las historias
- Hablar del acto de recordar, de la voz oral, de lo que se preserva al escribir
- Tono cálido pero sin sentimentalismo fácil
- No mencionar nombres propios

Solo el texto del prólogo.
"""

    # Fallecidos: mencionarlos en el epílogo si corresponde
    fallecidos = fallecidos or []
    fallecidos_en_libro = [
        f for f in fallecidos
        if any(f["nombre"].lower() == n.lower() for n in orden)
    ]
    fallecidos_hint = ""
    if fallecidos_en_libro:
        nombres_fallecidos = ", ".join(
            f"{f['nombre']} (falleció {f['fecha_fallec'] or 'antes de la publicación'})"
            for f in fallecidos_en_libro
        )
        fallecidos_hint = (
            f"\nALGUNOS PROTAGONISTAS YA NO ESTÁN: {nombres_fallecidos}. "
            "Si lo considerás narrativamente apropiado, podés aludir a su ausencia "
            "con delicadeza — sin convertirlo en un obituario.\n"
        )

    epilogo_prompt = f"""\
Escribí el epílogo de un libro de memorias familiares.

Estos son los cierres de cada capítulo:
{cierres}

Estas son algunas citas directas de los protagonistas:
{citas_text}
{fallecidos_hint}
El epílogo debe:
- Tener entre 400 y 600 palabras
- Cerrar el libro sin resumir lo que ya se dijo
- Puede incluir 1 o 2 de las citas anteriores, integradas naturalmente
- Hablar del tiempo, de la familia como forma de continuidad
- Tono íntimo, como el final de una conversación larga

Solo el texto del epílogo.
"""

    prologo_msg = call_with_retry(
        client.messages.create,
        model=MODEL,
        max_tokens=1024,
        system=_SYSTEM_EDITOR,
        messages=[{"role": "user", "content": prologo_prompt}],
        label="claude/editor/prologo",
    )
    if costos is not None:
        costos.add_claude_usage(prologo_msg.usage.input_tokens, prologo_msg.usage.output_tokens)
    prologo = prologo_msg.content[0].text.strip()

    epilogo_msg = call_with_retry(
        client.messages.create,
        model=MODEL,
        max_tokens=1024,
        system=_SYSTEM_EDITOR,
        messages=[{"role": "user", "content": epilogo_prompt}],
        label="claude/editor/epilogo",
    )
    if costos is not None:
        costos.add_claude_usage(epilogo_msg.usage.input_tokens, epilogo_msg.usage.output_tokens)
    epilogo = epilogo_msg.content[0].text.strip()

    return prologo, epilogo


def run(
    personas: list[dict],
    capitulos: dict[str, str],
    relaciones: list[dict] | None = None,
    fallecidos: list[dict] | None = None,
    costos=None,
) -> BookManuscript:
    """
    personas: list of {nombre, fecha_nac, perfil_voz}
    capitulos: {nombre: texto_capitulo}
    relaciones: list from sheets.get_familia_relaciones()
    fallecidos: list from sheets.get_fallecidos()
    costos: pipeline.utils.costos.CostAccumulator opcional.
    Returns a BookManuscript.
    """
    client = anthropic.Anthropic(timeout=120.0)
    manuscript = BookManuscript(capitulos=capitulos)

    # 1. Determinar orden
    orden_data = _call_orden(client, personas, costos=costos)
    manuscript.orden = orden_data.get("orden", [p["nombre"] for p in personas])
    manuscript.razonamiento_orden = orden_data.get("razonamiento", "")

    # Ensure all nombres are in orden (fallback for missing ones)
    nombres_en_orden = set(manuscript.orden)
    for p in personas:
        if p["nombre"] not in nombres_en_orden:
            manuscript.orden.append(p["nombre"])

    # 2. Transiciones (con contexto relacional)
    manuscript.transiciones = _call_transiciones(
        client, manuscript.orden, capitulos, relaciones or [], costos=costos
    )

    # 3. Prólogo y epílogo (con fallecidos)
    perfiles = {p["nombre"]: p for p in personas}
    manuscript.prologo, manuscript.epilogo = _call_prologo_epilogo(
        client, manuscript.orden, capitulos, perfiles, fallecidos or [], costos=costos
    )

    return manuscript
