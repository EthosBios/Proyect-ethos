"""
Lingüista descriptivo: analiza las transcripciones de cada persona
y construye un perfil de voz JSON de 7 campos.
"""

import json
import re
from datetime import datetime

import anthropic

from pipeline.utils import sheets
from pipeline.utils.retry import call_with_retry

MODEL = "claude-opus-4-7"

_SYSTEM = """\
Sos un lingüista descriptivo especializado en oralidad latinoamericana.
Tu trabajo es registrar — no corregir ni juzgar — cómo habla una persona.
Describís su voz escrita con precisión etnográfica.
"""

_PROMPT_TEMPLATE = """\
Analizá las siguientes transcripciones orales de {nombre}.

<transcripciones>
{bloques}
</transcripciones>

Devolvé EXCLUSIVAMENTE un JSON válido con estos 6 campos:

{{
  "muletillas": ["lista de muletillas y palabras de relleno que usa habitualmente"],
  "frases_propias": ["frases o expresiones características que usa más de una vez o que lo/la identifican"],
  "registro": "descripción del registro lingüístico: formal/informal/coloquial/técnico/mixto, con ejemplos",
  "detalles_sensoriales": ["imágenes, metáforas, referencias concretas al cuerpo, al espacio, a los sentidos"],
  "tono": "descripción del tono emocional predominante y sus variaciones",
  "citas_directas": ["5 a 8 fragmentos literales especialmente expresivos o reveladores, mínimo 20 palabras cada uno"]
}}

Solo JSON. Sin explicaciones. Sin markdown.
"""


def _parse_json_response(text: str) -> dict:
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    return json.loads(text)


def _build_perfil(
    client: anthropic.Anthropic, nombre: str, transcripciones: list[dict], costos=None
) -> tuple[dict, str]:
    """
    Core logic: recibe transcripciones [{pregunta, transcripcion}],
    llama a Claude y retorna (perfil_dict, transcripcion_completa).
    costos: pipeline.utils.costos.CostAccumulator opcional.
    """
    if not transcripciones:
        raise ValueError(f"No hay transcripciones para {nombre}")

    bloques = "\n\n".join(
        f"[Pregunta {t['pregunta']}]\n{t['transcripcion']}"
        for t in transcripciones
    )

    message = call_with_retry(
        client.messages.create,
        model=MODEL,
        max_tokens=8192,
        system=_SYSTEM,
        messages=[{"role": "user", "content": _PROMPT_TEMPLATE.format(nombre=nombre, bloques=bloques)}],
        label=f"claude/voz/{nombre}",
    )
    if costos is not None:
        costos.add_claude_usage(message.usage.input_tokens, message.usage.output_tokens)

    perfil = _parse_json_response(message.content[0].text)
    transcripcion_completa = "\n\n".join(t["transcripcion"] for t in transcripciones)
    return perfil, transcripcion_completa


def _analyze_persona(client: anthropic.Anthropic, nombre: str, costos=None) -> dict:
    transcripciones = sheets.get_transcripciones(nombre)
    perfil, transcripcion_completa = _build_perfil(client, nombre, transcripciones, costos=costos)
    fecha_process = datetime.now().strftime("%d/%m/%Y %H:%M")
    sheets.save_profile(nombre, fecha_process, json.dumps(perfil, ensure_ascii=False), transcripcion_completa)
    return perfil


def run(nombres: list[str], costos=None) -> dict[str, dict]:
    """Analyze each persona and return {nombre: perfil_dict}. Saves to Sheets."""
    from concurrent.futures import ThreadPoolExecutor, as_completed
    client = anthropic.Anthropic(timeout=120.0)
    results = {}

    def _tarea(nombre):
        return nombre, _analyze_persona(client, nombre, costos=costos)

    with ThreadPoolExecutor(max_workers=min(6, len(nombres))) as executor:
        futures = {executor.submit(_tarea, n): n for n in nombres}
        for future in as_completed(futures):
            nombre = futures[future]
            try:
                nombre, perfil = future.result()
                results[nombre] = perfil
            except Exception as e:
                print(f"[voice_agent] Error con {nombre}: {e}")
                results[nombre] = {"error": str(e)}
    return results


def run_from_firestore(familia_id: str, integrantes: list[dict], costos=None) -> dict[str, dict]:
    """
    Variante Firestore: lee transcripciones de Firestore por integrante_id,
    genera perfil de voz, guarda resultado en Firestore.
    Recibe lista de dicts con al menos {id, nombre}. Retorna {nombre: perfil_dict}.
    costos: pipeline.utils.costos.CostAccumulator opcional.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from pipeline.utils import firestore as fs

    if not integrantes:
        return {}

    client = anthropic.Anthropic(timeout=120.0)
    results = {}

    def _tarea(integrante: dict):
        nombre = integrante["nombre"]
        integrante_id = integrante["id"]
        transcripciones = fs.get_transcripciones_integrante(familia_id, integrante_id)

        # Fallback: si no hay transcripciones en Firestore, intentar Sheets
        if not transcripciones:
            transcripciones = sheets.get_transcripciones(nombre)
            if not transcripciones:
                raise ValueError(f"Sin transcripciones para {nombre} (ni Firestore ni Sheets)")

        perfil, transcripcion_completa = _build_perfil(client, nombre, transcripciones, costos=costos)
        fs.save_perfil_voz(familia_id, integrante_id, perfil, transcripcion_completa)
        return nombre, perfil

    with ThreadPoolExecutor(max_workers=min(6, len(integrantes))) as executor:
        futures = {executor.submit(_tarea, p): p for p in integrantes}
        for future in as_completed(futures):
            integrante = futures[future]
            nombre = integrante["nombre"]
            try:
                nombre, perfil = future.result()
                results[nombre] = perfil
            except Exception as e:
                print(f"[voice_agent] Error con {nombre}: {e}")
                results[nombre] = {"error": str(e)}
    return results
