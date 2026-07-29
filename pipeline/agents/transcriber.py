"""
Whisper-based transcriber agent.
Reads audio links from the Sheet, transcribes each one, writes back to col F.
"""

import os
import tempfile

from openai import OpenAI

from pipeline.utils import sheets
from pipeline.utils.retry import call_with_retry

# Regional vocabulary hints to nudge Whisper's acoustic model.
# These are NOT for analysis — voice_agent handles linguistic profiling.
_VOCAB_HINTS: dict[str, str] = {
    "argentina": (
        "che, boludo, pibe, mina, laburo, quilombo, morfar, chabón, guita, copado, "
        "birome, colectivo, campera, heladera, boliche, asado, mate, yerba, pileta, "
        "reo, trucho, posta, joda, fiaca, macanudo, bondi, verdulería, kiosco"
    ),
    "uruguay": (
        "che, gurí, pila, ta, barra, farra, cañero, torta, boliche, rambla, "
        "candombe, manya, manya eso, ta bien, un toque, pinta, cuchita, chiquilín"
    ),
    "chile": (
        "huevón, cachai, po, fome, cuático, bacán, pololo, polola, once, "
        "carrete, pega, plata, micro, guagua, cabro, buena onda, al tiro, "
        "agarrar papa, piola, weon, nan"
    ),
    "colombia": (
        "parcero, parce, bacano, berraco, chimba, gonorrea, marica, listo, "
        "chévere, qué más, pues, vaina, plata, finca, tinto, aguardiente, "
        "rumba, jarta, estar mamado"
    ),
    "mexico": (
        "güey, wey, chido, chavo, chava, neta, órale, ándale, chamba, lana, "
        "cuate, chela, torta, taco, chilango, mande, sale, a huevo, chingón, "
        "pendejo, mamón, naco, fresa"
    ),
    "venezuela": (
        "chamo, chama, pana, coño, vaina, chimbo, broma, ladilla, burda, "
        "arrechera, vergación, catire, hallaca, arepa, cachapa, pabellón, "
        "¿qué fue?, bacán, estar arecho"
    ),
    "peru": (
        "causa, pata, llave, bacán, pata, pe, ah no, está bravazo, "
        "chamba, chibolo, jerma, a la orden, seco y volteado, "
        "ceviche, lomo saltado, ¿cómo así?"
    ),
    "españa": (
        "tío, tía, mola, guay, chulo, vale, hostia, joder, coño, mazo, "
        "pisha, colega, pasta, curro, mogollón, chaval, flipar, rollo, "
        "¿qué tal?, venga"
    ),
}
_DEFAULT_HINTS = (
    "familia, recuerdos, infancia, trabajo, amor, abuelos, hijos, "
    "nietos, historia, vida, pueblo, campo, ciudad"
)


def _get_prompt(pais: str) -> str:
    key = pais.lower().strip()
    base = _VOCAB_HINTS.get(key, _DEFAULT_HINTS)
    return (
        f"Transcripción en español rioplatense. Vocabulario regional: {base}. "
        "Incluir muletillas y expresiones coloquiales tal como se dicen."
    )


def run(row_indices: list[int], pais: str = "argentina", costos=None) -> dict:
    """
    Transcribe audio for the given sheet row indices (1-based, skipping header).
    Updates col F (Transcripción) in the Sheet for each row.
    Returns {"procesadas": N, "errores": M}.
    costos: pipeline.utils.costos.CostAccumulator opcional, para acumular minutos Whisper.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"], timeout=60.0)
    prompt = _get_prompt(pais)
    all_rows = sheets.get_all_rows()

    def _transcribir_fila(row_idx: int) -> bool:
        row = all_rows[row_idx - 1]
        audio_url = row[sheets.COL_LINK_AUDIO - 1].strip() if len(row) >= sheets.COL_LINK_AUDIO else ""
        if not audio_url:
            return False

        with tempfile.NamedTemporaryFile(suffix=".webm", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            sheets.download_drive_file(audio_url, tmp_path)
            with open(tmp_path, "rb") as audio_file:
                result = call_with_retry(
                    client.audio.transcriptions.create,
                    model="whisper-1",
                    file=audio_file,
                    language="es",
                    prompt=prompt,
                    response_format="verbose_json",
                    label="whisper/fila",
                )
            if costos is not None:
                costos.add_whisper_minutos(getattr(result, "duration", 0) / 60)
            sheets.save_transcription(row_idx, result.text.strip())
            return True
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    procesadas = 0
    errores = 0

    with ThreadPoolExecutor(max_workers=min(8, len(row_indices))) as executor:
        futures = {executor.submit(_transcribir_fila, idx): idx for idx in row_indices}
        for future in as_completed(futures):
            row_idx = futures[future]
            try:
                if future.result():
                    procesadas += 1
                else:
                    errores += 1
            except Exception as e:
                print(f"[transcriber] Error en fila {row_idx}: {e}")
                errores += 1

    return {"procesadas": procesadas, "errores": errores}


def run_from_firestore(
    familia_id: str,
    pais: str = "argentina",
    costos=None,
    integrante_ids: set[str] | None = None,
) -> dict:
    """
    Transcribe los audios pendientes de una familia desde Firestore/GCS.
    Solo procesa respuestas donde audio_url está seteado y transcripcion está vacía.
    integrante_ids: si se pasa, solo procesa esos integrantes (filtro de corrida parcial).
    costos: pipeline.utils.costos.CostAccumulator opcional, para acumular minutos Whisper.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from pipeline.utils import firestore as fs, storage as gcs_storage

    client = OpenAI(api_key=os.environ["OPENAI_API_KEY"], timeout=60.0)
    prompt = _get_prompt(pais)

    # Recolectar tareas pendientes — respetando el filtro de integrantes si se especificó
    integrantes = fs.get_integrantes(familia_id)
    tasks = []  # [(integrante_id, pregunta_id, audio_url)]
    for integrante in integrantes:
        integrante_id = integrante["id"]
        if integrante_ids is not None and integrante_id not in integrante_ids:
            continue
        respuestas = fs.get_respuestas(familia_id, integrante_id)
        for r in respuestas:
            audio_url = r.get("audio_url", "").strip()
            transcripcion = r.get("transcripcion", "").strip()
            if audio_url and not transcripcion:
                tasks.append((integrante_id, r["id"], audio_url))

    if not tasks:
        return {"procesadas": 0, "errores": 0}

    def _transcribir(integrante_id: str, pregunta_id: str, audio_url: str) -> bool:
        suffix = ".webm" if audio_url.endswith(".webm") else ".mp3"
        with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
            tmp_path = tmp.name
        try:
            gcs_storage.download_from_gcs(audio_url, tmp_path)
            with open(tmp_path, "rb") as audio_file:
                result = call_with_retry(
                    client.audio.transcriptions.create,
                    model="whisper-1",
                    file=audio_file,
                    language="es",
                    prompt=prompt,
                    response_format="verbose_json",
                    label="whisper/firestore",
                )
            if costos is not None:
                costos.add_whisper_minutos(getattr(result, "duration", 0) / 60)
            fs.save_transcripcion(familia_id, integrante_id, pregunta_id, result.text.strip())
            return True
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    procesadas = 0
    errores = 0
    with ThreadPoolExecutor(max_workers=min(8, len(tasks))) as executor:
        futures = {
            executor.submit(_transcribir, iid, pid, url): (iid, pid)
            for iid, pid, url in tasks
        }
        for future in as_completed(futures):
            iid, pid = futures[future]
            try:
                if future.result():
                    procesadas += 1
                else:
                    errores += 1
            except Exception as e:
                print(f"[transcriber] Error en {iid}/{pid}: {e}")
                errores += 1

    return {"procesadas": procesadas, "errores": errores}
