import os
import json
import re
import tempfile
import threading
import time

import gspread
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# Cache de lecturas para evitar rate limit 429 cuando múltiples threads leen en paralelo
_cache: dict = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 300  # 5 minutos


def _cached(key: str, fetcher):
    """Retorna valor cacheado o llama fetcher() y lo guarda."""
    with _cache_lock:
        entry = _cache.get(key)
        if entry and (time.time() - entry["ts"]) < _CACHE_TTL:
            return entry["data"]
    data = fetcher()
    with _cache_lock:
        _cache[key] = {"data": data, "ts": time.time()}
    return data


def invalidate_cache():
    """Llama al inicio del pipeline para forzar datos frescos."""
    with _cache_lock:
        _cache.clear()

SHEET_ID = os.environ.get("SHEET_ID", "1A1M79ITLeRVWkwct7pqjUTmLu9NWXn9uDLpKWMMomgM")
FAMILIA_SHEET_ID = os.environ.get("FAMILIA_SHEET_ID", "1iEpnly_f3OQL6nLH41XU76zg1iM2vHZQyQdF0RLVQFE")
FOLDER_ID = os.environ.get("FOLDER_ID", "1rZmvh5WC9KEPSQ99AtC3ZvJkJRKJp6L3")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "familia-marino-pdfs")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# Column indices (1-based for gspread)
COL_FECHA = 1
COL_NOMBRE = 2
COL_FECHA_NAC = 3
COL_PREGUNTA = 4
COL_LINK_AUDIO = 5
COL_TRANSCRIPCION = 6
COL_FOTO = 7


def _get_creds():
    creds_json = os.environ.get("GCP_SA_KEY_JSON")
    if creds_json:
        info = json.loads(creds_json)
    else:
        path = os.environ.get("GOOGLE_CREDENTIALS_FILE", "/secrets/credentials.json")
        with open(path) as f:
            info = json.load(f)
    return service_account.Credentials.from_service_account_info(info, scopes=SCOPES)


_gc_client = None


def _gc():
    global _gc_client
    if _gc_client is None:
        _gc_client = gspread.authorize(_get_creds())
    return _gc_client


def _ss():
    return _gc().open_by_key(SHEET_ID)


def _ensure_sheet(ss, name: str, headers: list[str]):
    try:
        ws = ss.worksheet(name)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(name, rows=1000, cols=len(headers))
        ws.append_row(headers)
    return ws


def respuestas_sheet():
    ss = _ss()
    return _ensure_sheet(
        ss,
        "Respuestas",
        ["Fecha/hora", "Nombre", "FechaNac", "Nº pregunta", "Link Audio", "Transcripción", "Fotografía"],
    )


def perfiles_sheet():
    ss = _ss()
    return _ensure_sheet(
        ss,
        "Perfiles",
        ["Nombre", "FechaProcess", "PerfilVoz(JSON)", "TranscripciónCompleta", "Capítulo", "CapítuloRevisado"],
    )


def _familia_ss():
    return _gc().open_by_key(FAMILIA_SHEET_ID)


def integrantes_sheet():
    ss = _familia_ss()
    return _ensure_sheet(
        ss,
        "Integrantes",
        ["Nombre completo", "Fecha Nac (YYYY-MM-DD)", "Fecha Fallec (YYYY-MM-DD)", "Rol familiar", "¿Es menor de edad?"],
    )


def relaciones_sheet():
    ss = _familia_ss()
    return _ensure_sheet(
        ss,
        "Relaciones",
        ["Persona A", "Relación", "Persona B"],
    )


# ─── Respuestas ───────────────────────────────────────────────────────────────

def get_all_rows() -> list[list[str]]:
    return _cached("all_rows", lambda: respuestas_sheet().get_all_values())


def get_rows_for_name(nombre: str) -> list[tuple[int, list[str]]]:
    """Return [(sheet_row_index, row_data), ...] for a persona (1-based, includes header offset)."""
    all_rows = get_all_rows()
    result = []
    for i, row in enumerate(all_rows[1:], start=2):
        if len(row) >= 2 and row[1].strip().lower() == nombre.strip().lower():
            result.append((i, row))
    return result


def save_transcription(row_index: int, text: str):
    ws = respuestas_sheet()
    ws.update_cell(row_index, COL_TRANSCRIPCION, text)


def get_foto_url(nombre: str) -> str | None:
    all_rows = get_all_rows()
    for row in all_rows[1:]:
        if (
            len(row) >= COL_FOTO
            and row[COL_NOMBRE - 1].strip().lower() == nombre.strip().lower()
            and row[COL_FOTO - 1].strip()
        ):
            return row[COL_FOTO - 1].strip()
    return None


def get_transcripciones(nombre: str) -> list[dict]:
    rows = get_rows_for_name(nombre)
    result = []
    for _, row in rows:
        transcripcion = row[COL_TRANSCRIPCION - 1].strip() if len(row) >= COL_TRANSCRIPCION else ""
        if transcripcion:
            result.append(
                {
                    "pregunta": row[COL_PREGUNTA - 1] if len(row) >= COL_PREGUNTA else "",
                    "transcripcion": transcripcion,
                }
            )
    return result


def get_fecha_nac(nombre: str) -> str:
    rows = get_rows_for_name(nombre)
    if rows:
        row = rows[0][1]
        return row[COL_FECHA_NAC - 1] if len(row) >= COL_FECHA_NAC else ""
    return ""


def get_all_nombres() -> list[str]:
    all_rows = get_all_rows()
    nombres = set()
    for row in all_rows[1:]:
        if len(row) >= 2 and row[1].strip():
            nombres.add(row[1].strip())
    return sorted(nombres)


# ─── Perfiles ─────────────────────────────────────────────────────────────────

def save_profile(nombre: str, fecha_process: str, perfil_json: str, transcripcion_completa: str):
    ws = perfiles_sheet()
    all_rows = ws.get_all_values()
    for i, row in enumerate(all_rows[1:], start=2):
        if row and row[0].strip().lower() == nombre.strip().lower():
            ws.update(f"A{i}:D{i}", [[nombre, fecha_process, perfil_json, transcripcion_completa]])
            return
    ws.append_row([nombre, fecha_process, perfil_json, transcripcion_completa, "", ""])


def save_chapter(nombre: str, capitulo: str, capitulo_revisado: str = ""):
    ws = perfiles_sheet()
    all_rows = ws.get_all_values()
    for i, row in enumerate(all_rows[1:], start=2):
        if row and row[0].strip().lower() == nombre.strip().lower():
            ws.update_cell(i, 5, capitulo)
            if capitulo_revisado:
                ws.update_cell(i, 6, capitulo_revisado)
            return
    ws.append_row([nombre, "", "", "", capitulo, capitulo_revisado])


def get_profile(nombre: str) -> dict | None:
    ws = perfiles_sheet()
    all_rows = _cached("perfiles_rows", lambda: ws.get_all_values())
    for row in all_rows[1:]:
        if row and row[0].strip().lower() == nombre.strip().lower():
            perfil_str = row[2] if len(row) > 2 else ""
            try:
                perfil_voz = json.loads(perfil_str) if perfil_str else {}
            except json.JSONDecodeError:
                perfil_voz = {}
            return {
                "nombre": row[0] if len(row) > 0 else "",
                "fecha_process": row[1] if len(row) > 1 else "",
                "perfil_voz": perfil_voz,
                "transcripcion": row[3] if len(row) > 3 else "",
                "capitulo": row[4] if len(row) > 4 else "",
                "capitulo_revisado": row[5] if len(row) > 5 else "",
            }
    return None


# ─── Familia: Integrantes + Relaciones ───────────────────────────────────────

def get_familia_integrantes() -> list[dict]:
    """
    Returns list of integrante dicts:
      nombre, fecha_nac, fecha_fallec, rol, es_menor, vive
    """
    ws = integrantes_sheet()
    rows = _cached("integrantes_rows", lambda: ws.get_all_values())
    if len(rows) < 2:
        return []
    result = []
    for row in rows[1:]:
        if not any(cell.strip() for cell in row):
            continue
        nombre = row[0].strip() if len(row) > 0 else ""
        if not nombre:
            continue
        fecha_fallec = row[2].strip() if len(row) > 2 else ""
        result.append({
            "nombre": nombre,
            "fecha_nac": row[1].strip() if len(row) > 1 else "",
            "fecha_fallec": fecha_fallec,
            "rol": row[3].strip().lower() if len(row) > 3 else "",
            "es_menor": (row[4].strip().lower() in ("sí", "si", "s", "yes")) if len(row) > 4 else False,
            "vive": not bool(fecha_fallec),
        })
    return result


def get_familia_relaciones() -> list[dict]:
    """
    Returns list of relation dicts: {persona_a, relacion, persona_b}
    relacion is one of: padre, madre, cónyuge
    """
    ws = relaciones_sheet()
    rows = _cached("relaciones_rows", lambda: ws.get_all_values())
    if len(rows) < 2:
        return []
    result = []
    for row in rows[1:]:
        if not any(cell.strip() for cell in row):
            continue
        persona_a = row[0].strip() if len(row) > 0 else ""
        relacion = row[1].strip().lower() if len(row) > 1 else ""
        persona_b = row[2].strip() if len(row) > 2 else ""
        if persona_a and relacion and persona_b:
            result.append({"persona_a": persona_a, "relacion": relacion, "persona_b": persona_b})
    return result


def get_integrante(nombre: str) -> dict | None:
    integrantes = get_familia_integrantes()
    nombre_lower = nombre.strip().lower()
    for p in integrantes:
        if p["nombre"].lower() == nombre_lower:
            return p
    return None


def build_family_context(nombre: str, integrantes: list[dict], relaciones: list[dict]) -> dict:
    """
    Returns a context dict for one person:
      rol, vive, fecha_fallec, conyuges, hijos, padres, hermanos (inferred)
    """
    nombre_lower = nombre.strip().lower()

    # Direct relations where this person appears
    conyuges, padres_de, hijos_de = [], [], []
    for r in relaciones:
        a, rel, b = r["persona_a"].lower(), r["relacion"], r["persona_b"].lower()
        if rel == "cónyuge" or rel == "conyuge":
            if a == nombre_lower:
                conyuges.append(r["persona_b"])
            elif b == nombre_lower:
                conyuges.append(r["persona_a"])
        elif rel in ("padre", "madre"):
            if a == nombre_lower:
                # nombre es padre/madre de persona_b
                hijos_de.append(r["persona_b"])
            elif b == nombre_lower:
                # nombre es hijo de persona_a
                padres_de.append(r["persona_a"])

    # Infer siblings: share at least one parent
    siblings = set()
    mis_padres = {r["persona_a"].lower() for r in relaciones
                  if r["persona_b"].lower() == nombre_lower and r["relacion"] in ("padre", "madre")}
    for r in relaciones:
        if r["relacion"] in ("padre", "madre") and r["persona_a"].lower() in mis_padres:
            if r["persona_b"].lower() != nombre_lower:
                siblings.add(r["persona_b"])

    integrante = next(
        (p for p in integrantes if p["nombre"].strip().lower() == nombre_lower), {}
    )
    return {
        "rol": integrante.get("rol", ""),
        "vive": integrante.get("vive", True),
        "fecha_fallec": integrante.get("fecha_fallec", ""),
        "es_menor": integrante.get("es_menor", False),
        "conyuges": conyuges,
        "hijos": hijos_de,
        "padres": padres_de,
        "hermanos": sorted(siblings),
    }


def get_fallecidos(integrantes: list[dict]) -> list[dict]:
    return [p for p in integrantes if not p["vive"]]


# ─── Drive ────────────────────────────────────────────────────────────────────

def _extract_file_id(url: str) -> str:
    for pattern in [r"/d/([a-zA-Z0-9_-]+)", r"id=([a-zA-Z0-9_-]+)"]:
        m = re.search(pattern, url)
        if m:
            return m.group(1)
    raise ValueError(f"Cannot extract Drive file ID from: {url}")


def download_drive_file(url: str, dest_path: str):
    file_id = _extract_file_id(url)
    creds = _get_creds()
    service = build("drive", "v3", credentials=creds)
    request = service.files().get_media(fileId=file_id)
    with open(dest_path, "wb") as fh:
        dl = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = dl.next_chunk()


def upload_to_gcs(local_path: str, filename: str, mime_type: str = "application/pdf") -> str:
    """Upload a file to GCS and return its public URL."""
    from google.cloud import storage as gcs

    client = gcs.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(f"pdfs/{filename}")
    blob.upload_from_filename(local_path, content_type=mime_type)
    blob.make_public()
    return blob.public_url
