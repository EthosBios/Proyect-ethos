import os
import json
import uuid as _uuid

from google.cloud import firestore

FIRESTORE_PROJECT_ID = os.environ.get("FIRESTORE_PROJECT_ID", "familia-marino")

_client = None


def _db() -> firestore.Client:
    global _client
    if _client is None:
        _client = firestore.Client(project=FIRESTORE_PROJECT_ID)
    return _client


def _ts_to_str(ts) -> str:
    """Convert a Firestore timestamp to dd/mm/YYYY string, or empty string."""
    if ts is None:
        return ""
    try:
        return ts.strftime("%d/%m/%Y")
    except AttributeError:
        return str(ts)


def _to_bool(value) -> bool:
    """Coerce a Firestore field to bool. Handles real bools and string variants
    ('true'/'false'/'1'/'0'/'si'/'no') that can sneak in via manual edits."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        return value.strip().lower() in ("true", "1", "si", "sí", "yes")
    return bool(value)


# ─── Familias ────────────────────────────────────────────────────────────────

def get_familia(familia_id: str) -> dict | None:
    doc = _db().collection("familias").document(familia_id).get()
    if not doc.exists:
        return None
    data = doc.to_dict()
    data["id"] = doc.id
    return data


def list_familias() -> list[dict]:
    docs = _db().collection("familias").stream()
    result = []
    for doc in docs:
        data = doc.to_dict()
        result.append({
            "id": doc.id,
            "nombre": data.get("nombre", ""),
            "estado": data.get("estado", ""),
            "pack": data.get("pack", ""),
            "integrantes_extra": data.get("integrantes_extra", 0),
            "fecha_compra": _ts_to_str(data.get("fecha_compra")),
            "fecha_entrega": _ts_to_str(data.get("fecha_entrega")),
            "comprador": data.get("comprador", {}),
        })
    return result


def update_familia_estado(familia_id: str, estado: str) -> None:
    _db().collection("familias").document(familia_id).update({"estado": estado})


def save_libro_url(familia_id: str, url: str) -> None:
    _db().collection("familias").document(familia_id).update({"libro_url": url})


# ─── Timestamps operativos (Briefing #32) ────────────────────────────────────

def update_familia_ultima_grabacion(familia_id: str) -> None:
    """Marca el momento del último audio recibido de cualquier integrante de la familia."""
    _db().collection("familias").document(familia_id).set(
        {"ultima_grabacion": firestore.SERVER_TIMESTAMP}, merge=True
    )


def marcar_pipeline_inicio(familia_id: str, paso_actual: str) -> None:
    """Marca el inicio de una corrida del pipeline y limpia pipeline_fin de corridas previas."""
    _db().collection("familias").document(familia_id).set(
        {
            "pipeline_inicio": firestore.SERVER_TIMESTAMP,
            "pipeline_fin": None,
            "pipeline_paso_actual": paso_actual,
        },
        merge=True,
    )


def update_pipeline_paso_actual(familia_id: str, paso_actual: str) -> None:
    _db().collection("familias").document(familia_id).set(
        {"pipeline_paso_actual": paso_actual}, merge=True
    )


def marcar_pipeline_fin(familia_id: str, ok: bool) -> None:
    _db().collection("familias").document(familia_id).set(
        {
            "pipeline_fin": firestore.SERVER_TIMESTAMP,
            "pipeline_paso_actual": None,
            "pipeline_ultima_corrida_ok": ok,
        },
        merge=True,
    )


def set_entregado_at(familia_id: str) -> None:
    """Se marca la primera vez que se envía el email de libro listo (Postmark)."""
    doc = _db().collection("familias").document(familia_id).get()
    if doc.exists and doc.to_dict().get("entregado_at"):
        return  # ya estaba seteado, no lo pisamos con reenvíos
    _db().collection("familias").document(familia_id).set(
        {"entregado_at": firestore.SERVER_TIMESTAMP}, merge=True
    )


def increment_numero_corrida(familia_id: str) -> int:
    """Contador que incrementa en cada ejecución del pipeline para la misma familia."""
    ref = _db().collection("familias").document(familia_id)
    ref.set({"pipeline_numero_corrida": firestore.Increment(1)}, merge=True)
    doc = ref.get()
    return (doc.to_dict() or {}).get("pipeline_numero_corrida", 1)


def save_costos(familia_id: str, costos: dict, numero_corrida: int) -> None:
    """Persiste el costo de la corrida en un subdoc (histórico) y actualiza el rollup en la familia."""
    from datetime import datetime, timezone

    _db().collection("familias").document(familia_id).collection("costos").document(
        str(numero_corrida)
    ).set(
        {
            **costos,
            "numero_corrida": numero_corrida,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )
    _db().collection("familias").document(familia_id).set({"costos": costos}, merge=True)


# ─── Integrantes ─────────────────────────────────────────────────────────────

def get_integrantes(familia_id: str) -> list[dict]:
    docs = (
        _db()
        .collection("familias")
        .document(familia_id)
        .collection("integrantes")
        .stream()
    )
    result = []
    for doc in docs:
        data = doc.to_dict()
        data["id"] = doc.id
        result.append(data)
    return result


def get_integrante_by_token(token: str) -> tuple[str, str, dict] | None:
    """
    Resolve a token to (familia_id, integrante_id, data) in O(1) via tokens/ collection.
    Falls back to full scan with a warning if the token is not indexed (legacy tokens).
    Returns None if not found.
    """
    import logging
    logger = logging.getLogger(__name__)

    token_doc = _db().collection("tokens").document(token).get()
    if token_doc.exists:
        ref = token_doc.to_dict()
        familia_id = ref.get("familia_id", "")
        integrante_id = ref.get("integrante_id", "")
        integrante_doc = (
            _db()
            .collection("familias")
            .document(familia_id)
            .collection("integrantes")
            .document(integrante_id)
            .get()
        )
        if integrante_doc.exists:
            data = integrante_doc.to_dict()
            data["id"] = integrante_doc.id
            return familia_id, integrante_id, data

    # Fallback: full scan for legacy tokens not yet in tokens/ collection
    logger.warning("[tokens] token %s no encontrado en colección tokens/, haciendo scan completo", token)
    familias = _db().collection("familias").stream()
    for familia_doc in familias:
        matches = (
            _db()
            .collection("familias")
            .document(familia_doc.id)
            .collection("integrantes")
            .where("token_unico", "==", token)
            .limit(1)
            .stream()
        )
        for integrante_doc in matches:
            data = integrante_doc.to_dict()
            data["id"] = integrante_doc.id
            return familia_doc.id, integrante_doc.id, data
    return None


def update_integrante_estado(familia_id: str, integrante_id: str, estado: str) -> None:
    (
        _db()
        .collection("familias")
        .document(familia_id)
        .collection("integrantes")
        .document(integrante_id)
        .update({"estado": estado})
    )


# ─── Respuestas ──────────────────────────────────────────────────────────────

def save_respuesta(familia_id: str, integrante_id: str, pregunta_id: str, audio_url: str) -> None:
    from datetime import datetime, timezone
    (
        _db()
        .collection("familias")
        .document(familia_id)
        .collection("integrantes")
        .document(integrante_id)
        .collection("respuestas")
        .document(str(pregunta_id))
        .set(
            {
                "audio_url": audio_url,
                "transcripcion": "",
                "duracion_seg": 0,
                "timestamp": datetime.now(timezone.utc),
            },
            merge=True,
        )
    )


def update_porcentaje_avance(familia_id: str, integrante_id: str, pct: int) -> None:
    (
        _db()
        .collection("familias")
        .document(familia_id)
        .collection("integrantes")
        .document(integrante_id)
        .update({"porcentaje_avance": pct})
    )


def save_voz_permanente(familia_id: str, integrante_id: str, voz_token: str, audio_gs_url: str) -> None:
    """Persiste el token permanente de voz bajo el integrante e indexa en voz_tokens/ para lookup O(1)."""
    _db().collection("familias").document(familia_id) \
        .collection("integrantes").document(integrante_id) \
        .update({"voz_token": voz_token, "voz_audio_url": audio_gs_url})
    _db().collection("voz_tokens").document(voz_token).set({
        "familia_id": familia_id,
        "integrante_id": integrante_id,
        "audio_gs_url": audio_gs_url,
    })


def get_integrante_by_voz_token(voz_token: str) -> tuple[str, str, dict] | None:
    """Resuelve un voz_token a (familia_id, integrante_id, data). None si no existe."""
    doc = _db().collection("voz_tokens").document(voz_token).get()
    if not doc.exists:
        return None
    ref = doc.to_dict()
    familia_id = ref.get("familia_id", "")
    integrante_id = ref.get("integrante_id", "")
    integrante_doc = (
        _db().collection("familias").document(familia_id)
        .collection("integrantes").document(integrante_id).get()
    )
    if not integrante_doc.exists:
        return None
    data = integrante_doc.to_dict()
    data["id"] = integrante_doc.id
    data["_audio_gs_url"] = ref.get("audio_gs_url", "")
    return familia_id, integrante_id, data


def update_integrante_foto(familia_id: str, integrante_id: str, foto_url: str) -> None:
    (
        _db()
        .collection("familias")
        .document(familia_id)
        .collection("integrantes")
        .document(integrante_id)
        .update({"foto_url": foto_url})
    )


def get_respuestas(familia_id: str, integrante_id: str) -> list[dict]:
    docs = (
        _db()
        .collection("familias")
        .document(familia_id)
        .collection("integrantes")
        .document(integrante_id)
        .collection("respuestas")
        .order_by("__name__")
        .stream()
    )
    result = []
    for doc in docs:
        data = doc.to_dict()
        data["id"] = doc.id
        result.append(data)
    return result


def get_transcripciones_integrante(familia_id: str, integrante_id: str) -> list[dict]:
    """Returns [{pregunta: str, transcripcion: str}] — only entries with non-empty transcripcion."""
    respuestas = get_respuestas(familia_id, integrante_id)
    result = []
    for r in respuestas:
        transcripcion = r.get("transcripcion", "").strip()
        if transcripcion:
            result.append({
                "pregunta": r.get("id", ""),
                "transcripcion": transcripcion,
            })
    return result


def save_transcripcion(
    familia_id: str, integrante_id: str, pregunta_id: str, transcripcion: str
) -> None:
    (
        _db()
        .collection("familias")
        .document(familia_id)
        .collection("integrantes")
        .document(integrante_id)
        .collection("respuestas")
        .document(pregunta_id)
        .update({"transcripcion": transcripcion})
    )


# ─── Perfil de voz y capítulo ────────────────────────────────────────────────

def save_perfil_voz(
    familia_id: str,
    integrante_id: str,
    perfil: dict,
    transcripcion_completa: str,
) -> None:
    (
        _db()
        .collection("familias")
        .document(familia_id)
        .collection("integrantes")
        .document(integrante_id)
        .update({
            "perfil_voz": perfil,
            "transcripcion_completa": transcripcion_completa,
        })
    )


def save_capitulo(familia_id: str, integrante_id: str, capitulo: str) -> None:
    (
        _db()
        .collection("familias")
        .document(familia_id)
        .collection("integrantes")
        .document(integrante_id)
        .update({"capitulo": capitulo})
    )


# ─── Para el orchestrator ─────────────────────────────────────────────────────

def get_integrantes_para_pipeline(familia_id: str) -> list[dict]:
    """
    Returns a list of dicts for each integrante with all fields needed by the pipeline:
      nombre, fecha_nac, rol, es_menor, vive, fecha_fallec,
      foto_url, relacion_con_comprador, perfil_voz (dict),
      transcripcion (str), capitulo (str)
    """
    integrantes = get_integrantes(familia_id)
    result = []
    for integrante in integrantes:
        fecha_nac_raw = integrante.get("fecha_nac")
        fecha_nac = _ts_to_str(fecha_nac_raw) if fecha_nac_raw else ""

        fecha_fallec_raw = integrante.get("fecha_fallec")
        fecha_fallec = _ts_to_str(fecha_fallec_raw) if fecha_fallec_raw else ""

        vive = not bool(fecha_fallec)

        perfil_voz = integrante.get("perfil_voz", {})
        if isinstance(perfil_voz, str):
            try:
                perfil_voz = json.loads(perfil_voz)
            except (json.JSONDecodeError, TypeError):
                perfil_voz = {}

        result.append({
            "id": integrante.get("id", ""),
            "nombre": integrante.get("nombre", ""),
            "fecha_nac": fecha_nac,
            "rol": integrante.get("rol", ""),
            "es_menor": _to_bool(integrante.get("es_menor", False)),
            "vive": vive,
            "fecha_fallec": fecha_fallec,
            "foto_url": integrante.get("foto_url", ""),
            "relacion_con_comprador": integrante.get("relacion_con_comprador", ""),
            "es_comprador": _to_bool(integrante.get("es_comprador", False)),
            "perfil_voz": perfil_voz,
            "transcripcion": integrante.get("transcripcion_completa", ""),
            "capitulo": integrante.get("capitulo", ""),
            "voz_token": integrante.get("voz_token", ""),
        })
    return result


def get_relaciones(familia_id: str) -> list[dict]:
    """
    Infer relations from relacion_con_comprador on each integrante.
    The comprador (es_comprador=True) is the reference point.
    Returns [{persona_a, relacion, persona_b}].
    Valid relaciones: padre, madre, cónyuge, esposo, esposa, hijo, hija
    """
    RELACIONES_VALIDAS = {"padre", "madre", "cónyuge", "conyuge", "esposo", "esposa", "hijo", "hija"}

    integrantes = get_integrantes(familia_id)

    comprador_nombre = None
    for integrante in integrantes:
        if integrante.get("es_comprador"):
            comprador_nombre = integrante.get("nombre", "")
            break

    if not comprador_nombre:
        return []

    result = []
    for integrante in integrantes:
        if integrante.get("es_comprador"):
            continue

        relacion = integrante.get("relacion_con_comprador", "").strip().lower()
        if not relacion:
            continue

        nombre_integrante = integrante.get("nombre", "")
        if not nombre_integrante:
            continue

        # Normalize cónyuge variants
        if relacion in ("cónyuge", "conyuge", "esposo", "esposa"):
            relacion_normalizada = relacion
        elif relacion in RELACIONES_VALIDAS:
            relacion_normalizada = relacion
        else:
            continue

        result.append({
            "persona_a": nombre_integrante,
            "relacion": relacion_normalizada,
            "persona_b": comprador_nombre,
        })

    return result


# ─── Onboarding: crear familia e integrantes ─────────────────────────────────

def create_familia(nombre: str, comprador: dict, pack: str, pais: str = "argentina") -> str:
    """Crea un doc de familia y retorna el familia_id (16 chars del UUID = 64 bits)."""
    familia_id = _uuid.uuid4().hex[:16]
    _db().collection("familias").document(familia_id).set({
        "nombre": nombre,
        "comprador": comprador,
        "pack": pack,
        "pais": pais,
        "estado": "onboarding",
        "integrantes_extra": 0,
        "fecha_compra": firestore.SERVER_TIMESTAMP,
        "fecha_entrega": None,
    })
    return familia_id


def add_integrante(
    familia_id: str,
    nombre: str,
    relacion_con_comprador: str,
    es_menor: bool = False,
    fecha_nac: str = "",
    es_comprador: bool = False,
) -> tuple[str, str]:
    """Agrega un integrante a la familia. Retorna (integrante_id, token_unico)."""
    integrante_id = _uuid.uuid4().hex[:8]
    token = str(_uuid.uuid4())
    db = _db()
    db.collection("familias").document(familia_id).collection("integrantes").document(integrante_id).set({
        "nombre": nombre,
        "relacion_con_comprador": relacion_con_comprador,
        "token_unico": token,
        "es_comprador": es_comprador,
        "es_menor": es_menor,
        "fecha_nac": fecha_nac,
        "estado": "pendiente",
        "foto_url": "",
        "porcentaje_avance": 0,
        "ultimo_acceso": None,
    })
    # Índice plano para resolución O(1)
    db.collection("tokens").document(token).set({
        "familia_id": familia_id,
        "integrante_id": integrante_id,
    })
    return integrante_id, token


# ─── Jobs (pipeline async) ────────────────────────────────────────────────────

def create_job(job_id: str, familia_id: str | None = None) -> None:
    from datetime import datetime, timezone
    _db().collection("jobs").document(job_id).set({
        "status": "pending",
        "familia_id": familia_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "result": None,
        "error": None,
    })


def update_job_status(job_id: str, status: str) -> None:
    _db().collection("jobs").document(job_id).update({"status": status})


def update_job_done(job_id: str, result: dict) -> None:
    _db().collection("jobs").document(job_id).update({
        "status": "done",
        "result": json.dumps(result, ensure_ascii=False),
    })


def update_job_error(job_id: str, error: str) -> None:
    _db().collection("jobs").document(job_id).update({
        "status": "error",
        "error": error,
    })


def update_job_alerta_enviada(job_id: str) -> None:
    _db().collection("jobs").document(job_id).update({"alerta_enviada": True})


def marcar_pipeline_fallido(job_id: str, familia_id: str, etapa: str, error: str) -> None:
    from datetime import datetime, timezone
    _db().collection("familias").document(familia_id).update({
        "estado": "pipeline_fallido",
        "pipeline_error": {
            "job_id": job_id,
            "etapa": etapa,
            "mensaje": error,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
    })


def get_job(job_id: str) -> dict | None:
    doc = _db().collection("jobs").document(job_id).get()
    if not doc.exists:
        return None
    data = doc.to_dict()
    if data.get("result") and isinstance(data["result"], str):
        try:
            data["result"] = json.loads(data["result"])
        except (json.JSONDecodeError, TypeError):
            pass
    return data


# ─── Access tokens (magic link auth) ─────────────────────────────────────────

def set_access_token(familia_id: str, token: str, expires_at) -> None:
    _db().collection("familias").document(familia_id).update({
        "access_token": token,
        "access_token_expires_at": expires_at,
    })


def get_familia_by_access_token(token: str) -> tuple[str, dict] | None:
    """Find familia by access_token. Returns (familia_id, data) or None if not found/expired."""
    from datetime import datetime, timezone
    if not token:
        return None
    docs = (
        _db().collection("familias")
        .where("access_token", "==", token)
        .limit(1)
        .stream()
    )
    for doc in docs:
        data = doc.to_dict()
        expires_at = data.get("access_token_expires_at")
        if expires_at:
            now = datetime.now(timezone.utc)
            try:
                if expires_at.tzinfo is None:
                    expires_at = expires_at.replace(tzinfo=timezone.utc)
                if expires_at < now:
                    return None
            except Exception:
                pass
        data["id"] = doc.id
        return doc.id, data
    return None


def get_access_token(familia_id: str) -> str | None:
    """Returns access_token if familia exists and token is not expired."""
    from datetime import datetime, timezone
    doc = _db().collection("familias").document(familia_id).get()
    if not doc.exists:
        return None
    data = doc.to_dict()
    token = data.get("access_token")
    expires_at = data.get("access_token_expires_at")
    if not token or not expires_at:
        return None
    now = datetime.now(timezone.utc)
    try:
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if expires_at < now:
            return None
    except Exception:
        pass
    return token


# ─── Temp tokens (display_token para gracias, onboarding_token para onboarding) ──

def create_temp_token(token: str, familia_id: str, ttl_minutes: int) -> None:
    """Store a short-lived single-purpose token. Used for pre-login endpoints."""
    from datetime import datetime, timezone, timedelta
    expires_at = (datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)).isoformat()
    _db().collection("temp_tokens").document(token).set({
        "familia_id": familia_id,
        "expires_at": expires_at,
    })


def consume_temp_token(token: str, familia_id: str) -> bool:
    """Validate and DELETE a temp token (single-use). Returns True if valid for that familia."""
    from datetime import datetime, timezone
    doc = _db().collection("temp_tokens").document(token).get()
    if not doc.exists:
        return False
    data = doc.to_dict()
    if data.get("familia_id") != familia_id:
        return False
    try:
        expires_at = datetime.fromisoformat(data["expires_at"])
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        if datetime.now(timezone.utc) > expires_at:
            doc.reference.delete()
            return False
    except Exception:
        return False
    doc.reference.delete()
    return True


def validate_temp_token(token: str, familia_id: str) -> bool:
    """Validate a temp token WITHOUT consuming it (multi-use within TTL)."""
    from datetime import datetime, timezone
    doc = _db().collection("temp_tokens").document(token).get()
    if not doc.exists:
        return False
    data = doc.to_dict()
    if data.get("familia_id") != familia_id:
        return False
    try:
        expires_at = datetime.fromisoformat(data["expires_at"])
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) <= expires_at
    except Exception:
        return False


def get_familia_by_email(email: str) -> tuple[str, dict] | None:
    """Find familia by comprador email. Returns (familia_id, data) or None."""
    docs = (
        _db().collection("familias")
        .where("comprador.email", "==", email)
        .limit(1)
        .stream()
    )
    for doc in docs:
        data = doc.to_dict()
        data["id"] = doc.id
        return doc.id, data
    return None


# ─── Evaluaciones de calidad (quality_agent) ─────────────────────────────────

def save_evaluacion_calidad(
    familia_id: str, integrante_id: str, evaluacion: dict, intento: int
) -> None:
    """Persiste la evaluación de calidad de un capítulo como subdoc numerado por intento."""
    (
        _db()
        .collection("familias")
        .document(familia_id)
        .collection("integrantes")
        .document(integrante_id)
        .collection("evaluaciones_calidad")
        .document(str(intento))
        .set(evaluacion)
    )


def mark_escalacion_humana(
    familia_id: str, integrante_id: str, motivo: str, evaluacion: dict
) -> None:
    """Marca un integrante para revisión humana y registra el motivo."""
    from datetime import datetime, timezone
    (
        _db()
        .collection("familias")
        .document(familia_id)
        .collection("integrantes")
        .document(integrante_id)
        .set(
            {
                "escalacion_humana": {
                    "motivo": motivo,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "ultima_evaluacion": evaluacion,
                }
            },
            merge=True,
        )
    )
    # Marcar también en la familia para visibilidad en el panel
    _db().collection("familias").document(familia_id).set(
        {"tiene_escalaciones": True}, merge=True
    )


def get_escalaciones(familia_id: str) -> list[dict]:
    """Lista los integrantes con escalación humana pendiente."""
    integrantes = get_integrantes(familia_id)
    return [
        {
            "integrante_id": i.get("id", ""),
            "nombre": i.get("nombre", ""),
            "escalacion": i.get("escalacion_humana", {}),
        }
        for i in integrantes
        if i.get("escalacion_humana")
    ]


# ─── Upsell checkouts ────────────────────────────────────────────────────────

def create_upsell_checkout(upsell_token: str, familia_id: str, nombre: str, relacion: str) -> None:
    from datetime import datetime, timezone
    _db().collection("upsell_checkouts").document(upsell_token).set({
        "familia_id": familia_id,
        "nombre": nombre,
        "relacion": relacion,
        "procesado": False,
        "created_at": datetime.now(timezone.utc).isoformat(),
    })


def get_upsell_checkout(upsell_token: str) -> dict | None:
    doc = _db().collection("upsell_checkouts").document(upsell_token).get()
    if not doc.exists:
        return None
    return doc.to_dict()


def mark_upsell_checkout_procesado(upsell_token: str) -> None:
    _db().collection("upsell_checkouts").document(upsell_token).update({"procesado": True})


def check_and_record_rate_limit(key: str, max_count: int, window_seconds: int) -> bool:
    """
    Check rate limit for a key. Records the hit if allowed.
    Returns True if under limit (allowed), False if exceeded (blocked).
    """
    from datetime import datetime, timezone, timedelta
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=window_seconds)

    ref = _db().collection("rate_limits").document(key)
    doc = ref.get()

    hits: list = []
    if doc.exists:
        for h in doc.to_dict().get("hits", []):
            try:
                if h.tzinfo is None:
                    h = h.replace(tzinfo=timezone.utc)
                if h > cutoff:
                    hits.append(h)
            except Exception:
                pass

    if len(hits) >= max_count:
        return False

    hits.append(now)
    ref.set({"hits": hits})
    return True
