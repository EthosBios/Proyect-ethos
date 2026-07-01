"""
FastAPI entrypoint for the pipeline.
All heavy work happens in the agent modules.
"""

import base64
import hashlib
import hmac
import json
import logging
import os
import tempfile
import time as _time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
from fastapi import Depends, FastAPI, File, Form, Header, HTTPException, Query, Request, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from itsdangerous import BadSignature, URLSafeTimedSerializer
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

_STATIC_DIR = Path(__file__).parent / "static"

logger = logging.getLogger(__name__)

from pipeline.agents import orchestrator, transcriber, voice_agent, chapter_agent, layout_agent
from pipeline.agents.editor_agent import BookManuscript
from pipeline.utils import sheets

# ─── Async job store (Firestore) ─────────────────────────────────────────────

def _enviar_alerta_pipeline_fallido(job_id: str, familia_id: str, errores: list[str]) -> None:
    from pipeline.utils import firestore as fs
    from pipeline.utils.email import send_alerta_admin

    job = fs.get_job(job_id)
    if job and job.get("alerta_enviada"):
        logger.info("[alerta-pipeline] ya enviada para job=%s, skipping", job_id)
        return

    first_error = errores[0] if errores else "desconocida"
    etapa = first_error.split(":")[0].split("/")[0]
    error_detalle = "\n".join(errores[:5])

    try:
        fs.marcar_pipeline_fallido(job_id, familia_id, etapa, error_detalle)
        send_alerta_admin(familia_id=familia_id, job_id=job_id, etapa=etapa, error=error_detalle)
        fs.update_job_alerta_enviada(job_id)
        logger.error("[alerta-pipeline] enviada para job=%s familia=%s etapa=%s", job_id, familia_id, etapa)
    except Exception as exc:  # noqa: BLE001
        logger.error("[alerta-pipeline] error enviando alerta para job=%s: %s", job_id, exc)


def _run_pipeline_job(job_id: str, req_dict: dict) -> None:
    from pipeline.utils import firestore as fs
    fs.update_job_status(job_id, "running")
    familia_id_job = req_dict.get("familia_id")
    try:
        result = orchestrator.run(
            nombres=req_dict["nombres"],
            pais=req_dict["pais"],
            solo_desde=req_dict["solo_desde"],
            familia=req_dict["familia"],
            upload_to_gcs=req_dict["upload_to_gcs"],
            familia_id=familia_id_job,
            from_job_id=req_dict.get("from_job_id"),
        )
        payload = {
            "ok": result.ok,
            "personas": result.personas,
            "transcriber": result.transcriber,
            "voice": {k: v for k, v in result.voice.items()},
            "chapters_generados": list(result.chapters.keys()),
            "chapters": result.chapters,
            "orden": result.editor.orden if result.editor else [],
            "prologo": result.editor.prologo if result.editor else "",
            "epilogo": result.editor.epilogo if result.editor else "",
            "transiciones": result.editor.transiciones if result.editor else {},
            "layout": result.layout,
            "errores": result.errores,
        }
        fs.update_job_done(job_id, payload)
        if familia_id_job and result.ok:
            layout_url = result.layout or ""
            if layout_url.startswith("gs://"):
                fs.save_libro_url(familia_id_job, layout_url)
            fs.update_familia_estado(familia_id_job, "entregado")
            try:
                from pipeline.utils import firestore as _fs_email, storage as st
                from pipeline.utils.email import send_libro_listo
                familia = _fs_email.get_familia(familia_id_job) or {}
                comprador = familia.get('comprador', {})
                comprador_email = comprador.get('email', '')
                nombre_familia = familia.get('nombre', 'tu familia')
                if comprador_email and layout_url:
                    signed = st.get_signed_url(layout_url, expiration_hours=168)
                    send_libro_listo(email_comprador=comprador_email, nombre_familia=nombre_familia, signed_url=signed)
                    logger.info('[email-libro-listo] enviado a %s', comprador_email)
            except Exception as exc:  # noqa: BLE001
                logger.warning('[email-libro-listo] error: %s', exc)
        elif familia_id_job and not result.ok:
            _enviar_alerta_pipeline_fallido(job_id, familia_id_job, result.errores)
    except Exception as exc:  # noqa: BLE001
        fs.update_job_error(job_id, str(exc))
        if familia_id_job:
            _enviar_alerta_pipeline_fallido(job_id, familia_id_job, [str(exc)])

def _admin_auth(x_admin_key: str | None = Header(default=None)) -> None:
    pwd = os.environ.get("ADMIN_PASSWORD", "")
    if not pwd or x_admin_key != pwd:
        raise HTTPException(status_code=401, detail="No autorizado")


# ─── Session helpers (itsdangerous cookie) ───────────────────────────────────

_SESSION_COOKIE = "session"
_SESSION_MAX_AGE = 30 * 24 * 3600  # 30 days


def _session_serializer() -> URLSafeTimedSerializer:
    secret = os.environ.get("SESSION_SECRET", "")
    if not secret:
        raise RuntimeError("SESSION_SECRET no configurado")
    return URLSafeTimedSerializer(secret, salt="session")


def _sign_session(familia_id: str) -> str:
    return _session_serializer().dumps({"familia_id": familia_id})


def _verify_session(cookie_value: str) -> str | None:
    """Returns familia_id if cookie is valid and not expired, None otherwise."""
    try:
        data = _session_serializer().loads(cookie_value, max_age=_SESSION_MAX_AGE)
        return data.get("familia_id")
    except (BadSignature, Exception):
        return None


# ─── Stripe webhook signature verification ───────────────────────────────────

def _verify_mp_signature(data_id: str, x_request_id: str, ts: str, v1: str, secret: str) -> bool:
    """Verify MercadoPago webhook v2 HMAC-SHA256 signature."""
    signed_payload = f"id:{data_id};request-id:{x_request_id};ts:{ts};"
    expected = hmac.new(
        secret.encode("utf-8"),
        signed_payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, v1)


def _verify_stripe_signature(payload: bytes, sig_header: str, secret: str) -> bool:
    timestamp: int | None = None
    v1_sigs: list[str] = []
    for part in sig_header.split(","):
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        if k.strip() == "t":
            try:
                timestamp = int(v.strip())
            except ValueError:
                pass
        elif k.strip() == "v1":
            v1_sigs.append(v.strip())

    if timestamp is None or not v1_sigs:
        return False

    if abs(_time.time() - timestamp) > 300:  # 5-minute tolerance
        return False

    signed_payload = f"{timestamp}.{payload.decode('utf-8')}"
    expected = hmac.new(
        secret.encode("utf-8"),
        signed_payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return any(hmac.compare_digest(expected, sig) for sig in v1_sigs)


limiter = Limiter(key_func=get_remote_address)

app = FastAPI(title="Familia Libro Pipeline", version="1.0")
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://fedegiacomozzi.github.io",
        "https://ethosbios.com",
        "https://www.ethosbios.com",
        "https://ethosbios.vercel.app",
    ],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Health ───────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/health/deep")
def health_deep():
    import os
    import time as _time

    checks: dict[str, dict] = {}

    # 1. Sheets (gspread): read first row
    t0 = _time.monotonic()
    try:
        sheets.get_all_nombres()  # lightweight read; raises on auth/network errors
        checks["sheets"] = {"ok": True, "latency_ms": int((_time.monotonic() - t0) * 1000), "error": None}
    except Exception as exc:  # noqa: BLE001
        checks["sheets"] = {"ok": False, "latency_ms": int((_time.monotonic() - t0) * 1000), "error": str(exc)}

    # 2. Anthropic: minimal message
    t0 = _time.monotonic()
    try:
        import anthropic as _anthropic
        _client = _anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
        _client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=1,
            messages=[{"role": "user", "content": "ping"}],
        )
        checks["anthropic"] = {"ok": True, "latency_ms": int((_time.monotonic() - t0) * 1000), "error": None}
    except Exception as exc:  # noqa: BLE001
        checks["anthropic"] = {"ok": False, "latency_ms": int((_time.monotonic() - t0) * 1000), "error": str(exc)}

    # 3. GCS: verificar bucket de libros
    t0 = _time.monotonic()
    try:
        from google.cloud import storage as _gcs
        from pipeline.utils.storage import GCS_BUCKET_LIBROS
        _gcs_client = _gcs.Client()
        _bucket = _gcs_client.get_bucket(GCS_BUCKET_LIBROS)
        _ = _bucket.name  # forces the API call
        checks["gcs"] = {"ok": True, "latency_ms": int((_time.monotonic() - t0) * 1000), "error": None}
    except Exception as exc:  # noqa: BLE001
        checks["gcs"] = {"ok": False, "latency_ms": int((_time.monotonic() - t0) * 1000), "error": str(exc)}

    # 4. OpenAI: list models
    t0 = _time.monotonic()
    try:
        import openai as _openai
        _openai_client = _openai.OpenAI(api_key=os.environ["OPENAI_API_KEY"])
        _openai_client.models.list()
        checks["openai"] = {"ok": True, "latency_ms": int((_time.monotonic() - t0) * 1000), "error": None}
    except Exception as exc:  # noqa: BLE001
        checks["openai"] = {"ok": False, "latency_ms": int((_time.monotonic() - t0) * 1000), "error": str(exc)}

    overall_ok = all(v["ok"] for v in checks.values())
    return {"ok": overall_ok, "checks": checks}


# ─── Redirect de token de grabación ──────────────────────────────────────────

@app.get("/r/{token}")
def redirect_token(token: str):
    from pipeline.utils import firestore as fs
    if fs.get_integrante_by_token(token) is None:
        raise HTTPException(status_code=404, detail="Token inválido o no encontrado")
    return RedirectResponse(url=f"/recording?token={token}")


@app.get("/recording")
def serve_recording():
    return FileResponse(_STATIC_DIR / "recording.html")


# ─── Full pipeline ────────────────────────────────────────────────────────────

class PipelineRequest(BaseModel):
    nombres: list[str]
    pais: str = "argentina"
    solo_desde: str | None = None
    familia: str = ""
    upload_to_gcs: bool = False
    familia_id: str | None = None
    from_job_id: str | None = None  # reutilizar capítulos de un job anterior


@app.post("/run/pipeline")
def run_pipeline(req: PipelineRequest, _: None = Depends(_admin_auth)):
    result = orchestrator.run(
        nombres=req.nombres,
        pais=req.pais,
        solo_desde=req.solo_desde,
        familia=req.familia,
        upload_to_gcs=req.upload_to_gcs,
        familia_id=req.familia_id,
    )
    return {
        "ok": result.ok,
        "personas": result.personas,
        "transcriber": result.transcriber,
        "voice": {k: v for k, v in result.voice.items()},
        "chapters_generados": list(result.chapters.keys()),
        "orden": result.editor.orden if result.editor else [],
        "layout": result.layout,
        "errores": result.errores,
    }


@app.post("/run/pipeline/async")
def run_pipeline_async(req: PipelineRequest, _: None = Depends(_admin_auth)):
    from pipeline.utils import firestore as fs
    from pipeline.utils.tasks import enqueue_pipeline
    job_id = str(uuid.uuid4())
    fs.create_job(job_id, familia_id=req.familia_id)
    task_name = enqueue_pipeline(job_id, req.model_dump())
    return {"job_id": job_id, "status": "pending", "task_name": task_name}


class WorkerRequest(PipelineRequest):
    job_id: str


@app.post("/run/pipeline/worker")
def run_pipeline_worker(
    req: WorkerRequest,
    x_cloudtasks_queuename: str | None = Header(default=None),
):
    expected = os.environ.get("CLOUD_TASKS_QUEUE", "pipeline-jobs")
    if x_cloudtasks_queuename != expected:
        raise HTTPException(status_code=403, detail="Forbidden")
    _run_pipeline_job(req.job_id, req.model_dump())
    return {"ok": True, "job_id": req.job_id}


@app.get("/job/{job_id}")
def get_job_status(job_id: str):
    from pipeline.utils import firestore as fs, storage as st
    job = fs.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
    response: dict = {
        "job_id": job_id,
        "status": job["status"],
        "created_at": job.get("created_at", ""),
        "familia_id": job.get("familia_id"),
    }
    if job["status"] == "done":
        result = job.get("result") or {}
        gs_url = result.get("layout", "")
        pdf_url = None
        if gs_url and gs_url.startswith("gs://"):
            try:
                pdf_url = st.get_signed_url(gs_url, expiration_hours=168)  # 7 días
            except Exception as exc:  # noqa: BLE001
                logger.warning("No se pudo generar signed URL para %s: %s", gs_url, exc)
                pdf_url = gs_url
        response["pdf_url"] = pdf_url
        response["result"] = result
    elif job["status"] == "error":
        response["error"] = job.get("error")
    return response


# ─── Paso 1: Transcriber ──────────────────────────────────────────────────────

class TranscriberRequest(BaseModel):
    row_indices: list[int]
    pais: str = "argentina"


@app.post("/run/transcriber")
def run_transcriber(req: TranscriberRequest, _: None = Depends(_admin_auth)):
    result = transcriber.run(req.row_indices, req.pais)
    return result


# ─── Paso 2: Voice agent ──────────────────────────────────────────────────────

class NombresRequest(BaseModel):
    nombres: list[str]


@app.post("/run/voice")
def run_voice(req: NombresRequest, _: None = Depends(_admin_auth)):
    result = voice_agent.run(req.nombres)
    return result


# ─── Paso 3: Chapters ─────────────────────────────────────────────────────────

@app.post("/run/chapters")
def run_chapters(req: NombresRequest, _: None = Depends(_admin_auth)):
    result = chapter_agent.run(req.nombres)
    return {"chapters": {k: len(v) for k, v in result.items()}}


# ─── Paso 4: Editor ───────────────────────────────────────────────────────────

class EditorRequest(BaseModel):
    nombres: list[str]


@app.post("/run/editor")
def run_editor(req: EditorRequest, _: None = Depends(_admin_auth)):
    from pipeline.agents import editor_agent

    personas_meta = []
    capitulos = {}
    for nombre in req.nombres:
        p = sheets.get_profile(nombre)
        if not p:
            raise HTTPException(status_code=404, detail=f"Perfil no encontrado: {nombre}")
        personas_meta.append(
            {
                "nombre": nombre,
                "fecha_nac": sheets.get_fecha_nac(nombre),
                "perfil_voz": p.get("perfil_voz", {}),
            }
        )
        capitulos[nombre] = p.get("capitulo", "")

    manuscript = editor_agent.run(personas_meta, capitulos)
    return {
        "orden": manuscript.orden,
        "prologo_chars": len(manuscript.prologo),
        "epilogo_chars": len(manuscript.epilogo),
        "transiciones": list(manuscript.transiciones.keys()),
    }


# ─── Paso 5: Layout ───────────────────────────────────────────────────────────

class LayoutRequest(BaseModel):
    nombres: list[str]
    familia: str = ""
    upload_to_gcs: bool = False


@app.post("/run/layout")
def run_layout(req: LayoutRequest, _: None = Depends(_admin_auth)):
    from pipeline.agents import editor_agent

    personas_meta = []
    capitulos = {}
    for nombre in req.nombres:
        p = sheets.get_profile(nombre)
        if not p:
            raise HTTPException(status_code=404, detail=f"Perfil no encontrado: {nombre}")
        personas_meta.append(
            {
                "nombre": nombre,
                "fecha_nac": sheets.get_fecha_nac(nombre),
                "perfil_voz": p.get("perfil_voz", {}),
            }
        )
        capitulos[nombre] = p.get("capitulo_revisado") or p.get("capitulo", "")

    manuscript = editor_agent.run(personas_meta, capitulos)
    pdf_path = layout_agent.run(
        manuscript=manuscript,
        personas_meta=personas_meta,
        nombre_familia=req.familia,
    )

    if req.upload_to_gcs:
        import os
        gcs_url = sheets.upload_to_gcs(pdf_path, os.path.basename(pdf_path), "application/pdf")
        return {"pdf": gcs_url, "uploaded": True}

    return {"pdf": pdf_path, "uploaded": False}


# ─── Onboarding unificado (usado por onboarding.html) ────────────────────────

class OnboardingIntegranteRequest(BaseModel):
    nombre: str
    email: str = ""
    rol: str = ""
    fecha_nac: str = ""
    es_menor: bool = False
    pais: str = "argentina"


class OnboardingRequest(BaseModel):
    nombre_familia: str
    email_comprador: str
    integrantes: list[OnboardingIntegranteRequest]
    relaciones: list = []


def _recording_base() -> str:
    return os.environ.get(
        "BASE_URL",
        os.environ.get("CLOUD_RUN_URL", "https://familia-pipeline-776445604502.southamerica-east1.run.app"),
    )


@app.post("/onboarding", status_code=201)
@limiter.limit("5/hour")
async def onboarding(request: Request, req: OnboardingRequest, _: None = Depends(_admin_auth)):
    """
    Crea la familia e integrantes en Firestore y devuelve los tokens de grabación.
    familia_id siempre generado en servidor. Tokens generados en servidor vía add_integrante().
    Idempotente por nombre de integrante. Rate-limited: 5 req/IP/hora.
    """
    from google.cloud import firestore as _firestore
    from pipeline.utils import firestore as fs

    familia_id = uuid.uuid4().hex[:16]
    db = fs._db()

    # Upsert familia
    db.collection("familias").document(familia_id).set(
        {
            "nombre": req.nombre_familia,
            "comprador": {
                "email": req.email_comprador,
                "nombre": "",
                "es_tambien_retratado": False,
            },
            "estado": "onboarding",
            "pack": "base",
            "pais": req.integrantes[0].pais if req.integrantes else "argentina",
            "integrantes_extra": max(0, len(req.integrantes) - 4),
            "fecha_compra": _firestore.SERVER_TIMESTAMP,
            "fecha_entrega": None,
            "acepta_tyc": True,
            "acepta_tyc_timestamp": datetime.utcnow().isoformat(),
        },
        merge=True,
    )

    # Índice de integrantes existentes por nombre para idempotencia
    existentes = {
        i.get("nombre", "").lower(): i
        for i in fs.get_integrantes(familia_id)
    }

    base = _recording_base()
    tokens = []

    for ing in req.integrantes:
        existing = existentes.get(ing.nombre.lower())
        if existing:
            token = existing.get("token_unico", "")
        else:
            integrante_id, token = fs.add_integrante(
                familia_id=familia_id,
                nombre=ing.nombre,
                relacion_con_comprador=ing.rol,
                es_menor=ing.es_menor,
                fecha_nac=ing.fecha_nac,
            )
            # Campos extra que add_integrante no acepta
            db.collection("familias").document(familia_id) \
              .collection("integrantes").document(integrante_id) \
              .update({"email": ing.email, "pais": ing.pais})

        tokens.append({
            "nombre": ing.nombre,
            "link": f"{base}/r/{token}",
            "token": token,
        })

    # Token de onboarding (60 min, multi-uso) para foto-portada y tokens-estado pre-login
    from pipeline.utils import firestore as _fs_ot
    onboarding_token = uuid.uuid4().hex
    _fs_ot.create_temp_token(onboarding_token, familia_id, ttl_minutes=60)

    return {"familia_id": familia_id, "tokens": tokens, "onboarding_token": onboarding_token}


@app.post("/familia/{familia_id}/foto-portada")
async def foto_portada(
    familia_id: str,
    request: Request,
    file: UploadFile = File(...),
    ot: str | None = Query(default=None),
):
    """Sube la foto de portada del libro a GCS y guarda la URL en Firestore."""
    from pipeline.utils import firestore as fs, storage as st

    if ot:
        if not fs.validate_temp_token(ot, familia_id):
            raise HTTPException(status_code=401, detail="Token de onboarding inválido o expirado")
    else:
        cookie_value = request.cookies.get(_SESSION_COOKIE, "")
        session_familia_id = _verify_session(cookie_value) if cookie_value else None
        if not session_familia_id or session_familia_id != familia_id:
            raise HTTPException(status_code=401, detail="No autenticado")

    if not fs.get_familia(familia_id):
        raise HTTPException(status_code=404, detail=f"Familia no encontrada: {familia_id}")

    # Validar MIME type
    content_type = (file.content_type or "").split(";")[0].strip().lower()
    if content_type not in _ALLOWED_IMAGE_TYPES:
        raise HTTPException(
            status_code=415,
            detail=f"Tipo de archivo no permitido: {content_type!r}. Se aceptan: {', '.join(sorted(_ALLOWED_IMAGE_TYPES))}",
        )

    # Leer y validar tamaño
    file_bytes = await file.read()
    if len(file_bytes) > _MAX_IMAGE_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Imagen demasiado grande ({len(file_bytes) / 1024 / 1024:.1f} MB). Máximo permitido: 10 MB.",
        )

    filename = file.filename or "portada.jpg"
    ext = filename.rsplit(".", 1)[-1] if "." in filename else "jpg"
    blob_name = f"{familia_id}/portada.{ext}"

    with tempfile.NamedTemporaryFile(suffix=f".{ext}", delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name

    try:
        gs_url = st.upload_to_gcs(tmp_path, st.GCS_BUCKET_FOTOS, blob_name, file.content_type or "image/jpeg")
    finally:
        os.unlink(tmp_path)

    fs._db().collection("familias").document(familia_id).update({"foto_portada_url": gs_url})
    return {"ok": True, "foto_portada_url": gs_url}


@app.get("/familia/{familia_id}/tokens-estado")
def tokens_estado(
    familia_id: str,
    request: Request,
    ot: str | None = Query(default=None),
):
    """Devuelve estado actual de cada token. Requiere onboarding_token (ot) o sesión autenticada."""
    from pipeline.utils import firestore as fs

    if ot:
        if not fs.validate_temp_token(ot, familia_id):
            raise HTTPException(status_code=401, detail="Token de onboarding inválido o expirado")
    else:
        cookie_value = request.cookies.get(_SESSION_COOKIE, "")
        session_familia_id = _verify_session(cookie_value) if cookie_value else None
        if not session_familia_id or session_familia_id != familia_id:
            raise HTTPException(status_code=401, detail="No autenticado")

    if not fs.get_familia(familia_id):
        raise HTTPException(status_code=404, detail=f"Familia no encontrada: {familia_id}")

    integrantes = fs.get_integrantes(familia_id)
    base = _recording_base()

    tokens = []
    for i in integrantes:
        token = i.get("token_unico", "")
        tokens.append({
            "nombre": i.get("nombre", ""),
            "link": f"{base}/r/{token}" if token else "",
            "estado": i.get("estado", "pendiente"),
            "usado": i.get("ultimo_acceso") is not None,
            "email": i.get("email", ""),
            "token": token,
        })

    return {"tokens": tokens}


# ─── Onboarding: Familias ─────────────────────────────────────────────────────

class CompradorInfo(BaseModel):
    email: str
    nombre: str
    es_tambien_retratado: bool = False


class FamiliaRequest(BaseModel):
    nombre: str
    comprador: CompradorInfo
    pack: str = "base"
    pais: str = "argentina"


class IntegranteRequest(BaseModel):
    nombre: str
    relacion_con_comprador: str
    es_menor: bool = False
    fecha_nac: str = ""


@app.post("/familia", status_code=201)
def crear_familia(req: FamiliaRequest, _: None = Depends(_admin_auth)):
    from pipeline.utils import firestore as fs

    familia_id = fs.create_familia(
        nombre=req.nombre,
        comprador=req.comprador.model_dump(),
        pack=req.pack,
        pais=req.pais,
    )

    token_comprador = None
    if req.comprador.es_tambien_retratado:
        _, token_comprador = fs.add_integrante(
            familia_id=familia_id,
            nombre=req.comprador.nombre,
            relacion_con_comprador="comprador",
            es_comprador=True,
        )

    return {"familia_id": familia_id, "token_comprador": token_comprador}


@app.post("/familia/{familia_id}/integrantes", status_code=201)
def agregar_integrante(familia_id: str, req: IntegranteRequest, _: None = Depends(_admin_auth)):
    from pipeline.utils import firestore as fs

    if not fs.get_familia(familia_id):
        raise HTTPException(status_code=404, detail=f"Familia no encontrada: {familia_id}")

    integrante_id, token = fs.add_integrante(
        familia_id=familia_id,
        nombre=req.nombre,
        relacion_con_comprador=req.relacion_con_comprador,
        es_menor=req.es_menor,
        fecha_nac=req.fecha_nac,
    )
    return {"integrante_id": integrante_id, "token_unico": token}


@app.get("/familia/{familia_id}")
def get_familia_detail(familia_id: str, _: None = Depends(_admin_auth)):
    from pipeline.utils import firestore as fs

    familia = fs.get_familia(familia_id)
    if not familia:
        raise HTTPException(status_code=404, detail=f"Familia no encontrada: {familia_id}")

    integrantes = fs.get_integrantes_para_pipeline(familia_id)
    return {
        **familia,
        "integrantes": [
            {
                "id": p["id"],
                "nombre": p["nombre"],
                "relacion_con_comprador": p["relacion_con_comprador"],
                "es_comprador": p["es_comprador"],
                "es_menor": p["es_menor"],
                "estado": "pendiente",
                "porcentaje_avance": 0,
            }
            for p in integrantes
        ],
    }


# ─── Recepción de audio (recording.html / app móvil) ─────────────────────────

class AudioRequest(BaseModel):
    audio_base64: str
    mime_type: str = "audio/webm"


def _enviar_email_generando(familia_id: str, job_id: str) -> None:
    from pipeline.utils import firestore as fs
    from pipeline.utils.email import send_generando
    familia = fs.get_familia(familia_id) or {}
    comprador = familia.get('comprador', {})
    comprador_email = comprador.get('email', '')
    nombre_familia = familia.get('nombre', 'tu familia')
    logger.info('[email-generando] familia_id=%s job_id=%s email=%s', familia_id, job_id, comprador_email)
    if comprador_email:
        send_generando(email_comprador=comprador_email, nombre_familia=nombre_familia, familia_id=familia_id)


def _check_y_trigger(familia_id: str) -> None:
    """Auto-trigger del pipeline cuando todos los integrantes grabaron."""
    from pipeline.utils import firestore as fs
    from pipeline.utils.tasks import enqueue_pipeline

    integrantes = fs.get_integrantes(familia_id)
    pendientes = [i for i in integrantes if i.get("estado") != "completo"]
    if pendientes:
        return

    familia = fs.get_familia(familia_id) or {}
    nombres = [i.get("nombre", "") for i in integrantes if i.get("nombre")]
    job_id = str(uuid.uuid4())
    fs.create_job(job_id, familia_id=familia_id)
    enqueue_pipeline(
        job_id,
        {
            "nombres": nombres,
            "pais": familia.get("pais", "argentina"),
            "solo_desde": None,
            "familia": familia.get("nombre", ""),
            "upload_to_gcs": True,
            "familia_id": familia_id,
            "from_job_id": None,
        },
    )
    fs.update_familia_estado(familia_id, "generando")
    _enviar_email_generando(familia_id, job_id)
    logger.info("[auto-trigger] familia %s completa → job %s", familia_id, job_id)


@app.post("/audio/{token}")
def recibir_audio(token: str, req: AudioRequest):
    """
    Recibe el audio de un integrante (base64), lo sube a GCS,
    marca el integrante como completo y dispara el pipeline si todos grabaron.
    """
    from pipeline.utils import firestore as fs, storage as st

    match = fs.get_integrante_by_token(token)
    if match is None:
        raise HTTPException(status_code=404, detail="Token inválido o no encontrado")

    familia_id, integrante_id, _ = match

    # Validar MIME type
    mime = req.mime_type.split(";")[0].strip().lower()
    if mime not in _ALLOWED_AUDIO_TYPES:
        raise HTTPException(
            status_code=415,
            detail=f"Tipo de archivo no permitido: {mime!r}. Se aceptan: {', '.join(sorted(_ALLOWED_AUDIO_TYPES))}",
        )

    # Decodificar y validar tamaño
    audio_bytes = base64.b64decode(req.audio_base64)
    if len(audio_bytes) > _MAX_AUDIO_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Archivo demasiado grande ({len(audio_bytes) / 1024 / 1024:.1f} MB). Máximo permitido: 25 MB.",
        )

    ext = mime.split("/")[-1]
    blob_name = (
        f"{familia_id}/{integrante_id}_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.{ext}"
    )

    with tempfile.NamedTemporaryFile(suffix=f".{ext}", delete=False) as tmp:
        tmp.write(audio_bytes)
        tmp_path = tmp.name

    try:
        gcs_uri = st.upload_to_gcs(tmp_path, st.GCS_BUCKET_AUDIOS, blob_name, req.mime_type)
    finally:
        os.unlink(tmp_path)

    fs.update_integrante_estado(familia_id, integrante_id, "completo")
    _check_y_trigger(familia_id)

    return {"ok": True, "audio_url": gcs_uri}


# ─── Endpoints de grabación (token-based, usados por recording.html) ─────────

@app.get("/token/{token}/info")
def token_info(token: str):
    from pipeline.utils import firestore as fs
    match = fs.get_integrante_by_token(token)
    if match is None:
        raise HTTPException(status_code=404, detail="Token inválido o no encontrado")
    familia_id, integrante_id, data = match
    familia = fs.get_familia(familia_id)
    respuestas = fs.get_respuestas(familia_id, integrante_id)
    preguntas_grabadas = [r["id"] for r in respuestas if r.get("audio_url")]
    return {
        "nombre": data.get("nombre", ""),
        "nombre_familia": familia.get("nombre", "") if familia else "",
        "preguntas_grabadas": preguntas_grabadas,
    }


@app.post("/token/{token}/foto")
async def token_foto(token: str, foto: UploadFile = File(...)):
    from pipeline.utils import firestore as fs, storage as st
    match = fs.get_integrante_by_token(token)
    if match is None:
        raise HTTPException(status_code=404, detail="Token inválido o no encontrado")
    familia_id, integrante_id, _ = match

    # Validar MIME type
    content_type = (foto.content_type or "").split(";")[0].strip().lower()
    if content_type not in _ALLOWED_IMAGE_TYPES:
        raise HTTPException(
            status_code=415,
            detail=f"Tipo de archivo no permitido: {content_type!r}. Se aceptan: {', '.join(sorted(_ALLOWED_IMAGE_TYPES))}",
        )

    # Leer y validar tamaño
    foto_bytes = await foto.read()
    if len(foto_bytes) > _MAX_IMAGE_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Imagen demasiado grande ({len(foto_bytes) / 1024 / 1024:.1f} MB). Máximo permitido: 10 MB.",
        )

    filename = foto.filename or "foto.jpg"
    ext = filename.rsplit(".", 1)[-1] if "." in filename else "jpg"
    blob_name = f"{familia_id}/{integrante_id}/foto.{ext}"

    with tempfile.NamedTemporaryFile(suffix=f".{ext}", delete=False) as tmp:
        tmp.write(foto_bytes)
        tmp_path = tmp.name

    try:
        gs_url = st.upload_to_gcs(tmp_path, st.GCS_BUCKET_FOTOS, blob_name, foto.content_type or "image/jpeg")
    finally:
        os.unlink(tmp_path)

    fs.update_integrante_foto(familia_id, integrante_id, gs_url)
    return {"ok": True}


_ALLOWED_AUDIO_TYPES = {
    "audio/webm", "audio/mpeg", "audio/mp4",
    "audio/ogg", "audio/wav", "audio/x-m4a",
}
_MAX_AUDIO_BYTES = 25 * 1024 * 1024  # 25 MB

_ALLOWED_IMAGE_TYPES = {"image/jpeg", "image/png", "image/webp"}
_MAX_IMAGE_BYTES = 10 * 1024 * 1024  # 10 MB


@app.post("/token/{token}/respuesta")
async def token_respuesta(
    token: str,
    pregunta: str = Form(...),
    audio: UploadFile = File(...),
):
    from pipeline.utils import firestore as fs, storage as st
    match = fs.get_integrante_by_token(token)
    if match is None:
        raise HTTPException(status_code=404, detail="Token inválido o no encontrado")
    familia_id, integrante_id, _ = match

    # Fix 4a: validar MIME type
    content_type = (audio.content_type or "").split(";")[0].strip().lower()
    if content_type not in _ALLOWED_AUDIO_TYPES:
        raise HTTPException(
            status_code=415,
            detail=f"Tipo de archivo no permitido: {content_type!r}. Se aceptan: {', '.join(sorted(_ALLOWED_AUDIO_TYPES))}",
        )

    # Fix 4b: leer y validar tamaño
    audio_bytes = await audio.read()
    if len(audio_bytes) > _MAX_AUDIO_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Archivo demasiado grande ({len(audio_bytes) / 1024 / 1024:.1f} MB). Máximo permitido: 25 MB.",
        )

    filename = audio.filename or f"q{pregunta}.webm"
    ext = filename.rsplit(".", 1)[-1] if "." in filename else "webm"
    blob_name = f"{familia_id}/{integrante_id}/q{pregunta}.{ext}"

    with tempfile.NamedTemporaryFile(suffix=f".{ext}", delete=False) as tmp:
        tmp.write(audio_bytes)
        tmp_path = tmp.name

    try:
        gs_url = st.upload_to_gcs(tmp_path, st.GCS_BUCKET_AUDIOS, blob_name, audio.content_type or "audio/webm")
    finally:
        os.unlink(tmp_path)

    fs.save_respuesta(familia_id, integrante_id, pregunta, gs_url)
    respuestas_guardadas = fs.get_respuestas(familia_id, integrante_id)
    pct = round(len([r for r in respuestas_guardadas if r.get("audio_url")]) / 16 * 100)
    fs.update_porcentaje_avance(familia_id, integrante_id, pct)
    if pct >= 100:
        fs.update_integrante_estado(familia_id, integrante_id, "completo")
        _check_y_trigger(familia_id)
    else:
        fs.update_integrante_estado(familia_id, integrante_id, "en_progreso")
    return {"ok": True}


@app.patch("/token/{token}/nombre")
def token_update_nombre(token: str, nombre: str):
    """Permite al integrante corregir su nombre antes de grabar."""
    from pipeline.utils import firestore as fs
    match = fs.get_integrante_by_token(token)
    if match is None:
        raise HTTPException(status_code=404, detail="Token inválido o no encontrado")
    familia_id, integrante_id, _ = match
    nombre_clean = nombre.strip()
    if not nombre_clean or len(nombre_clean) > 100:
        raise HTTPException(status_code=400, detail="Nombre inválido")
    fs._db().collection("familias").document(familia_id) \
        .collection("integrantes").document(integrante_id) \
        .update({"nombre": nombre_clean})
    return {"ok": True, "nombre": nombre_clean}


@app.post("/token/{token}/consentimiento")
def token_consentimiento(token: str):
    """Registra el consentimiento de grabación del integrante en Firestore."""
    from pipeline.utils import firestore as fs
    match = fs.get_integrante_by_token(token)
    if match is None:
        raise HTTPException(status_code=404, detail="Token inválido o no encontrado")
    familia_id, integrante_id, _ = match
    fs._db().collection("familias").document(familia_id) \
        .collection("integrantes").document(integrante_id) \
        .update({
            "acepta_grabacion": True,
            "acepta_grabacion_timestamp": datetime.utcnow().isoformat(),
        })
    return {"ok": True}


@app.post("/token/{token}/completar")
def token_completar(token: str):
    from pipeline.utils import firestore as fs
    match = fs.get_integrante_by_token(token)
    if match is None:
        raise HTTPException(status_code=404, detail="Token inválido o no encontrado")
    familia_id, integrante_id, _ = match
    fs.update_integrante_estado(familia_id, integrante_id, "completo")
    _check_y_trigger(familia_id)
    return {"ok": True}


# ─── Webhooks de pago ────────────────────────────────────────────────────────

def _enviar_email_bienvenida(familia_id: str) -> None:
    """Send welcome email with per-member recording links. Idempotent via email_bienvenida_enviado flag."""
    from pipeline.utils import firestore as fs
    from pipeline.utils.email import send_bienvenida

    familia = fs.get_familia(familia_id)
    if not familia:
        logger.warning("[email-bienvenida] familia no encontrada: %s", familia_id)
        return

    if familia.get("email_bienvenida_enviado"):
        logger.info("[email-bienvenida] ya enviado para familia=%s, skipping", familia_id)
        return

    comprador = familia.get("comprador", {})
    email_comprador = comprador.get("email", "")
    nombre_familia = familia.get("nombre", "tu familia")

    if not email_comprador:
        logger.warning("[email-bienvenida] sin email para familia=%s", familia_id)
        return

    integrantes = fs.get_integrantes(familia_id)
    base = _recording_base()
    tokens = [
        {"nombre": i.get("nombre", ""), "url": f"{base}/r/{i.get('token_unico', '')}"}
        for i in integrantes
        if i.get("token_unico") and not i.get("es_menor")
    ]

    if not tokens:
        logger.warning("[email-bienvenida] sin tokens para familia=%s", familia_id)
        return

    try:
        send_bienvenida(email_comprador=email_comprador, nombre_familia=nombre_familia, tokens=tokens)
        fs._db().collection("familias").document(familia_id).update({"email_bienvenida_enviado": True})
        logger.info("[email-bienvenida] enviado a %s para familia=%s", email_comprador, familia_id)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[email-bienvenida] error para familia=%s: %s", familia_id, exc)


def _generar_access_token_familia(familia_id: str) -> None:
    """Generate and persist access_token (UUID4, 90 days) for a familia."""
    from pipeline.utils import firestore as fs

    if not fs.get_familia(familia_id):
        logger.warning("[webhook] familia no encontrada: %s", familia_id)
        return

    token = str(uuid.uuid4())
    expires_at = datetime.now(timezone.utc) + timedelta(days=90)
    fs.set_access_token(familia_id, token, expires_at)
    logger.info("[webhook] access_token generado para familia=%s expira=%s", familia_id, expires_at.date())
    _enviar_email_bienvenida(familia_id)


def _procesar_upsell_integrante(upsell_token: str) -> None:
    """Agrega el integrante al libro y envía email. Idempotente via flag procesado."""
    from pipeline.utils import firestore as fs
    from pipeline.utils.email import send_integrante_agregado

    checkout = fs.get_upsell_checkout(upsell_token)
    if not checkout:
        logger.warning("[upsell] checkout no encontrado: %s", upsell_token)
        return

    if checkout.get("procesado"):
        logger.info("[upsell] ya procesado, skipping idempotente: %s", upsell_token)
        return

    familia_id = checkout["familia_id"]
    nombre = checkout["nombre"]
    relacion = checkout["relacion"]

    familia = fs.get_familia(familia_id)
    if not familia:
        logger.warning("[upsell] familia no encontrada: %s", familia_id)
        return

    _, token_unico = fs.add_integrante(
        familia_id=familia_id,
        nombre=nombre,
        relacion_con_comprador=relacion,
    )

    fs.mark_upsell_checkout_procesado(upsell_token)
    logger.info("[upsell] integrante '%s' agregado a familia=%s token=%s", nombre, familia_id, token_unico)

    comprador = familia.get("comprador", {})
    email_comprador = comprador.get("email", "")
    nombre_familia = familia.get("nombre", "")
    base = _recording_base()
    token_url = f"{base}/r/{token_unico}"

    if email_comprador:
        try:
            send_integrante_agregado(
                email_comprador=email_comprador,
                nombre_familia=nombre_familia,
                nombre_integrante=nombre,
                token_url=token_url,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("[upsell] error enviando email: %s", exc)


@app.post("/webhook/stripe")
async def webhook_stripe(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")
    secret = os.environ.get("STRIPE_WEBHOOK_SECRET", "")

    if not secret:
        logger.error("[webhook-stripe] STRIPE_WEBHOOK_SECRET no configurado, rechazando webhook")
        raise HTTPException(status_code=500, detail="Webhook no configurado")

    if not _verify_stripe_signature(payload, sig_header, secret):
        raise HTTPException(status_code=400, detail="Firma de webhook inválida")

    try:
        event = json.loads(payload)
    except Exception:
        raise HTTPException(status_code=400, detail="JSON inválido")

    if event.get("type") == "checkout.session.completed":
        session = event.get("data", {}).get("object", {})
        metadata = session.get("metadata", {})
        if metadata.get("tipo") == "upsell_integrante":
            upsell_token = metadata.get("upsell_token", "")
            if upsell_token:
                _procesar_upsell_integrante(upsell_token)
        else:
            familia_id = (
                metadata.get("familia_id")
                or session.get("client_reference_id")
            )
            if familia_id:
                _generar_access_token_familia(familia_id)

    return {"ok": True}


def _handle_mp_payment(payment_id: str) -> None:
    """Fetch payment from MercadoPago API and generate access_token if approved."""
    mp_token = os.environ.get("MP_ACCESS_TOKEN", "")
    if not mp_token:
        logger.warning("[webhook-mp] MP_ACCESS_TOKEN no configurado")
        return

    try:
        resp = httpx.get(
            f"https://api.mercadopago.com/v1/payments/{payment_id}",
            headers={"Authorization": f"Bearer {mp_token}"},
            timeout=10,
        )
        if not resp.is_success:
            logger.warning("[webhook-mp] error al obtener pago %s: %s", payment_id, resp.status_code)
            return

        payment = resp.json()
        if payment.get("status") == "approved":
            external_reference = payment.get("external_reference", "")
            if external_reference.startswith("upsell_integrante:"):
                upsell_token = external_reference.split(":", 1)[1]
                _procesar_upsell_integrante(upsell_token)
            elif external_reference:
                _generar_access_token_familia(external_reference)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[webhook-mp] excepción procesando pago %s: %s", payment_id, exc)


@app.post("/webhook/mercadopago")
async def webhook_mercadopago(request: Request):
    mp_secret = os.environ.get("MP_WEBHOOK_SECRET", "")
    if not mp_secret:
        logger.warning("[webhook-mp] MP_WEBHOOK_SECRET no configurado, rechazando webhook")
        raise HTTPException(status_code=401, detail="Webhook no configurado")

    x_sig = request.headers.get("x-signature", "")
    x_request_id = request.headers.get("x-request-id", "")
    ts = ""
    v1 = ""
    for part in x_sig.split(","):
        if "=" not in part:
            continue
        k, val = part.split("=", 1)
        if k.strip() == "ts":
            ts = val.strip()
        elif k.strip() == "v1":
            v1 = val.strip()

    topic = request.query_params.get("topic", "")
    payment_id_query = request.query_params.get("id", "")

    try:
        body = await request.json()
    except Exception:
        body = {}

    data_id = str(body.get("data", {}).get("id") or payment_id_query or "")

    if not _verify_mp_signature(data_id, x_request_id, ts, v1, mp_secret):
        logger.warning("[webhook-mp] firma inválida, rechazando")
        raise HTTPException(status_code=401, detail="Firma de webhook inválida")

    event_type = body.get("type", "") or topic
    payment_id = body.get("data", {}).get("id") or (payment_id_query if topic == "payment" else "")

    if event_type == "payment" and payment_id:
        _handle_mp_payment(str(payment_id))

    return {"ok": True}


# ─── Webhook Hotmart ──────────────────────────────────────────────────────────

_HOTMART_PACK_MAP: dict[str, str] = {
    "recuerdo": "recuerdo",
    "legado": "legado",
    "bios": "bios",
}


def _hotmart_pack(product_name: str) -> str:
    lower = product_name.lower()
    for key, pack in _HOTMART_PACK_MAP.items():
        if key in lower:
            return pack
    return "familiar"


def _crear_familia_hotmart(email: str, nombre: str, pack: str, transaction: str) -> str:
    from google.cloud import firestore as _firestore
    from pipeline.utils import firestore as fs

    familia_id = uuid.uuid4().hex[:16]
    nombre_familia = f"Familia {nombre.split()[0]}" if nombre else "Mi Familia"
    fs._db().collection("familias").document(familia_id).set({
        "nombre": nombre_familia,
        "comprador": {
            "email": email,
            "nombre": nombre,
            "es_tambien_retratado": False,
        },
        "estado": "onboarding",
        "pack": pack,
        "pais": "argentina",
        "fecha_compra": _firestore.SERVER_TIMESTAMP,
        "fecha_entrega": None,
        "origen": "hotmart",
        "hotmart_transaction": transaction,
    })
    fs.add_integrante(
        familia_id=familia_id,
        nombre=nombre or email,
        relacion_con_comprador="comprador",
        es_comprador=True,
    )
    _generar_access_token_familia(familia_id)
    logger.info("[webhook-hotmart] familia=%s email=%s pack=%s transaction=%s", familia_id, email, pack, transaction)
    return familia_id


@app.post("/webhook/hotmart")
async def webhook_hotmart(request: Request):
    hottok = request.headers.get("x-hotmart-hottok", "")
    expected = os.environ.get("HOTMART_HOTTOK", "")
    if not expected or hottok != expected:
        logger.warning("[webhook-hotmart] hottok inválido, rechazando")
        raise HTTPException(status_code=401, detail="No autorizado")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON inválido")

    event = body.get("event", "")
    if event != "PURCHASE_APPROVED":
        logger.info("[webhook-hotmart] evento ignorado: %s", event)
        return {"ok": True}

    data = body.get("data", {})
    buyer = data.get("buyer", {})
    product = data.get("product", {})

    email = (buyer.get("email") or "").strip().lower()
    nombre = (buyer.get("name") or "").strip()
    product_name = product.get("name", "")
    transaction = data.get("purchase", {}).get("transaction", "")

    if not email:
        logger.warning("[webhook-hotmart] PURCHASE_APPROVED sin email")
        raise HTTPException(status_code=422, detail="Sin email de comprador")

    pack = _hotmart_pack(product_name)
    familia_id = _crear_familia_hotmart(email, nombre, pack, transaction)
    return {"ok": True, "familia_id": familia_id}


# ─── Auth: magic link ─────────────────────────────────────────────────────────

@app.get("/auth/{token}")
def auth_magic_link(token: str):
    """Validate access_token, set session cookie, redirect to /mi-familia."""
    from pipeline.utils import firestore as fs

    result = fs.get_familia_by_access_token(token)
    if result is None:
        raise HTTPException(status_code=404, detail="Link inválido o expirado")

    familia_id, _ = result
    signed = _sign_session(familia_id)

    response = RedirectResponse(url="/mi-familia", status_code=303)
    response.set_cookie(
        key=_SESSION_COOKIE,
        value=signed,
        httponly=True,
        secure=True,
        samesite="lax",
        max_age=_SESSION_MAX_AGE,
    )
    return response


class RequestLinkBody(BaseModel):
    email: str


_MAGIC_LINK_MAX_REQUESTS = 3
_MAGIC_LINK_WINDOW_SECONDS = 3600


@app.post("/auth/request-link")
@limiter.limit("20/hour")  # coarse IP-level guard
async def request_magic_link(request: Request, body: RequestLinkBody):
    """
    Send a magic link to the given email.
    Rate-limited to 3 requests/hour per email.
    Returns the same response regardless of whether the email exists.
    """
    from pipeline.utils import firestore as fs
    from pipeline.utils.email import send_magic_link

    email = body.email.strip().lower()
    _GENERIC_OK = {"ok": True, "message": "Si el email está registrado, recibirás el link en breve."}

    # Per-email rate limit (stored in Firestore)
    email_key = hashlib.sha256(email.encode()).hexdigest()[:32]
    if not fs.check_and_record_rate_limit(email_key, _MAGIC_LINK_MAX_REQUESTS, _MAGIC_LINK_WINDOW_SECONDS):
        return JSONResponse(content=_GENERIC_OK)

    try:
        result = fs.get_familia_by_email(email)
        if result:
            familia_id, familia = result
            token = fs.get_access_token(familia_id)
            if token:
                base = _recording_base()
                magic_link = f"{base}/auth/{token}"
                nombre_familia = familia.get("nombre", "tu familia")
                send_magic_link(email, nombre_familia, magic_link)
    except Exception as exc:  # noqa: BLE001
        logger.warning("[request-link] error para %s: %s", email, exc)

    return _GENERIC_OK


# ─── Session: GET /me ─────────────────────────────────────────────────────────

@app.get("/me")
def get_me(request: Request):
    """Return familia data for the currently authenticated session."""
    from pipeline.utils import firestore as fs, storage as st

    cookie_value = request.cookies.get(_SESSION_COOKIE, "")
    if not cookie_value:
        raise HTTPException(status_code=401, detail="No autenticado")

    familia_id = _verify_session(cookie_value)
    if not familia_id:
        raise HTTPException(status_code=401, detail="Sesión inválida o expirada")

    familia = fs.get_familia(familia_id)
    if not familia:
        raise HTTPException(status_code=404, detail="Familia no encontrada")

    integrantes = fs.get_integrantes(familia_id)
    base = _recording_base()
    tokens_estado = [
        {
            "nombre": i.get("nombre", ""),
            "estado": i.get("estado", "pendiente"),
            "link": f"{base}/r/{i.get('token_unico', '')}" if i.get("token_unico") else "",
            "email": i.get("email", ""),
            "token": i.get("token_unico", ""),
        }
        for i in integrantes
    ]

    # Derive book production status
    total = len(integrantes)
    completados = sum(1 for i in integrantes if i.get("estado") == "completo")
    familia_estado = familia.get("estado", "")
    if familia_estado in ("entregado",):
        libro_status = "listo"
    elif familia_estado in ("generando",) or (total > 0 and completados == total):
        libro_status = "produccion"
    else:
        libro_status = "esperando"

    # Signed URL for cover photo (24h)
    foto_portada_url = None
    gs_foto = familia.get("foto_portada_url", "")
    if gs_foto and gs_foto.startswith("gs://"):
        try:
            foto_portada_url = st.get_signed_url(gs_foto, expiration_hours=24)
        except Exception:
            pass

    # PDF signed URL if book is done (7 days)
    pdf_url = None
    if libro_status == "listo":
        gs_libro = familia.get("libro_url", "")
        if gs_libro and gs_libro.startswith("gs://"):
            try:
                pdf_url = st.get_signed_url(gs_libro, expiration_hours=168)
            except Exception:
                pass

    return {
        "familia_id": familia_id,
        "nombre_familia": familia.get("nombre", ""),
        "comprador": familia.get("comprador", {}),
        "tokens": tokens_estado,
        "estado": familia_estado,
        "libro_status": libro_status,
        "foto_portada_url": foto_portada_url,
        "pdf_url": pdf_url,
    }


# ─── Familia: link de acceso (usado por /gracias) ────────────────────────────

@app.get("/familia/{familia_id}/link-acceso")
def familia_link_acceso(
    familia_id: str,
    request: Request,
    dt: str | None = Query(default=None),
):
    """
    Retorna el link de acceso. Acepta dos mecanismos de auth:
    - dt (display_token): token de un solo uso generado al checkout (30 min). Para gracias.html.
    - Cookie de sesión: para usuarios ya autenticados.
    """
    from pipeline.utils import firestore as fs

    if dt:
        # validate sin consumir: el poll puede repetirse hasta que el webhook genere el access_token
        if not fs.validate_temp_token(dt, familia_id):
            raise HTTPException(status_code=401, detail="Token de acceso inválido o expirado")
    else:
        cookie_value = request.cookies.get(_SESSION_COOKIE, "")
        if not cookie_value:
            raise HTTPException(status_code=401, detail="No autenticado")
        session_familia_id = _verify_session(cookie_value)
        if not session_familia_id or session_familia_id != familia_id:
            raise HTTPException(status_code=403, detail="No autorizado")

    token = fs.get_access_token(familia_id)
    if token is None:
        return {"disponible": False, "link": None}

    base = _recording_base()
    return {"disponible": True, "link": f"{base}/auth/{token}"}


# ─── Reenviar link de grabación ──────────────────────────────────────────────

class ReenviarInvitacionBody(BaseModel):
    token: str


@app.post("/familia/{familia_id}/reenviar-invitacion")
@limiter.limit("20/hour")
async def reenviar_invitacion(familia_id: str, request: Request, body: ReenviarInvitacionBody):
    """Reenvía el link de grabación por email al integrante identificado por su token."""
    from pipeline.utils import firestore as fs
    from pipeline.utils.email import send_recordatorio

    cookie_value = request.cookies.get(_SESSION_COOKIE, "")
    if not cookie_value:
        raise HTTPException(status_code=401, detail="No autenticado")

    session_familia_id = _verify_session(cookie_value)
    if not session_familia_id or session_familia_id != familia_id:
        raise HTTPException(status_code=403, detail="No autorizado")

    familia = fs.get_familia(familia_id)
    if not familia:
        raise HTTPException(status_code=404, detail="Familia no encontrada")

    match = fs.get_integrante_by_token(body.token)
    if match is None:
        raise HTTPException(status_code=404, detail="Token inválido")

    match_familia_id, _, integrante_data = match
    if match_familia_id != familia_id:
        raise HTTPException(status_code=403, detail="Token no pertenece a esta familia")

    email = integrante_data.get("email", "")
    if not email:
        raise HTTPException(status_code=422, detail="Este integrante no tiene email registrado")

    nombre = integrante_data.get("nombre", "")
    nombre_familia = familia.get("nombre", "")
    base = _recording_base()
    token_url = f"{base}/r/{body.token}"

    send_recordatorio(
        email_integrante=email,
        nombre_integrante=nombre,
        nombre_familia=nombre_familia,
        token_url=token_url,
    )

    return {"ok": True}


# ─── Panel familia: progreso por integrante ──────────────────────────────────

@app.get("/familia/{familia_id}/progreso")
def familia_progreso(familia_id: str, request: Request):
    """
    Devuelve el progreso de cada integrante de la familia.
    Requiere cookie de sesión válida correspondiente a esta familia.
    """
    from pipeline.utils import firestore as fs

    cookie_value = request.cookies.get(_SESSION_COOKIE, "")
    if not cookie_value:
        raise HTTPException(status_code=401, detail="No autenticado")

    session_familia_id = _verify_session(cookie_value)
    if not session_familia_id:
        raise HTTPException(status_code=401, detail="Sesión inválida o expirada")

    if session_familia_id != familia_id:
        raise HTTPException(status_code=403, detail="No autorizado")

    familia = fs.get_familia(familia_id)
    if not familia:
        raise HTTPException(status_code=404, detail="Familia no encontrada")

    integrantes = fs.get_integrantes(familia_id)
    integrantes_progreso = []
    for i in integrantes:
        integrante_id = i.get("id", "")
        estado = i.get("estado", "pendiente")
        preguntas_respondidas = len(fs.get_respuestas(familia_id, integrante_id))
        integrantes_progreso.append({
            "nombre": i.get("nombre", ""),
            "preguntas_respondidas": preguntas_respondidas,
            "preguntas_total": 16,
            "estado": estado,
        })

    return {
        "familia_id": familia_id,
        "nombre_familia": familia.get("nombre", ""),
        "integrantes": integrantes_progreso,
    }


# ─── Admin ────────────────────────────────────────────────────────────────────

# ─── Endpoints de pago ───────────────────────────────────────────────────────

class CheckoutRequest(BaseModel):
    nombre_familia: str
    email_comprador: str
    nombre_comprador: str = ''
    integrantes: list[dict]
    pack: str = 'familiar'

_PRECIOS = {'esencial': 59, 'familiar': 79, 'extendido': 129}
_MAX_INTEG = {'esencial': 2, 'familiar': 4, 'extendido': 8}
_PRECIO_UPSELL = 8  # USD por integrante extra

def _calcular_total(pack: str, n_integrantes: int) -> int:
    base = _PRECIOS.get(pack, 79)
    max_inc = _MAX_INTEG.get(pack, 4)
    extra = max(0, n_integrantes - max_inc)
    return base + extra * 8

def _crear_familia_checkout(req: CheckoutRequest) -> tuple[str, str]:
    """Crea familia en Firestore para el checkout. Retorna (familia_id, display_token)."""
    from pipeline.utils import firestore as fs
    from google.cloud import firestore as _firestore
    familia_id = uuid.uuid4().hex[:16]
    display_token = uuid.uuid4().hex
    db = fs._db()
    db.collection('familias').document(familia_id).set({
        'nombre': req.nombre_familia,
        'comprador': {'email': req.email_comprador, 'nombre': req.nombre_comprador, 'es_tambien_retratado': False},
        'estado': 'checkout',
        'pack': req.pack,
        'pais': 'argentina',
        'fecha_compra': _firestore.SERVER_TIMESTAMP,
        'acepta_tyc': True,
        'acepta_tyc_timestamp': datetime.utcnow().isoformat(),
    })
    for ing in req.integrantes:
        fs.add_integrante(familia_id=familia_id, nombre=ing.get('nombre', ''), relacion_con_comprador='integrante')
    # Token de un solo uso (30 min) para que gracias.html pueda mostrar el link de acceso
    fs.create_temp_token(display_token, familia_id, ttl_minutes=30)
    return familia_id, display_token

@app.post('/pago/crear-checkout')
@limiter.limit('10/hour')
async def crear_checkout_mp(request: Request, req: CheckoutRequest):
    raise HTTPException(status_code=503, detail='Canal de pago no disponible. Comprá en Hotmart.')
    mp_token = os.environ.get('MP_ACCESS_TOKEN', '')
    if not mp_token or mp_token == 'placeholder_mp':
        raise HTTPException(status_code=503, detail='MercadoPago no configurado')
    familia_id, display_token = _crear_familia_checkout(req)
    total = _calcular_total(req.pack, len(req.integrantes))
    base = _recording_base()
    payload = {
        'items': [{'title': f'Ethos Bios — {req.nombre_familia}', 'quantity': 1, 'unit_price': total, 'currency_id': 'USD'}],
        'payer': {'email': req.email_comprador},
        'external_reference': familia_id,
        'back_urls': {'success': f'{base}/gracias?familia_id={familia_id}&dt={display_token}', 'failure': f'{base}/gracias?familia_id={familia_id}&error=1', 'pending': f'{base}/gracias?familia_id={familia_id}&pending=1'},
        'auto_return': 'approved',
        'notification_url': f'{base}/webhook/mercadopago',
    }
    try:
        resp = httpx.post('https://api.mercadopago.com/checkout/preferences', headers={'Authorization': f'Bearer {mp_token}', 'Content-Type': 'application/json'}, json=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return {'familia_id': familia_id, 'init_point': data.get('init_point'), 'sandbox_init_point': data.get('sandbox_init_point')}
    except Exception as exc:
        logger.error('[mp-checkout] error: %s', exc)
        raise HTTPException(status_code=502, detail='Error al crear preferencia MP')

class UpsellIntegranteRequest(BaseModel):
    nombre: str
    relacion_con_comprador: str = 'integrante'


def _session_auth_familia(request: Request, familia_id: str):
    """Verifica sesión y que corresponda al familia_id del path. Retorna familia o lanza HTTP error."""
    from pipeline.utils import firestore as fs
    cookie_value = request.cookies.get(_SESSION_COOKIE, "")
    if not cookie_value:
        raise HTTPException(status_code=401, detail="No autenticado")
    session_familia_id = _verify_session(cookie_value)
    if not session_familia_id or session_familia_id != familia_id:
        raise HTTPException(status_code=403, detail="No autorizado")
    familia = fs.get_familia(familia_id)
    if not familia:
        raise HTTPException(status_code=404, detail="Familia no encontrada")
    return familia


@app.post('/familia/{familia_id}/upsell-integrante/crear-checkout')
@limiter.limit('10/hour')
async def upsell_crear_checkout_mp(familia_id: str, request: Request, req: UpsellIntegranteRequest):
    """Crea preferencia MP para agregar un integrante extra (USD 8). Requiere sesión del comprador."""
    raise HTTPException(status_code=503, detail='Canal de pago no disponible.')
    from pipeline.utils import firestore as fs
    familia = _session_auth_familia(request, familia_id)
    mp_token = os.environ.get('MP_ACCESS_TOKEN', '')
    if not mp_token or mp_token == 'placeholder_mp':
        raise HTTPException(status_code=503, detail='MercadoPago no configurado')

    upsell_token = uuid.uuid4().hex
    fs.create_upsell_checkout(upsell_token, familia_id, req.nombre, req.relacion_con_comprador)

    base = _recording_base()
    nombre_familia = familia.get('nombre', '')
    email_comprador = familia.get('comprador', {}).get('email', '')
    import urllib.parse
    nombre_enc = urllib.parse.quote(req.nombre)
    payload = {
        'items': [{'title': f'Ethos Bios — {nombre_familia} (+1 integrante: {req.nombre})', 'quantity': 1, 'unit_price': _PRECIO_UPSELL, 'currency_id': 'USD'}],
        'payer': {'email': email_comprador},
        'external_reference': f'upsell_integrante:{upsell_token}',
        'back_urls': {
            'success': f'{base}/mi-familia?upsell=ok&nombre={nombre_enc}',
            'failure': f'{base}/mi-familia?upsell=error',
            'pending': f'{base}/mi-familia?upsell=pendiente',
        },
        'auto_return': 'approved',
        'notification_url': f'{base}/webhook/mercadopago',
    }
    try:
        resp = httpx.post('https://api.mercadopago.com/checkout/preferences', headers={'Authorization': f'Bearer {mp_token}', 'Content-Type': 'application/json'}, json=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return {'checkout_url': data.get('init_point'), 'sandbox_checkout_url': data.get('sandbox_init_point')}
    except Exception as exc:
        logger.error('[upsell-mp] error: %s', exc)
        raise HTTPException(status_code=502, detail='Error al crear preferencia MP')


@app.post('/familia/{familia_id}/upsell-integrante/crear-stripe-checkout')
@limiter.limit('10/hour')
async def upsell_crear_checkout_stripe(familia_id: str, request: Request, req: UpsellIntegranteRequest):
    """Crea sesión Stripe para agregar un integrante extra (USD 8). Requiere sesión del comprador."""
    raise HTTPException(status_code=503, detail='Canal de pago no disponible.')
    from pipeline.utils import firestore as fs
    familia = _session_auth_familia(request, familia_id)
    stripe_key = os.environ.get('STRIPE_SECRET_KEY', '')
    if not stripe_key or stripe_key == 'placeholder_stripe':
        raise HTTPException(status_code=503, detail='Stripe no configurado')

    upsell_token = uuid.uuid4().hex
    fs.create_upsell_checkout(upsell_token, familia_id, req.nombre, req.relacion_con_comprador)

    base = _recording_base()
    nombre_familia = familia.get('nombre', '')
    email_comprador = familia.get('comprador', {}).get('email', '')
    import urllib.parse
    nombre_enc = urllib.parse.quote(req.nombre)
    payload = {
        'payment_method_types[]': 'card',
        'line_items[0][price_data][currency]': 'usd',
        'line_items[0][price_data][product_data][name]': f'Ethos Bios — {nombre_familia} (+1 integrante: {req.nombre})',
        'line_items[0][price_data][unit_amount]': str(_PRECIO_UPSELL * 100),
        'line_items[0][quantity]': '1',
        'mode': 'payment',
        'customer_email': email_comprador,
        'success_url': f'{base}/mi-familia?upsell=ok&nombre={nombre_enc}',
        'cancel_url': f'{base}/mi-familia',
        'metadata[tipo]': 'upsell_integrante',
        'metadata[upsell_token]': upsell_token,
        'metadata[familia_id]': familia_id,
    }
    try:
        resp = httpx.post('https://api.stripe.com/v1/checkout/sessions', headers={'Authorization': f'Bearer {stripe_key}'}, data=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return {'checkout_url': data.get('url')}
    except Exception as exc:
        logger.error('[upsell-stripe] error: %s', exc)
        raise HTTPException(status_code=502, detail='Error al crear sesión Stripe')


@app.post('/pago/crear-stripe-checkout')
@limiter.limit('10/hour')
async def crear_checkout_stripe(request: Request, req: CheckoutRequest):
    raise HTTPException(status_code=503, detail='Canal de pago no disponible. Comprá en Hotmart.')
    stripe_key = os.environ.get('STRIPE_SECRET_KEY', '')
    if not stripe_key or stripe_key == 'placeholder_stripe':
        raise HTTPException(status_code=503, detail='Stripe no configurado')
    familia_id, display_token = _crear_familia_checkout(req)
    total = _calcular_total(req.pack, len(req.integrantes))
    base = _recording_base()
    payload = {
        'payment_method_types[]': 'card',
        'line_items[0][price_data][currency]': 'usd',
        'line_items[0][price_data][product_data][name]': f'Ethos Bios — {req.nombre_familia}',
        'line_items[0][price_data][unit_amount]': total * 100,
        'line_items[0][quantity]': '1',
        'mode': 'payment',
        'client_reference_id': familia_id,
        'customer_email': req.email_comprador,
        'success_url': f'{base}/gracias?familia_id={familia_id}&dt={display_token}',
        'cancel_url': f'{base}/#precios',
        'metadata[familia_id]': familia_id,
    }
    try:
        resp = httpx.post('https://api.stripe.com/v1/checkout/sessions', headers={'Authorization': f'Bearer {stripe_key}'}, data=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return {'familia_id': familia_id, 'checkout_url': data.get('url')}
    except Exception as exc:
        logger.error('[stripe-checkout] error: %s', exc)
        raise HTTPException(status_code=502, detail='Error al crear sesión Stripe')


# ─── Admin ────────────────────────────────────────────────────────────────────

@app.post("/admin/test-bienvenida")
def test_bienvenida(familia_id: str, email: str | None = None, _: None = Depends(_admin_auth)):
    """Test send_bienvenida() with real Firestore data. Does NOT set email_bienvenida_enviado flag."""
    from pipeline.utils import firestore as fs
    from pipeline.utils.email import send_bienvenida

    familia = fs.get_familia(familia_id)
    if not familia:
        raise HTTPException(status_code=404, detail=f"Familia no encontrada: {familia_id}")

    comprador = familia.get("comprador", {})
    email_dest = email or comprador.get("email", "")
    nombre_familia = familia.get("nombre", "tu familia")

    integrantes = fs.get_integrantes(familia_id)
    base = _recording_base()
    tokens = [
        {"nombre": i.get("nombre", ""), "url": f"{base}/r/{i.get('token_unico', '')}"}
        for i in integrantes
        if i.get("token_unico") and not i.get("es_menor")
    ]

    send_bienvenida(email_comprador=email_dest, nombre_familia=nombre_familia, tokens=tokens)
    return {"ok": True, "email": email_dest, "tokens_count": len(tokens), "base_url": base}


@app.post("/admin/test-email-libro")
def test_email_libro(email: str = "fede.giacomozzi@gmail.com", _: None = Depends(_admin_auth)):
    from pipeline.utils.email import send_libro_listo
    send_libro_listo(email_comprador=email, nombre_familia="Familia García", signed_url="https://ethosbios.com")
    return {"ok": True, "message": "Email libro listo enviado"}


@app.post("/admin/reenviar-libro")
def admin_reenviar_libro(
    familia_id: str,
    email_override: str | None = None,
    _: None = Depends(_admin_auth),
):
    """Reenvía el email de entrega del libro a la familia. Usa email_override si se especifica."""
    from pipeline.utils import firestore as fs, storage as st
    from pipeline.utils.email import send_libro_listo

    familia = fs.get_familia(familia_id)
    if not familia:
        raise HTTPException(status_code=404, detail=f"Familia no encontrada: {familia_id}")

    libro_url = familia.get("libro_url", "")
    if not libro_url:
        raise HTTPException(status_code=404, detail="No hay libro_url guardada para esta familia")

    comprador = familia.get("comprador", {})
    email_dest = email_override or comprador.get("email", "")
    if not email_dest:
        raise HTTPException(status_code=422, detail="Sin email de destino")

    nombre_familia = familia.get("nombre", "tu familia")

    # Generar signed URL si es gs://, usar directamente si es https://
    if libro_url.startswith("gs://"):
        signed = st.get_signed_url(libro_url, expiration_hours=168)
    else:
        signed = libro_url

    send_libro_listo(email_comprador=email_dest, nombre_familia=nombre_familia, signed_url=signed)
    logger.info("[admin-reenviar-libro] email enviado a %s familia=%s url=%s", email_dest, familia_id, libro_url)
    return {"ok": True, "email": email_dest, "libro_url": libro_url}


@app.post("/admin/test-email")
def test_email(email: str = "test@raices.app", _: None = Depends(_admin_auth)):
    from pipeline.utils.email import send_bienvenida

    base = _recording_base()
    send_bienvenida(
        email_comprador=email,
        nombre_familia="Familia García",
        tokens=[
            {"nombre": "Abuela Rosa", "url": f"{base}/r/test-token-abc123"},
            {"nombre": "Tío Carlos", "url": f"{base}/r/test-token-def456"},
        ],
    )
    return {"ok": True, "message": "Email de prueba enviado", "base_url": base}
