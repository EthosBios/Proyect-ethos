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
from fastapi.templating import Jinja2Templates
from itsdangerous import BadSignature, URLSafeTimedSerializer
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

_STATIC_DIR = Path(__file__).parent / "static"
_TEMPLATES_DIR = Path(__file__).parent / "templates"
_templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))

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
    # Idempotencia a nivel job: si el job ya se completó (típicamente un reintento
    # de Cloud Tasks tras un intento previo que no ackeó a tiempo), no reprocesar el
    # pipeline completo (voice→chapters→quality→editor→layout) ni reenviar el email.
    existing = fs.get_job(job_id)
    if existing and existing.get("status") == "done":
        logger.info("[pipeline-job] job=%s ya está done, skip idempotente", job_id)
        return
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
                    if familia.get('entregado_at'):
                        logger.info('[email-libro-listo] ya enviado (entregado_at) familia=%s, skip', familia_id_job)
                    else:
                        signed = st.get_signed_url(layout_url, expiration_hours=168)
                        send_libro_listo(email_comprador=comprador_email, nombre_familia=nombre_familia, signed_url=signed)
                        _fs_email.set_entregado_at(familia_id_job)
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


# ─── Página de reproducción de voz permanente ────────────────────────────────

@app.get("/voz/{voz_token}")
def voz_permanente(voz_token: str):
    """Página pública (noindex) que reproduce el mensaje de voz de un integrante."""
    from pipeline.utils import firestore as fs, storage as st

    match = fs.get_integrante_by_voz_token(voz_token)
    if match is None:
        raise HTTPException(status_code=404, detail="Link inválido o no encontrado")

    familia_id, integrante_id, data = match
    familia = fs.get_familia(familia_id) or {}
    nombre = data.get("nombre", "")
    nombre_familia = familia.get("nombre", "")
    nombre_pila = nombre.split()[0] if nombre else nombre
    audio_gs_url = data.get("_audio_gs_url", "") or data.get("voz_audio_url", "")
    pais = data.get("pais", "")
    pronombre = "vos" if _es_voseo(pais) else "ti"
    copy_voz = (
        f"Lo que vas a escuchar es la voz de {nombre_pila}, guardada para que nunca se pierda "
        f"— para {pronombre}, y para quienes lleguen después en tu familia."
    )

    audio_url = ""
    if audio_gs_url and audio_gs_url.startswith("gs://"):
        try:
            audio_url = st.get_signed_url(audio_gs_url, expiration_hours=2)
        except Exception as exc:
            logger.warning("[voz] no se pudo generar signed URL: %s", exc)

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex,nofollow">
<title>{nombre} — su voz, para siempre · Ethos Bios</title>
<link href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:ital,wght@0,400;0,600;1,400&family=DM+Sans:wght@300;400;500&display=swap" rel="stylesheet">
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'DM Sans',sans-serif;background:#0F0A06;min-height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:40px 20px;color:#F0E8DC}}
.logo{{font-family:'Cormorant Garamond',serif;font-size:14px;font-weight:400;letter-spacing:2px;color:#B8924A;text-transform:uppercase;margin-bottom:48px;text-align:center}}
.card{{background:#1C1208;border:1px solid #3d2b1a;border-radius:16px;padding:48px 40px;max-width:480px;width:100%;text-align:center}}
.nombre{{font-family:'Cormorant Garamond',serif;font-size:36px;font-style:italic;color:#F0E8DC;margin-bottom:6px;line-height:1.2}}
.familia{{font-size:13px;color:#B8924A;letter-spacing:1px;text-transform:uppercase;margin-bottom:32px}}
.sep{{width:40px;height:1px;background:#B8924A;margin:0 auto 32px;opacity:0.5}}
audio{{width:100%;border-radius:99px;accent-color:#B8924A;margin-bottom:32px}}
.copy-voz{{font-family:'Cormorant Garamond',serif;font-style:italic;font-size:16px;color:#B8924A;line-height:1.65;margin-bottom:28px}}
.firma{{font-family:'Cormorant Garamond',serif;font-style:italic;font-size:15px;color:#6B3D1E;line-height:1.6}}
@media(max-width:480px){{.card{{padding:36px 24px}}.nombre{{font-size:28px}}}}
</style>
</head>
<body>
<div class="logo">Ethos Bios</div>
<div class="card">
  <h1 class="nombre">{nombre}</h1>
  <p class="familia">{nombre_familia}</p>
  <div class="sep"></div>
  <p class="copy-voz">{copy_voz}</p>
  {'<audio controls preload="auto" src="' + audio_url + '"></audio>' if audio_url else '<p style="color:#6B3D1E;font-size:14px;margin-bottom:32px">El audio no está disponible temporalmente.</p>'}
  <p class="firma">Su voz, guardada para siempre.</p>
</div>
</body>
</html>"""

    from fastapi.responses import HTMLResponse
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})


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
    familia_id: str | None = None  # si viene, actualiza doc existente (Hotmart); si no, crea nuevo
    buyer_token: str | None = None  # token de comprador (2h, Hotmart flow); alternativa a X-Admin-Key
    integrantes: list[OnboardingIntegranteRequest]
    relaciones: list = []


def _recording_base() -> str:
    return os.environ.get("BASE_URL", "https://www.ethosbios.com")


@app.post("/onboarding", status_code=201)
@limiter.limit("5/hour")
async def onboarding(request: Request, req: OnboardingRequest):
    """
    Crea o actualiza la familia en Firestore y devuelve tokens de grabación.
    - Sin familia_id: crea familia nueva (flujo admin manual, requiere X-Admin-Key).
    - Con familia_id + buyer_token: actualiza doc existente (flujo comprador Hotmart).
    - Con familia_id + X-Admin-Key: flujo admin con familia existente.
    Idempotente por nombre de integrante. Rate-limited: 5 req/hora.
    """
    # Auth: X-Admin-Key (admin) o buyer_token válido para este familia_id (comprador)
    x_admin_key = request.headers.get("x-admin-key", "")
    pwd = os.environ.get("ADMIN_PASSWORD", "")
    if pwd and x_admin_key == pwd:
        pass  # admin autenticado
    elif req.buyer_token and req.familia_id:
        from pipeline.utils import firestore as _fs_bt
        if not _fs_bt.validate_temp_token(req.buyer_token, req.familia_id):
            raise HTTPException(status_code=401, detail="Token de comprador inválido o expirado")
    else:
        raise HTTPException(status_code=401, detail="No autorizado")
    from google.cloud import firestore as _firestore
    from pipeline.utils import firestore as fs

    db = fs._db()

    if req.familia_id:
        # ── Flujo Hotmart: actualizar documento existente ──────────────────
        familia_id = req.familia_id
        doc_ref = db.collection("familias").document(familia_id)
        doc = doc_ref.get()
        if not doc.exists:
            raise HTTPException(status_code=404, detail=f"Familia {familia_id} no encontrada en Firestore")

        existing = doc.to_dict()
        # Preservar email del comprador: usar el del request solo si no hay uno ya guardado
        email_existente = (existing.get("comprador") or {}).get("email", "")
        email_final = email_existente or req.email_comprador.strip()

        # update() con dot-notation toca solo esos campos; pack/origen/hotmart_transaction no se tocan
        doc_ref.update({
            "nombre": req.nombre_familia,
            "estado": "onboarding",
            "comprador.email": email_final,
            "acepta_tyc": True,
            "acepta_tyc_timestamp": datetime.utcnow().isoformat(),
        })
    else:
        # ── Flujo admin manual: crear familia nueva ────────────────────────
        familia_id = uuid.uuid4().hex[:16]
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
                "pais": req.integrantes[0].pais if req.integrantes else "",
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
    fs.update_familia_ultima_grabacion(familia_id)
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
    fs.update_familia_ultima_grabacion(familia_id)
    respuestas_guardadas = fs.get_respuestas(familia_id, integrante_id)
    pct = round(len([r for r in respuestas_guardadas if r.get("audio_url")]) / 17 * 100)
    fs.update_porcentaje_avance(familia_id, integrante_id, pct)
    if pct >= 100:
        fs.update_integrante_estado(familia_id, integrante_id, "completo")
        _check_y_trigger(familia_id)
    else:
        fs.update_integrante_estado(familia_id, integrante_id, "en_progreso")
    return {"ok": True}


@app.post("/token/{token}/voz-permanente")
async def token_voz_permanente(token: str, audio: UploadFile = File(...)):
    """
    Recibe el mensaje de voz de la pregunta 18, lo copia al bucket permanente,
    genera un token de 64 bits y lo persiste en Firestore.
    """
    from pipeline.utils import firestore as fs, storage as st

    match = fs.get_integrante_by_token(token)
    if match is None:
        raise HTTPException(status_code=404, detail="Token inválido o no encontrado")
    familia_id, integrante_id, _ = match

    content_type = (audio.content_type or "").split(";")[0].strip().lower()
    if content_type not in _ALLOWED_AUDIO_TYPES:
        raise HTTPException(
            status_code=415,
            detail=f"Tipo de archivo no permitido: {content_type!r}. Se aceptan: {', '.join(sorted(_ALLOWED_AUDIO_TYPES))}",
        )

    audio_bytes = await audio.read()
    if len(audio_bytes) > _MAX_AUDIO_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Archivo demasiado grande ({len(audio_bytes) / 1024 / 1024:.1f} MB). Máximo permitido: 25 MB.",
        )

    filename = audio.filename or "voz.webm"
    ext = filename.rsplit(".", 1)[-1] if "." in filename else "webm"
    blob_name = f"{familia_id}/{integrante_id}_voz_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}.{ext}"

    with tempfile.NamedTemporaryFile(suffix=f".{ext}", delete=False) as tmp:
        tmp.write(audio_bytes)
        tmp_path = tmp.name

    try:
        temp_gs_url = st.upload_to_gcs(tmp_path, st.GCS_BUCKET_AUDIOS, blob_name, audio.content_type or "audio/webm")
        voz_gs_url = st.copy_to_voces_permanentes(temp_gs_url, familia_id, integrante_id)
    finally:
        os.unlink(tmp_path)

    voz_token = uuid.uuid4().hex[:16]  # 64 bits
    fs.save_voz_permanente(familia_id, integrante_id, voz_token, voz_gs_url)

    base = _recording_base()
    return {"ok": True, "voz_token": voz_token, "voz_url": f"{base}/voz/{voz_token}"}


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

    base = _recording_base()

    # Botones de grabación (solo si ya existen integrantes — no en el momento del webhook)
    integrantes = fs.get_integrantes(familia_id)
    tokens = [
        {"nombre": i.get("nombre", ""), "url": f"{base}/r/{i.get('token_unico', '')}"}
        for i in integrantes
        if i.get("token_unico") and not i.get("es_menor")
    ]

    # Si la familia tiene buyer_token (Hotmart flow), incluir link de onboarding en el email
    buyer_token = familia.get("buyer_token", "")
    onboarding_url = f"{base}/onboarding?familia_id={familia_id}&dt={buyer_token}" if buyer_token else None

    # Magic link al panel /mi-familia — siempre presente si hay access_token
    access_token = familia.get("access_token", "")
    magic_link_url = f"{base}/auth/{access_token}" if access_token else None

    if not tokens and not onboarding_url and not magic_link_url:
        logger.warning("[email-bienvenida] sin contenido para familia=%s, omitiendo", familia_id)
        return

    try:
        send_bienvenida(
            email_comprador=email_comprador,
            nombre_familia=nombre_familia,
            tokens=tokens,
            onboarding_url=onboarding_url,
            magic_link_url=magic_link_url,
        )
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


_VOSEO_PAISES = {"argentina", "uruguay", "paraguay", "ar", "uy", "py"}


def _es_voseo(pais: str) -> bool:
    return pais.lower().strip() in _VOSEO_PAISES


def _crear_familia_hotmart(email: str, nombre: str, pack: str, transaction: str) -> str:
    from google.cloud import firestore as _firestore
    from pipeline.utils import firestore as fs

    # Idempotencia: si ya existe un doc con este transaction, devolver ese ID sin crear otro
    if transaction:
        existing = list(
            fs._db().collection("familias")
            .where("hotmart_transaction", "==", transaction)
            .limit(1)
            .stream()
        )
        if existing:
            familia_id = existing[0].id
            logger.info("[webhook-hotmart] familia ya existe familia=%s transaction=%s (retry idempotente)", familia_id, transaction)
            return familia_id

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
        "pais": "",
        "fecha_compra": _firestore.SERVER_TIMESTAMP,
        "fecha_entrega": None,
        "origen": "hotmart",
        "hotmart_transaction": transaction,
    })
    # Buyer token (2h) para self-service onboarding — guardado en familia doc
    # para que _enviar_email_bienvenida lo incluya en el email de bienvenida.
    buyer_token = uuid.uuid4().hex
    fs.create_temp_token(buyer_token, familia_id, ttl_minutes=120)
    fs._db().collection("familias").document(familia_id).update({"buyer_token": buyer_token})

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


@app.get("/familia/by-hotmart-transaction")
def familia_by_hotmart_transaction(hp_tx: str = Query(...)):
    """
    Lookup de familia por Hotmart transaction ID.
    Devuelve familia_id + buyer_token (2h, multi-uso) para que gracias.html pueda:
    - encuestar /familia/{id}/link-acceso con dt=buyer_token
    - abrir /onboarding?familia_id={id}&dt=buyer_token
    El hp_tx actúa como prueba de compra — no requiere auth adicional.
    """
    from pipeline.utils import firestore as fs

    results = list(
        fs._db().collection("familias")
        .where("hotmart_transaction", "==", hp_tx)
        .limit(1)
        .stream()
    )

    if not results:
        return {"ok": False}

    familia_id = results[0].id
    buyer_token = uuid.uuid4().hex
    fs.create_temp_token(buyer_token, familia_id, ttl_minutes=120)

    return {"ok": True, "familia_id": familia_id, "dt": buyer_token}


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
            "preguntas_total": 17,
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
def test_email_libro(
    email: str = os.environ.get("ADMIN_EMAIL", "hola@ethosbios.com"),
    _: None = Depends(_admin_auth),
):
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


# ─── Panel Admin Pro: /admin/dashboard (Briefing #32) ─────────────────────────

_ADMIN_SESSION_COOKIE = "admin_session"
_ADMIN_SESSION_MAX_AGE = 8 * 3600  # 8 horas


def _sign_admin_session() -> str:
    return URLSafeTimedSerializer(
        os.environ.get("SESSION_SECRET", "fallback"), salt="admin"
    ).dumps({"admin": True})


def _verify_admin_session(cookie_value: str) -> bool:
    try:
        URLSafeTimedSerializer(
            os.environ.get("SESSION_SECRET", "fallback"), salt="admin"
        ).loads(cookie_value, max_age=_ADMIN_SESSION_MAX_AGE)
        return True
    except Exception:
        return False


def _admin_auth_browser(request: Request, x_admin_key: str | None = Header(default=None)) -> None:
    """Acepta autenticación por header X-Admin-Key (curl/API) O por cookie de sesión (browser)."""
    cookie = request.cookies.get(_ADMIN_SESSION_COOKIE, "")
    if cookie and _verify_admin_session(cookie):
        return
    pwd = os.environ.get("ADMIN_PASSWORD", "")
    if pwd and x_admin_key == pwd:
        return
    raise HTTPException(
        status_code=302,
        headers={"Location": "/admin/login"},
        detail="No autorizado",
    )


@app.get("/admin")
def admin_root():
    """Redirige /admin → /admin/dashboard."""
    return RedirectResponse(url="/admin/dashboard", status_code=302)


@app.get("/admin/login")
def admin_login_form(request: Request):
    cookie = request.cookies.get(_ADMIN_SESSION_COOKIE, "")
    if cookie and _verify_admin_session(cookie):
        return RedirectResponse(url="/admin/dashboard", status_code=302)
    return _templates.TemplateResponse(request=request, name="admin_login.html", context={})


@app.post("/admin/login")
async def admin_login_submit(request: Request):
    form = await request.form()
    pwd_input = form.get("password", "")
    pwd_real = os.environ.get("ADMIN_PASSWORD", "")
    if not pwd_real or pwd_input != pwd_real:
        return _templates.TemplateResponse(
            request=request,
            name="admin_login.html",
            context={"error": "Contraseña incorrecta"},
            status_code=401,
        )
    response = RedirectResponse(url="/admin/dashboard", status_code=303)
    response.set_cookie(
        key=_ADMIN_SESSION_COOKIE,
        value=_sign_admin_session(),
        max_age=_ADMIN_SESSION_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=True,
    )
    return response


@app.get("/admin/logout")
def admin_logout():
    response = RedirectResponse(url="/admin/login", status_code=302)
    response.delete_cookie(_ADMIN_SESSION_COOKIE)
    return response


@app.get("/admin/dashboard")
def admin_dashboard(
    request: Request,
    mostrar_tests: bool = Query(default=False, alias="mostrar_tests"),
    _: None = Depends(_admin_auth_browser),
):
    from pipeline.utils import dashboard as dash

    data = dash.build_dashboard_data(show_tests=mostrar_tests)
    return _templates.TemplateResponse(
        request=request,
        name="admin_dashboard.html",
        context={
            "alertas": data["alertas"],
            "kpis": data["kpis"],
            "familias": data["familias"],
            "quality": data["quality"],
            "mostrar_tests": mostrar_tests,
        },
    )


@app.get("/admin/dashboard/export.csv")
def admin_dashboard_export_csv(
    request: Request,
    mostrar_tests: bool = Query(default=False, alias="mostrar_tests"),
    _: None = Depends(_admin_auth_browser),
):
    """Exporta la tabla de familias como CSV (mismas columnas que el Bloque D)."""
    import csv
    import io
    from pipeline.utils import dashboard as dash

    data = dash.build_dashboard_data(show_tests=mostrar_tests)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "familia_id", "nombre", "estado", "avance_%",
        "dias_desde_compra", "semaforo_refund", "pack", "precio_usd",
        "costo_real_usd", "margen_real_usd", "comprador_email",
    ])
    for f in data["familias"]:
        writer.writerow([
            f["familia_id"], f["nombre"], f["estado_operativo"],
            f["avance"], f["dias_desde_compra"] if f["dias_desde_compra"] is not None else "",
            f["semaforo_refund"] or "", f["pack"] or "",
            f["precio"] if f["precio"] is not None else "",
            f["costo_real"] if f["costo_real"] is not None else "",
            f["margen_real"] if f["margen_real"] is not None else "",
            f["comprador_email"],
        ])
    from fastapi.responses import Response
    return Response(
        content=buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=familias.csv"},
    )


class ResolverEscalacionBody(BaseModel):
    nota: str = ""


@app.post("/admin/familia/{familia_id}/integrante/{integrante_id}/resolver-escalacion")
def admin_resolver_escalacion(
    familia_id: str,
    integrante_id: str,
    body: ResolverEscalacionBody,
    _: None = Depends(_admin_auth),
):
    """Marca una escalación humana como revisada y resuelta manualmente."""
    from pipeline.utils import firestore as fs

    familia = fs.get_familia(familia_id)
    if not familia:
        raise HTTPException(status_code=404, detail=f"Familia no encontrada: {familia_id}")

    fs.mark_escalacion_resuelta(familia_id, integrante_id, nota=body.nota)
    logger.info(
        "[admin-resolver-escalacion] familia=%s integrante=%s nota=%r",
        familia_id, integrante_id, body.nota,
    )
    return {"ok": True, "familia_id": familia_id, "integrante_id": integrante_id}


@app.post("/admin/familia/{familia_id}/recordatorio")
def admin_familia_recordatorio(familia_id: str, _: None = Depends(_admin_auth)):
    """Envía el email de recordatorio a los integrantes con grabación pendiente."""
    from pipeline.utils import firestore as fs
    from pipeline.utils.email import send_recordatorio

    familia = fs.get_familia(familia_id)
    if not familia:
        raise HTTPException(status_code=404, detail=f"Familia no encontrada: {familia_id}")

    nombre_familia = familia.get("nombre", "tu familia")
    base = _recording_base()
    integrantes = fs.get_integrantes(familia_id)
    pendientes = [i for i in integrantes if i.get("estado") != "completo" and i.get("email")]

    enviados = []
    for integrante in pendientes:
        token = integrante.get("token_unico", "")
        if not token:
            continue
        try:
            send_recordatorio(
                email_integrante=integrante["email"],
                nombre_integrante=integrante.get("nombre", ""),
                nombre_familia=nombre_familia,
                token_url=f"{base}/r/{token}",
            )
            enviados.append(integrante.get("nombre", ""))
        except Exception as exc:  # noqa: BLE001
            logger.warning("[admin-recordatorio] error enviando a %s: %s", integrante.get("email"), exc)

    return {"ok": True, "familia_id": familia_id, "enviados": enviados, "pendientes_sin_email": len(pendientes) - len(enviados)}


class ReintentarPipelineBody(BaseModel):
    solo_desde: str | None = None


@app.post("/admin/familia/{familia_id}/reintentar-pipeline")
def admin_familia_reintentar_pipeline(
    familia_id: str, body: ReintentarPipelineBody, _: None = Depends(_admin_auth)
):
    """Reintenta el pipeline para una familia desde el paso indicado (o desde el principio)."""
    from pipeline.utils import firestore as fs
    from pipeline.utils.tasks import enqueue_pipeline

    familia = fs.get_familia(familia_id)
    if not familia:
        raise HTTPException(status_code=404, detail=f"Familia no encontrada: {familia_id}")

    if body.solo_desde and body.solo_desde not in orchestrator.STEPS:
        raise HTTPException(status_code=400, detail=f"solo_desde inválido: {body.solo_desde!r}")

    integrantes = fs.get_integrantes(familia_id)
    nombres = [i.get("nombre", "") for i in integrantes if i.get("nombre")]

    job_id = str(uuid.uuid4())
    fs.create_job(job_id, familia_id=familia_id)
    enqueue_pipeline(
        job_id,
        {
            "nombres": nombres,
            "pais": familia.get("pais", "argentina"),
            "solo_desde": body.solo_desde,
            "familia": familia.get("nombre", ""),
            "upload_to_gcs": True,
            "familia_id": familia_id,
            "from_job_id": None,
        },
    )
    logger.info("[admin-reintentar-pipeline] familia=%s solo_desde=%s job=%s", familia_id, body.solo_desde, job_id)
    return {"ok": True, "job_id": job_id, "solo_desde": body.solo_desde}


# ─── Familia de prueba end-to-end (Briefing #42) ─────────────────────────────

_TEST_INTEGRANTES = [
    {
        "nombre": "Rosa Pérez",
        "relacion": "abuela",
        "pais": "argentina",
        "transcripcion": (
            "[Pregunta 1] Mirá, yo nací en Realicó, un pueblo chico de La Pampa. Mi papá tenía un almacén "
            "de campo, de esos que vendían de todo: yerba, harina, pala, alambre. Me acuerdo perfectamente "
            "del olor adentro: era como a cedro viejo y a yerba fresca juntos. Los viernes yo ayudaba a "
            "mi papá a contar la plata de la caja, eso era mi responsabilidad. Tenía diez años y me "
            "enorgullecía un montón. Me sentaba en el taburete alto y contaba billete por billete.\n\n"
            "[Pregunta 2] Mi mamá se llamaba Bernarda. Era una mujer de pocas palabras pero de mucho "
            "trabajo. Se levantaba a las cinco de la mañana, todos los días sin excepción, aunque nevara. "
            "Una vez me enseñó a hacer empanadas y me dijo: 'Rosita, las empanadas hay que hacerlas con "
            "calma, como la vida.' Eso lo repito hasta hoy cuando cocino con mis nietos.\n\n"
            "[Pregunta 3] Me casé a los veintidós años con Héctor, que era el hijo del veterinario del "
            "pueblo. Nos conocimos en un baile de carnaval. Yo tenía un vestido celeste que me había "
            "hecho mi tía Herminia en cuatro noches seguidas de costura. Héctor me sacó a bailar y me "
            "pisó tres veces el pie. Le dije: 'Con esos pies no llegás a ningún lado.' Y se reía, se "
            "reía. Fue un gran amor, más de cincuenta años juntos.\n\n"
            "[Pregunta 5] Lo que le diría a mis nietos es que el tiempo pasa muy rápido. Que aprovechen "
            "cada momento con la gente que quieren. Que no peleen por cosas materiales. Y que aprendan "
            "a cocinar, porque la comida es amor concreto, es la única cosa que podés darle a alguien "
            "que entra directo al cuerpo."
        ),
    },
    {
        "nombre": "Carlos Martínez",
        "relacion": "tío",
        "pais": "españa",
        "transcripcion": (
            "[Pregunta 1] Soy de Sevilla, aunque llevo treinta años viviendo aquí en Argentina. Me vine "
            "a los treinta y cinco por una oferta de trabajo en un hospital privado, y me quedé porque "
            "me enamoré de este país. Tengo dos pasaportes y, como me gusta decir, dos corazones: "
            "uno andaluz y uno porteño.\n\n"
            "[Pregunta 3] Soy traumatólogo, trabajo en el Hospital Italiano de Buenos Aires. Lo que más "
            "me satisface es ver a un paciente que llegó en silla de ruedas salir caminando por su propio "
            "pie. He tenido casos muy difíciles, fracturas que parecían irreparables, y la recuperación "
            "fue total. El cuerpo humano tiene una capacidad de recuperación que me asombra todavía "
            "después de treinta años de profesión.\n\n"
            "[Pregunta 4] Mis amigos aquí son como mi familia adoptiva. Con Gerardo, que es dentista, "
            "nos conocemos desde el primer día que llegué a Buenos Aires. Me encontraba perdido en "
            "Palermo con un mapa de papel y él me explicó cómo llegar al hospital. Tomamos un café, "
            "después una cerveza, y desde entonces somos inseparables. Eso no se planea."
        ),
    },
    {
        "nombre": "Graciela López",
        "relacion": "abuela",
        "pais": "uruguay",
        "transcripcion": (
            "[Pregunta 1] Bueno... no sé muy bien por dónde empezar la verdad. Yo nací en Montevideo, "
            "eso sí. O por ahí, cerca. Éramos varios hermanos, no me acuerdo cuántos exactamente. "
            "La infancia fue, bueno, normal. Como la de todos, supongo. No tengo algo especial "
            "para contar, ¿sabés?\n\n"
            "[Pregunta 2] Mis padres eran buenas personas, eso sí. Mi mamá trabajaba, creo que en algo "
            "de costura, no estoy segura. Mi papá también hacía algo, no me acuerdo qué exactamente. "
            "Vivíamos en un departamento, creo. O era una casa. Fue hace mucho tiempo todo eso, "
            "la memoria me falla.\n\n"
            "[Pregunta 3] Trabajé en varias cosas. En una oficina, en un comercio, no me acuerdo bien "
            "la secuencia. Hice algunas cosas que me gustaron y otras que no tanto. Nada extraordinario, "
            "en realidad. La vida normal de cualquier persona. No sé si eso sirve para el libro."
        ),
    },
    {
        "nombre": "Martín Pérez",
        "relacion": "hijo",
        "pais": "colombia",
        "transcripcion": (
            "[Pregunta 1] Soy de Medellín, aunque llegué a Argentina hace quince años. Estudié ingeniería "
            "de sistemas y trabajé varios años en telecomunicaciones. Me vine porque conseguí trabajo "
            "acá y me quedé porque me enamoré de Buenos Aires. Tengo tres hijos y son lo más importante "
            "de mi vida.\n\n"
            "[Pregunta 3] Hace cinco años armé mi propia empresa de desarrollo de software. Fue duro "
            "al principio, muchas noches sin dormir, muchas incertidumbres. Pero ahora está bien "
            "encaminada. Tengo un equipo de ocho personas, todos muy comprometidos. Mi sueño siempre "
            "fue tener mi propio negocio y lo logré, así que no me puedo quejar.\n\n"
            "[Pregunta 5] Le quiero dejar a mis hijos el ejemplo de que con trabajo y honestidad se "
            "puede salir adelante. Que no hay atajos en la vida. Y que la familia primero, siempre, "
            "porque el trabajo se puede recuperar pero el tiempo con la familia no."
        ),
    },
]

# Costo estimado del pipeline con 4 personas (Claude voice+chapters+quality+editor+coherencia)
_COSTO_ESTIMADO_TEST_USD = 1.88


@app.post("/admin/generar-familia-test")
def admin_generar_familia_test(_: None = Depends(_admin_auth)):
    """
    Genera una familia sintética con 4 integrantes y transcripciones de prueba variadas,
    y dispara el pipeline real completo (voice→chapters→quality→editor→layout→email).

    ADVERTENCIA: consume API real (~USD 1.88: Claude + pipeline completo). No es gratis.

    Devuelve todos los links navegables del sistema para esa familia test.
    La familia queda marcada con es_test:true para no contaminar métricas de negocio.
    """
    from google.cloud import firestore as _firestore
    from pipeline.utils import firestore as fs
    from pipeline.utils.tasks import enqueue_pipeline

    familia_id = uuid.uuid4().hex[:16]
    nombre_familia = f"Familia Test {familia_id[:6].upper()}"
    test_email = os.environ.get("ADMIN_EMAIL", "hola@ethosbios.com")
    base = _recording_base()

    # 1. Crear familia con es_test: True
    db = fs._db()
    db.collection("familias").document(familia_id).set({
        "nombre": nombre_familia,
        "comprador": {
            "email": test_email,
            "nombre": "Admin Test",
            "es_tambien_retratado": False,
        },
        "estado": "grabando",
        "pack": "familiar",
        "pais": "",
        "es_test": True,
        "fecha_compra": _firestore.SERVER_TIMESTAMP,
        "fecha_entrega": None,
        "origen": "admin_test",
    })

    # 2. Crear integrantes con transcripciones sintéticas y marcarlos como completos
    tokens_info = []
    nombres = []
    for ing_def in _TEST_INTEGRANTES:
        integrante_id, token = fs.add_integrante(
            familia_id=familia_id,
            nombre=ing_def["nombre"],
            relacion_con_comprador=ing_def["relacion"],
        )
        db.collection("familias").document(familia_id).collection("integrantes").document(integrante_id).update({
            "pais": ing_def["pais"],
            "estado": "completo",
            "porcentaje_avance": 100,
            "transcripcion_completa": ing_def["transcripcion"],
        })

        # Guardar transcripciones en respuestas/ por pregunta — mismo formato que Whisper.
        # voice_agent.run_from_firestore lee de get_transcripciones_integrante(), que lee
        # de respuestas/{pregunta_id}.transcripcion. Sin esto, voice_agent falla silenciosamente.
        import re as _re
        bloques = _re.split(r"\[Pregunta (\d+)\]", ing_def["transcripcion"])
        # bloques: ["", "1", "texto1", "3", "texto3", ...]
        it = iter(bloques[1:])  # saltar el string vacío inicial
        for pregunta_id, texto in zip(it, it):
            texto = texto.strip()
            if texto:
                fs.save_respuesta(familia_id, integrante_id, pregunta_id, audio_url="")
                fs.save_transcripcion(familia_id, integrante_id, pregunta_id, texto)

        # Token de voz permanente sintético (sin audio real; /voz/ mostrará "no disponible")
        voz_token = uuid.uuid4().hex[:16]
        fs.save_voz_permanente(familia_id, integrante_id, voz_token, "")

        tokens_info.append({
            "nombre": ing_def["nombre"],
            "pais": ing_def["pais"],
            "voseo": _es_voseo(ing_def["pais"]),
            "token": token,
            "voz_token": voz_token,
        })
        nombres.append(ing_def["nombre"])

    # 3. Crear access_token para el panel /mi-familia
    access_token = str(uuid.uuid4())
    access_expires = datetime.now(timezone.utc) + timedelta(days=90)
    fs.set_access_token(familia_id, access_token, access_expires)

    # 4. Disparar pipeline real desde "voice" (saltamos transcripción; ya tenemos transcripcion_completa)
    job_id = str(uuid.uuid4())
    fs.create_job(job_id, familia_id=familia_id)
    enqueue_pipeline(
        job_id,
        {
            "nombres": nombres,
            "pais": "",
            "solo_desde": "voice",
            "familia": nombre_familia,
            "upload_to_gcs": True,
            "familia_id": familia_id,
            "from_job_id": None,
        },
    )
    fs.update_familia_estado(familia_id, "generando")
    logger.info(
        "[admin-generar-familia-test] familia=%s job=%s nombres=%s email=%s",
        familia_id, job_id, nombres, test_email,
    )

    return {
        "ok": True,
        "familia_id": familia_id,
        "job_id": job_id,
        "advertencia_costo": (
            f"⚠️ Esta corrida consume API real. Costo estimado: ~USD {_COSTO_ESTIMADO_TEST_USD} "
            "(Claude voice + chapters + quality A/B/C + coherencia + editor). "
            "No incluye Whisper (omitido: transcripciones sintéticas). "
            f"El email de entrega va a: {test_email}"
        ),
        "costo_estimado_usd": _COSTO_ESTIMADO_TEST_USD,
        "links": {
            "estado_job": f"{base}/job/{job_id}",
            "panel_comprador_magic_link": f"{base}/auth/{access_token}",
            "panel_comprador": f"{base}/mi-familia",
            "admin_dashboard_test": f"{base}/admin/dashboard?mostrar_tests=true",
            "pdf_cuando_listo": f"Consultar GET {base}/job/{job_id} — campo pdf_url cuando status=done",
        },
        "integrantes": [
            {
                "nombre": t["nombre"],
                "pais": t["pais"],
                "voseo": t["voseo"],
                "copy_esperado": "vos/tuyo" if t["voseo"] else "tú/tuyo",
                "link_grabacion": f"{base}/r/{t['token']}",
                "link_voz": f"{base}/voz/{t['voz_token']}",
            }
            for t in tokens_info
        ],
        "instrucciones": [
            f"Pipeline corriendo en background (~30-40 min). Estado: GET {base}/job/{job_id}",
            f"El PDF y el email de entrega llegarán a {test_email} cuando termine.",
            f"Panel del comprador: {base}/auth/{access_token}",
            f"Ver en dashboard admin: /admin/dashboard?mostrar_tests=true",
            f"Limpiar cuando termines: DELETE /admin/familias-test/{familia_id}",
        ],
    }


@app.delete("/admin/familias-test/{familia_id}")
def admin_borrar_familia_test(familia_id: str, _: None = Depends(_admin_auth)):
    """
    Elimina una familia de test específica (es_test:true) de Firestore y GCS.
    Falla de forma segura si la familia no está marcada como es_test.
    """
    from pipeline.utils import firestore as fs, storage as st

    try:
        result = fs.delete_familia_completa(familia_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # Limpieza GCS
    gcs_stats: dict = {}
    for bucket_name in [st.GCS_BUCKET_AUDIOS, st.GCS_BUCKET_FOTOS, st.GCS_BUCKET_VOCES]:
        try:
            n = st.delete_gcs_prefix(bucket_name, familia_id + "/")
            gcs_stats[bucket_name] = n
        except Exception as exc:  # noqa: BLE001
            logger.warning("[admin-borrar-familia-test] GCS error bucket=%s: %s", bucket_name, exc)
            gcs_stats[bucket_name] = f"error: {exc}"

    # Borrar el PDF del libro si tiene URL GCS
    libro_url = result.get("libro_url", "")
    if libro_url and libro_url.startswith("gs://"):
        try:
            from pipeline.utils.storage import _parse_gs_url, _gcs
            bucket_name_l, blob_name_l = _parse_gs_url(libro_url)
            _gcs().bucket(bucket_name_l).blob(blob_name_l).delete()
            gcs_stats["pdf_deleted"] = blob_name_l
        except Exception as exc:  # noqa: BLE001
            logger.warning("[admin-borrar-familia-test] no se pudo borrar PDF %s: %s", libro_url, exc)

    logger.info("[admin-borrar-familia-test] familia=%s stats=%s gcs=%s", familia_id, result["stats"], gcs_stats)
    return {
        "ok": True,
        "familia_id": familia_id,
        "firestore": result["stats"],
        "gcs": gcs_stats,
    }


@app.delete("/admin/familias-test")
def admin_borrar_todas_familias_test(_: None = Depends(_admin_auth)):
    """
    Elimina TODAS las familias marcadas como es_test:true de Firestore y GCS.
    Útil para limpieza periódica. Devuelve resumen por familia.
    """
    from pipeline.utils import firestore as fs, storage as st

    familia_ids = fs.list_familias_test()
    if not familia_ids:
        return {"ok": True, "eliminadas": 0, "familias": []}

    resultados = []
    for fid in familia_ids:
        try:
            res = fs.delete_familia_completa(fid)
            gcs_stats: dict = {}
            for bucket_name in [st.GCS_BUCKET_AUDIOS, st.GCS_BUCKET_FOTOS, st.GCS_BUCKET_VOCES]:
                try:
                    gcs_stats[bucket_name] = st.delete_gcs_prefix(bucket_name, fid + "/")
                except Exception as exc:  # noqa: BLE001
                    gcs_stats[bucket_name] = f"error: {exc}"
            libro_url = res.get("libro_url", "")
            if libro_url and libro_url.startswith("gs://"):
                try:
                    from pipeline.utils.storage import _parse_gs_url, _gcs
                    bn, blob = _parse_gs_url(libro_url)
                    _gcs().bucket(bn).blob(blob).delete()
                    gcs_stats["pdf_deleted"] = blob
                except Exception as exc:  # noqa: BLE001
                    gcs_stats["pdf_error"] = str(exc)
            resultados.append({"familia_id": fid, "ok": True, "firestore": res["stats"], "gcs": gcs_stats})
        except Exception as exc:  # noqa: BLE001
            logger.warning("[admin-borrar-todas-test] error eliminando familia=%s: %s", fid, exc)
            resultados.append({"familia_id": fid, "ok": False, "error": str(exc)})

    eliminadas = sum(1 for r in resultados if r.get("ok"))
    logger.info("[admin-borrar-todas-test] eliminadas=%d de %d", eliminadas, len(familia_ids))
    return {"ok": True, "eliminadas": eliminadas, "total": len(familia_ids), "familias": resultados}
