import logging
import os
import re
from datetime import datetime
from html import escape as _escape
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

_TEMPLATES_DIR = Path(__file__).parent.parent / "templates"
_FROM = "hola@ethosbios.com"
_POSTMARK_URL = "https://api.postmarkapp.com/email"


def _get_key() -> str | None:
    key = os.environ.get("POSTMARK_API_KEY", "")
    if not key:
        logger.warning("POSTMARK_API_KEY no configurada")
        return None
    return key


def _html_to_text(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r" {2,}", " ", text).strip()


def _send(key: str, to: str, subject: str, html: str) -> None:
    resp = httpx.post(
        _POSTMARK_URL,
        headers={"X-Postmark-Server-Token": key, "Content-Type": "application/json"},
        json={
            "From": _FROM,
            "To": to,
            "Subject": subject,
            "HtmlBody": html,
            "TextBody": _html_to_text(html),
        },
        timeout=15,
    )
    if not resp.is_success:
        logger.error(
            "Postmark error %s — %s — body: %s",
            resp.status_code,
            subject,
            resp.text,
        )
    resp.raise_for_status()


def send_bienvenida(
    email_comprador: str,
    nombre_familia: str,
    tokens: list[dict],
    onboarding_url: str | None = None,
) -> None:
    key = _get_key()
    if not key:
        return

    html = (_TEMPLATES_DIR / "email_completado.html").read_text()

    ctaes = "".join(
        f'<a href="{t["url"]}" class="em-cta" style="margin-bottom:0.5rem;display:block">'
        f'{t["nombre"]} — empezar a grabar ↗</a>'
        for t in tokens
    )
    if onboarding_url:
        ctaes += (
            f'<a href="{onboarding_url}" class="em-cta" '
            f'style="margin-bottom:0.5rem;display:block;background:#C49B18;">'
            f'Configurar los integrantes de tu familia →</a>'
        )

    html = (
        html
        .replace("Hola, {{COMPRADOR_NOMBRE}}", "Hola")
        .replace("{{FAMILIA_NOMBRE}}", nombre_familia)
        .replace("{{INTEGRANTE_NOMBRE}}", "")
        .replace("{{FECHA_HORA}}", "")
        .replace('<a href="{{DASHBOARD_URL}}" class="em-cta">Ver progreso de la familia →</a>', ctaes)
    )

    try:
        _send(key, email_comprador, f"Ya podés empezar — {nombre_familia}", html)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Error enviando bienvenida a %s: %s", email_comprador, exc)


def send_libro_listo(email_comprador: str, nombre_familia: str, signed_url: str) -> None:
    key = _get_key()
    if not key:
        return

    html = (_TEMPLATES_DIR / "email_entrega.html").read_text()

    hoy = datetime.now().strftime("%d/%m/%Y")

    html = (
        html
        .replace("{{FAMILIA_NOMBRE}}", nombre_familia)
        .replace("¡Hola, {{COMPRADOR_NOMBRE}}!", "¡Hola!")
        .replace("{{PDF_URL}}", signed_url)
        .replace("{{DASHBOARD_URL}}", "https://ethosbios.com/mi-familia")
        .replace("{{AÑO}}", str(datetime.now().year))
        .replace("{{TOTAL_HISTORIAS}}", "")
        .replace("{{FECHA_GENERACION}}", hoy)
    )

    try:
        _send(key, email_comprador, f"Tu libro está listo — {nombre_familia}", html)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Error enviando libro_listo a %s: %s", email_comprador, exc)


def send_magic_link(email: str, nombre_familia: str, magic_link_url: str) -> None:
    key = _get_key()
    if not key:
        return

    html = (_TEMPLATES_DIR / "email_magic_link.html").read_text()
    html = (
        html
        .replace("{{NOMBRE_FAMILIA}}", nombre_familia)
        .replace("{{MAGIC_LINK}}", magic_link_url)
        .replace("{{AÑO}}", str(datetime.now().year))
    )

    try:
        _send(key, email, f"Tu link de acceso — {nombre_familia}", html)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Error enviando magic link a %s: %s", email, exc)


def send_recordatorio(
    email_integrante: str,
    nombre_integrante: str,
    nombre_familia: str,
    token_url: str,
) -> None:
    key = _get_key()
    if not key:
        return

    html = (_TEMPLATES_DIR / "email_reminder.html").read_text()

    html = (
        html
        .replace("{{NOMBRE}}", nombre_integrante)
        .replace("{{RECORDING_URL}}", token_url)
    )

    try:
        _send(key, email_integrante, f"{nombre_familia} te está esperando", html)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Error enviando recordatorio a %s: %s", email_integrante, exc)


def send_integrante_agregado(
    email_comprador: str,
    nombre_familia: str,
    nombre_integrante: str,
    token_url: str,
) -> None:
    key = _get_key()
    if not key:
        return

    html = f'''<html><body style="font-family:sans-serif;color:#3d2b0a;max-width:560px;margin:0 auto;padding:24px">
<p style="font-family:Georgia,serif;font-size:22px;margin-bottom:16px">Ethos Bios</p>
<h2 style="font-weight:400;margin-bottom:8px">Nuevo integrante agregado</h2>
<p style="color:#9a7b5a;margin-bottom:20px">Acabás de agregar a <strong>{nombre_integrante}</strong> al libro de <strong>{nombre_familia}</strong>.</p>
<p style="margin-bottom:20px">Ya puede empezar a grabar sus historias usando el link de abajo. Podés compartírselo directamente.</p>
<a href="{token_url}" style="display:inline-block;background:#2C1A0E;color:#F5EDD8;padding:12px 24px;border-radius:8px;text-decoration:none;font-weight:700;font-size:14px">{nombre_integrante} — empezar a grabar ↗</a>
<p style="margin-top:32px;font-size:12px;color:#b89a7a">Ethos Bios · hola@ethosbios.com</p>
</body></html>'''

    try:
        _send(key, email_comprador, f"Nuevo integrante agregado — {nombre_familia}", html)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Error enviando integrante_agregado a %s: %s", email_comprador, exc)


def send_alerta_admin(familia_id: str, job_id: str, etapa: str, error: str) -> None:
    key = _get_key()
    if not key:
        return
    admin_email = os.environ.get("ADMIN_EMAIL", "hola@ethosbios.com")
    html = f"""<html><body style="font-family:sans-serif;color:#3d2b0a;max-width:560px;margin:0 auto;padding:24px">
<p style="font-family:Georgia,serif;font-size:22px;margin-bottom:16px">Ethos Bios</p>
<h2 style="font-weight:400;margin-bottom:8px;color:#c0392b">Pipeline fallido</h2>
<table style="border-collapse:collapse;width:100%;margin-bottom:20px">
<tr><td style="padding:8px;background:#fdf0f0;font-weight:700;width:120px">Familia ID</td><td style="padding:8px;background:#fdf0f0"><code>{familia_id}</code></td></tr>
<tr><td style="padding:8px;font-weight:700">Etapa</td><td style="padding:8px">{etapa}</td></tr>
<tr><td style="padding:8px;background:#fdf0f0;font-weight:700">Job ID</td><td style="padding:8px;background:#fdf0f0"><code>{job_id}</code></td></tr>
</table>
<p style="margin-bottom:8px;font-weight:700">Error:</p>
<pre style="background:#f5f5f5;padding:12px;border-radius:4px;font-size:13px;overflow-x:auto;white-space:pre-wrap">{error}</pre>
<p style="margin-top:32px;font-size:12px;color:#b89a7a">Ethos Bios · hola@ethosbios.com</p>
</body></html>"""
    try:
        _send(key, admin_email, f"[Pipeline fallido] familia={familia_id} etapa={etapa}", html)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error enviando alerta admin: %s", exc)


def send_alerta_escalacion_humana(
    familia_id: str,
    nombre_integrante: str,
    motivo: str,
    violaciones: list[str],
    nombre_familia: str | None = None,
) -> None:
    """
    Alerta a admin cuando el quality_agent escala un capítulo a revisión humana.
    Se dispara en el momento de la escalación e incluye qué familia, qué integrante
    y qué se detectó (las alucinaciones específicas de Checklist A, o las violaciones
    editoriales de Checklist B agotado). Mismo transporte Postmark que el resto.
    """
    key = _get_key()
    if not key:
        return
    admin_email = os.environ.get("ADMIN_EMAIL", "hola@ethosbios.com")

    # Nombre legible de la familia (best-effort desde Firestore; cae a familia_id).
    if not nombre_familia:
        try:
            from pipeline.utils import firestore as fs
            fam = fs.get_familia(familia_id) or {}
            nombre_familia = fam.get("nombre") or familia_id
        except Exception:  # noqa: BLE001
            nombre_familia = familia_id

    es_checklist_a = motivo == "checklist_a"
    if es_checklist_a:
        titulo = "Alucinación detectada — revisión humana"
        tipo = "Checklist A (factual, bloqueante) — invenciones / mezcla de datos"
        detalle_label = "Se detectó (alucinaciones específicas):"
    else:
        titulo = "Escalación a revisión humana"
        tipo = "Checklist B (editorial) — agotó los reintentos sin pasar"
        detalle_label = "Se detectó (violaciones editoriales):"

    items = "".join(
        f'<li style="margin-bottom:6px">{_escape(str(v))}</li>'
        for v in (violaciones or [])
    ) or "<li>(sin detalle disponible en la evaluación)</li>"

    html = f"""<html><body style="font-family:sans-serif;color:#3d2b0a;max-width:560px;margin:0 auto;padding:24px">
<p style="font-family:Georgia,serif;font-size:22px;margin-bottom:16px">Ethos Bios</p>
<h2 style="font-weight:400;margin-bottom:8px;color:#c0392b">{titulo}</h2>
<table style="border-collapse:collapse;width:100%;margin-bottom:20px">
<tr><td style="padding:8px;background:#fdf0f0;font-weight:700;width:120px">Familia</td><td style="padding:8px;background:#fdf0f0">{_escape(str(nombre_familia))} <code style="color:#9a7b5a">({_escape(familia_id)})</code></td></tr>
<tr><td style="padding:8px;font-weight:700">Integrante</td><td style="padding:8px"><strong>{_escape(nombre_integrante)}</strong></td></tr>
<tr><td style="padding:8px;background:#fdf0f0;font-weight:700">Motivo</td><td style="padding:8px;background:#fdf0f0">{_escape(tipo)}</td></tr>
</table>
<p style="margin-bottom:8px;font-weight:700">{detalle_label}</p>
<ul style="background:#f5f5f5;padding:12px 12px 12px 30px;border-radius:4px;font-size:13px;line-height:1.5">{items}</ul>
<p style="margin-top:24px;font-size:13px;color:#9a7b5a">El capítulo se generó igual y quedó marcado para revisión humana en el dashboard.</p>
<p style="margin-top:32px;font-size:12px;color:#b89a7a">Ethos Bios · hola@ethosbios.com</p>
</body></html>"""

    subject = f"[Revisión humana] {nombre_integrante} — {nombre_familia}"
    if es_checklist_a:
        subject += " · alucinación"
    try:
        _send(key, admin_email, subject, html)
        logger.info("[quality] alerta de escalación enviada a %s (%s / %s)", admin_email, nombre_familia, nombre_integrante)
    except Exception as exc:  # noqa: BLE001
        logger.error("Error enviando alerta de escalación humana: %s", exc)


def send_generando(email_comprador: str, nombre_familia: str, familia_id: str) -> None:
    key = _get_key()
    if not key:
        return
    dashboard_url = 'https://ethosbios.com/mi-familia'
    html = f'''<html><body style="font-family:sans-serif;color:#3d2b0a;max-width:560px;margin:0 auto;padding:24px"><p style="font-family:Georgia,serif;font-size:22px;margin-bottom:16px">Ethos Bios</p><h2 style="font-weight:400;margin-bottom:8px">¡Todos grabaron!</h2><p style="color:#9a7b5a;margin-bottom:20px">Todos los integrantes de <strong>{nombre_familia}</strong> ya compartieron sus historias.</p><p style="margin-bottom:20px">Estamos creando tu libro familiar. El proceso tarda aproximadamente <strong>30 a 40 minutos</strong>. Te avisaremos por email cuando el PDF esté listo para descargar.</p><a href="{dashboard_url}" style="display:inline-block;background:#2C1A0E;color:#F5EDD8;padding:12px 24px;border-radius:8px;text-decoration:none;font-weight:700;font-size:14px">Seguir el progreso</a><p style="margin-top:32px;font-size:12px;color:#b89a7a">Ethos Bios · hola@ethosbios.com</p></body></html>'''
    try:
        _send(key, email_comprador, f'¡Todos grabaron! Tu libro está siendo creado — {nombre_familia}', html)
    except Exception as exc:  # noqa: BLE001
        logger.warning('Error enviando generando a %s: %s', email_comprador, exc)
