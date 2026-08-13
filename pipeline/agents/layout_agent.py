"""
Layout agent: genera el PDF del libro con las plantillas 6×9in (Jinja2 + WeasyPrint).
Estructura: Portada → [Apertura + Interior(s)] × N capítulos → Índice

Fuente de datos: recibe integrantes/relaciones como parámetros (agnóstico a la fuente).
El orquestador es responsable de cargarlos desde Firestore/GCS o donde corresponda.
"""

import os
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML

from pipeline.agents.editor_agent import BookManuscript

TEMPLATES_DIR = Path(__file__).parent.parent / "templates"
ASSETS_DIR = TEMPLATES_DIR / "assets"

# ── Sello de marca Ethos Bios ────────────────────────────────────────────────
# Dos assets de marca (dorado #C49B18):
#   · sello-completo.svg  → logo completo (árbol/libro en el centro). Apertura y
#                            cierre SIN QR. Su fondo crema se vuelve transparente.
#   · sello-vacio.png     → mismo sello con el centro vacío, para componer el QR
#                            real perfectamente centrado (cierre CON QR, 100%).
# De cada uno se pre-genera una versión transparente (PNG) que es lo que se
# embebe en el PDF: en runtime sólo se usa PIL (sin numpy/cairosvg).
SELLO_COMPLETO_SVG    = ASSETS_DIR / "sello-completo.svg"
SELLO_VACIO_PNG       = ASSETS_DIR / "sello-vacio.png"
SELLO_COMPLETO_TRANSP = ASSETS_DIR / "sello-completo_transp.png"  # apertura + cierre sin QR
SELLO_VACIO_TRANSP    = ASSETS_DIR / "sello-vacio_transp.png"     # base para componer QR

SELLO_GOLD  = (196, 155, 24)    # #C49B18 dorado de marca
CREAM_BG    = (251, 247, 240)   # #FBF7F0 fondo del SVG completo
WHITE_BG    = (255, 255, 255)   # fondo del PNG vacío
QR_DARK     = (90, 66, 38)      # #5A4226 marrón cálido para módulos del QR (contraste ~8:1)
QR_PANEL_BG = (245, 240, 232)   # #F5F0E8 fondo real de la página (var --paper)
# Centro geométrico del sello vacío (1024×1024) y lado del QR compuesto.
VACIO_CENTER  = (506, 504)
QR_BOX_TARGET = 360

# Caracteres por página interior (ajuste empírico para A5, 11px Mulish)
CHARS_PER_PAGE = 2400

# ── Presupuesto de altura de una cita ────────────────────────────────────────
# El presupuesto de página cuenta "caracteres" (~0.225px de alto por char de
# párrafo). Una cita se renderiza en itálica 25px (blockquote .pull.quote) flanqueada
# por dos separadores: el trío ocupa 154–352px REALES según su largo, muchísimo más
# que los ~81px que sugería su conteo de caracteres. Si el presupuesto la subestima,
# la página supera los 794px fijos y el `overflow:hidden` recorta el texto (ver
# Briefing #48: cita cortada a mitad de palabra). Por eso el trío recibe un costo
# equivalente calibrado empíricamente (WeasyPrint) contra la densidad de párrafo.
CITA_CHARS_PER_LINE = 30      # a 25px itálica, max-width 380px
CITA_COST_BASE      = 530     # overhead fijo del trío sep+cita+sep (~120px reales)
CITA_COST_PER_LINE  = 107     # ~24px reales por línea de cita


def _costo_cita(texto: str) -> int:
    """Costo de altura (en caracteres-equivalentes) del trío separador+cita+separador."""
    import math as _math
    lineas = max(1, _math.ceil(len(texto) / CITA_CHARS_PER_LINE))
    return CITA_COST_BASE + lineas * CITA_COST_PER_LINE


# ── Sello de marca: transparencia + composición del QR ────────────────────────

def _unmix_gold(img, bg):
    """
    Devuelve un PIL.Image RGBA con sólo el dorado del sello y el fondo `bg`
    convertido a transparente. La cobertura de dorado se estima por 'unmixing'
    lineal (dorado ↔ fondo), dejando bordes anti-aliased con alpha parcial que
    componen bien sobre cualquier papel. Requiere numpy (sólo en dev/generación).
    """
    import numpy as np
    from PIL import Image

    arr = np.asarray(img.convert("RGB"), dtype=float)
    gold = np.array(SELLO_GOLD, dtype=float)
    c = np.array(bg, dtype=float)
    d = gold - c
    t = np.clip(((arr - c) @ d) / (d @ d), 0.0, 1.0)
    out = np.zeros((*arr.shape[:2], 4), dtype=float)
    out[..., 0], out[..., 1], out[..., 2] = SELLO_GOLD
    out[..., 3] = t * 255.0
    return Image.fromarray(out.astype("uint8"), "RGBA")


def _stale(dst: Path, src: Path) -> bool:
    return not dst.exists() or dst.stat().st_mtime < src.stat().st_mtime


def ensure_derived_seals() -> None:
    """
    Pre-genera (en dev) los PNG transparentes derivados de los assets de marca.
    En runtime (Cloud Run) se usan los PNG ya committeados; si faltan numpy o
    cairosvg, no falla: se asume que los derivados ya están en el repo.
    """
    try:
        from PIL import Image
        if _stale(SELLO_VACIO_TRANSP, SELLO_VACIO_PNG):
            _unmix_gold(Image.open(SELLO_VACIO_PNG), WHITE_BG).save(SELLO_VACIO_TRANSP)
        if _stale(SELLO_COMPLETO_TRANSP, SELLO_COMPLETO_SVG):
            import io
            import cairosvg
            png = cairosvg.svg2png(url=str(SELLO_COMPLETO_SVG), output_width=1024, output_height=1024)
            _unmix_gold(Image.open(io.BytesIO(png)), CREAM_BG).save(SELLO_COMPLETO_TRANSP)
    except Exception as e:  # noqa: BLE001
        print(f"[layout] assets derivados no regenerados (se usan los committeados): {e}")


def _sello_con_qr_b64(url: str) -> str:
    """
    Compone el QR real (módulos #3D3226 sobre panel #F5F0E8) perfectamente
    centrado en el círculo vacío del sello y devuelve el PNG en base64 (100%).
    Sólo usa PIL + qrcode (apto para runtime).
    """
    import base64
    import io
    import qrcode
    from PIL import Image

    # QR en tamaño de módulo nativo (sin resize) para módulos nítidos.
    qr = qrcode.QRCode(error_correction=qrcode.constants.ERROR_CORRECT_M, border=4)
    qr.add_data(url)
    qr.make(fit=True)
    modules = len(qr.get_matrix())  # incluye quiet zone (border)
    box = max(4, round(QR_BOX_TARGET / modules))
    qr.box_size = box
    qimg = qr.make_image(fill_color=QR_DARK, back_color=QR_PANEL_BG).convert("RGBA")

    seal = Image.open(SELLO_VACIO_TRANSP).convert("RGBA")
    cx, cy = VACIO_CENTER
    qw, qh = qimg.size
    seal.alpha_composite(qimg, (cx - qw // 2, cy - qh // 2))

    buf = io.BytesIO()
    seal.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode()


# ── Helpers de contenido ──────────────────────────────────────────────────────

_ROMANOS = [
    (1000, "M"), (900, "CM"), (500, "D"), (400, "CD"), (100, "C"), (90, "XC"),
    (50, "L"), (40, "XL"), (10, "X"), (9, "IX"), (5, "V"), (4, "IV"), (1, "I"),
]


def _a_romano(n: int) -> str:
    """Convierte un entero positivo a numeral romano (1 → I, 4 → IV, 21 → XXI)."""
    if n <= 0:
        return str(n)
    out = []
    for val, sym in _ROMANOS:
        while n >= val:
            out.append(sym)
            n -= val
    return "".join(out)


def _elegir_cita(citas_directas: list[str], fallback_texto: str) -> str:
    """
    Elige la cita de apertura desde las citas_directas del perfil de voz (voice_agent):
    la más corta y limpia; si no hay, cae al heurístico sobre el capítulo.
    """
    candidatas = [c.strip().strip('"«»*').strip() for c in (citas_directas or []) if c and c.strip()]
    candidatas = [c for c in candidatas if len(c) >= 20]
    if candidatas:
        cita = min(candidatas, key=len)
        # Si es muy larga, recortar a la primera oración.
        if len(cita) > 180:
            m = re.match(r"^(.{40,180}?[.!?…])(\s|$)", cita)
            if m:
                cita = m.group(1)
        return cita.rstrip(" .,;:").strip()
    return _extraer_frase(fallback_texto)

def _extraer_frase(texto: str) -> str:
    """Extrae la primera cita (—…) o la primera oración del texto."""
    for line in texto.split("\n\n"):
        line = line.strip()
        if line.startswith("—"):
            return line.lstrip("—").strip().rstrip(".")
    # Primera oración significativa
    primera = texto.strip().split("\n\n")[0]
    m = re.match(r"^(.{20,120}?[.!?])", primera)
    return m.group(1).rstrip() if m else primera[:80]


def _md_a_html(texto: str) -> str:
    """Convierte *texto* → <em>texto</em> para citas directas de transcripción."""
    return re.sub(r"\*([^*]+)\*", r"<em>\1</em>", texto)


def _strip_md_headers(texto: str) -> str:
    """Elimina headers markdown (# Título, ## Subtítulo) del texto."""
    lines = []
    for line in texto.split("\n"):
        if re.match(r"^#{1,6}\s+", line):
            continue
        lines.append(line)
    return "\n".join(lines)


def _texto_a_bloques(
    texto: str,
    foto_info: Optional[dict] = None,
) -> list[list[dict]]:
    """
    Convierte el texto de un capítulo en páginas de bloques.
    Cada página es una lista de dicts con .tipo = parrafo|separador|cita|foto.
    """
    texto = _strip_md_headers(texto)
    raw = [p.strip() for p in texto.split("\n\n") if p.strip()]

    all_blocks: list[dict] = []
    for i, p in enumerate(raw):
        if p.startswith("—"):
            cita_txt = p.lstrip("—").strip()
            # El costo real del trío completo se atribuye al separador LÍDER: así el
            # corte de página se dispara ANTES del trío y lo mantiene junto en la
            # misma página (cita + sus dos separadores), sin recortes por overflow.
            all_blocks.append({"tipo": "separador", "cost": _costo_cita(cita_txt)})
            all_blocks.append({"tipo": "cita", "texto": _md_a_html(cita_txt), "cost": 0})
            all_blocks.append({"tipo": "separador", "cost": 0})
        else:
            all_blocks.append({
                "tipo": "parrafo",
                "texto": _md_a_html(p),
                "dropcap": (i == 0),
                "serif": False,
                "cost": len(p),
            })

    # Insertar foto después del cuarto bloque de párrafo
    if foto_info:
        parrafo_count = 0
        insert_idx = len(all_blocks)
        for idx, b in enumerate(all_blocks):
            if b["tipo"] == "parrafo":
                parrafo_count += 1
                if parrafo_count == 4:
                    insert_idx = idx + 1
                    break
        all_blocks.insert(insert_idx, foto_info)

    # Dividir en páginas por presupuesto de caracteres
    pages: list[list[dict]] = []
    current_page: list[dict] = []
    char_count = 0

    for b in all_blocks:
        if b["tipo"] == "foto":
            cost = CHARS_PER_PAGE // 2
        else:
            # parrafo/separador/cita ya traen su costo calibrado desde all_blocks;
            # el trío de cita concentra su altura real en el separador líder.
            cost = b.get("cost", 0)

        if current_page and (char_count + cost) > CHARS_PER_PAGE:
            pages.append(current_page)
            current_page = []
            char_count = 0

        current_page.append(b)
        char_count += cost

    if current_page:
        pages.append(current_page)

    return pages or [[]]


def _foto_local(nombre: str, fotos: dict[str, str]) -> Optional[dict]:
    path = fotos.get(nombre)
    if path and os.path.exists(path):
        return {
            "tipo": "foto",
            "img": f"file://{path}",
            "alt_ph": nombre.lower(),
            "caption": nombre.lower(),
            "rot": -2.2,
        }
    return None


# ── Render principal ──────────────────────────────────────────────────────────

def _render_libro(
    manuscript: BookManuscript,
    nombre_familia: str,
    fotos: dict[str, str],
    integrantes: list[dict],
    relaciones: list[dict],
    qr_data: dict[str, str] | None = None,
    citas_data: dict[str, list[str]] | None = None,
) -> str:
    """Ensambla el HTML completo del libro concatenando todas las páginas."""
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))
    citas_data = citas_data or {}

    apellidos = nombre_familia.replace("Familia", "").strip().split("·")
    apellido_1 = apellidos[0].strip() if apellidos else nombre_familia
    apellido_2 = apellidos[1].strip() if len(apellidos) > 1 else ""
    apellido_pie = " · ".join(a for a in (apellido_1, apellido_2) if a) or nombre_familia

    folio_counter = [1]

    def next_folio() -> str:
        f = str(folio_counter[0])
        folio_counter[0] += 1
        return f

    partes: list[str] = []

    tmpl_seccion    = env.get_template("05-seccion.html")
    tmpl_transicion = env.get_template("06-transicion.html")

    # ── 01 · Portada ──
    tmpl_portada = env.get_template("01-portada.html")
    libro_ctx = {
        "apellido_1": apellido_1,
        "apellido_2": apellido_2,
        "kicker": "El libro de la familia",
        "lede": (
            "Cada voz, un capítulo. "
            "Cada capítulo, una rama nueva en el árbol de los que somos."
        ),
        "anio_label": f"MEMORIAS · {datetime.now().year}",
        "firma": nombre_familia,
        "portada_img": None,
        "portada_cap": "la familia, hoy",
    }
    partes.append(_extract_body(tmpl_portada.render(libro=libro_ctx)))

    # ── Prólogo ──
    if manuscript.prologo:
        for i, pag_bloques in enumerate(_texto_a_bloques(manuscript.prologo)):
            ctx = {
                "tipo": "Prólogo",
                "primer_pagina": (i == 0),
                "folio": next_folio(),
                "bloques": pag_bloques,
            }
            partes.append(_extract_body(tmpl_seccion.render(pagina=ctx)))

    # ── Capítulos ──
    for idx, nombre in enumerate(manuscript.orden, start=1):
        capitulo_texto = manuscript.capitulos.get(nombre, "")
        if not capitulo_texto:
            continue

        cita = _elegir_cita(citas_data.get(nombre, []), capitulo_texto)
        folio_apertura = next_folio()

        # 02 · Apertura de capítulo (sello a baja opacidad + cita real)
        tmpl_apertura = env.get_template("02-apertura.html")
        cap_ctx = {
            "numero": idx,
            "romano": _a_romano(idx),
            "nombre": nombre,
            "epigrafe": cita,
            "folio": folio_apertura,
        }
        partes.append(_extract_body(tmpl_apertura.render(capitulo=cap_ctx)))

        # 03 · Páginas interiores
        tmpl_interior = env.get_template("03-interior.html")
        foto_info = _foto_local(nombre, fotos)
        paginas_bloques = _texto_a_bloques(capitulo_texto, foto_info)

        for pagina_bloques in paginas_bloques:
            pagina_ctx = {
                "nombre": nombre,
                "numero": idx,
                "folio": next_folio(),
                "bloques": pagina_bloques,
            }
            partes.append(_extract_body(tmpl_interior.render(pagina=pagina_ctx)))

        # 07 · Cierre de capítulo — sello de marca. Variante CON QR si la persona
        #      grabó su voz (voz_token → seal_qr_b64); si no, sólo el sello.
        tmpl_cierre = env.get_template("07-cierre.html")
        seal_qr_b64 = (qr_data or {}).get(nombre, "")
        cierre_ctx = {
            "con_qr": bool(seal_qr_b64),
            "seal_qr_b64": seal_qr_b64,
            "apellido": apellido_pie,
            "folio": next_folio(),
        }
        partes.append(_extract_body(tmpl_cierre.render(cierre=cierre_ctx)))

        # 06 · Transición hacia el siguiente capítulo
        if idx < len(manuscript.orden):
            siguiente = manuscript.orden[idx]
            key = f"{nombre}→{siguiente}"
            trans_texto = manuscript.transiciones.get(key, "")
            if trans_texto:
                trans_html = _md_a_html(trans_texto)
                trans_parrafos = "\n".join(
                    f"<p>{p.strip()}</p>"
                    for p in trans_html.split("\n\n") if p.strip()
                )
                partes.append(_extract_body(tmpl_transicion.render(transicion={
                    "texto": trans_parrafos,
                    "folio": next_folio(),
                })))

    # ── Epílogo ──
    if manuscript.epilogo:
        for i, pag_bloques in enumerate(_texto_a_bloques(manuscript.epilogo)):
            ctx = {
                "tipo": "Epílogo",
                "primer_pagina": (i == 0),
                "folio": next_folio(),
                "bloques": pag_bloques,
            }
            partes.append(_extract_body(tmpl_seccion.render(pagina=ctx)))

    # ── 04 · Índice ──
    tmpl_indice = env.get_template("04-indice.html")
    personas_indice = []
    for i, nombre in enumerate(manuscript.orden, start=1):
        capitulo_texto = manuscript.capitulos.get(nombre, "")
        frase = _extraer_frase(capitulo_texto) if capitulo_texto else ""
        personas_indice.append({
            "numero": i,
            "nombre": nombre,
            "frase": frase,
            "destacar": False,
        })

    indice_ctx = {
        "kicker": "Quiénes somos",
        "titulo": "Los protagonistas",
        "folio": next_folio(),
        "personas": personas_indice,
    }
    partes.append(_extract_body(tmpl_indice.render(indice=indice_ctx)))

    css_path = TEMPLATES_DIR / "estilos.css"
    body_html = "\n".join(partes)
    return f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8"/>
<link rel="stylesheet" href="{css_path}"/>
</head>
<body>
{body_html}
</body>
</html>"""


def _extract_body(page_html: str) -> str:
    """Extrae el contenido del <body> de un fragmento HTML de plantilla."""
    m = re.search(r"<body>(.*?)</body>", page_html, re.DOTALL)
    return m.group(1).strip() if m else page_html


# ── Entrada pública ───────────────────────────────────────────────────────────

def run(
    manuscript: BookManuscript,
    personas_meta: list[dict],
    nombre_familia: str = "",
    output_path: Optional[str] = None,
    todos_integrantes: Optional[list[dict]] = None,
    relaciones: Optional[list[dict]] = None,
) -> str:
    """
    Genera el PDF y devuelve su ruta.

    personas_meta:     [{nombre, fecha_nac, rol, ...}] — personas con capítulo
    todos_integrantes: lista completa de integrantes de la familia (para el árbol)
    relaciones:        [{persona_a, relacion, persona_b}] — cargadas desde Firestore/GCS
    """
    # Datos de familia para el árbol
    integrantes = todos_integrantes or personas_meta
    rels = relaciones or []

    if not rels:
        # Compatibilidad hacia atrás: intentar cargar desde sheets si está disponible
        try:
            from pipeline.utils import sheets as _sheets
            rels = _sheets.get_familia_relaciones()
        except Exception:
            pass

    # Descargar fotos (de GCS URL o ruta local)
    fotos: dict[str, str] = {}
    for p in personas_meta:
        nombre = p["nombre"]
        foto_url = p.get("foto_url") or p.get("foto")
        if foto_url:
            dest = f"/tmp/foto_{re.sub(r'[^a-zA-Z0-9]', '_', nombre)}.jpg"
            try:
                if foto_url.startswith("gs://"):
                    _download_gcs(foto_url, dest)
                elif foto_url.startswith("http"):
                    _download_http(foto_url, dest)
                else:
                    dest = foto_url  # ruta local directa
                fotos[nombre] = dest
            except Exception as e:
                print(f"[layout] No se pudo descargar foto de {nombre}: {e}")
        elif not foto_url:
            # compatibilidad sheets
            try:
                from pipeline.utils import sheets as _sheets
                url = _sheets.get_foto_url(nombre)
                if url:
                    dest = f"/tmp/foto_{re.sub(r'[^a-zA-Z0-9]', '_', nombre)}.jpg"
                    _sheets.download_drive_file(url, dest)
                    fotos[nombre] = dest
            except Exception:
                pass

    # Asegurar los sellos transparentes derivados (apertura / cierre con y sin QR).
    ensure_derived_seals()

    # Sello con QR de voz compuesto, por integrante que grabó su voz (Pregunta 18).
    qr_data: dict[str, str] = {}
    citas_data: dict[str, list[str]] = {}
    for p in personas_meta:
        nombre = p["nombre"]
        perfil = p.get("perfil_voz") or {}
        citas_data[nombre] = p.get("citas_directas") or perfil.get("citas_directas") or []
        voz_token = p.get("voz_token", "")
        if voz_token:
            try:
                qr_url = f"{os.environ.get('BASE_URL', 'https://www.ethosbios.com')}/voz/{voz_token}"
                qr_data[nombre] = _sello_con_qr_b64(qr_url)
            except Exception as e:
                print(f"[layout] No se pudo componer sello+QR para {nombre}: {e}")

    html_content = _render_libro(
        manuscript, nombre_familia, fotos, integrantes, rels,
        qr_data=qr_data, citas_data=citas_data,
    )

    if output_path is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"/tmp/libro_{ts}.pdf"

    HTML(string=html_content, base_url=str(TEMPLATES_DIR)).write_pdf(output_path)
    return output_path


def _download_gcs(gcs_uri: str, dest: str) -> None:
    from google.cloud import storage
    # gs://bucket/path
    parts = gcs_uri[5:].split("/", 1)
    bucket_name, blob_name = parts[0], parts[1]
    client = storage.Client()
    client.bucket(bucket_name).blob(blob_name).download_to_filename(dest)


def _download_http(url: str, dest: str) -> None:
    import urllib.request
    urllib.request.urlretrieve(url, dest)
