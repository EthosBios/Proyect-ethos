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

# ── Constantes del árbol genealógico ─────────────────────────────────────────
R_BY_GEN = [30, 22, 16, 13, 11]
SLOT      = 58
SIDE_PAD  = 46
TOP_PAD   = 46
BOT_PAD   = 86
VB_HEIGHT = 420
COL_BROWN     = "#7B5E3A"
COL_SAGE_DEEP = "#6C7A59"
COL_TINT      = "#B69A72"

# Caracteres por página interior (ajuste empírico para A5, 11px Mulish)
CHARS_PER_PAGE = 2400


# ── Árbol genealógico SVG ─────────────────────────────────────────────────────

def layout_family(root: dict) -> dict:
    """
    Genera las coordenadas SVG del árbol a partir de la estructura de unidades.

    root = {
      "people": [{"i": "I", "written": True, "current": False}, ...],
      "children": [ <unidad>, ... ]
    }

    Retorna: {"viewBox": "0 0 W H", "ramas": [...], "hojas": [...], "nodos": [...]}
    """
    max_gen = 0
    units: list[dict] = []

    def walk(u: dict, g: int) -> None:
        nonlocal max_gen
        u["_g"] = g
        max_gen = max(max_gen, g)
        units.append(u)
        for c in u.get("children", []):
            walk(c, g + 1)

    walk(root, 0)

    cursor = {"v": 0}

    def place(u: dict) -> None:
        ch = u.get("children", [])
        if not ch:
            u["_x"] = cursor["v"]
            cursor["v"] += 1
        else:
            for c in ch:
                place(c)
            u["_x"] = (ch[0]["_x"] + ch[-1]["_x"]) / 2

    place(root)
    leaves = max(1, cursor["v"])

    vb_w    = SIDE_PAD * 2 + (leaves - 1) * SLOT
    gen_gap = (VB_HEIGHT - TOP_PAD - BOT_PAD) / max(1, max_gen)
    X = lambda u: SIDE_PAD + u["_x"] * SLOT
    if max_gen == 0:
        Y = lambda u: int(VB_HEIGHT * 0.50)
    else:
        Y = lambda u: TOP_PAD + (max_gen - u["_g"]) * gen_gap

    ramas: list[dict] = []
    hojas: list[dict] = []
    nodos: list[dict] = []

    # tronco vertical
    rx = X(root)
    ry = Y(root)
    r0 = R_BY_GEN[0]
    ramas.append({
        "d": (f"M{rx:.1f},{VB_HEIGHT} C{rx:.1f},{VB_HEIGHT-40} "
              f"{rx:.1f},{ry+r0+30:.1f} {rx:.1f},{ry+r0:.1f}"),
        "w": 3.2, "color": COL_BROWN, "op": 1,
    })

    for u in units:
        ux, uy = X(u), Y(u)
        r = R_BY_GEN[u["_g"]] if u["_g"] < len(R_BY_GEN) else 11

        for c in u.get("children", []):
            cx, cy = X(c), Y(c)
            cr = R_BY_GEN[c["_g"]] if c["_g"] < len(R_BY_GEN) else 11
            top_y = uy - r
            mid_y = (top_y + (cy + cr)) / 2
            w   = max(1.4, 2.8 - u["_g"] * 0.5)
            col = COL_SAGE_DEEP if u["_g"] >= 1 else COL_BROWN
            op  = 0.85 if u["_g"] >= 1 else 1.0
            ramas.append({
                "d": (f"M{ux:.1f},{top_y:.1f} C{ux:.1f},{top_y-gen_gap*0.4:.1f} "
                      f"{cx:.1f},{mid_y+4:.1f} {cx:.1f},{cy+cr:.1f}"),
                "w": w, "color": col, "op": op,
            })
            lx  = (ux + cx) / 2 + (-4 if cx < ux else 4)
            rot = -50 if cx < ux else 50
            s   = max(0.6, 0.9 - u["_g"] * 0.12)
            hojas.append({"x": lx, "y": mid_y, "rot": rot, "s": s, "deep": (u["_g"] % 2 == 0)})

        ppl = u.get("people", [])
        if len(ppl) > 1:
            ramas.append({
                "d": f"M{ux-(r+7):.1f},{uy} L{ux+(r+7):.1f},{uy}",
                "w": 1.4, "color": COL_TINT, "op": 0.7,
            })

        for i, p in enumerate(ppl):
            px = (ux - (r + 7) if i == 0 else ux + (r + 7)) if len(ppl) > 1 else ux
            nodos.append({
                "x": px, "y": uy, "r": r,
                "inicial": p["i"],
                "written": p.get("written", True),
                "current": bool(p.get("current", False)),
            })

    return {
        "viewBox": f"0 0 {int(vb_w)} {VB_HEIGHT}",
        "ramas": ramas,
        "hojas": hojas,
        "nodos": nodos,
    }


def build_family_tree(
    integrantes: list[dict],
    relaciones: list[dict],
    capitulo_actual: str,
    nombres_escritos: Optional[list[str]] = None,
) -> dict:
    """
    Construye el root dict para layout_family() a partir de los datos del sistema.

    integrantes: [{nombre, ...}]
    relaciones:  [{persona_a, relacion, persona_b}]
                 relacion ∈ {padre, madre, cónyuge, conyuge, esposo, esposa}
    capitulo_actual: nombre de la persona cuyo capítulo se está abriendo (current=True)
    nombres_escritos: nombres con capítulo escrito (written=True); por defecto todos
    """
    escrito_set = set(nombres_escritos) if nombres_escritos else {p["nombre"] for p in integrantes}
    nombres_set  = {p["nombre"] for p in integrantes}

    conyuges_map: dict[str, set[str]] = {}
    hijos_map:    dict[str, list[str]] = {}
    padres_map:   dict[str, set[str]] = {}

    for r in relaciones:
        a   = r["persona_a"]
        rel = r["relacion"].lower()
        # normalizar tilde
        rel = rel.replace("ó", "o")   # ó → o
        b   = r["persona_b"]
        if rel in ("conyuge", "esposo", "esposa", "pareja"):
            conyuges_map.setdefault(a, set()).add(b)
            conyuges_map.setdefault(b, set()).add(a)
        elif rel in ("padre", "madre", "progenitor"):
            if b not in hijos_map.get(a, []):
                hijos_map.setdefault(a, []).append(b)
            padres_map.setdefault(b, set()).add(a)

    visited: set[frozenset] = set()

    def make_unit(names: list[str]) -> Optional[dict]:
        key = frozenset(names)
        if key in visited:
            return None
        visited.add(key)

        # Solo mostrar en el nodo a quienes tienen capítulo propio o son la persona actual
        visible = [n for n in names if n in escrito_set or n == capitulo_actual]
        if not visible:
            # Si ninguno tiene capítulo, mostrar solo el primero como nodo gris
            visible = names[:1]

        people = [
            {
                "i": n[0].upper(),
                "written": n in escrito_set,
                "current": n == capitulo_actual,
            }
            for n in visible
        ]

        # Hijos: unión de hijos de todas las personas de la unidad
        all_children: list[str] = []
        seen_children: set[str] = set()
        for n in names:
            for hijo in hijos_map.get(n, []):
                if hijo not in seen_children and hijo in nombres_set:
                    seen_children.add(hijo)
                    all_children.append(hijo)

        child_units: list[dict] = []
        processed: set[str] = set()
        for hijo in all_children:
            if hijo in processed:
                continue
            processed.add(hijo)
            # Buscar cónyuge del hijo que también sea integrante
            spouse: Optional[str] = None
            for c in conyuges_map.get(hijo, set()):
                if c in nombres_set and c not in processed:
                    # Solo incluir cónyuge si tiene capítulo o es la persona actual
                    if c in escrito_set or c == capitulo_actual:
                        spouse = c
                        break
            if spouse:
                processed.add(spouse)
                unit = make_unit([hijo, spouse])
            else:
                unit = make_unit([hijo])
            if unit:
                child_units.append(unit)

        return {"people": people, "children": child_units}

    # Raíces: personas sin padres en el dataset
    root_candidates = [p["nombre"] for p in integrantes if p["nombre"] not in padres_map]
    if not root_candidates:
        root_candidates = [p["nombre"] for p in integrantes]

    # Buscar pareja raíz (cónyuges sin padres)
    for i, a in enumerate(root_candidates):
        for b in root_candidates[i + 1:]:
            if b in conyuges_map.get(a, set()):
                result = make_unit([a, b])
                return result if result else {"people": [], "children": []}

    # Raíz individual
    if root_candidates:
        result = make_unit([root_candidates[0]])
        return result if result else {"people": [], "children": []}

    return {"people": [], "children": []}


# ── Helpers de contenido ──────────────────────────────────────────────────────

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
            all_blocks.append({"tipo": "separador"})
            all_blocks.append({"tipo": "cita", "texto": _md_a_html(p.lstrip("—").strip())})
            all_blocks.append({"tipo": "separador"})
        else:
            all_blocks.append({
                "tipo": "parrafo",
                "texto": _md_a_html(p),
                "dropcap": (i == 0),
                "serif": False,
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
        if b["tipo"] == "parrafo":
            cost = len(b["texto"])
        elif b["tipo"] == "foto":
            cost = CHARS_PER_PAGE // 2
        elif b["tipo"] in ("separador", "cita"):
            cost = 120
        else:
            cost = 0

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
        slug = re.sub(r"[^a-zA-Z0-9_]", "", nombre.lower())
        return {
            "tipo": "foto",
            "img": f"file://{path}",
            "alt_ph": nombre.lower(),
            "caption": nombre.lower(),
            "rot": -2.2,
        }
    return {
        "tipo": "foto",
        "img": None,
        "alt_ph": nombre.lower(),
        "caption": nombre.lower(),
        "rot": -2.2,
    }


# ── Render principal ──────────────────────────────────────────────────────────

def _render_libro(
    manuscript: BookManuscript,
    nombre_familia: str,
    fotos: dict[str, str],
    integrantes: list[dict],
    relaciones: list[dict],
) -> str:
    """Ensambla el HTML completo del libro concatenando todas las páginas."""
    env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))

    apellidos = nombre_familia.replace("Familia", "").strip().split("·")
    apellido_1 = apellidos[0].strip() if apellidos else nombre_familia
    apellido_2 = apellidos[1].strip() if len(apellidos) > 1 else ""

    nombres_escritos = manuscript.orden
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

        epigrafe = _extraer_frase(capitulo_texto)
        folio_apertura = next_folio()

        # 02 · Apertura
        tmpl_apertura = env.get_template("02-apertura.html")
        cap_ctx = {
            "numero": idx,
            "nombre": nombre,
            "epigrafe": epigrafe,
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


def _arbol_vacio() -> dict:
    return {"viewBox": "0 0 320 400", "ramas": [], "hojas": [], "nodos": []}


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

    html_content = _render_libro(manuscript, nombre_familia, fotos, integrantes, rels)

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
