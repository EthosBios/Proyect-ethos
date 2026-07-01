"""
Genera un capítulo narrativo de 3200–3800 palabras por persona.
"""

import anthropic

from pipeline.utils import sheets
from pipeline.utils.retry import call_with_retry

MODEL = "claude-opus-4-7"

_SYSTEM = """\
Sos un escritor literario especializado en memorias familiares y narrativa oral latinoamericana.
Tu trabajo es transformar transcripciones y perfiles de voz en capítulos de libro con prosa literaria,
manteniendo la autenticidad de cada persona.
"""

_PROMPT_TEMPLATE = """\
Escribí un capítulo narrativo de entre 3200 y 3800 palabras sobre {nombre}.

CONTEXTO FAMILIAR:
- Rol en la familia: {rol}
- Estado: {estado}
- Cónyuge/s: {conyuges}
- Hijos: {hijos}
- Padres: {padres}
- Hermanos (inferidos): {hermanos}

PERFIL DE VOZ:
- Muletillas: {muletillas}
- Frases propias: {frases_propias}
- Registro: {registro}
- Detalles sensoriales: {detalles_sensoriales}
- Tono: {tono}

TRANSCRIPCIÓN COMPLETA:
{transcripcion}

ARCO NARRATIVO SUGERIDO (no obligatorio, podés adaptarlo):
1. Apertura con imagen sensorial del lugar de origen
2. La infancia y quiénes lo/la formaron
3. Las elecciones que marcaron el camino
4. La vida que construyó — trabajo, familia, vínculos
5. Lo que sabe ahora que no sabía antes

REGLAS DE ESCRITURA:
- La voz del protagonista va en cursiva cuando aparece directamente, NUNCA entre comillas
- Integrá 2 o 3 de sus frases propias de forma orgánica, no forzada: {frases_propias_lista}
- No uses estas palabras: memorable, invaluable, legado, tesoro, entrañable, inmortal, huella
- Prosa fluida, párrafos de longitud variada
- Podés abrir con una cita directa o una imagen antes del nombre del protagonista
- El capítulo debe poder leerse solo, sin conocer a la persona previamente
- Usá el rol familiar ({rol}) como lente narrativo: cómo lo/la ven los demás, qué lugar ocupa en la trama familiar

Devolvé SOLO el texto del capítulo. Sin título. Sin notas. Sin explicaciones.
"""

def generar_capitulo(client: anthropic.Anthropic, persona: dict) -> str:
    """
    persona dict esperado:
      nombre, perfil_voz (dict con los 7 campos), transcripcion,
      familia_ctx (optional dict from sheets.build_family_context)
    """
    nombre = persona["nombre"]
    perfil = persona.get("perfil_voz", {})
    transcripcion = persona.get("transcripcion", "")
    fctx = persona.get("familia_ctx", {})

    frases_propias = perfil.get("frases_propias", [])
    frases_propias_lista = ", ".join(f'"{f}"' for f in frases_propias[:5]) if frases_propias else "ninguna registrada"

    def _lista(items): return ", ".join(items) if items else "—"

    estado = "vive" if fctx.get("vive", True) else f"falleció el {fctx.get('fecha_fallec', 'fecha desconocida')}"

    if len(transcripcion) > 12000:
        print(f"[chapter_agent] Transcripción de {nombre} truncada: {len(transcripcion)} → 12000 chars")
    transcripcion_input = transcripcion[:12000]

    prompt = _PROMPT_TEMPLATE.format(
        nombre=nombre,
        rol=fctx.get("rol", "no especificado"),
        estado=estado,
        conyuges=_lista(fctx.get("conyuges", [])),
        hijos=_lista(fctx.get("hijos", [])),
        padres=_lista(fctx.get("padres", [])),
        hermanos=_lista(fctx.get("hermanos", [])),
        muletillas=", ".join(perfil.get("muletillas", [])) or "no registradas",
        frases_propias=frases_propias_lista,
        registro=perfil.get("registro", "no registrado"),
        detalles_sensoriales=", ".join(perfil.get("detalles_sensoriales", [])) or "no registrados",
        tono=perfil.get("tono", "no registrado"),
        transcripcion=transcripcion_input,
        frases_propias_lista=frases_propias_lista,
    )

    message = call_with_retry(
        client.messages.create,
        model=MODEL,
        max_tokens=7000,
        system=_SYSTEM,
        messages=[{"role": "user", "content": prompt}],
        label=f"claude/capitulo/{nombre}",
    )

    capitulo = message.content[0].text.strip()

    MIN_WORDS = 3200
    MAX_ATTEMPTS = 3
    attempt = 1

    while len(capitulo.split()) < MIN_WORDS and attempt < MAX_ATTEMPTS:
        attempt += 1
        words = len(capitulo.split())
        refuerzo = (
            f"\n\nATENCIÓN: el capítulo que generaste tiene {words} palabras, "
            f"por debajo del mínimo de {MIN_WORDS}. "
            "Reescribilo completo, expandiendo cada etapa de vida con más detalle narrativo, "
            "más anécdotas concretas de la transcripción, más color sensorial. "
            "NO resumas — desarrollá. El resultado DEBE tener entre 3.200 y 3.800 palabras."
        )
        message = call_with_retry(
            client.messages.create,
            model=MODEL,
            max_tokens=7000,
            system=_SYSTEM,
            messages=[{"role": "user", "content": prompt + refuerzo}],
            label=f"claude/capitulo/{nombre}/retry{attempt}",
        )
        capitulo = message.content[0].text.strip()

    words_final = len(capitulo.split())
    if words_final < MIN_WORDS:
        print(
            f"[chapter_agent] AVISO: capítulo de {nombre} quedó con {words_final} palabras "
            f"tras {attempt} intentos (mínimo {MIN_WORDS}). Aceptado igual.",
            flush=True,
        )

    sheets.save_chapter(nombre, capitulo)
    return capitulo


def run(nombres: list[str]) -> dict[str, str]:
    """Standalone: generate chapters for each nombre. Returns {nombre: capitulo_str}."""
    client = anthropic.Anthropic(timeout=600.0)
    results = {}

    try:
        integrantes = sheets.get_familia_integrantes()
        relaciones = sheets.get_familia_relaciones()
    except Exception as e:
        print(f"[chapter_agent] No se pudieron cargar datos de familia: {e}")
        integrantes, relaciones = [], []

    for nombre in nombres:
        try:
            profile = sheets.get_profile(nombre)
            if not profile:
                raise ValueError(f"No hay perfil guardado para {nombre}")

            familia_ctx = sheets.build_family_context(nombre, integrantes, relaciones)

            persona = {
                "nombre": nombre,
                "perfil_voz": profile.get("perfil_voz", {}),
                "transcripcion": profile.get("transcripcion", ""),
                "familia_ctx": familia_ctx,
            }

            results[nombre] = generar_capitulo(client, persona)

        except Exception as e:
            print(f"[chapter_agent] Error con {nombre}: {e}")
            results[nombre] = f"ERROR: {e}"

    return results
