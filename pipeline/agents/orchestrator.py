"""
Orchestrator: coordina los 5 agentes del pipeline de extremo a extremo.
"""

import logging
from dataclasses import dataclass, field

from pipeline.agents import (
    chapter_agent,
    editor_agent,
    layout_agent,
    transcriber,
    voice_agent,
)
from pipeline.agents.editor_agent import BookManuscript
from pipeline.utils import sheets
from pipeline.utils.costos import CostAccumulator

logger = logging.getLogger(__name__)

STEPS = ["transcriber", "voice", "chapters", "editor", "layout"]


def _try_firestore_integrantes(familia_id: str) -> list[dict]:
    from pipeline.utils import firestore as fs
    return fs.get_integrantes_para_pipeline(familia_id)


def _try_firestore_relaciones(familia_id: str) -> list[dict]:
    from pipeline.utils import firestore as fs
    return fs.get_relaciones(familia_id)


@dataclass
class PipelineResult:
    personas: list[str] = field(default_factory=list)
    transcriber: dict = field(default_factory=dict)
    voice: dict[str, dict] = field(default_factory=dict)
    chapters: dict[str, str] = field(default_factory=dict)
    editor: BookManuscript | None = None
    layout: str = ""
    errores: list[str] = field(default_factory=list)
    _manuscript: BookManuscript | None = field(default=None, repr=False)

    @property
    def ok(self) -> bool:
        return len(self.errores) == 0


def _get_row_indices(nombres: list[str]) -> list[int]:
    all_rows = sheets.get_all_rows()
    indices = []
    nombres_lower = {n.lower() for n in nombres}
    for i, row in enumerate(all_rows[1:], start=2):
        if len(row) >= 2 and row[1].strip().lower() in nombres_lower:
            indices.append(i)
    return indices


def _build_personas_meta(
    nombres: list[str],
    integrantes: list[dict],
    relaciones: list[dict],
) -> list[dict]:
    """
    Build per-persona metadata merging Integrantes data with Sheets fecha_nac fallback.
    Returns list of dicts with: nombre, fecha_nac, rol, fecha_fallec, vive, es_menor, familia_ctx
    """
    meta = []
    for nombre in nombres:
        integrante = next(
            (p for p in integrantes if p["nombre"].lower() == nombre.lower()), {}
        )
        fecha_nac = integrante.get("fecha_nac") or sheets.get_fecha_nac(nombre)
        ctx = sheets.build_family_context(nombre, integrantes, relaciones)
        meta.append(
            {
                "nombre": nombre,
                "fecha_nac": fecha_nac,
                "rol": integrante.get("rol", ""),
                "fecha_fallec": integrante.get("fecha_fallec", ""),
                "vive": integrante.get("vive", True),
                "es_menor": integrante.get("es_menor", False),
                "familia_ctx": ctx,
            }
        )
    return meta


def run(
    nombres: list[str],
    pais: str = "argentina",
    solo_desde: str | None = None,
    familia: str = "",
    upload_to_gcs: bool = False,
    familia_id: str | None = None,
    from_job_id: str | None = None,
) -> PipelineResult:
    """
    Corre el pipeline completo (o desde un paso específico).
    solo_desde: uno de "transcriber", "voice", "chapters", "editor", "layout"
    from_job_id: job anterior del que reutilizar capítulos (evita volver a llamar a Claude)

    Envuelve _run_pipeline: arranca el contador de costos y los timestamps de
    pipeline_inicio/pipeline_fin/pipeline_paso_actual/numero_corrida (Briefing #32).
    """
    costos = CostAccumulator()
    numero_corrida = None

    if familia_id:
        from pipeline.utils import firestore as fs
        start_idx = STEPS.index(solo_desde) if solo_desde in STEPS else 0
        try:
            numero_corrida = fs.increment_numero_corrida(familia_id)
            fs.marcar_pipeline_inicio(familia_id, STEPS[start_idx])
        except Exception as _e:  # noqa: BLE001
            logger.warning("[orchestrator] familia=%s no se pudo marcar pipeline_inicio: %s", familia_id, _e)

    result = _run_pipeline(
        nombres=nombres,
        pais=pais,
        solo_desde=solo_desde,
        familia=familia,
        upload_to_gcs=upload_to_gcs,
        familia_id=familia_id,
        from_job_id=from_job_id,
        costos=costos,
    )

    if familia_id:
        from pipeline.utils import firestore as fs
        try:
            fs.save_costos(familia_id, costos.as_dict(), numero_corrida or 0)
        except Exception as _e:  # noqa: BLE001
            logger.warning("[orchestrator] familia=%s no se pudo guardar costos: %s", familia_id, _e)
        try:
            fs.marcar_pipeline_fin(familia_id, result.ok)
        except Exception as _e:  # noqa: BLE001
            logger.warning("[orchestrator] familia=%s no se pudo marcar pipeline_fin: %s", familia_id, _e)

    return result


def _run_pipeline(
    nombres: list[str],
    pais: str,
    solo_desde: str | None,
    familia: str,
    upload_to_gcs: bool,
    familia_id: str | None,
    from_job_id: str | None,
    costos: CostAccumulator,
) -> PipelineResult:
    result = PipelineResult(personas=nombres)

    # Forzar datos frescos de Sheets al inicio de cada pipeline
    sheets.invalidate_cache()

    start_idx = 0
    if solo_desde and solo_desde in STEPS:
        start_idx = STEPS.index(solo_desde)

    def _marcar_paso(paso: str) -> None:
        if not familia_id:
            return
        try:
            from pipeline.utils import firestore as fs_mod
            fs_mod.update_pipeline_paso_actual(familia_id, paso)
        except Exception as _e:  # noqa: BLE001
            logger.warning("[orchestrator] familia=%s no se pudo actualizar pipeline_paso_actual: %s", familia_id, _e)

    # ── Cargar datos de familia (Firestore primero, Sheets como fallback) ────────
    integrantes, relaciones, fallecidos = [], [], []
    _fs_integrantes: list[dict] = []
    if familia_id:
        # 1. Carga CRÍTICA: integrantes de Firestore. Solo un fallo acá
        #    invalida el path Firestore.
        try:
            _fs_integrantes = _try_firestore_integrantes(familia_id)
        except Exception as e:
            logger.warning("[orchestrator] familia=%s no se pudieron cargar integrantes de Firestore: %s", familia_id, e)
            _fs_integrantes = []

        # Filtrar por nombres si se especificaron (evita procesar integrantes no solicitados)
        if nombres and _fs_integrantes:
            nombres_lower = {n.lower() for n in nombres}
            _fs_integrantes = [p for p in _fs_integrantes if p["nombre"].lower() in nombres_lower]
            logger.info("[orchestrator] familia=%s filtrado a %d integrantes por nombres=%s", familia_id, len(_fs_integrantes), nombres)

        # 2. Enriquecimiento OPCIONAL: relaciones (Firestore) y fallecidos (Sheets).
        #    Un fallo acá NO debe descartar _fs_integrantes ya cargado.
        if _fs_integrantes:
            # Convert Firestore format to Sheets format for compatibility
            integrantes = [
                {
                    "nombre": p["nombre"],
                    "fecha_nac": p["fecha_nac"],
                    "fecha_fallec": p["fecha_fallec"],
                    "rol": p["rol"],
                    "es_menor": p["es_menor"],
                    "vive": p["vive"],
                }
                for p in _fs_integrantes
            ]
            try:
                relaciones = _try_firestore_relaciones(familia_id)
            except Exception as e:
                logger.warning("[orchestrator] familia=%s no se pudieron cargar relaciones: %s", familia_id, e)
                relaciones = []
            try:
                fallecidos = sheets.get_fallecidos(integrantes)
            except Exception as e:
                logger.warning("[orchestrator] familia=%s no se pudieron cargar fallecidos (Sheets): %s", familia_id, e)
                fallecidos = []
            logger.info("[orchestrator] familia=%s datos cargados desde Firestore: %d integrantes", familia_id, len(integrantes))

    if not integrantes:
        try:
            integrantes = sheets.get_familia_integrantes()
            relaciones = sheets.get_familia_relaciones()
            fallecidos = sheets.get_fallecidos(integrantes)
        except Exception as e:
            logger.warning("[orchestrator] familia=%s no se pudieron cargar datos: %s", familia_id, e)
            integrantes, relaciones, fallecidos = [], [], []

    if _fs_integrantes:
        # Firestore path: build personas_meta directly from fs objects — no name lookup
        personas_meta = [
            {
                "id": p["id"],
                "nombre": p["nombre"],
                "fecha_nac": p.get("fecha_nac", ""),
                "rol": p.get("rol", ""),
                "fecha_fallec": p.get("fecha_fallec", ""),
                "vive": p.get("vive", True),
                "es_menor": p.get("es_menor", False),
                "foto_url": p.get("foto_url", ""),
                "familia_ctx": sheets.build_family_context(p["nombre"], integrantes, relaciones),
            }
            for p in _fs_integrantes
        ]
    else:
        personas_meta = _build_personas_meta(nombres, integrantes, relaciones)

    adultos = [p["nombre"] for p in personas_meta]

    # ── Paso 1: Transcriber ───────────────────────────────────────────────────
    if start_idx <= 0:
        _marcar_paso("transcriber")
        logger.info("[orchestrator] familia=%s job=%s paso 1: transcripción", familia_id, from_job_id)
        try:
            if familia_id and _fs_integrantes:
                allowed_ids = {p["id"] for p in _fs_integrantes}
                result.transcriber = transcriber.run_from_firestore(familia_id, pais, costos=costos, integrante_ids=allowed_ids)
            else:
                row_indices = _get_row_indices(adultos)
                if not row_indices:
                    result.errores.append("No se encontraron filas en el Sheet para los nombres dados")
                    return result
                result.transcriber = transcriber.run(row_indices, pais, costos=costos)
            logger.info("[orchestrator] familia=%s transcripción: %s", familia_id, result.transcriber)
        except Exception as e:
            result.errores.append(f"transcriber: {e}")
            return result

    # ── Paso 2: Voice agent ───────────────────────────────────────────────────
    if start_idx <= 1:
        _marcar_paso("voice")
        logger.info("[orchestrator] familia=%s paso 2: análisis de voz", familia_id)
        try:
            if familia_id and _fs_integrantes:
                result.voice = voice_agent.run_from_firestore(familia_id, _fs_integrantes, costos=costos)
            else:
                logger.warning(
                    "[orchestrator] familia=%s sin _fs_integrantes (len=%d), usando Sheets (adultos=%s)",
                    familia_id, len(_fs_integrantes), adultos,
                )
                if not adultos:
                    result.errores.append("voice_agent: lista de adultos vacía y sin datos Firestore")
                    return result
                result.voice = voice_agent.run(adultos, costos=costos)
            errores_voz = [n for n, v in result.voice.items() if "error" in v]
            if errores_voz:
                result.errores.append(f"voice_agent falló para: {errores_voz}")
        except Exception as e:
            result.errores.append(f"voice_agent: {e}")
            return result

    # ── Refrescar _fs_integrantes tras voice_agent ────────────────────────────
    # voice_agent guarda perfil_voz + transcripcion_completa en Firestore.
    # Si empezamos desde voice, el dict en memoria estaba vacío para esos campos.
    if start_idx <= 1 and familia_id and _fs_integrantes:
        try:
            from pipeline.utils import firestore as fs_mod
            _fs_integrantes = fs_mod.get_integrantes_para_pipeline(familia_id)
            # Reaplicar filtro por nombres tras el refresh
            if nombres:
                nombres_lower = {n.lower() for n in nombres}
                _fs_integrantes = [p for p in _fs_integrantes if p["nombre"].lower() in nombres_lower]
            logger.info("[orchestrator] familia=%s _fs_integrantes refrescados post-voice", familia_id)
        except Exception as _e:
            logger.warning("[orchestrator] familia=%s no se pudo refrescar _fs_integrantes: %s", familia_id, _e)

    # ── Cargar capítulos de job anterior (from_job_id) ───────────────────────
    if from_job_id and not result.chapters:
        try:
            from pipeline.utils import firestore as fs_mod
            prev_job = fs_mod.get_job(from_job_id)
            if prev_job and prev_job.get("result", {}).get("chapters"):
                result.chapters = prev_job["result"]["chapters"]
                logger.info("[orchestrator] familia=%s capítulos cargados desde job %s: %s", familia_id, from_job_id, list(result.chapters.keys()))
            else:
                logger.warning("[orchestrator] familia=%s job %s no tiene capítulos guardados", familia_id, from_job_id)
        except Exception as e:
            logger.warning("[orchestrator] familia=%s no se pudieron cargar capítulos de job %s: %s", familia_id, from_job_id, e)

    # ── Paso 3: Chapter agent ─────────────────────────────────────────────────
    if start_idx <= 2:
        _marcar_paso("chapters")
        logger.info("[orchestrator] familia=%s paso 3: generación de capítulos (paralelo)", familia_id)
        try:
            from concurrent.futures import ThreadPoolExecutor, as_completed
            import anthropic as _anthropic
            client = _anthropic.Anthropic(timeout=600.0)

            if _fs_integrantes:
                # Firestore path: iteramos sobre los objetos directamente, matcheando por id
                personas_meta_by_id = {pm["id"]: pm for pm in personas_meta}
                gen_items = []
                for p in _fs_integrantes:
                    item = dict(p)
                    item["familia_ctx"] = personas_meta_by_id.get(p["id"], {}).get("familia_ctx", {})
                    gen_items.append(item)

                def _generar(item: dict):
                    nombre = item["nombre"]
                    integrante_id = item["id"]
                    perfil_voz = item.get("perfil_voz", {})
                    transcripcion = item.get("transcripcion", "")

                    if not perfil_voz and not transcripcion:
                        return nombre, None, f"chapter_agent/{nombre}: sin transcripcion ni perfil_voz en Firestore"

                    persona = {
                        "nombre": nombre,
                        "perfil_voz": perfil_voz,
                        "transcripcion": transcripcion,
                        "familia_ctx": item.get("familia_ctx", {}),
                    }
                    cap = chapter_agent.generar_capitulo(client, persona, costos=costos)

                    if familia_id and cap:
                        try:
                            from pipeline.utils import firestore as fs_mod
                            fs_mod.save_capitulo(familia_id, integrante_id, cap)
                        except Exception as _e:
                            logger.warning("[orchestrator] familia=%s no se pudo guardar capítulo para %s: %s", familia_id, nombre, _e)

                    return nombre, cap, None

            else:
                # Sheets path: iteramos sobre personas_meta
                gen_items = list(personas_meta)

                def _generar(item: dict):
                    nombre = item["nombre"]
                    p = sheets.get_profile(nombre)
                    if not p:
                        return nombre, None, f"chapter_agent/{nombre}: perfil no encontrado en Sheets"
                    persona = {
                        "nombre": nombre,
                        "perfil_voz": p.get("perfil_voz", {}),
                        "transcripcion": p.get("transcripcion", ""),
                        "familia_ctx": item.get("familia_ctx", {}),
                    }
                    cap = chapter_agent.generar_capitulo(client, persona, costos=costos)
                    return nombre, cap, None

            with ThreadPoolExecutor(max_workers=min(6, len(gen_items))) as executor:
                futures = {executor.submit(_generar, item): item for item in gen_items}
                for future in as_completed(futures):
                    item = futures[future]
                    nombre = item["nombre"]
                    try:
                        nombre, cap, err = future.result()
                        if err:
                            result.errores.append(err)
                        else:
                            result.chapters[nombre] = cap
                    except Exception as e:
                        result.errores.append(f"chapter_agent/{nombre}: {e}")

        except Exception as e:
            result.errores.append(f"chapter_agent: {e}")
            return result

    # ── Paso 4: Editor agent ──────────────────────────────────────────────────
    if start_idx <= 3:
        _marcar_paso("editor")
        logger.info("[orchestrator] familia=%s paso 4: edición del manuscrito", familia_id)
        try:
            if start_idx > 2:
                for pm in personas_meta:
                    nombre = pm["nombre"]
                    p = sheets.get_profile(nombre)
                    if p and p.get("capitulo"):
                        result.chapters[nombre] = p["capitulo"]

            editor_personas = [
                {
                    "nombre": pm["nombre"],
                    "fecha_nac": pm["fecha_nac"],
                    "perfil_voz": pm.get("familia_ctx", {}),
                }
                for pm in personas_meta
                if pm["nombre"] in result.chapters
            ]

            result.editor = editor_agent.run(
                personas=editor_personas,
                capitulos=result.chapters,
                relaciones=relaciones,
                fallecidos=fallecidos,
                costos=costos,
            )
            result._manuscript = result.editor

            if familia_id and result.editor:
                try:
                    from pipeline.utils import firestore as fs_mod
                    import json as _json
                    _db_ref = fs_mod._db().collection("familias").document(familia_id)
                    _db_ref.update({
                        "manuscrito_prologo": result.editor.prologo,
                        "manuscrito_epilogo": result.editor.epilogo,
                        "manuscrito_transiciones": _json.dumps(result.editor.transiciones),
                    })
                except Exception as _e:
                    logger.warning("[orchestrator] familia=%s no se pudo guardar editorial en Firestore: %s", familia_id, _e)

        except Exception as e:
            result.errores.append(f"editor_agent: {e}")
            return result

    # ── Paso 5: Layout agent ──────────────────────────────────────────────────
    if start_idx <= 4:
        _marcar_paso("layout")
        logger.info("[orchestrator] familia=%s paso 5: generación del PDF", familia_id)
        try:
            manuscript = result._manuscript or result.editor

            # Fallback: si solo_desde=layout, cargar capítulos guardados y armar manuscrito mínimo
            if manuscript is None:
                logger.warning("[orchestrator] familia=%s manuscrito no disponible — cargando capítulos guardados", familia_id)
                adultos_meta = [pm for pm in personas_meta if not pm.get("es_menor")]
                fs_integrantes_by_id = {p["id"]: p for p in _fs_integrantes} if _fs_integrantes else {}
                for pm in adultos_meta:
                    nombre = pm["nombre"]
                    if nombre in result.chapters:
                        continue
                    if familia_id and _fs_integrantes:
                        fs_data = fs_integrantes_by_id.get(pm.get("id"))
                        if fs_data and fs_data.get("capitulo"):
                            result.chapters[nombre] = fs_data["capitulo"]
                    if nombre not in result.chapters:
                        p = sheets.get_profile(nombre)
                        if p and p.get("capitulo"):
                            result.chapters[nombre] = p["capitulo"]

                if result.chapters:
                    orden = [pm["nombre"] for pm in personas_meta if pm["nombre"] in result.chapters]
                    prologo, epilogo, transiciones = "", "", {}
                    if familia_id:
                        try:
                            from pipeline.utils import firestore as fs_mod
                            import json as _json
                            fam_doc = fs_mod._db().collection("familias").document(familia_id).get().to_dict() or {}
                            prologo = fam_doc.get("manuscrito_prologo", "")
                            epilogo = fam_doc.get("manuscrito_epilogo", "")
                            raw_trans = fam_doc.get("manuscrito_transiciones", "{}")
                            transiciones = _json.loads(raw_trans) if raw_trans else {}
                        except Exception as _e:
                            logger.warning("[orchestrator] familia=%s no se pudo cargar editorial: %s", familia_id, _e)
                    manuscript = BookManuscript(
                        orden=orden,
                        capitulos=result.chapters,
                        prologo=prologo,
                        epilogo=epilogo,
                        transiciones=transiciones,
                    )
                    result._manuscript = manuscript
                else:
                    raise ValueError("No hay capítulos guardados. Correr primero sin solo_desde o con solo_desde=editor.")

            if manuscript is None:
                raise ValueError("No hay manuscrito disponible para el layout")

            # personas_meta for chapters (with rol/vive/fecha_fallec)
            capitulo_personas = list(personas_meta)

            # todos_integrantes = full family list for timeline
            todos_integrantes = [
                {
                    "nombre": p["nombre"],
                    "fecha_nac": p.get("fecha_nac", ""),
                    "fecha_fallec": p.get("fecha_fallec", ""),
                    "rol": p.get("rol", ""),
                    "vive": p.get("vive", True),
                }
                for p in integrantes
            ] if integrantes else capitulo_personas

            pdf_path = layout_agent.run(
                manuscript=manuscript,
                personas_meta=capitulo_personas,
                nombre_familia=familia,
                todos_integrantes=todos_integrantes,
            )
            result.layout = pdf_path
            logger.info("[orchestrator] familia=%s PDF generado: %s", familia_id, pdf_path)

            if upload_to_gcs and pdf_path:
                import os
                from pipeline.utils import storage as st
                filename = os.path.basename(pdf_path)
                gcs_url = st.upload_to_gcs(pdf_path, st.GCS_BUCKET_LIBROS, f"pdfs/{filename}", "application/pdf")
                logger.info("[orchestrator] familia=%s subido a GCS: %s", familia_id, gcs_url)
                result.layout = gcs_url

                if familia_id:
                    try:
                        from pipeline.utils import firestore as fs_mod
                        fs_mod.save_libro_url(familia_id, gcs_url)
                        fs_mod.update_familia_estado(familia_id, "entregado")
                        logger.info("[orchestrator] familia=%s URL y estado guardados en Firestore", familia_id)
                    except Exception as _e:
                        logger.warning("[orchestrator] familia=%s no se pudo guardar en Firestore: %s", familia_id, _e)

        except Exception as e:
            result.errores.append(f"layout_agent: {e}")

    return result
