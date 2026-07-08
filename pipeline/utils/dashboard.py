"""
Agregación de datos para el panel /admin/dashboard (Briefing #32).
Lee de Firestore; no escribe nada acá salvo lo que ya hacen los endpoints de acción.
"""

from datetime import datetime, timezone

from pipeline.utils import firestore as fs

_VENTANA_REFUND_DIAS = 7
_ESTANCADA_HORAS = 72
_PIPELINE_FALLIDO_HORAS = 2
_SIN_PIPELINE_HORAS = 1
_FUERA_PROMESA_HORAS = 24

_PRECIOS_USD = {"esencial": 59, "familiar": 79, "extendido": 129, "recuerdo": 39, "legado": 79, "bios": 129}


def _as_dt(value) -> datetime | None:
    """Normaliza un valor de Firestore (Timestamp, datetime, str o None) a datetime aware UTC."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        dt = value.ToDatetime() if hasattr(value, "ToDatetime") else None
        if dt:
            return dt.replace(tzinfo=timezone.utc) if not dt.tzinfo else dt
    except Exception:  # noqa: BLE001
        pass
    try:
        return datetime.fromisoformat(str(value)).astimezone(timezone.utc)
    except Exception:  # noqa: BLE001
        return None


def _horas_desde(dt: datetime | None) -> float | None:
    if dt is None:
        return None
    return (datetime.now(timezone.utc) - dt).total_seconds() / 3600


def _familia_avance(integrantes: list[dict]) -> int:
    if not integrantes:
        return 0
    total = sum(i.get("porcentaje_avance", 0) or 0 for i in integrantes)
    return round(total / len(integrantes))


def _costo_promedio_corrida_limpia() -> dict:
    """Costo promedio de numero_corrida=1 y % sobrecosto de reintentos (numero_corrida>1).
    Usa una collection-group query sobre familias/*/costos — requiere que Firestore
    tenga habilitado el índice de collection group para 'costos' (se crea automáticamente
    la primera vez que se ejecuta esta query en un proyecto con Firestore Native, o puede
    requerir crearlo manualmente desde la consola si Firestore devuelve FAILED_PRECONDITION)."""
    try:
        db = fs._db()
        limpias = list(db.collection_group("costos").where("numero_corrida", "==", 1).stream())
        reintentos = list(db.collection_group("costos").where("numero_corrida", ">", 1).stream())
    except Exception:  # noqa: BLE001
        return {"costo_promedio_limpio": None, "pct_sobrecosto_reintentos": None, "error": True}

    costos_limpios = [d.to_dict().get("total_usd", 0) for d in limpias]
    costos_reintentos = [d.to_dict().get("total_usd", 0) for d in reintentos]

    promedio_limpio = round(sum(costos_limpios) / len(costos_limpios), 2) if costos_limpios else None
    pct_sobrecosto = None
    if costos_reintentos and promedio_limpio:
        promedio_reintentos = sum(costos_reintentos) / len(costos_reintentos)
        pct_sobrecosto = round((promedio_reintentos - promedio_limpio) / promedio_limpio * 100, 1)

    return {
        "costo_promedio_limpio": promedio_limpio,
        "pct_sobrecosto_reintentos": pct_sobrecosto,
        "error": False,
    }


def build_dashboard_data() -> dict:
    db = fs._db()
    now = datetime.now(timezone.utc)

    familias_docs = list(db.collection("familias").stream())

    filas = []
    alertas = []
    libros_entregados_total = 0
    libros_entregados_30d = 0
    tiempos_entrega_horas: list[float] = []
    exposicion_refund_usd = 0.0
    margenes: list[float] = []
    familias_en_curso = 0

    for doc in familias_docs:
        data = doc.to_dict() or {}
        familia_id = doc.id
        nombre = data.get("nombre", "(sin nombre)")
        estado = data.get("estado", "")
        pack = data.get("pack", "")
        comprador = data.get("comprador", {})

        integrantes = fs.get_integrantes(familia_id)
        avance = _familia_avance(integrantes)

        fecha_compra = _as_dt(data.get("fecha_compra"))
        ultima_grabacion = _as_dt(data.get("ultima_grabacion"))
        pipeline_inicio = _as_dt(data.get("pipeline_inicio"))
        pipeline_fin = _as_dt(data.get("pipeline_fin"))
        entregado_at = _as_dt(data.get("entregado_at"))
        pipeline_paso_actual = data.get("pipeline_paso_actual")
        costos = data.get("costos") or {}

        entregada = bool(entregado_at) or estado == "entregado"

        # ── Estado operativo legible ──────────────────────────────────────
        if entregada:
            estado_operativo = "Entregado"
        elif pipeline_inicio and not pipeline_fin:
            estado_operativo = f"Pipeline corriendo ({pipeline_paso_actual or '?'})"
        elif pipeline_fin:
            estado_operativo = "Emitido"
        elif avance >= 100:
            estado_operativo = "Grabación completa"
        elif avance > 0:
            completos = sum(1 for i in integrantes if i.get("estado") == "completo")
            estado_operativo = f"Grabando ({completos}/{len(integrantes)})"
        else:
            estado_operativo = "Comprada"

        # ── Bloque A: alertas accionables ─────────────────────────────────
        horas_desde_grabacion = _horas_desde(ultima_grabacion)
        if horas_desde_grabacion is not None and horas_desde_grabacion > _ESTANCADA_HORAS and avance < 100:
            alertas.append({
                "tipo": "🔴 Familia estancada",
                "familia_id": familia_id,
                "familia_nombre": nombre,
                "detalle": f"Sin grabar hace {round(horas_desde_grabacion)}hs",
                "accion": "recordatorio",
            })

        horas_desde_inicio = _horas_desde(pipeline_inicio)
        if pipeline_inicio and not pipeline_fin and horas_desde_inicio is not None and horas_desde_inicio > _PIPELINE_FALLIDO_HORAS:
            alertas.append({
                "tipo": "🔴 Pipeline fallido",
                "familia_id": familia_id,
                "familia_nombre": nombre,
                "detalle": f"Paso actual: {pipeline_paso_actual or 'desconocido'} (corriendo hace {round(horas_desde_inicio)}hs)",
                "accion": "reintentar",
                "solo_desde": pipeline_paso_actual,
            })

        if avance >= 100 and not pipeline_inicio:
            # avance = 100% y pipeline_inicio vacío: usamos ultima_grabacion como proxy de "cuándo terminó de grabar"
            if horas_desde_grabacion is not None and horas_desde_grabacion > _SIN_PIPELINE_HORAS:
                alertas.append({
                    "tipo": "🟡 Grabación completa sin pipeline",
                    "familia_id": familia_id,
                    "familia_nombre": nombre,
                    "detalle": "Avance 100% pero el pipeline nunca se disparó",
                    "accion": "disparar",
                })

        if avance >= 100 and not entregada and horas_desde_grabacion is not None and horas_desde_grabacion > _FUERA_PROMESA_HORAS:
            alertas.append({
                "tipo": "🟡 Fuera de promesa 24hs",
                "familia_id": familia_id,
                "familia_nombre": nombre,
                "detalle": f"Avance 100% hace {round(horas_desde_grabacion)}hs, sin entregar",
                "accion": "detalle",
            })

        # ── Bloque C: semáforo de riesgo refund ───────────────────────────
        dias_desde_compra = None
        semaforo_refund = None
        exposicion_familia = 0.0
        if fecha_compra:
            dias_desde_compra = (now - fecha_compra).total_seconds() / 86400
            if dias_desde_compra <= _VENTANA_REFUND_DIAS:
                if entregada:
                    semaforo_refund = "🔴"
                    exposicion_familia = costos.get("total_usd", 0.0)
                elif avance > 0:
                    semaforo_refund = "🟡"
                    exposicion_familia = costos.get("whisper_usd", 0.0)
                else:
                    semaforo_refund = "🟢"
                    exposicion_familia = 0.50  # ~comisión Hotmart
                exposicion_refund_usd += exposicion_familia

        # ── Bloque B: KPIs agregados ───────────────────────────────────────
        if entregada:
            libros_entregados_total += 1
            if entregado_at and (now - entregado_at).days <= 30:
                libros_entregados_30d += 1
            if fecha_compra and entregado_at:
                tiempos_entrega_horas.append((entregado_at - fecha_compra).total_seconds() / 3600)
            precio = _PRECIOS_USD.get(pack)
            costo_real = costos.get("total_usd")
            if precio is not None and costo_real is not None:
                margenes.append(precio - costo_real)
        else:
            familias_en_curso += 1

        filas.append({
            "familia_id": familia_id,
            "nombre": nombre,
            "estado_operativo": estado_operativo,
            "avance": avance,
            "dias_desde_compra": round(dias_desde_compra, 1) if dias_desde_compra is not None else None,
            "semaforo_refund": semaforo_refund,
            "pack": pack,
            "precio": _PRECIOS_USD.get(pack),
            "costo_real": costos.get("total_usd"),
            "margen_real": (
                round(_PRECIOS_USD[pack] - costos["total_usd"], 2)
                if pack in _PRECIOS_USD and costos.get("total_usd") is not None
                else None
            ),
            "comprador_email": comprador.get("email", ""),
            "tiene_alerta": any(a["familia_id"] == familia_id for a in alertas),
        })

    # Orden: alertas primero, después en curso, después listas
    filas.sort(key=lambda f: (not f["tiene_alerta"], f["estado_operativo"] == "Entregado", f["nombre"]))

    tiempo_promedio_entrega_hs = (
        round(sum(tiempos_entrega_horas) / len(tiempos_entrega_horas), 1) if tiempos_entrega_horas else None
    )
    margen_promedio = round(sum(margenes) / len(margenes), 2) if margenes else None

    kpis = {
        "libros_entregados_total": libros_entregados_total,
        "libros_entregados_30d": libros_entregados_30d,
        "familias_en_curso": familias_en_curso,
        "tiempo_promedio_entrega_hs": tiempo_promedio_entrega_hs,
        "exposicion_refund_usd": round(exposicion_refund_usd, 2),
        "margen_promedio_usd": margen_promedio,
        **_costo_promedio_corrida_limpia(),
    }

    return {"alertas": alertas, "kpis": kpis, "familias": filas}
