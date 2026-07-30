#!/usr/bin/env python3
"""
Test e2e Briefing #35: chapter_agent (post B#34) + quality_agent.

Parte 1 — Capítulo real:
  Genera un capítulo NUEVO para Mariela Valeria Mariño (tiene transcripción + perfil
  en Firestore pero sin capítulo). Verifica que el word count pasa con el pipeline nuevo.

Parte 2 — Escalación humana forzada:
  Capítulo saboteado (50 palabras, palabras prohibidas, cursiva mal) + monkey-patch del
  rewriter para que siempre devuelva el capítulo roto. Confirma escalación a 3 fallos B.

Uso:
  python scripts/test_e2e_quality.py [--skip-real] [--skip-escalation]
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time

logging.basicConfig(level=logging.WARNING, format="[%(name)s] %(message)s")

FAMILIA_ID = "marino-saraniti"
INTEGRANTE_ID = "mariela-valeria-mari-o"
PROJECT_ID = "familia-marino"

SEPARADOR = "=" * 60


def _load_sa_creds():
    from google.oauth2 import service_account
    creds_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not creds_json:
        result = subprocess.run(
            ["gcloud", "secrets", "versions", "access", "latest",
             "--secret=GOOGLE_CREDENTIALS", f"--project={PROJECT_ID}"],
            capture_output=True, text=True, check=True,
        )
        creds_json = result.stdout.strip()
        os.environ["GOOGLE_CREDENTIALS_JSON"] = creds_json
    return service_account.Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )


def _load_api_key(secret_name: str) -> str:
    result = subprocess.run(
        ["gcloud", "secrets", "versions", "access", "latest",
         f"--secret={secret_name}", f"--project={PROJECT_ID}"],
        capture_output=True, text=True, check=True,
    )
    return result.stdout.strip()


def _setup():
    """Auth + path setup. Returns (anthropic_client, firestore_db)."""
    print("[auth] Cargando credenciales...")
    creds = _load_sa_creds()

    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("[auth] Cargando ANTHROPIC_API_KEY...")
        os.environ["ANTHROPIC_API_KEY"] = _load_api_key("ANTHROPIC_API_KEY")

    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    from google.cloud import firestore as fs_mod
    import anthropic
    import pipeline.utils.firestore as fs_utils

    db = fs_mod.Client(project=PROJECT_ID, credentials=creds)
    fs_utils._client = db

    client = anthropic.Anthropic()
    return client, db


# ─── Parte 1: Capítulo real ───────────────────────────────────────────────────

def test_capitulo_real(client, args):
    print(f"\n{SEPARADOR}")
    print("PARTE 1 — Capítulo real: Mariela Valeria Mariño")
    print(SEPARADOR)

    from pipeline.utils import firestore as fs
    from pipeline.utils.sheets import build_family_context
    from pipeline.agents import chapter_agent, quality_agent
    from pipeline.utils.costos import CostAccumulator

    costos = CostAccumulator()

    # Cargar integrante desde Firestore
    print(f"\n[firestore] Cargando datos de {INTEGRANTE_ID}...")
    integrantes = fs.get_integrantes_para_pipeline(FAMILIA_ID)
    integrante = next((i for i in integrantes if i["id"] == INTEGRANTE_ID), None)
    if not integrante:
        print(f"  ERROR: integrante {INTEGRANTE_ID} no encontrado en Firestore.")
        return None

    nombre = integrante["nombre"]
    perfil_voz = integrante.get("perfil_voz", {})
    transcripcion = integrante.get("transcripcion", "")
    palabras_trx = len(transcripcion.split())
    print(f"  Nombre        : {nombre}")
    print(f"  Palabras trx  : {palabras_trx}")
    print(f"  Tiene perfil  : {'SI' if perfil_voz else 'NO'}")

    if not transcripcion.strip() or not perfil_voz:
        print("  ERROR: sin transcripción o sin perfil_voz. Abortando parte 1.")
        return None

    relaciones = fs.get_relaciones(FAMILIA_ID)
    integrantes_base = [
        {"nombre": i["nombre"], "fecha_nac": i["fecha_nac"],
         "fecha_fallec": i["fecha_fallec"], "rol": i["rol"],
         "es_menor": i["es_menor"], "vive": i["vive"]}
        for i in integrantes
    ]
    familia_ctx = build_family_context(nombre, integrantes_base, relaciones)

    persona = {
        "nombre": nombre,
        "perfil_voz": perfil_voz,
        "transcripcion": transcripcion,
        "familia_ctx": familia_ctx,
    }

    # Paso 1: Generar capítulo
    print(f"\n[chapter_agent] Generando capítulo de {nombre}...")
    t0 = time.time()
    try:
        capitulo = chapter_agent.generar_capitulo(client, persona, costos=costos)
    except Exception as e:
        print(f"  ERROR generando capítulo: {e}")
        return None
    t1 = time.time()

    palabras_cap = len(capitulo.split())
    print(f"  Tiempo        : {t1-t0:.0f}s")
    print(f"  Palabras      : {palabras_cap}")
    pasa_wc = 3200 <= palabras_cap <= 3800
    print(f"  Word count    : {'✓ PASA' if pasa_wc else '✗ FALLA'} (esperado 3200–3800)")
    print(f"  Inicio cap    : {capitulo[:200].replace(chr(10),' ')!r}...")

    # Paso 2: Loop de calidad
    print(f"\n[quality_agent] Evaluando capítulo de {nombre}...")
    t2 = time.time()
    cap_final, ev = quality_agent.loop_calidad(
        client, persona, capitulo,
        costos=costos,
        familia_id=FAMILIA_ID if args.persist else None,
        integrante_id=INTEGRANTE_ID if args.persist else None,
    )
    t3 = time.time()

    print(f"\n  ── Evaluación (intento {ev.intento}) ──────────────────────")
    print(f"  Palabras finales : {len(cap_final.split())}")
    a = ev.checklist_a
    b = ev.checklist_b
    print(f"  Checklist A      : {'✓ PASA' if a.pasa else '✗ FALLA'}")
    if not a.pasa:
        for v in a.violaciones:
            print(f"    · {v}")
    print(f"  Checklist B      : {'✓ PASA' if b.pasa else '✗ FALLA'}")
    for item, ok in [("word_count", b.word_count_ok), ("sin_prohibidas", b.sin_prohibidas),
                     ("cursiva_ok", b.cursiva_ok), ("frases_integradas", b.frases_integradas),
                     ("tono_literario", b.tono_literario)]:
        print(f"    {'✓' if ok else '✗'} {item}")
    if b.violaciones:
        print(f"  Violaciones B    :")
        for v in b.violaciones:
            print(f"    · {v}")
    print(f"  Aprobado         : {'✓ SÍ' if ev.aprobado else '✗ NO'}")
    print(f"  Escalado         : {'SÍ' if ev.escalado else 'No'}")
    print(f"  Tiempo quality   : {t3-t2:.0f}s")

    costo_d = costos.as_dict()
    print(f"\n  Costo estimado   : USD {costo_d.get('total_usd', 0):.3f}")

    if args.persist and ev.aprobado:
        try:
            from pipeline.utils import firestore as fs
            fs.save_capitulo(FAMILIA_ID, INTEGRANTE_ID, cap_final)
            print(f"  Capítulo guardado en Firestore para {nombre}.")
        except Exception as e:
            print(f"  AVISO: no se pudo guardar: {e}")

    return cap_final, ev, persona


# ─── Parte 2: Escalación forzada ─────────────────────────────────────────────

CAPITULO_SABOTEADO = """\
José era un hombre inolvidable. Su legado fue memorable para siempre.
El tesoro que dejó fue invaluable. "Siempre lo recordaré", dijo su hija entre comillas.
Era un ser entrañable, inmortal en la memoria familiar. Su huella fue profunda.
Esta es toda la historia de José. Fin.
"""

def test_escalacion(client, args):
    print(f"\n{SEPARADOR}")
    print("PARTE 2 — Escalación humana forzada (capítulo saboteado)")
    print(SEPARADOR)

    import pipeline.agents.quality_agent as qa

    # Persona sintética (sin datos reales de Firestore)
    persona_test = {
        "nombre": "José Test Ficticio",
        "perfil_voz": {
            "muletillas": ["este", "bueno"],
            "frases_propias": ["hay que bancársela", "eso es lo que hay"],
            "registro": "informal rioplatense",
            "detalles_sensoriales": ["olor a asado", "campo abierto"],
            "tono": "melancólico con humor",
        },
        "transcripcion": (
            "Siempre me gustó el campo. Nací acá, en el campo, y acá me voy a morir. "
            "Hay que bancársela, eso es lo que hay. Mi padre me enseñó eso. "
            "Y bueno, uno va aprendiendo con los años. Eso es lo que hay."
        ),
        "familia_ctx": {
            "rol": "abuelo paterno",
            "conyuges": ["María Fernández"],
            "hijos": ["Pedro Test", "Ana Test"],
            "padres": [],
            "hermanos": [],
        },
    }

    palabras_sab = len(CAPITULO_SABOTEADO.split())
    print(f"\n  Capítulo saboteado: {palabras_sab} palabras")
    print(f"  Violations esperadas:")
    print(f"    · word_count={palabras_sab} (< 3200)")
    print(f"    · palabras_prohibidas: legado, memorable, tesoro, invaluable, entrañable, inmortal, huella")
    print(f"    · cursiva: citas entre 'comillas' en lugar de *asteriscos*")

    # Monkey-patch: el rewriter SIEMPRE devuelve el capítulo saboteado
    _original_reescribir = qa._reescribir

    def _reescribir_mock(*args, **kwargs):
        print("    [mock] rewriter llamado → devuelve capítulo saboteado")
        return CAPITULO_SABOTEADO

    qa._reescribir = _reescribir_mock

    print(f"\n[quality_agent] Corriendo loop_calidad con MAX_INTENTOS={qa.MAX_INTENTOS}...")
    try:
        _, ev = qa.loop_calidad(
            client, persona_test, CAPITULO_SABOTEADO,
            costos=None,
            familia_id=None,   # sin persistencia Firestore (persona sintética)
            integrante_id=None,
        )
    finally:
        # Restaurar siempre, incluso si hay error
        qa._reescribir = _original_reescribir

    print(f"\n  ── Resultado del loop ──────────────────────────────")
    print(f"  Intentos realizados : {ev.intento}")
    print(f"  Escalado            : {'✓ SÍ — CORRECTO' if ev.escalado else '✗ NO — ERROR: debería haberse escalado'}")
    print(f"  Aprobado            : {'SÍ (inesperado)' if ev.aprobado else 'No (esperado)'}")
    print(f"  Palabras            : {ev.palabras}")

    b = ev.checklist_b
    print(f"  Checklist B items:")
    for item, ok in [("word_count", b.word_count_ok), ("sin_prohibidas", b.sin_prohibidas),
                     ("cursiva_ok", b.cursiva_ok), ("frases_integradas", b.frases_integradas),
                     ("tono_literario", b.tono_literario)]:
        print(f"    {'✓' if ok else '✗'} {item}")

    if ev.escalado and ev.intento == qa.MAX_INTENTOS:
        print(f"\n  ✓ CRITERIO DE CIERRE CORRECTO: escaló tras {qa.MAX_INTENTOS} intentos B fallidos")
    elif ev.escalado and not ev.checklist_a.pasa:
        print(f"\n  ✓ CRITERIO DE CIERRE CORRECTO: escaló inmediatamente por fallo A")
    else:
        print(f"\n  ✗ Comportamiento inesperado — revisar lógica del loop")

    return ev


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-real", action="store_true",
                        help="Saltar generación de capítulo real (solo test escalación)")
    parser.add_argument("--skip-escalation", action="store_true",
                        help="Saltar test de escalación (solo capítulo real)")
    parser.add_argument("--persist", action="store_true",
                        help="Guardar capítulo y evaluación en Firestore si aprueba")
    args = parser.parse_args()

    client, _ = _setup()

    result_real = None
    if not args.skip_real:
        result_real = test_capitulo_real(client, args)

    if not args.skip_escalation:
        test_escalacion(client, args)

    print(f"\n{SEPARADOR}")
    print("RESUMEN FINAL")
    print(SEPARADOR)
    if result_real:
        _, ev_r, _ = result_real
        wc = ev_r.palabras
        print(f"  Capítulo real ({wc} palabras) : {'✓ PASA word count' if ev_r.checklist_b.word_count_ok else '✗ FALLA word count'}")
        print(f"  Aprobado quality            : {'✓ SÍ' if ev_r.aprobado else '✗ NO'}")
        print(f"  Escalación real             : {'SÍ' if ev_r.escalado else 'No'}")
    if not args.skip_escalation:
        print(f"  Escalación forzada          : verificada en parte 2")


if __name__ == "__main__":
    main()
