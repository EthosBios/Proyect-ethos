#!/usr/bin/env python3
"""
Test del quality_agent contra la corrida Bracho parcial (marino-saraniti).

Carga los capítulos ya generados en Firestore para la familia marino-saraniti
y corre el evaluador de calidad sobre cada uno.

Uso:
  python scripts/test_quality_bracho.py [--dry-run]

  --dry-run: no escribe evaluaciones en Firestore, solo imprime resultado.
"""

import argparse
import json
import os
import subprocess
import sys

FAMILIA_ID = "marino-saraniti"
PROJECT_ID = "familia-marino"


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="No persistir evaluaciones en Firestore")
    args = parser.parse_args()

    # ── Auth ──────────────────────────────────────────────────────────────────
    print("[auth] Cargando credenciales...")
    creds = _load_sa_creds()

    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("[auth] Cargando ANTHROPIC_API_KEY...")
        os.environ["ANTHROPIC_API_KEY"] = _load_api_key("ANTHROPIC_API_KEY")

    # ── Path al repo ──────────────────────────────────────────────────────────
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)

    from google.cloud import firestore as fs_module
    import anthropic
    from pipeline.agents.quality_agent import evaluar_capitulo, MAX_INTENTOS, PALABRAS_PROHIBIDAS
    from pipeline.utils import firestore as fs

    # Inyectar SA en el cliente Firestore del módulo
    import pipeline.utils.firestore as fs_mod
    fs_mod._client = fs_module.Client(project=PROJECT_ID, credentials=creds)

    client = anthropic.Anthropic()

    # ── Cargar integrantes con capítulos ──────────────────────────────────────
    print(f"\n[firestore] Cargando integrantes de '{FAMILIA_ID}'...")
    integrantes = fs.get_integrantes_para_pipeline(FAMILIA_ID)

    con_capitulo = [i for i in integrantes if i.get("capitulo", "").strip()]
    sin_capitulo = [i for i in integrantes if not i.get("capitulo", "").strip()]

    print(f"  Total integrantes : {len(integrantes)}")
    print(f"  Con capítulo      : {len(con_capitulo)}")
    print(f"  Sin capítulo      : {len(sin_capitulo)}")
    for i in sin_capitulo:
        print(f"    - {i['nombre']} (sin capítulo, saltando)")

    if not con_capitulo:
        print("\nNo hay capítulos para evaluar.")
        return

    # ── Cargar relaciones para familia_ctx ────────────────────────────────────
    relaciones = fs.get_relaciones(FAMILIA_ID)
    from pipeline.utils.sheets import build_family_context

    resultados = {}
    aprobados_primera = []
    con_violaciones = {}

    print(f"\n{'='*60}")
    print("EVALUANDO CAPÍTULOS")
    print(f"{'='*60}")

    for integrante in con_capitulo:
        nombre = integrante["nombre"]
        integrante_id = integrante["id"]
        capitulo = integrante["capitulo"]

        print(f"\n→ {nombre} ({len(capitulo.split())} palabras)")

        # Construir familia_ctx
        integrantes_base = [
            {"nombre": i["nombre"], "fecha_nac": i["fecha_nac"],
             "fecha_fallec": i["fecha_fallec"], "rol": i["rol"],
             "es_menor": i["es_menor"], "vive": i["vive"]}
            for i in integrantes
        ]
        familia_ctx = build_family_context(nombre, integrantes_base, relaciones)

        persona = {
            "nombre": nombre,
            "perfil_voz": integrante.get("perfil_voz", {}),
            "transcripcion": integrante.get("transcripcion", ""),
            "familia_ctx": familia_ctx,
        }

        try:
            ev = evaluar_capitulo(client, persona, capitulo, intento=1)
        except Exception as e:
            print(f"  ERROR evaluando: {e}")
            resultados[nombre] = {"error": str(e)}
            continue

        resultados[nombre] = ev.as_dict()

        # Persistir si no es dry-run
        if not args.dry_run:
            try:
                fs.save_evaluacion_calidad(FAMILIA_ID, integrante_id, ev.as_dict(), 1)
                print(f"  Evaluación persistida en Firestore.")
            except Exception as e:
                print(f"  AVISO: no se pudo persistir: {e}")

        # Resumen por persona
        a_ok = "✓" if ev.checklist_a.pasa else "✗"
        b_ok = "✓" if ev.checklist_b.pasa else "✗"
        print(f"  Checklist A (factual)  [{a_ok}]: ", end="")
        if ev.checklist_a.pasa:
            print("OK")
        else:
            print(f"FALLA — {ev.checklist_a.violaciones}")

        print(f"  Checklist B (editorial)[{b_ok}]: ", end="")
        if ev.checklist_b.pasa:
            print("OK")
        else:
            print(f"FALLA — {ev.checklist_b.violaciones}")
            if ev.checklist_b.feedback:
                print(f"  Feedback: {ev.checklist_b.feedback}")

        print(f"  Palabras: {ev.palabras}  |  Aprobado: {'SÍ' if ev.aprobado else 'NO'}")

        if ev.aprobado:
            aprobados_primera.append(nombre)
        else:
            viol = ev.checklist_a.violaciones + ev.checklist_b.violaciones
            con_violaciones[nombre] = viol

    # ── Resumen final ─────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print("RESUMEN CORRIDA BRACHO PARCIAL")
    print(f"{'='*60}")
    total = len(con_capitulo)
    print(f"Capítulos evaluados   : {total}")
    print(f"Aprobados a primera   : {len(aprobados_primera)}/{total}")
    print(f"Con violaciones       : {len(con_violaciones)}/{total}")

    if aprobados_primera:
        print(f"\nAprobados:")
        for n in aprobados_primera:
            print(f"  ✓ {n}")

    if con_violaciones:
        print(f"\nCon violaciones:")
        for n, viol in con_violaciones.items():
            print(f"  ✗ {n}:")
            for v in viol:
                print(f"      · {v}")

    if args.dry_run:
        print("\n[dry-run] Evaluaciones NO persistidas en Firestore.")
    else:
        print("\nEvaluaciones persistidas en Firestore bajo:")
        print(f"  familias/{FAMILIA_ID}/integrantes/{{id}}/evaluaciones_calidad/1")


if __name__ == "__main__":
    main()
