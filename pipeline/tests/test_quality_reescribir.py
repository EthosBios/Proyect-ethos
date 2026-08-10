"""
Test de la rama de reescritura por word_count en quality_agent._reescribir.

Regresión del fix (commit 0c438e2): antes el prompt siempre pedía "expandir",
incluso cuando el capítulo excedía MAX_WORDS. Ahora ramifica:
  - capítulo < MIN_WORDS  -> "expandiendo"
  - capítulo > MAX_WORDS  -> "condensando"

No consume API: se parchea call_with_retry para capturar el prompt sin llamar
a Anthropic. Correr desde la raíz del repo:
    python3 pipeline/tests/test_quality_reescribir.py
o con pytest:
    pytest pipeline/tests/test_quality_reescribir.py
"""
import os
import sys
import types

# Permitir `python3 pipeline/tests/test_quality_reescribir.py` desde la raíz:
# insertar la raíz del repo (dos niveles arriba) en sys.path antes del import.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import pipeline.agents.quality_agent as qa


class _Block:
    type = "text"
    text = "reescrito"


class _Usage:
    input_tokens = 1
    output_tokens = 1


class _Msg:
    content = [_Block()]
    usage = _Usage()


def _prompt_de_reescritura(n_palabras: int) -> str:
    """Corre _reescribir con un capítulo de n_palabras y devuelve el prompt enviado."""
    capturado = {}

    def fake_call_with_retry(fn, **kwargs):
        capturado["prompt"] = kwargs["messages"][0]["content"]
        return _Msg()

    original = qa.call_with_retry
    qa.call_with_retry = fake_call_with_retry
    try:
        client = types.SimpleNamespace(
            messages=types.SimpleNamespace(create=lambda **k: None)
        )
        persona = {"nombre": "Rosa Pérez", "perfil_voz": {"frases_propias": []}}
        capitulo = " ".join(["palabra"] * n_palabras)
        rb = qa.ResultadoB(
            word_count_ok=False,
            violaciones=[f"word_count={n_palabras} (esperado {qa.MIN_WORDS}–{qa.MAX_WORDS})"],
        )
        qa._reescribir(client, persona, capitulo, rb, costos=None)
    finally:
        qa.call_with_retry = original
    return capturado["prompt"]


def test_capitulo_largo_condensa():
    """Capítulo > MAX_WORDS -> el prompt pide condensar, no expandir."""
    prompt = _prompt_de_reescritura(qa.MAX_WORDS + 200)
    assert "condensando" in prompt
    assert "expandiendo" not in prompt


def test_capitulo_corto_expande():
    """Capítulo < MIN_WORDS -> el prompt pide expandir, no condensar."""
    prompt = _prompt_de_reescritura(100)
    assert "expandiendo" in prompt
    assert "condensando" not in prompt


def test_borde_justo_sobre_max_condensa():
    """Borde: MAX_WORDS + 1 cae en la rama de condensar."""
    prompt = _prompt_de_reescritura(qa.MAX_WORDS + 1)
    assert "condensando" in prompt
    assert "expandiendo" not in prompt


if __name__ == "__main__":
    casos = [
        ("largo  (>MAX)", test_capitulo_largo_condensa),
        ("corto  (<MIN)", test_capitulo_corto_expande),
        ("borde  (MAX+1)", test_borde_justo_sobre_max_condensa),
    ]
    fallos = 0
    print(f"MIN_WORDS={qa.MIN_WORDS}  MAX_WORDS={qa.MAX_WORDS}\n")
    for nombre, fn in casos:
        try:
            fn()
            print(f"  PASS  {nombre}")
        except AssertionError as exc:
            fallos += 1
            print(f"  FAIL  {nombre}: {exc}")
    print("\n" + ("TODOS PASS ✅" if fallos == 0 else f"{fallos} FALLO(S) ❌"))
    raise SystemExit(1 if fallos else 0)
