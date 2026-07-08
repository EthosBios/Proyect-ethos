"""
Acumulador de costos de una corrida del pipeline (Whisper + Claude).
Thread-safe: los agentes corren pasos en paralelo (ThreadPoolExecutor).
"""

import threading

WHISPER_USD_PER_MIN = 0.006

# Claude Opus 4.7 (MODEL usado en voice_agent, chapter_agent, editor_agent):
# USD 5.00 / USD 25.00 por millón de tokens input/output.
CLAUDE_USD_PER_M_INPUT = 5.00
CLAUDE_USD_PER_M_OUTPUT = 25.00


class CostAccumulator:
    """Una instancia por corrida de pipeline (por familia_id)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.whisper_minutos = 0.0
        self.claude_input_tokens = 0
        self.claude_output_tokens = 0

    def add_whisper_minutos(self, minutos: float) -> None:
        with self._lock:
            self.whisper_minutos += max(0.0, minutos)

    def add_claude_usage(self, input_tokens: int, output_tokens: int) -> None:
        with self._lock:
            self.claude_input_tokens += max(0, input_tokens)
            self.claude_output_tokens += max(0, output_tokens)

    def as_dict(self) -> dict:
        with self._lock:
            whisper_usd = self.whisper_minutos * WHISPER_USD_PER_MIN
            claude_usd = (
                self.claude_input_tokens / 1_000_000 * CLAUDE_USD_PER_M_INPUT
                + self.claude_output_tokens / 1_000_000 * CLAUDE_USD_PER_M_OUTPUT
            )
            return {
                "whisper_minutos": round(self.whisper_minutos, 2),
                "whisper_usd": round(whisper_usd, 4),
                "claude_input_tokens": self.claude_input_tokens,
                "claude_output_tokens": self.claude_output_tokens,
                "claude_usd": round(claude_usd, 4),
                "total_usd": round(whisper_usd + claude_usd, 4),
            }
