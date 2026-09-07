"""Continuous per-context PCM conversion with explicit end-of-stream flushing."""

import numpy as np
import soxr


class PCMStream:
    """Align arbitrary byte chunks and resample without losing the final filter tail.

    Each synthesis context owns a separate stream. Discard it on interruption;
    finish it only on successful completion so old audio cannot leak into a new turn.
    """

    def __init__(self, input_rate: int, output_rate: int):
        if input_rate <= 0 or output_rate <= 0:
            raise ValueError("Audio sample rates must be positive")
        self._pending = b""
        self._finished = False
        self._stream = (
            soxr.ResampleStream(input_rate, output_rate, 1, dtype="int16", quality="HQ")
            if input_rate != output_rate
            else None
        )

    def feed(self, data: bytes, *, final: bool = False) -> bytes:
        """Convert a chunk; final flushes delayed samples and rejects truncated PCM."""
        if self._finished:
            raise ValueError("PCM stream has already finished")
        data = self._pending + data
        size = len(data) & ~1
        self._pending = data[size:]
        if final and self._pending:
            raise ValueError("Maya returned a truncated 16-bit PCM sample")
        self._finished = final
        aligned = data[:size]
        if self._stream is None:
            return aligned
        samples = np.frombuffer(aligned, dtype="<i2").astype(np.int16, copy=False)
        return self._stream.resample_chunk(samples, last=final).astype("<i2").tobytes()
