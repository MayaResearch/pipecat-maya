"""Independent byte-preservation, signal, and configuration checks."""

import numpy as np
import pytest
import soxr
from pipecat.transcriptions.language import Language

from pipecat_maya.audio import PCMStream
from pipecat_maya.settings import MODELS, MayaTTSSettings, default_settings, request_settings


@pytest.mark.parametrize("output_rate", [8000, 16000, 24000, 48000])
def test_arbitrary_chunks_keep_complete_resampled_tail(output_rate):
    # A non-period-aligned signal makes dropped tails and restarted filters visible.
    signal = (np.sin(np.arange(24137) * 0.107) * 16000).astype("<i2")
    raw = signal.tobytes()
    stream = PCMStream(24000, output_rate)
    chunks = [stream.feed(raw[n : n + 137]) for n in range(0, len(raw), 137)]
    chunks.append(stream.feed(b"", final=True))
    actual = np.frombuffer(b"".join(chunks), dtype="<i2")
    expected = soxr.resample(signal, 24000, output_rate, quality="HQ")
    assert len(actual) == len(expected)
    # Independent integer resamplers can differ by two least-significant bits
    # from quantization/dither; duration and complete sample count must match.
    np.testing.assert_allclose(actual, expected, atol=2)


def test_identity_and_truncated_sample():
    stream = PCMStream(24000, 24000)
    assert stream.feed(b"\x01") == b""
    assert stream.feed(b"\x02") == b"\x01\x02"
    assert stream.feed(b"\x03") == b""
    with pytest.raises(ValueError, match="truncated"):
        stream.feed(b"", final=True)


@pytest.mark.parametrize("model,voice", [(m, v) for m, voices in MODELS.items() for v in voices])
def test_all_documented_voices(model, voice):
    settings = default_settings(MayaTTSSettings(model=model, voice=voice))
    payload = request_settings(settings)
    assert payload["model"] == model and payload["voice"] == voice
    assert "language" not in payload
    assert ("speed" in payload) == (model == "Maya 2 Native")


@pytest.mark.parametrize("speed", [True, False, "1.0", 0.49, 1.26, float("nan"), float("inf")])
def test_reject_invalid_speed(speed):
    with pytest.raises(ValueError, match="speed"):
        default_settings(MayaTTSSettings(speed=speed))


@pytest.mark.parametrize(
    "delta",
    [
        MayaTTSSettings(model="maya 2 native"),
        MayaTTSSettings(voice="ananya"),
        MayaTTSSettings(model="Maya Calyx"),
        MayaTTSSettings(language="en-US"),
        MayaTTSSettings(language="ar"),
        MayaTTSSettings(extra={"seed": 42}),
        MayaTTSSettings(model="Maya Calyx", voice="Aarav", speed=1),
    ],
)
def test_reject_unsupported_settings(delta):
    with pytest.raises(ValueError):
        default_settings(delta)


def test_language_enum_and_autodetect_are_not_confused():
    delta = MayaTTSSettings(language=Language.HI)
    settings = default_settings(delta)
    assert settings.language == "hi"
    assert delta.language is Language.HI
    settings.apply_update(MayaTTSSettings(language=None))
    assert "language" not in request_settings(settings)
