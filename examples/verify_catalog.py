"""Sequential synthetic catalog acceptance through MayaHttpTTSService.

This checks provider settings acceptance and the service's streamed PCM contract.
It does not claim perceptual language/voice quality or transport playback coverage.
Only MAYA_API_KEY is read from the environment; credentials are never printed.
"""

import argparse
import asyncio
import hashlib
import importlib.metadata
import json
import os
import platform
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import numpy as np
from loguru import logger
from pipecat.frames.frames import ErrorFrame, TTSAudioRawFrame

import pipecat_maya
from pipecat_maya import MayaHttpTTSService
from pipecat_maya.settings import LANGUAGES, MODELS

RESULT = Path(__file__).resolve().parent / "catalog-results.json"
TEXTS = {
    "hi": "नमस्ते। आप कैसे हैं?",
    "te": "నమస్కారం. మీరు ఎలా ఉన్నారు?",
    "bn": "নমস্কার। আপনি কেমন আছেন?",
    "gu": "નમસ્તે. તમે કેમ છો?",
    "kn": "ನಮಸ್ಕಾರ. ನೀವು ಹೇಗಿದ್ದೀರಿ?",
    "ml": "നമസ്കാരം. സുഖമാണോ?",
    "mr": "नमस्कार. तुम्ही कसे आहात?",
    "or": "ନମସ୍କାର। ଆପଣ କେମିତି ଅଛନ୍ତି?",
    "pa": "ਸਤ ਸ੍ਰੀ ਅਕਾਲ। ਤੁਸੀਂ ਕਿਵੇਂ ਹੋ?",
    "ta": "வணக்கம். நீங்கள் எப்படி இருக்கிறீர்கள்?",
    "en": "Hello. Your order is ready.",
}


def persist(report):
    RESULT.parent.mkdir(parents=True, exist_ok=True)
    RESULT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")


async def main():
    logger.remove()
    key = os.environ["MAYA_API_KEY"]
    assert set(TEXTS) == set(LANGUAGES)
    rows = [(model, voice, "en", "voice") for model, voices in MODELS.items() for voice in voices]
    rows += [
        (model, voices[0], language, "language")
        for model, voices in MODELS.items()
        for language in sorted(LANGUAGES - {"en"})
    ]
    report = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "scope": (
            "API/settings acceptance and nonzero aligned PCM through MayaHttpTTSService.run_tts"
        ),
        "not_evaluated": [
            "perceptual language or voice quality",
            "transport playback",
            "load capacity",
        ],
        "concurrency": 1,
        "planned_requests": len(rows),
        "python": platform.python_version(),
        "pipecat_version": importlib.metadata.version("pipecat-ai"),
        "package_version": importlib.metadata.version("pipecat-maya"),
        "source_sha256": {
            name: hashlib.sha256(
                (Path(pipecat_maya.__file__).parent / name).read_bytes()
            ).hexdigest()
            for name in ("http.py", "settings.py", "audio.py")
        },
        "results": [],
    }
    assert report["pipecat_version"] == "1.8.1"
    persist(report)
    async with aiohttp.ClientSession() as session:
        for model, voice, language, scope in rows:
            service = MayaHttpTTSService(
                api_key=key,
                aiohttp_session=session,
                settings=MayaHttpTTSService.Settings(model=model, voice=voice, language=language),
                timeout=45,
                max_retries=0,
                sample_rate=24000,
            )
            # Direct generator acceptance, matching the service's HTTP unit tests.
            # A running pipeline normally initializes this in setup().
            service._sample_rate = 24000
            context_id = uuid.uuid4().hex
            start = time.monotonic()
            first = None
            pcm = bytearray()
            formats = set()
            errors = []
            aligned = True
            contexts_match = True
            frames = 0
            try:
                async for frame in service.run_tts(TEXTS[language], context_id):
                    if isinstance(frame, ErrorFrame):
                        errors.append(frame.error.replace(key, "[redacted]")[:500])
                    if isinstance(frame, TTSAudioRawFrame):
                        if first is None:
                            first = time.monotonic() - start
                        pcm.extend(frame.audio)
                        formats.add((frame.sample_rate, frame.num_channels))
                        aligned = aligned and len(frame.audio) % 2 == 0
                        contexts_match = contexts_match and frame.context_id == context_id
                        frames += 1
            except Exception as exc:
                errors.append(f"{type(exc).__name__}: {str(exc).replace(key, '[redacted]')[:500]}")
            samples = np.frombuffer(pcm, dtype="<i2") if aligned else np.array([])
            nonzero = int(np.count_nonzero(samples))
            good = (
                bool(pcm)
                and aligned
                and nonzero > 0
                and formats == {(24000, 1)}
                and contexts_match
                and not errors
            )
            row = {
                "model": model,
                "voice": voice,
                "language": language,
                "coverage": scope,
                "input_text": TEXTS[language],
                "ok": good,
                "audio_frames": frames,
                "pcm_bytes": len(pcm),
                "pcm_duration_s": round(len(pcm) / 48000, 4),
                "time_to_first_pcm_s": round(first, 4) if first is not None else None,
                "elapsed_s": round(time.monotonic() - start, 4),
                "formats": sorted(formats),
                "each_frame_sample_aligned": aligned,
                "context_ids_match": contexts_match,
                "nonzero_samples": nonzero,
                "http_request_id": service.last_request_id,
                "errors": errors,
            }
            report["results"].append(row)
            persist(report)
            print(
                json.dumps(
                    {
                        k: row[k]
                        for k in ("model", "voice", "language", "ok", "pcm_bytes", "errors")
                    },
                    ensure_ascii=False,
                ),
                flush=True,
            )
    report["completed_at"] = datetime.now(timezone.utc).isoformat()
    report["passed"] = sum(row["ok"] for row in report["results"])
    report["failed"] = len(report["results"]) - report["passed"]
    persist(report)
    print(json.dumps({k: report[k] for k in ("planned_requests", "passed", "failed")}), flush=True)
    if report["failed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=RESULT)
    RESULT = parser.parse_args().output
    asyncio.run(main())
