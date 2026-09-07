"""Exercise the actual Pipecat frame pipeline against a controllable v2 server."""

import base64
import json

import numpy as np
import pytest
from pipecat.frames.frames import (
    ErrorFrame,
    InterruptionFrame,
    LLMFullResponseEndFrame,
    LLMFullResponseStartFrame,
    LLMTextFrame,
    TTSAudioRawFrame,
    TTSSpeakFrame,
    TTSStoppedFrame,
    TTSTextFrame,
    TTSUpdateSettingsFrame,
)
from pipecat.tests.utils import SleepFrame, run_test
from websockets.asyncio.server import serve

from pipecat_maya.settings import MayaTTSSettings
from pipecat_maya.tts import MayaTTSService

PCM = (np.sin(np.arange(4800) * 0.11) * 12000).astype("<i2").tobytes()


class Server:
    def __init__(self, mode="normal"):
        self.mode = mode
        self.messages = []
        self.connections = 0
        self.headers = []
        self.closed = 0

    async def handler(self, ws):
        self.connections += 1
        self.headers.append(ws.request.headers)
        try:
            async for raw in ws:
                msg = json.loads(raw)
                self.messages.append(msg)
                if msg["type"] == "start":
                    if self.mode == "bad_start":
                        await ws.send(json.dumps({"type": "error", "error": "invalid voice"}))
                    else:
                        await ws.send(
                            json.dumps(
                                {
                                    "type": "metadata",
                                    "sample_rate": 24000,
                                    "channels": 1,
                                    "encoding": "pcm_s16le",
                                    "session_id": "test-session",
                                }
                            )
                        )
                elif msg["type"] == "text":
                    cid = msg["context_id"]
                    if msg.get("text"):
                        if self.mode == "error":
                            await ws.send(
                                json.dumps(
                                    {"type": "error", "error": "origin_error", "context_id": cid}
                                )
                            )
                        elif self.mode == "untagged_error":
                            await ws.send(json.dumps({"type": "error", "error": "origin_error"}))
                        elif self.mode not in {"empty", "stall"}:
                            data = PCM if self.mode != "odd" else PCM + b"x"
                            for chunk in (data[:101], data[101:3000], data[3000:]):
                                await ws.send(
                                    json.dumps(
                                        {
                                            "type": "audio",
                                            "context_id": cid,
                                            "audio": base64.b64encode(chunk).decode(),
                                        }
                                    )
                                )
                    if not msg["continue"] and self.mode not in {
                        "stall",
                        "error",
                        "untagged_error",
                    }:
                        await ws.send(json.dumps({"type": "end", "context_id": cid}))
                elif msg["type"] == "cancel":
                    # Deliberately send late in-flight data before acknowledging.
                    await ws.send(
                        json.dumps(
                            {
                                "type": "audio",
                                "context_id": msg["context_id"],
                                "audio": base64.b64encode(b"\x7f" * 800).decode(),
                            }
                        )
                    )
                    await ws.send(
                        json.dumps({"type": "cancelled", "context_id": msg["context_id"]})
                    )
        finally:
            self.closed += 1


async def exercise(server, frames, **kwargs):
    async with serve(server.handler, "127.0.0.1", 0) as endpoint:
        url = f"ws://127.0.0.1:{endpoint.sockets[0].getsockname()[1]}"
        tts = MayaTTSService(api_key="test-key", url=url, **kwargs)
        output, upstream = await run_test(tts, frames_to_send=frames, start_timeout=10)
        assert tts._websocket is None
        assert tts._receive_task is None
        assert tts._watchdog_task is None
        return tts, output, upstream


async def test_token_chunks_share_one_turn_and_preserve_audio():
    server = Server()
    tts, output, errors = await exercise(
        server,
        [
            LLMFullResponseStartFrame(),
            LLMTextFrame("Hello"),
            LLMTextFrame(" world. "),
            LLMTextFrame("Thank you"),
            LLMTextFrame("."),
            LLMFullResponseEndFrame(),
            SleepFrame(0.5),
            TTSSpeakFrame("Next turn."),
            SleepFrame(0.5),
        ],
    )
    sent = [m for m in server.messages if m["type"] == "text"]
    texts = [m for m in sent if m["text"]]
    assert len(texts) == 3
    assert texts[0]["context_id"] == texts[1]["context_id"] != texts[2]["context_id"]
    assert all(m["continue"] for m in texts)
    assert len([m for m in sent if not m["continue"]]) == 2
    assert server.connections == 1
    assert server.headers[0]["Authorization"] == "Bearer test-key"
    assert server.headers[0]["User-Agent"] == "pipecat-maya/0.1.0"
    assert tts.session_id == "test-session"
    assert b"".join(f.audio for f in output if isinstance(f, TTSAudioRawFrame)) == PCM * 3
    assert len([f for f in output if isinstance(f, TTSStoppedFrame)]) == 2
    assert any(isinstance(f, TTSTextFrame) for f in output)
    assert not any(isinstance(f, ErrorFrame) for f in errors)


@pytest.mark.parametrize("rate", [8000, 16000])
async def test_pipeline_output_resampling_flushes_tail(rate):
    _, output, _ = await exercise(
        Server(), [TTSSpeakFrame("Test."), SleepFrame(0.5)], sample_rate=rate
    )
    audio = [f for f in output if isinstance(f, TTSAudioRawFrame)]
    assert all(f.sample_rate == rate and f.context_id and len(f.audio) % 2 == 0 for f in audio)
    assert sum(len(f.audio) for f in audio) == round(len(PCM) * rate / 24000)


async def test_interruption_discards_late_audio_and_reuses_socket():
    server = Server()
    _, output, _ = await exercise(
        server,
        [
            LLMFullResponseStartFrame(),
            LLMTextFrame("First turn. More"),
            SleepFrame(0.25),
            InterruptionFrame(),
            SleepFrame(0.1),
            TTSSpeakFrame("Replacement."),
            SleepFrame(0.5),
        ],
    )
    cancel = [m for m in server.messages if m["type"] == "cancel"]
    assert len(cancel) == 1
    interruption = next(i for i, f in enumerate(output) if isinstance(f, InterruptionFrame))
    audio_after = [f for f in output[interruption + 1 :] if isinstance(f, TTSAudioRawFrame)]
    assert audio_after
    assert all(f.context_id != cancel[0]["context_id"] for f in audio_after)
    assert b"".join(f.audio for f in audio_after) == PCM
    assert server.connections == 1


async def test_settings_change_reconnects_and_auto_language_is_omitted():
    server = Server()
    _, output, errors = await exercise(
        server,
        [
            TTSSpeakFrame("Hello."),
            SleepFrame(0.3),
            TTSUpdateSettingsFrame(
                delta=MayaTTSSettings(model="Maya Calyx", voice="Aarav", language=None)
            ),
            TTSSpeakFrame("Again."),
            SleepFrame(0.5),
        ],
        settings=MayaTTSSettings(language="en"),
    )
    starts = [m for m in server.messages if m["type"] == "start"]
    assert len(starts) == 2
    assert starts[0]["language"] == "en"
    assert "language" not in starts[1] and "speed" not in starts[1]
    assert starts[1]["model"] == "Maya Calyx"
    assert len([f for f in output if isinstance(f, TTSStoppedFrame)]) == 2
    assert not any(isinstance(f, ErrorFrame) for f in errors)


@pytest.mark.parametrize("mode", ["error", "untagged_error", "empty", "odd", "stall"])
async def test_bad_provider_results_fail_explicitly_and_finish(mode):
    server = Server(mode)
    _, output, errors = await exercise(
        server, [TTSSpeakFrame("Failure."), SleepFrame(0.7)], request_timeout=0.2
    )
    assert any(isinstance(f, ErrorFrame) for f in errors)
    assert len([f for f in output if isinstance(f, TTSStoppedFrame)]) == 1


async def test_no_text_turn_does_not_send_invalid_empty_closer():
    server = Server()
    await exercise(
        server, [LLMFullResponseStartFrame(), LLMFullResponseEndFrame(), SleepFrame(0.1)]
    )
    assert all(m["type"] == "start" for m in server.messages)


async def test_start_rejection_closes_connection_without_sending_text():
    server = Server("bad_start")
    async with serve(server.handler, "127.0.0.1", 0) as endpoint:
        tts = MayaTTSService(
            api_key="test", url=f"ws://127.0.0.1:{endpoint.sockets[0].getsockname()[1]}"
        )
        with pytest.raises(ValueError, match="rejected start"):
            await tts._connect_websocket()
        assert tts._websocket is None
        assert len(server.messages) == 1


async def test_invalid_update_does_not_change_settings():
    tts = MayaTTSService(api_key="test")
    with pytest.raises(ValueError):
        await tts._update_settings(MayaTTSSettings(model="Maya Calyx"))
    assert tts._settings.model == "Maya 2 Native"
    assert tts._settings.voice == "Ananya"
