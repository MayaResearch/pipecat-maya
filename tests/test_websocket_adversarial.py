"""Lifecycle tests against real sockets, including adversarial connection timing."""

import asyncio
import base64
import json
import time

import pytest
from pipecat.frames.frames import (
    CancelFrame,
    ErrorFrame,
    InterruptionFrame,
    LLMFullResponseStartFrame,
    LLMTextFrame,
    TTSAudioRawFrame,
    TTSSpeakFrame,
    TTSStoppedFrame,
    TTSUpdateSettingsFrame,
)
from pipecat.tests.utils import SleepFrame, run_test
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

from pipecat_maya.settings import MayaTTSSettings
from pipecat_maya.tts import MayaTTSService

AUDIO = b"\x00\x10" * 1200
STALE = b"\x00\x70" * 800


class AdversarialServer:
    def __init__(self, mode="normal"):
        self.mode = mode
        self.messages = []
        self.connections = 0
        self.closed = 0
        self.new_text = asyncio.Event()

    async def audio(self, ws, cid, data=AUDIO):
        await ws.send(
            json.dumps(
                {"type": "audio", "context_id": cid, "audio": base64.b64encode(data).decode()}
            )
        )

    async def handler(self, ws):
        self.connections += 1
        connection = self.connections
        try:
            async for raw in ws:
                msg = json.loads(raw)
                self.messages.append((connection, msg))
                if msg["type"] == "start":
                    if self.mode == "metadata_stall":
                        continue
                    await ws.send(
                        json.dumps(
                            {
                                "type": "metadata",
                                "sample_rate": 24000,
                                "channels": 1,
                                "encoding": "pcm_s16le",
                                "session_id": f"session-{connection}",
                            }
                        )
                    )
                    if self.mode in {"idle_close", "reconnect_race"} and connection == 1:
                        await asyncio.sleep(0.05)
                        await ws.close()
                elif msg["type"] == "text":
                    cid = msg["context_id"]
                    if msg.get("text"):
                        self.new_text.set()
                        if self.mode != "stall":
                            await self.audio(ws, cid)
                        if self.mode == "midturn_close" and connection == 1:
                            await ws.close()
                    if not msg["continue"] and self.mode != "stall":
                        await ws.send(json.dumps({"type": "end", "context_id": cid}))
                elif msg["type"] == "cancel":
                    await self.audio(ws, msg["context_id"], STALE)
                    await ws.send(
                        json.dumps({"type": "cancelled", "context_id": msg["context_id"]})
                    )
        except ConnectionClosed:
            pass
        finally:
            self.closed += 1


async def exercise(server, frames, **kwargs):
    async with serve(server.handler, "127.0.0.1", 0) as endpoint:
        tts = MayaTTSService(
            api_key="test-key",
            url=f"ws://127.0.0.1:{endpoint.sockets[0].getsockname()[1]}",
            **kwargs,
        )
        down, up = await asyncio.wait_for(
            run_test(tts, frames_to_send=frames, start_timeout=2), timeout=6
        )
        assert tts._websocket is None
        assert tts._receive_task is None
        assert tts._watchdog_task is None
        assert not tts._cancel_tasks
        assert not tts._turns
        return tts, down, up


def audio_bytes(frames):
    return b"".join(frame.audio for frame in frames if isinstance(frame, TTSAudioRawFrame))


async def test_idle_close_reconnects_before_next_turn():
    server = AdversarialServer("idle_close")
    tts, down, _ = await exercise(
        server, [SleepFrame(0.7), TTSSpeakFrame("After idle close."), SleepFrame(0.2)]
    )
    assert audio_bytes(down) == AUDIO
    assert server.connections == 2
    assert tts.session_id == "session-2"


async def test_midturn_disconnect_is_explicit_and_does_not_replay():
    server = AdversarialServer("midturn_close")
    _, down, up = await exercise(
        server,
        [TTSSpeakFrame("First."), SleepFrame(0.7), TTSSpeakFrame("Second."), SleepFrame(0.2)],
    )
    text = [msg for _, msg in server.messages if msg["type"] == "text" and msg.get("text")]
    assert [msg["text"] for msg in text] == ["First.", "Second."]
    assert text[0]["context_id"] != text[1]["context_id"]
    assert any(isinstance(frame, ErrorFrame) for frame in up)
    assert audio_bytes(down).endswith(AUDIO)
    assert server.connections == 2


async def test_cancel_and_next_turn_without_intermediate_sleep():
    server = AdversarialServer()
    _, down, up = await exercise(
        server,
        [
            LLMFullResponseStartFrame(),
            LLMTextFrame("First turn. Waiting"),
            SleepFrame(0.1),
            InterruptionFrame(),
            TTSSpeakFrame("Replacement."),
            SleepFrame(0.3),
        ],
    )
    cancelled = [msg["context_id"] for _, msg in server.messages if msg["type"] == "cancel"]
    assert len(cancelled) == 1
    interruption = next(i for i, frame in enumerate(down) if isinstance(frame, InterruptionFrame))
    after = down[interruption + 1 :]
    assert audio_bytes(after) == AUDIO
    assert all(
        frame.context_id not in cancelled for frame in after if isinstance(frame, TTSAudioRawFrame)
    )
    assert not any(isinstance(frame, ErrorFrame) for frame in up)
    assert server.connections == 1


async def test_settings_update_drains_inflight_turn_before_new_connection():
    server = AdversarialServer()
    _, down, up = await exercise(
        server,
        [
            LLMFullResponseStartFrame(),
            LLMTextFrame("Old voice. Pending"),
            SleepFrame(0.1),
            TTSUpdateSettingsFrame(delta=MayaTTSSettings(voice="Arjun")),
            TTSSpeakFrame("New voice."),
            SleepFrame(0.3),
        ],
    )
    starts = [(connection, msg) for connection, msg in server.messages if msg["type"] == "start"]
    assert len(starts) == 2
    assert starts[0][1]["voice"] == "Ananya" and starts[1][1]["voice"] == "Arjun"
    assert any(
        connection == 1 and msg["type"] == "text" and msg["continue"] is False
        for connection, msg in server.messages
    )
    spoken = [
        (connection, msg["text"])
        for connection, msg in server.messages
        if msg["type"] == "text" and msg.get("text")
    ]
    assert spoken == [(1, "Old voice."), (1, "Pending"), (2, "New voice.")]
    assert audio_bytes(down) == AUDIO * 3
    assert not any(isinstance(frame, ErrorFrame) for frame in up)


async def test_watchdog_closes_stalled_turn_and_emits_one_stop():
    server = AdversarialServer("stall")
    _, down, up = await exercise(
        server, [TTSSpeakFrame("Stall."), SleepFrame(0.4)], request_timeout=0.1
    )
    assert any(isinstance(frame, ErrorFrame) and "timed out" in frame.error for frame in up)
    assert len([frame for frame in down if isinstance(frame, TTSStoppedFrame)]) == 1
    assert len([msg for _, msg in server.messages if msg["type"] == "cancel"]) == 1
    assert audio_bytes(down) == b""


async def test_end_frame_flushes_partial_llm_tail_on_same_socket():
    server = AdversarialServer()
    _, down, up = await exercise(
        server,
        [
            LLMFullResponseStartFrame(),
            LLMTextFrame("First sentence. Trailing text"),
            SleepFrame(0.1),
        ],
    )
    spoken = [
        msg["text"] for _, msg in server.messages if msg["type"] == "text" and msg.get("text")
    ]
    assert spoken == ["First sentence.", "Trailing text"]
    assert audio_bytes(down) == AUDIO * 2
    assert server.connections == 1
    assert not any(isinstance(frame, ErrorFrame) for frame in up)


async def test_cancel_frame_stops_active_pipeline_promptly_and_cleans_tasks():
    server = AdversarialServer("stall")
    started = time.monotonic()
    await exercise(
        server,
        [TTSSpeakFrame("Stall."), SleepFrame(0.1), CancelFrame()],
        request_timeout=30,
    )
    assert time.monotonic() - started < 2


async def test_metadata_timeout_closes_partial_handshake():
    server = AdversarialServer("metadata_stall")
    async with serve(server.handler, "127.0.0.1", 0) as endpoint:
        tts = MayaTTSService(
            api_key="test-key",
            url=f"ws://127.0.0.1:{endpoint.sockets[0].getsockname()[1]}",
            connect_timeout=0.1,
        )
        with pytest.raises(TimeoutError):
            await tts._connect_websocket()
        assert tts._websocket is None
    assert server.closed == 1


async def test_shutdown_cancels_scheduled_cancel_sender():
    server = AdversarialServer()
    cancel_started = asyncio.Event()
    cancel_finished = asyncio.Event()

    class BlockedCancelService(MayaTTSService):
        async def _send_cancel(self, context_id):
            cancel_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancel_finished.set()

    async with serve(server.handler, "127.0.0.1", 0) as endpoint:
        tts = BlockedCancelService(
            api_key="test-key", url=f"ws://127.0.0.1:{endpoint.sockets[0].getsockname()[1]}"
        )
        await asyncio.wait_for(
            run_test(
                tts,
                frames_to_send=[
                    LLMFullResponseStartFrame(),
                    LLMTextFrame("Old turn. Waiting"),
                    SleepFrame(0.1),
                    InterruptionFrame(),
                    SleepFrame(0.03),
                    CancelFrame(),
                ],
            ),
            timeout=2,
        )
    assert cancel_started.is_set() and cancel_finished.is_set()
    assert not tts._cancel_tasks
    assert tts._receive_task is None and tts._watchdog_task is None


async def test_new_turn_during_disconnect_reporting_survives_reconnect():
    """A failed old socket must never retire a turn submitted on its replacement."""
    server = AdversarialServer("reconnect_race")
    disconnect_seen = asyncio.Event()
    report_allowed = asyncio.Event()
    async with serve(server.handler, "127.0.0.1", 0) as endpoint:
        tts = MayaTTSService(
            api_key="test-key",
            url=f"ws://127.0.0.1:{endpoint.sockets[0].getsockname()[1]}",
        )
        original_report = tts._report

        async def delayed_report(message):
            if "WebSocket closed" in message and not disconnect_seen.is_set():
                disconnect_seen.set()
                await report_allowed.wait()
            await original_report(message)

        tts._report = delayed_report
        task = asyncio.create_task(run_test(tts, frames_to_send=[SleepFrame(1.2)], start_timeout=2))
        try:
            await asyncio.wait_for(disconnect_seen.wait(), timeout=1)
            assert [frame async for frame in tts.run_tts("New socket.", "new-context")] == [None]
            await tts.flush_audio("new-context")
            await asyncio.wait_for(server.new_text.wait(), timeout=1)
            report_allowed.set()
            down, _ = await asyncio.wait_for(task, timeout=3)
            assert audio_bytes(down) == AUDIO
            assert server.connections == 2
        finally:
            report_allowed.set()
            if not task.done():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
