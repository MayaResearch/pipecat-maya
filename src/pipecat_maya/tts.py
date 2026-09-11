"""Persistent Maya WebSocket v2 TTS following Pipecat's audio-context lifecycle."""

import asyncio
import base64
import copy
import json
import math
import time
from collections import deque
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field

from loguru import logger
from pipecat.frames.frames import (
    AggregatedTextFrame,
    EndFrame,
    ErrorFrame,
    Frame,
    InterruptionFrame,
    TTSAudioRawFrame,
    TTSStoppedFrame,
)
from pipecat.processors.frame_processor import FrameDirection, FrameProcessorSetup
from pipecat.services.tts_service import TextAggregationMode, WebsocketTTSService
from pipecat.utils.tracing.service_decorators import traced_tts
from websockets.exceptions import InvalidStatus
from websockets.protocol import State

from .audio import PCMStream
from .settings import (
    MayaTTSSettings,
    default_settings,
    request_settings,
    service_language,
    validated_settings,
)


@dataclass
class _Turn:
    stream: PCMStream
    websocket: object
    closed: bool = False
    received_audio: bool = False
    request_ids: list[str] = field(default_factory=list)
    updated_at: float = field(default_factory=time.monotonic)
    done: asyncio.Event = field(default_factory=asyncio.Event)


class MayaTTSService(WebsocketTTSService):
    """Stream Maya speech over one authenticated WebSocket per conversation.

    Pipecat aggregates LLM tokens into sentences. Sentences share one context and
    are sent without waiting for synthesis. The response-end frame closes that
    context. Interruption discards local contexts immediately and sends targeted
    cancellation; no acknowledgement is required before the next turn.
    """

    Settings = MayaTTSSettings

    def __init__(
        self,
        *,
        api_key: str,
        settings: MayaTTSSettings | None = None,
        url: str = "wss://tts.mayaresearch.ai/v1/tts/stream",
        sample_rate: int = 24000,
        connect_timeout: float = 15,
        request_timeout: float = 60,
        **kwargs,
    ):
        """Initialize the service.

        Args:
            api_key: Server-side Maya API key, sent only in the Authorization header.
            settings: Model, voice, language, and Native-only speaking speed.
            url: WebSocket endpoint override (use ws:// only for local tests).
            sample_rate: Pipecat PCM output rate. Actual input rate comes from metadata.
            connect_timeout: Deadline for connection and metadata readiness, seconds.
            request_timeout: Maximum time without progress on a submitted turn, seconds.
            **kwargs: Additional Pipecat service options. Sentence aggregation is required.
        """
        if not isinstance(api_key, str) or not api_key.strip():
            raise ValueError("MAYA_API_KEY is required")
        if isinstance(sample_rate, bool) or not isinstance(sample_rate, int) or sample_rate <= 0:
            raise ValueError("sample_rate must be a positive integer")
        for timeout in (connect_timeout, request_timeout):
            if (
                isinstance(timeout, bool)
                or not isinstance(timeout, (int, float))
                or not math.isfinite(timeout)
                or timeout <= 0
            ):
                raise ValueError("Timeouts must be finite positive numbers")
        if not kwargs.pop("reuse_context_id_within_turn", True):
            raise ValueError("Maya requires one shared context per LLM turn")
        aggregation = kwargs.pop("text_aggregation_mode", TextAggregationMode.SENTENCE)
        if aggregation != TextAggregationMode.SENTENCE or "aggregate_sentences" in kwargs:
            raise ValueError("Maya requires sentence aggregation; send LLM tokens to the service")
        super().__init__(
            settings=default_settings(settings),
            sample_rate=sample_rate,
            text_aggregation_mode=TextAggregationMode.SENTENCE,
            push_start_frame=True,
            push_stop_frames=False,
            push_text_frames=True,
            pause_frame_processing=False,
            stop_frame_timeout_s=request_timeout + 5,
            **kwargs,
        )
        self._api_key = api_key
        self._url = url
        self._connect_timeout = connect_timeout
        self._request_timeout = request_timeout
        self._receive_task = None
        self._watchdog_task = None
        self._connect_lock = asyncio.Lock()
        self._cancel_tasks: set[asyncio.Task] = set()
        self._turns: dict[str, _Turn] = {}
        self._retired: deque[str] = deque(maxlen=256)
        self._input_rate = 24000
        self._session_id: str | None = None
        self._permanent_failure = False
        self._closing = False

    @property
    def session_id(self) -> str | None:
        """Provider session identifier for support, set after metadata readiness."""
        return self._session_id

    def can_generate_metrics(self) -> bool:
        """Support TTFB and character usage metrics."""
        return True

    def language_to_service_language(self, language):
        """Translate a supported Pipecat language to Maya's language code."""
        return service_language(language)

    async def setup(self, setup: FrameProcessorSetup):
        """Initialize Pipecat resources, then complete the v2 readiness handshake."""
        await super().setup(setup)
        await self._connect()
        self._watchdog_task = self.create_task(self._watchdog(), name="maya-watchdog")

    async def _connect(self):
        if self._closing:
            raise RuntimeError("Maya service has stopped")
        await super()._connect()
        async with self._connect_lock:
            await self._connect_websocket()
            if self._receive_task is None or self._receive_task.done():
                self._receive_task = self.create_task(self._receive_loop(), name="maya-receive")

    async def _connect_websocket(self):
        if self._websocket is not None and self._websocket.state is State.OPEN:
            return
        if self._permanent_failure:
            raise RuntimeError("Maya connection requires corrected credentials or settings")
        ws = None
        try:
            ws = await self._websocket_connect(
                self._url,
                additional_headers={"Authorization": f"Bearer {self._api_key}"},
                user_agent_header="pipecat-maya/0.1.0",
                open_timeout=self._connect_timeout,
                ping_interval=20,
                ping_timeout=20,
                max_size=8 * 1024 * 1024,
            )
            async with asyncio.timeout(self._connect_timeout):
                await ws.send(
                    json.dumps({"type": "start", "v2": True, **request_settings(self._settings)})
                )
                metadata = json.loads(await ws.recv())
            if not isinstance(metadata, dict) or metadata.get("type") != "metadata":
                self._permanent_failure = True
                detail = (
                    metadata.get("error", "metadata required")
                    if isinstance(metadata, dict)
                    else "metadata required"
                )
                raise ValueError(f"Maya rejected start: {detail}")
            rate = metadata.get("sample_rate")
            if (
                not isinstance(rate, int)
                or isinstance(rate, bool)
                or not 8000 <= rate <= 192000
                or metadata.get("channels") != 1
                or metadata.get("encoding") != "pcm_s16le"
            ):
                self._permanent_failure = True
                raise ValueError(
                    "Unsupported Maya metadata: expected mono pcm_s16le with a valid rate"
                )
            self._input_rate = rate
            self._session_id = metadata.get("session_id")
            self._websocket = ws
            logger.debug(f"Maya connected session_id={self._session_id}, input_rate={rate}")
            await self._call_event_handler("on_connected")
        except BaseException as error:
            if isinstance(error, InvalidStatus) and 400 <= error.response.status_code < 500:
                self._permanent_failure = True
            if ws is not None:
                await ws.close()
            raise

    async def _disconnect_websocket(self):
        ws, self._websocket = self._websocket, None
        if ws is not None:
            await ws.close()
            await self._call_event_handler("on_disconnected")

    async def _disconnect(self):
        self._closing = True
        await super()._disconnect()
        for name in ("_receive_task", "_watchdog_task"):
            task = getattr(self, name)
            setattr(self, name, None)
            if task is not None and task is not asyncio.current_task():
                await self.cancel_task(task)
        for task in list(self._cancel_tasks):
            await self.cancel_task(task)
        self._cancel_tasks.clear()
        await self._disconnect_websocket()
        for turn in self._turns.values():
            turn.done.set()
        self._turns.clear()
        await self.stop_all_metrics()

    async def _report(self, message: str):
        safe = message.replace(self._api_key, "[redacted]")
        await self.push_error(f"Maya TTS: {safe}")

    async def _receive_loop(self):
        while not self._disconnecting:
            socket = self._websocket
            session_id = self._session_id
            try:
                await self._receive_messages()
                if self._disconnecting:
                    return
                raise ConnectionError("WebSocket closed")
            except asyncio.CancelledError:
                raise
            except Exception as error:
                # Retire only the failed connection's work before reporting. An
                # error handler can await while a new turn opens a replacement.
                async with self._connect_lock:
                    targets = [cid for cid, turn in self._turns.items() if turn.websocket is socket]
                    for context_id in targets:
                        await self._finish(context_id, successful=False)
                    if self._websocket is socket:
                        await self._disconnect_websocket()
                await self._report(f"{type(error).__name__}: {error}; session_id={session_id}")
                if self._websocket is not None and self._websocket.state is State.OPEN:
                    continue
                if not self._reconnect_on_error or self._permanent_failure:
                    return
                for attempt in range(3):
                    if self._disconnecting:
                        return
                    await asyncio.sleep(min(0.5 * 2**attempt, 2))
                    try:
                        async with self._connect_lock:
                            await self._connect_websocket()
                        break
                    except Exception as reconnect_error:
                        if self._permanent_failure or attempt == 2:
                            await self._report(f"Reconnect failed: {reconnect_error}")
                            return

    async def _receive_messages(self):
        ws = self._websocket
        session_id = self._session_id
        if ws is None:
            raise ConnectionError("WebSocket not ready")
        async for raw in ws:
            message = json.loads(raw)
            if not isinstance(message, dict):
                raise ValueError("Invalid Maya message")
            kind = message.get("type")
            context_id = message.get("context_id")
            if kind == "pong":
                continue
            if kind == "error":
                if context_id and context_id not in self._turns:
                    continue
                targets = [
                    cid
                    for cid, turn in self._turns.items()
                    if turn.websocket is ws and (not context_id or cid == context_id)
                ]
                await self._report(
                    f"{message.get('error', 'provider error')}; context_id={context_id}; "
                    f"session_id={session_id}"
                )
                for target in targets:
                    await self._finish(target, successful=False)
                    await self._send_cancel(target)
                continue
            if context_id not in self._turns:
                continue  # Includes delayed audio and terminators for interrupted turns.
            turn = self._turns[context_id]
            turn.updated_at = time.monotonic()
            if kind == "audio":
                request_id = message.get("request_id")
                if isinstance(request_id, str) and request_id not in turn.request_ids:
                    turn.request_ids.append(request_id)
                    logger.debug(
                        f"Maya request_id={request_id}, context_id={context_id}, "
                        f"session_id={self._session_id}"
                    )
                try:
                    data = base64.b64decode(message["audio"], validate=True)
                    audio = turn.stream.feed(data)
                except (ValueError, KeyError, TypeError) as error:
                    await self._report(f"Invalid audio for context_id={context_id}: {error}")
                    await self._finish(context_id, successful=False)
                    await self._send_cancel(context_id)
                    continue
                if data:
                    turn.received_audio = True
                if audio:
                    await self.stop_ttfb_metrics()
                    await self.append_to_audio_context(
                        context_id,
                        TTSAudioRawFrame(audio, self.sample_rate, 1, context_id=context_id),
                    )
            elif kind == "end":
                reported = message.get("request_ids")
                request_ids = reported if isinstance(reported, list) else turn.request_ids
                logger.debug(
                    f"Maya turn complete context_id={context_id}, "
                    f"session_id={message.get('session_id') or self._session_id}, "
                    f"request_ids={request_ids}"
                )
                await self._finish(context_id, successful=True)
            elif kind == "cancelled":
                await self._finish(context_id, successful=False)

    async def _finish(self, context_id: str, *, successful: bool):
        turn = self._turns.pop(context_id, None)
        if turn is None:
            return
        self._retired.append(context_id)
        try:
            if successful:
                try:
                    tail = turn.stream.feed(b"", final=True)
                    if tail:
                        await self.append_to_audio_context(
                            context_id,
                            TTSAudioRawFrame(tail, self.sample_rate, 1, context_id=context_id),
                        )
                    if not turn.received_audio:
                        await self._report(f"Turn completed without audio; context_id={context_id}")
                except ValueError as error:
                    await self._report(f"{error}; context_id={context_id}")
            await self.stop_ttfb_metrics()
            if self.audio_context_available(context_id):
                await self.append_to_audio_context(
                    context_id, TTSStoppedFrame(context_id=context_id)
                )
                await self.remove_audio_context(context_id)
        finally:
            turn.done.set()

    async def _send_cancel(self, context_id: str):
        if self._websocket is not None and self._websocket.state is State.OPEN:
            try:
                async with asyncio.timeout(2):
                    await self._websocket.send(
                        json.dumps({"type": "cancel", "context_id": context_id})
                    )
            except Exception:
                # The receive loop owns reconnection. Local playback has already
                # discarded this context, even if the peer cannot receive cancel.
                pass

    async def _handle_interruption(self, frame: InterruptionFrame, direction: FrameDirection):
        turns, self._turns = self._turns, {}
        for context_id, turn in turns.items():
            self._retired.append(context_id)
            turn.done.set()
        # Pipecat clears its playback contexts before waiting on any network I/O.
        await super()._handle_interruption(frame, direction)
        for context_id in turns:
            task = self.create_task(self._send_cancel(context_id), name="maya-cancel")
            self._cancel_tasks.add(task)
            task.add_done_callback(self._cancel_tasks.discard)

    async def flush_audio(self, context_id: str | None = None):
        """Close an open turn once, without waiting for the final audio."""
        target = context_id or self.get_active_audio_context_id()
        turn = self._turns.get(target)
        if turn is None or turn.closed:
            return
        turn.closed = True
        turn.updated_at = time.monotonic()
        try:
            await self._websocket.send(
                json.dumps({"type": "text", "context_id": target, "text": "", "continue": False})
            )
        except Exception as error:
            await self._report(f"Turn close failed: {error}; context_id={target}")
            await self._finish(target, successful=False)

    async def _flush_pending_text(self):
        remaining = await self._text_aggregator.flush()
        if remaining:
            await self._push_tts_frames(
                AggregatedTextFrame(
                    remaining.text,
                    remaining.type,
                    raw_text=getattr(remaining, "full_match", remaining.text),
                )
            )

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        """Flush residual LLM text before graceful shutdown closes the socket."""
        if isinstance(frame, EndFrame):
            await self._flush_pending_text()
            await self._push_sequencer_frames(
                await self._aggregated_frame_sequencer.finalize(self._turn_context_id),
                self._turn_context_id,
            )
            await self.on_turn_context_completed()
        await super().process_frame(frame, direction)

    async def _update_settings(self, delta: MayaTTSSettings) -> dict:
        candidate = copy.deepcopy(self._settings)
        candidate.apply_update(delta)
        candidate = validated_settings(candidate)
        if candidate == self._settings:
            return {}
        # Text already accepted belongs to the old settings, even if sentence
        # lookahead has not yet released it to synthesis.
        await self._flush_pending_text()
        if self._turn_context_id:
            await self._push_sequencer_frames(
                await self._aggregated_frame_sequencer.finalize(self._turn_context_id),
                self._turn_context_id,
            )
        turns = list(self._turns.items())
        for context_id, _ in turns:
            await self.flush_audio(context_id)
        async with asyncio.timeout(self._request_timeout):
            for _, turn in turns:
                await turn.done.wait()
        changed = await super()._update_settings(candidate)
        if self._turn_context_id:
            self._turn_context_id = None
            self._turn_context_id = self.create_context_id()
        self._permanent_failure = False
        if self._websocket is not None:
            # Sticky settings, including language=None, get a fresh v2 handshake.
            if self._receive_task:
                await self.cancel_task(self._receive_task)
                self._receive_task = None
            await self._disconnect_websocket()
            await self._connect()
        return changed

    async def _watchdog(self):
        while True:
            await asyncio.sleep(min(1, self._request_timeout / 2))
            for context_id, turn in list(self._turns.items()):
                if time.monotonic() - turn.updated_at >= self._request_timeout:
                    await self._report(f"Audio progress timed out; context_id={context_id}")
                    await self._finish(context_id, successful=False)
                    await self._send_cancel(context_id)

    @traced_tts
    async def run_tts(self, text: str, context_id: str) -> AsyncGenerator[Frame | None, None]:
        """Send an aggregated sentence; the receive task streams the resulting PCM."""
        if not text.strip():
            yield None
            return
        if context_id in self._retired:
            yield ErrorFrame("Maya context has ended; start a new turn before sending more text")
            return
        try:
            await self._connect()
            if context_id not in self._turns:
                if not self.audio_context_available(context_id):
                    await self.create_audio_context(context_id)
                self._turns[context_id] = _Turn(
                    PCMStream(self._input_rate, self.sample_rate), self._websocket
                )
                logger.debug(f"Maya context_id={context_id}, session_id={self._session_id}")
            turn = self._turns[context_id]
            if turn.closed:
                raise ValueError("Cannot append to a closed Maya turn")
            turn.updated_at = time.monotonic()
            message = {"type": "text", "context_id": context_id, "text": text, "continue": True}
            if self._settings.model == "Maya 2 Native":
                message["speed"] = self._settings.speed if self._settings.speed is not None else 1.0
            await self._websocket.send(json.dumps(message))
            await self.start_tts_usage_metrics(text)
            yield None
        except asyncio.CancelledError:
            raise
        except Exception as error:
            await self._report(f"Synthesis failed: {error}; context_id={context_id}")
            await self._finish(context_id, successful=False)
