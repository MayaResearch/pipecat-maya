"""Maya's HTTP speech API, with streamed PCM output for Pipecat transports."""

import asyncio
import audioop
import copy
import json
import math
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from email.message import Message
from email.utils import parsedate_to_datetime
from typing import Any

import aiohttp
from pipecat.frames.frames import (
    AggregatedTextFrame,
    CancelFrame,
    EndFrame,
    ErrorFrame,
    Frame,
    TTSAudioRawFrame,
)
from pipecat.processors.frame_processor import FrameDirection
from pipecat.services.tts_service import TTSService
from pipecat.transcriptions.language import Language
from pipecat.utils.tracing.service_decorators import traced_tts

from .audio import PCMStream
from .settings import (
    MayaTTSSettings,
    default_settings,
    request_settings,
    service_language,
    validated_settings,
)

MAYA_HTTP_URL = "https://tts.mayaresearch.ai/v1/tts"


def _audio_format(content_type: str) -> tuple[int, str]:
    """Read the actual rate and encoding, refusing ambiguous or non-mono audio."""
    message = Message()
    message["content-type"] = content_type
    media_type = message.get_content_type().lower()
    if media_type not in ("audio/l16", "audio/basic"):
        raise ValueError(f"Maya returned unsupported audio content-type {content_type!r}")
    rate_text = message.get_param("rate")
    if not isinstance(rate_text, str) or rate_text not in ("8000", "16000", "24000"):
        raise ValueError("Maya audio content-type must declare rate=8000, 16000 or 24000")
    channels = message.get_param("channels", "1")
    if channels != "1":
        raise ValueError("Maya must return mono audio (channels=1)")
    return int(rate_text), "mulaw" if media_type == "audio/basic" else "pcm_s16le"


class MayaHttpTTSService(TTSService):
    """Synthesize complete text over HTTP and stream decoded PCM to Pipecat.

    Use ``TTSSpeakFrame`` to send a complete known paragraph in one request.
    For incoming LLM sentence streams, prefer ``MayaTTSService`` and its persistent
    WebSocket. Calyx telephony encodings are decoded to PCM before entering the
    pipeline, allowing the selected transport to perform its own wire encoding.
    """

    Settings = MayaTTSSettings
    _settings: MayaTTSSettings

    def __init__(
        self,
        *,
        api_key: str,
        aiohttp_session: aiohttp.ClientSession | None = None,
        url: str = MAYA_HTTP_URL,
        sample_rate: int = 24000,
        provider_sample_rate: int | None = None,
        encoding: str = "pcm_s16le",
        settings: MayaTTSSettings | None = None,
        timeout: float = 120.0,
        max_retries: int = 2,
        retry_delay: float = 0.5,
        **kwargs,
    ):
        """Initialize a reusable HTTP speech client.

        Args:
            api_key: Server-side Maya API key, sent in the Authorization header.
            aiohttp_session: Optional caller-owned session; never closed by this service.
            url: Complete HTTP synthesis endpoint.
            sample_rate: Pipecat output rate. Provider audio is resampled if needed.
            provider_sample_rate: Calyx-only source rate: 8000, 16000 or 24000.
            encoding: Calyx source encoding, ``pcm_s16le`` or ``mulaw``.
            settings: Model, voice, language and optional Native speed.
            timeout: Maximum seconds for each complete request, including audio.
            max_retries: Number of retries before any audio is received, between 0 and 5.
            retry_delay: Initial exponential backoff delay, between 0 and 30 seconds.
            **kwargs: Additional Pipecat TTSService arguments, including aggregation mode.
        """
        if not isinstance(api_key, str) or not api_key.strip():
            raise ValueError("api_key must be a nonempty string")
        if isinstance(sample_rate, bool) or not isinstance(sample_rate, int) or sample_rate <= 0:
            raise ValueError("sample_rate must be a positive integer")
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)):
            raise ValueError("timeout must be a positive finite number")
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout must be a positive finite number")
        if isinstance(max_retries, bool) or not isinstance(max_retries, int):
            raise ValueError("max_retries must be an integer between 0 and 5")
        if not 0 <= max_retries <= 5:
            raise ValueError("max_retries must be an integer between 0 and 5")
        if (
            isinstance(retry_delay, bool)
            or not isinstance(retry_delay, (int, float))
            or not math.isfinite(retry_delay)
            or not 0 <= retry_delay <= 30
        ):
            raise ValueError("retry_delay must be between 0 and 30 seconds")

        self._provider_sample_rate = provider_sample_rate
        self._encoding = encoding
        initial_settings = default_settings(settings)
        self._validate_format_settings(initial_settings)
        super().__init__(
            sample_rate=sample_rate,
            settings=initial_settings,
            push_start_frame=True,
            push_stop_frames=True,
            **kwargs,
        )
        self._api_key = api_key
        self._url = url
        self._session = aiohttp_session
        self._owns_session = aiohttp_session is None
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._active_responses: dict[str, aiohttp.ClientResponse] = {}
        self._active_contexts: set[str] = set()
        self._interrupted_contexts: set[str] = set()
        self._last_request_id: str | None = None
        self._closing = False

    @property
    def last_request_id(self) -> str | None:
        """Most recently received Maya HTTP request ID, useful for support."""
        return self._last_request_id

    def can_generate_metrics(self) -> bool:
        """Support Pipecat TTFB and text usage metrics."""
        return True

    def language_to_service_language(self, language: Language) -> str | None:
        """Resolve a language to its documented Maya API code."""
        return service_language(language)

    def _validate_format_settings(self, settings: MayaTTSSettings) -> None:
        if self._encoding not in ("pcm_s16le", "mulaw"):
            raise ValueError("encoding must be pcm_s16le or mulaw")
        if self._provider_sample_rate is not None and (
            isinstance(self._provider_sample_rate, bool)
            or not isinstance(self._provider_sample_rate, int)
            or self._provider_sample_rate not in (8000, 16000, 24000)
        ):
            raise ValueError("provider_sample_rate must be 8000, 16000 or 24000")
        if settings.model != "Maya Calyx" and (
            self._provider_sample_rate is not None or self._encoding != "pcm_s16le"
        ):
            raise ValueError("provider_sample_rate and mulaw encoding require Maya Calyx")

    async def _update_settings(self, delta: MayaTTSSettings) -> dict[str, Any]:
        """Validate the merged configuration before changing the current settings."""
        candidate = copy.deepcopy(self._settings)
        candidate.apply_update(delta)
        candidate = validated_settings(candidate)
        self._validate_format_settings(candidate)
        if candidate != self._settings:
            await self._flush_pending_text()
        return await super()._update_settings(candidate)

    async def _flush_pending_text(self) -> None:
        remaining = await self._text_aggregator.flush()
        if remaining:
            await self._push_tts_frames(
                AggregatedTextFrame(
                    remaining.text,
                    remaining.type,
                    raw_text=getattr(remaining, "full_match", remaining.text),
                )
            )
        if self._turn_context_id is not None:
            await self._push_sequencer_frames(
                await self._aggregated_frame_sequencer.finalize(self._turn_context_id),
                self._turn_context_id,
            )
            await self.on_turn_context_completed()

    async def process_frame(self, frame: Frame, direction: FrameDirection):
        """Finish buffered LLM text before graceful teardown closes HTTP resources."""
        if isinstance(frame, EndFrame):
            await self._flush_pending_text()
        await super().process_frame(frame, direction)

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._closing:
            raise RuntimeError("Maya HTTP service has stopped")
        if self._session is None or self._session.closed:
            if not self._owns_session:
                raise RuntimeError("The caller-owned aiohttp session is closed")
            self._session = aiohttp.ClientSession(timeout=self._timeout)
        return self._session

    async def _close_session(self) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    def _close_active_responses(self) -> None:
        self._interrupted_contexts.update(self._active_contexts)
        for response in self._active_responses.values():
            response.close()

    async def stop(self, frame: EndFrame):
        """Drain scheduled speech and release the internally owned session."""
        self._closing = True
        try:
            await super().stop(frame)
        finally:
            self._close_active_responses()
            await self._close_session()

    async def cancel(self, frame: CancelFrame):
        """Abort outstanding responses immediately on pipeline cancellation."""
        self._closing = True
        self._close_active_responses()
        try:
            await super().cancel(frame)
        finally:
            await self._close_session()

    async def cleanup(self):
        """Release owned resources; shared sessions remain usable by the caller."""
        self._closing = True
        self._close_active_responses()
        try:
            await super().cleanup()
        finally:
            await self._close_session()

    async def on_audio_context_interrupted(self, context_id: str):
        """Release an interrupted HTTP response instead of leaving synthesis running."""
        if context_id in self._active_contexts:
            self._interrupted_contexts.add(context_id)
        response = self._active_responses.get(context_id)
        if response is not None:
            response.close()
        await super().on_audio_context_interrupted(context_id)

    async def flush_audio(self, context_id: str | None = None):
        """HTTP closes each synthesis at response EOF, so no protocol flush is needed."""

    def _retry_wait(self, attempt: int, retry_after: str | None = None) -> float | None:
        delay = min(30.0, self._retry_delay * 2**attempt)
        if retry_after:
            try:
                server_delay = float(retry_after)
            except ValueError:
                try:
                    when = parsedate_to_datetime(retry_after)
                    if when.tzinfo is None:
                        when = when.replace(tzinfo=timezone.utc)
                    server_delay = (when - datetime.now(timezone.utc)).total_seconds()
                except (TypeError, ValueError, OverflowError):
                    server_delay = 0.0
            if not math.isfinite(server_delay) or server_delay > 30:
                return None
            delay = max(delay, server_delay)
        return max(0.0, delay)

    def _safe_error(self, message: str) -> str:
        suffix = f" (request_id={self._last_request_id})" if self._last_request_id else ""
        result = f"Maya HTTP TTS: {message[:1024]}{suffix}"
        return result.replace(self._api_key, "[redacted]")

    async def _response_error(self, response: aiohttp.ClientResponse) -> str:
        try:
            body = await response.content.read(8192)
        except (aiohttp.ClientError, asyncio.TimeoutError):
            return self._safe_error(f"HTTP {response.status}: unable to read error details")
        try:
            detail = json.loads(body)
        except (ValueError, UnicodeDecodeError):
            detail = None
        if isinstance(detail, dict):
            request_id = detail.get("request_id")
            if isinstance(request_id, str):
                self._last_request_id = request_id[:200]
            reason = str(detail.get("error", detail.get("message", "request failed")))
            for key in (
                "available_voices",
                "available_models",
                "available_languages",
                "speed_range",
            ):
                if key in detail:
                    reason += f"; {key}={detail[key]}"
        else:
            reason = "non-JSON error response"
        return self._safe_error(f"HTTP {response.status}: {reason}")

    @traced_tts
    async def run_tts(self, text: str, context_id: str) -> AsyncGenerator[Frame | None, None]:
        """Send complete text and yield PCM immediately as audio arrives.

        Retries are bounded and stop as soon as any audio body bytes arrive,
        including bytes held by the resampler. This prevents replaying speech
        after a partial response or silently joining two synthesis attempts.
        """
        if not isinstance(text, str) or not text.strip():
            yield ErrorFrame(error="Maya HTTP TTS requires nonempty plain text")
            return
        payload = {"text": text, **request_settings(self._settings)}
        if self._settings.model == "Maya Calyx":
            payload["encoding"] = self._encoding
            if self._provider_sample_rate is not None:
                payload["sample_rate"] = self._provider_sample_rate
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
            "User-Agent": "pipecat-maya/0.1.0",
        }
        received_audio = False
        emitted_audio = False
        self._last_request_id = None
        self._active_contexts.add(context_id)
        try:
            session = await self._get_session()
            for attempt in range(self._max_retries + 1):
                if context_id in self._interrupted_contexts:
                    return
                wait = None
                try:
                    async with session.post(
                        self._url,
                        json=payload,
                        headers=headers,
                        timeout=self._timeout,
                        allow_redirects=False,
                        raise_for_status=False,
                    ) as response:
                        self._active_responses[context_id] = response
                        self._last_request_id = response.headers.get("x-request-id")
                        if response.status != 200:
                            error_message = await self._response_error(response)
                            retryable = response.status == 429 or 500 <= response.status < 600
                            if retryable and attempt < self._max_retries:
                                wait = self._retry_wait(
                                    attempt, response.headers.get("Retry-After")
                                )
                            if wait is None:
                                yield ErrorFrame(error=error_message)
                                return
                        else:
                            rate, encoding = _audio_format(response.headers.get("Content-Type", ""))
                            stream = PCMStream(rate, self.sample_rate)
                            await self.start_tts_usage_metrics(text)
                            async for chunk in response.content.iter_chunked(4096):
                                if context_id in self._interrupted_contexts:
                                    return
                                if not chunk:
                                    continue
                                received_audio = True
                                pcm = audioop.ulaw2lin(chunk, 2) if encoding == "mulaw" else chunk
                                audio = stream.feed(pcm)
                                if audio:
                                    if not emitted_audio:
                                        await self.stop_ttfb_metrics()
                                    emitted_audio = True
                                    yield TTSAudioRawFrame(audio, self.sample_rate, 1, context_id)
                            if context_id in self._interrupted_contexts:
                                return
                            tail = stream.feed(b"", final=True)
                            if tail:
                                if not emitted_audio:
                                    await self.stop_ttfb_metrics()
                                emitted_audio = True
                                yield TTSAudioRawFrame(tail, self.sample_rate, 1, context_id)
                            if not emitted_audio:
                                yield ErrorFrame(
                                    error=self._safe_error("response contained no audio")
                                )
                            return
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    if context_id in self._interrupted_contexts:
                        return
                    if received_audio or attempt >= self._max_retries:
                        yield ErrorFrame(error=self._safe_error(str(exc)))
                        return
                    wait = self._retry_wait(attempt)
                finally:
                    self._active_responses.pop(context_id, None)
                if wait is not None:
                    await asyncio.sleep(wait)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if context_id not in self._interrupted_contexts:
                yield ErrorFrame(error=self._safe_error(str(exc)))
        finally:
            self._active_contexts.discard(context_id)
            self._interrupted_contexts.discard(context_id)
            await self.stop_ttfb_metrics()
