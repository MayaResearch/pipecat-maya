import asyncio
import audioop
import json

import aiohttp
import numpy as np
import pytest
from multidict import CIMultiDict
from pipecat.frames.frames import (
    ErrorFrame,
    LLMFullResponseStartFrame,
    LLMTextFrame,
    TTSAudioRawFrame,
    TTSSpeakFrame,
    TTSUpdateSettingsFrame,
)
from pipecat.tests.utils import run_test
from pipecat.utils.types import NOT_GIVEN

from pipecat_maya.http import MayaHttpTTSService, _audio_format


class FakeContent:
    def __init__(self, chunks=(), body=b""):
        self.chunks = chunks
        self.body = body
        self.iterated = False

    async def read(self, size):
        return self.body[:size]

    async def iter_chunked(self, size):
        self.iterated = True
        for chunk in self.chunks:
            if isinstance(chunk, Exception):
                raise chunk
            yield chunk


class FakeResponse:
    def __init__(self, chunks=(), *, status=200, headers=None, body=b""):
        self.status = status
        self.headers = CIMultiDict(
            headers
            or {"Content-Type": "audio/L16; rate=24000; channels=1", "x-request-id": "req-1"}
        )
        self.content = FakeContent(chunks, body)
        self.closed = False
        self.exited = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.exited = True
        self.close()

    def close(self):
        self.closed = True


class FakeSession:
    def __init__(self, responses=(), **kwargs):
        self.responses = iter(responses)
        self.requests = []
        self.closed = False
        self.close_count = 0

    def post(self, url, **kwargs):
        self.requests.append((url, kwargs))
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return response

    async def close(self):
        self.closed = True
        self.close_count += 1


def service_for(*responses, **kwargs):
    session = FakeSession(responses)
    service = MayaHttpTTSService(api_key="secret-key", aiohttp_session=session, **kwargs)
    service._sample_rate = kwargs.get("sample_rate", 24000)
    return service, session


async def collect(service, text="नमस्ते।", context_id="ctx-1"):
    return [frame async for frame in service.run_tts(text, context_id)]


def pcm(frames):
    return b"".join(frame.audio for frame in frames if isinstance(frame, TTSAudioRawFrame))


async def test_raw_pcm_stream_preserves_arbitrary_chunk_boundaries_and_request_contract():
    data = np.arange(1000, dtype="<i2").tobytes()
    response = FakeResponse([data[:1], data[1:903], b"", data[903:]])
    service, session = service_for(response)
    frames = await collect(service, text="पूरा वाक्य। दूसरा वाक्य।")
    assert pcm(frames) == data
    assert all(frame.context_id == "ctx-1" for frame in frames)
    assert all(frame.sample_rate == 24000 and frame.num_channels == 1 for frame in frames)
    url, request = session.requests[0]
    assert url == "https://tts.mayaresearch.ai/v1/tts"
    assert request["json"] == {
        "text": "पूरा वाक्य। दूसरा वाक्य।",
        "model": "Maya 2 Native",
        "voice": "Ananya",
        "speed": 1.0,
    }
    assert request["headers"]["Authorization"] == "Bearer secret-key"
    assert request["headers"]["User-Agent"]
    assert request["allow_redirects"] is False
    assert request["raise_for_status"] is False
    assert service.last_request_id == "req-1"
    assert response.exited and response.closed


async def test_content_type_actual_rate_wins_and_resampling_flushes_tail():
    data = (10000 * np.sin(np.arange(2400) * 0.05)).astype("<i2").tobytes()
    response = FakeResponse([data[:201], data[201:3013], data[3013:]])
    service, _ = service_for(response, sample_rate=16000)
    frames = await collect(service)
    assert len(pcm(frames)) == 3200
    assert all(frame.sample_rate == 16000 and frame.context_id == "ctx-1" for frame in frames)


async def test_mulaw_decodes_before_pipeline_and_uses_actual_header_rate():
    data = bytes(range(256)) * 5
    response = FakeResponse(
        [data[:7], data[7:]], headers={"Content-Type": "audio/basic; rate=8000"}
    )
    settings = MayaHttpTTSService.Settings(model="Maya Calyx", voice="Aarav")
    service, session = service_for(
        response, settings=settings, provider_sample_rate=16000, encoding="mulaw", sample_rate=8000
    )
    frames = await collect(service)
    assert pcm(frames) == audioop.ulaw2lin(data, 2)
    assert session.requests[0][1]["json"]["sample_rate"] == 16000
    assert session.requests[0][1]["json"]["encoding"] == "mulaw"
    assert "speed" not in session.requests[0][1]["json"]


@pytest.mark.parametrize("status", [400, 401, 403, 404, 302])
async def test_nonretryable_status_is_checked_before_audio(status):
    response = FakeResponse(
        [b"not audio"],
        status=status,
        body=json.dumps({"error": "invalid_key", "request_id": "bad-req"}).encode(),
    )
    service, session = service_for(response)
    frames = await collect(service)
    assert len(session.requests) == 1
    assert len(frames) == 1 and isinstance(frames[0], ErrorFrame)
    assert str(status) in frames[0].error and "bad-req" in frames[0].error
    assert not response.content.iterated


@pytest.mark.parametrize("status", [429, 500, 502, 503])
async def test_transient_error_retries_then_streams(status):
    bad = FakeResponse(status=status, body=b'{"error":"origin_error"}')
    good = FakeResponse([b"\x00\x01" * 20])
    service, session = service_for(bad, good, retry_delay=0)
    frames = await collect(service)
    assert len(session.requests) == 2
    assert pcm(frames) == b"\x00\x01" * 20
    assert bad.closed and good.closed


async def test_retry_limit_is_bounded():
    service, session = service_for(
        *(FakeResponse(status=502) for _ in range(3)), retry_delay=0, max_retries=2
    )
    frames = await collect(service)
    assert len(session.requests) == 3
    assert isinstance(frames[0], ErrorFrame)


async def test_large_retry_after_is_reported_instead_of_retrying_too_early():
    response = FakeResponse(status=429, headers={"Retry-After": "120"})
    service, session = service_for(response, retry_delay=0)
    frames = await collect(service)
    assert len(session.requests) == 1
    assert isinstance(frames[0], ErrorFrame)


async def test_midstream_failure_never_replays_audio():
    response = FakeResponse([b"\x00\x10" * 500, aiohttp.ClientPayloadError("truncated")])
    service, session = service_for(response, retry_delay=0)
    frames = await collect(service)
    assert len(session.requests) == 1
    assert len(pcm(frames)) == 1000
    assert isinstance(frames[-1], ErrorFrame)


async def test_failure_after_bytes_buffered_by_resampler_is_also_not_retried():
    response = FakeResponse([b"\x00\x10", aiohttp.ClientPayloadError("truncated")])
    service, session = service_for(response, retry_delay=0, sample_rate=8000)
    frames = await collect(service)
    assert len(session.requests) == 1
    assert len(pcm(frames)) == 0
    assert isinstance(frames[-1], ErrorFrame)


async def test_connection_failure_before_audio_retries():
    good = FakeResponse([b"\x01\x00" * 80])
    service, session = service_for(aiohttp.ClientConnectionError("disconnect"), good, retry_delay=0)
    frames = await collect(service)
    assert len(session.requests) == 2
    assert len(pcm(frames)) == 160


@pytest.mark.parametrize("chunks", [[], [b"\x00"]])
async def test_empty_or_truncated_pcm_reports_error(chunks):
    service, session = service_for(FakeResponse(chunks))
    frames = await collect(service)
    assert len(session.requests) == 1
    assert not pcm(frames)
    assert isinstance(frames[-1], ErrorFrame)


@pytest.mark.parametrize(
    "content_type",
    ["application/json", "audio/L16", "audio/L16; rate=44100", "audio/L16; rate=24000; channels=2"],
)
async def test_invalid_audio_headers_never_produce_audio(content_type):
    response = FakeResponse([b"oops"], headers={"Content-Type": content_type})
    service, session = service_for(response)
    frames = await collect(service)
    assert isinstance(frames[0], ErrorFrame)
    assert not response.content.iterated
    assert len(session.requests) == 1


def test_case_insensitive_mime_type_and_quoted_rate():
    assert _audio_format('Audio/L16; rate="16000"; channels=1') == (16000, "pcm_s16le")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"provider_sample_rate": 8000},
        {"encoding": "mulaw"},
        {"encoding": "mp3"},
        {"provider_sample_rate": True},
        {"sample_rate": 0},
        {"sample_rate": True},
        {"timeout": float("inf")},
        {"max_retries": -1},
        {"retry_delay": float("nan")},
    ],
)
def test_invalid_configuration_fails_before_network(kwargs):
    with pytest.raises(ValueError):
        MayaHttpTTSService(api_key="secret-key", **kwargs)


async def test_runtime_settings_update_is_transactional():
    service, _ = service_for()
    with pytest.raises(ValueError):
        await service._update_settings(MayaHttpTTSService.Settings(voice="Aarav"))
    assert service._settings.voice == "Ananya"
    delta = MayaHttpTTSService.Settings(model="Maya Calyx", voice="Aarav")
    changed = await service._update_settings(delta)
    assert service._settings.model == "Maya Calyx"
    assert "model" in changed and "voice" in changed
    assert delta.language is NOT_GIVEN


async def test_calyx_format_blocks_incompatible_model_update():
    service, _ = service_for(
        settings=MayaHttpTTSService.Settings(model="Maya Calyx", voice="Aarav"),
        provider_sample_rate=8000,
        encoding="mulaw",
    )
    with pytest.raises(ValueError):
        await service._update_settings(
            MayaHttpTTSService.Settings(model="Maya 2 Native", voice="Ananya")
        )
    assert service._settings.model == "Maya Calyx"


async def test_interruption_closes_response_and_discards_remaining_audio():
    response = FakeResponse([b"\x01\x00" * 100, b"\x02\x00" * 100])
    service, session = service_for(response)
    generator = service.run_tts("नमस्ते।", "ctx-1")
    first = await anext(generator)
    assert isinstance(first, TTSAudioRawFrame)
    await service.on_audio_context_interrupted("ctx-1")
    assert response.closed
    assert [frame async for frame in generator] == []
    assert len(session.requests) == 1 and not session.closed


async def test_generator_cancellation_releases_response():
    response = FakeResponse([b"\x01\x00" * 100, b"\x02\x00" * 100])
    service, _ = service_for(response)
    generator = service.run_tts("नमस्ते।", "ctx-1")
    await anext(generator)
    await generator.aclose()
    assert response.closed
    assert not service._active_responses


async def test_external_session_is_never_closed():
    service, session = service_for()
    await service.cleanup()
    await service.cleanup()
    assert session.close_count == 0


async def test_internal_session_reused_and_closed_once(monkeypatch):
    session = FakeSession([FakeResponse([b"\x01\x00" * 100]) for _ in range(2)])
    monkeypatch.setattr("pipecat_maya.http.aiohttp.ClientSession", lambda **kwargs: session)
    service = MayaHttpTTSService(api_key="secret-key")
    service._sample_rate = 24000
    await collect(service)
    await collect(service, context_id="ctx-2")
    assert len(session.requests) == 2
    await service.cleanup()
    await service.cleanup()
    assert session.close_count == 1


async def test_key_is_redacted_from_provider_error():
    response = FakeResponse(status=400, body=b'{"error":"secret-key was refused"}')
    service, _ = service_for(response)
    frames = await collect(service)
    assert "secret-key" not in frames[0].error
    assert "[redacted]" in frames[0].error


async def test_error_body_read_failure_does_not_retry_invalid_request():
    response = FakeResponse(status=400)

    async def broken_read(size):
        raise aiohttp.ClientPayloadError("error body disconnected")

    response.content.read = broken_read
    service, session = service_for(response, retry_delay=0)
    frames = await collect(service)
    assert len(session.requests) == 1
    assert isinstance(frames[0], ErrorFrame)
    assert "HTTP 400" in frames[0].error


async def test_interruption_during_backoff_prevents_new_request(monkeypatch):
    service, session = service_for(FakeResponse(status=502))

    async def interrupted_sleep(delay):
        await service.on_audio_context_interrupted("ctx-1")

    monkeypatch.setattr("pipecat_maya.http.asyncio.sleep", interrupted_sleep)
    frames = await collect(service)
    assert len(session.requests) == 1
    assert not frames


async def test_valid_options_survive_provider_error_for_actionable_feedback():
    response = FakeResponse(
        status=400,
        body=b'{"error":"invalid voice","available_voices":["Ananya","Arjun"]}',
    )
    service, _ = service_for(response)
    frames = await collect(service)
    assert "Ananya" in frames[0].error and "Arjun" in frames[0].error


async def test_complete_paragraph_flows_through_real_pipecat_pipeline():
    data = b"\x01\x00" * 2400
    service, session = service_for(FakeResponse([data]))
    down, up = await asyncio.wait_for(
        run_test(service, frames_to_send=[TTSSpeakFrame("नमस्ते। आपका दिन अच्छा हो।")]),
        timeout=10,
    )
    assert len(session.requests) == 1
    assert session.requests[0][1]["json"]["text"] == "नमस्ते। आपका दिन अच्छा हो।"
    assert pcm(down) == data
    assert not any(isinstance(frame, ErrorFrame) for frame in up)
    assert not session.closed


async def test_end_frame_preserves_pending_llm_tail_without_reopening_session(monkeypatch):
    data = b"\x01\x00" * 2400
    sessions = []

    def create_session(**kwargs):
        session = FakeSession([FakeResponse([data]), FakeResponse([data])])
        sessions.append(session)
        return session

    monkeypatch.setattr("pipecat_maya.http.aiohttp.ClientSession", create_session)
    service = MayaHttpTTSService(api_key="secret-key")
    down, up = await asyncio.wait_for(
        run_test(
            service,
            frames_to_send=[
                LLMFullResponseStartFrame(),
                LLMTextFrame("First sentence. Trailing text"),
            ],
        ),
        timeout=10,
    )
    assert len(sessions) == 1
    assert [request[1]["json"]["text"] for request in sessions[0].requests] == [
        "First sentence.",
        "Trailing text",
    ]
    assert pcm(down) == data * 2
    assert not any(isinstance(frame, ErrorFrame) for frame in up)
    assert sessions[0].closed


async def test_settings_change_synthesizes_buffered_text_in_original_voice():
    data = b"\x01\x00" * 2400
    service, session = service_for(*(FakeResponse([data]) for _ in range(3)))
    down, up = await asyncio.wait_for(
        run_test(
            service,
            frames_to_send=[
                LLMFullResponseStartFrame(),
                LLMTextFrame("Old voice. Pending"),
                TTSUpdateSettingsFrame(delta=MayaHttpTTSService.Settings(voice="Arjun")),
                TTSSpeakFrame("New voice."),
            ],
        ),
        timeout=10,
    )
    assert [
        (request[1]["json"]["text"], request[1]["json"]["voice"]) for request in session.requests
    ] == [("Old voice.", "Ananya"), ("Pending", "Ananya"), ("New voice.", "Arjun")]
    assert pcm(down) == data * 3
    assert not any(isinstance(frame, ErrorFrame) for frame in up)
