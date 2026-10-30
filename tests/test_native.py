"""Self-hosted contract checks; synthetic PCM, no GPU or provider calls."""

import asyncio

import aiohttp
import numpy as np
import pytest
from aiohttp import web
from pipecat.frames.frames import (
    ErrorFrame,
    LLMFullResponseStartFrame,
    LLMTextFrame,
    TTSAudioRawFrame,
    TTSSpeakFrame,
    TTSUpdateSettingsFrame,
)
from pipecat.tests.utils import run_test
from pipecat.transcriptions.language import Language
from test_http import FakeResponse, FakeSession, pcm

from pipecat_maya import MayaNativeTTSService

HEADERS = {
    "Content-Type": "audio/pcm",
    "X-Sample-Rate": "24000",
    "X-Channels": "1",
    "X-Encoding": "pcm_s16le",
    "X-Request-Id": "native-1",
}


def response(chunks=(), **kwargs):
    return FakeResponse(chunks, headers=HEADERS.copy(), **kwargs)


def service_for(*responses, **kwargs):
    session = FakeSession(responses)
    service = MayaNativeTTSService(aiohttp_session=session, **kwargs)
    service._sample_rate = kwargs.get("sample_rate", 24000)
    return service, session


async def collect(service, text="नमस्ते।", context="ctx"):
    return [frame async for frame in service.run_tts(text, context)]


async def test_self_hosted_contract_and_split_pcm_without_api_key():
    data = np.arange(500, dtype="<i2").tobytes()
    service, session = service_for(
        response([data[:1], data[1:377], data[377:]]),
        settings=MayaNativeTTSService.Settings(voice="Sreekar", language="te", normalize=False),
    )
    frames = await collect(service, "నా దగ్గర 25 రూపాయలు ఉన్నాయి")
    assert pcm(frames) == data
    assert all(f.sample_rate == 24000 and f.num_channels == 1 for f in frames)
    url, request = session.requests[0]
    assert url == "http://127.0.0.1:8080/v1/audio/speech"
    assert request["json"] == {
        "model": "maya3",
        "input": "నా దగ్గర 25 రూపాయలు ఉన్నాయి",
        "voice": "Sreekar",
        "language": "te",
        "response_format": "pcm",
        "normalize": False,
    }
    assert "Authorization" not in request["headers"]
    assert not request["allow_redirects"]
    assert service.last_request_id == "native-1"


async def test_auto_language_custom_clone_and_optional_proxy_key():
    service, session = service_for(
        response([b"\x01\x00" * 20]),
        base_url="https://example.test/maya/",
        api_key="proxy-secret",
        settings=MayaNativeTTSService.Settings(voice="my_clone", language=None),
    )
    await collect(service)
    url, request = session.requests[0]
    assert url == "https://example.test/maya/v1/audio/speech"
    assert "language" not in request["json"]
    assert request["json"]["normalize"] is True
    assert request["headers"]["Authorization"] == "Bearer proxy-secret"


@pytest.mark.parametrize(
    "headers",
    [
        {**HEADERS, "Content-Type": "audio/wav"},
        {**HEADERS, "X-Sample-Rate": "16000"},
        {**HEADERS, "X-Channels": "2"},
        {**HEADERS, "X-Encoding": "mulaw"},
        {"Content-Type": "audio/pcm"},
    ],
)
async def test_wrong_or_missing_metadata_never_becomes_pcm(headers):
    item = FakeResponse([b"not pcm"], headers=headers)
    service, session = service_for(item)
    frames = await collect(service)
    assert len(frames) == 1 and isinstance(frames[0], ErrorFrame)
    assert not item.content.iterated
    assert len(session.requests) == 1


async def test_resampling_flushes_and_truncated_pcm_is_an_error():
    data = np.arange(2400, dtype="<i2").tobytes()
    service, _ = service_for(response([data]), sample_rate=16000)
    frames = await collect(service)
    assert len(pcm(frames)) == 3200
    assert all(f.sample_rate == 16000 for f in frames)
    service, session = service_for(response([b"\x01"]))
    frames = await collect(service)
    assert isinstance(frames[-1], ErrorFrame) and not pcm(frames)
    assert len(session.requests) == 1


async def test_retries_before_audio_but_never_replays_partial_speech():
    service, session = service_for(
        response(status=503, body=b'{"error":{"code":"overloaded"}}'),
        response([b"\x01\x00" * 20]),
        retry_delay=0,
    )
    assert pcm(await collect(service)) == b"\x01\x00" * 20
    assert len(session.requests) == 2
    service, session = service_for(response([b"\x01", aiohttp.ClientPayloadError("truncated")]))
    frames = await collect(service)
    assert isinstance(frames[-1], ErrorFrame)
    assert len(session.requests) == 1


async def test_interrupt_and_generator_close_release_responses():
    item = response([b"\x01\x00" * 20, b"\x02\x00" * 20])
    service, session = service_for(item)
    stream = service.run_tts("Hello", "ctx")
    assert isinstance(await anext(stream), TTSAudioRawFrame)
    await service.on_audio_context_interrupted("ctx")
    assert item.closed and [f async for f in stream] == []
    assert not session.closed
    item = response([b"\x01\x00" * 20, b"\x02\x00" * 20])
    service, _ = service_for(item)
    stream = service.run_tts("Hello", "next")
    await anext(stream)
    await stream.aclose()
    assert item.closed and not service._active_responses


async def test_runtime_settings_flush_old_text_and_clear_language():
    data = b"\x01\x00" * 2400
    service, session = service_for(
        *(response([data]) for _ in range(3)),
        settings=MayaNativeTTSService.Settings(language="hi"),
    )
    down, up = await asyncio.wait_for(
        run_test(
            service,
            frames_to_send=[
                LLMFullResponseStartFrame(),
                LLMTextFrame("First sentence. Pending"),
                TTSUpdateSettingsFrame(
                    delta=MayaNativeTTSService.Settings(
                        voice="my_clone",
                        language=None,
                        normalize=False,
                    )
                ),
                TTSSpeakFrame("New voice."),
            ],
        ),
        timeout=10,
    )
    bodies = [r[1]["json"] for r in session.requests]
    assert [(b["input"], b["voice"]) for b in bodies] == [
        ("First sentence.", "Ahana"),
        ("Pending", "Ahana"),
        ("New voice.", "my_clone"),
    ]
    assert bodies[0]["language"] == bodies[1]["language"] == "hi"
    assert "language" not in bodies[2] and bodies[2]["normalize"] is False
    assert pcm(down) == data * 3
    assert not any(isinstance(frame, ErrorFrame) for frame in up)


async def test_invalid_update_is_atomic_and_assamese_is_supported():
    service, _ = service_for(settings=MayaNativeTTSService.Settings(language="as"))
    assert service.language_to_service_language(Language.EN_IN) == "en"
    with pytest.raises(ValueError):
        await service._update_settings(MayaNativeTTSService.Settings(voice="bad voice"))
    assert service._settings.voice == "Ahana" and service._settings.language == "as"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"base_url": "file:///tmp/server"},
        {"base_url": "https://u:p@example.test"},
        {"base_url": "http://example.test?key=secret"},
        {"url": "http://example.test"},
        {"encoding": "mulaw"},
        {"provider_sample_rate": 8000},
        {"api_key": ""},
        {"settings": MayaNativeTTSService.Settings(model="Maya Calyx")},
        {"settings": MayaNativeTTSService.Settings(language="fr")},
        {"settings": MayaNativeTTSService.Settings(normalize="yes")},
    ],
)
def test_invalid_configuration_is_rejected(kwargs):
    with pytest.raises(ValueError):
        MayaNativeTTSService(**kwargs)


async def test_real_http_and_pipecat_pipeline_with_synthetic_audio():
    requests = []
    data = np.arange(2400, dtype="<i2").tobytes()

    async def speech(request):
        requests.append(await request.json())
        reply = web.StreamResponse(headers=HEADERS)
        await reply.prepare(request)
        await reply.write(data[:501])
        await reply.write(data[501:])
        await reply.write_eof()
        return reply

    app = web.Application()
    app.router.add_post("/v1/audio/speech", speech)
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        service = MayaNativeTTSService(base_url=f"http://127.0.0.1:{port}")
        down, up = await asyncio.wait_for(
            run_test(service, frames_to_send=[TTSSpeakFrame("Hello there.")]),
            timeout=10,
        )
        assert pcm(down) == data
        assert requests[0]["input"] == "Hello there."
        assert not any(isinstance(frame, ErrorFrame) for frame in up)
        assert service._session is None  # Graceful EndFrame closes the owned session.
    finally:
        await runner.cleanup()
