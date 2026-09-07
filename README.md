# Maya TTS for Pipecat

Stream Maya Research speech in a Pipecat pipeline with one API key. The service handles sentence aggregation, connection reuse, turn completion, interruption, PCM decoding, and sample-rate conversion.

This is a **community-maintained integration by Maya Research**, the company providing the API. Pipecat does not maintain or validate this package. Built for and tested with **Pipecat 1.8.1**, pinned explicitly until additional versions are verified. Python 3.11 or newer.

## Install

```bash
pip install "pipecat-maya @ git+https://github.com/MayaResearch/pipecat-maya.git@v0.1.0"
export MAYA_API_KEY="your-key"
```

Or use `uv add "pipecat-maya @ git+https://github.com/MayaResearch/pipecat-maya.git@v0.1.0"`.
This release installs from GitHub; it is not published on PyPI. Keys are issued through the contacts in [Maya's API documentation](https://www.mayaresearch.ai/llm.txt). Keep the key on your server.

## Add to your voice agent

```python
import os
from pipecat_maya import MayaTTSService

tts = MayaTTSService(api_key=os.environ["MAYA_API_KEY"])
```

Put `tts` after your LLM and before the output transport in your existing Pipecat pipeline:

```python
from pipecat.pipeline.pipeline import Pipeline

pipeline = Pipeline(
    [
        transport.input(),
        stt,
        context_aggregator.user(),
        llm,
        tts,
        transport.output(),
        context_aggregator.assistant(),
    ]
)
```

Here `transport`, `stt`, `llm`, and `context_aggregator` are your application's existing components. See [Pipecat's foundational examples](https://github.com/pipecat-ai/pipecat/tree/v1.8.1/examples/voice) for complete conversational bots. Pipecat sends LLM response start/text/end frames and broadcasts `InterruptionFrame` when the user interrupts. Use its normal transport and turn-management path so buffered playback is discarded immediately.

**Do not split tokens, parse Base64, send flush messages, or manage sockets yourself.** Feed standard Pipecat LLM frames. The adapter aggregates sentences, sends them without waiting for audio, reuses one Maya context for the whole response, and closes it exactly once. Text without final punctuation is flushed at response end. Empty responses do not open a turn.

## Run a complete example

```bash
git clone https://github.com/MayaResearch/pipecat-maya.git
cd pipecat-maya
git checkout v0.1.0
pip install .
python examples/speak.py --text 'नमस्ते! आपका ऑर्डर कल पहुँच जाएगा।' --output greeting.wav
```

This single-file example runs a real `PipelineWorker` and `WorkerRunner`, writes correctly headed WAV audio, and exits unsuccessfully on service errors or missing audio. It needs only `MAYA_API_KEY`, without a microphone or another AI provider. To synthesize through HTTP, add `--http`. To change output rate, add `--sample-rate 16000`.

## Models, voices, and language

The defaults are `Maya 2 Native`, `Ananya`, and automatic language detection. For another voice:

```python
tts = MayaTTSService(
    api_key=os.environ["MAYA_API_KEY"],
    settings=MayaTTSService.Settings(
        model="Maya Calyx",
        voice="Aarav",
        language="hi",
    ),
)
```

| Model | Voices |
| --- | --- |
| `Maya 2 Native` | Ananya, Arjun |
| `Maya Calyx` | Amit, Seema, Tripti, Gargi, Aarav, Zara, Rahul, Nila, Riley, Riya, Vikram, Christine, Sagar, Rohan, Jackson, Sana, Tarini, Christopher, Vance |

Both models document `hi`, `te`, `bn`, `gu`, `kn`, `ml`, `mr`, `or`, `pa`, `ta`, and `en`. `en` is Indian English; this package does not claim US or British accents. Pipecat `Language` values for these codes are also accepted (`Language.EN_IN` maps to `en`). Model and voice names are case-sensitive, and voices must belong to the selected model. The supported catalog is exported as `MODELS` and `LANGUAGES`.

Leave `language=None` for mixed-language text. An explicit language must match the text's script. Send plain text; Maya does not interpret SSML, HTML, or Markdown. Write numbers, currencies, and dates naturally, and avoid a second client-side normalization pass. These are provider requirements from the [current Maya contract](https://www.mayaresearch.ai/llm.txt), not claims that every language/voice combination has undergone perceptual evaluation here.

Native supports `speed=0.5` through `1.25`; `None` means its normal speed of `1.0`. Calyx requires `speed=None`. Pitch, seeds, and manual region selection are not supported controls. Routing is provider-managed.

## Change settings during a call

```python
from pipecat.frames.frames import TTSUpdateSettingsFrame

await worker.queue_frame(
    TTSUpdateSettingsFrame(delta=MayaTTSService.Settings(voice="Arjun", speed=0.9))
)

# Switch models and restore automatic language detection.
await worker.queue_frame(
    TTSUpdateSettingsFrame(
        delta=MayaTTSService.Settings(
            model="Maya Calyx",
            voice="Tarini",
            language=None,
            speed=None,
        )
    )
)
```

Updates are validated before changing the active settings. WebSocket updates finish the submitted turn before reopening the socket with a fresh readiness handshake. This also reliably resets sticky language and speed. Apply updates between conversational turns when possible; do not expect a voice to change inside audio already queued for playback. Unspecified fields retain their values. Settings that are invalid for a model fail explicitly.

## Audio and telephony

Both services emit mono, signed 16-bit little-endian `TTSAudioRawFrame` objects. Their `sample_rate` constructor argument is the **Pipecat output rate**, defaulting to 24000. Resampling maintains continuous state per synthesis context and flushes the final filter tail on completion. Interrupted tails are discarded.

WebSocket input uses the documented PCM defaults and reads its actual rate from the readiness metadata. Do not pass provider encoding/rate overrides on Native WebSockets: live validation found those unsupported options can produce misleading metadata. Use `sample_rate=8000` or `16000` for local output conversion.

For Calyx HTTP telephony:

```python
from pipecat_maya import MayaHttpTTSService

tts = MayaHttpTTSService(
    api_key=os.environ["MAYA_API_KEY"],
    settings=MayaHttpTTSService.Settings(model="Maya Calyx", voice="Aarav"),
    provider_sample_rate=8000,
    encoding="mulaw",
    sample_rate=8000,
)
```

`provider_sample_rate` accepts 8000, 16000, or 24000 on Calyx; `encoding` accepts `pcm_s16le` or `mulaw`. The HTTP response header determines what is actually decoded. µ-law is decoded to PCM before entering Pipecat; your telephony serializer handles the outgoing carrier encoding. Never pass raw µ-law as a PCM frame. Native rejects these provider format overrides locally because the API otherwise silently ignores them.

Use HTTP for a known full paragraph via `TTSSpeakFrame(text)`, and WebSocket for streaming LLM replies. An optional `aiohttp_session` lets HTTP share your existing connection pool; the adapter closes only sessions it creates.

## Errors and recovery

- Bad settings fail locally. HTTP 400/401 and rejected WebSocket startup are not retried unchanged. Auth is sent in headers, with a nonempty User-Agent.
- HTTP retries 5xx/429 and transient connection failures only before any audio body bytes arrive, with bounded backoff. It respects `Retry-After` up to 30 seconds; longer waits surface an error. Default: two retries, 120-second request deadline.
- A WebSocket close fails affected contexts and reconnects with bounded backoff. Already-submitted speech is never replayed automatically. The next turn uses a fresh context. Default: 15-second readiness deadline and 60-second progress deadline.
- Interruptions cancel all affected open contexts and ignore their late audio. They do not wait for `cancelled`, because a completed/unknown cancel may receive no acknowledgement.
- Provider errors, empty successful responses, invalid metadata/Base64, truncated PCM, and progress timeouts surface as Pipecat errors. Let your application's error handler decide whether to speak a fallback or switch provider.
- Quote `tts.session_id` plus `context_id` for WebSocket support, or `tts.last_request_id` for HTTP. Do not log keys or private audio. Metrics support TTFB and synthesized character usage; these are not a latency SLA.

## Validation and contribution

See [validation evidence](docs/validation.md), [complete API coverage and known limitations](docs/api-coverage.md), and the [integration guide for coding agents](llms.txt). Automated tests use synthetic audio and local protocol servers; live tests use synthetic text against the production Maya endpoint. Production tests are opt-in and never run automatically in CI.

```bash
pip install -e '.[dev]'
python -m pytest
ruff check .
ruff format --check .
python -m build
```

Report reproducible problems through [GitHub issues](https://github.com/MayaResearch/pipecat-maya/issues). Include package/Pipecat versions and provider IDs, with credentials and customer content removed. The integration follows [Pipecat's community guide](https://github.com/pipecat-ai/pipecat/blob/main/COMMUNITY_INTEGRATIONS.md). Changes are recorded in [CHANGELOG.md](CHANGELOG.md).
