# Self-host Maya 2 Native with Pipecat

Use the open-weight [Maya 2 Native server](https://github.com/MayaResearch/maya2-native)
as the TTS stage of an existing **Pipecat 1.11.0** app. Keep your STT, LLM,
transport, prompts and turn handling. No hosted Maya API key is required.

This adapter is maintained by Maya Research. It is a proposed addition, not part
of the existing `v0.2.0` tag or an upstream Pipecat release. Install the PR's
checked-out source with `pip install .`; use its commit when testing in another
project. Do not replace a working app's lockfile or force a framework migration.

## Start the model server

On a Linux NVIDIA GPU machine, follow the inference repository's
[installation instructions](https://github.com/MayaResearch/maya2-native#install).
Its documented loading peak is approximately 9.2 GB plus CUDA context; allow at
least 12 GB for a small setup and review serving capacity for your workload.
Start with one seat on loopback:

```sh
python -m maya3.server --ckpt maya-research/maya2-native \
  --host 127.0.0.1 --port 8080 --max-seats 1
curl --fail http://127.0.0.1:8080/health
curl --fail http://127.0.0.1:8080/v1/voices
```

Wait for `/health` to return 200. Use a voice ID from `/v1/voices`; `Ahana` is a
built-in release voice. Custom registered clone IDs also work. The model server
has no built-in authentication: keep it private; a remote app should reach it
through a private connection or an authenticated TLS proxy that you configure.

## Replace the TTS stage

In the Pipecat application's own environment, install this checked-out package:

```sh
pip install .
```

```python
from pipecat_maya import MayaNativeTTSService

tts = MayaNativeTTSService(
    base_url="http://127.0.0.1:8080",  # Server root, without /v1
    sample_rate=24000,  # Pipecat output rate
    settings=MayaNativeTTSService.Settings(
        voice="Ahana",
        language="hi",
        normalize=True,
    ),
)
```

Put `tts` after your existing LLM and before `transport.output()`. Standard
Pipecat LLM frames are sentence-aggregated by the base service. `TTSSpeakFrame`
can send a complete known paragraph. Unpunctuated text is flushed at response
end. This is HTTP audio streaming, with one request per aggregated text segment;
it does not append tokens to the model's WebSocket context.

Set `language` explicitly, particularly for Marathi/Devanagari and
Assamese/Bengali-script input. The twelve server language codes are `as`, `bn`,
`en`, `gu`, `hi`, `kn`, `ml`, `mr`, `or`, `pa`, `ta`, `te`. `None` omits the
language and uses the server's script heuristic; it cannot distinguish all
languages sharing a script. Set `normalize=False` only if text is already
prepared. The model name on the wire remains `maya3`; its public name is Maya 2 Native.

Runtime updates use the normal Pipecat settings frame:

```python
from pipecat.frames.frames import TTSUpdateSettingsFrame

await worker.queue_frame(
    TTSUpdateSettingsFrame(
        delta=MayaNativeTTSService.Settings(voice="Sreekar", language="te"),
    )
)
```

An update flushes buffered text with its old settings before applying the new
ones. Invalid settings leave the active configuration unchanged. `language=None`
clears a previous language. Speed is not a supported control for this adapter.

## Write a WAV through a real Pipecat pipeline

From this package checkout, with the model server already running:

```sh
python examples/speak.py --native-url http://127.0.0.1:8080 \
  --language hi --voice Ahana --text 'नमस्ते! आपका ऑर्डर कल पहुँच जाएगा।' \
  --output native.wav
```

No microphone or STT/LLM account is needed. The command refuses an existing
output file and fails if no audio or a service error is reported. For 16 kHz
output, add `--sample-rate 16000`; conversion is local and preserves the
resampler's final tail. The server always returns mono PCM s16le at 24 kHz.

## Failure handling and validation

The adapter validates response format/rate/channel metadata before emitting PCM,
aligns split 16-bit samples, rejects truncated samples, and closes active HTTP
responses on interruption. It retries transient failures only before receiving
audio; a partial utterance is never replayed automatically. A caller-owned
`aiohttp_session` remains open. `api_key` is optional and is sent only if you
explicitly configure an authenticated reverse proxy. Redirects are not followed.

Keyless tests exercise the real Pipecat pipeline against a local synthetic-audio
HTTP server, plus settings, cancellation, retry, error, sample-alignment and
resampling cases. Existing hosted adapter tests run unchanged. These checks do
not prove real GPU synthesis, browser playback, human listening, or latency.
Run the WAV example and check the final spoken word and interruption behavior
in your own app before relying on it. No GPU instance is provisioned by this guide.

The contract was checked against the public inference server's
[`c6b3768` revision](https://github.com/MayaResearch/maya2-native/blob/c6b376868e268551d065c4206193e96257bef4a1/maya3/server.py).
Its open-weight HTTP endpoint differs from the hosted Maya API supported by
`MayaTTSService` and `MayaHttpTTSService`; do not swap their endpoint URLs.
