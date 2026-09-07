# Validation evidence

Validation date: 2026-09-07. Baseline: Pipecat1.8.1, Python3.12 on macOS arm64. The GitHub Actions matrix additionally runs Python3.11/3.12/3.13 on Linux. A configured CI job is not evidence that it has passed; check the repository's Actions page for the current commit.

## Automated regression tests

The initial combined suite passed **95 tests**. It exercises the actual Pipecat pipeline against local HTTP/WebSocket protocol servers, plus signal and settings tests:

- One shared context for streamed sentences, fresh context for another reply, one terminal frame per turn, and no invalid empty closer for an empty LLM response.
- Auth headers, metadata gate, socket reuse, interrupted late audio, settings-driven reconnect, and reset to automatic language detection.
- Progress timeout, tagged/untagged errors, empty audio, truncated final PCM, and invalid startup/settings.
- Every documented voice and invalid model/voice/language/speed combination.
- Arbitrarily split PCM samples, continuous resampling to8/16/24/48kHz, exact final sample counts, and independent signal comparison within two16-bit quantization units.
- HTTP status/format validation, µ-law decoding, connection reuse, bounded5xx/429 retries, no replay after any audio bytes, and ownership-aware cleanup.

`ruff check`, formatting, and wheel/source-distribution builds also passed. Expanded adversarial tests and release CI are recorded in the final release evidence.

## Production endpoint checks

The first independent protocol phase synthesized21 short synthetic utterances against the production Maya endpoint, with sequential or bounded requests. Both Native and Calyx passed ordered two-sentence turns, one terminal reply, repeated turns on one socket, targeted cancellation immediately followed by a new context, and stale-frame isolation. Native speed0.5/1.25 followed by an omitted-speed request demonstrated the documented sticky-speed behavior.

Separate Soniox transcription of each model's ordered clip preserved both sentences and their order, including the final words. ElevenLabs Scribe transcribed each model's immediate post-cancel reply correctly. These are machine transcription checks of synthetic speech, not human perceptual ratings.

The exact public `examples/speak.py` succeeded through a real Pipecat worker:

| Path | Configuration | Output |
| --- | --- | --- |
| WebSocket | Native/Ananya, English,24kHz |188160 PCM bytes, valid mono WAV |
| HTTP | Calyx/Aarav, English,16kHz output |101120 PCM bytes, valid mono WAV |

Synthetic input was: “Your order arrives tomorrow. Thank you for choosing Maya.” Keys stayed in the protected environment and were not copied into the package.

## Provider discrepancy caught

An unsupported Native WS8kHz/µ-law request returned metadata claiming that format, but the107520-byte output was actual24kHz PCM. Decoding as24kHz produced2.24seconds of correctly transcribed speech; interpreting it as8kHzµ-law would corrupt it. The integration prevents this path by keeping Native's documented wire defaults and resampling PCM locally. Calyx's same probe returned actual8kHzµ-law. See [API coverage](api-coverage.md) for the scope boundary.

## Limits

These checks do not establish a latency SLA, high-concurrency capacity, long-duration soak reliability, full perceptual quality across every voice-language combination, exact word-level interruption alignment, or behavior through a physical microphone/browser/phone carrier. No customer traffic or private customer recordings were used. Upstream docs approval and PyPI publication are separate from successful GitHub installation.
