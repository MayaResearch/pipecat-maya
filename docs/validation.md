# Validation evidence

**The adapter passes its regression and production pipeline checks. One provider content defect remains open:** Calyx/Amit sometimes omits the second sentence of a Hindi HTTP request. This release does not certify every voice-language combination for production use.

Validation date: 2026-09-07. Runtime: Pipecat 1.8.1. Local production and regression checks used Python 3.12 on macOS arm64. [GitHub Actions passed on Python 3.11, 3.12, and 3.13 on Linux](https://github.com/MayaResearch/pipecat-maya/actions/runs/34110227938).

[Watch the 31-second demo](https://github.com/MayaResearch/pipecat-maya/releases/download/v0.1.0/maya-pipecat-demo.mp4) or [download the complete synthetic validation evidence](https://github.com/MayaResearch/pipecat-maya/releases/download/v0.1.0/validation-evidence.zip). The video renders the actual production run's events and recorded PCM, with labeled introduction/conclusion. It is not a physical microphone, browser, or carrier demonstration.

## Regression and packaging

**110 tests passed**, including actual Pipecat pipelines against local HTTP/WebSocket servers. CI also passed lint, formatting, and wheel/source archive builds on all three Python versions.

Coverage includes:

- Shared context for streamed sentences; fresh context for the next turn; one completion; no invalid empty closer; final text without punctuation.
- Auth headers, metadata readiness, connection reuse, immediate cancellation followed by new speech, stale packet filtering, and explicit task cleanup.
- Disconnect during synthesis, replacement connection races, bounded recovery without replay, partial startup failure, cancellation during retry, and stalled turn timeout.
- Buffered text draining under the old settings before a voice change; complete text draining before shutdown; no reopened socket/session after stop.
- All documented voices and invalid model, voice, language, speed, and format settings.
- Arbitrary PCM byte boundaries; continuous resampling to 8/16/24/48 kHz; exact final sample counts; independent signal comparison within two 16-bit quantization units.
- HTTP PCM/µ-law conversion, authoritative response formats, bounded 5xx/429 retry, no replay after any audio bytes, and caller-owned session preservation.

Independent review reproduced and fixed the replacement-connection race and buffered-text/settings/shutdown defects before the release. No reproduced adapter P0/P1 remains in this bounded test set.

## Production Pipecat pipeline

[Final machine-readable results](evidence/pipeline.json) include SHA-256 fingerprints of the exact service modules tested. [Independent Soniox transcripts](evidence/transcripts.json) cover all six completed output clips, without supplying the expected text to the recognizer.

| Scenario | Result |
| --- | --- |
| Stream text in four-character fragments | Both sentences preserved in order, one Maya context, 5.70 seconds at 16 kHz |
| Direct utterance without final punctuation | Complete ending preserved, 2.26 seconds |
| Interrupt after 0.5 seconds of playback | Zero old-context playback writes after the output interruption event |
| Immediate replacement reply | Correct new speech, no replay of the old context |
| Runtime Arjun + Hindi + speed 0.75 | Fresh session and complete Hindi output |
| Runtime Calyx + Aarav + automatic language | Complete mixed Hindi/English output |
| Native 8 kHz output | All 35,840 PCM bytes retained, 2.24 seconds, including the final word |

The transport received the exact service PCM for completed contexts, allowing only its final silent padding of less than 20 ms. Each completed context emitted exactly one start and stop. The interrupted context was discarded rather than completed. All six independently transcribed outputs preserved their expected words; punctuation and numeral formatting can differ.

Reproduce the production pipeline check with your own key:

```bash
python examples/verify_pipeline.py --output-dir artifacts/pipeline
```

This is opt-in; it calls the production Maya API and can consume provider usage. The script reads `MAYA_API_KEY`, needs no other provider account, and exits unsuccessfully on failed pipeline assertions. The optional independent STT analysis is represented separately in the evidence.

## Catalog and protocol checks

[41 API/PCM acceptance cases](evidence/catalog.json) covered all 21 voices and all 11 language codes on both models, reusing two overlapping English rows. Every case returned nonempty aligned PCM with the expected frame format. **This checks acceptance, not perceptual quality or completeness.** A suspicious short result triggered the content investigation below.

```bash
python examples/verify_catalog.py --output artifacts/catalog.json
```

A separate protocol phase checked both models' ordered sentences, explicit empty close, exactly one terminal reply, same-socket reuse, cancel followed immediately by a new context, and stale-frame isolation. Independent Soniox/Scribe transcription confirmed ordered and post-cancel synthetic clips. Speed 0.5/1.25 followed by an omitted value demonstrated the documented sticky-speed behavior.

The exact public `examples/speak.py` also succeeded through a real Pipecat worker: Native WebSocket produced 188,160 PCM bytes at 24 kHz; Calyx/Aarav HTTP produced 101,120 PCM bytes at 16 kHz, both correctly wrapped as WAV.

## Open provider P1: missing Calyx/Amit sentence

Input: `नमस्ते। आप कैसे हैं?`. The [same-request reproduction](evidence/calyx-content-loss.json) returned HTTP 200, mono PCM at 24 kHz, and only 24,290 bytes (0.506 seconds). Independent Soniox transcription contained only `नमस्ते।`.

The raw HTTP response and adapter output were **byte-for-byte identical**, with SHA-256 `f5c047cd474e43dad349ceb5d41271bfbd2f86fd8ccc8bb870cce31b24437b33`. The second sentence was absent before the adapter processed the response. Support request ID: `3dcaaf8a-23c3-4f15-8401-dedd67f0b04d`.

Earlier Malayalam/Amit samples also omitted the second sentence. Subsequent raw HTTP control requests preserved both sentences, establishing variability. Two sentence-streamed WebSocket controls preserved the Hindi and Malayalam text. These successes do not prove the provider defect resolved. The package keeps Native as its default, makes no silent voice/model substitutions, and documents the Calyx risk. Correcting the provider output requires a separate model/service change and fresh content validation.

## Prevented provider format trap

An unsupported Native WS 8 kHz/µ-law request returned metadata claiming that format while returning actual 24 kHz PCM. The 107,520-byte body produced 2.24 seconds of correctly transcribed speech when decoded as PCM. The adapter never sends unsupported Native wire-format overrides; it uses documented PCM defaults and resamples locally. Calyx HTTP source format options remain supported and are read from the response headers.

## Remaining boundaries

P0 is a release-blocking safety/correctness failure; P1 is a major reliability/content failure; P2 is a smaller issue or qualification gap. These are review priorities. They are not a protocol called P0P or P2P.

The open provider P1 above is not represented as fixed. Other boundaries include full perceptual evaluation of every voice-language pair, exact last-spoken-word alignment without provider timestamps, long-duration soak/load tests, physical device/carrier testing, and compatibility beyond pinned Pipecat 1.8.1. Published latency measurements are not an SLA. No customer traffic, customer recordings, or production configuration changes were used. The docs PR's merge and PyPI publication are separate from the available GitHub release.
