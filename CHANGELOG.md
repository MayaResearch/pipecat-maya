# Changelog

## 0.2.0

- Target Pipecat 1.11.0 instead of 1.8.1. Install this release when migrating your application to 1.11.0; v0.1.0 remains available for the previous pinned runtime.
- Re-run the HTTP/WebSocket protocol and Pipecat pipeline regression suite on the new runtime, including interruption, late audio, socket reuse, errors and resampling tails.
- Synchronize package metadata, runtime version, request User-Agent and installation instructions.
- Preserve historical live-audio validation separately; this compatibility release does not claim new live-provider or physical microphone validation.

## 0.1.0

- Add persistent WebSocket v2 Maya TTS with per-turn contexts, sentence streaming, interruption, metadata readiness, bounded reconnect, and runtime settings.
- Add HTTP TTS with Calyx PCM/µ-law options, authoritative response-format decoding, connection reuse, and bounded retries before audio begins.
- Add per-context continuous PCM resampling with explicit final-tail flushing, regression tests, production validation, and a runnable Pipecat example.
- Pin tested compatibility to Pipecat 1.8.1.
