# Changelog

## 0.1.0

- Add persistent WebSocket v2 Maya TTS with per-turn contexts, sentence streaming, interruption, metadata readiness, bounded reconnect, and runtime settings.
- Add HTTP TTS with Calyx PCM/µ-law options, authoritative response-format decoding, connection reuse, and bounded retries before audio begins.
- Add per-context continuous PCM resampling with explicit final-tail flushing, regression tests, production validation, and a runnable Pipecat example.
- Pin tested compatibility to Pipecat 1.8.1.
