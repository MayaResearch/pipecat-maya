# Changelog

## Unreleased

- Add `refresh_catalog()`, an opt-in coroutine that replaces the shipped voice catalog with what the provider currently serves, so a voice added after a release is reachable without a package update. The provider documents no catalogue endpoint; this reads the valid values from the documented 400 returned for an unknown voice. It fails safe: a network error, a non-400 status, an unparseable body or an empty list leaves the shipped catalog untouched and is reported rather than raised.

- Sync the Calyx voice catalog with [Maya's API documentation](https://www.mayaresearch.ai/llm.txt) checked on 2026-09-08, adding Diya, SagarM, Samar, Shailika, Shreeraj, Vikas, Arushi, Kavita, Neeraj, Neha, Rehan, and Kabir. Calyx now accepts 31 case-sensitive voice names; SagarM remains distinct from Sagar.

## 0.1.0

- Add persistent WebSocket v2 Maya TTS with per-turn contexts, sentence streaming, interruption, metadata readiness, bounded reconnect, and runtime settings.
- Add HTTP TTS with Calyx PCM/µ-law options, authoritative response-format decoding, connection reuse, and bounded retries before audio begins.
- Add per-context continuous PCM resampling with explicit final-tail flushing, regression tests, production validation, and a runnable Pipecat example.
- Pin tested compatibility to Pipecat 1.8.1.
