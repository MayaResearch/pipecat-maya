# Maya API coverage and design decisions

Reviewed against the complete [Maya `llm.txt`](https://www.mayaresearch.ai/llm.txt), fetched 2026-09-07, and [Pipecat's current community integration guide](https://github.com/pipecat-ai/pipecat/blob/main/COMMUNITY_INTEGRATIONS.md). The provider document defines the public contract; the implementation is tested against Pipecat 1.8.1. This matrix records every functional topic in that document, including deliberate omissions, so an unsupported feature is not silently implied.

## Contract coverage

| Provider topic | Integration behavior | Verification or boundary |
| --- | --- | --- |
| HTTP POST `/v1/tts` | `MayaHttpTTSService`, pooled aiohttp session, streaming response | Production example and format probes; local failure tests |
| Persistent WebSocket `/v1/tts/stream` | `MayaTTSService`, one authenticated socket across turns | Production reuse and pipeline tests |
| Bearer authentication | Server-only Authorization header on both transports | Local header assertions; live authenticated requests |
| Browser query-key fallback | Intentionally not exposed, because this is a server integration | No secret in URL; use a server-side Pipecat bot |
| Nonempty User-Agent and filtered 403 | Sends `pipecat-maya/0.1.0` from the first request | Deterministic403 surfaces an error; no repeated unchanged retries |
| Model/voice defaults and exact case | Native/Ananya; all 2 Native and 19 Calyx names validated | Complete current roster exported; unsupported names fail before network |
| Cross-model voice restrictions | Model and voice validated together, including updates | No automatic voice substitution |
| Eleven language codes | hi,te,bn,gu,kn,ml,mr,or,pa,ta,en plus corresponding Pipecat enums | Catalog acceptance is distinct from perceptual quality |
| Automatic language/code-switching | `language=None` omits the wire field; updates reconnect WS to clear sticky state | Wrong explicit language can still mispronounce; match script yourself |
| English accent scope | en means Indian English; en-IN maps to en | No US/GB/Arabic language or accent support claimed |
| Native speed 0.5–1.25 | Numeric finite values validated, booleans/strings rejected; default1.0 explicit on Native | Per-text sticky speed cannot accidentally persist after reset |
| Calyx speed rejection | Explicit non-None speed rejected locally | Clear speed when switching from Native |
| Pitch/time stretching | No pitch parameter; no client pitch transformation | Provider pitch-preservation measurements are not independently certified here |
| Calyx HTTP sample rate | `provider_sample_rate` accepts 8000/16000/24000 | Actual response header overrides requested assumptions |
| Calyx HTTP encoding | PCM16LE or G.711 µ-law; decoded to Pipecat PCM | Every supported source rate/encoding combination covered by format logic |
| Non-Calyx ignored format fields | Reject provider format overrides locally | Avoid silently mislabeled output |
| Invalid format yields 502 upstream | Reject invalid rate/encoding before request | Avoid retrying a known deterministic provider validation problem |
| HTTP 200 raw audio | Validate status/content-type before consuming as audio | JSON errors never become audio; missing/bad rate/channel rejected |
| Headerless PCM and WAV wrapping | Frames contain mono16LE PCM; example adds actual-rate WAV header | Byte alignment across arbitrary network chunks, strict odd EOF error |
| Output sample rate | `sample_rate` is Pipecat output rate, independent from provider rate | Per-context streaming resampling, explicit tail flush, no padding truncation |
| `x-request-id` / JSON request_id | HTTP `last_request_id`, sanitized error diagnostics | Identifiers useful for support, not authentication |
| WS start/v2 selection | First frame `{type:start,v2:true,...}` and wait for metadata | No text before accepted start; rejected start closes client socket |
| Corrected start on rejected socket | API permits correction; adapter uses a fresh connection with corrected settings | Simpler lifecycle; no legacy v1 fallback |
| v1 to v2 upgrade forbidden | Always fresh v2 connection | Does not implement undocumented legacy v1 |
| Sticky model/voice/language | Complete settings on start, fresh socket for runtime changes | Submitted old context finishes before the new configuration |
| WS metadata rate/channels/encoding | Validate mono PCM and use reported input rate; expose session_id | Never send unsupported Native wire format overrides; see discrepancy below |
| One context per LLM reply | Reuse Pipecat's turn ID for all aggregated sentences | Distinct UUID for every new turn; bounded retired-ID guard |
| Nonblocking sentence submission | `continue:true`; receive loop runs separately | No request-response wait between sentences; no arbitrary token synthesis |
| Final/empty closer | Explicit empty text with `continue:false` once per open context | Empty LLM replies never send a closer to an unopened ID |
| End or cancelled | End finalizes audio/tail once; cancelled discards remainder | Explicit stop/context retirement, stale terminators ignored |
| Unknown/completed cancel no reply | Never wait for cancellation acknowledgement | New turn can begin immediately |
| Cancel/clear/no-ID variants | Targeted `cancel` for all affected contexts; aliases intentionally unnecessary | Prevent global cancellation of unrelated IDs |
| Already-buffered playback | Pipecat's interruption path clears transport queues immediately | Custom clients must also clear their own buffers |
| In-flight old audio | Reject packets whose context is no longer active | Tests inject late audio before cancel acknowledgement |
| Single-use IDs/request_id not alias | Only context_id for WS; no HTTP ledger ID overloading | Stale/reused contexts not resurrected |
| Base64 JSON audio | Strict Base64 decode, continuous PCM streaming | Invalid JSON/Base64/PCM becomes an error |
| Tagged WS errors | Fail and close named context, send targeted cancellation | Keeps unaffected contexts isolated |
| Untagged WS errors | Fail currently open contexts and cancel them | Avoid associating an unscoped failure with the next turn |
| Unknown WS message types | Ignore unknown informational types; no request awaiting their reply | Bounds stalled real work with progress deadline |
| Ping/pong/keepalive | WebSocket protocol ping/pong keepalive; accept JSON pong | Uses standard WebSocket library keepalive, no redundant text heartbeat |
| Idle/deploy disconnects | Bounded exponential reconnect; fail uncertain active contexts | Never replay partially delivered speech automatically |
| Long text/known paragraphs | `TTSSpeakFrame` sends full known text on HTTP | Streaming LLM uses sentence aggregation, per WS-specific guidance |
| Do not buffer whole response | Emit audio progressively in both services | Full-file accumulation exists only in optional QA/example consumers |
| Region | Provider-managed routing; no region control | No region latency promise |
| Seed/determinism | No seed input or audio-byte identity/caching promise | Provider ignores seed and allows numerical differences |
| Concurrency/rate limits | No invented hard cap; HTTP 429 Retry-After handling | Local production sweeps bounded and sequential, not a load benchmark |
| HTTP 400/401 | Surface errors without unchanged retries | Deterministic negative cases in regression suite |
| HTTP 5xx/502/retry | Bounded exponential retry before any audio bytes only | Never merge partial audio from two attempts |
| Abandoned HTTP response | Close response promptly on cancellation; close owned session on lifecycle teardown | Caller-owned session remains open |
| SSML/HTML/Markdown | No SSML parser claimed; send plain text, use Pipecat text filters if needed | Avoid speaking literal markup |
| Natural numbers/currencies/dates | Preserve text and allow provider normalization | No invented external normalization contract |
| Latency guidance | Streaming, connection reuse, modest concurrency | Published provider measurements are not an integration SLA |
| Support contacts | Refer to live provider docs; include transport-specific IDs | No credentials, private audio, or customer text in public issues |
| Existing Pipecat reference | Prior core PR 5222 closed; new separate package plus docs listing | Follows maintainer's explicit community-integration direction |
| Word timing | No timestamps invented | Text/context accuracy after barge-in has framework limitations below |

## Discrepancies found during research

The [earlier core PR](https://github.com/pipecat-ai/pipecat/pull/5222) was closed with an explicit [request for a community package and documentation PR](https://github.com/pipecat-ai/pipecat/pull/5222#issuecomment-5186553125). Its installation instruction does not describe a released core extra. Install this versioned package instead.

The provider document mixes generic two-voice language with the detailed 19-voice Calyx roster, and generic concurrency-cap advice with its current no-cap section. The scoped model/rate-limit sections take precedence. Its description of playing8k audio as24k reverses the speed direction: that mismatch plays three times faster. The adapter uses the actual format rather than repeating the prose assumption.

Live production testing found **Native WS `sample_rate=8000, encoding=mulaw` can echo those values in metadata while returning PCM24k bytes**. The same sentence independently transcribed correctly when decoded as PCM24k. Those Native overrides are unsupported in the documented contract; this package never sends them. Calyx HTTP format options are supported and decoded from the response headers. Calyx WS format overrides are deliberately not exposed because the WS-specific contract documents PCM defaults; output resampling remains available.

The docs list several untagged settings-error cases; live testing also observed an untagged invalid-speed error. The integration treats an error's context_id as optional.

## Bias check and remaining limits

P0 means a release blocker such as wrong audio decoding, leaked credentials, stale interrupted speech, deadlock, or lost final samples. P1 means a major usability/reliability failure such as broken recovery or settings changes. P2 means smaller defects or evidence gaps. These are our review priorities, not a new protocol named P0P/P2P.

The design addresses known Pipecat failures: [stale audio/context routing](https://github.com/pipecat-ai/pipecat/issues/3986#issuecomment-4128817211), [delayed stop frames](https://github.com/pipecat-ai/pipecat/pull/4639), explicit idempotent lifecycle cleanup, and [zero-audio/context problems](https://github.com/pipecat-ai/pipecat/issues/5305). It does not claim zero possible defects.

- No provider word timestamps exist. Pipecat can associate submitted text with a turn, but this adapter cannot certify the exact last spoken word after interruption. Use heard-audio transcription or an application-level reconciliation policy where exact conversation history matters.
- Complete catalogs are API acceptance checks, not an independent MOS/pronunciation evaluation of every voice-language combination. Synthesized test speech is distinguished from consented customer call audio.
- The playback demonstration uses synthetic input events and a real Pipecat paced output transport. It is not proof of a physical microphone, browser device, or phone carrier route.
- Production tests measure one client/network and bounded concurrency. They are not an SLA, capacity benchmark, or a soak test.
- The package pins Pipecat 1.8.1. Newer releases need compatibility checks before widening the dependency.
- GitHub installation is available for this release. A PyPI publication and Pipecat's upstream listing approval are separate states.
