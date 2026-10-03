# Codex Live proxy

The implementation uses the existing Axum 0.8, reqwest 0.12, tokio-tungstenite 0.28, futures-util, and Tokio dependencies. No dependency or lockfile change is required. Those libraries implement HTTP, TLS, upgrades, WebSocket framing, and asynchronous I/O. Application code supplies account selection, call ownership, authentication, deadlines, and relay lifecycle.

## Crate review

| Candidate | Decision |
| --- | --- |
| Existing Axum/reqwest/tokio-tungstenite stack | Reuse the existing transport and OAuth machinery. |
| axum-reverse-proxy 2.2.0 | Compatible versions, but upstream handshake failures become generic 502 responses and the private relay needs lifecycle adaptation. |
| oai-rt-rs 0.5.1 | Supports subscription GPT-Live; its typed client transport omits account headers on call creation and does not preserve arbitrary outbound events or raw HTTP responses. |
| nanocodex-oai-api 0.6.5 | Supports subscription SDP setup but owns the session and brings WebRTC/native Opus dependencies unnecessary for signaling proxying. |
| Official codex-api/codex-websocket-client | Git-only integration with workspace dependencies and root Cargo patches; the raw WebSocket client is usable but adds no needed capability over the existing transport. |
| reqwest-websocket, tokio-websockets, fastwebsockets | Alternative client transports do not supply account binding and would change the current transport stack. |
| tokio-util DelayQueue, Moka, DashMap | A fixed-capacity registry can prune disconnected entries during call operations; no timer task or evictable ownership cache is needed. |

Sources: [axum-reverse-proxy handshake handling](https://github.com/tom-lubenow/axum-reverse-proxy/blob/7350d118a69b6d98ae19a767b9d71630b2a3bc80/src/forward.rs#L387-L403), [oai-rt-rs transport](https://github.com/lukacf/oai-rt-rs/blob/0f92886b64e632da4cc28d123ec7912f2a61fe50/src/experimental/gpt_live/transport.rs), [nanocodex transport](https://github.com/gakonst/nanocodex/blob/6e3bb302fdf4258fa37f25e31ac42cd382d3660a/crates/nanocodex-oai-api/src/realtime/webrtc.rs#L873), [Codex workspace patches](https://github.com/openai/codex/blob/rust-v0.157.1/codex-rs/Cargo.toml#L630-L641).

## Protocol boundary

The target is Codex's V3 WebRTC subscription flow, verified against `rust-v0.157.1`:

- POST JSON to the account's Codex backend `/realtime/calls`, preserving the query, offer, and session fields.
- Authenticate with the selected ChatGPT bearer and account ID; preserve Codex session headers and `openai-alpha`.
- Accept successful HTTP responses containing UTF-8 SDP and a `Location` with an `rtc_...` or UUID call ID.
- Connect to the separately configured `/v1/live/{call_id}` sideband using that same account, ignoring any host in `Location`.
- Relay opaque text/binary/control messages continuously; do not require `response.create` or parse voice events.
- Permit reconnection to the same call after disconnect; upstream 404/410 invalidate the binding.

Codex sources: [call setup and ID extraction](https://github.com/openai/codex/blob/rust-v0.157.1/codex-rs/codex-api/src/endpoint/realtime_call.rs#L129-L293), [sideband URL](https://github.com/openai/codex/blob/rust-v0.157.1/codex-rs/codex-api/src/endpoint/realtime_websocket/methods.rs#L1212-L1259), [native V3 request](https://github.com/openai/codex/blob/rust-v0.157.1/codex-rs/tui/src/app_server_session/realtime.rs#L20-L55), [reconnect behavior](https://github.com/openai/codex/blob/rust-v0.157.1/codex-rs/core/src/realtime_conversation/sideband.rs#L23-L190).

The call ID parser accepts bounded ASCII alphanumeric, underscore, and hyphen identifiers. It intentionally rejects encoded separators and traversal characters. The caller binding distinguishes the shared downstream credential from individual accepted ChatGPT credentials. Rotation of that shared downstream key invalidates access to its existing bindings. Multiple Tokenproxy instances require sticky routing because ownership is local to a process.

HTTP call responses preserve status, body, selected safe headers, and `Location`. WebSocket handshake errors preserve status and safe headers, but their bodies are limited to the bytes supplied by tungstenite's handshake error; fragmented HTTP error bodies may be incomplete. No transparent retry follows ambiguous setup/transport failure, quota errors, or established-session errors. Only an explicit 401 permits one same-account refresh/retry. A voice-specific 403 does not poison global account auth health.

## Reproducible experiments

```sh
cargo check --locked
cargo fmt --check
cargo test --locked
cargo clippy --all-targets --all-features --locked -- -D warnings
TOKENPROXY_STRESS_CALLS=256 TOKENPROXY_STRESS_MESSAGES=2000 \
  cargo test --locked --test realtime stress_concurrent_calls -- --nocapture
TOKENPROXY_STRESS_CALLS=64 TOKENPROXY_STRESS_MESSAGES=10000 \
  cargo test --locked --test realtime stress_concurrent_calls -- --nocapture
```

The integration suite uses local HTTP/WebSocket servers and exact message/header counters. It exercises spontaneous events, simultaneous send/receive, byte-preserving text/binary relay, ping and close frames, duplicate attachment, reconnection, request/message limits, 1,024-call admission, delayed setup/handshake, ambiguous body failure, slow/stalled writers, quiet media sessions, and shutdown. The private tests cover expiry, 2,048 simultaneous reservations, duplicate upstream IDs, caller isolation, config reload identity/backend changes, token rotation, and bounded refresh retries.

Stress clients synchronize after attachment and verify every echoed frame and final message counts. They verify that no fallback account is used and that upstream connections and the active-WebSocket gauge return to zero. These measurements cover signaling and control transport; they do not measure WebRTC audio latency or quality.

Local results on September 26, 2026:

| Experiment | Result |
| --- | --- |
| Full suite | 378 tests passed: 321 library, 7 CLI, 22 reset, 28 realtime |
| Complete realtime suite repeated 20 times | 560 test executions passed in 20.48 seconds |
| 256 concurrent calls, 2,000 messages each | 512,000 exact echoes in 10.53 seconds; connections and active-session gauge returned to zero |
| 64 concurrent calls, 10,000 messages each | 640,000 exact echoes in 12.14 seconds; all upstream connections closed |
| Check, formatting, Clippy with warnings denied | Passed |

The 256-call run peaked at 232,669,184 bytes RSS for the combined test process containing Tokenproxy, upstream mocks, and clients. This is not a standalone proxy memory measurement. Stress found and corrected a configured message-limit mismatch; repeated runs also exposed ephemeral-port exhaustion in the test harness, corrected by reusing HTTP clients and draining response bodies.

No live OpenAI voice call was performed. Subscription entitlement, actual media exchange, and compatibility with a future private upstream protocol require a live Codex smoke test.
