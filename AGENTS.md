# Tokenproxy Agent Guide

Tokenproxy is a single-binary Rust server that fronts OpenAI Chat Completions, Responses (HTTP and WebSocket), and Anthropic Messages behind one endpoint, and spreads traffic across a pool of upstream accounts (OpenAI API keys, Cerebras API keys, Anthropic API keys, ChatGPT Codex `auth.json` credentials). When an account hits a usage limit, gets throttled, or fails auth, routing shifts to healthy accounts.

The implementation lives in `src/`. `CLAUDE.md` is a symlink to this file, so this map is also the project instruction set for coding agents.

## Read in this order

Stop as soon as you have what you need.

1. `README.md` — what the server does, install, inline config examples.
2. This file — the map below.
3. `src/main.rs` — CLI parsing, config assembly, startup and shutdown.
4. `src/server/state/proxy.rs` — the router and the whole request hot path. By far the largest file and home to most tests. Read the `app()` function first; it lists every route.
5. The module you actually need, via the map below.

## Repo map

| Path | What it is |
|------|------------|
| `src/` | The entire implementation (one crate, binary + lib). |
| `.github/workflows/ci.yml` | Format, build, test on push and PR. |
| `.github/workflows/release.yml` | Tag `v*` push: builds 9 targets, publishes a GitHub release, triggers the Homebrew tap update. |
| `stage_one_evidence/` | Captured measurement and probe artifacts. |
| `CLAUDE.md` | Symlink to this file. |
| `CLIProxyAPI/`, `codex/`, `iggy/`, `monoio/`, `pingora/`, `quiche/`, `ripgrep/`, `rust-sdks/`, `s2n-quic/`, `uv/` | Local reference clones for studying mature implementations. Git-ignored, not tracked, not submodules. Never edit them. |

## Source modules

| Module | Role |
|--------|------|
| `main.rs` | CLI entry: flag parsing, config load, server startup, shutdown signals. |
| `config.rs` | Config schema and parsing: `Config`, `AccountConfig`, `AccountKind`, timeouts, retries, downstream auth, admin auth; resolves to `EffectiveConfig`. |
| `cerebras/request.rs` | Stateless Responses-to-Cerebras Chat translation, instruction normalization, function namespaces, and compatibility validation. |
| `cerebras/response.rs` | Chat-to-Responses JSON and SSE conversion, stable item identities, tool deltas, usage, and terminal status. |
| `auth.rs` | ChatGPT Codex `auth.json` handling: token snapshots per account, bearer checks, refresh after upstream 401 (`ChatGptAuthCell`). |
| `server/state.rs` | `AppState`: runtime account-health store and persistence across reloads. |
| `server/state/reset.rs` | Opt-in ChatGPT reset redemption, per-identity coordination, idempotency, and reset API mocks. |
| `server/state/realtime.rs` | Opt-in Codex Live JSON call setup, bounded call/account ownership, same-account auth refresh, and continuous sideband relay. |
| `server/state/realtime/tests.rs` | Call expiry/admission, caller isolation, config reload identity checks, and local refresh experiments. |
| `server/state/proxy.rs` | The hot path: axum router, request handlers, upstream orchestration, SSE and WebSocket streaming, retry/failover, health recording, metrics emission. |
| `routing/select.rs` | Account selection: health-based exclusion, scoring, priority, stable hashing. |
| `routing/health.rs` | `AccountHealth` enum: `Open`, `Unknown`, `Throttled`, `UsageLimited`, `AuthFailed`. |
| `routing/account.rs` | Routing-facing types: `AccountState`, `RouteRequest`, endpoint/transport capabilities, model family labels. |
| `http/forward.rs` | Upstream request construction and the provider header allowlist. |
| `http/classify.rs` | Maps incoming path and body to a `ClassifiedRequest` (endpoint, request shape). |
| `http/sse_repair.rs` | Repairs partial SSE frames from upstream streams. |
| `responses/replay.rs` | Responses API output-item replay for stateless clients. |
| `responses/state.rs` | `ReplayState`: request template, `previous_response_id` bookkeeping, compaction reset. |
| `responses/websocket.rs` | WebSocket message framing between client and upstream. |
| `usage.rs` | Usage windows and limit interpretation. |
| `tests/auto_use_reset.rs` | Local HTTP/SSE/WebSocket experiments for automatic reset recovery and failure behavior. |
| `tests/realtime.rs` | Local voice setup/sideband experiments, concurrency stress, backpressure, and cleanup checks. |
| `metrics.rs` | Metrics registry behind `/metrics`. |
| `observability.rs` | Request-body dump records and hashing for debugging. |
| `logging.rs`, `error.rs`, `time_parse.rs` | Structured logging, error-to-response mapping, timestamp parsing. |

## HTTP surface

Defined in `app()` in `src/server/state/proxy.rs`: `/healthz`, `/metrics`, `/usage`, `/admin/config/status`, `/admin/config/reload`, `/v1/models`, `/v1/chat/completions`, `/v1/messages`, `/v1/responses` (POST for HTTP, GET upgrades to WebSocket), `/v1/responses/compact`, `/backend-api/codex/realtime/calls` (POST), `/v1/live/{call_id}` (GET WebSocket), and a fallback that passes unknown paths through to the upstream.

## Request hot path

Client request → router (`proxy.rs`) → downstream auth check → request classification (`http/classify.rs`) → account selection (`routing/select.rs`) → auth snapshot (`auth.rs`) → upstream request (`http/forward.rs`) → streamed response with SSE repair or WebSocket relay → health and metrics recording (`server/state.rs`, `metrics.rs`).

`server.max_body_bytes` defaults to `usize::MAX`, leaving general HTTP bodies unlimited so providers enforce request-size limits. Explicit byte values cap inbound and decoded request bodies, buffered JSON/compact responses, and accumulated Cerebras output. Provider HTTP 413 responses retain their status and body. Codex Live control messages retain their separate bounded limits.

Cerebras accounts use `cerebras_api_key`, API-key bearer authentication, and the default `https://api.cerebras.ai/v1` upstream. Only HTTP Chat Completions and HTTP Responses are supported; incremental continuations are disabled. Responses translate per selected account, preserving the original request for failover. System/developer instructions are consolidated into one initial system message. Plaintext agent messages retain sender and recipient attribution; native collaboration calls mark task arguments as plaintext. Named standalone tool outputs without call IDs become separate assistant messages with tool attribution; ordinary paired outputs retain their tool role and ID. Paired image outputs retain text and image markers in the tool result, with attributed user-image attachments deferred until all parallel results arrive. Encrypted agent messages remain unsupported. The adapter rejects unsupported semantic features, bounds raw SSE frames, applies any configured accumulated-output cap, and waits for upstream completion before announcing executable tool results. Incomplete tool calls never emit item-done events. See `docs/cerebras.md` for the supported subset and source references.

ChatGPT accounts may opt into `auto_use_reset` (default false). Explicit usage exhaustion attempts a banked reset through the Codex backend and permits one same-account retry before downstream output. After output, the error is forwarded and reset recovery benefits future requests. Per-backend/account coordination retains ambiguous redemption keys across config reloads; selection can reconcile an exhausted account when no ordinary account is eligible. Generic throttling never consumes a reset.

ChatGPT accounts may separately opt into `supports_realtime`. Voice setup selects a healthy voice account; its call ID stays bound to that account/backend and authenticated caller across reconnects. Voice uses no Responses transformations, reset redemption, replay, or cross-account retry. The shared control HTTP client disables redirects for reset and voice requests. Call state is process-local, capped at 1,024 including setup reservations, with five-minute disconnected expiry and no eviction of active calls. WebRTC media bypasses Tokenproxy. Text-model discovery does not restrict the voice session model; upstream entitlement remains authoritative.

## Working in this repo

- `cargo check` before building; `cargo test --lib --bin tokenproxy` runs the full suite (~300 tests, under a minute); `cargo fmt --check` before pushing.
- Tests are inline `#[cfg(test)]` modules next to the code they cover; most live in `server/state/proxy.rs`.
- Run `cargo test --test auto_use_reset` for the local backend API and streaming experiments, in addition to the library and binary suite.
- Run `cargo test --test realtime` for voice integration experiments; `TOKENPROXY_STRESS_CALLS` and `TOKENPROXY_STRESS_MESSAGES` scale its concurrent relay test, as described in `docs/realtime-validation.md`.
- Run `cargo test --test cerebras` for native auth/routing, fragmented tool streams, two-turn tool history, and failure/commit behavior.
- Releases: bump `version` in `Cargo.toml` and `Cargo.lock` in one commit on main, tag it `vX.Y.Z`, push the tag. `release.yml` does the rest.
- Finding things: routes are in `app()`; config keys are the struct fields in `config.rs`; account-health transitions are the `AccountHealth` writes in `proxy.rs` and reads in `routing/select.rs`.

## Keep this file current

Update this file in the same change that alters the repo's structure or core behavior: adding, moving, or removing modules, directories, endpoints, or workflows, and changes to routing, account health, auth, streaming, or config semantics.
