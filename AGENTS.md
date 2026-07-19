# Tokenproxy Agent Guide

Tokenproxy is a single-binary Rust server that fronts OpenAI Chat Completions, Responses (HTTP and WebSocket), and Anthropic Messages behind one endpoint, and spreads traffic across a pool of upstream accounts (OpenAI API keys, Anthropic API keys, ChatGPT Codex `auth.json` credentials). When an account hits a usage limit, gets throttled, or fails auth, routing shifts to healthy accounts.

The implementation lives in `src/`. `CLAUDE.md` is a symlink to this file, so this map is also the project instruction set for coding agents; the earlier spec-writing-stage instructions this file used to hold live in git history.

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
| `stage_one_evidence/` | Measurement artifacts from the spec stage. |
| `CLAUDE.md` | Symlink to this file. |
| `CLIProxyAPI/`, `codex/`, `iggy/`, `monoio/`, `pingora/`, `quiche/`, `ripgrep/`, `rust-sdks/`, `s2n-quic/`, `uv/` | Local reference clones for studying mature implementations. Git-ignored, not tracked, not submodules. Never edit them. |

## Source modules

| Module | Role |
|--------|------|
| `main.rs` | CLI entry: flag parsing, config load, server startup, shutdown signals. |
| `config.rs` | Config schema and parsing: `Config`, `AccountConfig`, `AccountKind`, timeouts, retries, downstream auth, admin auth; resolves to `EffectiveConfig`. |
| `auth.rs` | ChatGPT Codex `auth.json` handling: token snapshots per account, bearer checks, refresh after upstream 401 (`ChatGptAuthCell`). |
| `server/state.rs` | `AppState`: runtime account-health store and persistence across reloads. |
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
| `metrics.rs` | Metrics registry behind `/metrics`. |
| `observability.rs` | Request-body dump records and hashing for debugging. |
| `logging.rs`, `error.rs`, `time_parse.rs` | Structured logging, error-to-response mapping, timestamp parsing. |

## HTTP surface

Defined in `app()` in `src/server/state/proxy.rs`: `/healthz`, `/metrics`, `/usage`, `/admin/config/status`, `/admin/config/reload`, `/v1/models`, `/v1/chat/completions`, `/v1/messages`, `/v1/responses` (POST for HTTP, GET upgrades to WebSocket), `/v1/responses/compact`, and a fallback that passes unknown paths through to the upstream.

## Request hot path

Client request → router (`proxy.rs`) → downstream auth check → request classification (`http/classify.rs`) → account selection (`routing/select.rs`) → auth snapshot (`auth.rs`) → upstream request (`http/forward.rs`) → streamed response with SSE repair or WebSocket relay → health and metrics recording (`server/state.rs`, `metrics.rs`).

## Working in this repo

- `cargo check` before building; `cargo test --lib --bin tokenproxy` runs the full suite (~300 tests, under a minute); `cargo fmt --check` before pushing.
- Tests are inline `#[cfg(test)]` modules next to the code they cover; most live in `server/state/proxy.rs`.
- Releases: bump `version` in `Cargo.toml` and `Cargo.lock` in one commit on main, tag it `vX.Y.Z`, push the tag. `release.yml` does the rest.
- Finding things: routes are in `app()`; config keys are the struct fields in `config.rs`; account-health transitions are the `AccountHealth` writes in `proxy.rs` and reads in `routing/select.rs`.

## Keep this file current

Update this file in the same change that alters the repo's structure or core behavior: adding, moving, or removing modules, directories, endpoints, or workflows, and changes to routing, account health, auth, streaming, or config semantics. A stale map misleads the next reader; fixing it costs one table row now.
