# Automatic reset validation

The implementation follows the Codex CLI v0.154.0 reset API. All experiments use local mock servers and synthetic account credentials; no live banked resets were consumed.

## Upstream contract

- [Codex backend client](https://github.com/openai/codex/blob/rust-v0.154.0/codex-rs/backend-client/src/client/rate_limit_resets.rs) uses GET `/backend-api/wham/rate-limit-reset-credits` and POST to its `/consume` subpath with `redeem_request_id` and optional `credit_id`.
- [Contract fixtures](https://github.com/openai/codex/blob/rust-v0.154.0/codex-rs/backend-client/src/client/rate_limit_resets_tests.rs) show a top-level `available_count`, plus consume responses containing `code` and optional `windows_reset`.
- [App-server tests](https://github.com/openai/codex/blob/rust-v0.154.0/codex-rs/app-server/tests/suite/v2/rate_limit_reset_credits.rs) exercise ChatGPT bearer/account headers and the outcomes `reset`, `already_redeemed`, `nothing_to_reset`, and `no_credit`.
- [TUI consumption handling](https://github.com/openai/codex/blob/rust-v0.154.0/codex-rs/tui/src/chatwidget/usage.rs#L359) preserves the same redemption key across errors and treats `already_redeemed` as success.
- [HTTP quota fixture](https://github.com/openai/codex/blob/rust-v0.154.0/codex-rs/core/tests/suite/client.rs#L3349) uses `error.type = "usage_limit_reached"` and numeric Unix seconds in `resets_at`.
- [WebSocket quota fixture](https://github.com/openai/codex/blob/rust-v0.154.0/codex-rs/core/tests/suite/client_websockets.rs#L1637) embeds that error in an error event; [SSE fixtures](https://github.com/openai/codex/blob/rust-v0.154.0/codex-rs/codex-api/src/sse/responses.rs#L1095) establish that `rate_limit_exceeded` is a distinct throttle signal.

Tokenproxy omits `credit_id` and lets the backend select a credit. It does not promise a particular credit order. SSE usage-exhaustion events are synthetic compatibility cases; the referenced Codex SSE fixture covers temporary throttling, not banked-reset redemption.

## Experiments

The integration suite runs actual Axum HTTP and WebSocket servers on ephemeral loopback ports, validates full parsed requests, and bounds client waits. It uses no fixed-size single-read TCP mocks.

| Boundary | Verified behavior |
| --- | --- |
| Configuration | Default off; inline TOML opt-in; API-key/peer accounts and ambiguous reset backend paths rejected |
| Account auth | Synthetic bearer and ChatGPT account ID sent; 401 reloads updated auth JSON and retries with the current token |
| HTTP | Explicit quota error redeems and retries the same payload/account even with zero failover budget; type-only errors and numeric reset times recognized |
| Eligibility | Disabled option and generic throttling make no reset requests; zero available credits makes no consume request |
| Outcomes | Successful and duplicate-success responses recover; no-credit, nothing-to-reset, 401, 500, unknown codes, malformed JSON and timeout retain failure/failover behavior |
| Request integrity | Redirects are not followed to another origin; redemption keys are UUIDs; reset response bodies are bounded |
| SSE | First quota event retries before output; one-byte chunks split JSON, UTF-8, and frame delimiters; late quota errors preserve visible output; nested temporary throttling does not redeem |
| WebSocket | Handshake and first quota event recover; late quota errors preserve output and allow another create on the same downstream socket; persistent exhaustion stops after one redemption |
| Concurrency | Eight simultaneous stale failures share one redemption; distinct configuration aliases for the same backend/account also share one redemption |
| Ambiguous completion | Timeout and cancellation preserve the same key; normal routing reconciles it after the cooldown, including across config reload |
| Reload | Disabling resets during a blocked lookup prevents consumption; enabling resets recovers an already exhausted account; account replacement invalidates old stream context |

Validation commands:

```sh
cargo fmt --check
cargo check --all-targets
cargo build --locked --profile fast-build
cargo test --locked --profile fast-build
cargo clippy --all-targets --all-features --locked -- -D warnings
```

The suite contains 308 library tests, 7 binary tests, and 22 reset integration tests. The two concurrent-redemption tests are additionally repeated 100 times to exercise scheduling variation.

## Scope

Local mocks establish client behavior against the source-defined contract; they do not establish account entitlement or live backend availability. Reset coordination and pending redemption keys survive config reload in one process, but are not persisted across process restart or shared between proxy instances. Transparent replay ends at the first downstream SSE event or WebSocket text output. The reset request uses the configured account's backend, so that backend must expose the Codex reset API.
