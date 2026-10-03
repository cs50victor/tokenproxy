<h1 align="center">tokenproxy</h1>

<p align="center">Small, fast Rust proxy for OpenAI, Cerebras, and Anthropic agent traffic.</p>

Tokenproxy is a server that fronts OpenAI Chat Completions, Responses (HTTP and WebSocket), and Anthropic Messages with one endpoint and spreads the traffic across a pool of upstream accounts: OpenAI API keys, Cerebras API keys, Anthropic API keys, and ChatGPT Codex `auth.json` credentials. When an account hits a usage limit, gets throttled, or fails auth, traffic shifts to the rest, so Codex and other agent clients keep working.

## Install and run

Download a binary from [releases](https://github.com/cs50victor/tokenproxy/releases) (macOS arm64 shown; pick your platform):

```sh
curl -sL https://github.com/cs50victor/tokenproxy/releases/download/v0.1.1/tokenproxy-v0.1.1-aarch64-apple-darwin.tar.gz | tar xz
cd tokenproxy-v0.1.1-aarch64-apple-darwin
```

Set the token your clients will use, plus your upstream key:

```sh
export TOKENPROXY_CLIENT_KEY=secret
export OPENAI_API_KEY=sk-...
```

Start it with inline config; no config file needed:

```sh
./tokenproxy -c 'accounts=[{
  id = "a",
  kind = "openai_api_key",
  token_env = "OPENAI_API_KEY",
  supports_responses = true,
  supports_responses_ws = true
}]'
```

Binds to `127.0.0.1:8787` by default; to serve remote clients, set a public `server.bind` and `server.allow_non_loopback = true`. OpenAI and ChatGPT accounts discover their available models at startup; an optional `models = [...]` list acts as an allowlist over discovered models, with unknown IDs ignored. Clients authenticate with the bearer token from `TOKENPROXY_CLIENT_KEY`. Set `TOKENPROXY_CONFIG_UPDATE_ENDPOINT` only when refreshed ChatGPT auth JSON should be posted to a compatible config service; local `auth_json_path` files are still rewritten directly. For a persistent setup use `--config tokenproxy.toml`; `-c key=value` overrides any config value with dotted TOML paths, Codex CLI style.

## HTTP body limits

HTTP request bodies have no configured size cap by default, including decoded zstd requests and passthrough routes. Providers enforce their own request limits, and their HTTP 413 status and error body are forwarded to the client.

If account failover cannot succeed, Tokenproxy preserves the last provider HTTP error status, body, and allowed response headers, including `Retry-After` and `x-request-id`. Provider quota errors remain HTTP 429. Later requests rejected locally because no account is eligible return HTTP 503 with the exclusion reasons and earliest applicable cooldown or quota deadline. These local rejections do not contact the provider or replay a previous request's error. Expired deadlines permit routing again, including passthrough requests.

To impose a local cap, set `server.max_body_bytes` in bytes:

```toml
[server]
max_body_bytes = 67108864
```

This example sets a 64 MiB cap. Omit the setting to leave HTTP bodies unlimited. The setting also bounds buffered JSON and compact responses and accumulated Cerebras output. Bodies are still buffered in memory; Codex Live control-message limits are separate.

## Cerebras and Codex

Cerebras accounts accept native Chat Completions and translate HTTP Responses requests inside Tokenproxy, including streaming text, reasoning, and function-tool calls. No companion proxy or Python package is required.

```toml
[[accounts]]
id = "cerebras"
kind = "cerebras_api_key"
token_env = "CEREBRAS_API_KEY"
supports_chat_completions = true
supports_responses = true
models = ["qwen-3.8-27b", "gpt-oss-120b"]
```

The default upstream is `https://api.cerebras.ai/v1`; omit `models` to discover available IDs. Add this account alongside existing OpenAI or ChatGPT accounts to route both providers through one endpoint.

Codex requires a matching model catalog for its `/model` picker, HTTP Responses transport, and hosted web search disabled for Cerebras sessions. WebSocket Responses, remote compaction, stored continuations, and custom grammar tools are not supported by this adapter. See [configuration, compatibility, and validation](docs/cerebras.md).

## Load balancing

Each request first filters the account pool: disabled, auth-failed, usage-limited, cooling-down, and capability-mismatched accounts (endpoint, model, service tier, WebSocket) are excluded. The rest are ranked by continuation affinity (stay on the account that holds the `previous_response_id` state), health, configured priority, smoothed connect and first-event latency, and recent failure count. The best-ranked account gets the request, and failures feed back into health so traffic shifts automatically.

## Automatic usage resets

Set `auto_use_reset = true` on a ChatGPT account to redeem a banked usage-limit reset when that account returns `usage_limit_reached`:

```toml
[[accounts]]
id = "chatgpt"
kind = "chatgpt_codex_auth_json"
auth_json_path = "~/.codex/auth.json"
supports_responses = true
supports_responses_ws = true
auto_use_reset = true
```

The option defaults to `false` and is supported only for ChatGPT accounts with a base URL ending in `/codex`. It uses the account's existing credentials with the same backend reset API as Codex `/usage`; the backend selects the next available reset. Ordinary request throttling does not redeem a reset.

After a successful redemption, Tokenproxy clears cached quota health and retries the same account once, even when `retry.max_precommit_retries = 0`. HTTP 429 responses, the first SSE quota-error event, WebSocket handshakes, and WebSocket quota-error events before downstream output can recover transparently. A quota error after stream output attempts recovery for future requests and forwards the error without replaying output. Failed or unavailable resets retain normal failure and failover behavior.

Redemptions are serialized per backend and ChatGPT account identity, with a 30-second cooldown and a total timeout capped at 10 seconds by `timeouts.request_header_ms`. Ambiguous failures retain the same redemption key across retries and config reloads; when no account is eligible, a later request can reconcile the pending redemption after the cooldown. This coordination is local to one process and does not survive restart. Reloading config can enable or disable the option.

See [validation and upstream API references](docs/auto-use-reset-validation.md) for the mock experiments.

## Codex Live voice

Enable subscription voice on a ChatGPT account:

```toml
[[accounts]]
id = "chatgpt"
kind = "chatgpt_codex_auth_json"
auth_json_path = "~/.codex/auth.json"
supports_responses = true
supports_responses_ws = true
supports_realtime = true
```

For Codex CLI v0.157.1, put these overrides at the root of its `config.toml`, before any table, while retaining your existing Tokenproxy provider and client credential:

```toml
experimental_realtime_webrtc_call_base_url = "http://127.0.0.1:8787/backend-api/codex"
experimental_realtime_ws_base_url = "ws://127.0.0.1:8787/v1/live"
```

Tokenproxy forwards JSON call setup and the continuous control WebSocket using the same ChatGPT account. WebRTC audio travels directly between Codex and OpenAI. Native `/voice` uses V3; programmatic app-server clients must initialize with `experimentalApi = true` and explicitly request `version = "v3"` with a WebRTC SDP offer. Access still depends on the upstream account's voice entitlement.

Voice is opt-in and independent of the text-model discovery allowlist. New calls choose the highest-priority eligible voice account. A call never fails over or replays: only an explicit 401 can trigger one same-account credential refresh and retry. Voice 403 responses do not disable otherwise-working text credentials. Call ownership survives sideband disconnects for five minutes, allowing reconnection to the original account; config changes that remove that account or change its identity/backend block reconnection. Restarting Tokenproxy loses call bindings.

The process admits at most 1,024 pending or connected calls, including in-flight setup requests. Expired disconnected bindings are removed on subsequent call operations. Setup bodies and WebSocket messages are limited to the smaller of `server.max_body_bytes` and 1 MiB. Setup and handshake use the existing request/connection timeouts; `websocket_idle_ms` bounds stalled writes, while quiet control connections remain open because audio uses WebRTC. Only one sideband can attach to a call at a time.

The default upstream sideband is `wss://api.openai.com/v1/live`; an account can override `realtime_ws_base_url` for a compatible backend. The upstream `Location` supplies only the call ID, never the connection destination. See [implementation choices, protocol references, and validation](docs/realtime-validation.md).

## Credits

Tokenproxy is a minified Rust port of [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI), narrowed to OpenAI and Anthropic agent traffic with a focus on latency and Codex workflows.
