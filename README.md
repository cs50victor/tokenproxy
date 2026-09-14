<h1 align="center">tokenproxy</h1>

<p align="center">Small, fast Rust proxy for OpenAI and Anthropic agent traffic.</p>

Tokenproxy is a server that fronts OpenAI Chat Completions, Responses (HTTP and WebSocket), and Anthropic Messages with one endpoint and spreads the traffic across a pool of upstream accounts: OpenAI API keys, Anthropic API keys, and ChatGPT Codex `auth.json` credentials. When an account hits a usage limit, gets throttled, or fails auth, traffic shifts to the rest, so Codex and other agent clients keep working.

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

## Credits

Tokenproxy is a minified Rust port of [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI), narrowed to OpenAI and Anthropic agent traffic with a focus on latency and Codex workflows.
