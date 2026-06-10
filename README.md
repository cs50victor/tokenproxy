<h1 align="center">tokenproxy</h1>

<p align="center">Small, fast Rust proxy for OpenAI and Anthropic agent traffic.</p>

Tokenproxy is a server that fronts OpenAI Chat Completions, Responses (HTTP and WebSocket), and Anthropic Messages with one endpoint and spreads the traffic across a pool of upstream accounts: OpenAI API keys, Anthropic API keys, and ChatGPT Codex `auth.json` credentials.

## Why

One agent endpoint backed by many accounts. When an account hits a usage limit, gets throttled, or fails auth, tokenproxy cools it down and routes around it, so Codex and other agent clients keep working. It ships as a single binary with WebSocket Responses support built in.

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
  models = ["gpt-5.5"],
  supports_responses = true,
  supports_responses_ws = true
}]'
```

Binds to `127.0.0.1:8787` by default; to serve remote clients, set a public `server.bind` and `server.allow_non_loopback = true`. Clients authenticate with the bearer token from `TOKENPROXY_CLIENT_KEY`. For a persistent setup use `--config tokenproxy.toml`; `-c key=value` overrides any config value with dotted TOML paths, Codex CLI style.

## Load balancing

Each request first filters the account pool: disabled, auth-failed, usage-limited, cooling-down, and capability-mismatched accounts (endpoint, model, service tier, WebSocket) are excluded. The rest are ranked by continuation affinity (stay on the account that holds the `previous_response_id` state), health, configured priority, smoothed connect and first-event latency, and recent failure count. The best-ranked account gets the request, and failures feed back into health so traffic shifts automatically.

## Releases

| Version | Date | |
|---|---|---|
| [v0.1.1](https://github.com/cs50victor/tokenproxy/releases/tag/v0.1.1) | 2026-06-05 | Latest |
| [v0.1.0](https://github.com/cs50victor/tokenproxy/releases/tag/v0.1.0) | 2026-06-05 | First release |

Prebuilt for macOS (arm64, x86_64), Linux (gnu and musl, arm64 and x86_64), and Windows (x86_64, i686, arm64), each with a sha256 checksum.

## Design

The system design spec lives in [`very_detailed_tokenproxy_spec.html`](very_detailed_tokenproxy_spec.html). The repos checked out alongside the source (pingora, monoio, quiche, ripgrep, and others) are reference material studied during design.

## Credits

Tokenproxy is a minified Rust port of [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI), narrowed to OpenAI and Anthropic agent traffic with a focus on latency and Codex workflows.
