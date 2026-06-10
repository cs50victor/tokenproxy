<h1 align="center">tokenproxy</h1>

<p align="center">Small, fast Rust proxy for OpenAI and Anthropic agent traffic.</p>

Tokenproxy serves OpenAI Chat Completions, Responses (HTTP and WebSocket), and Anthropic Messages on one port. It routes each request to the best eligible upstream account — OpenAI API keys, Anthropic API keys, or ChatGPT Codex `auth.json` credentials — scoring accounts by health, priority, and observed latency. Accounts that fail auth, hit usage limits, or get throttled cool down while traffic shifts to the rest.

## Usage

```sh
cargo run --release -- --config tokenproxy.toml
```

Listens on `127.0.0.1:8787` by default. Override any config value with `-c key=value` (dotted TOML paths, Codex CLI style). Run `tokenproxy --help` for network probes and benchmark modes.

## Design

The system design spec lives in [`very_detailed_tokenproxy_spec.html`](very_detailed_tokenproxy_spec.html). The submodules are reference material for high-performance Rust projects studied during design.
