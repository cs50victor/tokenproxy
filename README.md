<h1 align="center">tokenproxy</h1>

<p align="center">Small, fast Rust proxy for OpenAI-compatible agent traffic.</p>

Tokenproxy routes Chat Completions, Responses (HTTP and WebSocket), and Anthropic Messages requests across multiple ChatGPT accounts. It targets low latency and high availability for Codex and agent workloads.

## Usage

```sh
cargo run --release -- --config tokenproxy.toml
```

Listens on `127.0.0.1:8787` by default. Override any config value with `-c key=value` (dotted TOML paths, Codex CLI style). Run `tokenproxy --help` for network probes and benchmark modes.

## Design

The system design spec lives in [`very_detailed_tokenproxy_spec.html`](very_detailed_tokenproxy_spec.html). The submodules are reference material for high-performance Rust projects studied during design.
