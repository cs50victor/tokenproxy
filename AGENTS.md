# Tokenproxy Stage One

We are in stage one of this repository.

Tokenproxy is a minified Rust port of the CLIProxyAPI project. It only supports the OpenAI Chat Completions API and the OpenAI Responses API, with special focus on Codex usage and WebSocket support.

Like CLIProxyAPI, tokenproxy is intended to let users sign in to multiple ChatGPT accounts and route requests across them. Its primary design goal is very low latency with high uptime and availability at all costs.

The project goal is to write a system design spec for the next stage, which is coding tokenproxy. The spec should describe how tokenproxy becomes a small, fast Rust proxy for OpenAI-compatible agent traffic. It should guide architecture, crate choices, data paths, concurrency choices, failure behavior, and benchmark targets.

Optimize design decisions for benchmarkability and concise code. Prefer choices that can be measured with small Rust experiments, passive network measurements, and short source snippets. Avoid complex designs unless a benchmark, source citation, network trace, or failure-mode analysis justifies the added code.

OpenAI's WebSockets work is important context for this goal. Treat WebSocket support as a first-class design target for Codex and agentic workflows because persistent Responses API connections can avoid repeated per-request work, reuse previous response state, cache reusable context, process only new input, and keep the API shape close to `response.create` plus `previous_response_id`.

In the report, call out API and orchestration choices that affect latency and reliability: `previous_response_id`, returned output item replay for stateless flows, assistant `phase` preservation, prompt caching, reasoning effort, verbosity controls, tool preambles, and state compaction.

Latency and response quality require more than Rust benchmarking. Stage one should include passive network and API-path investigation:

- Identify which OpenAI and ChatGPT endpoints matter for tokenproxy, including Chat Completions, Responses, Codex-oriented Responses traffic, and WebSocket transport.
- Use OpenAI status data, especially `https://status.openai.com/`, as operational context for uptime and incident-aware routing.
- Measure DNS resolution, resolved IPs, TLS negotiation, HTTP protocol selection, time to connect, time to first byte, total response time, and stream/WebSocket stability from relevant client networks.
- Compare endpoint behavior with `curl`, `dig`, traceroute-style tools, and small Rust probes where useful.
- Investigate whether endpoint, protocol, connection reuse, request shape, service tier, prompt caching, `prompt_cache_key`, headers, or account selection changes latency, cache hit rate, or reliability.
- Treat OpenAI region, edge, route, and server-IP findings as measured observations, not stable facts, unless repeated measurements prove stability.

Stage one is writing the technical report and system design spec for tokenproxy. The report file must be named `very_detailed_tokenproxy_spec.html`. It should be a plain black and white single `.html` file, visually similar in restraint and readability to `https://burntsushi.net/ripgrep/`.

Treat `very_detailed_tokenproxy_spec.html` as the project's single self-contained implementation authority. Any engineer should be able to read that file and implement tokenproxy from start to finish. Do not leave unresolved assumptions, "likely" statements, TODO-style placeholders, or design claims that depend on unstated context. Present claims as facts only when measurements, citations, traces, or source excerpts support them; otherwise mark them as unanswered research questions and keep them out of implementation decisions.

Write the report like a technical research paper. Define the method, cite every external claim inline, and include an APA-style References section at the end of the report. The final section of the HTML file must be a performance-review attestation. That attestation must summarize which submodule references were reviewed, state what implementation choices they support, and include references from every repository submodule. If a submodule cannot be reviewed, state the reason in the attestation.

Use the ripgrep article as the structural model:

- State the claims the spec will defend.
- Explain the anatomy of the proxy before presenting benchmarks.
- Describe the benchmark methodology and environment.
- Use end-user workflows as benchmark cases.
- For each result, explain the system design choice behind it.
- Include tradeoffs, anti-pitch notes, and cases where a simpler design wins.
- End by mapping measured results back to concrete implementation decisions.

Use basic Tailwind CSS from the HTML header when styling the report.

The submodules are reference material for high-performance Rust projects. Use them to study system design choices, implementation decisions, and performance-oriented tradeoffs that can inform the tokenproxy report.

Do not write product or application code in this stage. Work should be limited to:

- Running tiny Rust experiments.
- Running Rust performance experiments.
- Running passive network traversal and endpoint latency experiments.
- Capturing reproducible measurements.
- Evaluating crate and package choices.
- Explaining the reasoning behind each code decision planned for stage two.
- Adding citations.
- Adding relevant code snippets to the `.html` report.

Keep the report evidence-led. Claims should be backed by measurements, citations, or directly quoted source snippets.

Write plainly. Use active voice, concrete claims, short paragraphs, and no promotional language. Omit needless words.
