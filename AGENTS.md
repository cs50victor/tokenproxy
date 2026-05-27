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

Write the report like a technical research paper. Define the method, cite every external claim inline, and include an APA-style References section at the end of the report. The final section of the HTML file must be a performance-review attestation. That attestation is an evidence audit, not a narrative summary.

Performance-review attestation requirements:

- Re-read `very_detailed_tokenproxy_spec.html` before editing the attestation. Do not preserve old attestation claims unless the current checkout still proves them.
- Run `git submodule status --recursive` and account for every listed submodule. For each initialized submodule, record its commit and the exact source files reviewed. For each uninitialized submodule, record the `git submodule status` line and do not use it to support a design choice.
- Source review must inspect implementation files, benchmark harnesses, or tests, not README prose alone. Cite exact file paths and line ranges for each performance-relevant fact: pooling, retry boundaries, backpressure, runtime model, parser choice, event-loop design, WebSocket/SSE flow, benchmark method, or telemetry.
- The attestation table must include, for each submodule: commit, source paths with line ranges, verified source fact, implementation decision supported, and any benchmark or probe artifact that supports the decision.
- Run actual measurements before making any performance claim. This includes passive network probes, tiny Rust experiments, Rust performance experiments, or workflow benchmarks appropriate to the claim. Include command, timestamp, environment, sample count, raw artifact path, and summary statistics such as p50, p95, p99, errors, and outliers.
- Do not replace benchmarks with instructions for how to run benchmarks. A section may describe reproducibility, but every accepted performance decision must also point to a completed local run or a captured upstream artifact that was reviewed.
- If credentials, network policy, missing tooling, platform limits, or absent product code prevent a benchmark, state that exact blocker, include the failed or skipped command, and mark the claim as an unanswered research question. Do not convert blocked measurements into design facts.
- Do not write phrases such as "benchmark-backed", "measured", "validated", "reviewed", or "performance-proven" unless the report includes the source line references and benchmark/probe artifacts that justify the word.
- The attestation must be the final substantive section of the HTML file and must map measured results back to concrete stage-two implementation decisions. If no actual performance experiments were run, the attestation must say so plainly and must not endorse latency-sensitive choices beyond source-backed correctness or complexity observations.

Integration-test evidence requirements:

- Do not stop at a required test matrix. If the report says an integration behavior is validated, include the actual test command, timestamp, environment, fixture or server used, pass/fail result, and artifact path or captured output.
- If product code does not exist yet, state that integration tests could not run because there is no implementation under test. Keep those cases in a future test matrix, not in the measured-results or attestation sections.
- Do not use integration-test language such as "validated", "verified", "passes", "covered", or "ready" for fake-server, SSE, WebSocket, failover, or metrics behavior unless a real test was executed in the current checkout.
- Every integration-test claim must name the boundary tested: direct upstream probe, local fake OpenAI server, generated Rust experiment, or future stage-two implementation. Do not blur planned tests with completed tests.

Comparative design-section requirements:

- Comparative sections that argue for or against a framework, runtime, protocol stack, parser, transport, pooling strategy, scheduler model, or other performance-sensitive dependency must be evidence sections, not deferrals. They must cite source lines for complexity claims and include actual local measurements or reviewed upstream benchmark artifacts for performance claims.
- Do not write final design rules that say only "measure first", "until a benchmark shows", "after a local implementation has a measured bottleneck", "Tokenproxy should measure", or equivalent future-tense gates. Replace them with one of two forms: a measured decision with the command, artifact, sample count, and result; or an explicit unanswered research question with the blocking reason.
- A decision to avoid any performance-motivated dependency or advanced implementation path must state whether the decision rests on measured performance, source-backed complexity, ecosystem compatibility, operational risk, maintainability, or absent product code. Do not imply a performance result when the real evidence is only complexity, compatibility, or scope control.
- If the report recommends any latency-sensitive default, it must show the local comparison that was run or state that the default is provisional and not performance-proven.

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
