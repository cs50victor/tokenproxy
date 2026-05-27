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

Treat `very_detailed_tokenproxy_spec.html` as the project's single self-contained implementation authority. Any engineer should be able to read that file and implement tokenproxy from start to finish. Do not leave unresolved assumptions, "likely" statements, TODO-style placeholders, open questions, or design claims that depend on unstated context. When evidence is missing, run source review, passive network probes, small Rust experiments, or workflow measurements to close the knowledge gap before writing the claim. If a gap cannot be closed because credentials, network policy, missing tooling, platform limits, or absent product code block the work, remove the affected design claim or narrow it to what the evidence supports. Do not leave unanswered questions in the HTML report.

Write the report like a technical research paper. Define the method, cite every external claim inline, and include an APA-style References section after the graphical user-experience section. Place the performance-review attestation after References as the final substantive section. That attestation is an evidence audit, not a narrative summary.

Graphical user-experience section requirements:

- Add a very detailed, visual-first user-experience section immediately before References. This section must show what operators and agent clients see when tokenproxy behaves correctly.
- Keep the section inside the single self-contained HTML file. Use inline HTML, Tailwind classes already available from the document header, inline SVG, CSS-only controls, `<details>` disclosures, tables, and small embedded scripts only when interaction materially improves comprehension. Do not add external assets, package dependencies, build tooling, or generated binary media.
- Keep the black-and-white, ripgrep-like restraint. Visuals should be dense, legible, and technical: thin borders, monospace labels where useful, clear axis labels, compact legends, and no decorative gradients, color-heavy dashboards, stock imagery, or marketing layout.
- Place each visual next to the design decision it explains. Each diagram, chart, graph, or UI mock must have a caption that states the implementation behavior it supports and cites measured or source-backed evidence.
- Include an operator-facing dashboard mock that shows account pool state, upstream health, route selection, open WebSocket sessions, prompt-cache status, rate-limit pressure, in-flight requests, retry/failover state, and current incident signals from OpenAI status context.
- Include an agent-client view that shows the developer-facing request lifecycle for Chat Completions, Responses, and persistent WebSocket Responses traffic: request admission, account choice, connection reuse, `previous_response_id`, output item replay, assistant `phase` preservation, tool preambles, compaction, streaming, and error return paths.
- Include an interactive architecture map that lets the reader expand the hot path from client request to upstream OpenAI/ChatGPT endpoint and back. It must distinguish synchronous request work, background health probes, account/session bookkeeping, credential storage boundaries, telemetry emission, and failure handling.
- Include a latency and reliability visual pack: request timeline, connection reuse comparison, cache-hit path versus cold path, retry/failover state machine, backpressure queue behavior, and p50/p95/p99 measurement summary cards sourced from completed local probes. If a measurement is missing, run the experiment or omit the measured value.
- Include endpoint and transport visuals for DNS resolution, TLS negotiation, HTTP protocol choice, SSE streaming, and WebSocket stability. Use measured artifact paths, commands, timestamps, and sample counts when those visuals contain measured results.
- Include account routing visuals that show how multiple ChatGPT accounts are selected, cooled down, quarantined, restored, and excluded from traffic. The visuals must separate authenticated user intent from proxy-internal routing state.
- Include failure-mode visuals for upstream outage, account throttling, WebSocket drop, partial SSE frame, malformed upstream response, local overload, credential expiration, and status-page incident detection. Each visual must identify the user-visible response and the internal recovery action.
- Include a compact "correct system at a glance" board that a stage-two engineer can use as a build checklist. It must map UI-visible behavior to concrete implementation modules, data structures, metrics, tests, and evidence artifacts.
- Source every visual. For each visual that shows performance, correctness, endpoint behavior, or upstream behavior, cite the supporting report section, source line range, benchmark artifact, or network probe artifact.
- Preserve this final ordering: graphical user-experience section, References, performance-review attestation.

Performance-review attestation requirements:

- Re-read `very_detailed_tokenproxy_spec.html` before editing the attestation. Do not preserve old attestation claims unless the current checkout still proves them.
- Run `git submodule status --recursive` and account for every listed submodule. For each initialized submodule, record its commit and the exact source files reviewed. For each uninitialized submodule, record the `git submodule status` line and do not use it to support a design choice.
- Source review must inspect implementation files, benchmark harnesses, or tests, not README prose alone. Cite exact file paths and line ranges for each performance-relevant fact: pooling, retry boundaries, backpressure, runtime model, parser choice, event-loop design, WebSocket/SSE flow, benchmark method, or telemetry.
- The attestation table must include, for each submodule: commit, source paths with line ranges, verified source fact, implementation decision supported, and any benchmark or probe artifact that supports the decision.
- Run actual measurements before making any performance claim. This includes passive network probes, tiny Rust experiments, Rust performance experiments, or workflow benchmarks appropriate to the claim. Include command, timestamp, environment, sample count, raw artifact path, and summary statistics such as p50, p95, p99, errors, and outliers.
- Do not replace benchmarks with instructions for how to run benchmarks. A section may describe reproducibility, but every accepted performance decision must also point to a completed local run or a captured upstream artifact that was reviewed.
- If credentials, network policy, missing tooling, platform limits, or absent product code prevent a benchmark, state that exact blocker and include the failed or skipped command in the attestation. Remove or narrow the related performance claim. Do not convert blocked measurements into design facts or leave them as open questions.
- Do not write phrases such as "benchmark-backed", "measured", "validated", "reviewed", or "performance-proven" unless the report includes the source line references and benchmark/probe artifacts that justify the word.
- The attestation must be the final substantive section of the HTML file and must map measured results back to concrete stage-two implementation decisions. If no actual performance experiments were run, the attestation must say so plainly and must not endorse latency-sensitive choices beyond source-backed correctness or complexity observations.

Integration-test evidence requirements:

- Do not stop at a required test matrix. If the report says an integration behavior is validated, include the actual test command, timestamp, environment, fixture or server used, pass/fail result, and artifact path or captured output.
- If product code does not exist yet, state that integration tests could not run because there is no implementation under test. Keep those cases in a future test matrix, not in the measured-results or attestation sections.
- Do not use integration-test language such as "validated", "verified", "passes", "covered", or "ready" for fake-server, SSE, WebSocket, failover, or metrics behavior unless a real test was executed in the current checkout.
- Every integration-test claim must name the boundary tested: direct upstream probe, local fake OpenAI server, generated Rust experiment, or future stage-two implementation. Do not blur planned tests with completed tests.

Comparative design-section requirements:

- Comparative sections that argue for or against a framework, runtime, protocol stack, parser, transport, pooling strategy, scheduler model, or other performance-sensitive dependency must be evidence sections, not deferrals. They must cite source lines for complexity claims and include actual local measurements or reviewed upstream benchmark artifacts for performance claims.
- Do not write final design rules that say only "measure first", "until a benchmark shows", "after a local implementation has a measured bottleneck", "Tokenproxy should measure", or equivalent future-tense gates. Replace them with a measured decision that names the command, artifact, sample count, and result. If evidence is blocked, remove or narrow the decision and record the blocker in the attestation.
- A decision to avoid any performance-motivated dependency or advanced implementation path must state whether the decision rests on measured performance, source-backed complexity, ecosystem compatibility, operational risk, maintainability, or absent product code. Do not imply a performance result when the real evidence is only complexity, compatibility, or scope control.
- If the report recommends any latency-sensitive default, it must show the local comparison that was run. Without local comparison, remove or narrow the recommendation.

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
