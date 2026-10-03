# Native Cerebras support

Tokenproxy translates the HTTP Responses API used by Codex into Cerebras Chat Completions. Both buffered JSON and incremental SSE responses are translated back. The implementation is part of the Rust binary and adds no dependencies.

## Account configuration

Export `CEREBRAS_API_KEY` and the existing `TOKENPROXY_CLIENT_KEY`, then add:

```toml
[[accounts]]
id = "cerebras"
kind = "cerebras_api_key"
token_env = "CEREBRAS_API_KEY"
supports_chat_completions = true
supports_responses = true
models = ["qwen-3.8-27b", "gpt-oss-120b"]
```

The default base URL is `https://api.cerebras.ai/v1`. An explicit `base_url` overrides it. Omitting `models` discovers IDs through Cerebras `/models`; an explicit list skips discovery. The account uses normal Tokenproxy routing, priorities, health tracking, and precommit failover. HTTP errors, including insufficient-credit 402 and rate-limit 429, retain their upstream status and body.

`supports_responses_ws`, `supports_compact`, and `supports_anthropic_messages` must remain false. `supports_incremental_previous_response_id` is normalized to false. `auto_use_reset` remains ChatGPT-only.

## Codex configuration

Keep the combined model catalog generated for your installed Codex release. Point the provider directly at Tokenproxy, with no LiteLLM URL:

```toml
model_provider = "tokenproxy"
model = "qwen-3.8-27b"
model_catalog_json = "/absolute/path/to/models-with-cerebras.json"
web_search = "disabled"

[model_providers.tokenproxy]
name = "OpenAI and Cerebras"
base_url = "http://127.0.0.1:8787/v1"
wire_api = "responses"
env_key = "TOKENPROXY_CLIENT_KEY"
requires_openai_auth = false
supports_websockets = false
```

For an existing Tokenproxy provider, retain its name, port, and credential settings. Disable hosted web search for sessions that use Cerebras, either in configuration or with `codex -c 'web_search="disabled"'`. Codex 0.160.0 still included that hosted tool in a live request even with the model's `supports_search_tool` set to false. Function-based MCP search tools can be used instead.

The Codex `/model` menu uses its rich local catalog, not just the IDs returned by `/v1/models`. Preserve OpenAI entries, add the Cerebras entries in the schema accepted by your installed CLI, and use these compatibility settings:

- Set `use_responses_lite = false` on every catalog entry using Tokenproxy, including hidden helper models.
- For Cerebras entries, set `apply_patch_tool_type = null`, `supports_search_tool = false`, and `supports_reasoning_summary_parameter = false`.
- Use function tools and an appropriate shell tool; Codex's freeform `apply_patch` tool requires a custom grammar and is rejected. Shell-based edits remain available.
- Advertise only the reasoning efforts and input modalities supported by the selected Cerebras model.
- Refresh the static catalog when updating Codex or adding models. A catalog copied from a different CLI version can fail schema validation.

OpenAI and Cerebras can share the same provider and `/model` menu because Tokenproxy routes by model ID. OpenAI accounts keep their existing authentication and request behavior. Existing OpenAI conversations containing custom-tool history or encrypted-only reasoning cannot be replayed through Cerebras; start a new conversation when those items are present. Model names must identify the intended upstream; overlapping model allowlists follow normal account selection.

Account service-tier filtering runs before translation. Cerebras defaults to `auto` and `default`. If a client sends another tier, it must be explicitly allowed in the account's `service_tiers` list to route there. The adapter omits that OpenAI tier upstream; allowing `priority` does not purchase or promise a Cerebras priority tier.

## Supported translation

| Responses feature | Cerebras behavior |
| --- | --- |
| Instructions and system/developer messages | Combine text in encounter order into one leading system message; the two instruction roles are collapsed |
| User/assistant text and user image URLs | Preserve message order and convert content-part shapes; image support depends on the model |
| Function definitions and namespaces | Flatten names with collision checks and deterministic aliases for provider name limits; restore original names and namespaces on output |
| Function calls and outputs | Preserve call IDs and arguments; combine adjacent assistant calls for parallel tool-result history |
| Named standalone function outputs (no call ID) | Preserve tool name, optional namespace, and text as a separate attributed assistant message, including Codex TUI child tasks and follow-ups |
| Plaintext agent messages | Preserve sender, recipient, and text in a separate assistant message; native collaboration calls explicitly mark their task arguments as plaintext |
| Plaintext reasoning history | Prefer full content, fall back to summary, and attach it to its assistant turn |
| Reasoning effort | Forward as `reasoning_effort`; Cerebras validates model-specific values |
| Output budget | Map `max_output_tokens` to `max_completion_tokens` |
| JSON object/schema format | Map to `response_format` without weakening schema constraints |
| Prompt cache key | Forward to Cerebras |
| Streaming reasoning/text/tools | Emit stable item IDs, indexes, ordered event sequence numbers, and argument deltas |
| Usage | Map input/output totals and cached/reasoning token details |
| Length/content-filter termination | Report `response.incomplete`; never announce an incomplete tool call as executable |

Metadata, storage-disabled hints, reasoning-summary preference, verbosity, cache-retention hints, safety identifiers, and OpenAI service tiers are not sent upstream. Generated plaintext reasoning is represented as a separate Responses reasoning item, rather than assistant answer text.

Chat Completions requires tool results to reference a matching call. For standalone outputs, the adapter uses assistant text with a `Tool output from namespace.name:` prefix. This preserves attribution and avoids fabricating a call or elevating tool content to user/system instructions, but cannot retain a distinct tool role. Missing or null call IDs use this translation only with a nonempty tool name; empty or wrongly typed IDs are rejected. Reasoning and later calls remain separate from the standalone output.

Unsupported semantic features return a clear 400: stored continuations (`previous_response_id` or `conversation`), `store=true`, background requests, automatic truncation, tool-call limits, item references, encrypted-only reasoning, file/audio inputs, hosted tools, and custom/freeform grammar tools. Nontext instructions or tool outputs are also rejected. The adapter does not implement remote compaction or retain response history; clients send the full conversation. Unexpected upstream refusal payloads return an upstream error rather than an empty successful answer.

Codex v2 collaboration can pass plaintext tasks from Cerebras parents to fresh subagents. OpenAI parents can produce encrypted task messages, which Cerebras cannot decrypt. Those messages are rejected explicitly. A shared provider and model catalog do not make encrypted cross-provider delegation compatible; use a Cerebras parent or a client workflow that sends plaintext tasks.

## Streaming and failure behavior

The converter waits for a valid upstream event before emitting `response.created`, so failures before the first event can use Tokenproxy's existing retry policy. Once downstream output starts, failures end the stream without replaying the request. An incomplete or malformed stream never becomes `response.completed`.

Raw SSE parsing uses the existing pending-frame bound. Accumulated Cerebras SSE data is limited by `server.max_body_bytes`. Conversion remains incremental and follows existing idle timeouts, downstream backpressure, and cancellation. The final `[DONE]` marker is required after a valid finish reason; usage-only chunks before it are retained.

## Validation

```sh
cargo check --locked
cargo test --locked --profile fast-build
cargo fmt --check
```

The focused integration suite is `cargo test --test cerebras`. It covers native authentication and path mapping, billing-error passthrough, rate-limit and precommit failover, fragmented network frames, a complete tool-call round trip, standalone Codex TUI child tasks and follow-ups, unsupported requests, and failures after downstream commitment. Inline tests cover config/discovery, namespaces and aliases, reasoning placement, item ordering, usage, and incomplete-tool suppression.

Live checks on October 3, 2026 used Codex CLI 0.160.0 and a separately bound development binary:

- Qwen 3.8 27B streamed text and reasoning with token usage directly through Tokenproxy.
- Codex executed `printf NATIVE_CEREBRAS_OK` through Qwen and returned its output with hosted web search disabled.
- GPT-6 Astra returned `OPENAI_NATIVE_ROUTER_OK` through the same development router using the existing ChatGPT account.
- Qwen 3.8 27B and GPT OSS 120B both returned a forced namespaced function call with the original namespace and member name restored.

## References and implementation precedents

These implementations were inspected through GitHub source and PR diffs; they informed protocol handling and regression tests.

- [Cerebras Chat Completions API](https://inference-docs.cerebras.ai/api-reference/chat-completions), [reasoning](https://inference-docs.cerebras.ai/capabilities/reasoning), and [tool use](https://inference-docs.cerebras.ai/capabilities/tool-use): native request fields, reasoning deltas, and function calls.
- [Codex–Cerebras bridge](https://github.com/applyinnovations/codex-cerebras-bridge/blob/92c34928877dcbe16effcf412cc8808b1c865897/plugin/main.go#L376): consolidate instruction messages across the entire input without dropping text.
- [Bifrost namespace translation](https://github.com/maximhq/bifrost/blob/e8b579fd1a710753cfa97611b5cff6c3411b0825/core/providers/utils/namespacetools.go#L80): reversible names, length limits, collision checks, and unambiguous tool selection.
- [CLIProxyAPI Chat-to-Responses conversion](https://github.com/router-for-me/CLIProxyAPI/blob/d7914afdedca7af95ee974a42453dc49fc1388ce/internal/translator/openai/openai/responses/openai_openai-responses_response.go): buffered tool identity/argument fragments and terminal usage handling.
- [LiteLLM streaming tool events](https://github.com/BerriAI/litellm/pull/19368) and [message-announcement fix](https://github.com/BerriAI/litellm/pull/41564): announce items before their deltas and retain correct indexes for mixed reasoning, text, and tools.
- [Codex namespace failures](https://github.com/openai/codex/issues/23186), [Qwen compatibility report](https://github.com/openai/codex/issues/36942), and [namespace gate removal](https://github.com/openai/codex/pull/50447): restoring namespaces is necessary for actual tool dispatch.
- [Codex function-style apply_patch removal](https://github.com/openai/codex/pull/21651), [empty-tools local compaction](https://github.com/openai/codex/issues/46773), and [catalog version mismatch](https://github.com/openai/codex/issues/38934): client configuration and compatibility boundaries.
