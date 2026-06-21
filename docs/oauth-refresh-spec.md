# Tokenproxy ChatGPT OAuth refresh spec

Status: draft for the OAuth-refresh PR.

Date: 2026-06-21.

Scope: this document specifies stage-two OAuth refresh behavior for `ChatgptCodexAuthJson` accounts. It does not add product code. It replaces the removed stale HTML report with a smaller source-backed design note for this PR.

## Summary

Tokenproxy should refresh ChatGPT OAuth credentials itself for file-backed Codex auth JSON accounts. The refresh implementation should be local and narrow. It should not import the Codex login implementation as a crate in stage two.

The first implementation should add a small credential manager around the existing auth JSON parser. That manager should:

- load the current auth JSON at startup, as tokenproxy does now;
- keep the auth JSON source path and parsed refresh token for `ChatgptCodexAuthJson` accounts;
- refresh proactively shortly before the access token expires;
- refresh reactively when an upstream 401 arrives before tokenproxy has committed a downstream response;
- serialize refresh attempts per account;
- reread the auth JSON before calling the token authority, so an external relogin or another tokenproxy process can win without a duplicate refresh;
- persist the refreshed auth JSON before swapping the in-memory bearer token;
- mark the account `AuthFailed` only for permanent credential failure or an upstream 401/403 that cannot be repaired before the downstream response is committed.

The design should keep request routing, upstream forwarding, SSE repair, WebSocket forwarding, and metrics separate from credential refresh. OAuth refresh is credential bookkeeping, not a new routing layer.

## Current tokenproxy behavior

Tokenproxy currently treats ChatGPT Codex auth JSON as a static bearer-token source.

`src/config.rs` loads enabled accounts in `load_effective_config`. For `AccountKind::ChatgptCodexAuthJson`, it expands `auth_json_path`, reads the location once, parses it with `parse_chatgpt_auth_json`, and stores `chatgpt_auth.bearer_token` in `EffectiveAccount.bearer_token`.

`parse_chatgpt_auth_json` accepts these bearer-token fields in order:

1. `tokens.access_token`
2. top-level `access_token`
3. top-level `OPENAI_API_KEY`
4. `tokens.id_token`
5. top-level `id_token`

It validates `last_refresh` when present, preserves `account_id`, and rejects refresh-token-only files. The existing unit test `should_not_use_refresh_token_as_upstream_bearer` covers the refresh-token-only rejection.

At runtime, `record_account_http_status` stores `AccountHealth::AuthFailed` for upstream 401 and 403. `select_account` excludes accounts whose health is `AuthFailed`.

This behavior is correct for static API keys. It is incomplete for Codex OAuth auth JSON, because Codex auth JSON includes an access token and a refresh token. The access token can expire while the refresh token remains valid.

Source observations from this checkout:

| Claim | Source |
| --- | --- |
| `ChatgptCodexAuthJson` accounts are read once during effective config loading. | `src/config.rs:401-475` |
| The parser chooses access tokens before id tokens and rejects refresh-token-only auth JSON. | `src/config.rs:2070-2160` |
| Upstream 401 and 403 store `AccountHealth::AuthFailed`. | `src/server/proxy.rs:3655-3685` |
| Account selection excludes `AuthFailed` accounts. | `src/routing/select.rs:43-55` |

## Current Codex behavior

The local Codex checkout under `/tmp/codex` implements OAuth refresh in `codex-rs/login/src/auth/manager.rs`.

The Codex auth manager defines:

- `TOKEN_REFRESH_INTERVAL = 8` days;
- `CHATGPT_ACCESS_TOKEN_REFRESH_WINDOW_MINUTES = 5`;
- `REFRESH_TOKEN_URL = https://auth.openai.com/oauth/token`;
- `CLIENT_ID_OVERRIDE_ENV_VAR = CODEX_APP_SERVER_LOGIN_CLIENT_ID`;
- `REFRESH_TOKEN_URL_OVERRIDE_ENV_VAR = CODEX_REFRESH_TOKEN_URL_OVERRIDE`.

Its refresh request is a JSON POST with:

```json
{
  "client_id": "...",
  "grant_type": "refresh_token",
  "refresh_token": "..."
}
```

Codex treats these backend error codes as permanent refresh-token failures:

- `refresh_token_expired`
- `refresh_token_reused`
- `refresh_token_invalidated`

Codex also protects refresh with a guarded reload. Before calling the token authority, it reloads auth storage and checks that the persisted account id still matches the cached account id. If the persisted access token changed, it skips authority refresh because another writer already refreshed or replaced the credentials.

Codex refreshes proactively when the access-token JWT expires within five minutes. If it cannot parse JWT expiration, it falls back to `last_refresh` older than eight days.

Tokenproxy should copy the behavior, not the implementation.

Source observations from the local Codex checkout:

| Claim | Source |
| --- | --- |
| Codex defines the refresh endpoint, override names, stale interval, and five-minute access-token window. | `/tmp/codex/codex-rs/login/src/auth/manager.rs:100-113` |
| Codex posts `{client_id, grant_type, refresh_token}` to the token authority. | `/tmp/codex/codex-rs/login/src/auth/manager.rs:1055-1076` |
| Codex classifies expired, reused, and invalidated refresh tokens as permanent failures. | `/tmp/codex/codex-rs/login/src/auth/manager.rs:1086-1127` |
| Codex uses guarded reload before authority refresh. | `/tmp/codex/codex-rs/login/src/auth/manager.rs:1985-2024` |
| Codex refreshes proactively from JWT expiration or `last_refresh`. | `/tmp/codex/codex-rs/login/src/auth/manager.rs:2128-2150` |

## Do not import the Codex login crate in stage two

Importing Codex auth code would make tokenproxy larger and more coupled than the refresh problem requires.

The local checkout does not expose `/tmp/codex/codex-rs/login/Cargo.toml` as a direct path dependency. A direct `cargo metadata --manifest-path /tmp/codex/codex-rs/login/Cargo.toml` probe failed because that manifest does not exist.

The auth manager also depends on Codex-specific storage, config, protocol, app-server, client, and identity code. Tokenproxy only needs five pieces:

1. the refresh endpoint;
2. the request body shape;
3. permanent error classification;
4. proactive refresh timing;
5. guarded reload semantics.

Those pieces fit in a small local module. A local implementation is easier to test with tokenproxy's account routing and response-commit boundaries.

## Proposed module boundary

Add a new internal auth module in stage two, for example `src/auth/chatgpt.rs`.

The module should own ChatGPT OAuth credential state and expose a narrow API to the proxy:

```rust
pub struct ChatGptCredentialManager { /* per-account state */ }

impl ChatGptCredentialManager {
    pub async fn bearer_for_new_request(&self, account_id: &str) -> Result<CredentialSnapshot, RefreshError>;
    pub async fn recover_after_unauthorized(&self, snapshot: &CredentialSnapshot) -> Result<RecoveryOutcome, RefreshError>;
    pub async fn mark_committed(&self, request_id: RequestId);
}
```

The exact names can change. The boundary should not change: the server asks for a credential snapshot before upstream work and asks once for recovery after a pre-commit 401. The server should not know refresh-token fields, storage formats, or token-authority JSON.

## Credential data model

Keep parsed credential data explicit.

`ChatGptCredential` should contain:

- tokenproxy account id;
- auth JSON source key;
- access token;
- refresh token;
- optional id token;
- optional ChatGPT account id;
- optional `last_refresh`;
- generation number;
- permanent refresh failure, if one was observed for the current generation.

`CredentialSnapshot` should contain:

- tokenproxy account id;
- bearer token;
- optional ChatGPT account id;
- generation number;
- an opaque source version if the storage backend can supply one.

The proxy should attach a snapshot to each upstream attempt. Recovery should only refresh or retry when the failed attempt used the current generation or an older generation. It should never overwrite a newer credential with an older one.

## Storage support

Stage two should support refresh for local file-backed auth JSON first.

Local file persistence should:

- read the auth JSON from the configured expanded path;
- write the updated JSON to a temporary file in the same directory;
- flush the file;
- rename the temporary file over the original path;
- then swap the in-memory credential.

If tokenproxy later supports refresh for S3 auth JSON locations, it must use conditional writes such as ETag or equivalent compare-and-swap. Without a conditional write, S3 refresh should remain disabled and read-only. Concurrent writers without compare-and-swap can lose a rotated refresh token.

## Refresh triggers

Tokenproxy should refresh in three cases.

First, refresh before admitting new upstream work when the access-token JWT expires within five minutes.

Second, if access-token expiration cannot be parsed, refresh before admitting new upstream work when `last_refresh` is older than eight days.

Third, recover after an upstream 401 only when tokenproxy has not committed anything downstream for that request.

Tokenproxy should not refresh on every request. It should not refresh after the first downstream SSE frame. It should not refresh inside an already established WebSocket session. It should not retry a request after any downstream response body has been emitted.

## Guarded reload algorithm

Every authority refresh should start with a guarded reload.

Algorithm:

1. Acquire the per-account refresh lock.
2. Capture the current credential generation, access token, and ChatGPT account id.
3. Reread the auth JSON source.
4. If the persisted ChatGPT account id exists and differs from the current ChatGPT account id, stop and return permanent account-mismatch failure.
5. If the persisted access token differs from the current access token, parse and install the persisted credential as a new generation. Do not call the token authority.
6. If the persisted access token matches the current access token, call the token authority with the current refresh token.
7. Persist returned tokens.
8. Reread or rebuild the credential from the persisted JSON.
9. Install the new generation.

This prevents duplicate refresh calls when another tokenproxy instance or an operator relogin already rotated the file.

## Refresh request

The authority request should be a POST to:

```text
https://auth.openai.com/oauth/token
```

The request body should be:

```json
{
  "client_id": "app_EMoamEEZ73f0CkXaXp7hrann",
  "grant_type": "refresh_token",
  "refresh_token": "<redacted>"
}
```

The stage-two implementation should keep the endpoint and client id in one small internal function. Tests may override the endpoint with a test-only hook. User-facing configuration for the refresh endpoint should not be added unless tokenproxy needs it for production operation.

## Refresh response handling

On success, tokenproxy should update only fields returned by the authority:

- update `access_token` when returned;
- update `refresh_token` when returned;
- update `id_token` when returned;
- preserve existing fields when the response omits them;
- set `last_refresh` to the current UTC time;
- preserve unrelated auth JSON fields.

Tokenproxy must persist before exposing the new access token to new requests. If memory is updated before persistence and the process exits, the next process can restart with the old access token and an already-rotated refresh token.

## Failure classification

Permanent refresh failure:

- HTTP 401 from the token authority;
- backend error code `refresh_token_expired`;
- backend error code `refresh_token_reused`;
- backend error code `refresh_token_invalidated`;
- guarded reload account mismatch;
- auth JSON lacks a refresh token for an account configured for OAuth refresh.

Permanent failure should mark the account `AuthFailed` and exclude it from routing until the operator replaces or relogs the auth JSON.

Transient refresh failure:

- network connect error;
- timeout;
- token authority 5xx;
- malformed success body;
- persistence failure;
- local file lock or rename failure.

Transient failure should not poison the account permanently. If the failed request has not committed downstream, tokenproxy can select another healthy account. If no account is available, tokenproxy should return a clear upstream-auth-recovery error without logging secrets.

## Retry and response-commit boundaries

HTTP JSON requests can retry once after pre-commit credential recovery because tokenproxy already has the request body.

SSE requests can retry once only before the first downstream SSE frame. After tokenproxy emits a frame, it must stop retrying and surface the upstream stream result.

WebSocket create or upgrade flows can recover only before tokenproxy reports an upstream-ready state to the downstream client. After the WebSocket is established, tokenproxy should treat credential expiry as a session failure, close the session with a clear error, and rely on the client to open a new session.

These rules protect clients from duplicated model output, duplicated tool calls, and ambiguous partial responses.

## Concurrency

Refresh should be single-flight per tokenproxy account.

Requests that arrive while refresh is in progress should wait for the same refresh result when they need the same credential generation. They should not issue parallel refresh requests with the same refresh token.

The implementation should not add a global refresh queue. Account-level locking is enough because account credentials are independent and route selection already chooses between accounts.

## Observability

Metrics should expose refresh behavior without secrets:

- refresh attempts by account id and trigger;
- refresh outcome by class: success, permanent, transient, skipped_changed_on_reload;
- refresh duration histogram;
- credential generation number as a counter, not as a token-derived value;
- route exclusions caused by `AuthFailed`;
- request retries caused by pre-commit refresh recovery.

Logs must never include:

- access tokens;
- refresh tokens;
- id tokens;
- full auth JSON;
- token authority request bodies;
- token authority response bodies.

For unknown authority errors, logs should include status code and a redacted error code when parseable.

## Security rules

Refresh tokens are bearer secrets. Treat them as more sensitive than access tokens because a refresh token can mint future access tokens.

Stage two should add tests or log-capture checks that prove token strings are not emitted through tracing, metrics labels, debug output, or error formatting.

Do not place tokens in route-selection state, request extensions visible to middleware, or metric labels. Keep them inside credential manager state and short-lived upstream request builders.

## Tiny experiments already run

These commands were run from the current checkout or from the throwaway probe directory on 2026-06-21.

```text
cargo test --lib auth_tests
result: 5 passed
meaning: current parser uses access tokens as upstream bearers, rejects refresh-token-only auth JSON, and validates last_refresh.
```

```text
cargo test --lib auth_failed
result: 1 passed
meaning: current route selection excludes AuthFailed accounts.
```

```text
cargo test --lib chatgpt
result: 23 passed
meaning: current ChatGPT account auth, routing, header, and related tests pass before refresh work.
```

```text
cargo metadata --format-version 1 --manifest-path /tmp/codex/codex-rs/login/Cargo.toml
result: failed, manifest path does not exist
meaning: importing Codex login as a direct path crate from this checkout is not a simple dependency change.
```

```text
cd /private/tmp/tokenproxy-oauth-refresh-probe
cargo run --quiet
result: tokenproxy-oauth-refresh-probe: all checks passed
meaning: a tiny Rust probe reproduced the refresh POST shape, permanent error classification, and guarded reload decisions without importing Codex.
```

These experiments support a narrow local implementation. They do not prove upstream latency, endpoint availability, or production refresh success against real credentials.

## Stage-two implementation plan

Step one: refactor config loading to retain credential source metadata for `ChatgptCodexAuthJson` accounts. Preserve current behavior and tests.

Step two: add `ChatGptCredentialManager` with local file read, parse, guarded reload, and persistence tests. Do not connect it to request forwarding yet.

Step three: add proactive refresh before new upstream work. Cover JWT-expiration parsing, `last_refresh` fallback, permanent failure, transient failure, and single-flight behavior.

Step four: add pre-commit 401 recovery to HTTP JSON and SSE paths. Cover retry-before-commit and no-retry-after-first-frame behavior.

Step five: add WebSocket pre-ready recovery only. Cover no mid-session refresh or retry.

Step six: add redaction and metrics tests.

Step seven: decide whether S3 refresh support is in scope. If the current S3 client cannot perform conditional writes, keep S3 auth JSON refresh disabled and document that S3 credentials require an external updater.

## Required stage-two tests

Add unit tests for:

- access-token, refresh-token, id-token, account-id, and `last_refresh` parsing;
- refresh-token-only rejection as upstream bearer;
- refresh request JSON body;
- permanent authority failure classification;
- transient authority failure classification;
- guarded reload account match, mismatch, changed token, and unchanged token branches;
- persistence preserves unrelated JSON fields;
- persistence updates only returned token fields;
- JWT expiration refresh window;
- `last_refresh` fallback;
- single-flight refresh under concurrent requests;
- no secret leakage through error display or tracing.

Add integration-style tests with a local fake authority server for:

- proactive refresh before request forwarding;
- 401 recovery before downstream commit;
- no retry after first SSE frame;
- no retry after WebSocket ready;
- account becomes `AuthFailed` after permanent refresh failure;
- another account is selected after transient refresh failure when available.

## Acceptance criteria for the implementation PR

The implementation PR should pass the current auth and ChatGPT tests plus the new refresh tests.

It should not use a refresh token as an upstream bearer.

It should not emit raw tokens in logs, metrics, or errors.

It should not retry after downstream commitment.

It should not issue parallel authority refresh requests for the same account generation.

It should not add a global routing layer, global queue, or broad Codex dependency.

It should leave S3 refresh read-only unless conditional-write behavior is implemented and tested.
