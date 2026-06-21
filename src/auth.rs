use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::RwLock;

use axum::http::StatusCode;
use chrono::{DateTime, TimeDelta, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::config::{AccountKind, EffectiveConfig};
use crate::error::{ErrorCode, TokenproxyError};
use crate::time_parse::{format_rfc3339, parse_rfc3339};

const TOKEN_REFRESH_INTERVAL_DAYS: i64 = 8;
const ACCESS_TOKEN_REFRESH_WINDOW_MINUTES: i64 = 5;
const REFRESH_TOKEN_URL: &str = "https://auth.openai.com/oauth/token";
const REFRESH_CLIENT_ID: &str = "app_EMoamEEZ73f0CkXaXp7hrann";
const CODEX_REFRESH_TOKEN_URL_OVERRIDE_ENV_VAR: &str = "CODEX_REFRESH_TOKEN_URL_OVERRIDE";
const CODEX_CLIENT_ID_OVERRIDE_ENV_VAR: &str = "CODEX_APP_SERVER_LOGIN_CLIENT_ID";

/*
Tokenproxy intentionally mirrors the OpenAI Codex OAuth refresh boundary instead
of inventing a token lifecycle. The source of truth reviewed for this file was
openai/codex commit 6f5dd7b4226f3c77d4d253d8be1e10ac1686ccf9, not main:

- Codex keeps the same default ChatGPT OAuth client id used below and exposes
  the same CODEX_APP_SERVER_LOGIN_CLIENT_ID override:
  https://github.com/openai/codex/blob/6f5dd7b4226f3c77d4d253d8be1e10ac1686ccf9/codex-rs/login/src/auth/manager.rs#L1329-L1355
- Codex treats access-token expiry as a five-minute refresh window and also has
  an eight-day refresh interval fallback:
  https://github.com/openai/codex/blob/6f5dd7b4226f3c77d4d253d8be1e10ac1686ccf9/codex-rs/login/src/auth/manager.rs#L117-L118
- Codex does not refresh on every request. For unauthorized responses it first
  reloads auth.json, then refreshes the token only if the same account is still
  active:
  https://github.com/openai/codex/blob/6f5dd7b4226f3c77d4d253d8be1e10ac1686ccf9/codex-rs/login/src/auth/manager.rs#L1488-L1506
  https://github.com/openai/codex/blob/6f5dd7b4226f3c77d4d253d8be1e10ac1686ccf9/codex-rs/login/src/auth/manager.rs#L1985-L2015
- Codex records permanent refresh-token failures only if the auth snapshot has
  not changed under it, avoiding an old failed refresh poisoning a freshly
  replaced auth.json:
  https://github.com/openai/codex/blob/6f5dd7b4226f3c77d4d253d8be1e10ac1686ccf9/codex-rs/login/src/auth/manager.rs#L2048-L2064
- Codex HTTP Responses traffic builds an UnauthorizedRecovery value and retries
  after a 401:
  https://github.com/openai/codex/blob/6f5dd7b4226f3c77d4d253d8be1e10ac1686ccf9/codex-rs/core/src/client.rs#L1281-L1359
- Codex WebSocket Responses traffic uses the same recovery boundary for a 401
  handshake before sending the response.create payload:
  https://github.com/openai/codex/blob/6f5dd7b4226f3c77d4d253d8be1e10ac1686ccf9/codex-rs/core/src/client.rs#L1404-L1465
- Codex WebSocket auth is header-only at connect time, and HTTP handshake
  failures are surfaced as transport HTTP errors:
  https://github.com/openai/codex/blob/6f5dd7b4226f3c77d4d253d8be1e10ac1686ccf9/codex-api/src/auth.rs#L26-L68
  https://github.com/openai/codex/blob/6f5dd7b4226f3c77d4d253d8be1e10ac1686ccf9/codex-api/src/endpoint/responses_websocket.rs#L322-L360
  https://github.com/openai/codex/blob/6f5dd7b4226f3c77d4d253d8be1e10ac1686ccf9/codex-api/src/endpoint/responses_websocket.rs#L516-L538

Tokenproxy follows the transport constraints implemented below: load the current
auth.json token into HTTP and WebSocket headers, recover only after an upstream
401, reload before refreshing, retry once with the same account, and let the
existing routing layer handle any remaining pre-commit failover.
*/

#[derive(Clone, Debug)]
pub struct ChatGptAuthSnapshot {
    pub bearer_token: String,
    pub account_id: Option<String>,
}

#[derive(Debug)]
pub struct ChatGptAuthCell {
    path: PathBuf,
    state: RwLock<ChatGptAuthState>,
    refresh_lock: Mutex<()>,
}

#[derive(Clone, Debug)]
struct ChatGptAuthState {
    raw: Value,
    access_token: String,
    refresh_token: String,
    account_id: Option<String>,
    last_refresh: Option<DateTime<Utc>>,
    layout: AuthJsonLayout,
    permanent_failure: Option<PermanentRefreshFailure>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AuthJsonLayout {
    Tokens,
    TopLevel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PermanentRefreshFailure {
    Expired,
    Reused,
    Invalidated,
    Unauthorized,
    AccountMismatch,
    MissingRefreshToken,
}

#[derive(Debug)]
enum RefreshError {
    Permanent(PermanentRefreshFailure),
    Transient(String),
}

#[derive(Serialize)]
struct RefreshRequest<'a> {
    client_id: String,
    grant_type: &'static str,
    refresh_token: &'a str,
}

#[derive(Deserialize)]
struct RefreshResponse {
    id_token: Option<String>,
    access_token: Option<String>,
    refresh_token: Option<String>,
}

pub fn chatgpt_auth_cells(
    effective: &EffectiveConfig,
) -> Result<BTreeMap<String, Arc<ChatGptAuthCell>>, TokenproxyError> {
    let mut cells = BTreeMap::new();
    for account in &effective.accounts {
        if !matches!(account.config.kind, AccountKind::ChatgptCodexAuthJson) {
            continue;
        }
        let Some(path) = account.config.auth_json_path.as_ref() else {
            continue;
        };
        if path.to_string_lossy().starts_with("s3://") {
            continue;
        }
        if let Some(cell) = ChatGptAuthCell::from_path(path)? {
            cells.insert(account.config.id.clone(), Arc::new(cell));
        }
    }
    Ok(cells)
}

impl ChatGptAuthCell {
    fn from_path(path: &Path) -> Result<Option<Self>, TokenproxyError> {
        let raw = std::fs::read_to_string(path).map_err(|error| {
            TokenproxyError::invalid_config(format!(
                "failed to read refreshable auth_json_path {}: {error}",
                path.display()
            ))
        })?;
        let state = parse_refreshable_auth_json(&raw)?;
        if state.refresh_token.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self {
            path: path.to_path_buf(),
            state: RwLock::new(state),
            refresh_lock: Mutex::new(()),
        }))
    }

    #[cfg(test)]
    fn from_path_for_testing(path: PathBuf) -> Self {
        let raw = std::fs::read_to_string(&path).expect("test auth json reads");
        Self {
            path,
            state: RwLock::new(parse_refreshable_auth_json(&raw).expect("test auth json parses")),
            refresh_lock: Mutex::new(()),
        }
    }

    pub fn snapshot(&self) -> ChatGptAuthSnapshot {
        let state = self.state.read().expect("chatgpt auth state lock poisoned");
        ChatGptAuthSnapshot {
            bearer_token: state.access_token.clone(),
            account_id: state.account_id.clone(),
        }
    }

    pub fn bearer_matches(&self, actual: &str) -> bool {
        let state = self.state.read().expect("chatgpt auth state lock poisoned");
        bearer_header_matches(actual, &state.access_token)
    }

    pub async fn snapshot_for_request(&self, client: &reqwest::Client) -> ChatGptAuthSnapshot {
        if self.should_refresh_proactively(Utc::now()) {
            let _ = self.refresh_token(client).await;
        }
        self.snapshot()
    }

    pub async fn recover_after_unauthorized(
        &self,
        client: &reqwest::Client,
        attempted_bearer_token: &str,
    ) -> Result<ChatGptAuthSnapshot, TokenproxyError> {
        {
            let state = self.state.read().expect("chatgpt auth state lock poisoned");
            if state.access_token != attempted_bearer_token {
                return Ok(ChatGptAuthSnapshot {
                    bearer_token: state.access_token.clone(),
                    account_id: state.account_id.clone(),
                });
            }
        }
        self.refresh_token(client).await.map_err(refresh_error)?;
        Ok(self.snapshot())
    }

    fn should_refresh_proactively(&self, now: DateTime<Utc>) -> bool {
        let state = self.state.read().expect("chatgpt auth state lock poisoned");
        should_refresh_proactively(&state, now)
    }

    async fn refresh_token(&self, client: &reqwest::Client) -> Result<(), RefreshError> {
        let _guard = self.refresh_lock.lock().await;
        let current = {
            let state = self.state.read().expect("chatgpt auth state lock poisoned");
            if let Some(error) = state.permanent_failure {
                return Err(RefreshError::Permanent(error));
            }
            state.clone()
        };
        let reloaded = read_auth_state(&self.path).await?;
        if reloaded.account_id.as_deref() != current.account_id.as_deref() {
            record_permanent_failure(&self.state, PermanentRefreshFailure::AccountMismatch);
            return Err(RefreshError::Permanent(
                PermanentRefreshFailure::AccountMismatch,
            ));
        }
        if reloaded.access_token != current.access_token || reloaded.raw != current.raw {
            *self
                .state
                .write()
                .expect("chatgpt auth state lock poisoned") = reloaded;
            return Ok(());
        }

        let response = match request_chatgpt_token_refresh(client, &current.refresh_token).await {
            Ok(response) => response,
            Err(RefreshError::Permanent(failure)) => {
                record_permanent_failure(&self.state, failure);
                return Err(RefreshError::Permanent(failure));
            }
            Err(error) => return Err(error),
        };
        let updated = apply_refresh_response(current, response, Utc::now())?;
        persist_auth_json(&self.path, &updated.raw).await?;
        *self
            .state
            .write()
            .expect("chatgpt auth state lock poisoned") = updated;
        Ok(())
    }
}

fn should_refresh_proactively(state: &ChatGptAuthState, now: DateTime<Utc>) -> bool {
    if let Some(expires_at) = jwt_expiration(&state.access_token) {
        return expires_at <= now + TimeDelta::minutes(ACCESS_TOKEN_REFRESH_WINDOW_MINUTES);
    }
    state.last_refresh.is_some_and(|last_refresh| {
        last_refresh < now - TimeDelta::days(TOKEN_REFRESH_INTERVAL_DAYS)
    })
}

async fn read_auth_state(path: &Path) -> Result<ChatGptAuthState, RefreshError> {
    let raw = tokio::fs::read_to_string(path)
        .await
        .map_err(|error| RefreshError::Transient(format!("failed to read auth JSON: {error}")))?;
    parse_refreshable_auth_json(&raw).map_err(|error| RefreshError::Transient(error.message))
}

fn parse_refreshable_auth_json(input: &str) -> Result<ChatGptAuthState, TokenproxyError> {
    let raw: Value = serde_json::from_str(input).map_err(|error| {
        TokenproxyError::invalid_config(format!("auth_json_path contains invalid JSON: {error}"))
    })?;
    parse_refreshable_auth_value(raw)
}

fn parse_refreshable_auth_value(raw: Value) -> Result<ChatGptAuthState, TokenproxyError> {
    let tokens = raw.get("tokens").unwrap_or(&Value::Null);
    let access_token = string_field(tokens, "access_token")
        .or_else(|| string_field(&raw, "access_token"))
        .or_else(|| string_field(&raw, "OPENAI_API_KEY"))
        .or_else(|| string_field(tokens, "id_token"))
        .or_else(|| string_field(&raw, "id_token"))
        .ok_or_else(|| {
            TokenproxyError::invalid_config("auth_json_path lacks ChatGPT token data")
        })?;
    let refresh_token = string_field(tokens, "refresh_token")
        .or_else(|| string_field(&raw, "refresh_token"))
        .unwrap_or_default();
    let last_refresh = string_field(&raw, "last_refresh")
        .map(|last_refresh| {
            parse_rfc3339(&last_refresh)
                .map(|value| value.to_utc())
                .ok_or_else(|| {
                    TokenproxyError::invalid_config(
                        "auth_json_path field last_refresh must be RFC3339",
                    )
                })
        })
        .transpose()?;
    let layout = if tokens.is_object() {
        AuthJsonLayout::Tokens
    } else {
        AuthJsonLayout::TopLevel
    };
    let account_id =
        string_field(tokens, "account_id").or_else(|| string_field(&raw, "account_id"));
    Ok(ChatGptAuthState {
        raw,
        access_token,
        refresh_token,
        account_id,
        last_refresh,
        layout,
        permanent_failure: None,
    })
}

fn apply_refresh_response(
    mut state: ChatGptAuthState,
    response: RefreshResponse,
    now: DateTime<Utc>,
) -> Result<ChatGptAuthState, RefreshError> {
    if let Some(access_token) = response.access_token {
        set_auth_json_string(&mut state.raw, state.layout, "access_token", &access_token);
    }
    if let Some(refresh_token) = response.refresh_token {
        set_auth_json_string(
            &mut state.raw,
            state.layout,
            "refresh_token",
            &refresh_token,
        );
    }
    if let Some(id_token) = response.id_token {
        set_auth_json_string(&mut state.raw, state.layout, "id_token", &id_token);
    }
    state.raw["last_refresh"] = Value::String(format_rfc3339(now).ok_or_else(|| {
        RefreshError::Transient("failed to format refresh timestamp".to_string())
    })?);
    parse_refreshable_auth_value(state.raw).map_err(|error| RefreshError::Transient(error.message))
}

async fn request_chatgpt_token_refresh(
    client: &reqwest::Client,
    refresh_token: &str,
) -> Result<RefreshResponse, RefreshError> {
    if refresh_token.is_empty() {
        return Err(RefreshError::Permanent(
            PermanentRefreshFailure::MissingRefreshToken,
        ));
    }
    let request = RefreshRequest {
        client_id: oauth_client_id(),
        grant_type: "refresh_token",
        refresh_token,
    };
    let response = client
        .post(refresh_token_endpoint())
        .json(&request)
        .send()
        .await
        .map_err(|error| {
            RefreshError::Transient(format!("token refresh request failed: {error}"))
        })?;
    let status = response.status();
    if status.is_success() {
        return response.json::<RefreshResponse>().await.map_err(|error| {
            RefreshError::Transient(format!("token refresh JSON failed: {error}"))
        });
    }
    let body = response.text().await.unwrap_or_default();
    let permanent = classify_refresh_token_failure(&body);
    if status == StatusCode::UNAUTHORIZED {
        return Err(RefreshError::Permanent(
            permanent.unwrap_or(PermanentRefreshFailure::Unauthorized),
        ));
    }
    if let Some(permanent) = permanent {
        return Err(RefreshError::Permanent(permanent));
    }
    Err(RefreshError::Transient(format!(
        "token refresh failed with HTTP {status}"
    )))
}

fn oauth_client_id() -> String {
    std::env::var(CODEX_CLIENT_ID_OVERRIDE_ENV_VAR)
        .ok()
        .filter(|client_id| !client_id.trim().is_empty())
        .unwrap_or_else(|| REFRESH_CLIENT_ID.to_string())
}

fn refresh_token_endpoint() -> String {
    #[cfg(test)]
    {
        let url = TEST_REFRESH_ENDPOINT_VALUE
            .lock()
            .expect("test refresh endpoint override lock is not poisoned")
            .clone();
        if let Some(url) = url {
            return url;
        }
    }

    std::env::var(CODEX_REFRESH_TOKEN_URL_OVERRIDE_ENV_VAR)
        .unwrap_or_else(|_| REFRESH_TOKEN_URL.to_string())
}

fn classify_refresh_token_failure(body: &str) -> Option<PermanentRefreshFailure> {
    let Value::Object(map) = serde_json::from_str::<Value>(body).ok()? else {
        return None;
    };
    let code = match map.get("error") {
        Some(Value::Object(error)) => error.get("code").and_then(Value::as_str),
        Some(Value::String(code)) => Some(code.as_str()),
        _ => map.get("code").and_then(Value::as_str),
    }?;
    match code {
        code if code.eq_ignore_ascii_case("refresh_token_expired") => {
            Some(PermanentRefreshFailure::Expired)
        }
        code if code.eq_ignore_ascii_case("refresh_token_reused") => {
            Some(PermanentRefreshFailure::Reused)
        }
        code if code.eq_ignore_ascii_case("refresh_token_invalidated") => {
            Some(PermanentRefreshFailure::Invalidated)
        }
        _ => None,
    }
}

async fn persist_auth_json(path: &Path, value: &Value) -> Result<(), RefreshError> {
    let parent = path
        .parent()
        .ok_or_else(|| RefreshError::Transient("auth JSON path lacks parent".to_string()))?;
    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| RefreshError::Transient("auth JSON path lacks file name".to_string()))?;
    let tmp = parent.join(format!(
        ".{file_name}.tokenproxy-refresh-{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .await
        .map_err(|error| {
            RefreshError::Transient(format!("failed to create auth JSON temp file: {error}"))
        })?;
    let mut body = serde_json::to_vec(value).map_err(|error| {
        RefreshError::Transient(format!("failed to serialize auth JSON: {error}"))
    })?;
    body.push(b'\n');
    file.write_all(&body)
        .await
        .map_err(|error| RefreshError::Transient(format!("failed to write auth JSON: {error}")))?;
    file.sync_data()
        .await
        .map_err(|error| RefreshError::Transient(format!("failed to sync auth JSON: {error}")))?;
    drop(file);
    tokio::fs::rename(&tmp, path).await.map_err(|error| {
        RefreshError::Transient(format!("failed to replace auth JSON: {error}"))
    })?;
    Ok(())
}

fn refresh_error(error: RefreshError) -> TokenproxyError {
    let message = match error {
        RefreshError::Permanent(reason) => {
            format!(
                "ChatGPT OAuth refresh failed permanently: {}",
                reason.as_str()
            )
        }
        RefreshError::Transient(message) => format!("ChatGPT OAuth refresh failed: {message}"),
    };
    TokenproxyError::new(StatusCode::BAD_GATEWAY, ErrorCode::UpstreamFailure, message)
}

fn record_permanent_failure(state: &RwLock<ChatGptAuthState>, failure: PermanentRefreshFailure) {
    if let Ok(mut state) = state.write() {
        state.permanent_failure = Some(failure);
    }
}

impl PermanentRefreshFailure {
    const fn as_str(&self) -> &'static str {
        match self {
            Self::Expired => "refresh_token_expired",
            Self::Reused => "refresh_token_reused",
            Self::Invalidated => "refresh_token_invalidated",
            Self::Unauthorized => "unauthorized",
            Self::AccountMismatch => "account_mismatch",
            Self::MissingRefreshToken => "missing_refresh_token",
        }
    }
}

#[cfg(test)]
static TEST_REFRESH_ENDPOINT_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
static TEST_REFRESH_ENDPOINT_VALUE: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

#[cfg(test)]
pub(crate) struct RefreshEndpointOverrideGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

#[cfg(test)]
impl Drop for RefreshEndpointOverrideGuard {
    fn drop(&mut self) {
        *TEST_REFRESH_ENDPOINT_VALUE
            .lock()
            .expect("test refresh endpoint override lock is not poisoned") = None;
    }
}

#[cfg(test)]
pub(crate) fn use_refresh_endpoint_for_testing(url: String) -> RefreshEndpointOverrideGuard {
    let lock = TEST_REFRESH_ENDPOINT_LOCK
        .lock()
        .expect("test refresh endpoint serial lock is not poisoned");
    *TEST_REFRESH_ENDPOINT_VALUE
        .lock()
        .expect("test refresh endpoint override lock is not poisoned") = Some(url);
    RefreshEndpointOverrideGuard { _lock: lock }
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
}

fn set_auth_json_string(raw: &mut Value, layout: AuthJsonLayout, field: &str, value: &str) {
    match layout {
        AuthJsonLayout::Tokens => {
            if !raw.get("tokens").is_some_and(Value::is_object) {
                raw["tokens"] = Value::Object(serde_json::Map::new());
            }
            raw["tokens"][field] = Value::String(value.to_owned());
        }
        AuthJsonLayout::TopLevel => {
            raw[field] = Value::String(value.to_owned());
        }
    }
}

fn jwt_expiration(token: &str) -> Option<DateTime<Utc>> {
    let payload = token.split('.').nth(1)?;
    let decoded = base64_url_decode(payload)?;
    let value: Value = serde_json::from_slice(&decoded).ok()?;
    let exp = value.get("exp")?.as_i64()?;
    DateTime::from_timestamp(exp, 0)
}

fn base64_url_decode(input: &str) -> Option<Vec<u8>> {
    let mut bits = 0u32;
    let mut bit_count = 0u8;
    let mut output = Vec::with_capacity(input.len() * 3 / 4);
    for byte in input.bytes() {
        let value = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            b'=' => break,
            _ => return None,
        };
        bits = (bits << 6) | u32::from(value);
        bit_count += 6;
        if bit_count >= 8 {
            bit_count -= 8;
            output.push(((bits >> bit_count) & 0xff) as u8);
        }
    }
    Some(output)
}

pub(crate) fn bearer_header_matches(actual: &str, bearer_token: &str) -> bool {
    const PREFIX: &[u8] = b"Bearer ";
    let actual = actual.as_bytes();
    let token = bearer_token.as_bytes();
    let mut diff = actual.len() ^ (PREFIX.len() + token.len());
    for (index, expected) in PREFIX.iter().chain(token).copied().enumerate() {
        diff |= usize::from(actual.get(index).copied().unwrap_or(0) ^ expected);
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    #[test]
    fn should_parse_jwt_expiration_from_access_token() {
        let expiration = jwt_expiration("x.eyJleHAiOjE3OTAwMDAwMDB9.y").unwrap();

        assert_eq!(expiration.timestamp(), 1_790_000_000);
    }

    #[test]
    fn should_refresh_from_jwt_expiration_before_window() {
        let state = parse_refreshable_auth_json(
            r#"{"tokens":{"access_token":"x.eyJleHAiOjQwMH0.y","refresh_token":"refresh"}}"#,
        )
        .unwrap();
        let now = DateTime::from_timestamp(100, 0).unwrap();

        assert!(should_refresh_proactively(&state, now));
    }

    #[test]
    fn should_keep_fresh_jwt_until_refresh_window() {
        let state = parse_refreshable_auth_json(
            r#"{"tokens":{"access_token":"x.eyJleHAiOjQwMX0.y","refresh_token":"refresh"}}"#,
        )
        .unwrap();
        let now = DateTime::from_timestamp(100, 0).unwrap();

        assert!(!should_refresh_proactively(&state, now));
    }

    #[test]
    fn should_fall_back_to_last_refresh_when_jwt_expiration_is_unavailable() {
        let state = parse_refreshable_auth_json(
            r#"{"last_refresh":"2026-06-01T00:00:00Z","tokens":{"access_token":"opaque","refresh_token":"refresh"}}"#,
        )
        .unwrap();
        let now = DateTime::parse_from_rfc3339("2026-06-09T00:00:01Z")
            .unwrap()
            .to_utc();

        assert!(should_refresh_proactively(&state, now));
    }

    #[test]
    fn should_preserve_nested_auth_json_fields_after_refresh_response() {
        let state = parse_refreshable_auth_json(
            r#"{"auth_mode":"chatgpt","last_refresh":"2026-06-01T00:00:00Z","tokens":{"access_token":"old-access","refresh_token":"old-refresh","id_token":"old-id","account_id":"acct"},"extra":true}"#,
        )
        .unwrap();
        let updated = apply_refresh_response(
            state,
            RefreshResponse {
                access_token: Some("new-access".to_string()),
                refresh_token: None,
                id_token: None,
            },
            DateTime::parse_from_rfc3339("2026-06-21T13:55:00Z")
                .unwrap()
                .to_utc(),
        )
        .unwrap();

        assert_eq!(updated.raw["tokens"]["access_token"], "new-access");
        assert_eq!(updated.raw["tokens"]["refresh_token"], "old-refresh");
        assert_eq!(updated.raw["extra"], true);
    }

    #[test]
    fn should_classify_codex_refresh_token_failures() {
        assert_eq!(
            classify_refresh_token_failure(r#"{"error":{"code":"refresh_token_reused"}}"#),
            Some(PermanentRefreshFailure::Reused)
        );
    }

    #[tokio::test]
    async fn should_guard_reload_changed_auth_without_calling_authority() {
        let dir = std::env::temp_dir().join(format!(
            "tokenproxy-auth-reload-{}",
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("auth.json");
        std::fs::write(
            &path,
            r#"{"tokens":{"access_token":"old-access","refresh_token":"old-refresh","account_id":"acct"}}"#,
        )
        .unwrap();
        let cell = ChatGptAuthCell::from_path_for_testing(path.clone());
        std::fs::write(
            &path,
            r#"{"tokens":{"access_token":"new-access","refresh_token":"new-refresh","account_id":"acct"}}"#,
        )
        .unwrap();
        let client = reqwest::Client::new();

        cell.recover_after_unauthorized(&client, "old-access")
            .await
            .unwrap();

        assert_eq!(cell.snapshot().bearer_token, "new-access");
    }

    #[tokio::test]
    async fn should_refresh_and_persist_tokens_from_authority() {
        let authority = fake_refresh_authority().await;
        let _refresh_endpoint =
            use_refresh_endpoint_for_testing(format!("http://{authority}/oauth/token"));
        let dir = std::env::temp_dir().join(format!(
            "tokenproxy-auth-refresh-{}",
            Utc::now().timestamp_nanos_opt().unwrap()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("auth.json");
        std::fs::write(
            &path,
            r#"{"tokens":{"access_token":"old-access","refresh_token":"old-refresh","account_id":"acct"}}"#,
        )
        .unwrap();
        let cell = ChatGptAuthCell::from_path_for_testing(path.clone());
        let client = reqwest::Client::new();

        cell.recover_after_unauthorized(&client, "old-access")
            .await
            .unwrap();

        let saved = std::fs::read_to_string(&path).unwrap();
        let saved: Value = serde_json::from_str(&saved).unwrap();
        assert_eq!(saved["tokens"]["access_token"], "new-access");
    }

    async fn fake_refresh_authority() -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buffer = [0; 4096];
                    let Ok(size) = stream.read(&mut buffer).await else {
                        return;
                    };
                    let request = String::from_utf8_lossy(&buffer[..size]);
                    if !request.contains(r#""grant_type":"refresh_token""#)
                        || !request.contains(r#""refresh_token":"old-refresh""#)
                    {
                        return;
                    }
                    let body = br#"{"access_token":"new-access","refresh_token":"new-refresh"}"#;
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n",
                        body.len()
                    );
                    let _ = stream.write_all(response.as_bytes()).await;
                    let _ = stream.write_all(body).await;
                });
            }
        });
        address
    }
}
