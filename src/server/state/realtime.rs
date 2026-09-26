use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes, to_bytes};
use axum::extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderValue, Request, StatusCode};
use axum::response::{IntoResponse, Response};
use futures_util::{SinkExt, StreamExt};
use reqwest::Url;
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame as UpstreamCloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{Error as WebSocketError, Message as UpstreamMessage};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};

use crate::auth::bearer_header_matches;
use crate::config::{AccountKind, EffectiveAccount};
use crate::error::{ErrorCode, TokenproxyError};
use crate::http::forward::{
    UpstreamAuth, build_upstream_headers, filter_downstream_response_headers,
};
use crate::observability::sha256_hex;
use crate::routing::AccountHealth;
use crate::time_parse::now_unix_ms;

use super::proxy::{
    ActiveWebSocketSessionGuard, account_selection_health, record_account_http_status,
    require_auth, response_body_with_limit,
};
use super::{AppState, apply_chatgpt_auth_snapshot};

const MAX_CALLS: usize = 1024;
const MAX_MESSAGE_BYTES: usize = 1024 * 1024;
const RECONNECT_WINDOW: Duration = Duration::from_secs(5 * 60);
const DEFAULT_WS_BASE: &str = "wss://api.openai.com/v1/live";
type UpstreamSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(super) struct Calls {
    entries: Mutex<HashMap<String, Call>>,
    capacity: Arc<Semaphore>,
}

struct Call {
    account: EffectiveAccount,
    caller: String,
    expires: Instant,
    connected: bool,
    ended: bool,
    _permit: OwnedSemaphorePermit,
}

impl Default for Calls {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            capacity: Arc::new(Semaphore::new(MAX_CALLS)),
        }
    }
}

impl Calls {
    fn prune(&self) {
        self.entries
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|_, call| call.connected || call.expires > Instant::now());
    }

    fn reserve(&self) -> Result<OwnedSemaphorePermit, TokenproxyError> {
        self.prune();
        self.capacity.clone().try_acquire_owned().map_err(|_| {
            failure(
                StatusCode::SERVICE_UNAVAILABLE,
                "realtime call capacity reached",
            )
        })
    }

    fn insert(&self, id: String, call: Call) -> Result<(), TokenproxyError> {
        use std::collections::hash_map::Entry;
        match self
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entry(id)
        {
            Entry::Vacant(entry) => {
                entry.insert(call);
                Ok(())
            }
            Entry::Occupied(_) => Err(failure(
                StatusCode::BAD_GATEWAY,
                "duplicate upstream call ID",
            )),
        }
    }

    fn claim(self: &Arc<Self>, id: String, caller: &str) -> Result<CallLease, TokenproxyError> {
        self.prune();
        let mut entries = self.entries.lock().unwrap_or_else(|e| e.into_inner());
        let call = entries
            .get_mut(&id)
            .filter(|call| call.caller == caller)
            .ok_or_else(|| failure(StatusCode::NOT_FOUND, "unknown or expired realtime call"))?;
        if call.connected {
            return Err(failure(
                StatusCode::CONFLICT,
                "realtime call already has a sideband",
            ));
        }
        call.connected = true;
        Ok(CallLease {
            calls: self.clone(),
            id,
            account: call.account.clone(),
        })
    }
}

struct CallLease {
    calls: Arc<Calls>,
    id: String,
    account: EffectiveAccount,
}

impl CallLease {
    fn forget(&self) {
        if let Some(call) = self
            .calls
            .entries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get_mut(&self.id)
        {
            call.ended = true;
        }
    }
}

impl Drop for CallLease {
    fn drop(&mut self) {
        let mut entries = self.calls.entries.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(call) = entries.get_mut(&self.id) {
            if call.ended {
                entries.remove(&self.id);
                return;
            }
            call.connected = false;
            call.expires = Instant::now() + RECONNECT_WINDOW;
        }
    }
}

#[derive(Deserialize)]
struct CallRequest {
    sdp: String,
    session: SessionRequest,
}

#[derive(Deserialize)]
struct SessionRequest {
    model: String,
}

pub(super) async fn create_call(
    State(state): State<AppState>,
    request: Request<Body>,
) -> Result<Response, TokenproxyError> {
    require_auth(&state, request.headers())?;
    state.metrics.increment_requests();
    let deadline = Duration::from_millis(state.effective().config.timeouts.request_header_ms);
    bounded(&state, deadline, create_call_inner(&state, request)).await
}

async fn create_call_inner(
    state: &AppState,
    request: Request<Body>,
) -> Result<Response, TokenproxyError> {
    let (parts, body) = request.into_parts();
    let caller = caller_key(state, &parts.headers)?;
    if !parts
        .headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(';')
                .next()
                .is_some_and(|v| v.trim().eq_ignore_ascii_case("application/json"))
        })
    {
        return Err(TokenproxyError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            ErrorCode::UnsupportedMediaType,
            "realtime call creation requires application/json",
        ));
    }
    let limit = state
        .effective()
        .config
        .server
        .max_body_bytes
        .min(MAX_MESSAGE_BYTES);
    let body = to_bytes(body, limit).await.map_err(|_| {
        TokenproxyError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            ErrorCode::BodyTooLarge,
            "realtime request body exceeds limit",
        )
    })?;
    let call: CallRequest = serde_json::from_slice(&body).map_err(|_| {
        TokenproxyError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidJson,
            "realtime call requires sdp and session.model",
        )
    })?;
    if call.sdp.trim().is_empty() || call.session.model.trim().is_empty() {
        return Err(TokenproxyError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidJson,
            "sdp and session.model cannot be empty",
        ));
    }
    let permit = state.realtime_calls.reserve()?;
    let selected = select_voice_account(state).await?;
    let mut account = voice_credentials(state, &selected, false).await?;
    let mut url = Url::parse(&account.config.base_url)
        .map_err(|_| failure(StatusCode::BAD_GATEWAY, "invalid realtime backend"))?;
    url.set_path(&format!(
        "{}/realtime/calls",
        url.path().trim_end_matches('/')
    ));
    url.set_query(parts.uri.query());
    let request_id = state.next_request_id();
    let mut retried = false;
    loop {
        let headers = voice_headers(&parts.headers, &url, &account, &request_id)?;
        let response = state
            .control_client
            .post(url.clone())
            .headers(headers)
            .body(body.clone())
            .send()
            .await
            .map_err(|_| failure(StatusCode::BAD_GATEWAY, "realtime call creation failed"))?;
        let status = response.status();
        let headers = response.headers().clone();
        let response_body = response_body_with_limit(response, limit, "realtime response").await?;
        if status == StatusCode::UNAUTHORIZED
            && !retried
            && has_refresh_credentials(state, &account)
        {
            account = voice_credentials(state, &account, true).await?;
            retried = true;
            continue;
        }
        if status != StatusCode::FORBIDDEN && current_account(state, &account).is_ok() {
            record_account_http_status(state, &account, status, &headers, None).await;
        }
        if status.is_success() {
            let id = headers
                .get("location")
                .and_then(|v| v.to_str().ok())
                .and_then(call_id_from_location)
                .ok_or_else(|| {
                    failure(
                        StatusCode::BAD_GATEWAY,
                        "realtime response has no valid call Location",
                    )
                })?;
            if std::str::from_utf8(&response_body)
                .ok()
                .is_none_or(|sdp| sdp.trim().is_empty())
            {
                return Err(failure(
                    StatusCode::BAD_GATEWAY,
                    "realtime response has no valid SDP answer",
                ));
            }
            current_account(state, &account)?;
            state.realtime_calls.insert(
                id,
                Call {
                    account,
                    caller,
                    expires: Instant::now() + RECONNECT_WINDOW,
                    connected: false,
                    ended: false,
                    _permit: permit,
                },
            )?;
        }
        return Ok(upstream_response(status, &headers, response_body));
    }
}

pub(super) async fn join_call(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
    upgrade: Result<WebSocketUpgrade, axum::extract::ws::rejection::WebSocketUpgradeRejection>,
) -> Result<Response, TokenproxyError> {
    require_auth(&state, &headers)?;
    let caller = caller_key(&state, &headers)?;
    let upgrade = upgrade.map_err(|_| {
        failure(
            StatusCode::BAD_REQUEST,
            "realtime sideband requires a WebSocket upgrade",
        )
    })?;
    let lease = state.realtime_calls.claim(id, &caller)?;
    state.metrics.increment_requests();
    let deadline = Duration::from_millis(state.effective().config.timeouts.websocket_connect_ms);
    let result = bounded(&state, deadline, connect_sideband(&state, &lease, &headers)).await?;
    let limit = state
        .effective()
        .config
        .server
        .max_body_bytes
        .min(MAX_MESSAGE_BYTES);
    match result {
        Ok(upstream) => Ok(upgrade
            .max_message_size(limit)
            .max_frame_size(limit)
            .on_upgrade(move |downstream| async move {
                let _lease = lease;
                relay(&state, downstream, upstream).await;
            })),
        Err(response) => Ok(response),
    }
}

async fn connect_sideband(
    state: &AppState,
    lease: &CallLease,
    inbound: &HeaderMap,
) -> Result<Result<UpstreamSocket, Response>, TokenproxyError> {
    let mut account = voice_credentials(state, &lease.account, false).await?;
    let mut url = Url::parse(
        account
            .config
            .realtime_ws_base_url
            .as_deref()
            .unwrap_or(DEFAULT_WS_BASE),
    )
    .map_err(|_| {
        failure(
            StatusCode::BAD_GATEWAY,
            "invalid realtime WebSocket backend",
        )
    })?;
    url.path_segments_mut()
        .map_err(|_| failure(StatusCode::BAD_GATEWAY, "invalid realtime WebSocket path"))?
        .pop_if_empty()
        .push(&lease.id);
    let request_id = state.next_request_id();
    let mut retried = false;
    loop {
        let mut request = url.as_str().into_client_request().map_err(|_| {
            failure(
                StatusCode::BAD_GATEWAY,
                "invalid realtime WebSocket request",
            )
        })?;
        request
            .headers_mut()
            .extend(voice_headers(inbound, &url, &account, &request_id)?);
        let limit = state
            .effective()
            .config
            .server
            .max_body_bytes
            .min(MAX_MESSAGE_BYTES);
        let config = WebSocketConfig::default()
            .max_message_size(Some(limit))
            .max_frame_size(Some(limit));
        match connect_async_with_config(request, Some(config), true).await {
            Ok((socket, _)) => {
                current_account(state, &account)?;
                return Ok(Ok(socket));
            }
            Err(WebSocketError::Http(response)) => {
                let status = response.status();
                if status == StatusCode::UNAUTHORIZED
                    && !retried
                    && has_refresh_credentials(state, &account)
                {
                    account = voice_credentials(state, &account, true).await?;
                    retried = true;
                    continue;
                }
                if matches!(status, StatusCode::NOT_FOUND | StatusCode::GONE) {
                    lease.forget();
                }
                if status != StatusCode::FORBIDDEN && current_account(state, &account).is_ok() {
                    record_account_http_status(state, &account, status, response.headers(), None)
                        .await;
                }
                let body = response
                    .body()
                    .as_ref()
                    .map(|body| Bytes::copy_from_slice(body))
                    .unwrap_or_default();
                return Ok(Err(upstream_response(status, response.headers(), body)));
            }
            Err(_) => {
                return Err(failure(
                    StatusCode::BAD_GATEWAY,
                    "realtime sideband connection failed",
                ));
            }
        }
    }
}

async fn relay(state: &AppState, downstream: WebSocket, upstream: UpstreamSocket) {
    let _active = ActiveWebSocketSessionGuard::new(&state.metrics);
    let deadline = Duration::from_millis(state.effective().config.timeouts.websocket_idle_ms);
    let (mut downstream_tx, mut downstream_rx) = downstream.split();
    let (mut upstream_tx, mut upstream_rx) = upstream.split();
    let mut shutdown = state.shutdown_receiver();
    if *shutdown.borrow() {
        return;
    }
    let to_upstream = async {
        while let Some(Ok(message)) = downstream_rx.next().await {
            let close = matches!(message, Message::Close(_));
            let message = match message {
                Message::Text(text) => UpstreamMessage::Text(text.as_str().into()),
                Message::Binary(bytes) => UpstreamMessage::Binary(bytes),
                Message::Ping(bytes) => UpstreamMessage::Ping(bytes),
                Message::Pong(bytes) => UpstreamMessage::Pong(bytes),
                Message::Close(frame) => {
                    UpstreamMessage::Close(frame.map(|frame| UpstreamCloseFrame {
                        code: frame.code.into(),
                        reason: frame.reason.as_str().into(),
                    }))
                }
            };
            if !matches!(
                tokio::time::timeout(deadline, upstream_tx.send(message)).await,
                Ok(Ok(()))
            ) || close
            {
                break;
            }
        }
    };
    let to_downstream = async {
        while let Some(Ok(message)) = upstream_rx.next().await {
            let close = matches!(message, UpstreamMessage::Close(_));
            let message = match message {
                UpstreamMessage::Text(text) => Message::Text(text.as_str().into()),
                UpstreamMessage::Binary(bytes) => Message::Binary(bytes),
                UpstreamMessage::Ping(bytes) => Message::Ping(bytes),
                UpstreamMessage::Pong(bytes) => Message::Pong(bytes),
                UpstreamMessage::Close(frame) => Message::Close(frame.map(|frame| CloseFrame {
                    code: frame.code.into(),
                    reason: frame.reason.as_str().into(),
                })),
                UpstreamMessage::Frame(_) => continue,
            };
            if !matches!(
                tokio::time::timeout(deadline, downstream_tx.send(message)).await,
                Ok(Ok(()))
            ) || close
            {
                break;
            }
        }
    };
    // Keep both directions polled so a stalled write cannot block incoming events.
    tokio::select! {
        _ = to_upstream => {},
        _ = to_downstream => {},
        _ = shutdown.changed() => {},
    }
    let _ = tokio::time::timeout(deadline.min(Duration::from_secs(1)), async {
        tokio::join!(downstream_tx.close(), upstream_tx.close())
    })
    .await;
}

async fn select_voice_account(state: &AppState) -> Result<EffectiveAccount, TokenproxyError> {
    let usage = state.usage_windows.lock().await;
    state
        .routing_accounts()
        .into_iter()
        .filter(|account| {
            account.config.enabled
                && account.config.supports_realtime
                && account.config.kind == AccountKind::ChatgptCodexAuthJson
        })
        .filter(|account| {
            match account_selection_health(
                state,
                account,
                usage.get(&account.config.id).map(Vec::as_slice),
            ) {
                AccountHealth::Open | AccountHealth::Unknown => true,
                AccountHealth::Throttled { next_retry_at_ms } => now_unix_ms() >= next_retry_at_ms,
                AccountHealth::UsageLimited { reset_at_ms } => now_unix_ms() >= reset_at_ms,
                AccountHealth::AuthFailed => false,
            }
        })
        .max_by_key(|account| account.config.priority)
        .ok_or_else(|| {
            TokenproxyError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorCode::NoEligibleAccount,
                "no eligible ChatGPT account with supports_realtime enabled",
            )
        })
}

fn same_identity(left: &EffectiveAccount, right: &EffectiveAccount) -> bool {
    left.config.id == right.config.id
        && left.config.kind == right.config.kind
        && left.config.base_url == right.config.base_url
        && left.config.realtime_ws_base_url == right.config.realtime_ws_base_url
        && left.config.auth_json_path == right.config.auth_json_path
        && left.chatgpt_account_id == right.chatgpt_account_id
        && left
            .chatgpt_account_id
            .as_ref()
            .is_some_and(|id| !id.is_empty())
}

fn current_account(
    state: &AppState,
    pinned: &EffectiveAccount,
) -> Result<EffectiveAccount, TokenproxyError> {
    state
        .routing_accounts()
        .into_iter()
        .find(|account| {
            account.config.enabled
                && account.config.supports_realtime
                && same_identity(account, pinned)
        })
        .ok_or_else(|| {
            failure(
                StatusCode::CONFLICT,
                "realtime call account was removed or its identity changed",
            )
        })
}

fn has_refresh_credentials(state: &AppState, account: &EffectiveAccount) -> bool {
    state
        .chatgpt_auth
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .contains_key(&account.config.id)
}

async fn voice_credentials(
    state: &AppState,
    pinned: &EffectiveAccount,
    recover: bool,
) -> Result<EffectiveAccount, TokenproxyError> {
    let current = current_account(state, pinned)?;
    let cell = state
        .chatgpt_auth
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&pinned.config.id)
        .cloned();
    let account = if let Some(cell) = cell {
        if cell.snapshot().account_id != pinned.chatgpt_account_id {
            return Err(failure(
                StatusCode::CONFLICT,
                "realtime credentials changed identity",
            ));
        }
        let snapshot = if recover {
            cell.recover_after_unauthorized(&state.control_client, &pinned.bearer_token)
                .await?
        } else {
            cell.snapshot_for_request(&state.control_client).await
        };
        apply_chatgpt_auth_snapshot(&current, snapshot)
    } else {
        current
    };
    if !same_identity(&account, pinned) {
        return Err(failure(
            StatusCode::CONFLICT,
            "realtime credentials changed identity",
        ));
    }
    current_account(state, pinned)?;
    Ok(account)
}

fn caller_key(state: &AppState, headers: &HeaderMap) -> Result<String, TokenproxyError> {
    let effective = state.effective();
    let bearer = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if bearer_header_matches(bearer, &effective.downstream_token)
        || headers.get("x-api-key").and_then(|v| v.to_str().ok())
            == Some(effective.downstream_token.as_str())
    {
        return Ok(sha256_hex(effective.downstream_token.as_bytes()));
    }
    let cells = state.chatgpt_auth.read().unwrap_or_else(|e| e.into_inner());
    for account in &effective.accounts {
        if account.config.kind == AccountKind::ChatgptCodexAuthJson
            && (bearer_header_matches(bearer, &account.bearer_token)
                || cells
                    .get(&account.config.id)
                    .is_some_and(|cell| cell.bearer_matches(bearer)))
        {
            return Ok(sha256_hex(
                format!(
                    "{}\0{}",
                    account.config.id,
                    account.chatgpt_account_id.as_deref().unwrap_or_default()
                )
                .as_bytes(),
            ));
        }
    }
    Err(TokenproxyError::new(
        StatusCode::UNAUTHORIZED,
        ErrorCode::Unauthorized,
        "missing or invalid downstream credential",
    ))
}

fn voice_headers(
    inbound: &HeaderMap,
    url: &Url,
    account: &EffectiveAccount,
    request_id: &str,
) -> Result<HeaderMap, TokenproxyError> {
    let host = match url.port() {
        Some(port) => format!("{}:{port}", url.host_str().unwrap_or_default()),
        None => url.host_str().unwrap_or_default().to_owned(),
    };
    let mut headers = build_upstream_headers(
        inbound,
        &host,
        &account.bearer_token,
        account.chatgpt_account_id.as_deref(),
        request_id,
        UpstreamAuth::ChatGptBearer,
        false,
    )?;
    if !inbound.contains_key("openai-beta") {
        headers.remove("openai-beta");
    }
    for name in ["openai-alpha", "x-session-id"] {
        if let Some(value) = inbound.get(name) {
            headers.insert(name, value.clone());
        }
    }
    headers
        .entry("openai-alpha")
        .or_insert(HeaderValue::from_static("quicksilver=v2"));
    Ok(headers)
}

fn call_id_from_location(location: &str) -> Option<String> {
    location
        .split('?')
        .next()?
        .split('/')
        .rev()
        .find(|id| {
            let safe = id.len() <= 256
                && id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
            safe && ((id.starts_with("rtc_") && id.len() > 4)
                || uuid::Uuid::parse_str(id).is_ok_and(|_| id.len() == 36))
        })
        .map(str::to_owned)
}

fn upstream_response(status: StatusCode, headers: &HeaderMap, body: Bytes) -> Response {
    let mut output = filter_downstream_response_headers(headers);
    if let Some(location) = headers.get("location") {
        output.insert("location", location.clone());
    }
    (status, output, body).into_response()
}

async fn bounded<T>(
    state: &AppState,
    deadline: Duration,
    future: impl Future<Output = Result<T, TokenproxyError>>,
) -> Result<T, TokenproxyError> {
    let mut shutdown = state.shutdown_receiver();
    if *shutdown.borrow() {
        return Err(failure(
            StatusCode::SERVICE_UNAVAILABLE,
            "server is shutting down",
        ));
    }
    tokio::select! {
        result = tokio::time::timeout(deadline, future) => result.unwrap_or_else(|_| Err(failure(StatusCode::GATEWAY_TIMEOUT, "realtime upstream timed out"))),
        _ = shutdown.changed() => Err(failure(StatusCode::SERVICE_UNAVAILABLE, "server is shutting down")),
    }
}

fn failure(status: StatusCode, message: &str) -> TokenproxyError {
    TokenproxyError::new(status, ErrorCode::UpstreamFailure, message)
}

#[cfg(test)]
mod tests;
