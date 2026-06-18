use std::fmt::Display;
use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use std::{collections::VecDeque, error::Error, pin::Pin};

use axum::body::{Body, Bytes, to_bytes};
use axum::extract::State;
use axum::extract::ws::{
    CloseFrame as DownstreamCloseFrame, Message as DownstreamMessage, WebSocket, WebSocketUpgrade,
    rejection::WebSocketUpgradeRejection,
};
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use eventsource_stream::{EventStream, Eventsource};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use reqwest::Url;
use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::Message as UpstreamMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::config::RetryConfig;
use crate::config::{AccountKind, EffectiveAccount};
use crate::error::{ErrorCode, TokenproxyError};
use crate::http::classify::{RequestShape, classify_request};
use crate::http::forward::{
    UpstreamAuth, build_upstream_headers, filter_downstream_response_headers,
};

use crate::http::sse_repair::SseRepair;
use crate::logging::{RequestLog, RouteSelectionLog};
use crate::metrics::{Metrics, prometheus_text_with_usage};
use crate::observability::{compact_body_hash_record, request_body_dump_record, sha256_hex};
use crate::responses::replay::{
    ReplayPlan, is_compacted_request_window, is_previous_response_not_found_event,
    normalize_websocket_create, plan_next_request, previous_response_not_found_retry_payload,
};
use crate::responses::state::ReplayState;
use crate::responses::websocket::{WebSocketAction, classify_downstream_message};
use crate::routing::account::normalize_service_tier;
use crate::routing::model_family_label;
use crate::routing::{
    AccountConfig as RoutingAccountConfig, AccountHealth, AccountState, Endpoint, RouteRequest,
    Transport, account_static_compatible, select_account,
};
use crate::time_parse::{
    now_rfc3339, now_timestamp_pair, now_unix_ms,
    retry_after_deadline_ms as parse_retry_after_deadline_ms,
};
use crate::usage::{
    UsageWindow, account_id_hash, usage_health_from_windows, usage_snapshot,
    usage_windows_from_error_body, usage_windows_from_headers,
    usage_windows_from_usage_limit_error_value,
};

use super::state::AppState;

type UpstreamWebSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type DownstreamWebSocketSink = SplitSink<WebSocket, DownstreamMessage>;
type DownstreamWebSocketStream = SplitStream<WebSocket>;
const UPSTREAM_WS_MAX_SESSION_AGE: Duration = Duration::from_secs(60 * 60);
const WEBSOCKET_SESSION_EVENT_BUFFER: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpProxyAttempt {
    request_id: String,
    method: Method,
    path: String,
    inbound_headers: HeaderMap,
    body: Bytes,
    request_shape: Option<RequestShape>,
    compact_request_body: Option<Bytes>,
}

struct UpstreamForward<'a> {
    request_id: &'a str,
    method: Method,
    path: &'a str,
    inbound_headers: HeaderMap,
    body: Bytes,
    model_family: &'a str,
    retry_phase: &'a str,
    compact_request_body: Option<Bytes>,
}

struct SseFirstEvent<'a, S> {
    status: StatusCode,
    headers: HeaderMap,
    stream: S,
    metrics: &'a Metrics,
    endpoint: &'a str,
    model_family: &'a str,
    account_id_hash: &'a str,
    started: Instant,
    idle_timeout: Duration,
}

struct UpstreamSession {
    account_id: String,
    opened_at: Instant,
    socket: UpstreamWebSocket,
}

#[derive(Debug)]
enum DownstreamSessionEvent {
    Message(DownstreamMessage),
    ReceiveError(String),
    Closed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WebSocketRouteContext {
    model: String,
    service_tier: Option<String>,
    model_family: String,
}

struct ActiveWebSocketSessionGuard {
    metrics: Metrics,
}

impl ActiveWebSocketSessionGuard {
    fn new(metrics: &Metrics) -> Self {
        metrics.increment_active_websocket_sessions();
        Self {
            metrics: metrics.clone(),
        }
    }
}

impl Drop for ActiveWebSocketSessionGuard {
    fn drop(&mut self) {
        self.metrics.decrement_active_websocket_sessions();
    }
}

#[derive(Debug, Clone)]
struct HttpLogContext {
    account_id_hash: String,
    upstream_request_id: Option<String>,
    cloudflare_ray: Option<String>,
    actual_service_tier: Option<String>,
    cached_input_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
}

#[derive(Debug, Clone)]
struct HttpMetricContext {
    model_family: String,
    stream: bool,
    requested_service_tier: Option<String>,
    reasoning_effort: Option<String>,
    verbosity: Option<String>,
    store: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct UsageMetadata {
    cached_input_tokens: Option<u64>,
    reasoning_tokens: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct StreamResponseMetadata {
    actual_service_tier: Option<String>,
    usage: UsageMetadata,
    first_event_duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
struct SseFirstFrameObserved;

pub fn app(state: AppState) -> Router {
    Router::new()
        .route(
            "/healthz",
            get(healthz).fallback(authenticated_method_not_allowed),
        )
        .route(
            "/metrics",
            get(metrics).fallback(authenticated_method_not_allowed),
        )
        .route(
            "/usage",
            get(usage).fallback(authenticated_method_not_allowed),
        )
        .route(
            "/v1/models",
            get(models).fallback(authenticated_method_not_allowed),
        )
        .route(
            "/v1/chat/completions",
            post(proxy_http).fallback(authenticated_openai_unsupported_route),
        )
        .route(
            "/v1/messages",
            post(proxy_http).fallback(authenticated_openai_unsupported_route),
        )
        .route(
            "/v1/responses",
            post(proxy_http)
                .get(responses_ws)
                .fallback(authenticated_openai_unsupported_route),
        )
        .route(
            "/v1/responses/compact",
            post(proxy_http)
                .get(responses_compact_get)
                .fallback(authenticated_method_not_allowed),
        )
        .fallback(authenticated_openai_unsupported_route)
        .with_state(state)
}

async fn healthz(State(state): State<AppState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "ok": true,
        "version": env!("CARGO_PKG_VERSION"),
        "enabled_accounts": state.routing_accounts().len(),
    }))
}

async fn metrics(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Response, TokenproxyError> {
    require_auth(&state, &headers)?;
    if !state.effective.config.observability.metrics {
        return Err(TokenproxyError::new(
            StatusCode::NOT_FOUND,
            ErrorCode::UnsupportedRoute,
            "metrics endpoint is disabled",
        ));
    }
    let usage_windows = state.usage_windows.lock().await.clone();
    let account_health = state.account_health_snapshot();
    let observed_at = now_rfc3339();
    let snapshot = usage_snapshot(
        &state.effective.config.server.id,
        &observed_at,
        &state.effective.config.accounts,
        &usage_windows,
        &account_health,
        &state.effective.account_hash_key,
    );
    Ok((
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4")],
        prometheus_text_with_usage(&state.metrics.snapshot(), Some(&snapshot)),
    )
        .into_response())
}

async fn usage(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, TokenproxyError> {
    require_auth(&state, &headers)?;
    let usage_windows = state.usage_windows.lock().await;
    let account_health = state.account_health_snapshot();
    let observed_at = now_rfc3339();
    Ok(Json(usage_snapshot(
        &state.effective.config.server.id,
        &observed_at,
        &state.effective.config.accounts,
        &usage_windows,
        &account_health,
        &state.effective.account_hash_key,
    )))
}

use serde::Serialize;
use std::collections::BTreeSet;

#[derive(Debug, Serialize, PartialEq, Eq)]
struct ModelList {
    object: &'static str,
    data: Vec<ModelEntry>,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
struct ModelEntry {
    id: String,
    object: &'static str,
}

fn model_list(accounts: &[EffectiveAccount]) -> ModelList {
    let mut ids = BTreeSet::new();
    for account in accounts {
        let config = &account.config;
        let supports_generation_model = config.supports_chat_completions
            || config.supports_responses
            || config.supports_responses_ws
            || config.supports_anthropic_messages;
        if !config.enabled || !supports_generation_model {
            continue;
        }
        ids.extend(config.models.iter().cloned());
    }

    ModelList {
        object: "list",
        data: ids
            .into_iter()
            .map(|id| ModelEntry {
                id,
                object: "model",
            })
            .collect(),
    }
}

async fn models(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, TokenproxyError> {
    require_auth(&state, &headers)?;
    let accounts = state.routing_accounts();
    Ok(Json(model_list(accounts)))
}

fn unsupported_route(method: Method, uri: Uri) -> TokenproxyError {
    TokenproxyError::new(
        StatusCode::NOT_FOUND,
        ErrorCode::UnsupportedRoute,
        format!("unsupported route: {method} {}", uri.path()),
    )
}

fn method_not_allowed(method: Method, uri: Uri) -> TokenproxyError {
    TokenproxyError::new(
        StatusCode::METHOD_NOT_ALLOWED,
        ErrorCode::UnsupportedMethod,
        format!("unsupported method: {method} {}", uri.path()),
    )
}

async fn authenticated_method_not_allowed(
    State(state): State<AppState>,
    headers: HeaderMap,
    method: Method,
    uri: Uri,
) -> Response {
    match require_auth(&state, &headers) {
        Ok(()) => {
            let started = Instant::now();
            let method_name = method.as_str().to_string();
            let path = uri.path().to_string();
            let error = method_not_allowed(method, uri);
            local_error_response(&state, &method_name, &path, &path, error, started)
        }
        Err(error) => error.into_response(),
    }
}

async fn authenticated_openai_unsupported_route(
    State(state): State<AppState>,
    headers: HeaderMap,
    method: Method,
    uri: Uri,
) -> Response {
    if uri.path().starts_with("/v1/")
        && let Err(error) = require_auth(&state, &headers)
    {
        return error.into_response();
    }
    let started = Instant::now();
    let method_name = method.as_str().to_string();
    let path = uri.path().to_string();
    let error = unsupported_route(method, uri);
    // Unmatched paths get a fixed metric label so attacker-chosen paths cannot
    // grow metric label cardinality without bound; the log keeps the real path.
    local_error_response(&state, &method_name, &path, "unmatched", error, started)
}

async fn proxy_http(State(state): State<AppState>, request: Request<Body>) -> Response {
    let started = Instant::now();
    let method = request.method().as_str().to_string();
    let path = request.uri().path().to_string();
    let request_id = state.next_request_id();
    let result = proxy_http_inner(&state, request, request_id.clone()).await;
    let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let (status, error_code, log_context) = match result.as_ref() {
        Ok(response) => (
            response.status(),
            None,
            response.extensions().get::<HttpLogContext>(),
        ),
        Err(error) => (error.status, Some(error.code.as_str()), None),
    };
    let metric_context = result
        .as_ref()
        .ok()
        .and_then(|response| response.extensions().get::<HttpMetricContext>())
        .cloned();
    let model_family = metric_context
        .as_ref()
        .map(|context| context.model_family.as_str())
        .unwrap_or("unknown");
    let stream = metric_context
        .as_ref()
        .map(|context| context.stream.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let transport = metric_context
        .as_ref()
        .map(|context| if context.stream { "sse" } else { "http" })
        .unwrap_or("http");
    let account_metric_label = log_context
        .map(|context| context.account_id_hash.as_str())
        .unwrap_or("none");
    state.metrics.record_request_duration_labeled(
        &path,
        transport,
        model_family,
        &stream,
        duration_ms,
    );
    state.metrics.increment_request_outcome(
        &path,
        transport,
        status_class(status),
        model_family,
        account_metric_label,
    );
    let timestamps = now_timestamp_pair();
    state.emit_request_log(&RequestLog {
        event: "request",
        timestamp_local: &timestamps.local,
        timestamp_utc: &timestamps.utc,
        tokenproxy_request_id: &request_id,
        method: &method,
        endpoint: &path,
        transport,
        status: status.as_u16(),
        duration_ms,
        account_id_hash: log_context.map(|context| context.account_id_hash.as_str()),
        upstream_request_id: log_context.and_then(|context| context.upstream_request_id.as_deref()),
        cloudflare_ray: log_context.and_then(|context| context.cloudflare_ray.as_deref()),
        requested_service_tier: metric_context
            .as_ref()
            .and_then(|context| context.requested_service_tier.as_deref()),
        reasoning_effort: metric_context
            .as_ref()
            .and_then(|context| context.reasoning_effort.as_deref()),
        verbosity: metric_context
            .as_ref()
            .and_then(|context| context.verbosity.as_deref()),
        store: metric_context
            .as_ref()
            .and_then(|context| context.store.as_deref()),
        actual_service_tier: log_context.and_then(|context| context.actual_service_tier.as_deref()),
        cached_input_tokens: log_context.and_then(|context| context.cached_input_tokens),
        reasoning_tokens: log_context.and_then(|context| context.reasoning_tokens),
        error_code,
    });

    match result {
        Ok(response) => response,
        Err(error) => error_response_with_request_id(error, &request_id),
    }
}

async fn proxy_http_inner(
    state: &AppState,
    request: Request<Body>,
    request_id: String,
) -> Result<Response, TokenproxyError> {
    let (parts, body) = request.into_parts();
    require_auth(state, &parts.headers)?;
    state.metrics.increment_requests();
    reject_compressed_body(&parts.headers)?;

    let body = to_bytes(body, state.effective.config.server.max_body_bytes)
        .await
        .map_err(|error| {
            TokenproxyError::new(
                StatusCode::PAYLOAD_TOO_LARGE,
                ErrorCode::BodyTooLarge,
                format!("failed to read request body: {error}"),
            )
        })?;
    if parts.uri.path() != "/v1/responses/compact" {
        maybe_dump_request_body(
            state,
            &request_id,
            parts.method.as_str(),
            parts.uri.path(),
            &parts.headers,
            &body,
        )
        .await?;
    }

    let classified = classify_request(parts.uri.path(), body)?;

    let route_request = classified.route_request;
    let compact_request_body =
        (route_request.endpoint == Endpoint::ResponsesCompact).then(|| classified.body.clone());
    let request_shape = classified.request_shape.clone();
    if let Some(shape) = request_shape.as_ref() {
        state.metrics.increment_request_shape(
            parts.uri.path(),
            &route_request.model_family,
            &shape.service_tier,
            &shape.reasoning_effort,
            &shape.verbosity,
            &shape.store,
        );
    }

    forward_with_precommit_failover(
        state,
        &route_request,
        HttpProxyAttempt {
            request_id,
            method: parts.method,
            path: parts.uri.path().to_string(),
            inbound_headers: parts.headers,
            body: classified.body,
            request_shape,
            compact_request_body,
        },
    )
    .await
}

fn error_response_with_request_id(error: TokenproxyError, request_id: &str) -> Response {
    let status = error.status;
    let body = serde_json::json!({
        "error": {
            "message": error.message,
            "type": "tokenproxy_error",
            "code": error.code.as_str(),
            "param": null,
            "tokenproxy_request_id": request_id
        }
    });
    (status, Json(body)).into_response()
}

fn local_error_response(
    state: &AppState,
    method: &str,
    path: &str,
    metric_endpoint: &str,
    error: TokenproxyError,
    started: Instant,
) -> Response {
    let request_id = state.next_request_id();
    let status = error.status;
    let error_code = error.code.as_str();
    let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    state.metrics.increment_requests();
    state.metrics.record_request_duration_labeled(
        metric_endpoint,
        "http",
        "unknown",
        "unknown",
        duration_ms,
    );
    state.metrics.increment_request_outcome(
        metric_endpoint,
        "http",
        status_class(status),
        "unknown",
        "none",
    );
    let timestamps = now_timestamp_pair();
    state.emit_request_log(&RequestLog {
        event: "request",
        timestamp_local: &timestamps.local,
        timestamp_utc: &timestamps.utc,
        tokenproxy_request_id: &request_id,
        method,
        endpoint: path,
        transport: "http",
        status: status.as_u16(),
        duration_ms,
        account_id_hash: None,
        upstream_request_id: None,
        cloudflare_ray: None,
        requested_service_tier: None,
        reasoning_effort: None,
        verbosity: None,
        store: None,
        actual_service_tier: None,
        cached_input_tokens: None,
        reasoning_tokens: None,
        error_code: Some(error_code),
    });
    error_response_with_request_id(error, &request_id)
}

async fn responses_ws(
    State(state): State<AppState>,
    headers: HeaderMap,
    method: Method,
    uri: Uri,
    ws: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Response {
    let started = Instant::now();
    if let Err(error) = require_auth(&state, &headers) {
        return error.into_response();
    }
    let ws = match ws {
        Ok(ws) => ws,
        Err(_) => {
            let error = TokenproxyError::new(
                StatusCode::UPGRADE_REQUIRED,
                ErrorCode::UnsupportedMethod,
                "GET /v1/responses requires WebSocket upgrade",
            );
            return local_error_response(
                &state,
                method.as_str(),
                uri.path(),
                uri.path(),
                error,
                started,
            );
        }
    };
    let request_id = state.next_request_id();
    ws.on_upgrade(move |socket| async move {
        relay_websocket_session(state, socket, request_id).await;
    })
    .into_response()
}

async fn responses_compact_get(
    State(state): State<AppState>,
    headers: HeaderMap,
    method: Method,
    uri: Uri,
) -> Response {
    let started = Instant::now();
    if let Err(error) = require_auth(&state, &headers) {
        return error.into_response();
    }
    local_error_response(
        &state,
        method.as_str(),
        uri.path(),
        uri.path(),
        TokenproxyError::new(
            StatusCode::UPGRADE_REQUIRED,
            ErrorCode::UnsupportedMethod,
            "GET /v1/responses/compact does not support WebSocket transport",
        ),
        started,
    )
}

async fn relay_websocket_session(state: AppState, socket: WebSocket, request_id: String) {
    let _active_session = ActiveWebSocketSessionGuard::new(&state.metrics);
    let started = Instant::now();
    let mut replay_state = ReplayState::default();
    let mut upstream_session = None;
    let mut status = StatusCode::SWITCHING_PROTOCOLS;
    let mut error_code = None;
    let mut shutdown_rx = state.shutdown_receiver();
    let (mut downstream, downstream_stream) = socket.split();
    let (events_tx, mut events_rx) = mpsc::channel(WEBSOCKET_SESSION_EVENT_BUFFER);
    let (overflow_tx, mut overflow_rx) = watch::channel(false);
    let downstream_metrics = state.metrics.clone();
    tokio::spawn(pump_downstream_session_events(
        downstream_stream,
        events_tx,
        overflow_tx,
        downstream_metrics,
    ));
    let downstream_send_timeout =
        Duration::from_millis(state.effective.config.timeouts.websocket_idle_ms);

    loop {
        if *shutdown_rx.borrow() {
            status = StatusCode::SERVICE_UNAVAILABLE;
            error_code = Some("shutdown");
            close_idle_upstream_session(&state.metrics, &mut upstream_session).await;
            let _ = close_downstream_for_shutdown(
                &state.metrics,
                &mut downstream,
                downstream_send_timeout,
            )
            .await;
            break;
        }

        let event = tokio::select! {
            event = events_rx.recv() => event,
            _ = wait_for_session_shutdown(&mut shutdown_rx), if !*shutdown_rx.borrow() => {
                status = StatusCode::SERVICE_UNAVAILABLE;
                error_code = Some("shutdown");
                close_idle_upstream_session(&state.metrics, &mut upstream_session).await;
                let _ = close_downstream_for_shutdown(
                    &state.metrics,
                    &mut downstream,
                    downstream_send_timeout,
                )
                .await;
                break;
            }
            _ = wait_for_session_event_overflow(&mut overflow_rx), if !*overflow_rx.borrow() => {
                status = StatusCode::SERVICE_UNAVAILABLE;
                error_code = Some("upstream_failure");
                close_idle_upstream_session(&state.metrics, &mut upstream_session).await;
                let _ = send_ws_error(
                    &state.metrics,
                    &mut downstream,
                    &request_id,
                    ErrorCode::UpstreamFailure.as_str(),
                    &websocket_session_event_overflow_error().message,
                    downstream_send_timeout,
                )
                .await;
                // Complete the RFC 6455 close handshake instead of dropping the TCP stream.
                let _ = send_downstream_with_backpressure(
                    &state.metrics,
                    downstream.send(DownstreamMessage::Close(Some(DownstreamCloseFrame {
                        code: 1011,
                        reason: "session event overflow".into(),
                    }))),
                    downstream_send_timeout,
                )
                .await;
                break;
            }
        };
        let Some(event) = event else {
            break;
        };
        let Some(action) = classify_downstream_session_event(event, &replay_state) else {
            break;
        };

        match action {
            Ok(WebSocketAction::Create(value)) => {
                if let Err(error) = relay_single_websocket_create(
                    &state,
                    &request_id,
                    &mut replay_state,
                    value,
                    WebSocketRelayIo {
                        downstream: &mut downstream,
                        downstream_events: &mut events_rx,
                        overflow_rx: &mut overflow_rx,
                        upstream_session: &mut upstream_session,
                        shutdown_rx: &mut shutdown_rx,
                    },
                )
                .await
                {
                    status = error.status;
                    error_code = Some(error.code.as_str());
                    if *shutdown_rx.borrow() {
                        error_code = Some("shutdown");
                        break;
                    }
                    let _ = send_ws_error(
                        &state.metrics,
                        &mut downstream,
                        &request_id,
                        error.code.as_str(),
                        &error.message,
                        downstream_send_timeout,
                    )
                    .await;
                    replay_state.in_flight = false;
                    close_idle_upstream_session(&state.metrics, &mut upstream_session).await;
                }
            }
            Ok(WebSocketAction::Ping) => {
                state
                    .metrics
                    .increment_websocket_event_outcome("downstream_ping", true);
            }
            Ok(WebSocketAction::Ignore) => {}
            Ok(WebSocketAction::Close {
                code,
                reason,
                event_type,
                success,
            }) => {
                state
                    .metrics
                    .increment_websocket_event_outcome(event_type, success);
                let _ = send_downstream_with_backpressure(
                    &state.metrics,
                    downstream.send(DownstreamMessage::Close(Some(DownstreamCloseFrame {
                        code,
                        reason: reason.into(),
                    }))),
                    downstream_send_timeout,
                )
                .await;
                break;
            }
            Err(error) => {
                status = error.status;
                error_code = Some(error.code.as_str());
                let event_type = websocket_error_event_type(error.code);
                let _ = send_ws_error(
                    &state.metrics,
                    &mut downstream,
                    &request_id,
                    error.code.as_str(),
                    &error.message,
                    downstream_send_timeout,
                )
                .await;
                state
                    .metrics
                    .increment_websocket_event_outcome(event_type, false);
            }
        }
    }
    close_idle_upstream_session(&state.metrics, &mut upstream_session).await;

    let timestamps = now_timestamp_pair();
    let duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let model_family = model_family_from_payload(&replay_state, &Value::Null);
    record_websocket_request_metrics(
        &state.metrics,
        status,
        duration_ms,
        model_family.as_deref(),
        replay_state.account_id_hash.as_deref(),
    );
    state.emit_request_log(&RequestLog {
        event: "request",
        timestamp_local: &timestamps.local,
        timestamp_utc: &timestamps.utc,
        tokenproxy_request_id: &request_id,
        method: "GET",
        endpoint: "/v1/responses",
        transport: "websocket",
        status: status.as_u16(),
        duration_ms,
        account_id_hash: replay_state.account_id_hash.as_deref(),
        upstream_request_id: None,
        cloudflare_ray: None,
        requested_service_tier: replay_state.requested_service_tier.as_deref(),
        reasoning_effort: replay_state.reasoning_effort.as_deref(),
        verbosity: replay_state.verbosity.as_deref(),
        store: replay_state.store.as_deref(),
        actual_service_tier: replay_state.actual_service_tier.as_deref(),
        cached_input_tokens: replay_state.cached_input_tokens,
        reasoning_tokens: replay_state.reasoning_tokens,
        error_code,
    });
}

struct WebSocketRelayIo<'a> {
    downstream: &'a mut DownstreamWebSocketSink,
    downstream_events: &'a mut mpsc::Receiver<DownstreamSessionEvent>,
    overflow_rx: &'a mut watch::Receiver<bool>,
    upstream_session: &'a mut Option<UpstreamSession>,
    shutdown_rx: &'a mut watch::Receiver<bool>,
}

async fn relay_single_websocket_create(
    state: &AppState,
    request_id: &str,
    replay_state: &mut ReplayState,
    value: Value,
    relay_io: WebSocketRelayIo<'_>,
) -> Result<(), TokenproxyError> {
    let WebSocketRelayIo {
        downstream,
        downstream_events,
        overflow_rx,
        upstream_session,
        shutdown_rx,
    } = relay_io;
    let route_context = websocket_route_context(replay_state, &value)?;
    let request_shape = websocket_request_shape(replay_state, &value);
    record_websocket_request_shape(
        &state.metrics,
        replay_state,
        &route_context.model_family,
        &request_shape,
    );
    let initial_route_request = websocket_route_request(replay_state, &route_context, &value);
    let base_replay_state = replay_state.clone();
    let max_attempts = usize::from(state.effective.config.retry.max_precommit_retries) + 1;
    let mut attempted_ids = Vec::new();
    let mut last_retryable_error = None;

    'attempts: loop {
        let route_request = if attempted_ids.is_empty() {
            initial_route_request.clone()
        } else {
            let mut retry = initial_route_request.clone();
            retry.pinned_account_id = None;
            retry.requires_incremental_previous_response_id = false;
            retry
        };
        let account = match select_next_account(state, &route_request, &attempted_ids).await {
            Ok(account) => account,
            Err(error) => return Err(last_retryable_error.unwrap_or(error)),
        };
        attempted_ids.push(account.config.id.clone());
        *replay_state = base_replay_state.clone();

        let reused_upstream_previous_response = match ensure_upstream_session(
            state,
            &account,
            upstream_session,
            &route_request.model_family,
            request_id,
        )
        .await
        {
            Ok(reused) => reused,
            Err(error) if should_retry_precommit_error(&error) => {
                if attempted_ids.len() < max_attempts {
                    last_retryable_error = Some(error);
                    close_idle_upstream_session(&state.metrics, upstream_session).await;
                    continue 'attempts;
                }
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        let normalized = prepare_websocket_upstream_payload_with_hash_key(
            replay_state,
            &account,
            &state.effective.account_hash_key,
            value.clone(),
            reused_upstream_previous_response,
        )?;
        if !replay_state.last_completed_output_items.is_empty()
            && normalized.get("previous_response_id").is_none()
        {
            state.metrics.add_replay_items_for_reason(
                "websocket",
                "full_replay",
                replay_input_item_count(&normalized),
            );
        }

        replay_state.in_flight = true;

        let mut upstream_closed = false;
        let mut retried_previous_response_not_found = false;
        let mut recorded_first_event = false;
        let mut draining_shutdown = false;
        let request_started = Instant::now();
        let idle_timeout = Duration::from_millis(state.effective.config.timeouts.websocket_idle_ms);
        let shutdown_grace = Duration::from_millis(state.effective.config.server.shutdown_grace_ms);
        let mut shutdown_deadline = Box::pin(tokio::time::sleep(Duration::from_secs(u64::MAX)));
        // A reused upstream session can die while idle. Allow one redial with the
        // same payload before surfacing the failure; if the fresh connection lacks
        // previous-response state, the previous_response_not_found path recovers.
        let mut reuse_retry_available = reused_upstream_previous_response;
        let mut first_event_seen = false;
        if let Err(error) = upstream_session
            .as_mut()
            .expect("ensure_upstream_session creates session")
            .socket
            .send(UpstreamMessage::Text(normalized.to_string().into()))
            .await
        {
            if !reuse_retry_available {
                let error = upstream_send_error(error);
                record_account_transient_failure(state, &account, &HeaderMap::new());
                replay_state.in_flight = false;
                if attempted_ids.len() < max_attempts {
                    last_retryable_error = Some(error);
                    close_idle_upstream_session(&state.metrics, upstream_session).await;
                    continue 'attempts;
                }
                return Err(error);
            }
            reuse_retry_available = false;
            if let Err(error) = redial_and_resend(
                state,
                &account,
                upstream_session,
                &route_context.model_family,
                request_id,
                &normalized,
            )
            .await
            {
                record_account_transient_failure(state, &account, &HeaderMap::new());
                replay_state.in_flight = false;
                if attempted_ids.len() < max_attempts {
                    last_retryable_error = Some(error);
                    close_idle_upstream_session(&state.metrics, upstream_session).await;
                    continue 'attempts;
                }
                return Err(error);
            }
        }

        'relay: loop {
            let upstream = &mut upstream_session
                .as_mut()
                .expect("ensure_upstream_session creates session")
                .socket;
            loop {
                tokio::select! {
                    message = upstream.next() => {
                        let message = match message {
                            Some(Ok(message)) => message,
                            failure => {
                                state
                                    .metrics
                                    .increment_websocket_event_outcome("upstream_close", false);
                                if reuse_retry_available && !first_event_seen {
                                    break;
                                }
                                let error = match failure {
                                    Some(Err(error)) => TokenproxyError::new(
                                        StatusCode::BAD_GATEWAY,
                                        ErrorCode::UpstreamFailure,
                                        format!("upstream WebSocket read failed: {error}"),
                                    ),
                                    _ => upstream_closed_before_completed_error(),
                                };
                                if !first_event_seen {
                                    record_account_transient_failure(state, &account, &HeaderMap::new());
                                    replay_state.in_flight = false;
                                    if attempted_ids.len() < max_attempts {
                                        last_retryable_error = Some(error);
                                        close_upstream_socket(&state.metrics, upstream).await;
                                        continue 'attempts;
                                    }
                                }
                                return Err(error);
                            }
                        };
                        if !matches!(message, UpstreamMessage::Close(_)) {
                            first_event_seen = true;
                        }
                        match message {
                            UpstreamMessage::Text(text) => {
                                state
                                    .metrics
                                    .increment_websocket_event_outcome("upstream_text", true);
                                // Parse each upstream frame once; the recorders below all
                                // read from this shared Value.
                                let event: Option<Value> = serde_json::from_str(&text).ok();
                                if let Some(event) = event.as_ref() {
                                    record_upstream_websocket_response_event_metric(&state.metrics, event);
                                    record_websocket_actual_service_tier(replay_state, event);
                                    record_websocket_usage_metadata(
                                        &state.metrics,
                                        replay_state,
                                        &route_context.model_family,
                                        event,
                                    );
                                    record_account_websocket_event_health(state, &account, event);
                                    record_websocket_usage_limit_error_event(state, &account, event).await;
                                }
                                if !recorded_first_event {
                                    let first_event_duration_ms =
                                        u64::try_from(request_started.elapsed().as_millis())
                                            .unwrap_or(u64::MAX);
                                    state.metrics.record_first_event_duration_labeled(
                                        "/v1/responses",
                                        "websocket",
                                        &route_context.model_family,
                                        first_event_duration_ms,
                                    );
                                    record_account_first_event_duration(
                                        state,
                                        &account,
                                        first_event_duration_ms,
                                    );
                                    recorded_first_event = true;
                                }
                                let previous_response_not_found = event
                                    .as_ref()
                                    .is_some_and(is_previous_response_not_found_event);
                                if previous_response_not_found
                                    && let Some(retry_payload) = previous_response_not_found_retry_payload(
                                        replay_state,
                                        normalized.clone(),
                                        retried_previous_response_not_found,
                                    )?
                                {
                                    replay_state.invalidate_previous_response();
                                    retried_previous_response_not_found = true;
                                    state
                                        .metrics
                                        .add_replay_items_for_reason(
                                            "websocket",
                                            "previous_response_not_found",
                                            replay_input_item_count(&retry_payload),
                                        );
                                    upstream
                                        .send(UpstreamMessage::Text(retry_payload.to_string().into()))
                                        .await
                                        .map_err(|error| {
                                            TokenproxyError::new(
                                                StatusCode::BAD_GATEWAY,
                                                ErrorCode::UpstreamFailure,
                                                format!(
                                                    "failed to send previous_response_not_found full replay: {error}"
                                                ),
                                            )
                                    })?;
                                    continue;
                                }
                                if previous_response_not_found {
                                    replay_state.invalidate_previous_response();
                                }
                                if let Some(event) = event.as_ref() {
                                    capture_completed_event(replay_state, event);
                                }
                                send_downstream_with_backpressure(
                                    &state.metrics,
                                    downstream.send(DownstreamMessage::Text(text.to_string().into())),
                                    idle_timeout,
                                )
                                .await?;
                                if !replay_state.in_flight {
                                    break 'relay;
                                }
                            }
                            UpstreamMessage::Binary(_) => {
                                state
                                    .metrics
                                    .increment_websocket_event_outcome("upstream_binary", false);
                                return Err(TokenproxyError::new(
                                    StatusCode::BAD_GATEWAY,
                                    ErrorCode::WebSocketUnsupportedMessage,
                                    "upstream sent unsupported binary WebSocket frame",
                                ));
                            }
                            UpstreamMessage::Ping(bytes) => {
                                upstream
                                    .send(UpstreamMessage::Pong(bytes))
                                    .await
                                    .map_err(|error| {
                                        TokenproxyError::new(
                                            StatusCode::BAD_GATEWAY,
                                            ErrorCode::UpstreamFailure,
                                            format!("failed to pong upstream WebSocket: {error}"),
                                        )
                                    })?;
                            }
                            UpstreamMessage::Close(_) => {
                                state
                                    .metrics
                                    .increment_websocket_event_outcome("upstream_close", false);
                                if reuse_retry_available && !first_event_seen {
                                    break;
                                }
                                let error = upstream_closed_before_completed_error();
                                if !first_event_seen {
                                    record_account_transient_failure(state, &account, &HeaderMap::new());
                                    replay_state.in_flight = false;
                                    if attempted_ids.len() < max_attempts {
                                        last_retryable_error = Some(error);
                                        close_upstream_socket(&state.metrics, upstream).await;
                                        continue 'attempts;
                                    }
                                }
                                return Err(error);
                            }
                            _ => {}
                        }
                    }
                    downstream_event = downstream_events.recv() => {
                        let Some(downstream_event) = downstream_event else {
                            state
                                .metrics
                                .increment_websocket_event_outcome("downstream_close", true);
                            close_upstream_socket(&state.metrics, upstream).await;
                            upstream_closed = true;
                            break 'relay;
                        };
                        let Some(action) = classify_downstream_session_event(downstream_event, replay_state) else {
                            state
                                .metrics
                                .increment_websocket_event_outcome("downstream_close", true);
                            close_upstream_socket(&state.metrics, upstream).await;
                            upstream_closed = true;
                            break 'relay;
                        };
                        match action {
                            Ok(WebSocketAction::Ping) => {
                                state
                                    .metrics
                                    .increment_websocket_event_outcome("downstream_ping", true);
                            }
                            Ok(WebSocketAction::Ignore) => {}
                            Ok(WebSocketAction::Close {
                                code,
                                reason,
                                event_type,
                                success,
                            }) => {
                                state
                                    .metrics
                                    .increment_websocket_event_outcome(event_type, success);
                                let _ = send_downstream_with_backpressure(
                                    &state.metrics,
                                    downstream.send(DownstreamMessage::Close(Some(DownstreamCloseFrame {
                                        code,
                                        reason: reason.into(),
                                    }))),
                                    idle_timeout,
                                )
                                .await;
                                close_upstream_socket(&state.metrics, upstream).await;
                                upstream_closed = true;
                                break 'relay;
                            }
                            // While a response is in flight classify_downstream_message
                            // rejects text frames with Err(WebSocketInFlight), so a Create
                            // here is unreachable; both paths report the same error.
                            other => {
                                let error = match other {
                                    Err(error) => error,
                                    _ => TokenproxyError::new(
                                        StatusCode::CONFLICT,
                                        ErrorCode::WebSocketInFlight,
                                        "one response is already in flight on this WebSocket",
                                    ),
                                };
                                let event_type = websocket_error_event_type(error.code);
                                state
                                    .metrics
                                    .increment_websocket_event_outcome(event_type, false);
                                let _ = send_ws_error(
                                    &state.metrics,
                                    downstream,
                                    request_id,
                                    error.code.as_str(),
                                    &error.message,
                                    idle_timeout,
                                )
                                .await;
                            }
                        }
                    }
                    _ = tokio::time::sleep(idle_timeout) => {
                        let error = TokenproxyError::new(
                            StatusCode::GATEWAY_TIMEOUT,
                            ErrorCode::UpstreamFailure,
                            "upstream WebSocket idle timeout",
                        );
                        if !first_event_seen {
                            record_account_transient_failure(state, &account, &HeaderMap::new());
                            replay_state.in_flight = false;
                            if attempted_ids.len() < max_attempts {
                                last_retryable_error = Some(error);
                                close_upstream_socket(&state.metrics, upstream).await;
                                continue 'attempts;
                            }
                        }
                        return Err(error);
                    }
                    _ = wait_for_session_shutdown(shutdown_rx), if !*shutdown_rx.borrow() && !draining_shutdown => {
                        draining_shutdown = true;
                        shutdown_deadline
                            .as_mut()
                            .reset(tokio::time::Instant::now() + shutdown_grace);
                        state
                            .metrics
                            .increment_websocket_event_outcome("shutdown_drain", true);
                    }
                    _ = wait_for_session_event_overflow(overflow_rx), if !*overflow_rx.borrow() => {
                        close_upstream_socket(&state.metrics, upstream).await;
                        replay_state.in_flight = false;
                        return Err(websocket_session_event_overflow_error());
                    }
                    _ = &mut shutdown_deadline, if draining_shutdown => {
                        let _ = close_downstream_for_shutdown(&state.metrics, downstream, idle_timeout).await;
                        replay_state.in_flight = false;
                        return Err(TokenproxyError::new(
                            StatusCode::SERVICE_UNAVAILABLE,
                            ErrorCode::UpstreamFailure,
                            "server shutdown grace elapsed",
                        ));
                    }
                }
            }

            // Only a recoverable reused-session failure breaks the inner loop.
            reuse_retry_available = false;
            if let Err(error) = redial_and_resend(
                state,
                &account,
                upstream_session,
                &route_context.model_family,
                request_id,
                &normalized,
            )
            .await
            {
                record_account_transient_failure(state, &account, &HeaderMap::new());
                replay_state.in_flight = false;
                if attempted_ids.len() < max_attempts {
                    last_retryable_error = Some(error);
                    close_idle_upstream_session(&state.metrics, upstream_session).await;
                    continue 'attempts;
                }
                return Err(error);
            }
        }

        replay_state.in_flight = false;
        if upstream_closed {
            *upstream_session = None;
        }
        return Ok(());
    }
}

fn upstream_send_error(error: impl Display) -> TokenproxyError {
    TokenproxyError::new(
        StatusCode::BAD_GATEWAY,
        ErrorCode::UpstreamFailure,
        format!("failed to send upstream WebSocket request: {error}"),
    )
}

async fn redial_and_resend(
    state: &AppState,
    account: &EffectiveAccount,
    upstream_session: &mut Option<UpstreamSession>,
    model_family: &str,
    request_id: &str,
    payload: &Value,
) -> Result<(), TokenproxyError> {
    if let Some(mut dead) = upstream_session.take() {
        close_upstream_socket(&state.metrics, &mut dead.socket).await;
    }
    state
        .metrics
        .increment_websocket_event_outcome("upstream_redial", true);
    ensure_upstream_session(state, account, upstream_session, model_family, request_id).await?;
    upstream_session
        .as_mut()
        .expect("ensure_upstream_session creates session")
        .socket
        .send(UpstreamMessage::Text(payload.to_string().into()))
        .await
        .map_err(upstream_send_error)
}

fn upstream_closed_before_completed_error() -> TokenproxyError {
    TokenproxyError::new(
        StatusCode::BAD_GATEWAY,
        ErrorCode::UpstreamFailure,
        "upstream WebSocket closed before response completed",
    )
}

async fn wait_for_session_shutdown(shutdown_rx: &mut watch::Receiver<bool>) {
    if *shutdown_rx.borrow() {
        return;
    }
    while shutdown_rx.changed().await.is_ok() {
        if *shutdown_rx.borrow() {
            return;
        }
    }
}

async fn wait_for_session_event_overflow(overflow_rx: &mut watch::Receiver<bool>) {
    if *overflow_rx.borrow() {
        return;
    }
    while overflow_rx.changed().await.is_ok() {
        if *overflow_rx.borrow() {
            return;
        }
    }
    std::future::pending::<()>().await;
}

async fn pump_downstream_session_events(
    mut downstream: DownstreamWebSocketStream,
    events_tx: mpsc::Sender<DownstreamSessionEvent>,
    overflow_tx: watch::Sender<bool>,
    metrics: Metrics,
) {
    while let Some(message) = downstream.next().await {
        let event = match message {
            Ok(message) => DownstreamSessionEvent::Message(message),
            Err(error) => DownstreamSessionEvent::ReceiveError(error.to_string()),
        };
        let should_continue =
            try_enqueue_downstream_session_event(&events_tx, &overflow_tx, &metrics, event);
        if !should_continue {
            return;
        }
    }

    let _ = try_enqueue_downstream_session_event(
        &events_tx,
        &overflow_tx,
        &metrics,
        DownstreamSessionEvent::Closed,
    );
}

fn try_enqueue_downstream_session_event(
    events_tx: &mpsc::Sender<DownstreamSessionEvent>,
    overflow_tx: &watch::Sender<bool>,
    metrics: &Metrics,
    event: DownstreamSessionEvent,
) -> bool {
    match events_tx.try_send(event) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Full(_)) => {
            metrics.increment_websocket_event_outcome("downstream_event_overflow", false);
            let _ = overflow_tx.send(true);
            false
        }
        Err(mpsc::error::TrySendError::Closed(_)) => false,
    }
}

fn classify_downstream_session_event(
    event: DownstreamSessionEvent,
    replay_state: &ReplayState,
) -> Option<Result<WebSocketAction, TokenproxyError>> {
    match event {
        DownstreamSessionEvent::Message(message) => {
            Some(classify_downstream_message(message, replay_state))
        }
        DownstreamSessionEvent::ReceiveError(error) => Some(Err(TokenproxyError::new(
            StatusCode::BAD_GATEWAY,
            ErrorCode::WebSocketUnsupportedMessage,
            format!("downstream WebSocket receive failed: {error}"),
        ))),
        DownstreamSessionEvent::Closed => None,
    }
}

fn websocket_session_event_overflow_error() -> TokenproxyError {
    TokenproxyError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::UpstreamFailure,
        "downstream WebSocket session event queue overflowed",
    )
}

async fn close_downstream_for_shutdown(
    metrics: &Metrics,
    downstream: &mut DownstreamWebSocketSink,
    timeout: Duration,
) -> Result<(), TokenproxyError> {
    let result = send_downstream_with_backpressure(
        metrics,
        downstream.send(DownstreamMessage::Close(Some(DownstreamCloseFrame {
            code: 1001,
            reason: "server shutdown".into(),
        }))),
        timeout,
    )
    .await;
    metrics.increment_websocket_event_outcome("shutdown_close", result.is_ok());
    result
}

async fn close_idle_upstream_session(
    metrics: &Metrics,
    upstream_session: &mut Option<UpstreamSession>,
) {
    let Some(mut session) = upstream_session.take() else {
        return;
    };
    close_upstream_socket(metrics, &mut session.socket).await;
}

async fn close_upstream_socket(metrics: &Metrics, upstream: &mut UpstreamWebSocket) {
    let success = upstream.close(None).await.is_ok();
    metrics.increment_websocket_event_outcome("upstream_session_close", success);
}

fn websocket_error_event_type(code: ErrorCode) -> &'static str {
    match code {
        ErrorCode::WebSocketInFlight => "downstream_create",
        _ => "downstream_parse",
    }
}

fn replay_input_item_count(payload: &Value) -> u64 {
    match payload.get("input").and_then(Value::as_array) {
        Some(items) => u64::try_from(items.len()).unwrap_or(u64::MAX),
        None => 0,
    }
}

async fn ensure_upstream_session(
    state: &AppState,
    account: &EffectiveAccount,
    upstream_session: &mut Option<UpstreamSession>,
    model_family: &str,
    request_id: &str,
) -> Result<bool, TokenproxyError> {
    let existing_account_id = upstream_session
        .as_ref()
        .map(|session| session.account_id.as_str());
    let existing_age = upstream_session
        .as_ref()
        .map(|session| session.opened_at.elapsed());
    let reused = !should_replace_upstream_session(
        existing_account_id,
        &account.config.id,
        existing_age,
        UPSTREAM_WS_MAX_SESSION_AGE,
    );
    if reused {
        state.metrics.record_ws_connect_duration_labeled(
            &websocket_origin(&account.config.base_url),
            model_family,
            true,
            0,
        );
        return Ok(true);
    }

    let ws_url = websocket_upstream_url_for_account(account)?;
    let origin = websocket_origin(&account.config.base_url);
    let mut request = ws_url.as_str().into_client_request().map_err(|error| {
        TokenproxyError::new(
            StatusCode::BAD_GATEWAY,
            ErrorCode::UpstreamFailure,
            format!("failed to build upstream WebSocket request: {error}"),
        )
    })?;
    request.headers_mut().insert(
        "authorization",
        upstream_authorization_header(&account.bearer_token)?,
    );
    if let Some(account_id) = account.chatgpt_account_id.as_deref() {
        request
            .headers_mut()
            .insert("chatgpt-account-id", header_value_from_str(account_id)?);
    }
    request.headers_mut().insert(
        "x-tokenproxy-request-id",
        header_value_from_str(request_id)?,
    );

    let started = Instant::now();
    let connect_result = tokio::time::timeout(
        Duration::from_millis(state.effective.config.timeouts.websocket_connect_ms),
        connect_async(request),
    )
    .await;
    let connect_duration_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    record_account_connect_duration(state, account, connect_duration_ms);
    state.metrics.record_upstream_connect_duration_labeled(
        &origin,
        "websocket",
        connect_duration_ms,
    );
    state.metrics.record_ws_connect_duration_labeled(
        &origin,
        model_family,
        false,
        connect_duration_ms,
    );
    let outcome = if matches!(connect_result, Ok(Ok(_))) {
        "connected"
    } else {
        "transport_error"
    };
    state.metrics.increment_upstream_attempt(
        "/v1/responses",
        "websocket",
        model_family,
        &account_id_hash(&account.config.id, &state.effective.account_hash_key),
        "initial",
        outcome,
    );
    let (socket, _) = match connect_result {
        Ok(Ok(socket)) => socket,
        Ok(Err(error)) => {
            record_account_transient_failure(state, account, &HeaderMap::new());
            return Err(TokenproxyError::new(
                StatusCode::BAD_GATEWAY,
                ErrorCode::UpstreamFailure,
                format!("failed to connect upstream WebSocket: {error}"),
            ));
        }
        Err(_) => {
            record_account_transient_failure(state, account, &HeaderMap::new());
            return Err(TokenproxyError::new(
                StatusCode::GATEWAY_TIMEOUT,
                ErrorCode::UpstreamFailure,
                "timed out connecting upstream WebSocket",
            ));
        }
    };
    let old_session = upstream_session.replace(UpstreamSession {
        account_id: account.config.id.clone(),
        opened_at: Instant::now(),
        socket,
    });
    if let Some(mut old_session) = old_session {
        close_upstream_socket(&state.metrics, &mut old_session.socket).await;
    }
    Ok(false)
}

fn header_value_from_str(value: &str) -> Result<HeaderValue, TokenproxyError> {
    value.parse().map_err(|error| {
        TokenproxyError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::UpstreamFailure,
            format!("failed to build upstream header value: {error}"),
        )
    })
}

fn upstream_authorization_header(bearer_token: &str) -> Result<HeaderValue, TokenproxyError> {
    let mut value = header_value_from_str(&format!("Bearer {bearer_token}"))?;
    value.set_sensitive(true);
    Ok(value)
}

fn should_replace_upstream_session(
    existing_account_id: Option<&str>,
    selected_account_id: &str,
    existing_age: Option<Duration>,
    max_age: Duration,
) -> bool {
    existing_account_id != Some(selected_account_id)
        || existing_age.is_some_and(|age| age >= max_age)
}

fn prepare_websocket_upstream_payload_with_hash_key(
    replay_state: &mut ReplayState,
    account: &EffectiveAccount,
    account_hash_key: &str,
    value: Value,
    connection_previous_response_available: bool,
) -> Result<Value, TokenproxyError> {
    let account_id_hash = account_id_hash(&account.config.id, account_hash_key);
    replay_state.account_id = Some(account.config.id.clone());
    replay_state.account_id_hash = Some(account_id_hash.clone());
    replay_state.supports_incremental_previous_response_id =
        account.config.supports_incremental_previous_response_id;

    if replay_state.last_request_template.is_none() {
        let normalized = normalize_websocket_create(value)?;
        let model_family = model_family_from_payload(replay_state, &normalized)
            .unwrap_or_else(|| "unknown".to_string());
        let normalized =
            value_with_prompt_cache_key(normalized, account, "downstream", &model_family);
        replay_state.record_request_template(normalized.clone());
        return Ok(normalized);
    }

    let normalized = normalize_websocket_create(value.clone())?;
    if is_compacted_request_window(&normalized) {
        let model_family = model_family_from_payload(replay_state, &value)
            .unwrap_or_else(|| "unknown".to_string());
        let normalized =
            value_with_prompt_cache_key(normalized, account, "downstream", &model_family);
        replay_state.reset_after_compaction(normalized.clone());
        return Ok(normalized);
    }

    let model_family =
        model_family_from_payload(replay_state, &value).unwrap_or_else(|| "unknown".to_string());
    match plan_next_request(
        replay_state,
        value,
        &account_id_hash,
        connection_previous_response_available,
    )? {
        ReplayPlan::Incremental(value) => Ok(value_with_prompt_cache_key(
            value,
            account,
            "downstream",
            &model_family,
        )),
        ReplayPlan::FullReplay(value) => {
            let value = value_with_prompt_cache_key(value, account, "downstream", &model_family);
            replay_state.record_request_template(value.clone());
            Ok(value)
        }
    }
}

fn websocket_route_context(
    replay_state: &ReplayState,
    value: &Value,
) -> Result<WebSocketRouteContext, TokenproxyError> {
    let model = value
        .get("model")
        .and_then(Value::as_str)
        .or_else(|| {
            replay_state
                .last_request_template
                .as_ref()
                .and_then(|template| template.get("model"))
                .and_then(Value::as_str)
        })
        .filter(|model| !model.is_empty())
        .ok_or_else(|| {
            TokenproxyError::new(
                StatusCode::BAD_REQUEST,
                ErrorCode::WebSocketUnsupportedMessage,
                "first WebSocket response.create must include model",
            )
        })?
        .to_string();
    let service_tier = value
        .get("service_tier")
        .and_then(Value::as_str)
        .or_else(|| {
            replay_state
                .last_request_template
                .as_ref()
                .and_then(|template| template.get("service_tier"))
                .and_then(Value::as_str)
        })
        .filter(|service_tier| !service_tier.is_empty())
        .map(ToOwned::to_owned);
    let model_family = model_family_label(&model);

    Ok(WebSocketRouteContext {
        model,
        service_tier,
        model_family,
    })
}

fn websocket_route_request(
    replay_state: &ReplayState,
    route_context: &WebSocketRouteContext,
    value: &Value,
) -> RouteRequest {
    RouteRequest {
        endpoint: Endpoint::Responses,
        transport: Transport::WebSocket,
        model: route_context.model.clone(),
        service_tier: route_context.service_tier.clone(),
        pinned_account_id: replay_state.account_id.clone(),
        requires_incremental_previous_response_id: value
            .get("previous_response_id")
            .and_then(Value::as_str)
            .is_some(),
        model_family: route_context.model_family.clone(),
        stream: true,
    }
}

fn capture_completed_event(state: &mut ReplayState, value: &Value) {
    if value.get("type").and_then(Value::as_str) == Some("response.output_item.done") {
        if let Some(item) = value.get("item").cloned() {
            state.pending_output_items.push(item);
        }
        return;
    }
    if value.get("type").and_then(Value::as_str) != Some("response.completed") {
        return;
    }
    let Some(response) = value.get("response") else {
        return;
    };
    let Some(response_id) = response.get("id").and_then(Value::as_str) else {
        return;
    };
    let mut output_items = response
        .get("output")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if output_items.is_empty() {
        output_items = state.pending_output_items.clone();
    }
    state.record_completed(response_id.to_string(), output_items);
}

async fn send_downstream_with_backpressure<F, E>(
    metrics: &Metrics,
    send: F,
    timeout: Duration,
) -> Result<(), TokenproxyError>
where
    F: Future<Output = Result<(), E>>,
    E: Display,
{
    match tokio::time::timeout(timeout, send).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(TokenproxyError::new(
            StatusCode::BAD_GATEWAY,
            ErrorCode::UpstreamFailure,
            format!("failed to send downstream WebSocket frame: {error}"),
        )),
        Err(_) => {
            metrics.increment_websocket_event_outcome("downstream_backpressure", false);
            Err(TokenproxyError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorCode::UpstreamFailure,
                "downstream WebSocket send backpressure timeout",
            ))
        }
    }
}

async fn send_ws_error(
    metrics: &Metrics,
    downstream: &mut DownstreamWebSocketSink,
    request_id: &str,
    code: &str,
    message: &str,
    timeout: Duration,
) -> Result<(), TokenproxyError> {
    send_downstream_with_backpressure(
        metrics,
        downstream.send(DownstreamMessage::Text(
            serde_json::json!({
                "type": "error",
                "error": {
                    "type": "tokenproxy_error",
                    "code": code,
                    "message": message,
                    "tokenproxy_request_id": request_id
                }
            })
            .to_string()
            .into(),
        )),
        timeout,
    )
    .await
}

fn websocket_upstream_url_for_account(account: &EffectiveAccount) -> Result<Url, TokenproxyError> {
    let mut url = upstream_url_for_path(account, "/v1/responses")?;
    match url.scheme() {
        "https" => url.set_scheme("wss").expect("wss scheme is valid"),
        "http" => url.set_scheme("ws").expect("ws scheme is valid"),
        _ => {
            return Err(TokenproxyError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::InvalidConfig,
                "WebSocket base_url must use http or https",
            ));
        }
    }
    Ok(url)
}

fn upstream_url_for_path(
    account: &EffectiveAccount,
    public_path: &str,
) -> Result<Url, TokenproxyError> {
    let base_url = Url::parse(&account.config.base_url).map_err(|error| {
        TokenproxyError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::InvalidConfig,
            format!("invalid account base_url: {error}"),
        )
    })?;

    match account.config.kind {
        AccountKind::OpenAiApiKey => match public_path {
            "/v1/chat/completions" | "/v1/responses" | "/v1/responses/compact" => base_url
                .join(public_path.trim_start_matches('/'))
                .map_err(|error| {
                    TokenproxyError::new(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        ErrorCode::InvalidConfig,
                        format!("failed to build upstream URL: {error}"),
                    )
                }),
            _ => Err(TokenproxyError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::InvalidConfig,
                format!("OpenAI account cannot serve upstream path {public_path}"),
            )),
        },
        AccountKind::AnthropicApiKey => {
            if public_path != "/v1/messages" {
                return Err(TokenproxyError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ErrorCode::InvalidConfig,
                    format!("Anthropic account cannot serve upstream path {public_path}"),
                ));
            }

            let mut url = base_url;
            let base_path = url.path().trim_end_matches('/');
            let upstream_path = if base_path.ends_with("/v1") {
                format!("{base_path}/messages")
            } else if base_path.ends_with("/v1/messages") {
                base_path.to_string()
            } else {
                format!("{base_path}/v1/messages")
            };
            url.set_path(&upstream_path);
            Ok(url)
        }
        AccountKind::ChatgptCodexAuthJson => chatgpt_codex_upstream_url(base_url, public_path),
    }
}

fn chatgpt_codex_upstream_url(
    mut base_url: Url,
    public_path: &str,
) -> Result<Url, TokenproxyError> {
    let upstream_suffix = match public_path {
        "/v1/responses" => "responses",
        "/v1/responses/compact" => "responses/compact",
        _ => {
            return Err(TokenproxyError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::InvalidConfig,
                format!("ChatGPT/Codex account cannot serve upstream path {public_path}"),
            ));
        }
    };
    let base_path = base_url.path().trim_end_matches('/');
    base_url.set_path(&format!("{base_path}/{upstream_suffix}"));
    Ok(base_url)
}

fn websocket_origin(base_url: &str) -> String {
    let Ok(url) = Url::parse(base_url) else {
        return "unknown".to_string();
    };
    url_origin(&url)
}

fn url_origin(url: &Url) -> String {
    let Some(host) = url.host_str() else {
        return "unknown".to_string();
    };
    match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    }
}

async fn forward_to_upstream(
    state: &AppState,
    account: &EffectiveAccount,
    forward: UpstreamForward<'_>,
) -> Result<Response, TokenproxyError> {
    let account_id_hash = account_id_hash(&account.config.id, &state.effective.account_hash_key);
    let method_name = forward.method.as_str().to_string();
    let upstream_url = upstream_url_for_path(account, forward.path)?;
    let origin = url_origin(&upstream_url);
    let upstream_host = upstream_url.host_str().ok_or_else(|| {
        TokenproxyError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::InvalidConfig,
            "upstream URL lacks host",
        )
    })?;
    let headers = build_upstream_headers(
        &forward.inbound_headers,
        upstream_host,
        &account.bearer_token,
        account.chatgpt_account_id.as_deref(),
        forward.request_id,
        match account.config.kind {
            AccountKind::AnthropicApiKey => UpstreamAuth::AnthropicApiKey,
            AccountKind::OpenAiApiKey | AccountKind::ChatgptCodexAuthJson => UpstreamAuth::Bearer,
        },
        state.effective.config.server.allow_openai_request_headers,
    )?;

    let upstream_started = Instant::now();
    let upstream_request = state
        .upstream_client
        .request(forward.method, upstream_url)
        .headers(headers)
        .body(forward.body);
    let response = match tokio::time::timeout(
        Duration::from_millis(state.effective.config.timeouts.request_header_ms),
        upstream_request.send(),
    )
    .await
    {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => {
            record_account_transient_failure(state, account, &HeaderMap::new());
            let connect_duration_ms =
                u64::try_from(upstream_started.elapsed().as_millis()).unwrap_or(u64::MAX);
            record_account_connect_duration(state, account, connect_duration_ms);
            state.metrics.record_upstream_connect_duration_labeled(
                &origin,
                "http",
                connect_duration_ms,
            );
            state.metrics.increment_upstream_attempt(
                forward.path,
                "http",
                forward.model_family,
                &account_id_hash,
                forward.retry_phase,
                "transport_error",
            );
            return Err(TokenproxyError::new(
                StatusCode::BAD_GATEWAY,
                ErrorCode::UpstreamFailure,
                format!("upstream request failed: {error}"),
            ));
        }
        Err(_) => {
            record_account_transient_failure(state, account, &HeaderMap::new());
            let connect_duration_ms =
                u64::try_from(upstream_started.elapsed().as_millis()).unwrap_or(u64::MAX);
            record_account_connect_duration(state, account, connect_duration_ms);
            state.metrics.record_upstream_connect_duration_labeled(
                &origin,
                "http",
                connect_duration_ms,
            );
            state.metrics.increment_upstream_attempt(
                forward.path,
                "http",
                forward.model_family,
                &account_id_hash,
                forward.retry_phase,
                "transport_error",
            );
            return Err(TokenproxyError::new(
                StatusCode::GATEWAY_TIMEOUT,
                ErrorCode::UpstreamFailure,
                "timed out waiting for upstream response headers",
            ));
        }
    };
    let connect_duration_ms =
        u64::try_from(upstream_started.elapsed().as_millis()).unwrap_or(u64::MAX);
    record_account_connect_duration(state, account, connect_duration_ms);
    state
        .metrics
        .record_upstream_connect_duration_labeled(&origin, "http", connect_duration_ms);

    let status = response.status();
    state.metrics.increment_upstream_attempt(
        forward.path,
        "http",
        forward.model_family,
        &account_id_hash,
        forward.retry_phase,
        status_class(status),
    );
    let response_headers = response.headers().clone();
    let log_context = http_log_context(
        account,
        &response_headers,
        &state.effective.account_hash_key,
    );
    let repair_sse = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("text/event-stream"));
    let observe_json_usage = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.starts_with("application/json"));
    let observed_at = now_rfc3339();
    let mut usage_windows = usage_windows_from_headers(response.headers(), &observed_at);
    let headers = filter_downstream_response_headers(response.headers());
    if status == StatusCode::TOO_MANY_REQUESTS {
        let body = response.bytes().await.map_err(|error| {
            TokenproxyError::new(
                StatusCode::BAD_GATEWAY,
                ErrorCode::UpstreamFailure,
                format!("failed to read upstream error body: {error}"),
            )
        })?;
        usage_windows.extend(usage_windows_from_error_body(status, &body, &observed_at));
        let usage_limited_health = usage_limited_health_from_windows(&usage_windows);
        record_account_http_status(
            state,
            account,
            status,
            &response_headers,
            usage_limited_health,
        )
        .await;
        if !usage_windows.is_empty() {
            state
                .usage_windows
                .lock()
                .await
                .insert(account.config.id.clone(), usage_windows);
        }
        if let Some(compact_request_body) = forward.compact_request_body.as_ref() {
            maybe_dump_compact_body_hash(
                state,
                forward.request_id,
                &method_name,
                forward.path,
                status,
                compact_request_body,
                &body,
            )
            .await?;
        }

        let response = response_with_headers(status, headers, Body::from(body))?;
        return Ok(response_with_log_context(response, log_context));
    }
    record_account_http_status(state, account, status, &response_headers, None).await;
    record_usage_limited_health_from_windows(state, &account.config.id, &usage_windows);
    if !usage_windows.is_empty() {
        state
            .usage_windows
            .lock()
            .await
            .insert(account.config.id.clone(), usage_windows);
    }
    if let Some(compact_request_body) = forward.compact_request_body.as_ref() {
        let body = response_body_with_limit(
            response,
            state.effective.config.server.max_body_bytes,
            "upstream compact response body",
        )
        .await?;
        maybe_dump_compact_body_hash(
            state,
            forward.request_id,
            &method_name,
            forward.path,
            status,
            compact_request_body,
            &body,
        )
        .await?;
        let response = response_with_headers(status, headers, Body::from(body))?;
        return Ok(response_with_log_context(response, log_context));
    }
    if repair_sse {
        let (response, stream_metadata) = sse_response_after_first_event(SseFirstEvent {
            status,
            headers,
            stream: response.bytes_stream(),
            metrics: &state.metrics,
            endpoint: forward.path,
            model_family: forward.model_family,
            account_id_hash: &account_id_hash,
            started: upstream_started,
            idle_timeout: Duration::from_millis(state.effective.config.timeouts.stream_idle_ms),
        })
        .await?;
        if let Some(first_event_duration_ms) = stream_metadata.first_event_duration_ms {
            record_account_first_event_duration(state, account, first_event_duration_ms);
        }
        let mut log_context = log_context;
        log_context.actual_service_tier = stream_metadata.actual_service_tier;
        log_context.cached_input_tokens = stream_metadata.usage.cached_input_tokens;
        log_context.reasoning_tokens = stream_metadata.usage.reasoning_tokens;
        if let Some(cached_input_tokens) = stream_metadata.usage.cached_input_tokens {
            state.metrics.add_cached_input_tokens(
                forward.path,
                forward.model_family,
                cached_input_tokens,
            );
        }
        return Ok(response_with_log_context(response, log_context));
    }
    if observe_json_usage {
        let body = response_body_with_limit(
            response,
            state.effective.config.server.max_body_bytes,
            "upstream JSON response body",
        )
        .await?;
        let mut log_context = log_context;
        // Parse the JSON body once; usage metadata and service tier share it.
        let value: Option<Value> = serde_json::from_slice(&body).ok();
        let metadata = value
            .as_ref()
            .map(usage_metadata_from_value)
            .unwrap_or_default();
        if let Some(cached_input_tokens) = metadata.cached_input_tokens {
            state.metrics.add_cached_input_tokens(
                forward.path,
                forward.model_family,
                cached_input_tokens,
            );
        }
        log_context.cached_input_tokens = metadata.cached_input_tokens;
        log_context.reasoning_tokens = metadata.reasoning_tokens;
        log_context.actual_service_tier = value.as_ref().and_then(actual_service_tier_from_value);
        let response = response_with_headers(status, headers, Body::from(body))?;
        return Ok(response_with_log_context(response, log_context));
    }
    let body = Body::from_stream(response.bytes_stream());

    let response = response_with_headers(status, headers, body)?;
    Ok(response_with_log_context(response, log_context))
}

fn http_log_context(
    account: &EffectiveAccount,
    headers: &HeaderMap,
    account_hash_key: &str,
) -> HttpLogContext {
    HttpLogContext {
        account_id_hash: account_id_hash(&account.config.id, account_hash_key),
        upstream_request_id: header_value(headers, "x-request-id")
            .or_else(|| header_value(headers, "openai-request-id"))
            .or_else(|| header_value(headers, "request-id")),
        cloudflare_ray: header_value(headers, "cf-ray"),
        actual_service_tier: None,
        cached_input_tokens: None,
        reasoning_tokens: None,
    }
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn response_with_log_context(mut response: Response, context: HttpLogContext) -> Response {
    response.extensions_mut().insert(context);
    response
}

fn response_with_headers(
    status: StatusCode,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, TokenproxyError> {
    let mut builder = Response::builder().status(status);
    for (name, value) in headers {
        if let Some(name) = name {
            builder = builder.header(name, value);
        }
    }
    builder.body(body).map_err(|error| {
        TokenproxyError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::UpstreamFailure,
            format!("failed to build downstream response: {error}"),
        )
    })
}

async fn response_body_with_limit(
    response: reqwest::Response,
    max_body_bytes: usize,
    label: &str,
) -> Result<Bytes, TokenproxyError> {
    let mut body = Vec::new();
    let mut stream = response.bytes_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|error| {
            TokenproxyError::new(
                StatusCode::BAD_GATEWAY,
                ErrorCode::UpstreamFailure,
                format!("failed to read {label}: {error}"),
            )
        })?;
        let next_len = body.len().checked_add(chunk.len()).ok_or_else(|| {
            TokenproxyError::new(
                StatusCode::BAD_GATEWAY,
                ErrorCode::UpstreamFailure,
                "upstream compact response body length overflowed usize",
            )
        })?;
        if next_len > max_body_bytes {
            return Err(TokenproxyError::new(
                StatusCode::BAD_GATEWAY,
                ErrorCode::UpstreamFailure,
                format!("{label} exceeds server.max_body_bytes"),
            ));
        }
        body.extend_from_slice(&chunk);
    }

    Ok(Bytes::from(body))
}

fn usage_metadata_from_value(value: &Value) -> UsageMetadata {
    let Some(usage) = value
        .get("usage")
        .or_else(|| value.pointer("/response/usage"))
    else {
        return UsageMetadata::default();
    };

    let mut cached_total = 0u64;
    let mut cached_found = false;
    for pointer in [
        "/input_tokens_details/cached_tokens",
        "/prompt_tokens_details/cached_tokens",
    ] {
        if let Some(count) = usage.pointer(pointer).and_then(Value::as_u64) {
            cached_found = true;
            cached_total = cached_total.saturating_add(count);
        }
    }

    UsageMetadata {
        cached_input_tokens: cached_found.then_some(cached_total),
        reasoning_tokens: usage
            .pointer("/output_tokens_details/reasoning_tokens")
            .and_then(Value::as_u64),
    }
}

fn actual_service_tier_from_value(value: &Value) -> Option<String> {
    value
        .get("service_tier")
        .and_then(Value::as_str)
        .filter(|tier| !tier.is_empty())
        .map(ToOwned::to_owned)
}

fn actual_service_tier_from_sse_frames(frames: &[Bytes]) -> Option<String> {
    frames.iter().find_map(|frame| {
        let text = std::str::from_utf8(frame).ok()?;
        text.lines()
            .find_map(|line| line.strip_prefix("data:").map(str::trim))
            .and_then(|data| {
                if data == "[DONE]" {
                    return None;
                }

                let value = serde_json::from_str::<Value>(data).ok()?;
                if value.get("type").and_then(Value::as_str) != Some("response.created") {
                    return None;
                }

                value
                    .pointer("/response/service_tier")
                    .or_else(|| value.get("service_tier"))
                    .and_then(Value::as_str)
                    .filter(|tier| !tier.is_empty())
                    .map(ToOwned::to_owned)
            })
    })
}

fn usage_metadata_from_sse_frames(frames: &[Bytes]) -> UsageMetadata {
    let mut metadata = UsageMetadata::default();
    for frame in frames {
        let Ok(text) = std::str::from_utf8(frame) else {
            continue;
        };
        for data in text
            .lines()
            .filter_map(|line| line.strip_prefix("data:").map(str::trim))
        {
            let observed = usage_metadata_from_sse_data(data);
            if observed.cached_input_tokens.is_some() {
                metadata.cached_input_tokens = observed.cached_input_tokens;
            }
            if observed.reasoning_tokens.is_some() {
                metadata.reasoning_tokens = observed.reasoning_tokens;
            }
        }
    }
    metadata
}

fn usage_metadata_from_sse_data(data: &str) -> UsageMetadata {
    if data == "[DONE]" {
        return UsageMetadata::default();
    }

    serde_json::from_str::<Value>(data)
        .ok()
        .map(|value| usage_metadata_from_value(&value))
        .unwrap_or_default()
}

fn record_websocket_request_shape(
    metrics: &Metrics,
    replay_state: &mut ReplayState,
    model_family: &str,
    shape: &RequestShape,
) {
    replay_state.requested_service_tier =
        Some(normalize_service_tier(&shape.service_tier).to_string());
    replay_state.reasoning_effort = Some(shape.reasoning_effort.clone());
    replay_state.verbosity = Some(shape.verbosity.clone());
    replay_state.store = Some(shape.store.clone());
    metrics.increment_request_shape(
        "/v1/responses",
        model_family,
        &shape.service_tier,
        &shape.reasoning_effort,
        &shape.verbosity,
        &shape.store,
    );
}

fn websocket_request_shape(replay_state: &ReplayState, value: &Value) -> RequestShape {
    let service_tier = match websocket_string_field(replay_state, value, "service_tier") {
        Some(tier) if is_legacy_fast_service_tier(Some(&tier)) => "priority".to_string(),
        Some(tier) => tier,
        None => "unknown".to_string(),
    };
    let store = value
        .get("store")
        .and_then(Value::as_bool)
        .or_else(|| {
            replay_state
                .last_request_template
                .as_ref()
                .and_then(|template| template.get("store"))
                .and_then(Value::as_bool)
        })
        .map(|store| store.to_string())
        .unwrap_or_else(|| "unset".to_string());

    RequestShape {
        service_tier,
        reasoning_effort: websocket_nested_string_field(replay_state, value, "reasoning", "effort")
            .unwrap_or_else(|| "unset".to_string()),
        verbosity: websocket_nested_string_field(replay_state, value, "text", "verbosity")
            .unwrap_or_else(|| "unset".to_string()),
        store,
    }
}

fn websocket_string_field(
    replay_state: &ReplayState,
    value: &Value,
    field: &str,
) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .or_else(|| {
            replay_state
                .last_request_template
                .as_ref()
                .and_then(|template| template.get(field))
                .and_then(Value::as_str)
        })
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn websocket_nested_string_field(
    replay_state: &ReplayState,
    value: &Value,
    object: &str,
    field: &str,
) -> Option<String> {
    value
        .get(object)
        .and_then(Value::as_object)
        .and_then(|object| object.get(field))
        .and_then(Value::as_str)
        .or_else(|| {
            replay_state
                .last_request_template
                .as_ref()
                .and_then(|template| template.get(object))
                .and_then(Value::as_object)
                .and_then(|object| object.get(field))
                .and_then(Value::as_str)
        })
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn record_websocket_actual_service_tier(replay_state: &mut ReplayState, value: &Value) {
    if value.get("type").and_then(Value::as_str) != Some("response.created") {
        return;
    }
    let actual_service_tier = value
        .pointer("/response/service_tier")
        .or_else(|| value.get("service_tier"))
        .and_then(Value::as_str)
        .filter(|tier| !tier.is_empty())
        .map(ToOwned::to_owned);
    if actual_service_tier.is_some() {
        replay_state.actual_service_tier = actual_service_tier;
    }
}

fn record_websocket_usage_metadata(
    metrics: &Metrics,
    replay_state: &mut ReplayState,
    model_family: &str,
    value: &Value,
) {
    let metadata = usage_metadata_from_value(value);
    if let Some(cached_input_tokens) = metadata.cached_input_tokens {
        replay_state.cached_input_tokens = Some(cached_input_tokens);
        metrics.add_cached_input_tokens("/v1/responses", model_family, cached_input_tokens);
    }
    if metadata.reasoning_tokens.is_some() {
        replay_state.reasoning_tokens = metadata.reasoning_tokens;
    }
}

fn record_account_websocket_event_health(
    state: &AppState,
    account: &EffectiveAccount,
    event: &Value,
) {
    if event
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|event_type| {
            event_type.starts_with("response.")
                && !event_type.contains("error")
                && !event_type.contains("failed")
        })
    {
        state.clear_account_health_if_not_auth_failed(&account.config.id);
    }
}

fn record_upstream_websocket_response_event_metric(metrics: &Metrics, event: &Value) {
    let Some((event_type, success)) = event
        .get("type")
        .and_then(Value::as_str)
        .and_then(bounded_response_event_metric_type)
    else {
        return;
    };
    metrics.increment_websocket_event_outcome(event_type, success);
}

fn record_sse_response_event_metrics_labeled(
    metrics: &Metrics,
    frames: &[Bytes],
    model_family: &str,
    account_id_hash: &str,
) {
    for frame in frames {
        let Ok(text) = std::str::from_utf8(frame) else {
            metrics.increment_sse_event_outcome_labeled(
                "parse_error",
                false,
                model_family,
                account_id_hash,
            );
            continue;
        };
        let Some((event_type, success)) = bounded_sse_response_event_metric(text) else {
            continue;
        };
        metrics.increment_sse_event_outcome_labeled(
            event_type,
            success,
            model_family,
            account_id_hash,
        );
    }
}

fn bounded_sse_response_event_metric(frame: &str) -> Option<(&'static str, bool)> {
    if let Some(event_type) = frame
        .lines()
        .find_map(|line| line.strip_prefix("event:").map(str::trim))
        .filter(|event_type| !event_type.is_empty())
    {
        return bounded_response_event_metric_type(event_type);
    }

    let data = frame
        .lines()
        .find_map(|line| line.strip_prefix("data:").map(str::trim))?;
    if data == "[DONE]" {
        return None;
    }
    let value = serde_json::from_str::<Value>(data).ok()?;
    let event_type = value.get("type").and_then(Value::as_str)?;
    bounded_response_event_metric_type(event_type)
}

fn bounded_response_event_metric_type(event_type: &str) -> Option<(&'static str, bool)> {
    match event_type {
        "error" => Some(("error", false)),
        "response.completed" => Some(("response.completed", true)),
        "response.created" => Some(("response.created", true)),
        "response.failed" => Some(("response.failed", false)),
        "response.output_item.added" => Some(("response.output_item.added", true)),
        "response.output_item.done" => Some(("response.output_item.done", true)),
        "response.output_text.delta" => Some(("response.output_text.delta", true)),
        event_type if event_type.starts_with("response.") => Some(("response.other", true)),
        _ => None,
    }
}

async fn record_websocket_usage_limit_error_event(
    state: &AppState,
    account: &EffectiveAccount,
    event: &Value,
) {
    let observed_at = now_rfc3339();
    let usage_windows = usage_windows_from_usage_limit_error_value(event, &observed_at);
    if usage_windows.is_empty() {
        return;
    }

    record_usage_limited_health_from_windows(state, &account.config.id, &usage_windows);
    state
        .usage_windows
        .lock()
        .await
        .insert(account.config.id.clone(), usage_windows);
}

async fn maybe_dump_compact_body_hash(
    state: &AppState,
    request_id: &str,
    method: &str,
    path: &str,
    status: StatusCode,
    compact_request_body: &Bytes,
    response_body: &[u8],
) -> Result<(), TokenproxyError> {
    let observability = &state.effective.config.observability;
    if !observability.request_body_dumps {
        return Ok(());
    }

    append_observability_record(
        state,
        "compact-body-hashes.jsonl",
        compact_body_hash_record(
            request_id,
            method,
            path,
            status.as_u16(),
            compact_request_body,
            response_body,
        ),
    )
    .await
}

type BoxStreamError = Box<dyn Error + Send + Sync>;
type BoxedSseEventStream<S> = Pin<Box<EventStream<PendingLimitedSseStream<S>>>>;
const MAX_PENDING_SSE_BYTES: usize = 16 * 1024 * 1024;

struct SseStreamState<S> {
    stream: BoxedSseEventStream<S>,
    repair: SseRepair,
    pending: VecDeque<Bytes>,
    finished: bool,
    idle_timeout: Duration,
    cancellation: SseClientCancellation,
    pending_bytes: Arc<AtomicUsize>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SseMetricContext {
    model_family: String,
    account_id_hash: String,
}

impl SseMetricContext {
    fn new(model_family: &str, account_id_hash: &str) -> Self {
        Self {
            model_family: model_family.to_string(),
            account_id_hash: account_id_hash.to_string(),
        }
    }
}

struct PendingLimitedSseStream<S> {
    stream: Pin<Box<S>>,
    pending_bytes: Arc<AtomicUsize>,
}

fn bounded_eventsource_stream<S, E>(stream: S) -> (BoxedSseEventStream<S>, Arc<AtomicUsize>)
where
    S: futures_util::Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: Error + Send + Sync + 'static,
{
    let pending_bytes = Arc::new(AtomicUsize::new(0));
    let stream = Box::pin(
        PendingLimitedSseStream::new(Box::pin(stream), Arc::clone(&pending_bytes)).eventsource(),
    );
    (stream, pending_bytes)
}

impl<S> PendingLimitedSseStream<S> {
    fn new(stream: Pin<Box<S>>, pending_bytes: Arc<AtomicUsize>) -> Self {
        Self {
            stream,
            pending_bytes,
        }
    }
}

impl<S, E> futures_util::Stream for PendingLimitedSseStream<S>
where
    S: futures_util::Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: Error + Send + Sync + 'static,
{
    type Item = Result<Bytes, BoxStreamError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.stream.as_mut().poll_next(cx) {
            std::task::Poll::Ready(Some(Ok(chunk))) => {
                let pending =
                    self.pending_bytes.fetch_add(chunk.len(), Ordering::Relaxed) + chunk.len();
                if pending > MAX_PENDING_SSE_BYTES {
                    let error = TokenproxyError::new(
                        StatusCode::BAD_GATEWAY,
                        ErrorCode::UpstreamFailure,
                        "SSE stream exceeded frame buffer limit without a frame boundary",
                    );
                    std::task::Poll::Ready(Some(Err(Box::new(error) as BoxStreamError)))
                } else {
                    std::task::Poll::Ready(Some(Ok(chunk)))
                }
            }
            std::task::Poll::Ready(Some(Err(error))) => {
                std::task::Poll::Ready(Some(Err(Box::new(error) as BoxStreamError)))
            }
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

struct SseClientCancellation {
    metrics: Option<Metrics>,
    metric_context: SseMetricContext,
    terminal: bool,
}

impl SseClientCancellation {
    fn new(metrics: Option<Metrics>, metric_context: SseMetricContext) -> Self {
        Self {
            metrics,
            metric_context,
            terminal: false,
        }
    }

    fn mark_terminal(&mut self) {
        self.terminal = true;
    }

    fn metrics(&self) -> Option<&Metrics> {
        self.metrics.as_ref()
    }

    fn metric_context(&self) -> &SseMetricContext {
        &self.metric_context
    }
}

impl Drop for SseClientCancellation {
    fn drop(&mut self) {
        if !self.terminal
            && let Some(metrics) = &self.metrics
        {
            metrics.increment_sse_event_outcome_labeled(
                "client_cancelled",
                true,
                &self.metric_context.model_family,
                &self.metric_context.account_id_hash,
            );
        }
    }
}

async fn sse_response_after_first_event<S, E>(
    args: SseFirstEvent<'_, S>,
) -> Result<(Response, StreamResponseMetadata), TokenproxyError>
where
    S: futures_util::Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: Error + Send + Sync + 'static,
{
    let (mut stream, pending_bytes) = bounded_eventsource_stream(args.stream);
    let mut repair = SseRepair::default();

    match tokio::time::timeout(args.idle_timeout, stream.as_mut().next()).await {
        Ok(Some(Ok(event))) => {
            pending_bytes.store(0, Ordering::Relaxed);
            let frames = vec![repair.observe_event(event)?];

            let first_event_duration_ms =
                u64::try_from(args.started.elapsed().as_millis()).unwrap_or(u64::MAX);
            args.metrics.record_first_event_duration_labeled(
                args.endpoint,
                "sse",
                args.model_family,
                first_event_duration_ms,
            );
            record_sse_response_event_metrics_labeled(
                args.metrics,
                &frames,
                args.model_family,
                args.account_id_hash,
            );
            let metadata = StreamResponseMetadata {
                actual_service_tier: actual_service_tier_from_sse_frames(&frames),
                usage: usage_metadata_from_sse_frames(&frames),
                first_event_duration_ms: Some(first_event_duration_ms),
            };
            let body = Body::from_stream(repair_sse_stream_from_state(SseStreamState {
                stream,
                repair,
                pending: VecDeque::from(frames),
                finished: false,
                idle_timeout: args.idle_timeout,
                cancellation: SseClientCancellation::new(
                    Some(args.metrics.clone()),
                    SseMetricContext::new(args.model_family, args.account_id_hash),
                ),
                pending_bytes,
            }));
            let mut response = response_with_headers(args.status, args.headers, body)?;
            response.extensions_mut().insert(SseFirstFrameObserved);
            Ok((response, metadata))
        }
        Ok(Some(Err(error))) => Err(TokenproxyError::new(
            StatusCode::BAD_GATEWAY,
            ErrorCode::UpstreamFailure,
            format!("failed to read first upstream SSE event: {error}"),
        )),
        Ok(None) => Err(TokenproxyError::new(
            StatusCode::BAD_GATEWAY,
            ErrorCode::UpstreamFailure,
            "upstream SSE ended before first event",
        )),
        Err(_) => Err(TokenproxyError::new(
            StatusCode::GATEWAY_TIMEOUT,
            ErrorCode::UpstreamFailure,
            "upstream SSE idle timeout before first event",
        )),
    }
}

fn repair_sse_stream_from_state<S, E>(
    state: SseStreamState<S>,
) -> impl futures_util::Stream<Item = Result<Bytes, BoxStreamError>>
where
    S: futures_util::Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: Error + Send + Sync + 'static,
{
    futures_util::stream::unfold(state, |mut state| async move {
        if let Some(bytes) = state.pending.pop_front() {
            return Some((Ok(bytes), state));
        }
        if state.finished {
            state.cancellation.mark_terminal();
            return None;
        }

        loop {
            match tokio::time::timeout(state.idle_timeout, state.stream.as_mut().next()).await {
                Ok(Some(Ok(event))) => {
                    state.pending_bytes.store(0, Ordering::Relaxed);
                    match state.repair.observe_event(event) {
                        Ok(frame) => {
                            if let Some(metrics) = state.cancellation.metrics() {
                                let metric_context = state.cancellation.metric_context();
                                record_sse_response_event_metrics_labeled(
                                    metrics,
                                    std::slice::from_ref(&frame),
                                    &metric_context.model_family,
                                    &metric_context.account_id_hash,
                                );
                            }
                            state.pending.push_back(frame);
                            if let Some(bytes) = state.pending.pop_front() {
                                return Some((Ok(bytes), state));
                            }
                        }
                        Err(error) => {
                            if let Some(metrics) = state.cancellation.metrics() {
                                let metric_context = state.cancellation.metric_context();
                                metrics.increment_sse_event_outcome_labeled(
                                    "parse_error",
                                    false,
                                    &metric_context.model_family,
                                    &metric_context.account_id_hash,
                                );
                            }
                            state.finished = true;
                            state.cancellation.mark_terminal();
                            return Some((Err(Box::new(error) as BoxStreamError), state));
                        }
                    }
                }
                Ok(Some(Err(error))) => {
                    if let Some(metrics) = state.cancellation.metrics() {
                        let metric_context = state.cancellation.metric_context();
                        metrics.increment_sse_event_outcome_labeled(
                            "upstream_stream_error",
                            false,
                            &metric_context.model_family,
                            &metric_context.account_id_hash,
                        );
                    }
                    state.finished = true;
                    state.cancellation.mark_terminal();
                    return Some((Err(Box::new(error) as BoxStreamError), state));
                }
                Ok(None) => {
                    state.finished = true;
                    state.cancellation.mark_terminal();
                    return None;
                }
                Err(_) => {
                    state.finished = true;
                    state.cancellation.mark_terminal();
                    return Some((
                        Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "upstream SSE idle timeout",
                        )) as BoxStreamError),
                        state,
                    ));
                }
            }
        }
    })
}

async fn forward_with_precommit_failover(
    state: &AppState,
    route_request: &RouteRequest,
    attempt: HttpProxyAttempt,
) -> Result<Response, TokenproxyError> {
    let mut attempted_ids = Vec::new();
    let mut last_retryable_error = None;
    let max_attempts = usize::from(state.effective.config.retry.max_precommit_retries) + 1;

    for _ in 0..max_attempts {
        let retry_phase = if attempted_ids.is_empty() {
            "initial"
        } else {
            "retry"
        };
        let selected = match select_next_account(state, route_request, &attempted_ids).await {
            Ok(selected) => selected,
            Err(error) => {
                return Err(last_retryable_error.unwrap_or(error));
            }
        };
        attempted_ids.push(selected.config.id.clone());

        let response_result = forward_to_upstream(
            state,
            &selected,
            UpstreamForward {
                request_id: &attempt.request_id,
                method: attempt.method.clone(),
                path: &attempt.path,
                inbound_headers: attempt.inbound_headers.clone(),
                body: body_with_request_transforms(&attempt.body, route_request, &selected)?,
                model_family: &route_request.model_family,
                retry_phase,
                compact_request_body: attempt.compact_request_body.clone(),
            },
        )
        .await;
        let mut response = match response_result {
            Ok(response) => response,
            Err(error)
                if should_retry_precommit_error(&error) && attempted_ids.len() < max_attempts =>
            {
                last_retryable_error = Some(error);
                continue;
            }
            Err(error) => return Err(error),
        };
        response.extensions_mut().insert(HttpMetricContext {
            model_family: route_request.model_family.clone(),
            stream: route_request.stream,
            requested_service_tier: route_request
                .service_tier
                .as_deref()
                .map(|service_tier| normalize_service_tier(service_tier).to_string()),
            reasoning_effort: attempt
                .request_shape
                .as_ref()
                .map(|shape| shape.reasoning_effort.clone()),
            verbosity: attempt
                .request_shape
                .as_ref()
                .map(|shape| shape.verbosity.clone()),
            store: attempt
                .request_shape
                .as_ref()
                .map(|shape| shape.store.clone()),
        });

        if should_retry_precommit_response(&response) && attempted_ids.len() < max_attempts {
            continue;
        }

        if response.status() == StatusCode::TOO_MANY_REQUESTS
            && let Some(error) =
                all_compatible_accounts_usage_limited_error(state, route_request).await
        {
            return Err(error);
        }

        return Ok(response);
    }

    Err(TokenproxyError::new(
        StatusCode::BAD_GATEWAY,
        ErrorCode::UpstreamFailure,
        "all pre-commit upstream attempts failed",
    ))
}

async fn all_compatible_accounts_usage_limited_error(
    state: &AppState,
    route_request: &RouteRequest,
) -> Option<TokenproxyError> {
    let usage_windows = state.usage_windows.lock().await;
    let now_ms = now_unix_ms();
    let mut compatible_count = 0usize;
    let mut usage_limited_count = 0usize;
    let mut earliest_reset_at_ms = u64::MAX;

    let accounts = state.routing_accounts();
    for account in accounts.iter() {
        let routing_account = routing_account_state(account, AccountHealth::Open, 0, 0, 0);
        if !account_static_compatible(&routing_account, route_request) {
            continue;
        }
        compatible_count += 1;

        let AccountHealth::UsageLimited { reset_at_ms } = account_selection_health(
            state,
            account,
            usage_windows.get(&account.config.id).map(Vec::as_slice),
        ) else {
            continue;
        };
        if now_ms >= reset_at_ms {
            continue;
        }

        usage_limited_count += 1;
        earliest_reset_at_ms = earliest_reset_at_ms.min(reset_at_ms);
    }

    if compatible_count == 0 || compatible_count != usage_limited_count {
        return None;
    }

    let reset = if earliest_reset_at_ms == u64::MAX {
        "unknown reset deadline".to_string()
    } else {
        format!("earliest reset at unix_ms={earliest_reset_at_ms}")
    };
    Some(TokenproxyError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        ErrorCode::NoEligibleAccount,
        format!("no eligible upstream account: all compatible accounts are usage-limited; {reset}"),
    ))
}

fn account_selection_health(
    state: &AppState,
    account: &EffectiveAccount,
    usage_windows: Option<&[UsageWindow]>,
) -> AccountHealth {
    let runtime_health = match state.account_health_cell(&account.config.id) {
        Some(cell) => cell.load(),
        None => AccountHealth::Open,
    };
    if matches!(runtime_health, AccountHealth::UsageLimited { .. }) {
        return runtime_health;
    }

    let usage_health = usage_health_from_windows(usage_windows);
    if matches!(usage_health, AccountHealth::UsageLimited { .. }) {
        usage_health
    } else {
        runtime_health
    }
}

fn body_with_request_transforms(
    body: &Bytes,
    route_request: &RouteRequest,
    account: &EffectiveAccount,
) -> Result<Bytes, TokenproxyError> {
    let needs_prompt_cache_key =
        route_request.endpoint == Endpoint::Responses && account.prompt_cache_key_seed.is_some();
    let needs_service_tier_normalization =
        is_legacy_fast_service_tier(route_request.service_tier.as_deref());

    if !needs_prompt_cache_key && !needs_service_tier_normalization {
        return Ok(body.clone());
    }
    let mut value = serde_json::from_slice::<Value>(body).map_err(|error| {
        TokenproxyError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidJson,
            format!("failed to parse request body for prompt cache key: {error}"),
        )
    })?;
    let Some(object) = value.as_object_mut() else {
        return Ok(body.clone());
    };
    if object
        .get("service_tier")
        .and_then(Value::as_str)
        .is_some_and(|tier| is_legacy_fast_service_tier(Some(tier)))
    {
        object.insert(
            "service_tier".to_string(),
            Value::String("priority".to_string()),
        );
    }

    if needs_prompt_cache_key && !object.contains_key("prompt_cache_key") {
        value =
            value_with_prompt_cache_key(value, account, "downstream", &route_request.model_family);
    }

    serde_json::to_vec(&value)
        .map(Bytes::from)
        .map_err(|error| {
            TokenproxyError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::UpstreamFailure,
                format!("failed to serialize request body with prompt cache key: {error}"),
            )
        })
}

fn is_legacy_fast_service_tier(service_tier: Option<&str>) -> bool {
    service_tier.is_some_and(|tier| tier.trim().eq_ignore_ascii_case("fast"))
}

fn value_with_prompt_cache_key(
    mut value: Value,
    account: &EffectiveAccount,
    caller_hash: &str,
    model_family: &str,
) -> Value {
    let Some(seed) = account.prompt_cache_key_seed.as_deref() else {
        return value;
    };
    let Some(object) = value.as_object_mut() else {
        return value;
    };
    if object.contains_key("prompt_cache_key") {
        return value;
    }

    object.insert(
        "prompt_cache_key".to_string(),
        Value::String(derived_prompt_cache_key(
            seed,
            &account.config.id,
            caller_hash,
            model_family,
        )),
    );
    value
}

fn model_family_from_payload(state: &ReplayState, value: &Value) -> Option<String> {
    value
        .get("model")
        .and_then(Value::as_str)
        .or_else(|| {
            state
                .last_request_template
                .as_ref()
                .and_then(|template| template.get("model"))
                .and_then(Value::as_str)
        })
        .map(model_family_label)
}

fn derived_prompt_cache_key(
    seed: &str,
    account_id: &str,
    caller_hash: &str,
    model_family: &str,
) -> String {
    let digest =
        sha256_hex(format!("{seed}\0{account_id}\0{caller_hash}\0{model_family}").as_bytes());
    format!("tp_{}", &digest[..32])
}

async fn maybe_dump_request_body(
    state: &AppState,
    request_id: &str,
    method: &str,
    path: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> Result<(), TokenproxyError> {
    let observability = &state.effective.config.observability;
    if !observability.request_body_dumps {
        return Ok(());
    }

    let record = request_body_dump_record(
        request_id,
        method,
        path,
        headers,
        body,
        &observability.redact_json_pointers,
    );
    append_observability_record(state, "request-bodies.jsonl", record).await
}

async fn append_observability_record(
    state: &AppState,
    filename: &str,
    record: Value,
) -> Result<(), TokenproxyError> {
    let observability = &state.effective.config.observability;
    tokio::fs::create_dir_all(&observability.dump_dir)
        .await
        .map_err(|error| {
            TokenproxyError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::UpstreamFailure,
                format!("failed to create request body dump directory: {error}"),
            )
        })?;
    let dump_path = std::path::Path::new(&observability.dump_dir).join(filename);
    let mut file = tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dump_path)
        .await
        .map_err(|error| {
            TokenproxyError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::UpstreamFailure,
                format!("failed to open request body dump file: {error}"),
            )
        })?;
    file.write_all(record.to_string().as_bytes())
        .await
        .map_err(|error| {
            TokenproxyError::new(
                StatusCode::INTERNAL_SERVER_ERROR,
                ErrorCode::UpstreamFailure,
                format!("failed to write request body dump: {error}"),
            )
        })?;
    file.write_all(b"\n").await.map_err(|error| {
        TokenproxyError::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::UpstreamFailure,
            format!("failed to finish request body dump: {error}"),
        )
    })
}

async fn record_account_http_status(
    state: &AppState,
    account: &EffectiveAccount,
    status: StatusCode,
    headers: &HeaderMap,
    usage_limited_health: Option<AccountHealth>,
) {
    if let Some(health @ AccountHealth::UsageLimited { .. }) = usage_limited_health {
        state.store_account_health(&account.config.id, health);
        return;
    }

    let health = if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
        Some(AccountHealth::AuthFailed)
    } else if matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::SERVICE_UNAVAILABLE
    ) {
        record_account_transient_failure(state, account, headers);
        return;
    } else if status.is_success() {
        None
    } else {
        return;
    };

    if let Some(health) = health {
        state.store_account_health(&account.config.id, health);
    } else {
        state.clear_account_health_if_not_auth_failed(&account.config.id);
    }
}

fn record_usage_limited_health_from_windows(
    state: &AppState,
    account_id: &str,
    usage_windows: &[UsageWindow],
) {
    if let Some(health) = usage_limited_health_from_windows(usage_windows) {
        state.store_account_health(account_id, health);
    }
}

fn usage_limited_health_from_windows(usage_windows: &[UsageWindow]) -> Option<AccountHealth> {
    let health = usage_health_from_windows(Some(usage_windows));
    if matches!(health, AccountHealth::UsageLimited { .. }) {
        Some(health)
    } else {
        None
    }
}

fn record_account_connect_duration(state: &AppState, account: &EffectiveAccount, duration_ms: u64) {
    if let Some(cell) = state.account_health_cell(&account.config.id) {
        cell.record_connect_duration_ms(duration_ms);
    }
}

fn record_account_first_event_duration(
    state: &AppState,
    account: &EffectiveAccount,
    duration_ms: u64,
) {
    if let Some(cell) = state.account_health_cell(&account.config.id) {
        cell.record_first_event_duration_ms(duration_ms);
    }
}

fn record_account_transient_failure(
    state: &AppState,
    account: &EffectiveAccount,
    headers: &HeaderMap,
) {
    let now_ms = now_unix_ms();
    let failure_count = match state.account_health_cell(&account.config.id) {
        Some(cell) => cell.increment_transient_failure_count_at(now_ms),
        None => 1,
    };
    state.store_account_health(
        &account.config.id,
        AccountHealth::Throttled {
            next_retry_at_ms: throttle_deadline_ms(
                headers,
                now_ms,
                &state.effective.config.retry,
                &account.config.id,
                failure_count,
            ),
        },
    );
}

fn retry_after_deadline_ms(headers: &HeaderMap, now_ms: u64) -> Option<u64> {
    let value = headers
        .get("retry-after")
        .and_then(|value| value.to_str().ok())?
        .trim();
    parse_retry_after_deadline_ms(value, now_ms)
}

fn throttle_deadline_ms(
    headers: &HeaderMap,
    now_ms: u64,
    retry: &RetryConfig,
    account_id: &str,
    failure_count: u32,
) -> u64 {
    if retry.honor_retry_after
        && let Some(deadline) = retry_after_deadline_ms(headers, now_ms)
    {
        return deadline;
    }

    let capped_backoff = exponential_backoff_ms(retry, failure_count);
    let jitter_cap = capped_backoff.min(retry.max_backoff_ms.saturating_sub(capped_backoff));
    let jitter = deterministic_backoff_jitter_ms(account_id, now_ms, jitter_cap);
    now_ms.saturating_add(capped_backoff.saturating_add(jitter))
}

fn exponential_backoff_ms(retry: &RetryConfig, failure_count: u32) -> u64 {
    let exponent = failure_count.saturating_sub(1).min(63);
    let multiplier = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
    retry
        .base_backoff_ms
        .saturating_mul(multiplier)
        .min(retry.max_backoff_ms)
}

fn deterministic_backoff_jitter_ms(account_id: &str, now_ms: u64, jitter_cap_ms: u64) -> u64 {
    if jitter_cap_ms == 0 {
        return 0;
    }

    let mut hash = 0xcbf29ce484222325u64;
    for byte in account_id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash ^= now_ms;
    hash = hash.wrapping_mul(0x100000001b3);
    hash % (jitter_cap_ms + 1)
}

async fn select_next_account(
    state: &AppState,
    route_request: &RouteRequest,
    attempted_ids: &[String],
) -> Result<EffectiveAccount, TokenproxyError> {
    let usage_windows = state.usage_windows.lock().await;
    let accounts = state.routing_accounts();
    let routing_accounts = accounts
        .iter()
        .filter(|account| !attempted_ids.contains(&account.config.id))
        .map(|account| {
            let recent_failure_count = match state.account_health_cell(&account.config.id) {
                Some(cell) => cell.transient_failure_count(),
                None => 0,
            };
            let health = account_selection_health(
                state,
                account,
                usage_windows.get(&account.config.id).map(Vec::as_slice),
            );
            let (connect_bucket, first_event_bucket) =
                match state.account_health_cell(&account.config.id) {
                    Some(cell) => (
                        cell.connect_latency_bucket(),
                        cell.first_event_latency_bucket(),
                    ),
                    None => (0, 0),
                };
            routing_account_state(
                account,
                health,
                recent_failure_count,
                connect_bucket,
                first_event_bucket,
            )
        })
        .collect::<Vec<_>>();
    let selection = select_account(&routing_accounts, route_request, now_unix_ms());
    for (_, reason) in &selection.excluded {
        state.metrics.increment_route_exclusion(reason.as_str());
    }
    let selected_id = selection.selected.ok_or_else(|| {
        TokenproxyError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            ErrorCode::NoEligibleAccount,
            "no eligible upstream account",
        )
    })?;
    let selected_account_id_hash = account_id_hash(&selected_id, &state.effective.account_hash_key);
    let timestamps = now_timestamp_pair();
    for (excluded_account_id, reason) in &selection.excluded {
        let excluded_account_id_hash =
            account_id_hash(excluded_account_id, &state.effective.account_hash_key);
        state.emit_route_selection_log(&RouteSelectionLog {
            event: "route_selection",
            timestamp_local: &timestamps.local,
            timestamp_utc: &timestamps.utc,
            endpoint: match route_request.endpoint {
                Endpoint::ChatCompletions => "/v1/chat/completions",
                Endpoint::Responses => "/v1/responses",
                Endpoint::ResponsesCompact => "/v1/responses/compact",
                Endpoint::AnthropicMessages => "/v1/messages",
            },
            transport: match route_request.transport {
                Transport::Http => "http",
                Transport::WebSocket => "websocket",
            },
            model_family: &route_request.model_family,
            selected_account_id_hash: &selected_account_id_hash,
            excluded_account_id_hash: &excluded_account_id_hash,
            excluded_reason: reason.as_str(),
        });
    }

    accounts
        .iter()
        .find(|account| account.config.id == selected_id)
        .cloned()
        .ok_or_else(|| {
            TokenproxyError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                ErrorCode::NoEligibleAccount,
                "selected account is missing from effective config",
            )
        })
}

fn should_retry_precommit(status: StatusCode) -> bool {
    matches!(
        status,
        StatusCode::TOO_MANY_REQUESTS
            | StatusCode::INTERNAL_SERVER_ERROR
            | StatusCode::SERVICE_UNAVAILABLE
    )
}

fn should_retry_precommit_response(response: &Response) -> bool {
    should_retry_precommit(response.status())
        && response
            .extensions()
            .get::<SseFirstFrameObserved>()
            .is_none()
}

fn should_retry_precommit_error(error: &TokenproxyError) -> bool {
    error.code == ErrorCode::UpstreamFailure
        && matches!(
            error.status,
            StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
        )
}

fn status_class(status: StatusCode) -> &'static str {
    match status.as_u16() {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "unknown",
    }
}

fn record_websocket_request_metrics(
    metrics: &Metrics,
    status: StatusCode,
    duration_ms: u64,
    model_family: Option<&str>,
    account_id_hash: Option<&str>,
) {
    let model_family = model_family.unwrap_or("unknown");
    let account_id_hash = account_id_hash.unwrap_or("none");
    metrics.increment_requests();
    metrics.record_request_duration_labeled(
        "/v1/responses",
        "websocket",
        model_family,
        "true",
        duration_ms,
    );
    metrics.increment_request_outcome(
        "/v1/responses",
        "websocket",
        status_class(status),
        model_family,
        account_id_hash,
    );
}

fn require_auth(state: &AppState, headers: &HeaderMap) -> Result<(), TokenproxyError> {
    let expected = format!("Bearer {}", state.effective.downstream_token);
    let bearer_authorized = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|actual| constant_time_eq(actual.as_bytes(), expected.as_bytes()));
    let api_key_authorized = headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|actual| {
            constant_time_eq(
                actual.as_bytes(),
                state.effective.downstream_token.as_bytes(),
            )
        });

    if bearer_authorized || api_key_authorized {
        Ok(())
    } else {
        Err(TokenproxyError::new(
            StatusCode::UNAUTHORIZED,
            ErrorCode::Unauthorized,
            "missing or invalid downstream credential",
        ))
    }
}

fn reject_compressed_body(headers: &HeaderMap) -> Result<(), TokenproxyError> {
    for value in headers.get_all("content-encoding") {
        let Ok(value) = value.to_str() else {
            return Err(unsupported_content_encoding());
        };
        for encoding in value.split(',') {
            let encoding = encoding.trim();
            if !encoding.is_empty() && !encoding.eq_ignore_ascii_case("identity") {
                return Err(unsupported_content_encoding());
            }
        }
    }

    Ok(())
}

fn unsupported_content_encoding() -> TokenproxyError {
    TokenproxyError::new(
        StatusCode::UNSUPPORTED_MEDIA_TYPE,
        ErrorCode::UnsupportedMediaType,
        "compressed request bodies are unsupported in stage two",
    )
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    let max_len = left.len().max(right.len());
    let mut diff = left.len() ^ right.len();
    for index in 0..max_len {
        let left = left.get(index).copied().unwrap_or(0);
        let right = right.get(index).copied().unwrap_or(0);
        diff |= usize::from(left ^ right);
    }
    diff == 0
}

fn routing_account_state(
    account: &EffectiveAccount,
    health: AccountHealth,
    recent_failure_count: u32,
    ewma_connect_ms_bucket: u16,
    ewma_first_event_ms_bucket: u16,
) -> AccountState {
    AccountState {
        config: RoutingAccountConfig {
            id: account.config.id.clone(),
            priority: account.config.priority,
            models: account.config.models.clone(),
            service_tiers: account.config.service_tiers.clone(),
            supports_chat_completions: account.config.supports_chat_completions,
            supports_responses: account.config.supports_responses,
            supports_responses_ws: account.config.supports_responses_ws,
            supports_incremental_previous_response_id: account
                .config
                .supports_incremental_previous_response_id,
            supports_compact: account.config.supports_compact,
            supports_anthropic_messages: account.config.supports_anthropic_messages,
        },
        health,
        ewma_connect_ms_bucket,
        ewma_first_event_ms_bucket,
        recent_failure_count,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AccountConfig, Config, EffectiveConfig};
    use futures_util::TryStreamExt;
    use std::{net::SocketAddr, sync::Mutex as StdMutex};
    use tokio::{net::TcpListener, time::sleep};
    use tokio_tungstenite::{accept_async, accept_hdr_async};
    use tower::ServiceExt;

    impl SseMetricContext {
        fn unknown() -> Self {
            Self::new("unknown", "unknown")
        }
    }

    async fn fake_websocket_upstream(accepted_count: Arc<AtomicUsize>) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                accepted_count.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    if let Ok(mut socket) = accept_async(stream).await {
                        while socket.next().await.is_some() {}
                    }
                });
            }
        });
        address
    }

    #[allow(clippy::result_large_err)]
    async fn fake_header_capture_websocket_upstream(
        request_ids: Arc<StdMutex<Vec<Option<String>>>>,
    ) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let request_ids = Arc::clone(&request_ids);
                tokio::spawn(async move {
                    let callback =
                        |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                         response| {
                            let request_id = request
                                .headers()
                                .get("x-tokenproxy-request-id")
                                .and_then(|value| value.to_str().ok())
                                .map(ToOwned::to_owned);
                            request_ids
                                .lock()
                                .expect("request id capture lock is not poisoned")
                                .push(request_id);
                            Ok(response)
                        };
                    if let Ok(mut socket) = accept_hdr_async(stream, callback).await {
                        while socket.next().await.is_some() {}
                    }
                });
            }
        });
        address
    }

    fn effective_config(accounts: Vec<EffectiveAccount>) -> EffectiveConfig {
        let mut config = Config::default();
        config.accounts = accounts
            .iter()
            .map(|account| account.config.clone())
            .collect();
        config.server.allow_insecure_upstream = true;
        config.retry = RetryConfig {
            max_precommit_retries: 1,
            ..RetryConfig::default()
        };
        EffectiveConfig {
            config,
            downstream_token: "client-key".to_string(),
            account_hash_key: "test-account-hash-key".to_string(),
            accounts,
        }
    }

    fn account(id: &str, base_url: String, bearer_token: &str, priority: i32) -> EffectiveAccount {
        EffectiveAccount {
            config: AccountConfig {
                id: id.to_string(),
                kind: AccountKind::OpenAiApiKey,
                base_url,
                token_env: Some(format!("{id}_TOKEN")),
                priority,
                models: vec!["gpt-5.5".to_string()],
                supports_chat_completions: true,
                supports_responses: true,
                supports_responses_ws: true,
                supports_compact: true,
                service_tiers: vec!["default".to_string(), "priority".to_string()],
                ..AccountConfig::default()
            },
            bearer_token: bearer_token.to_string(),
            chatgpt_account_id: None,
            prompt_cache_key_seed: None,
        }
    }

    fn anthropic_account(
        id: &str,
        base_url: String,
        api_key: &str,
        priority: i32,
    ) -> EffectiveAccount {
        EffectiveAccount {
            config: AccountConfig {
                id: id.to_string(),
                kind: AccountKind::AnthropicApiKey,
                base_url,
                token_env: Some(format!("{id}_TOKEN")),
                priority,
                models: vec!["claude-sonnet-4.5".to_string()],
                supports_anthropic_messages: true,
                service_tiers: Vec::new(),
                ..AccountConfig::default()
            },
            bearer_token: api_key.to_string(),
            chatgpt_account_id: None,
            prompt_cache_key_seed: None,
        }
    }

    fn ws_event(text: &str) -> Value {
        serde_json::from_str(text).expect("test event payload parses")
    }

    #[test]
    fn should_record_first_websocket_template_without_transport_fields() {
        let account = account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        );
        let mut state = ReplayState::default();

        let payload = prepare_websocket_upstream_payload(
            &mut state,
            &account,
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.5",
                "stream": true,
                "background": true,
                "input": [{"type": "message", "role": "user", "content": "hello"}]
            }),
        )
        .unwrap();

        assert!(payload.get("stream").is_none());
        assert!(payload.get("background").is_none());
        assert_eq!(state.account_id.as_deref(), Some("primary"));
        assert_eq!(state.last_request_template.as_ref().unwrap(), &payload);
    }

    #[test]
    fn should_add_prompt_cache_key_to_first_websocket_template_when_account_has_seed() {
        let mut account = account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        );
        account.prompt_cache_key_seed = Some("stable-seed".to_string());
        let mut state = ReplayState::default();

        let payload = prepare_websocket_upstream_payload(
            &mut state,
            &account,
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.5",
                "input": [{"type": "message", "role": "user", "content": "hello"}]
            }),
        )
        .unwrap();

        assert_eq!(
            payload["prompt_cache_key"].as_str(),
            Some("tp_35de55d6cc39b7f4a248e35e6ed26116")
        );
        assert_eq!(state.last_request_template.as_ref().unwrap(), &payload);
    }

    #[test]
    fn should_preserve_websocket_prompt_cache_key_supplied_by_client() {
        let mut account = account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        );
        account.prompt_cache_key_seed = Some("stable-seed".to_string());
        let mut state = ReplayState::default();

        let payload = prepare_websocket_upstream_payload(
            &mut state,
            &account,
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.5",
                "prompt_cache_key": "client-key",
                "input": [{"type": "message", "role": "user", "content": "hello"}]
            }),
        )
        .unwrap();

        assert_eq!(payload["prompt_cache_key"].as_str(), Some("client-key"));
    }

    #[test]
    fn should_keep_prompt_cache_key_on_incremental_websocket_payload() {
        let mut account = account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        );
        account.prompt_cache_key_seed = Some("stable-seed".to_string());
        let mut state = ReplayState::default();
        prepare_websocket_upstream_payload(
            &mut state,
            &account,
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.5",
                "input": [{"type": "message", "role": "user", "content": "hello"}]
            }),
        )
        .unwrap();
        state.record_completed("resp_1".to_string(), vec![]);

        let payload = prepare_websocket_upstream_payload(
            &mut state,
            &account,
            serde_json::json!({
                "type": "response.create",
                "input": [{"type": "message", "role": "user", "content": "next"}]
            }),
        )
        .unwrap();

        assert_eq!(
            payload["prompt_cache_key"].as_str(),
            Some("tp_35de55d6cc39b7f4a248e35e6ed26116")
        );
    }

    #[test]
    fn should_route_followup_websocket_create_from_last_request_template() {
        let state = ReplayState {
            last_request_template: Some(serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.5",
                "service_tier": "priority",
                "input": [{"type": "message", "role": "user", "content": "hello"}]
            })),
            ..ReplayState::default()
        };

        let context = websocket_route_context(
            &state,
            &serde_json::json!({
                "type": "response.create",
                "input": [{"type": "function_call_output", "call_id": "call_1", "output": "ok"}]
            }),
        )
        .unwrap();

        assert_eq!(context.model, "gpt-5.5");
        assert_eq!(context.service_tier.as_deref(), Some("priority"));
        assert_eq!(context.model_family, "gpt-5");
    }

    #[test]
    fn should_mark_websocket_route_request_as_requiring_incremental_when_previous_response_id_is_present()
     {
        let state = ReplayState::default();
        let value = serde_json::json!({
            "type": "response.create",
            "model": "gpt-5.5",
            "previous_response_id": "resp_1",
            "input": [{"type": "message", "role": "user", "content": "next"}]
        });
        let context = websocket_route_context(&state, &value).unwrap();

        let request = websocket_route_request(&state, &context, &value);

        assert!(request.requires_incremental_previous_response_id);
    }

    #[test]
    fn should_prepare_incremental_websocket_payload_after_completed_response() {
        let account = account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        );
        let mut state = ReplayState::default();
        prepare_websocket_upstream_payload(
            &mut state,
            &account,
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.5",
                "input": [{"type": "message", "role": "user", "content": "hello"}]
            }),
        )
        .unwrap();
        state.record_completed(
            "resp_1".to_string(),
            vec![serde_json::json!({"type": "message", "phase": "final", "content": "hello"})],
        );

        let payload = prepare_websocket_upstream_payload(
            &mut state,
            &account,
            serde_json::json!({
                "type": "response.create",
                "input": [{"type": "function_call_output", "call_id": "call_1", "output": "ok"}]
            }),
        )
        .unwrap();

        assert_eq!(payload["previous_response_id"], "resp_1");
        assert_eq!(payload["input"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn should_prepare_full_replay_after_reconnected_websocket_loses_previous_state() {
        let account = account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        );
        let mut state = ReplayState::default();
        prepare_websocket_upstream_payload(
            &mut state,
            &account,
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.5",
                "input": [{"type": "message", "role": "user", "content": "hello"}]
            }),
        )
        .unwrap();
        state.record_completed(
            "resp_1".to_string(),
            vec![serde_json::json!({"type": "message", "phase": "final", "content": "hello"})],
        );

        let payload = prepare_websocket_upstream_payload_with_hash_key(
            &mut state,
            &account,
            "",
            serde_json::json!({
                "type": "response.create",
                "previous_response_id": "resp_1",
                "input": [{"type": "function_call_output", "call_id": "call_1", "output": "ok"}]
            }),
            false,
        )
        .unwrap();

        assert!(payload.get("previous_response_id").is_none());
        assert_eq!(payload["model"], "gpt-5.5");
        assert_eq!(payload["input"][1]["phase"], "final");
        assert_eq!(payload["input"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn should_prepare_full_replay_when_incremental_is_not_supported() {
        let mut account = account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        );
        account.config.supports_responses_ws = true;
        account.config.supports_incremental_previous_response_id = false;
        let mut state = ReplayState::default();
        prepare_websocket_upstream_payload(
            &mut state,
            &account,
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.5",
                "input": [{"type": "message", "role": "user", "content": "hello"}]
            }),
        )
        .unwrap();
        state.record_completed(
            "resp_1".to_string(),
            vec![serde_json::json!({"type": "message", "phase": "final", "content": "hello"})],
        );

        let payload = prepare_websocket_upstream_payload(
            &mut state,
            &account,
            serde_json::json!({
                "type": "response.create",
                "previous_response_id": "stale",
                "input": [{"type": "function_call_output", "call_id": "call_1", "output": "ok"}]
            }),
        )
        .unwrap();

        assert!(payload.get("previous_response_id").is_none());
        assert_eq!(payload["model"], "gpt-5.5");
        assert_eq!(payload["input"][1]["phase"], "final");
        assert_eq!(payload["input"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn should_advance_full_replay_template_after_non_incremental_turn() {
        let mut account = account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        );
        account.config.supports_responses_ws = true;
        account.config.supports_incremental_previous_response_id = false;
        let mut state = ReplayState::default();
        prepare_websocket_upstream_payload(
            &mut state,
            &account,
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.5",
                "input": [{"type": "message", "role": "user", "content": "hello"}]
            }),
        )
        .unwrap();
        state.record_completed(
            "resp_1".to_string(),
            vec![serde_json::json!({
                "type": "message",
                "phase": "final",
                "content": "first answer"
            })],
        );

        let first_replay = prepare_websocket_upstream_payload(
            &mut state,
            &account,
            serde_json::json!({
                "type": "response.create",
                "input": [{"type": "function_call_output", "call_id": "call_1", "output": "ok"}]
            }),
        )
        .unwrap();
        assert_eq!(state.last_request_template.as_ref().unwrap(), &first_replay);
        state.record_completed(
            "resp_2".to_string(),
            vec![serde_json::json!({
                "type": "message",
                "phase": "final",
                "content": "second answer"
            })],
        );

        let second_replay = prepare_websocket_upstream_payload(
            &mut state,
            &account,
            serde_json::json!({
                "type": "response.create",
                "input": [{"type": "message", "role": "user", "content": "next"}]
            }),
        )
        .unwrap();

        let input = second_replay["input"].as_array().unwrap();
        assert_eq!(input.len(), 5);
        assert_eq!(input[0]["content"], "hello");
        assert_eq!(input[1]["content"], "first answer");
        assert_eq!(input[2]["call_id"], "call_1");
        assert_eq!(input[3]["content"], "second answer");
        assert_eq!(input[4]["content"], "next");
    }

    #[test]
    fn should_capture_websocket_output_item_events_for_completed_replay_state() {
        let mut state = ReplayState::default();

        capture_completed_event(
            &mut state,
            &ws_event(
                r#"{"type":"response.output_item.done","item":{"type":"message","phase":"final","content":"hello","x_unknown":true}}"#,
            ),
        );
        capture_completed_event(
            &mut state,
            &ws_event(r#"{"type":"response.completed","response":{"id":"resp_1"}}"#),
        );

        assert_eq!(state.last_completed_response_id.as_deref(), Some("resp_1"));
        assert!(state.pending_output_items.is_empty());
        assert_eq!(state.last_completed_output_items.len(), 1);
        assert_eq!(state.last_completed_output_items[0]["phase"], "final");
        assert_eq!(state.last_completed_output_items[0]["x_unknown"], true);
    }

    #[test]
    fn should_prefer_completed_websocket_output_over_buffered_output_items() {
        let mut state = ReplayState::default();

        capture_completed_event(
            &mut state,
            &ws_event(
                r#"{"type":"response.output_item.done","item":{"type":"message","phase":"draft","content":"old"}}"#,
            ),
        );
        capture_completed_event(
            &mut state,
            &ws_event(
                r#"{"type":"response.completed","response":{"id":"resp_1","output":[{"type":"message","phase":"final","content":"new"}]}}"#,
            ),
        );

        assert!(state.pending_output_items.is_empty());
        assert_eq!(state.last_completed_output_items.len(), 1);
        assert_eq!(state.last_completed_output_items[0]["phase"], "final");
        assert_eq!(state.last_completed_output_items[0]["content"], "new");
    }

    #[test]
    fn should_reset_replay_state_when_compacted_window_is_used() {
        let account = account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        );
        let mut state = ReplayState::default();
        prepare_websocket_upstream_payload(
            &mut state,
            &account,
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.5",
                "input": [{"type": "message", "role": "user", "content": "old"}]
            }),
        )
        .unwrap();
        state.record_completed(
            "resp_old".to_string(),
            vec![serde_json::json!({"type": "message", "phase": "final", "content": "old"})],
        );

        let payload = prepare_websocket_upstream_payload(
            &mut state,
            &account,
            serde_json::json!({
                "type": "response.create",
                "model": "gpt-5.5",
                "input": [
                    {"type": "message", "role": "user", "content": "kept"},
                    {"type": "compaction", "encrypted_content": "gAAAAABpM0Yj"},
                    {"type": "message", "role": "user", "content": "next"}
                ]
            }),
        )
        .unwrap();

        assert!(payload.get("previous_response_id").is_none());
        assert_eq!(payload["input"].as_array().unwrap().len(), 3);
        assert!(state.last_completed_response_id.is_none());
        assert!(state.last_completed_output_items.is_empty());
        assert_eq!(state.last_request_template.as_ref().unwrap(), &payload);
    }

    fn prepare_websocket_upstream_payload(
        replay_state: &mut ReplayState,
        account: &EffectiveAccount,
        value: Value,
    ) -> Result<Value, TokenproxyError> {
        prepare_websocket_upstream_payload_with_hash_key(replay_state, account, "", value, true)
    }

    #[tokio::test]
    async fn should_open_one_upstream_websocket_for_reused_account_session() {
        let accepted_count = Arc::new(AtomicUsize::new(0));
        let upstream = fake_websocket_upstream(Arc::clone(&accepted_count)).await;
        let state = AppState::new(effective_config(vec![account(
            "primary",
            format!("http://{upstream}/v1"),
            "upstream-token",
            100,
        )]))
        .unwrap();
        let selected = state.effective.accounts.first().unwrap().clone();
        let mut session = None;

        ensure_upstream_session(&state, &selected, &mut session, "gpt", "req_ws_reuse")
            .await
            .unwrap();
        ensure_upstream_session(&state, &selected, &mut session, "gpt", "req_ws_reuse")
            .await
            .unwrap();
        sleep(Duration::from_millis(25)).await;

        assert_eq!(accepted_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn should_reconnect_expired_upstream_websocket_session() {
        let accepted_count = Arc::new(AtomicUsize::new(0));
        let upstream = fake_websocket_upstream(Arc::clone(&accepted_count)).await;
        let state = AppState::new(effective_config(vec![account(
            "primary",
            format!("http://{upstream}/v1"),
            "upstream-token",
            100,
        )]))
        .unwrap();
        let selected = state.effective.accounts.first().unwrap().clone();
        let mut session = None;

        ensure_upstream_session(&state, &selected, &mut session, "gpt", "req_ws_expired_1")
            .await
            .unwrap();
        session.as_mut().unwrap().opened_at = Instant::now() - UPSTREAM_WS_MAX_SESSION_AGE;
        ensure_upstream_session(&state, &selected, &mut session, "gpt", "req_ws_expired_2")
            .await
            .unwrap();
        sleep(Duration::from_millis(25)).await;

        assert_eq!(accepted_count.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn should_forward_generated_request_id_on_upstream_websocket_handshake() {
        let request_ids = Arc::new(StdMutex::new(Vec::new()));
        let upstream = fake_header_capture_websocket_upstream(Arc::clone(&request_ids)).await;
        let state = AppState::new(effective_config(vec![account(
            "primary",
            format!("http://{upstream}/v1"),
            "upstream-token",
            100,
        )]))
        .unwrap();
        let selected = state.effective.accounts.first().unwrap().clone();
        let mut session = None;

        ensure_upstream_session(
            &state,
            &selected,
            &mut session,
            "gpt",
            "req_0000000000000042",
        )
        .await
        .unwrap();
        sleep(Duration::from_millis(25)).await;

        assert_eq!(
            request_ids
                .lock()
                .expect("request id capture lock is not poisoned")
                .as_slice(),
            &[Some("req_0000000000000042".to_string())]
        );
    }

    #[tokio::test]
    async fn should_back_off_account_after_websocket_connect_failure() {
        let state = AppState::new(effective_config(vec![account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        )]))
        .unwrap();
        let selected = state.effective.accounts[0].clone();
        let mut session = None;

        let error =
            ensure_upstream_session(&state, &selected, &mut session, "gpt", "req_ws_failed")
                .await
                .expect_err("closed local port should fail websocket connect");

        assert_eq!(error.status, StatusCode::BAD_GATEWAY);
        let AccountHealth::Throttled { next_retry_at_ms } =
            state.account_health_cell("primary").unwrap().load()
        else {
            panic!("expected throttled health");
        };
        assert!(next_retry_at_ms > now_unix_ms());
    }

    #[test]
    fn should_reuse_persistent_upstream_websocket_for_same_account() {
        assert!(!should_replace_upstream_session(
            Some("primary"),
            "primary",
            Some(Duration::from_secs(30)),
            UPSTREAM_WS_MAX_SESSION_AGE
        ));
        assert!(should_replace_upstream_session(
            None,
            "primary",
            None,
            UPSTREAM_WS_MAX_SESSION_AGE
        ));
        assert!(should_replace_upstream_session(
            Some("primary"),
            "secondary",
            Some(Duration::from_secs(30)),
            UPSTREAM_WS_MAX_SESSION_AGE
        ));
        assert!(should_replace_upstream_session(
            Some("primary"),
            "primary",
            Some(UPSTREAM_WS_MAX_SESSION_AGE),
            UPSTREAM_WS_MAX_SESSION_AGE
        ));
    }

    #[test]
    fn should_mark_upstream_websocket_authorization_header_sensitive() {
        let header = upstream_authorization_header("upstream-token").unwrap();

        assert_eq!(header, "Bearer upstream-token");
        assert!(header.is_sensitive());
    }

    #[test]
    fn should_map_public_routes_to_chatgpt_codex_upstream_paths() {
        let mut chatgpt = account(
            "chatgpt",
            "https://chatgpt.com/backend-api/codex".to_string(),
            "upstream-token",
            100,
        );
        chatgpt.config.kind = AccountKind::ChatgptCodexAuthJson;

        let responses = upstream_url_for_path(&chatgpt, "/v1/responses").unwrap();
        let compact = upstream_url_for_path(&chatgpt, "/v1/responses/compact").unwrap();
        let websocket = websocket_upstream_url_for_account(&chatgpt).unwrap();

        assert_eq!(
            responses.as_str(),
            "https://chatgpt.com/backend-api/codex/responses"
        );
        assert_eq!(
            compact.as_str(),
            "https://chatgpt.com/backend-api/codex/responses/compact"
        );
        assert_eq!(
            websocket.as_str(),
            "wss://chatgpt.com/backend-api/codex/responses"
        );
    }

    #[test]
    fn should_keep_openai_api_key_upstream_paths_under_v1() {
        let openai = account(
            "openai",
            "https://api.openai.com/v1".to_string(),
            "upstream-token",
            100,
        );

        let responses = upstream_url_for_path(&openai, "/v1/responses").unwrap();
        let compact = upstream_url_for_path(&openai, "/v1/responses/compact").unwrap();
        let websocket = websocket_upstream_url_for_account(&openai).unwrap();

        assert_eq!(responses.as_str(), "https://api.openai.com/v1/responses");
        assert_eq!(
            compact.as_str(),
            "https://api.openai.com/v1/responses/compact"
        );
        assert_eq!(websocket.as_str(), "wss://api.openai.com/v1/responses");
        assert!(upstream_url_for_path(&openai, "/v1/messages").is_err());
    }

    #[test]
    fn should_keep_anthropic_api_key_upstream_messages_path_under_v1() {
        let anthropic = anthropic_account(
            "anthropic",
            "https://api.anthropic.com".to_string(),
            "upstream-token",
            100,
        );
        let versioned_anthropic = anthropic_account(
            "anthropic-versioned",
            "https://api.anthropic.com/v1".to_string(),
            "upstream-token",
            100,
        );

        let messages = upstream_url_for_path(&anthropic, "/v1/messages").unwrap();
        let versioned_messages =
            upstream_url_for_path(&versioned_anthropic, "/v1/messages").unwrap();

        assert_eq!(messages.as_str(), "https://api.anthropic.com/v1/messages");
        assert_eq!(
            versioned_messages.as_str(),
            "https://api.anthropic.com/v1/messages"
        );
        assert!(upstream_url_for_path(&anthropic, "/v1/responses").is_err());
    }

    #[test]
    fn should_extract_safe_http_log_context_from_upstream_headers() {
        let account = account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        );
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", "req_upstream".parse().unwrap());
        headers.insert("cf-ray", "ray-lax".parse().unwrap());
        headers.insert("authorization", "Bearer secret".parse().unwrap());

        let context = http_log_context(&account, &headers, "test-account-hash-key");

        assert_eq!(
            context.account_id_hash,
            account_id_hash("primary", "test-account-hash-key")
        );
        assert_eq!(context.upstream_request_id.as_deref(), Some("req_upstream"));
        assert_eq!(context.cloudflare_ray.as_deref(), Some("ray-lax"));
        assert!(!format!("{context:?}").contains("secret"));
    }

    #[test]
    fn should_extract_actual_service_tier_from_json_response_body() {
        assert_eq!(
            actual_service_tier_from_value(&ws_event(r#"{"id":"resp","service_tier":"default"}"#))
                .as_deref(),
            Some("default")
        );
        assert_eq!(
            actual_service_tier_from_value(&ws_event(r#"{"id":"resp","service_tier":""}"#)),
            None
        );
    }

    #[test]
    fn should_extract_cached_input_tokens_from_known_usage_shapes() {
        assert_eq!(
            usage_metadata_from_value(&ws_event(
                r#"{"usage":{"input_tokens_details":{"cached_tokens":17}}}"#
            ))
            .cached_input_tokens,
            Some(17)
        );
        assert_eq!(
            usage_metadata_from_value(&ws_event(
                r#"{"usage":{"prompt_tokens_details":{"cached_tokens":23}}}"#
            ))
            .cached_input_tokens,
            Some(23)
        );
        assert_eq!(
            usage_metadata_from_value(&ws_event(r#"{"usage":{}}"#)).cached_input_tokens,
            None
        );
    }

    #[test]
    fn should_extract_reasoning_tokens_from_responses_usage_shape() {
        assert_eq!(
            usage_metadata_from_value(&ws_event(
                r#"{"usage":{"output_tokens_details":{"reasoning_tokens":31}}}"#
            ))
            .reasoning_tokens,
            Some(31)
        );
        assert_eq!(
            usage_metadata_from_value(&ws_event(r#"{"usage":{"output_tokens_details":{}}}"#))
                .reasoning_tokens,
            None
        );
    }

    #[test]
    fn should_extract_usage_metadata_from_sse_completed_frame() {
        let frames = vec![Bytes::from_static(
        br#"event: response.completed
data: {"type":"response.completed","response":{"id":"resp_1","usage":{"input_tokens_details":{"cached_tokens":17},"output_tokens_details":{"reasoning_tokens":31}}}}

"#,
    )];

        assert_eq!(
            usage_metadata_from_sse_frames(&frames),
            UsageMetadata {
                cached_input_tokens: Some(17),
                reasoning_tokens: Some(31),
            }
        );
    }

    #[test]
    fn should_record_websocket_usage_metadata_and_service_tiers_for_log() {
        let mut replay_state = ReplayState::default();
        let metrics = Metrics::default();

        replay_state.requested_service_tier = Some(normalize_service_tier("fast").to_string());
        record_websocket_actual_service_tier(
            &mut replay_state,
            &ws_event(
                r#"{"type":"response.created","response":{"id":"resp_1","service_tier":"default"}}"#,
            ),
        );
        record_websocket_usage_metadata(
            &metrics,
            &mut replay_state,
            "gpt",
            &ws_event(
                r#"{"type":"response.completed","response":{"id":"resp_1","usage":{"prompt_tokens_details":{"cached_tokens":17},"output_tokens_details":{"reasoning_tokens":31}}}}"#,
            ),
        );
        record_websocket_actual_service_tier(
            &mut replay_state,
            &ws_event(r#"{"type":"response.output_text.delta","delta":"ignored"}"#),
        );

        assert_eq!(
            replay_state.requested_service_tier.as_deref(),
            Some("priority")
        );
        assert_eq!(replay_state.actual_service_tier.as_deref(), Some("default"));
        assert_eq!(replay_state.cached_input_tokens, Some(17));
        assert_eq!(replay_state.reasoning_tokens, Some(31));
        assert_eq!(
            metrics.snapshot().cached_input_tokens,
            vec![(
                crate::metrics::CachedInputTokensMetricKey {
                    endpoint: "/v1/responses".to_string(),
                    model_family: "gpt".to_string(),
                },
                17,
            )]
        );
    }

    #[test]
    fn should_record_websocket_request_shape_fields_for_log_and_metrics() {
        let mut replay_state = ReplayState {
            last_request_template: Some(serde_json::json!({
                "service_tier": "default",
                "reasoning": {"effort": "medium"},
                "text": {"verbosity": "high"},
                "store": true
            })),
            ..ReplayState::default()
        };
        let metrics = Metrics::default();
        let shape = websocket_request_shape(
            &replay_state,
            &serde_json::json!({
                "type": "response.create",
                "service_tier": "fast",
                "reasoning": {"effort": "high"},
                "store": false,
                "input": []
            }),
        );

        record_websocket_request_shape(&metrics, &mut replay_state, "gpt", &shape);

        assert_eq!(
            replay_state.requested_service_tier.as_deref(),
            Some("priority")
        );
        assert_eq!(replay_state.reasoning_effort.as_deref(), Some("high"));
        assert_eq!(replay_state.verbosity.as_deref(), Some("high"));
        assert_eq!(replay_state.store.as_deref(), Some("false"));
        assert_eq!(
            metrics.snapshot().request_shapes,
            vec![(
                crate::metrics::RequestShapeMetricKey {
                    endpoint: "/v1/responses".to_string(),
                    model_family: "gpt".to_string(),
                    service_tier: "priority".to_string(),
                    reasoning_effort: "high".to_string(),
                    verbosity: "high".to_string(),
                    store: "false".to_string(),
                },
                1
            )]
        );
    }

    #[test]
    fn should_extract_actual_service_tier_from_sse_response_created_frame() {
        let frames = vec![Bytes::from_static(
            br#"event: response.created
data: {"type":"response.created","response":{"id":"resp_1","service_tier":"default"}}

"#,
        )];

        assert_eq!(
            actual_service_tier_from_sse_frames(&frames).as_deref(),
            Some("default")
        );
    }

    fn record_sse_response_event_metrics(metrics: &Metrics, frames: &[Bytes]) {
        record_sse_response_event_metrics_labeled(metrics, frames, "unknown", "unknown");
    }

    #[test]
    fn should_record_websocket_sessions_as_request_metrics() {
        let metrics = Metrics::default();

        record_websocket_request_metrics(
            &metrics,
            StatusCode::SERVICE_UNAVAILABLE,
            37,
            Some("gpt"),
            Some("acct_hash"),
        );

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.requests_total, 1);
        assert_eq!(
            snapshot.request_outcomes,
            vec![(
                crate::metrics::RequestMetricKey {
                    endpoint: "/v1/responses".to_string(),
                    transport: "websocket".to_string(),
                    status_class: "5xx".to_string(),
                    model_family: "gpt".to_string(),
                    account_id_hash: "acct_hash".to_string(),
                },
                1,
            )]
        );
        assert_eq!(
            snapshot.request_duration_labels[0].0,
            crate::metrics::RequestDurationMetricKey {
                endpoint: "/v1/responses".to_string(),
                transport: "websocket".to_string(),
                model_family: "gpt".to_string(),
                stream: "true".to_string(),
            }
        );
        assert_eq!(snapshot.request_duration_labels[0].1.count, 1);
        assert_eq!(snapshot.request_duration_labels[0].1.sum_ms, 37);
    }

    #[test]
    fn should_record_bounded_upstream_websocket_response_event_metrics() {
        let metrics = Metrics::default();

        record_upstream_websocket_response_event_metric(
            &metrics,
            &ws_event(r#"{"type":"response.completed","response":{"id":"resp_1"}}"#),
        );
        record_upstream_websocket_response_event_metric(
            &metrics,
            &ws_event(r#"{"type":"response.output_text.delta","delta":"hello"}"#),
        );
        record_upstream_websocket_response_event_metric(
            &metrics,
            &ws_event(r#"{"type":"error","error":{"code":"usage_limit_reached"}}"#),
        );
        record_upstream_websocket_response_event_metric(
            &metrics,
            &ws_event(r#"{"type":"response.custom.future_event"}"#),
        );

        let snapshot = metrics.snapshot();

        assert_eq!(
            snapshot.websocket_event_outcomes,
            vec![
                (
                    crate::metrics::WebSocketEventMetricKey {
                        event_type: "error".to_string(),
                        success: false,
                    },
                    1,
                ),
                (
                    crate::metrics::WebSocketEventMetricKey {
                        event_type: "response.completed".to_string(),
                        success: true,
                    },
                    1,
                ),
                (
                    crate::metrics::WebSocketEventMetricKey {
                        event_type: "response.other".to_string(),
                        success: true,
                    },
                    1,
                ),
                (
                    crate::metrics::WebSocketEventMetricKey {
                        event_type: "response.output_text.delta".to_string(),
                        success: true,
                    },
                    1,
                ),
            ]
        );
    }

    #[tokio::test]
    async fn should_record_downstream_backpressure_when_websocket_send_waits_too_long() {
        let metrics = Metrics::default();

        let error = send_downstream_with_backpressure(
            &metrics,
            std::future::pending::<Result<(), std::io::Error>>(),
            Duration::from_millis(10),
        )
        .await
        .unwrap_err();

        assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error.code, ErrorCode::UpstreamFailure);
        assert!(
            error
                .message
                .contains("downstream WebSocket send backpressure")
        );
        assert_eq!(
            metrics.snapshot().websocket_event_outcomes,
            vec![(
                crate::metrics::WebSocketEventMetricKey {
                    event_type: "downstream_backpressure".to_string(),
                    success: false,
                },
                1,
            )]
        );
    }

    #[tokio::test]
    async fn should_signal_websocket_session_event_overflow() {
        let metrics = Metrics::default();
        let (events_tx, mut events_rx) = mpsc::channel(1);
        let (overflow_tx, overflow_rx) = watch::channel(false);

        assert!(try_enqueue_downstream_session_event(
            &events_tx,
            &overflow_tx,
            &metrics,
            DownstreamSessionEvent::Message(DownstreamMessage::Ping(vec![1].into())),
        ));
        assert!(!try_enqueue_downstream_session_event(
            &events_tx,
            &overflow_tx,
            &metrics,
            DownstreamSessionEvent::Message(DownstreamMessage::Ping(vec![2].into())),
        ));

        assert!(*overflow_rx.borrow());
        assert!(matches!(
            events_rx.recv().await,
            Some(DownstreamSessionEvent::Message(_))
        ));
        assert_eq!(
            metrics.snapshot().websocket_event_outcomes,
            vec![(
                crate::metrics::WebSocketEventMetricKey {
                    event_type: "downstream_event_overflow".to_string(),
                    success: false,
                },
                1,
            )]
        );
    }

    fn repair_sse_stream<S, E>(
        stream: S,
    ) -> impl futures_util::Stream<Item = Result<Bytes, BoxStreamError>>
    where
        S: futures_util::Stream<Item = Result<Bytes, E>> + Send + 'static,
        E: Error + Send + Sync + 'static,
    {
        repair_sse_stream_with_idle_timeout(stream, Duration::from_secs(300))
    }

    fn repair_sse_stream_with_idle_timeout<S, E>(
        stream: S,
        idle_timeout: Duration,
    ) -> impl futures_util::Stream<Item = Result<Bytes, BoxStreamError>>
    where
        S: futures_util::Stream<Item = Result<Bytes, E>> + Send + 'static,
        E: Error + Send + Sync + 'static,
    {
        let (stream, pending_bytes) = bounded_eventsource_stream(stream);
        repair_sse_stream_from_state(SseStreamState {
            stream,
            repair: SseRepair::default(),
            pending: VecDeque::new(),
            finished: false,
            idle_timeout,
            cancellation: SseClientCancellation::new(None, SseMetricContext::unknown()),
            pending_bytes,
        })
    }

    #[test]
    fn should_record_bounded_sse_response_event_metrics() {
        let metrics = Metrics::default();
        let frames = vec![
            Bytes::from_static(
                br#"event: response.completed
data: {"type":"response.completed","response":{"id":"resp_1"}}

"#,
            ),
            Bytes::from_static(
                br#"event: response.output_text.delta
data: {"type":"response.output_text.delta","delta":"hello"}

"#,
            ),
            Bytes::from_static(
                br#"data: {"type":"response.failed","response":{"id":"resp_2"}}

"#,
            ),
            Bytes::from_static(
                br#"event: response.custom.future_event
data: {"type":"response.custom.future_event"}

"#,
            ),
        ];

        record_sse_response_event_metrics(&metrics, &frames);

        let snapshot = metrics.snapshot();
        assert_eq!(
            snapshot.sse_event_outcomes,
            vec![
                (
                    crate::metrics::SseEventMetricKey {
                        event_type: "response.completed".to_string(),
                        success: true,
                        model_family: "unknown".to_string(),
                        account_id_hash: "unknown".to_string(),
                    },
                    1,
                ),
                (
                    crate::metrics::SseEventMetricKey {
                        event_type: "response.failed".to_string(),
                        success: false,
                        model_family: "unknown".to_string(),
                        account_id_hash: "unknown".to_string(),
                    },
                    1,
                ),
                (
                    crate::metrics::SseEventMetricKey {
                        event_type: "response.other".to_string(),
                        success: true,
                        model_family: "unknown".to_string(),
                        account_id_hash: "unknown".to_string(),
                    },
                    1,
                ),
                (
                    crate::metrics::SseEventMetricKey {
                        event_type: "response.output_text.delta".to_string(),
                        success: true,
                        model_family: "unknown".to_string(),
                        account_id_hash: "unknown".to_string(),
                    },
                    1,
                ),
            ]
        );
    }

    #[tokio::test]
    async fn should_timeout_waiting_for_first_sse_event() {
        let stream = futures_util::stream::pending::<Result<Bytes, std::io::Error>>();
        let metrics = Metrics::default();

        let error = sse_response_after_first_event(SseFirstEvent {
            status: StatusCode::OK,
            headers: HeaderMap::new(),
            stream,
            metrics: &metrics,
            endpoint: "/v1/responses",
            model_family: "gpt",
            account_id_hash: "acct_primary",
            started: Instant::now(),
            idle_timeout: Duration::from_millis(10),
        })
        .await
        .unwrap_err();

        assert_eq!(error.status, StatusCode::GATEWAY_TIMEOUT);
        assert_eq!(error.code, ErrorCode::UpstreamFailure);
        assert!(error.message.contains("SSE idle timeout"));
    }

    #[tokio::test]
    async fn should_stop_repaired_sse_body_on_idle_timeout() {
        let stream = futures_util::stream::pending::<Result<Bytes, std::io::Error>>();

        let repaired = repair_sse_stream_with_idle_timeout(stream, Duration::from_millis(10));
        futures_util::pin_mut!(repaired);
        let item = repaired.next().await.unwrap().unwrap_err();

        assert!(item.to_string().contains("SSE idle timeout"));
    }

    #[tokio::test]
    async fn should_fail_when_sse_pending_buffer_exceeds_cap_without_frame_boundary() {
        let chunk = Bytes::from(vec![b'a'; 1024 * 1024]);
        let stream = futures_util::stream::iter(
            (0..17).map(move |_| Ok::<Bytes, std::io::Error>(chunk.clone())),
        );

        let repaired = repair_sse_stream(stream);
        futures_util::pin_mut!(repaired);
        let error = repaired.next().await.unwrap().unwrap_err();

        assert!(
            error
                .to_string()
                .contains("SSE stream exceeded frame buffer limit")
        );
    }

    #[tokio::test]
    async fn should_record_sse_client_cancellation_when_repaired_body_is_dropped() {
        let metrics = Metrics::default();
        let stream = futures_util::stream::pending::<Result<Bytes, std::io::Error>>();
        let (stream, pending_bytes) = bounded_eventsource_stream(stream);
        let mut repaired = Box::pin(repair_sse_stream_from_state(SseStreamState {
            stream,
            repair: SseRepair::default(),
            pending: VecDeque::from([Bytes::from_static(b"event: response.created\n\n")]),
            finished: false,
            idle_timeout: Duration::from_secs(300),
            cancellation: SseClientCancellation::new(
                Some(metrics.clone()),
                SseMetricContext::new("gpt-5", "acct_primary"),
            ),
            pending_bytes,
        }));

        assert_eq!(
            repaired.as_mut().next().await.unwrap().unwrap(),
            Bytes::from_static(b"event: response.created\n\n")
        );
        drop(repaired);

        assert_eq!(
            metrics.snapshot().sse_event_outcomes,
            vec![(
                crate::metrics::SseEventMetricKey {
                    event_type: "client_cancelled".to_string(),
                    success: true,
                    model_family: "gpt-5".to_string(),
                    account_id_hash: "acct_primary".to_string(),
                },
                1,
            )]
        );
    }

    #[tokio::test]
    async fn should_record_sse_upstream_error_after_commit_with_route_labels() {
        let metrics = Metrics::default();
        let stream = futures_util::stream::iter(vec![Err::<Bytes, _>(std::io::Error::other(
            "upstream reset",
        ))]);
        let (stream, pending_bytes) = bounded_eventsource_stream(stream);
        let mut repaired = Box::pin(repair_sse_stream_from_state(SseStreamState {
            stream,
            repair: SseRepair::default(),
            pending: VecDeque::from([Bytes::from_static(b"event: response.created\n\n")]),
            finished: false,
            idle_timeout: Duration::from_secs(300),
            cancellation: SseClientCancellation::new(
                Some(metrics.clone()),
                SseMetricContext::new("gpt-5", "acct_primary"),
            ),
            pending_bytes,
        }));

        assert_eq!(
            repaired.as_mut().next().await.unwrap().unwrap(),
            Bytes::from_static(b"event: response.created\n\n")
        );
        let error = repaired.as_mut().next().await.unwrap().unwrap_err();
        assert!(error.to_string().contains("upstream reset"));

        assert_eq!(
            metrics.snapshot().sse_event_outcomes,
            vec![(
                crate::metrics::SseEventMetricKey {
                    event_type: "upstream_stream_error".to_string(),
                    success: false,
                    model_family: "gpt-5".to_string(),
                    account_id_hash: "acct_primary".to_string(),
                },
                1,
            )]
        );
    }

    #[tokio::test]
    async fn should_repair_sse_stream_chunks_before_downstream_body() {
        let stream = futures_util::stream::iter(vec![
        Ok::<_, std::io::Error>(Bytes::from_static(
            br#"data: {"type":"response.output_item.done","item":{"type":"message","phase":"final"}}"#,
        )),
        Ok(Bytes::from_static(b"\n\n")),
        Ok(Bytes::from_static(
            br#"data: {"type":"response.completed","response":{"id":"resp_1"}}"#,
        )),
        Ok(Bytes::from_static(b"\n\n")),
    ]);

        let chunks = repair_sse_stream(stream)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let joined = chunks
            .iter()
            .map(|bytes| std::str::from_utf8(bytes).unwrap())
            .collect::<String>();

        assert!(joined.contains(r#""response":{"id":"resp_1","output":[{"phase":"final""#));
    }

    #[tokio::test]
    async fn should_not_flush_partial_sse_frame_when_upstream_ends_after_commit() {
        let stream = futures_util::stream::iter(vec![
            Ok::<_, std::io::Error>(Bytes::from_static(br#"data: {"type":"response.created"}"#)),
            Ok(Bytes::from_static(b"\n\n")),
            Ok(Bytes::from_static(
                br#"data: {"type":"response.completed","response":{"id":"resp_1"}}"#,
            )),
        ]);

        let chunks = repair_sse_stream(stream)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(
            chunks,
            vec![Bytes::from_static(
                br#"data: {"type":"response.created"}

"#
            )]
        );
    }

    #[test]
    fn should_add_stable_prompt_cache_key_to_responses_when_account_has_seed() {
        let mut primary = account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        );
        primary.prompt_cache_key_seed = Some("stable-seed".to_string());
        let request = RouteRequest {
            endpoint: Endpoint::Responses,
            transport: Transport::Http,
            model: "gpt-5.5".to_string(),
            service_tier: None,
            pinned_account_id: None,
            requires_incremental_previous_response_id: false,
            model_family: "gpt-5".to_string(),
            stream: false,
        };

        let body = body_with_request_transforms(
            &Bytes::from_static(br#"{"model":"gpt-5.5","input":[]}"#),
            &request,
            &primary,
        )
        .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(
            body["prompt_cache_key"].as_str(),
            Some("tp_35de55d6cc39b7f4a248e35e6ed26116")
        );
    }

    #[test]
    fn should_preserve_client_supplied_prompt_cache_key() {
        let mut primary = account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        );
        primary.prompt_cache_key_seed = Some("stable-seed".to_string());
        let request = RouteRequest {
            endpoint: Endpoint::Responses,
            transport: Transport::Http,
            model: "gpt-5.5".to_string(),
            service_tier: None,
            pinned_account_id: None,
            requires_incremental_previous_response_id: false,
            model_family: "gpt-5".to_string(),
            stream: false,
        };

        let body = body_with_request_transforms(
            &Bytes::from_static(
                br#"{"model":"gpt-5.5","prompt_cache_key":"client-key","input":[]}"#,
            ),
            &request,
            &primary,
        )
        .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(body["prompt_cache_key"].as_str(), Some("client-key"));
    }

    #[test]
    fn should_normalize_legacy_fast_service_tier_before_http_upstream_forwarding() {
        let account = account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        );
        let request = RouteRequest {
            endpoint: Endpoint::ChatCompletions,
            transport: Transport::Http,
            model: "gpt-5.5".to_string(),
            service_tier: Some("fast".to_string()),
            pinned_account_id: None,
            requires_incremental_previous_response_id: false,
            model_family: "gpt-5".to_string(),
            stream: false,
        };

        let body = body_with_request_transforms(
            &Bytes::from_static(br#"{"model":"gpt-5.5","service_tier":"fast"}"#),
            &request,
            &account,
        )
        .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(body["service_tier"].as_str(), Some("priority"));
        assert!(body.get("prompt_cache_key").is_none());
    }

    #[test]
    fn should_identify_only_precommit_retry_statuses() {
        assert!(should_retry_precommit(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(should_retry_precommit(StatusCode::SERVICE_UNAVAILABLE));
        assert!(should_retry_precommit(StatusCode::TOO_MANY_REQUESTS));
        assert!(!should_retry_precommit(StatusCode::OK));
    }

    #[test]
    fn should_not_retry_sse_response_after_first_event_is_observed() {
        let mut response = Response::builder()
            .status(StatusCode::SERVICE_UNAVAILABLE)
            .body(Body::empty())
            .unwrap();
        response.extensions_mut().insert(SseFirstFrameObserved);

        assert!(!should_retry_precommit_response(&response));
    }

    #[test]
    fn should_reject_only_compressed_request_content_encoding() {
        let mut identity = HeaderMap::new();
        identity.insert("content-encoding", "identity".parse().unwrap());
        assert!(reject_compressed_body(&identity).is_ok());

        let mut compressed = HeaderMap::new();
        compressed.insert("content-encoding", "gzip".parse().unwrap());
        let error = reject_compressed_body(&compressed).unwrap_err();
        assert_eq!(error.status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(error.code, ErrorCode::UnsupportedMediaType);

        let mut layered = HeaderMap::new();
        layered.insert("content-encoding", "identity, br".parse().unwrap());
        assert_eq!(
            reject_compressed_body(&layered).unwrap_err().code,
            ErrorCode::UnsupportedMediaType
        );
    }

    #[test]
    fn should_compare_downstream_bearer_tokens_by_content_and_length() {
        assert!(constant_time_eq(b"Bearer client-key", b"Bearer client-key"));
        assert!(!constant_time_eq(
            b"Bearer client-key",
            b"Bearer client-kez"
        ));
        assert!(!constant_time_eq(
            b"Bearer client-key",
            b"Bearer client-key-extra"
        ));
        assert!(!constant_time_eq(
            b"Bearer client-key-extra",
            b"Bearer client-key"
        ));
    }

    #[tokio::test]
    async fn should_select_next_account_after_attempted_account_is_excluded() {
        let state = AppState::new(effective_config(vec![
            account(
                "first",
                "http://127.0.0.1:1/v1".to_string(),
                "first-token",
                100,
            ),
            account(
                "second",
                "http://127.0.0.1:2/v1".to_string(),
                "second-token",
                90,
            ),
        ]))
        .unwrap();
        let request = RouteRequest {
            endpoint: Endpoint::Responses,
            transport: Transport::Http,
            model: "gpt-5.5".to_string(),
            service_tier: None,
            pinned_account_id: None,
            requires_incremental_previous_response_id: false,
            model_family: "gpt-5".to_string(),
            stream: false,
        };

        let selected = select_next_account(&state, &request, &["first".to_string()])
            .await
            .unwrap();

        assert_eq!(selected.config.id, "second");
    }

    #[tokio::test]
    async fn should_use_observed_latency_buckets_when_selecting_equal_priority_accounts() {
        let state = AppState::new(effective_config(vec![
            account(
                "slow",
                "http://127.0.0.1:1/v1".to_string(),
                "slow-token",
                100,
            ),
            account(
                "fast",
                "http://127.0.0.1:2/v1".to_string(),
                "fast-token",
                100,
            ),
        ]))
        .unwrap();
        state
            .account_health_cell("slow")
            .unwrap()
            .record_connect_duration_ms(1_000);
        state
            .account_health_cell("slow")
            .unwrap()
            .record_first_event_duration_ms(2_000);
        state
            .account_health_cell("fast")
            .unwrap()
            .record_connect_duration_ms(25);
        state
            .account_health_cell("fast")
            .unwrap()
            .record_first_event_duration_ms(50);
        let request = RouteRequest {
            endpoint: Endpoint::Responses,
            transport: Transport::Http,
            model: "gpt-5.5".to_string(),
            service_tier: None,
            pinned_account_id: None,
            requires_incremental_previous_response_id: false,
            model_family: "gpt-5".to_string(),
            stream: false,
        };

        let selected = select_next_account(&state, &request, &[]).await.unwrap();

        assert_eq!(selected.config.id, "fast");
    }

    #[tokio::test]
    async fn should_record_route_exclusion_metrics_by_reason() {
        let mut wrong_model = account(
            "wrong-model",
            "http://127.0.0.1:1/v1".to_string(),
            "wrong-token",
            100,
        );
        wrong_model.config.models = vec!["gpt-4.1".to_string()];
        let state = AppState::new(effective_config(vec![
            wrong_model,
            account(
                "selected",
                "http://127.0.0.1:2/v1".to_string(),
                "selected-token",
                90,
            ),
        ]))
        .unwrap();
        let request = RouteRequest {
            endpoint: Endpoint::Responses,
            transport: Transport::Http,
            model: "gpt-5.5".to_string(),
            service_tier: None,
            pinned_account_id: None,
            requires_incremental_previous_response_id: false,
            model_family: "gpt-5".to_string(),
            stream: false,
        };

        let selected = select_next_account(&state, &request, &[]).await.unwrap();

        assert_eq!(selected.config.id, "selected");
        assert_eq!(
            state.metrics.snapshot().route_exclusions,
            vec![(
                crate::metrics::RouteExclusionMetricKey {
                    reason: "model_unsupported".to_string(),
                },
                1,
            )]
        );
    }

    #[tokio::test]
    async fn should_route_websocket_previous_response_id_to_incremental_account() {
        let mut non_incremental = account(
            "non-incremental",
            "http://127.0.0.1:1/v1".to_string(),
            "first-token",
            100,
        );
        non_incremental
            .config
            .supports_incremental_previous_response_id = false;
        let incremental = account(
            "incremental",
            "http://127.0.0.1:2/v1".to_string(),
            "second-token",
            90,
        );
        let state = AppState::new(effective_config(vec![non_incremental, incremental])).unwrap();
        let request = RouteRequest {
            endpoint: Endpoint::Responses,
            transport: Transport::WebSocket,
            model: "gpt-5.5".to_string(),
            service_tier: None,
            pinned_account_id: None,
            requires_incremental_previous_response_id: true,
            model_family: "gpt-5".to_string(),
            stream: true,
        };

        let selected = select_next_account(&state, &request, &[]).await.unwrap();

        assert_eq!(selected.config.id, "incremental");
        assert_eq!(
            state.metrics.snapshot().route_exclusions,
            vec![(
                crate::metrics::RouteExclusionMetricKey {
                    reason: "websocket_continuation_unsupported".to_string(),
                },
                1,
            )]
        );
    }

    #[tokio::test]
    async fn should_exclude_usage_limited_accounts_from_selection() {
        let state = AppState::new(effective_config(vec![
            account(
                "first",
                "http://127.0.0.1:1/v1".to_string(),
                "first-token",
                100,
            ),
            account(
                "second",
                "http://127.0.0.1:2/v1".to_string(),
                "second-token",
                90,
            ),
        ]))
        .unwrap();
        state.usage_windows.lock().await.insert(
            "first".to_string(),
            vec![UsageWindow {
                window: "codex_usage_limit".to_string(),
                limit: None,
                remaining: Some(0),
                remaining_percent: None,
                rate_limit_pressure: "limited".to_string(),
                reset_after: Some("60s".to_string()),
                reset_at: Some("2999-01-01T00:00:00Z".to_string()),
                source: "usage_limit_reached_error".to_string(),
                observed_at: "2026-05-27T11:24:18Z".to_string(),
                limited: true,
            }],
        );
        let request = RouteRequest {
            endpoint: Endpoint::Responses,
            transport: Transport::Http,
            model: "gpt-5.5".to_string(),
            service_tier: None,
            pinned_account_id: None,
            requires_incremental_previous_response_id: false,
            model_family: "gpt-5".to_string(),
            stream: false,
        };

        let selected = select_next_account(&state, &request, &[]).await.unwrap();

        assert_eq!(selected.config.id, "second");
    }

    #[tokio::test]
    async fn should_exclude_auth_failed_runtime_accounts_from_selection() {
        let state = AppState::new(effective_config(vec![
            account(
                "first",
                "http://127.0.0.1:1/v1".to_string(),
                "first-token",
                100,
            ),
            account(
                "second",
                "http://127.0.0.1:2/v1".to_string(),
                "second-token",
                90,
            ),
        ]))
        .unwrap();
        state.store_account_health("first", AccountHealth::AuthFailed);
        let request = RouteRequest {
            endpoint: Endpoint::Responses,
            transport: Transport::Http,
            model: "gpt-5.5".to_string(),
            service_tier: None,
            pinned_account_id: None,
            requires_incremental_previous_response_id: false,
            model_family: "gpt-5".to_string(),
            stream: false,
        };

        let selected = select_next_account(&state, &request, &[]).await.unwrap();

        assert_eq!(selected.config.id, "second");
    }

    #[test]
    fn should_store_runtime_health_in_per_account_atomic_cells() {
        let state = AppState::new(effective_config(vec![account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        )]))
        .unwrap();

        assert_eq!(
            state.account_health_cell("primary").unwrap().load(),
            AccountHealth::Open
        );

        state.store_account_health(
            "primary",
            AccountHealth::Throttled {
                next_retry_at_ms: 42,
            },
        );
        assert_eq!(
            state.account_health_snapshot().get("primary"),
            Some(&AccountHealth::Throttled {
                next_retry_at_ms: 42
            })
        );

        state.clear_account_health_if_not_auth_failed("primary");
        assert_eq!(
            state.account_health_cell("primary").unwrap().load(),
            AccountHealth::Open
        );

        state.store_account_health("primary", AccountHealth::AuthFailed);
        state.clear_account_health_if_not_auth_failed("primary");
        assert_eq!(
            state.account_health_cell("primary").unwrap().load(),
            AccountHealth::AuthFailed
        );
        state.clear_account_health("primary");
        assert_eq!(
            state.account_health_cell("primary").unwrap().load(),
            AccountHealth::Open
        );
    }

    #[tokio::test]
    async fn should_record_and_clear_runtime_http_account_health() {
        let state = AppState::new(effective_config(vec![account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        )]))
        .unwrap();
        let selected = state.effective.accounts[0].clone();

        record_account_http_status(
            &state,
            &selected,
            StatusCode::UNAUTHORIZED,
            &HeaderMap::new(),
            None,
        )
        .await;
        assert_eq!(
            state.account_health_cell("primary").unwrap().load(),
            AccountHealth::AuthFailed
        );

        record_account_http_status(&state, &selected, StatusCode::OK, &HeaderMap::new(), None)
            .await;
        assert_eq!(
            state.account_health_cell("primary").unwrap().load(),
            AccountHealth::AuthFailed
        );
    }

    #[tokio::test]
    async fn should_clear_transient_throttle_after_successful_http_response() {
        let state = AppState::new(effective_config(vec![account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        )]))
        .unwrap();
        let selected = state.effective.accounts[0].clone();
        state.store_account_health(
            "primary",
            AccountHealth::Throttled {
                next_retry_at_ms: 1_000,
            },
        );

        record_account_http_status(&state, &selected, StatusCode::OK, &HeaderMap::new(), None)
            .await;

        assert_eq!(
            state.account_health_cell("primary").unwrap().load(),
            AccountHealth::Open
        );
    }

    #[tokio::test]
    async fn should_clear_transient_throttle_after_successful_websocket_event() {
        let state = AppState::new(effective_config(vec![account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        )]))
        .unwrap();
        let selected = state.effective.accounts[0].clone();
        state.store_account_health(
            "primary",
            AccountHealth::Throttled {
                next_retry_at_ms: 1_000,
            },
        );

        record_account_websocket_event_health(
            &state,
            &selected,
            &ws_event(r#"{"type":"response.output_item.done","item":{"id":"item_1"}}"#),
        );

        assert_eq!(
            state.account_health_cell("primary").unwrap().load(),
            AccountHealth::Open
        );
    }

    #[tokio::test]
    async fn should_not_clear_transient_throttle_after_previous_response_not_found_websocket_event()
    {
        let state = AppState::new(effective_config(vec![account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        )]))
        .unwrap();
        let selected = state.effective.accounts[0].clone();
        state.store_account_health(
            "primary",
            AccountHealth::Throttled {
                next_retry_at_ms: 1_000,
            },
        );

        record_account_websocket_event_health(
            &state,
            &selected,
            &ws_event(r#"{"type":"error","error":{"code":"previous_response_not_found"}}"#),
        );

        assert_eq!(
            state.account_health_cell("primary").unwrap().load(),
            AccountHealth::Throttled {
                next_retry_at_ms: 1_000
            }
        );
    }

    #[tokio::test]
    async fn should_record_usage_limit_error_from_websocket_event() {
        let state = AppState::new(effective_config(vec![account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        )]))
        .unwrap();
        let selected = state.effective.accounts[0].clone();

        record_websocket_usage_limit_error_event(
        &state,
        &selected,
        &ws_event(r#"{"type":"error","error":{"code":"usage_limit_reached","resets_at":"2026-05-27T15:07:00Z"}}"#),
    )
    .await;

        assert_eq!(
            state.account_health_cell("primary").unwrap().load(),
            AccountHealth::UsageLimited {
                reset_at_ms: chrono::DateTime::parse_from_rfc3339("2026-05-27T15:07:00Z")
                    .unwrap()
                    .timestamp_millis() as u64
            }
        );
        let usage_windows = state.usage_windows.lock().await;
        let windows = usage_windows.get("primary").unwrap();
        assert_eq!(windows[0].window, "codex_usage_limit");
        assert_eq!(windows[0].source, "usage_limit_reached_error");
        assert_eq!(windows[0].reset_at.as_deref(), Some("2026-05-27T15:07:00Z"));
    }

    #[tokio::test]
    async fn should_back_off_account_after_transient_http_failure() {
        let state = AppState::new(effective_config(vec![account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        )]))
        .unwrap();
        let selected = state.effective.accounts[0].clone();

        record_account_http_status(
            &state,
            &selected,
            StatusCode::SERVICE_UNAVAILABLE,
            &HeaderMap::new(),
            None,
        )
        .await;

        let AccountHealth::Throttled { next_retry_at_ms } =
            state.account_health_cell("primary").unwrap().load()
        else {
            panic!("expected throttled health");
        };
        assert!(next_retry_at_ms > now_unix_ms());
    }

    #[tokio::test]
    async fn should_count_repeated_transient_failures_until_success() {
        let state = AppState::new(effective_config(vec![account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        )]))
        .unwrap();
        let selected = state.effective.accounts[0].clone();

        record_account_http_status(
            &state,
            &selected,
            StatusCode::SERVICE_UNAVAILABLE,
            &HeaderMap::new(),
            None,
        )
        .await;
        record_account_http_status(
            &state,
            &selected,
            StatusCode::INTERNAL_SERVER_ERROR,
            &HeaderMap::new(),
            None,
        )
        .await;

        assert_eq!(
            state
                .account_health_cell("primary")
                .unwrap()
                .transient_failure_count(),
            2
        );

        record_account_http_status(&state, &selected, StatusCode::OK, &HeaderMap::new(), None)
            .await;

        assert_eq!(
            state
                .account_health_cell("primary")
                .unwrap()
                .transient_failure_count(),
            0
        );
    }

    #[test]
    fn should_parse_retry_after_delta_and_http_date_deadlines() {
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", "12".parse().unwrap());

        assert_eq!(retry_after_deadline_ms(&headers, 5_000), Some(17_000));

        headers.insert(
            "retry-after",
            "Thu, 01 Jan 1970 00:00:10 GMT".parse().unwrap(),
        );
        assert_eq!(retry_after_deadline_ms(&headers, 5_000), Some(10_000));

        headers.insert(
            "retry-after",
            "Thursday, 01-Jan-70 00:00:11 GMT".parse().unwrap(),
        );
        assert_eq!(retry_after_deadline_ms(&headers, 5_000), Some(11_000));

        headers.insert("retry-after", "Thu Jan  1 00:00:12 1970".parse().unwrap());
        assert_eq!(retry_after_deadline_ms(&headers, 5_000), Some(12_000));
    }

    #[test]
    fn should_compute_throttle_deadline_with_retry_after_policy_and_bounded_jitter() {
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", "120".parse().unwrap());
        let retry = RetryConfig {
            honor_retry_after: true,
            base_backoff_ms: 250,
            max_backoff_ms: 30_000,
            max_precommit_retries: 1,
        };

        assert_eq!(
            throttle_deadline_ms(&headers, 1_000, &retry, "primary", 1),
            121_000
        );

        let no_retry_after = RetryConfig {
            honor_retry_after: false,
            base_backoff_ms: 250,
            max_backoff_ms: 400,
            max_precommit_retries: 1,
        };
        let deadline = throttle_deadline_ms(&headers, 1_000, &no_retry_after, "primary", 1);

        assert!((1_250..=1_400).contains(&deadline));
    }

    #[test]
    fn should_grow_transient_backoff_exponentially_until_cap() {
        let retry = RetryConfig {
            honor_retry_after: false,
            base_backoff_ms: 250,
            max_backoff_ms: 1_000,
            max_precommit_retries: 1,
        };

        assert_eq!(exponential_backoff_ms(&retry, 1), 250);
        assert_eq!(exponential_backoff_ms(&retry, 2), 500);
        assert_eq!(exponential_backoff_ms(&retry, 3), 1_000);
        assert_eq!(exponential_backoff_ms(&retry, 30), 1_000);
    }

    #[tokio::test]
    async fn should_record_throttled_health_until_retry_after_deadline() {
        let state = AppState::new(effective_config(vec![account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        )]))
        .unwrap();
        let selected = state.effective.accounts[0].clone();
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", "120".parse().unwrap());

        record_account_http_status(
            &state,
            &selected,
            StatusCode::TOO_MANY_REQUESTS,
            &headers,
            None,
        )
        .await;

        let AccountHealth::Throttled { next_retry_at_ms } =
            state.account_health_cell("primary").unwrap().load()
        else {
            panic!("expected throttled health");
        };
        assert!(next_retry_at_ms >= now_unix_ms() + 119_000);
    }

    #[tokio::test]
    async fn should_record_usage_limited_reset_in_account_health_cell() {
        let state = AppState::new(effective_config(vec![account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        )]))
        .unwrap();
        let selected = state.effective.accounts[0].clone();
        let reset_at_ms = now_unix_ms() + 60_000;

        record_account_http_status(
            &state,
            &selected,
            StatusCode::TOO_MANY_REQUESTS,
            &HeaderMap::new(),
            Some(AccountHealth::UsageLimited { reset_at_ms }),
        )
        .await;

        assert_eq!(
            state.account_health_cell("primary").unwrap().load(),
            AccountHealth::UsageLimited { reset_at_ms }
        );
    }

    #[tokio::test]
    async fn should_exclude_usage_limited_account_cell_from_selection() {
        let state = AppState::new(effective_config(vec![
            account(
                "first",
                "http://127.0.0.1:1/v1".to_string(),
                "first-token",
                100,
            ),
            account(
                "second",
                "http://127.0.0.1:2/v1".to_string(),
                "second-token",
                90,
            ),
        ]))
        .unwrap();
        state.store_account_health(
            "first",
            AccountHealth::UsageLimited {
                reset_at_ms: now_unix_ms() + 60_000,
            },
        );
        let request = RouteRequest {
            endpoint: Endpoint::Responses,
            transport: Transport::Http,
            model: "gpt-5.5".to_string(),
            service_tier: None,
            pinned_account_id: None,
            requires_incremental_previous_response_id: false,
            model_family: "gpt-5".to_string(),
            stream: false,
        };

        let selected = select_next_account(&state, &request, &[]).await.unwrap();

        assert_eq!(selected.config.id, "second");
    }

    #[tokio::test]
    async fn should_require_auth_before_route_errors_except_healthz() {
        let state = AppState::new(effective_config(vec![account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        )]))
        .unwrap();
        let app = app(state);

        let generation_wrong_method = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/chat/completions")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(generation_wrong_method.status(), StatusCode::UNAUTHORIZED);

        let models_wrong_method = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/models")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(models_wrong_method.status(), StatusCode::UNAUTHORIZED);

        let metrics_wrong_method = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/metrics")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(metrics_wrong_method.status(), StatusCode::UNAUTHORIZED);

        let unsupported_openai_route = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/files")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unsupported_openai_route.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn should_return_426_for_responses_and_compact_get_without_websocket_upgrade() {
        let state = AppState::new(effective_config(vec![account(
            "primary",
            "http://127.0.0.1:1/v1".to_string(),
            "upstream-token",
            100,
        )]))
        .unwrap();

        // GET /v1/responses without an upgrade exercises the responses_ws
        // rejection branch; GET /v1/responses/compact has no WebSocket transport
        // even with upgrade headers present.
        let responses = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/responses")
                    .header("authorization", "Bearer client-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(responses.status(), StatusCode::UPGRADE_REQUIRED);
        let body = to_bytes(responses.into_body(), 1024 * 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body["error"]["message"],
            "GET /v1/responses requires WebSocket upgrade"
        );
        assert_eq!(body["error"]["type"], "tokenproxy_error");
        assert_eq!(body["error"]["code"], "unsupported_method");
        assert!(body["error"]["param"].is_null());
        assert!(
            body["error"]["tokenproxy_request_id"]
                .as_str()
                .is_some_and(|request_id| request_id.starts_with("req_"))
        );

        let compact = app(state.clone())
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/v1/responses/compact")
                    .header("authorization", "Bearer client-key")
                    .header("connection", "upgrade")
                    .header("upgrade", "websocket")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(compact.status(), StatusCode::UPGRADE_REQUIRED);
        let body = to_bytes(compact.into_body(), 1024 * 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body["error"]["message"],
            "GET /v1/responses/compact does not support WebSocket transport"
        );
        assert_eq!(body["error"]["type"], "tokenproxy_error");
        assert_eq!(body["error"]["code"], "unsupported_method");
        assert!(body["error"]["param"].is_null());

        let metrics = state.metrics.snapshot();
        assert_eq!(
            metrics.request_outcomes,
            vec![
                (
                    crate::metrics::RequestMetricKey {
                        endpoint: "/v1/responses".to_string(),
                        transport: "http".to_string(),
                        status_class: "4xx".to_string(),
                        model_family: "unknown".to_string(),
                        account_id_hash: "none".to_string(),
                    },
                    1,
                ),
                (
                    crate::metrics::RequestMetricKey {
                        endpoint: "/v1/responses/compact".to_string(),
                        transport: "http".to_string(),
                        status_class: "4xx".to_string(),
                        model_family: "unknown".to_string(),
                        account_id_hash: "none".to_string(),
                    },
                    1,
                ),
            ]
        );
        assert_eq!(metrics.request_duration_count, 2);
    }

    #[test]
    fn should_return_sorted_unique_models_from_enabled_generation_capable_accounts() {
        use crate::config::AccountConfig;

        fn account(id: &str, models: &[&str]) -> EffectiveAccount {
            EffectiveAccount {
                config: AccountConfig {
                    id: id.to_string(),
                    models: models.iter().map(|model| model.to_string()).collect(),
                    supports_responses: true,
                    ..AccountConfig::default()
                },
                bearer_token: "token".to_string(),
                chatgpt_account_id: None,
                prompt_cache_key_seed: None,
            }
        }

        let mut disabled = account("disabled", &["gpt-disabled"]);
        disabled.config.enabled = false;

        let mut no_generation_capability = account("no-generation", &["gpt-hidden"]);
        no_generation_capability.config.supports_responses = false;

        let list = model_list(&[
            account("primary", &["gpt-5.5", "gpt-5.4"]),
            account("secondary", &["gpt-5.4", "gpt-5.3-codex"]),
            disabled,
            no_generation_capability,
        ]);

        assert_eq!(
            list.data,
            vec![
                ModelEntry {
                    id: "gpt-5.3-codex".to_string(),
                    object: "model",
                },
                ModelEntry {
                    id: "gpt-5.4".to_string(),
                    object: "model",
                },
                ModelEntry {
                    id: "gpt-5.5".to_string(),
                    object: "model",
                },
            ]
        );
    }
}
