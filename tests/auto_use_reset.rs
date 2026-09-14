use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::extract::ws::{Message, WebSocketUpgrade};
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokenproxy::config::{AccountConfig, AccountKind, Config, EffectiveAccount, EffectiveConfig};
use tokenproxy::logging::LogFormat;
use tokenproxy::server::{AppState, app};
use tokio::net::TcpListener;
use tokio::sync::{Barrier, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

const DEADLINE: Duration = Duration::from_secs(5);
const RESPONSES: &str = "/backend-api/codex/responses";
const CREDITS: &str = "/backend-api/wham/rate-limit-reset-credits";
const CONSUME: &str = "/backend-api/wham/rate-limit-reset-credits/consume";

#[derive(Clone, Copy)]
enum Mode {
    HttpQuota,
    HttpTypeOnlyQuota,
    AlwaysQuota,
    Throttled,
    SseFirst,
    SseLate,
    SseFragmented,
    SseNestedThrottled,
    WebSocketFirst,
    WebSocketHandshake,
    WebSocketLate,
    WebSocketAlwaysQuota,
    Success,
}

#[derive(Clone, Debug)]
struct RecordedRequest {
    method: Method,
    path: String,
    headers: HeaderMap,
    body: Value,
}

struct MockState {
    mode: Mode,
    available: u64,
    consume_status: StatusCode,
    consume_body: Value,
    credits_response: Option<(StatusCode, String)>,
    consume_raw_body: Option<String>,
    consume_redirect: Option<String>,
    consume_stalls: bool,
    requests: Mutex<Vec<RecordedRequest>>,
    response_attempts: std::sync::atomic::AtomicUsize,
    quota_barrier: Option<Barrier>,
    initial_quota_count: usize,
}

impl MockState {
    fn new(mode: Mode) -> Self {
        Self {
            mode,
            available: 3,
            consume_status: StatusCode::OK,
            consume_body: json!({"code":"reset", "windows_reset":2}),
            credits_response: None,
            consume_raw_body: None,
            consume_redirect: None,
            consume_stalls: false,
            requests: Mutex::new(Vec::new()),
            response_attempts: std::sync::atomic::AtomicUsize::new(0),
            quota_barrier: None,
            initial_quota_count: 1,
        }
    }

    fn record(&self, method: Method, uri: Uri, headers: HeaderMap, body: Value) {
        self.requests.lock().unwrap().push(RecordedRequest {
            method,
            path: uri.path().to_owned(),
            headers,
            body,
        });
    }

    fn requests_to(&self, path: &str) -> Vec<RecordedRequest> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.path == path)
            .cloned()
            .collect()
    }

    fn attempts(&self) -> usize {
        self.response_attempts
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

struct Server {
    url: String,
    task: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(router: Router) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    Server { url, task }
}

async fn upstream(state: Arc<MockState>) -> Server {
    serve(
        Router::new()
            .route(RESPONSES, post(mock_http).get(mock_websocket))
            .route(CREDITS, get(mock_credits))
            .route(CONSUME, post(mock_consume))
            .with_state(state),
    )
    .await
}

fn quota() -> Value {
    json!({"type":"error", "error":{"type":"usage_limit_reached", "code":"usage_limit_reached", "message":"Usage exhausted", "resets_in_seconds":3600}})
}

fn completed() -> Value {
    json!({"type":"response.completed", "response":{"id":"resp_recovered", "status":"completed", "output":[]}})
}

fn sse(events: &[Value]) -> Response {
    let body: String = events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect();
    ([("content-type", "text/event-stream")], body).into_response()
}

async fn mock_http(
    State(state): State<Arc<MockState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    state.record(method, uri, headers, serde_json::from_slice(&body).unwrap());
    let attempt = state
        .response_attempts
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    if attempt < state.initial_quota_count {
        if let Some(barrier) = &state.quota_barrier {
            tokio::time::timeout(DEADLINE, barrier.wait())
                .await
                .unwrap();
        }
        match state.mode {
            Mode::HttpQuota | Mode::AlwaysQuota => {
                return (StatusCode::TOO_MANY_REQUESTS, Json(quota())).into_response();
            }
            Mode::HttpTypeOnlyQuota => {
                return (StatusCode::TOO_MANY_REQUESTS, Json(json!({
                    "error":{"type":"usage_limit_reached", "message":"Usage exhausted", "resets_at":4102444800_u64}
                }))).into_response();
            }
            Mode::Throttled => return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(
                    json!({"error":{"code":"rate_limit_exceeded", "message":"Too many requests"}}),
                ),
            )
                .into_response(),
            Mode::SseFirst => return sse(&[quota()]),
            Mode::SseFragmented => {
                let mut event = quota();
                event["error"]["message"] = json!("Quota épuisé");
                let bytes = format!("event: error\ndata: {event}\n\n").into_bytes();
                let stream =
                    futures_util::stream::unfold((bytes, 0), |(bytes, index)| async move {
                        let byte = *bytes.get(index)?;
                        tokio::task::yield_now().await;
                        Some((
                            Ok::<_, std::convert::Infallible>(Bytes::from(vec![byte])),
                            (bytes, index + 1),
                        ))
                    });
                return (
                    [("content-type", "text/event-stream")],
                    Body::from_stream(stream),
                )
                    .into_response();
            }
            Mode::SseNestedThrottled => {
                return sse(&[json!({"type":"response.failed", "response": {
                    "id":"resp_throttled", "status":"failed", "error":{"code":"rate_limit_exceeded", "message":"Too many requests"}
                }})]);
            }
            Mode::SseLate => {
                return sse(&[
                    json!({"type":"response.output_text.delta", "delta":"Already visible"}),
                    quota(),
                ]);
            }
            Mode::Success
            | Mode::WebSocketFirst
            | Mode::WebSocketHandshake
            | Mode::WebSocketLate
            | Mode::WebSocketAlwaysQuota => {}
        }
    }
    if matches!(state.mode, Mode::AlwaysQuota) {
        return (StatusCode::TOO_MANY_REQUESTS, Json(quota())).into_response();
    }
    sse(&[completed()])
}

async fn mock_credits(
    State(state): State<Arc<MockState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    state.record(method, uri, headers, Value::Null);
    if let Some((status, body)) = &state.credits_response {
        return (
            *status,
            [("content-type", "application/json")],
            body.clone(),
        )
            .into_response();
    }
    Json(json!({"available_count":state.available,"credits":[]})).into_response()
}

async fn mock_consume(
    State(state): State<Arc<MockState>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    state.record(method, uri, headers, serde_json::from_slice(&body).unwrap());
    if state.consume_stalls {
        let _ = tokio::time::timeout(DEADLINE, std::future::pending::<()>()).await;
    }
    if let Some(location) = &state.consume_redirect {
        return (
            StatusCode::TEMPORARY_REDIRECT,
            [("location", location.clone())],
        )
            .into_response();
    }
    if let Some(body) = &state.consume_raw_body {
        return (
            StatusCode::OK,
            [("content-type", "application/json")],
            body.clone(),
        )
            .into_response();
    }
    (state.consume_status, Json(state.consume_body.clone())).into_response()
}

async fn mock_websocket(
    State(state): State<Arc<MockState>>,
    ws: WebSocketUpgrade,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    state.record(method, uri, headers, Value::Null);
    if matches!(state.mode, Mode::WebSocketHandshake) && state.attempts() == 0 {
        state
            .response_attempts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        return (StatusCode::TOO_MANY_REQUESTS, Json(quota())).into_response();
    }
    ws.on_upgrade(move |mut socket| async move {
        while let Ok(Some(Ok(Message::Text(text)))) =
            tokio::time::timeout(DEADLINE, socket.recv()).await
        {
            let request: Value = serde_json::from_str(&text).unwrap();
            assert_eq!(request["type"], "response.create");
            let attempt = state
                .response_attempts
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if attempt == 0 && matches!(state.mode, Mode::WebSocketLate) {
                let delta = json!({"type":"response.output_text.delta", "delta":"Already visible"});
                if socket
                    .send(Message::Text(delta.to_string().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            let event = if attempt == 0 || matches!(state.mode, Mode::WebSocketAlwaysQuota) {
                quota()
            } else {
                completed()
            };
            if socket
                .send(Message::Text(event.to_string().into()))
                .await
                .is_err()
            {
                break;
            }
        }
    })
}

fn account(server: &Server, id: &str, enabled: bool, priority: i32) -> EffectiveAccount {
    EffectiveAccount {
        config: AccountConfig {
            id: id.to_owned(),
            kind: AccountKind::ChatgptCodexAuthJson,
            base_url: format!("{}/backend-api/codex", server.url),
            auto_use_reset: enabled,
            priority,
            models: vec!["gpt-5.5".to_owned()],
            supports_responses: true,
            supports_responses_ws: true,
            ..AccountConfig::default()
        },
        bearer_token: "mock-access-token".to_owned(),
        chatgpt_account_id: Some("mock-chatgpt-account".to_owned()),
        auth_json: None,
        prompt_cache_key_seed: None,
    }
}

async fn proxy(accounts: Vec<EffectiveAccount>, retries: u8) -> Server {
    let mut config = Config::default();
    config.accounts = accounts
        .iter()
        .map(|account| account.config.clone())
        .collect();
    config.server.allow_insecure_upstream = true;
    config.retry.max_precommit_retries = retries;
    config.retry.base_backoff_ms = 0;
    config.retry.max_backoff_ms = 0;
    config.retry.honor_retry_after = false;
    config.timeouts.request_header_ms = 1000;
    config.timeouts.stream_idle_ms = 1000;
    config.timeouts.websocket_connect_ms = 1000;
    config.timeouts.websocket_idle_ms = 1000;
    let effective = EffectiveConfig {
        config,
        config_update_endpoint: None,
        admin_token: None,
        downstream_token: "mock-client-key".to_owned(),
        account_hash_key: "mock-hash-key".to_owned(),
        accounts,
    };
    let (shutdown_tx, _) = watch::channel(false);
    let state = AppState::new_with_log_format_and_shutdown(effective, LogFormat::Json, shutdown_tx)
        .unwrap();
    serve(app(state)).await
}

async fn request(proxy: &Server) -> (StatusCode, String) {
    request_model(proxy, "gpt-5.5").await
}

async fn request_model(proxy: &Server, model: &str) -> (StatusCode, String) {
    let response = reqwest::Client::builder()
        .timeout(DEADLINE)
        .build()
        .unwrap()
        .post(format!("{}/v1/responses", proxy.url))
        .bearer_auth("mock-client-key")
        .json(&json!({"model":model, "input":"Hello", "stream":true}))
        .send()
        .await
        .unwrap();
    let status = response.status();
    (status, response.text().await.unwrap())
}

fn assert_one_redemption(state: &MockState) {
    let requests = state.requests_to(CONSUME);
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].method, Method::POST);
    assert_eq!(
        requests[0].headers["authorization"],
        "Bearer mock-access-token"
    );
    assert_eq!(
        requests[0].headers["chatgpt-account-id"],
        "mock-chatgpt-account"
    );
    assert!(
        requests[0].body["redeem_request_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty())
    );
    assert_eq!(requests[0].body.as_object().unwrap().len(), 1);
    let credits = state.requests_to(CREDITS);
    assert_eq!(credits.len(), 1);
    assert_eq!(credits[0].method, Method::GET);
    assert_eq!(
        credits[0].headers["authorization"],
        "Bearer mock-access-token"
    );
    assert_eq!(
        credits[0].headers["chatgpt-account-id"],
        "mock-chatgpt-account"
    );
}

#[tokio::test]
async fn disabled_option_never_looks_up_or_consumes_credits() {
    let state = Arc::new(MockState::new(Mode::HttpQuota));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", false, 100)], 0).await;
    let (status, body) = request(&proxy).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(body.contains("all compatible accounts are usage-limited"));
    assert!(state.requests_to(CREDITS).is_empty());
    assert!(state.requests_to(CONSUME).is_empty());
}

#[tokio::test]
async fn generic_throttling_never_spends_a_usage_reset() {
    let state = Arc::new(MockState::new(Mode::Throttled));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", true, 100)], 0).await;
    assert_eq!(request(&proxy).await.0, StatusCode::TOO_MANY_REQUESTS);
    assert!(state.requests_to(CREDITS).is_empty());
    assert!(state.requests_to(CONSUME).is_empty());
}

#[tokio::test]
async fn quota_reset_retries_the_same_account_with_zero_failover_budget() {
    let state = Arc::new(MockState::new(Mode::HttpQuota));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", true, 100)], 0).await;
    let (status, body) = request(&proxy).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("resp_recovered"));
    assert_eq!(state.attempts(), 2);
    assert_one_redemption(&state);
    let forwarded = state.requests_to(RESPONSES);
    assert_eq!(forwarded[0].body, forwarded[1].body);
}

#[tokio::test]
async fn exhausted_credits_preserve_normal_account_failover() {
    let mut state = MockState::new(Mode::HttpQuota);
    state.available = 0;
    let state = Arc::new(state);
    let primary = upstream(state.clone()).await;
    let fallback_state = Arc::new(MockState::new(Mode::Success));
    let fallback = upstream(fallback_state.clone()).await;
    let proxy = proxy(
        vec![
            account(&primary, "primary", true, 100),
            account(&fallback, "fallback", false, 0),
        ],
        1,
    )
    .await;
    assert_eq!(request(&proxy).await.0, StatusCode::OK);
    assert_eq!(state.attempts(), 1);
    assert_eq!(fallback_state.attempts(), 1);
    assert_eq!(state.requests_to(CREDITS).len(), 1);
    assert!(state.requests_to(CONSUME).is_empty());
}

#[tokio::test]
async fn failed_redemption_preserves_normal_account_failover() {
    for (status, body) in [
        (
            StatusCode::OK,
            json!({"code":"no_credit", "windows_reset":0}),
        ),
        (
            StatusCode::OK,
            json!({"code":"nothing_to_reset", "windows_reset":0}),
        ),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error":"unavailable"}),
        ),
        (StatusCode::UNAUTHORIZED, json!({"error":"unauthorized"})),
        (
            StatusCode::OK,
            json!({"code":"unexpected", "windows_reset":2}),
        ),
    ] {
        let mut state = MockState::new(Mode::HttpQuota);
        state.consume_status = status;
        state.consume_body = body;
        let state = Arc::new(state);
        let primary = upstream(state.clone()).await;
        let fallback_state = Arc::new(MockState::new(Mode::Success));
        let fallback = upstream(fallback_state.clone()).await;
        let proxy = proxy(
            vec![
                account(&primary, "primary", true, 100),
                account(&fallback, "fallback", false, 0),
            ],
            1,
        )
        .await;
        assert_eq!(request(&proxy).await.0, StatusCode::OK);
        assert_eq!(state.attempts(), 1);
        assert_eq!(fallback_state.attempts(), 1);
        assert_one_redemption(&state);
    }
}

#[tokio::test]
async fn already_redeemed_result_retries_without_spending_a_second_credit() {
    let mut state = MockState::new(Mode::HttpQuota);
    state.consume_body = json!({"code":"already_redeemed", "windows_reset":0});
    let state = Arc::new(state);
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", true, 100)], 0).await;
    assert_eq!(request(&proxy).await.0, StatusCode::OK);
    assert_eq!(state.attempts(), 2);
    assert_one_redemption(&state);
}

#[tokio::test]
async fn quota_persisting_after_reset_does_not_consume_repeatedly() {
    let state = Arc::new(MockState::new(Mode::AlwaysQuota));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", true, 100)], 0).await;
    assert_eq!(request(&proxy).await.0, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(state.attempts(), 2);
    assert_one_redemption(&state);
}

#[tokio::test]
async fn first_sse_quota_event_is_reset_and_retried_before_output() {
    let state = Arc::new(MockState::new(Mode::SseFirst));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", true, 100)], 0).await;
    let (status, body) = request(&proxy).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("resp_recovered"));
    assert!(!body.contains("usage_limit_reached"));
    assert_eq!(state.attempts(), 2);
    assert_one_redemption(&state);
}

#[tokio::test]
async fn late_sse_quota_error_preserves_output_and_recovers_future_requests() {
    let state = Arc::new(MockState::new(Mode::SseLate));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", true, 100)], 0).await;
    let (status, body) = request(&proxy).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.matches("Already visible").count(), 1);
    assert!(body.contains("usage_limit_reached"));
    assert!(!body.contains("resp_recovered"));
    assert_eq!(state.attempts(), 1);
    assert_one_redemption(&state);
    let (status, body) = request(&proxy).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("resp_recovered"));
    assert_eq!(state.attempts(), 2);
    assert_eq!(state.requests_to(CONSUME).len(), 1);
}

#[tokio::test]
async fn first_websocket_quota_error_is_reset_and_retried() {
    assert_websocket_recovery(Mode::WebSocketFirst).await;
}

#[tokio::test]
async fn websocket_handshake_quota_is_reset_before_sending_create() {
    assert_websocket_recovery(Mode::WebSocketHandshake).await;
}

async fn assert_websocket_recovery(mode: Mode) {
    let state = Arc::new(MockState::new(mode));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", true, 100)], 0).await;
    let url = format!("{}/v1/responses", proxy.url.replace("http://", "ws://"));
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("authorization", "Bearer mock-client-key".parse().unwrap());
    tokio::time::timeout(DEADLINE, async {
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        socket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                json!({"type":"response.create", "model":"gpt-5.5", "input":[]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let received = socket.next().await.unwrap().unwrap();
        let event: Value = serde_json::from_str(received.to_text().unwrap()).unwrap();
        assert_eq!(event["type"], "response.completed");
        socket.close(None).await.unwrap();
    })
    .await
    .unwrap();
    assert_eq!(state.attempts(), 2);
    assert_one_redemption(&state);
}

#[tokio::test]
async fn simultaneous_stale_quota_errors_share_one_redemption() {
    let mut state = MockState::new(Mode::HttpQuota);
    state.initial_quota_count = 8;
    state.quota_barrier = Some(Barrier::new(8));
    let state = Arc::new(state);
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", true, 100)], 0).await;
    let requests = (0..8).map(|_| request(&proxy));
    for (status, body) in futures_util::future::join_all(requests).await {
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("resp_recovered"));
    }
    assert_eq!(state.attempts(), 16);
    assert_one_redemption(&state);
}

#[tokio::test]
async fn aliases_for_the_same_chatgpt_account_share_one_redemption() {
    let mut state = MockState::new(Mode::HttpQuota);
    state.initial_quota_count = 2;
    state.quota_barrier = Some(Barrier::new(2));
    let state = Arc::new(state);
    let upstream = upstream(state.clone()).await;
    let first = account(&upstream, "alias-one", true, 100);
    let mut second = account(&upstream, "alias-two", true, 100);
    second.config.models = vec!["gpt-5.4".to_owned()];
    let proxy = proxy(vec![first, second], 0).await;
    let responses = tokio::join!(
        request_model(&proxy, "gpt-5.5"),
        request_model(&proxy, "gpt-5.4")
    );
    for (status, body) in [responses.0, responses.1] {
        assert_eq!(status, StatusCode::OK);
        assert!(body.contains("resp_recovered"));
    }
    assert_eq!(state.attempts(), 4);
    assert_one_redemption(&state);
}

#[tokio::test]
async fn malformed_credit_inventory_never_authorizes_redemption() {
    for (status, body) in [
        (StatusCode::OK, "not json"),
        (StatusCode::OK, r#"{"available_count":"3","credits":[]}"#),
        (StatusCode::OK, r#"{"credits":[]}"#),
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"available_count":3,"credits":[]}"#,
        ),
    ] {
        let mut state = MockState::new(Mode::HttpQuota);
        state.credits_response = Some((status, body.to_owned()));
        let state = Arc::new(state);
        let upstream = upstream(state.clone()).await;
        let proxy = proxy(vec![account(&upstream, "primary", true, 100)], 0).await;
        assert_eq!(request(&proxy).await.0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(state.attempts(), 1);
        assert_eq!(state.requests_to(CREDITS).len(), 1);
        assert!(state.requests_to(CONSUME).is_empty());
    }
}

#[tokio::test]
async fn malformed_consume_response_does_not_claim_success() {
    let oversized = json!({"code":"reset", "padding":"x".repeat(65_536)}).to_string();
    for body in [
        "not json",
        r#"{"windows_reset":2}"#,
        r#"{"code":17,"windows_reset":2}"#,
        &oversized,
    ] {
        let mut state = MockState::new(Mode::HttpQuota);
        state.consume_raw_body = Some(body.to_owned());
        let state = Arc::new(state);
        let upstream = upstream(state.clone()).await;
        let proxy = proxy(vec![account(&upstream, "primary", true, 100)], 0).await;
        assert_eq!(request(&proxy).await.0, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(state.attempts(), 1);
        assert_one_redemption(&state);
    }
}

#[tokio::test]
async fn reset_timeout_preserves_normal_account_failover() {
    let mut state = MockState::new(Mode::HttpQuota);
    state.consume_stalls = true;
    let state = Arc::new(state);
    let primary = upstream(state.clone()).await;
    let fallback_state = Arc::new(MockState::new(Mode::Success));
    let fallback = upstream(fallback_state.clone()).await;
    let proxy = proxy(
        vec![
            account(&primary, "primary", true, 100),
            account(&fallback, "fallback", false, 0),
        ],
        1,
    )
    .await;
    assert_eq!(request(&proxy).await.0, StatusCode::OK);
    assert_eq!(state.attempts(), 1);
    assert_eq!(fallback_state.attempts(), 1);
    assert_one_redemption(&state);
}

#[tokio::test]
async fn reset_redirect_is_not_followed_to_another_origin() {
    let redirect_state = Arc::new(MockState::new(Mode::Success));
    let redirect_destination = upstream(redirect_state.clone()).await;
    let mut state = MockState::new(Mode::HttpQuota);
    state.consume_redirect = Some(format!("{}{CONSUME}", redirect_destination.url));
    let state = Arc::new(state);
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", true, 100)], 0).await;
    assert_eq!(request(&proxy).await.0, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(state.attempts(), 1);
    assert_one_redemption(&state);
    assert!(redirect_state.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn codex_type_only_quota_with_numeric_reset_time_recovers() {
    let state = Arc::new(MockState::new(Mode::HttpTypeOnlyQuota));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", true, 100)], 0).await;
    let (status, body) = request(&proxy).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("resp_recovered"));
    assert_eq!(state.attempts(), 2);
    assert_one_redemption(&state);
}

#[tokio::test]
async fn sse_quota_split_across_json_utf8_and_frame_boundaries_recovers() {
    let state = Arc::new(MockState::new(Mode::SseFragmented));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", true, 100)], 0).await;
    let (status, body) = request(&proxy).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("resp_recovered"));
    assert!(!body.contains("usage_limit_reached"));
    assert_eq!(state.attempts(), 2);
    assert_one_redemption(&state);
}

#[tokio::test]
async fn nested_sse_throttle_error_does_not_spend_a_reset() {
    let state = Arc::new(MockState::new(Mode::SseNestedThrottled));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", true, 100)], 0).await;
    let (status, body) = request(&proxy).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("response.failed"));
    assert!(body.contains("rate_limit_exceeded"));
    assert_eq!(state.attempts(), 1);
    assert!(state.requests_to(CREDITS).is_empty());
    assert!(state.requests_to(CONSUME).is_empty());
}

#[tokio::test]
async fn late_websocket_quota_preserves_output_and_recovers_next_create() {
    let state = Arc::new(MockState::new(Mode::WebSocketLate));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", true, 100)], 0).await;
    let url = format!("{}/v1/responses", proxy.url.replace("http://", "ws://"));
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("authorization", "Bearer mock-client-key".parse().unwrap());
    tokio::time::timeout(DEADLINE, async {
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        let create = tokio_tungstenite::tungstenite::Message::Text(
            json!({"type":"response.create", "model":"gpt-5.5", "input":[]})
                .to_string()
                .into(),
        );
        socket.send(create.clone()).await.unwrap();
        let first = socket.next().await.unwrap().unwrap();
        let first: Value = serde_json::from_str(first.to_text().unwrap()).unwrap();
        assert_eq!(first["type"], "response.output_text.delta");
        assert_eq!(first["delta"], "Already visible");
        let error = socket.next().await.unwrap().unwrap();
        let error: Value = serde_json::from_str(error.to_text().unwrap()).unwrap();
        assert_eq!(error["error"]["code"], "usage_limit_reached");
        assert_eq!(state.attempts(), 1);
        assert_one_redemption(&state);
        socket.send(create).await.unwrap();
        let next = socket.next().await.unwrap().unwrap();
        let next: Value = serde_json::from_str(next.to_text().unwrap()).unwrap();
        assert_eq!(next["type"], "response.completed");
        socket.close(None).await.unwrap();
    })
    .await
    .unwrap();
    assert_eq!(state.attempts(), 2);
    assert_one_redemption(&state);
}

#[tokio::test]
async fn websocket_quota_after_reset_does_not_consume_repeatedly() {
    let state = Arc::new(MockState::new(Mode::WebSocketAlwaysQuota));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", true, 100)], 0).await;
    let url = format!("{}/v1/responses", proxy.url.replace("http://", "ws://"));
    let mut request = url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert("authorization", "Bearer mock-client-key".parse().unwrap());
    tokio::time::timeout(DEADLINE, async {
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        socket
            .send(tokio_tungstenite::tungstenite::Message::Text(
                json!({"type":"response.create", "model":"gpt-5.5", "input":[]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let event = socket.next().await.unwrap().unwrap();
        let event: Value = serde_json::from_str(event.to_text().unwrap()).unwrap();
        assert_eq!(event["error"]["code"], "usage_limit_reached");
        socket.close(None).await.unwrap();
    })
    .await
    .unwrap();
    assert_eq!(state.attempts(), 2);
    assert_one_redemption(&state);
}
