use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use axum::routing::post;
use futures_util::TryStreamExt;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use tokio_tungstenite::{accept_async, accept_hdr_async};
use tower::ServiceExt;

use super::state::AccountHealthCell;
use super::*;
use crate::config::{
    AccountConfig, AccountKind, Config, EffectiveAccount, EffectiveConfig, RetryConfig,
};
use crate::logging::LogFormat;

#[derive(Debug, Clone, Default)]
struct CapturedUpstreamRequest {
    authorization: Option<String>,
    x_api_key: Option<String>,
    anthropic_version: Option<String>,
    host: Option<String>,
    openai_organization: Option<String>,
    openai_project: Option<String>,
    body: Vec<u8>,
}

type FakeUpstreamState = (
    StatusCode,
    &'static str,
    Arc<Mutex<Vec<CapturedUpstreamRequest>>>,
);

async fn capture_upstream_request(
    captured: &Mutex<Vec<CapturedUpstreamRequest>>,
    headers: &HeaderMap,
    request_body: Body,
) {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned)
    };
    let bytes = to_bytes(request_body, 1024 * 1024).await.unwrap();
    captured.lock().await.push(CapturedUpstreamRequest {
        authorization: header("authorization"),
        x_api_key: header("x-api-key"),
        anthropic_version: header("anthropic-version"),
        host: header("host"),
        openai_organization: header("openai-organization"),
        openai_project: header("openai-project"),
        body: bytes.to_vec(),
    });
}

async fn fake_upstream(
    status: StatusCode,
    body: &'static str,
    captured: Arc<Mutex<Vec<CapturedUpstreamRequest>>>,
) -> SocketAddr {
    async fn handler(
        State((status, body, captured)): State<FakeUpstreamState>,
        headers: HeaderMap,
        request_body: Body,
    ) -> Response {
        capture_upstream_request(&captured, &headers, request_body).await;
        (
            status,
            [
                ("content-type", "application/json"),
                ("x-ratelimit-limit-requests", "500"),
                ("x-ratelimit-remaining-requests", "499"),
                ("x-ratelimit-reset-requests", "120ms"),
            ],
            body,
        )
            .into_response()
    }

    let app = Router::new()
        .route("/v1/chat/completions", post(handler))
        .route("/v1/messages", post(handler))
        .route("/v1/responses", post(handler))
        .route("/v1/responses/compact", post(handler))
        .with_state((status, body, captured));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

async fn fake_delayed_upstream(
    delay: Duration,
    captured: Arc<Mutex<Vec<CapturedUpstreamRequest>>>,
) -> SocketAddr {
    async fn handler(
        State((delay, captured)): State<(Duration, Arc<Mutex<Vec<CapturedUpstreamRequest>>>)>,
        headers: HeaderMap,
        request_body: Body,
    ) -> Response {
        capture_upstream_request(&captured, &headers, request_body).await;
        sleep(delay).await;
        (StatusCode::OK, r#"{"id":"resp_ok"}"#).into_response()
    }

    let app = Router::new()
        .route("/v1/responses", post(handler))
        .with_state((delay, captured));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
}

async fn fake_sse_upstream(
    body: &'static str,
    captured: Arc<Mutex<Vec<CapturedUpstreamRequest>>>,
) -> SocketAddr {
    async fn handler(
        State((body, captured)): State<(&'static str, Arc<Mutex<Vec<CapturedUpstreamRequest>>>)>,
        headers: HeaderMap,
        request_body: Body,
    ) -> Response {
        capture_upstream_request(&captured, &headers, request_body).await;
        (
            StatusCode::OK,
            [("content-type", "text/event-stream")],
            body,
        )
            .into_response()
    }

    let app = Router::new()
        .route("/v1/responses", post(handler))
        .with_state((body, captured));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    addr
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

async fn fake_delayed_websocket_upstream(captured_messages: Arc<Mutex<Vec<String>>>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let captured_messages = Arc::clone(&captured_messages);
            tokio::spawn(async move {
                if let Ok(mut socket) = accept_async(stream).await {
                    let Some(Ok(UpstreamMessage::Text(text))) = socket.next().await else {
                        return;
                    };
                    captured_messages.lock().await.push(text.to_string());
                    sleep(Duration::from_millis(50)).await;
                    let _ = socket
                        .send(UpstreamMessage::Text(
                            serde_json::json!({
                                "type": "response.completed",
                                "response": {
                                    "id": "resp_1",
                                    "output": []
                                }
                            })
                            .to_string()
                            .into(),
                        ))
                        .await;
                    while let Some(Ok(UpstreamMessage::Text(text))) = socket.next().await {
                        captured_messages.lock().await.push(text.to_string());
                    }
                }
            });
        }
    });
    address
}

async fn fake_closing_websocket_upstream(captured_messages: Arc<Mutex<Vec<String>>>) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let captured_messages = Arc::clone(&captured_messages);
            tokio::spawn(async move {
                if let Ok(mut socket) = accept_async(stream).await
                    && let Some(Ok(UpstreamMessage::Text(text))) = socket.next().await
                {
                    captured_messages.lock().await.push(text.to_string());
                    let _ = socket.close(None).await;
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

fn anthropic_account(id: &str, base_url: String, api_key: &str, priority: i32) -> EffectiveAccount {
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

fn proxy_request(body: &'static str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/responses")
        .header("authorization", "Bearer client-key")
        .header("host", "tokenproxy.local")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

fn compact_proxy_request(body: &'static str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/responses/compact")
        .header("authorization", "Bearer client-key")
        .header("host", "tokenproxy.local")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

fn chat_proxy_request(body: &'static str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/chat/completions")
        .header("authorization", "Bearer client-key")
        .header("host", "tokenproxy.local")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

fn anthropic_messages_proxy_request(body: &'static str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri("/v1/messages")
        .header("authorization", "Bearer client-key")
        .header("host", "tokenproxy.local")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap()
}

#[tokio::test]
async fn should_proxy_json_body_and_replace_upstream_auth_headers() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream =
        fake_upstream(StatusCode::OK, r#"{"id":"resp_ok"}"#, Arc::clone(&captured)).await;
    let state = AppState::new(effective_config(vec![account(
        "primary",
        format!("http://{upstream}/v1"),
        "upstream-token",
        100,
    )]))
    .unwrap();

    let app = app(state.clone());
    let response = app
        .clone()
        .oneshot(proxy_request(r#"{"model":"gpt-5.5","input":[]}"#))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let upstream_requests = captured.lock().await;
    assert_eq!(upstream_requests.len(), 1);
    assert_eq!(
        upstream_requests[0].authorization.as_deref(),
        Some("Bearer upstream-token")
    );
    assert!(upstream_requests[0].host.is_some());
    assert_eq!(
        upstream_requests[0].body,
        br#"{"model":"gpt-5.5","input":[]}"#
    );
    drop(upstream_requests);

    let usage = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/usage")
                .header("authorization", "Bearer client-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(usage.into_body(), 1024 * 1024).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        body["accounts"][0]["usage"][0]["remaining_percent"],
        serde_json::json!(99.8)
    );
    assert_eq!(
        body["accounts"][0]["usage"][0]["rate_limit_pressure"],
        serde_json::json!("none")
    );

    let metrics = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .header("authorization", "Bearer client-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let metrics = to_bytes(metrics.into_body(), 1024 * 1024).await.unwrap();
    let metrics = String::from_utf8(metrics.to_vec()).unwrap();
    assert!(metrics.contains(&format!(
        r#"tokenproxy_upstream_connect_duration_ms_count{{origin="{upstream}",transport="http"}} 1"#
    )));
}

#[tokio::test]
async fn should_proxy_anthropic_messages_body_and_replace_upstream_auth_headers() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream = fake_upstream(StatusCode::OK, r#"{"id":"msg_ok"}"#, Arc::clone(&captured)).await;
    let state = AppState::new(effective_config(vec![anthropic_account(
        "primary",
        format!("http://{upstream}"),
        "upstream-token",
        100,
    )]))
    .unwrap();

    let response = app(state)
        .oneshot(anthropic_messages_proxy_request(
            r#"{"model":"claude-sonnet-4.5","max_tokens":1024,"messages":[{"role":"user","content":"hello"}]}"#,
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let upstream_requests = captured.lock().await;
    assert_eq!(upstream_requests.len(), 1);
    assert_eq!(upstream_requests[0].authorization, None);
    assert_eq!(
        upstream_requests[0].x_api_key.as_deref(),
        Some("upstream-token")
    );
    assert_eq!(
        upstream_requests[0].anthropic_version.as_deref(),
        Some("2023-06-01")
    );
    assert_eq!(
        upstream_requests[0].body,
        br#"{"model":"claude-sonnet-4.5","max_tokens":1024,"messages":[{"role":"user","content":"hello"}]}"#
    );
}

#[tokio::test]
async fn should_forward_openai_request_headers_only_when_config_allows_them() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream =
        fake_upstream(StatusCode::OK, r#"{"id":"resp_ok"}"#, Arc::clone(&captured)).await;
    let account = account(
        "primary",
        format!("http://{upstream}/v1"),
        "upstream-token",
        100,
    );
    let mut default_config = effective_config(vec![account.clone()]);
    default_config.config.server.allow_openai_request_headers = false;

    let default_response = app(AppState::new(default_config).unwrap())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .header("authorization", "Bearer client-key")
                .header("host", "tokenproxy.local")
                .header("content-type", "application/json")
                .header("openai-organization", "org_client")
                .header("openai-project", "proj_client")
                .body(Body::from(r#"{"model":"gpt-5.5","input":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(default_response.status(), StatusCode::OK);
    let mut allowed_config = effective_config(vec![account]);
    allowed_config.config.server.allow_openai_request_headers = true;

    let allowed_response = app(AppState::new(allowed_config).unwrap())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .header("authorization", "Bearer client-key")
                .header("host", "tokenproxy.local")
                .header("content-type", "application/json")
                .header("openai-organization", "org_client")
                .header("openai-project", "proj_client")
                .body(Body::from(r#"{"model":"gpt-5.5","input":[]}"#))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(allowed_response.status(), StatusCode::OK);
    let upstream_requests = captured.lock().await;
    assert_eq!(upstream_requests.len(), 2);
    assert!(upstream_requests[0].openai_organization.is_none());
    assert!(upstream_requests[0].openai_project.is_none());
    assert_eq!(
        upstream_requests[1].openai_organization.as_deref(),
        Some("org_client")
    );
    assert_eq!(
        upstream_requests[1].openai_project.as_deref(),
        Some("proj_client")
    );
}

#[tokio::test]
async fn should_timeout_waiting_for_upstream_response_headers() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream = fake_delayed_upstream(Duration::from_millis(50), Arc::clone(&captured)).await;
    let mut effective = effective_config(vec![account(
        "primary",
        format!("http://{upstream}/v1"),
        "upstream-token",
        100,
    )]);
    effective.config.timeouts.request_header_ms = 10;

    let response = app(AppState::new(effective).unwrap())
        .oneshot(proxy_request(r#"{"model":"gpt-5.5","input":[]}"#))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["code"], "upstream_failure");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("timed out waiting for upstream response headers")
    );
    assert_eq!(captured.lock().await.len(), 1);
}

#[tokio::test]
async fn should_add_stable_prompt_cache_key_to_responses_when_account_has_seed() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream =
        fake_upstream(StatusCode::OK, r#"{"id":"resp_ok"}"#, Arc::clone(&captured)).await;
    let mut primary = account(
        "primary",
        format!("http://{upstream}/v1"),
        "upstream-token",
        100,
    );
    primary.prompt_cache_key_seed = Some("stable-seed".to_string());
    let state = AppState::new(effective_config(vec![primary])).unwrap();

    let response = app(state)
        .oneshot(proxy_request(r#"{"model":"gpt-5.5","input":[]}"#))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let upstream_requests = captured.lock().await;
    let body: serde_json::Value = serde_json::from_slice(&upstream_requests[0].body).unwrap();
    assert_eq!(
        body["prompt_cache_key"].as_str(),
        Some("tp_35de55d6cc39b7f4a248e35e6ed26116")
    );
}

#[tokio::test]
async fn should_preserve_client_supplied_prompt_cache_key() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream =
        fake_upstream(StatusCode::OK, r#"{"id":"resp_ok"}"#, Arc::clone(&captured)).await;
    let mut primary = account(
        "primary",
        format!("http://{upstream}/v1"),
        "upstream-token",
        100,
    );
    primary.prompt_cache_key_seed = Some("stable-seed".to_string());
    let state = AppState::new(effective_config(vec![primary])).unwrap();

    let response = app(state)
        .oneshot(proxy_request(
            r#"{"model":"gpt-5.5","prompt_cache_key":"client-key","input":[]}"#,
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let upstream_requests = captured.lock().await;
    let body: serde_json::Value = serde_json::from_slice(&upstream_requests[0].body).unwrap();
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

#[tokio::test]
async fn should_record_request_shape_metrics_from_http_request_body() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream =
        fake_upstream(StatusCode::OK, r#"{"id":"resp_ok"}"#, Arc::clone(&captured)).await;
    let state = AppState::new(effective_config(vec![account(
        "primary",
        format!("http://{upstream}/v1"),
        "upstream-token",
        100,
    )]))
    .unwrap();

    let response = app(state.clone())
        .oneshot(proxy_request(
            r#"{"model":"gpt-5.5","store":true,"service_tier":"priority","reasoning":{"effort":"high"},"text":{"verbosity":"low"},"input":[]}"#,
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        state.metrics.snapshot().request_shapes,
        vec![(
            crate::metrics::RequestShapeMetricKey {
                endpoint: "/v1/responses".to_string(),
                model_family: "gpt-5".to_string(),
                service_tier: "priority".to_string(),
                reasoning_effort: "high".to_string(),
                verbosity: "low".to_string(),
                store: "true".to_string(),
            },
            1,
        )]
    );
}

#[tokio::test]
async fn should_record_cached_tokens_from_responses_usage_without_rewriting_body() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream_body = r#"{"id":"resp_ok","usage":{"input_tokens_details":{"cached_tokens":17}}}"#;
    let upstream = fake_upstream(StatusCode::OK, upstream_body, Arc::clone(&captured)).await;
    let state = AppState::new(effective_config(vec![account(
        "primary",
        format!("http://{upstream}/v1"),
        "upstream-token",
        100,
    )]))
    .unwrap();
    let app = app(state.clone());

    let response = app
        .clone()
        .oneshot(proxy_request(r#"{"model":"gpt-5.5","input":[]}"#))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    assert_eq!(body, upstream_body.as_bytes());

    let metrics = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .header("authorization", "Bearer client-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let metrics = to_bytes(metrics.into_body(), 1024 * 1024).await.unwrap();
    let metrics = String::from_utf8(metrics.to_vec()).unwrap();
    assert!(metrics.contains(
        r#"tokenproxy_cached_input_tokens_total{endpoint="/v1/responses",model_family="gpt-5"} 17"#
    ));
}

#[tokio::test]
async fn should_record_cached_tokens_from_chat_completions_usage_without_rewriting_body() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream_body =
        r#"{"id":"chatcmpl_ok","usage":{"prompt_tokens_details":{"cached_tokens":23}}}"#;
    let upstream = fake_upstream(StatusCode::OK, upstream_body, Arc::clone(&captured)).await;
    let state = AppState::new(effective_config(vec![account(
        "primary",
        format!("http://{upstream}/v1"),
        "upstream-token",
        100,
    )]))
    .unwrap();
    let app = app(state);

    let response = app
        .clone()
        .oneshot(chat_proxy_request(
            r#"{"model":"gpt-5.5","messages":[{"role":"user","content":"hello"}]}"#,
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    assert_eq!(body, upstream_body.as_bytes());

    let metrics = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .header("authorization", "Bearer client-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let metrics = to_bytes(metrics.into_body(), 1024 * 1024).await.unwrap();
    let metrics = String::from_utf8(metrics.to_vec()).unwrap();
    assert!(metrics.contains(
        r#"tokenproxy_cached_input_tokens_total{endpoint="/v1/chat/completions",model_family="gpt-5"} 23"#
    ));
}

#[tokio::test]
async fn should_hash_compact_request_and_response_bodies_without_dumping_raw_bodies() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream_body = r#"{"object":"response.compaction","output":"secret response"}"#;
    let upstream = fake_upstream(StatusCode::OK, upstream_body, Arc::clone(&captured)).await;
    let primary = account(
        "primary",
        format!("http://{upstream}/v1"),
        "upstream-token",
        100,
    );
    let mut effective = effective_config(vec![primary]);
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    effective.config.observability.request_body_dumps = true;
    effective.config.observability.dump_dir = std::env::temp_dir()
        .join(format!("tokenproxy-compact-hashes-{unique}"))
        .to_string_lossy()
        .into_owned();
    let dump_dir = effective.config.observability.dump_dir.clone();
    let state = AppState::new(effective).unwrap();

    let request_body =
        r#"{"model":"gpt-5.5","input":[{"role":"user","content":"secret request"}]}"#;
    let response = app(state)
        .oneshot(compact_proxy_request(request_body))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let request_sha256 = crate::observability::sha256_hex(request_body.as_bytes());
    let response_sha256 = crate::observability::sha256_hex(upstream_body.as_bytes());
    assert!(
        response
            .headers()
            .get("x-tokenproxy-request-body-sha256")
            .is_none()
    );
    assert!(
        response
            .headers()
            .get("x-tokenproxy-response-body-sha256")
            .is_none()
    );
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    assert_eq!(body, upstream_body.as_bytes());
    let compact_hash_dump =
        tokio::fs::read_to_string(format!("{dump_dir}/compact-body-hashes.jsonl"))
            .await
            .unwrap();
    assert!(compact_hash_dump.contains(&request_sha256));
    assert!(compact_hash_dump.contains(&response_sha256));
    assert!(!compact_hash_dump.contains("secret request"));
    assert!(!compact_hash_dump.contains("secret response"));
    assert!(
        !tokio::fs::try_exists(format!("{dump_dir}/request-bodies.jsonl"))
            .await
            .unwrap()
    );

    let upstream_requests = captured.lock().await;
    assert_eq!(upstream_requests.len(), 1);
    assert_eq!(upstream_requests[0].body, request_body.as_bytes());
}

#[tokio::test]
async fn should_return_json_error_before_sse_headers_when_first_frame_is_malformed() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream = fake_sse_upstream("data: {malformed-json}\n\n", Arc::clone(&captured)).await;
    let state = AppState::new(effective_config(vec![account(
        "primary",
        format!("http://{upstream}/v1"),
        "upstream-token",
        100,
    )]))
    .unwrap();

    let app = app(state);
    let response = app
        .clone()
        .oneshot(proxy_request(
            r#"{"model":"gpt-5.5","stream":true,"input":[]}"#,
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert_ne!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream")
    );
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["code"], "invalid_json");
    assert_eq!(captured.lock().await.len(), 1);
}

#[tokio::test]
async fn should_commit_sse_headers_after_first_valid_event_and_forward_stream() {
    let captured = Arc::new(Mutex::new(Vec::new()));
    let upstream_body = "data: {\"type\":\"response.created\"}\n\ndata: [DONE]\n\n";
    let upstream = fake_sse_upstream(upstream_body, Arc::clone(&captured)).await;
    let state = AppState::new(effective_config(vec![account(
        "primary",
        format!("http://{upstream}/v1"),
        "upstream-token",
        100,
    )]))
    .unwrap();

    let app = app(state);
    let response = app
        .clone()
        .oneshot(proxy_request(
            r#"{"model":"gpt-5.5","stream":true,"input":[]}"#,
        ))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("text/event-stream")
    );
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    assert_eq!(body, upstream_body.as_bytes());
    assert_eq!(captured.lock().await.len(), 1);

    let metrics = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .header("authorization", "Bearer client-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let metrics = to_bytes(metrics.into_body(), 1024 * 1024).await.unwrap();
    let metrics = String::from_utf8(metrics.to_vec()).unwrap();
    assert!(metrics.contains(&format!(
        r#"tokenproxy_requests_total{{endpoint="/v1/responses",transport="sse",status_class="2xx",model_family="gpt-5",account_id_hash="{}"}} 1"#,
        account_id_hash("primary", "test-account-hash-key")
    )));
    assert!(metrics.contains(
        r#"tokenproxy_first_event_duration_ms_bucket{endpoint="/v1/responses",transport="sse",model_family="gpt-5""#
    ));
    assert!(metrics.contains(&format!(
        r#"tokenproxy_sse_events_total{{event_type="response.created",success="true",model_family="gpt-5",account_id_hash="{}"}} 1"#,
        account_id_hash("primary", "test-account-hash-key")
    )));
}

#[tokio::test]
async fn should_retry_once_before_commit_and_report_metric_attempts() {
    let first_count = Arc::new(Mutex::new(Vec::new()));
    let second_count = Arc::new(Mutex::new(Vec::new()));
    let first = fake_upstream(
        StatusCode::SERVICE_UNAVAILABLE,
        r#"{"error":"first"}"#,
        Arc::clone(&first_count),
    )
    .await;
    let second = fake_upstream(
        StatusCode::OK,
        r#"{"id":"resp_ok"}"#,
        Arc::clone(&second_count),
    )
    .await;
    let state = AppState::new(effective_config(vec![
        account("first", format!("http://{first}/v1"), "first-token", 100),
        account("second", format!("http://{second}/v1"), "second-token", 90),
    ]))
    .unwrap();
    let app = app(state.clone());

    let response = app
        .clone()
        .oneshot(proxy_request(r#"{"model":"gpt-5.5","input":[]}"#))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(first_count.lock().await.len(), 1);
    assert_eq!(second_count.lock().await.len(), 1);
    assert!(matches!(
        state.account_health_cell("first").unwrap().load(),
        AccountHealth::Throttled { .. }
    ));

    let metrics = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .header("authorization", "Bearer client-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(metrics.into_body(), 1024 * 1024).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();

    assert!(body.contains("tokenproxy_requests_total 1"));
    assert!(body.contains("tokenproxy_upstream_attempts_total 2"));
    assert!(body.contains(&format!(
        r#"tokenproxy_upstream_attempts_total{{endpoint="/v1/responses",transport="http",model_family="gpt-5",account_id_hash="{}",retry_phase="initial",outcome="5xx"}} 1"#,
        account_id_hash("first", "test-account-hash-key")
    )));
    assert!(body.contains(&format!(
        r#"tokenproxy_upstream_attempts_total{{endpoint="/v1/responses",transport="http",model_family="gpt-5",account_id_hash="{}",retry_phase="retry",outcome="2xx"}} 1"#,
        account_id_hash("second", "test-account-hash-key")
    )));
}

#[tokio::test]
async fn should_retry_once_before_commit_after_transport_error() {
    let second_count = Arc::new(Mutex::new(Vec::new()));
    let second = fake_upstream(
        StatusCode::OK,
        r#"{"id":"resp_ok"}"#,
        Arc::clone(&second_count),
    )
    .await;
    let state = AppState::new(effective_config(vec![
        account(
            "first",
            "http://127.0.0.1:1/v1".to_string(),
            "first-token",
            100,
        ),
        account("second", format!("http://{second}/v1"), "second-token", 90),
    ]))
    .unwrap();
    let app = app(state.clone());

    let response = app
        .clone()
        .oneshot(proxy_request(r#"{"model":"gpt-5.5","input":[]}"#))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(second_count.lock().await.len(), 1);
    assert!(matches!(
        state.account_health_cell("first").unwrap().load(),
        AccountHealth::Throttled { .. }
    ));

    let metrics = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .header("authorization", "Bearer client-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(metrics.into_body(), 1024 * 1024).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();

    assert!(body.contains("tokenproxy_upstream_attempts_total 2"));
    assert!(body.contains(&format!(
        r#"tokenproxy_upstream_attempts_total{{endpoint="/v1/responses",transport="http",model_family="gpt-5",account_id_hash="{}",retry_phase="initial",outcome="transport_error"}} 1"#,
        account_id_hash("first", "test-account-hash-key")
    )));
    assert!(body.contains(&format!(
        r#"tokenproxy_upstream_attempts_total{{endpoint="/v1/responses",transport="http",model_family="gpt-5",account_id_hash="{}",retry_phase="retry",outcome="2xx"}} 1"#,
        account_id_hash("second", "test-account-hash-key")
    )));
}

#[tokio::test]
async fn should_not_retry_auth_failure_on_another_account() {
    let first_count = Arc::new(Mutex::new(Vec::new()));
    let second_count = Arc::new(Mutex::new(Vec::new()));
    let first = fake_upstream(
        StatusCode::UNAUTHORIZED,
        r#"{"error":{"code":"invalid_api_key"}}"#,
        Arc::clone(&first_count),
    )
    .await;
    let second = fake_upstream(
        StatusCode::OK,
        r#"{"id":"resp_ok"}"#,
        Arc::clone(&second_count),
    )
    .await;
    let state = AppState::new(effective_config(vec![
        account("first", format!("http://{first}/v1"), "first-token", 100),
        account("second", format!("http://{second}/v1"), "second-token", 90),
    ]))
    .unwrap();

    let response = app(state.clone())
        .oneshot(proxy_request(r#"{"model":"gpt-5.5","input":[]}"#))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(first_count.lock().await.len(), 1);
    assert_eq!(second_count.lock().await.len(), 0);
    assert_eq!(
        state.account_health_cell("first").unwrap().load(),
        AccountHealth::AuthFailed
    );
}

#[tokio::test]
async fn should_record_usage_limit_error_and_route_away_until_reset() {
    let first_count = Arc::new(Mutex::new(Vec::new()));
    let second_count = Arc::new(Mutex::new(Vec::new()));
    let first = fake_upstream(
        StatusCode::TOO_MANY_REQUESTS,
        r#"{"error":{"code":"usage_limit_reached","resets_in_seconds":600}}"#,
        Arc::clone(&first_count),
    )
    .await;
    let second = fake_upstream(
        StatusCode::OK,
        r#"{"id":"resp_ok"}"#,
        Arc::clone(&second_count),
    )
    .await;
    let state = AppState::new(effective_config(vec![
        account("first", format!("http://{first}/v1"), "first-token", 100),
        account("second", format!("http://{second}/v1"), "second-token", 90),
    ]))
    .unwrap();
    let app = app(state);

    let limited = app
        .clone()
        .oneshot(proxy_request(r#"{"model":"gpt-5.5","input":[]}"#))
        .await
        .unwrap();
    assert_eq!(limited.status(), StatusCode::OK);
    let limited_body = to_bytes(limited.into_body(), 1024 * 1024).await.unwrap();
    let limited_body: serde_json::Value = serde_json::from_slice(&limited_body).unwrap();
    assert_eq!(limited_body["id"], "resp_ok");

    let usage = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/usage")
                .header("authorization", "Bearer client-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let usage_body = to_bytes(usage.into_body(), 1024 * 1024).await.unwrap();
    let usage_body: serde_json::Value = serde_json::from_slice(&usage_body).unwrap();
    assert_eq!(usage_body["accounts"][0]["health"], "usage_limited");
    assert_eq!(
        usage_body["accounts"][0]["usage"][1]["source"],
        "usage_limit_reached_error"
    );

    let routed_away = app
        .oneshot(proxy_request(r#"{"model":"gpt-5.5","input":[]}"#))
        .await
        .unwrap();
    assert_eq!(routed_away.status(), StatusCode::OK);
    assert_eq!(first_count.lock().await.len(), 1);
    assert_eq!(second_count.lock().await.len(), 2);
}

#[tokio::test]
async fn should_return_no_eligible_account_when_all_compatible_accounts_are_usage_limited() {
    let first_count = Arc::new(Mutex::new(Vec::new()));
    let second_count = Arc::new(Mutex::new(Vec::new()));
    let usage_limited_body = r#"{"error":{"code":"usage_limit_reached","resets_in_seconds":600}}"#;
    let first = fake_upstream(
        StatusCode::TOO_MANY_REQUESTS,
        usage_limited_body,
        Arc::clone(&first_count),
    )
    .await;
    let second = fake_upstream(
        StatusCode::TOO_MANY_REQUESTS,
        usage_limited_body,
        Arc::clone(&second_count),
    )
    .await;
    let state = AppState::new(effective_config(vec![
        account("first", format!("http://{first}/v1"), "first-token", 100),
        account("second", format!("http://{second}/v1"), "second-token", 90),
    ]))
    .unwrap();
    let app = app(state);

    let response = app
        .oneshot(proxy_request(r#"{"model":"gpt-5.5","input":[]}"#))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["code"], "no_eligible_account");
    assert!(
        body["error"]["message"]
            .as_str()
            .unwrap()
            .contains("all compatible accounts are usage-limited")
    );
    assert_eq!(first_count.lock().await.len(), 1);
    assert_eq!(second_count.lock().await.len(), 1);
}

#[tokio::test]
async fn should_require_auth_for_operator_routes_except_healthz() {
    let state = AppState::new(effective_config(vec![account(
        "primary",
        "http://127.0.0.1:1/v1".to_string(),
        "upstream-token",
        100,
    )]))
    .unwrap();
    let app = app(state);

    let health = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health.status(), StatusCode::OK);

    let health_wrong_method = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(health_wrong_method.status(), StatusCode::UNAUTHORIZED);

    let authenticated_health_wrong_method = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/healthz")
                .header("authorization", "Bearer client-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        authenticated_health_wrong_method.status(),
        StatusCode::METHOD_NOT_ALLOWED
    );

    let x_api_key_authenticated_usage = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/usage")
                .header("x-api-key", "client-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(x_api_key_authenticated_usage.status(), StatusCode::OK);

    let body = to_bytes(health_wrong_method.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["code"], "unauthorized");

    let usage = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/usage")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(usage.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn should_include_disabled_configured_accounts_in_usage_snapshot() {
    let mut effective = effective_config(vec![account(
        "primary",
        "http://127.0.0.1:1/v1".to_string(),
        "upstream-token",
        100,
    )]);
    effective.config.accounts = effective
        .accounts
        .iter()
        .map(|account| account.config.clone())
        .collect();
    effective.config.accounts.push(AccountConfig {
        id: "disabled".to_string(),
        display_name: Some("paused account".to_string()),
        enabled: false,
        ..AccountConfig::default()
    });

    let response = app(AppState::new(effective).unwrap())
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/usage")
                .header("authorization", "Bearer client-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["accounts"][0]["health"], "open");
    assert_eq!(body["accounts"][1]["display_name"], "paused account");
    assert_eq!(body["accounts"][1]["health"], "disabled");
    assert!(body["accounts"][1]["usage"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn should_include_runtime_throttle_cooldown_in_usage_snapshot() {
    let state = AppState::new(effective_config(vec![account(
        "primary",
        "http://127.0.0.1:1/v1".to_string(),
        "upstream-token",
        100,
    )]))
    .unwrap();
    let retry_at = chrono::DateTime::parse_from_rfc3339("2026-05-27T11:25:18Z")
        .unwrap()
        .timestamp_millis() as u64;
    state.store_account_health(
        "primary",
        AccountHealth::Throttled {
            next_retry_at_ms: retry_at,
        },
    );

    let response = app(state)
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/usage")
                .header("authorization", "Bearer client-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["accounts"][0]["health"], "throttled");
    assert_eq!(
        body["accounts"][0]["cooldown_until"],
        serde_json::json!("2026-05-27T11:25:18Z")
    );
}

#[tokio::test]
async fn should_write_redacted_request_body_dump_before_json_route_rejection() {
    let mut effective = effective_config(vec![account(
        "primary",
        "http://127.0.0.1:1/v1".to_string(),
        "upstream-token",
        100,
    )]);
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    effective.config.observability.request_body_dumps = true;
    effective.config.observability.dump_dir = std::env::temp_dir()
        .join(format!("tokenproxy-request-dumps-{unique}"))
        .to_string_lossy()
        .into_owned();
    effective.config.observability.redact_json_pointers = vec!["/api_key".to_string()];
    let dump_dir = effective.config.observability.dump_dir.clone();
    let state = AppState::new(effective).unwrap();

    let response = app(state)
        .oneshot(proxy_request(r#"{"api_key":"secret"}"#))
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let dump = tokio::fs::read_to_string(format!("{dump_dir}/request-bodies.jsonl"))
        .await
        .unwrap();
    let dump: serde_json::Value = serde_json::from_str(dump.trim()).unwrap();
    assert!(
        dump["body_sha256"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    assert_eq!(dump["headers"]["authorization"], "[redacted]");
    assert_eq!(dump["headers"]["content-type"], "application/json");
    assert_eq!(dump["body_json"]["api_key"], "[redacted]");
    assert!(!dump.to_string().contains("secret"));
    assert!(!dump.to_string().contains("client-key"));
}

#[tokio::test]
async fn should_return_tokenproxy_error_envelope_for_unsupported_route_and_method() {
    let state = AppState::new(effective_config(vec![account(
        "primary",
        "http://127.0.0.1:1/v1".to_string(),
        "upstream-token",
        100,
    )]))
    .unwrap();
    let app = app(state.clone());

    let missing = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/files")
                .header("authorization", "Bearer client-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    let body = to_bytes(missing.into_body(), 1024 * 1024).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["code"], "unsupported_route");
    assert!(
        body["error"]["tokenproxy_request_id"]
            .as_str()
            .is_some_and(|request_id| request_id.starts_with("req_"))
    );

    let outside_route_set = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/not-openai")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(outside_route_set.status(), StatusCode::NOT_FOUND);
    let body = to_bytes(outside_route_set.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["code"], "unsupported_route");
    assert!(
        body["error"]["tokenproxy_request_id"]
            .as_str()
            .is_some_and(|request_id| request_id.starts_with("req_"))
    );

    let wrong_method = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/chat/completions")
                .header("authorization", "Bearer client-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(wrong_method.status(), StatusCode::NOT_FOUND);
    let body = to_bytes(wrong_method.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["code"], "unsupported_route");
    assert!(
        body["error"]["tokenproxy_request_id"]
            .as_str()
            .is_some_and(|request_id| request_id.starts_with("req_"))
    );

    // Unmatched routes share one fixed metric label so request paths cannot
    // grow metric cardinality without bound.
    let metrics = state.metrics.snapshot();
    assert_eq!(
        metrics.request_outcomes,
        vec![(
            crate::metrics::RequestMetricKey {
                endpoint: "unmatched".to_string(),
                transport: "http".to_string(),
                status_class: "4xx".to_string(),
                model_family: "unknown".to_string(),
                account_id_hash: "none".to_string(),
            },
            3
        )]
    );
    assert_eq!(metrics.request_duration_count, 3);
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
async fn should_record_request_duration_for_invalid_proxy_json() {
    let state = AppState::new(effective_config(vec![account(
        "primary",
        "http://127.0.0.1:1/v1".to_string(),
        "upstream-token",
        100,
    )]))
    .unwrap();
    let app = app(state);

    let response = app
        .clone()
        .oneshot(proxy_request(r#"{"model":"#))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["code"], "invalid_json");
    assert_eq!(
        body["error"]["tokenproxy_request_id"],
        "req_0000000000000001"
    );

    let metrics = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .header("authorization", "Bearer client-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(metrics.into_body(), 1024 * 1024).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();

    assert!(body.contains("tokenproxy_request_duration_ms_count 1"));
    assert!(body.contains(
        r#"tokenproxy_requests_total{endpoint="/v1/responses",transport="http",status_class="4xx",model_family="unknown",account_id_hash="none"} 1"#
    ));
}

#[tokio::test]
async fn should_record_request_metrics_for_compressed_proxy_body() {
    let state = AppState::new(effective_config(vec![account(
        "primary",
        "http://127.0.0.1:1/v1".to_string(),
        "upstream-token",
        100,
    )]))
    .unwrap();
    let app = app(state);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/responses")
                .header("authorization", "Bearer client-key")
                .header("content-type", "application/json")
                .header("content-encoding", "gzip")
                .body(Body::from(r#"{"model":"gpt-5.5"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["code"], "unsupported_media_type");
    assert_eq!(
        body["error"]["tokenproxy_request_id"],
        "req_0000000000000001"
    );

    let metrics = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/metrics")
                .header("authorization", "Bearer client-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = to_bytes(metrics.into_body(), 1024 * 1024).await.unwrap();
    let body = String::from_utf8(body.to_vec()).unwrap();

    assert!(body.contains("tokenproxy_requests_total 1"));
    assert!(body.contains("tokenproxy_request_duration_ms_count 1"));
    assert!(body.contains(
        r#"tokenproxy_requests_total{endpoint="/v1/responses",transport="http",status_class="4xx",model_family="unknown",account_id_hash="none"} 1"#
    ));
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
    assert_eq!(body["error"]["code"], "unsupported_method");
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
    assert_eq!(body["error"]["code"], "unsupported_method");

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

    replay_state.requested_service_tier =
        normalized_requested_service_tier(Some("fast".to_string()));
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
    let versioned_messages = upstream_url_for_path(&versioned_anthropic, "/v1/messages").unwrap();

    assert_eq!(messages.as_str(), "https://api.anthropic.com/v1/messages");
    assert_eq!(
        versioned_messages.as_str(),
        "https://api.anthropic.com/v1/messages"
    );
    assert!(upstream_url_for_path(&anthropic, "/v1/responses").is_err());
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

    let error = ensure_upstream_session(&state, &selected, &mut session, "gpt", "req_ws_failed")
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

#[tokio::test]
async fn should_reject_second_websocket_create_while_first_is_in_flight() {
    let captured_messages = Arc::new(Mutex::new(Vec::new()));
    let upstream = fake_delayed_websocket_upstream(Arc::clone(&captured_messages)).await;
    let state = AppState::new(effective_config(vec![account(
        "primary",
        format!("http://{upstream}/v1"),
        "upstream-token",
        100,
    )]))
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app(state)).await.unwrap();
    });
    let mut request = format!("ws://{proxy}/v1/responses")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", "Bearer client-key".parse().unwrap());
    let (mut socket, _) = connect_async(request).await.unwrap();
    let first = serde_json::json!({
        "type": "response.create",
        "model": "gpt-5.5",
        "input": "first"
    })
    .to_string();
    let second = serde_json::json!({
        "type": "response.create",
        "model": "gpt-5.5",
        "input": "second"
    })
    .to_string();

    socket
        .send(UpstreamMessage::Text(first.into()))
        .await
        .unwrap();
    socket
        .send(UpstreamMessage::Text(second.into()))
        .await
        .unwrap();

    let mut saw_in_flight_error = false;
    for _ in 0..3 {
        let Some(message) = tokio::time::timeout(Duration::from_millis(250), socket.next())
            .await
            .unwrap()
        else {
            break;
        };
        let message = message.unwrap();
        if let UpstreamMessage::Text(text) = message {
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            if value
                .pointer("/error/code")
                .and_then(serde_json::Value::as_str)
                == Some("websocket_in_flight")
            {
                saw_in_flight_error = true;
                break;
            }
        }
    }

    sleep(Duration::from_millis(75)).await;
    assert!(saw_in_flight_error);
    assert_eq!(captured_messages.lock().await.len(), 1);
    let metrics = reqwest::Client::new()
        .get(format!("http://{proxy}/metrics"))
        .header("authorization", "Bearer client-key")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains(
        r#"tokenproxy_ws_events_total{event_type="downstream_create",success="false"} 1"#
    ));
    assert!(
        metrics
            .contains(r#"tokenproxy_ws_events_total{event_type="upstream_text",success="true"} 1"#)
    );
}

#[tokio::test]
async fn should_close_binary_websocket_input_with_protocol_metric() {
    let state = AppState::new(effective_config(vec![account(
        "primary",
        "http://127.0.0.1:1/v1".to_string(),
        "upstream-token",
        100,
    )]))
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app(state)).await.unwrap();
    });
    let mut request = format!("ws://{proxy}/v1/responses")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", "Bearer client-key".parse().unwrap());
    let (mut socket, _) = connect_async(request).await.unwrap();

    socket
        .send(UpstreamMessage::Binary(Bytes::from_static(b"not-json")))
        .await
        .unwrap();
    let close = tokio::time::timeout(Duration::from_millis(250), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();

    let UpstreamMessage::Close(Some(frame)) = close else {
        panic!("expected close frame for binary input");
    };
    assert_eq!(frame.code.to_string(), "1003");

    let metrics = reqwest::Client::new()
        .get(format!("http://{proxy}/metrics"))
        .header("authorization", "Bearer client-key")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains(
        r#"tokenproxy_ws_events_total{event_type="downstream_binary",success="false"} 1"#
    ));
    assert!(
        !metrics.contains(
            r#"tokenproxy_ws_events_total{event_type="downstream_close",success="true"} 1"#
        )
    );
}

#[tokio::test]
async fn should_include_request_id_in_websocket_error_frames() {
    let state = AppState::new(effective_config(vec![account(
        "primary",
        "http://127.0.0.1:1/v1".to_string(),
        "upstream-token",
        100,
    )]))
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app(state)).await.unwrap();
    });
    let mut request = format!("ws://{proxy}/v1/responses")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", "Bearer client-key".parse().unwrap());
    let (mut socket, _) = connect_async(request).await.unwrap();

    socket
        .send(UpstreamMessage::Text(
            serde_json::json!({"type":"response.append","input":[]})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let message = tokio::time::timeout(Duration::from_millis(250), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let UpstreamMessage::Text(text) = message else {
        panic!("expected WebSocket error text frame");
    };
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();

    assert_eq!(
        value
            .pointer("/error/code")
            .and_then(serde_json::Value::as_str),
        Some("websocket_unsupported_message")
    );
    assert_eq!(
        value
            .pointer("/error/tokenproxy_request_id")
            .and_then(serde_json::Value::as_str),
        Some("req_0000000000000001")
    );
}

#[tokio::test]
async fn should_timeout_idle_websocket_response() {
    let captured_messages = Arc::new(Mutex::new(Vec::new()));
    let upstream = fake_delayed_websocket_upstream(Arc::clone(&captured_messages)).await;
    let mut effective = effective_config(vec![account(
        "primary",
        format!("http://{upstream}/v1"),
        "upstream-token",
        100,
    )]);
    effective.config.timeouts.websocket_idle_ms = 10;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app(AppState::new(effective).unwrap()))
            .await
            .unwrap();
    });
    let mut request = format!("ws://{proxy}/v1/responses")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", "Bearer client-key".parse().unwrap());
    let (mut socket, _) = connect_async(request).await.unwrap();
    let payload = serde_json::json!({
        "type": "response.create",
        "model": "gpt-5.5",
        "input": "first"
    })
    .to_string();

    socket
        .send(UpstreamMessage::Text(payload.into()))
        .await
        .unwrap();

    let message = tokio::time::timeout(Duration::from_millis(250), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let UpstreamMessage::Text(text) = message else {
        panic!("expected websocket error text frame");
    };
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();

    assert_eq!(value["error"]["code"], "upstream_failure");
    assert!(
        value["error"]["message"]
            .as_str()
            .unwrap()
            .contains("WebSocket idle timeout")
    );
    assert_eq!(captured_messages.lock().await.len(), 1);
}

#[tokio::test]
async fn should_record_downstream_websocket_close_during_in_flight_response() {
    let captured_messages = Arc::new(Mutex::new(Vec::new()));
    let upstream = fake_delayed_websocket_upstream(Arc::clone(&captured_messages)).await;
    let state = AppState::new(effective_config(vec![account(
        "primary",
        format!("http://{upstream}/v1"),
        "upstream-token",
        100,
    )]))
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app(state)).await.unwrap();
    });
    let mut request = format!("ws://{proxy}/v1/responses")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", "Bearer client-key".parse().unwrap());
    let (mut socket, _) = connect_async(request).await.unwrap();
    let payload = serde_json::json!({
        "type": "response.create",
        "model": "gpt-5.5",
        "input": "first"
    })
    .to_string();

    socket
        .send(UpstreamMessage::Text(payload.into()))
        .await
        .unwrap();
    socket.close(None).await.unwrap();
    sleep(Duration::from_millis(75)).await;

    let metrics = reqwest::Client::new()
        .get(format!("http://{proxy}/metrics"))
        .header("authorization", "Bearer client-key")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert_eq!(captured_messages.lock().await.len(), 1);
    assert!(
        metrics.contains(
            r#"tokenproxy_ws_events_total{event_type="downstream_close",success="true"} 1"#
        )
    );
}

#[tokio::test]
async fn should_record_upstream_websocket_drop_during_in_flight_response() {
    let captured_messages = Arc::new(Mutex::new(Vec::new()));
    let upstream = fake_closing_websocket_upstream(Arc::clone(&captured_messages)).await;
    let state = AppState::new(effective_config(vec![account(
        "primary",
        format!("http://{upstream}/v1"),
        "upstream-token",
        100,
    )]))
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app(state)).await.unwrap();
    });
    let mut request = format!("ws://{proxy}/v1/responses")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", "Bearer client-key".parse().unwrap());
    let (mut socket, _) = connect_async(request).await.unwrap();
    let payload = serde_json::json!({
        "type": "response.create",
        "model": "gpt-5.5",
        "input": "first"
    })
    .to_string();

    socket
        .send(UpstreamMessage::Text(payload.into()))
        .await
        .unwrap();
    let message = tokio::time::timeout(Duration::from_millis(250), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let UpstreamMessage::Text(text) = message else {
        panic!("expected websocket error text frame");
    };
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();

    let metrics = reqwest::Client::new()
        .get(format!("http://{proxy}/metrics"))
        .header("authorization", "Bearer client-key")
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    assert_eq!(value["error"]["code"], "upstream_failure");
    assert!(
        value["error"]["message"]
            .as_str()
            .unwrap()
            .contains("closed before response completed")
    );
    assert_eq!(captured_messages.lock().await.len(), 1);
    assert!(
        metrics.contains(
            r#"tokenproxy_ws_events_total{event_type="upstream_close",success="false"} 1"#
        )
    );
}

#[tokio::test]
async fn should_drain_in_flight_websocket_response_after_shutdown_signal() {
    let captured_messages = Arc::new(Mutex::new(Vec::new()));
    let upstream = fake_delayed_websocket_upstream(Arc::clone(&captured_messages)).await;
    let (shutdown_tx, _) = watch::channel(false);
    let mut effective = effective_config(vec![account(
        "primary",
        format!("http://{upstream}/v1"),
        "upstream-token",
        100,
    )]);
    effective.config.server.shutdown_grace_ms = 250;
    let state =
        AppState::new_with_log_format_and_shutdown(effective, LogFormat::Text, shutdown_tx.clone())
            .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app(state)).await.unwrap();
    });
    let mut request = format!("ws://{proxy}/v1/responses")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", "Bearer client-key".parse().unwrap());
    let (mut socket, _) = connect_async(request).await.unwrap();
    let payload = serde_json::json!({
        "type": "response.create",
        "model": "gpt-5.5",
        "input": "first"
    })
    .to_string();

    socket
        .send(UpstreamMessage::Text(payload.into()))
        .await
        .unwrap();
    sleep(Duration::from_millis(10)).await;
    shutdown_tx.send(true).unwrap();

    let message = tokio::time::timeout(Duration::from_millis(250), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let UpstreamMessage::Text(text) = message else {
        panic!("expected completed upstream text before shutdown close");
    };
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();

    assert_eq!(value["type"], "response.completed");
    assert_eq!(captured_messages.lock().await.len(), 1);
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

    record_account_http_status(&state, &selected, StatusCode::OK, &HeaderMap::new(), None).await;
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

    record_account_http_status(&state, &selected, StatusCode::OK, &HeaderMap::new(), None).await;

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
async fn should_not_clear_transient_throttle_after_previous_response_not_found_websocket_event() {
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

    record_account_http_status(&state, &selected, StatusCode::OK, &HeaderMap::new(), None).await;

    assert_eq!(
        state
            .account_health_cell("primary")
            .unwrap()
            .transient_failure_count(),
        0
    );
}

#[test]
fn should_reset_transient_failure_count_after_quiet_window() {
    let cell = AccountHealthCell::new();

    assert_eq!(cell.increment_transient_failure_count_at(1_000), 1);
    assert_eq!(cell.increment_transient_failure_count_at(2_000), 2);
    assert_eq!(cell.increment_transient_failure_count_at(302_000), 1);
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
async fn should_record_sse_client_cancellation_when_repaired_body_is_dropped() {
    let metrics = Metrics::default();
    let stream = futures_util::stream::pending::<Result<Bytes, std::io::Error>>();
    let mut repaired = Box::pin(repair_sse_stream_from_state(
        Box::pin(stream),
        SseRepair::default(),
        VecDeque::from([Bytes::from_static(b"event: response.created\n\n")]),
        false,
        Duration::from_secs(300),
        Some(metrics.clone()),
        SseMetricContext::new("gpt-5", "acct_primary"),
    ));

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
    let mut repaired = Box::pin(repair_sse_stream_from_state(
        Box::pin(stream),
        SseRepair::default(),
        VecDeque::from([Bytes::from_static(b"event: response.created\n\n")]),
        false,
        Duration::from_secs(300),
        Some(metrics.clone()),
        SseMetricContext::new("gpt-5", "acct_primary"),
    ));

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
