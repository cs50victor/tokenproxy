use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use axum::{Router, body::Body, http::StatusCode, response::IntoResponse};
use serde_json::json;
use tokenproxy::config::{AccountConfig, Config, EffectiveAccount, EffectiveConfig};
use tokenproxy::server::{AppState, app};
use tokio::{net::TcpListener, task::JoinHandle};

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

async fn upstream(status: StatusCode, body: &'static str) -> (Server, Arc<AtomicUsize>) {
    let requests = Arc::new(AtomicUsize::new(0));
    let count = requests.clone();
    let router = Router::new().fallback(move || {
        let count = count.clone();
        async move {
            count.fetch_add(1, Ordering::Relaxed);
            let chunks = body
                .as_bytes()
                .chunks(3)
                .map(|chunk| Ok::<_, std::io::Error>(axum::body::Bytes::copy_from_slice(chunk)))
                .collect::<Vec<_>>();
            (
                status,
                [
                    ("content-type", "application/json"),
                    ("retry-after", "2"),
                    ("x-request-id", "provider-request-123"),
                    ("set-cookie", "upstream-secret"),
                ],
                Body::from_stream(futures_util::stream::iter(chunks)),
            )
                .into_response()
        }
    });
    (serve(router).await, requests)
}

fn account(server: &Server, id: &str, priority: i32) -> EffectiveAccount {
    EffectiveAccount {
        config: AccountConfig {
            id: id.into(),
            priority,
            base_url: format!("{}/v1", server.url),
            models: vec!["gpt-5.5".into()],
            supports_responses: true,
            supports_responses_ws: true,
            supports_chat_completions: true,
            supports_compact: true,
            ..Default::default()
        },
        bearer_token: "upstream-secret".into(),
        chatgpt_account_id: None,
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
    let effective = EffectiveConfig {
        config,
        config_update_endpoint: None,
        admin_token: None,
        downstream_token: "client-secret".into(),
        account_hash_key: "hash".into(),
        accounts,
    };
    let (shutdown_tx, _) = tokio::sync::watch::channel(false);
    let state = AppState::new_with_log_format_and_shutdown(
        effective,
        tokenproxy::logging::LogFormat::Json,
        shutdown_tx,
    )
    .unwrap();
    serve(app(state)).await
}

async fn request(proxy: &Server, path: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{}{path}", proxy.url))
        .bearer_auth("client-secret")
        .json(&json!({"model":"gpt-5.5","input":"hi", "messages":[{"role":"user","content":"hi"}]}))
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap()
}

const ERROR_BODY: &str =
    "{\"error\":{\"code\":\"provider_overloaded\",\"message\":\"original provider error\"}}";

#[tokio::test]
async fn preserves_provider_errors_when_failover_has_no_other_account() {
    for status in [
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::SERVICE_UNAVAILABLE,
    ] {
        for retries in [0, 1] {
            for path in [
                "/v1/responses",
                "/v1/chat/completions",
                "/v1/responses/compact",
                "/v1/files",
            ] {
                let (upstream, requests) = upstream(status, ERROR_BODY).await;
                let proxy = proxy(vec![account(&upstream, "primary", 100)], retries).await;
                let response = request(&proxy, path).await;
                assert_eq!(response.status(), status, "{path}, retries={retries}");
                assert_eq!(response.headers()["retry-after"], "2");
                assert_eq!(response.headers()["x-request-id"], "provider-request-123");
                assert!(!response.headers().contains_key("set-cookie"));
                assert_eq!(response.text().await.unwrap(), ERROR_BODY);
                assert_eq!(requests.load(Ordering::Relaxed), 1);
            }
        }
    }
}

#[tokio::test]
async fn successful_failover_still_returns_the_second_account_response() {
    let (first, first_requests) = upstream(StatusCode::TOO_MANY_REQUESTS, ERROR_BODY).await;
    let (second, second_requests) = upstream(StatusCode::OK, "{\"id\":\"recovered\"}").await;
    for path in ["/v1/responses", "/v1/files"] {
        let proxy = proxy(
            vec![
                account(&first, "first", 100),
                account(&second, "second", 10),
            ],
            1,
        )
        .await;
        let response = request(&proxy, path).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.text().await.unwrap(), "{\"id\":\"recovered\"}");
    }
    assert_eq!(first_requests.load(Ordering::Relaxed), 2);
    assert_eq!(second_requests.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn exhausted_failover_preserves_the_last_provider_failure() {
    let (first, _) = upstream(StatusCode::TOO_MANY_REQUESTS, ERROR_BODY).await;
    let (second, _) = upstream(
        StatusCode::INTERNAL_SERVER_ERROR,
        "{\"error\":\"second provider failed\"}",
    )
    .await;
    for path in ["/v1/responses", "/v1/files"] {
        let proxy = proxy(
            vec![
                account(&first, "first", 100),
                account(&second, "second", 10),
            ],
            1,
        )
        .await;
        let response = request(&proxy, path).await;
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(
            response.text().await.unwrap(),
            "{\"error\":\"second provider failed\"}"
        );
    }
}

#[tokio::test]
async fn subsequent_cooldown_rejections_explain_why_without_replaying_provider_errors() {
    for path in ["/v1/responses", "/v1/files"] {
        let (upstream, requests) = upstream(StatusCode::TOO_MANY_REQUESTS, ERROR_BODY).await;
        let proxy = proxy(vec![account(&upstream, "private-account-id", 100)], 1).await;
        assert_eq!(
            request(&proxy, path).await.status(),
            StatusCode::TOO_MANY_REQUESTS
        );
        let response = request(&proxy, path).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = response.text().await.unwrap();
        assert!(body.contains("throttled_cooldown"), "{body}");
        assert!(body.contains("earliest retry at unix_ms="), "{body}");
        assert!(!body.contains("private-account-id"));
        assert!(!body.contains("upstream-secret"));
        assert!(!body.contains("original provider error"));
        assert_eq!(requests.load(Ordering::Relaxed), 1);
    }
}
