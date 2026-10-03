use tokenproxy::config::Config;

#[test]
fn should_accept_native_cerebras_account() {
    let config = toml::from_str::<Config>(
        r#"accounts = [{ id = "cerebras", kind = "cerebras_api_key", token_env = "CEREBRAS_API_KEY", supports_responses = true }]"#,
    );
    assert!(config.is_ok(), "{config:?}");
}

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use serde_json::{Value, json};
use tokenproxy::config::{AccountConfig, AccountKind, EffectiveAccount, EffectiveConfig};
use tokenproxy::server::{AppState, app};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

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

#[derive(Default)]
struct Mock {
    requests: Mutex<Vec<(HeaderMap, Value)>>,
    replies: Mutex<VecDeque<(StatusCode, &'static str, String)>>,
}

async fn upstream(replies: Vec<(StatusCode, &'static str, String)>) -> (Server, Arc<Mock>) {
    let state = Arc::new(Mock {
        replies: Mutex::new(replies.into()),
        ..Default::default()
    });
    let server = serve(
        Router::new()
            .route("/v1/chat/completions", post(mock))
            .with_state(state.clone()),
    )
    .await;
    (server, state)
}

async fn mock(State(state): State<Arc<Mock>>, headers: HeaderMap, body: Bytes) -> Response {
    state
        .requests
        .lock()
        .unwrap()
        .push((headers, serde_json::from_slice(&body).unwrap()));
    let (status, content_type, body) = state.replies.lock().unwrap().pop_front().unwrap();
    if content_type == "text/event-stream" {
        let chunks: Vec<Result<Bytes, std::io::Error>> = body
            .as_bytes()
            .chunks(7)
            .map(|b| Ok(Bytes::copy_from_slice(b)))
            .collect();
        (
            status,
            [("content-type", content_type)],
            Body::from_stream(futures_util::stream::iter(chunks)),
        )
            .into_response()
    } else {
        (status, [("content-type", content_type)], body).into_response()
    }
}

fn account(server: &Server, id: &str, priority: i32, kind: AccountKind) -> EffectiveAccount {
    EffectiveAccount {
        config: AccountConfig {
            id: id.into(),
            priority,
            kind,
            base_url: format!("{}/v1", server.url),
            models: vec!["qwen-3.8-27b".into()],
            supports_responses: true,
            supports_chat_completions: true,
            supports_incremental_previous_response_id: false,
            ..Default::default()
        },
        bearer_token: "cerebras-secret".into(),
        chatgpt_account_id: None,
        auth_json: None,
        prompt_cache_key_seed: None,
    }
}

async fn proxy(accounts: Vec<EffectiveAccount>) -> Server {
    let mut config = Config::default();
    config.accounts = accounts.iter().map(|a| a.config.clone()).collect();
    config.server.allow_insecure_upstream = true;
    config.retry.max_precommit_retries = 1;
    config.retry.base_backoff_ms = 0;
    config.retry.max_backoff_ms = 0;
    config.retry.honor_retry_after = false;
    config.timeouts.stream_idle_ms = 1000;
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

fn completion() -> Value {
    json!({"id":"chat_1","choices":[{"index":0,"message":{"role":"assistant","content":"OK"},"finish_reason":"stop"}],
        "usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12}})
}

fn json_reply() -> (StatusCode, &'static str, String) {
    (StatusCode::OK, "application/json", completion().to_string())
}
fn sse_reply(body: String) -> (StatusCode, &'static str, String) {
    (StatusCode::OK, "text/event-stream", body)
}
fn frame(value: Value) -> String {
    format!("data: {value}\n\n")
}
fn request_body() -> Value {
    json!({"model":"qwen-3.8-27b","input":"Reply OK","store":false})
}

async fn request(proxy: &Server, path: &str, body: Value) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{}{path}", proxy.url))
        .bearer_auth("client-secret")
        .header("chatgpt-account-id", "must-not-leak")
        .header("openai-organization", "must-not-leak")
        .json(&body)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn should_route_native_responses_with_cerebras_auth_and_instructions() {
    let (upstream, captured) = upstream(vec![json_reply()]).await;
    let proxy = proxy(vec![account(
        &upstream,
        "cerebras",
        0,
        AccountKind::CerebrasApiKey,
    )])
    .await;
    let mut body = request_body();
    body["instructions"] = json!("A");
    body["input"] =
        json!([{"role":"user","content":"question"},{"role":"developer","content":"B"}]);
    let response = request(&proxy, "/v1/responses", body).await;
    assert_eq!(response.status(), StatusCode::OK);
    let response: Value = response.json().await.unwrap();
    assert_eq!(response["output"][0]["content"][0]["text"], "OK");
    assert_eq!(response["usage"]["total_tokens"], 12);
    let requests = captured.requests.lock().unwrap();
    let (headers, body) = &requests[0];
    assert_eq!(headers["authorization"], "Bearer cerebras-secret");
    assert!(
        headers["user-agent"]
            .to_str()
            .unwrap()
            .starts_with("tokenproxy/")
    );
    assert!(!headers.contains_key("chatgpt-account-id"));
    assert!(!headers.contains_key("openai-organization"));
    assert_eq!(
        body["messages"][0],
        json!({"role":"system","content":"A\n\nB"})
    );
}

#[tokio::test]
async fn should_complete_streamed_tool_roundtrip_with_fragmented_network_frames() {
    let mut stream = frame(
        json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","function":{"name":"run","arguments":"{\"command\":"}}]}}]}),
    );
    stream += &frame(
        json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"printf OK\"}"}}]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}),
    );
    stream += "data: [DONE]\n\n";
    let (upstream, captured) = upstream(vec![sse_reply(stream), json_reply()]).await;
    let proxy = proxy(vec![account(
        &upstream,
        "cerebras",
        0,
        AccountKind::CerebrasApiKey,
    )])
    .await;
    let mut body = request_body();
    body["stream"] = json!(true);
    body["tools"] = json!([{"type":"function","name":"run","parameters":{"type":"object"}}]);
    let response = request(&proxy, "/v1/responses", body.clone()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let text = response.text().await.unwrap();
    let events: Vec<Value> = text
        .lines()
        .filter_map(|l| l.strip_prefix("data: "))
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let completed = &events.last().unwrap()["response"];
    assert_eq!(completed["status"], "completed");
    let call = &completed["output"][0];
    assert_eq!(call["arguments"], r#"{"command":"printf OK"}"#);
    body["stream"] = json!(false);
    body["input"] = json!([{"role":"user","content":"run"},call,{"type":"function_call_output","call_id":call["call_id"],"output":"OK"}]);
    let response: Value = request(&proxy, "/v1/responses", body)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(response["output"][0]["content"][0]["text"], "OK");
    let requests = captured.requests.lock().unwrap();
    assert_eq!(
        requests[1].1["messages"][2],
        json!({"role":"tool","tool_call_id":"call_1","content":"OK"})
    );
}

#[tokio::test]
async fn should_roundtrip_plaintext_collaboration_and_reject_encrypted_tasks() {
    let mut stream = frame(json!({"choices":[{"index":0,"delta":{"tool_calls":[
        {"index":0,"id":"task_1","function":{"name":"collaboration__spawn_agent","arguments":"{\"message\":\"Run the probe\"}"}}
    ]},"finish_reason":"tool_calls"}]}));
    stream += "data: [DONE]\n\n";
    let (upstream, captured) = upstream(vec![sse_reply(stream), json_reply()]).await;
    let proxy = proxy(vec![account(
        &upstream,
        "cerebras",
        0,
        AccountKind::CerebrasApiKey,
    )])
    .await;
    let mut body = request_body();
    body["stream"] = json!(true);
    body["tools"] = json!([{"type":"namespace","name":"collaboration","tools":[
        {"type":"function","name":"spawn_agent","parameters":{"type":"object"}}
    ]}]);
    let response = request(&proxy, "/v1/responses", body.clone()).await;
    assert_eq!(response.status(), StatusCode::OK);
    let text = response.text().await.unwrap();
    let events: Vec<Value> = text
        .lines()
        .filter_map(|line| line.strip_prefix("data: "))
        .map(|data| serde_json::from_str(data).unwrap())
        .collect();
    let call = &events.last().unwrap()["response"]["output"][0];
    assert_eq!(call["namespace"], "collaboration");
    assert_eq!(call["encrypted_function_args"], json!([]));
    body["stream"] = json!(false);
    body["input"] = json!([
        {"role":"user","content":"Run a child"}, call,
        {"type":"function_call_output","call_id":"task_1","output":"/root/child"},
        {"type":"reasoning","summary":[{"type":"summary_text","text":"Waiting"}]},
        {"type":"agent_message","author":"/root/child","recipient":"/root",
         "content":[{"type":"input_text","text":"Probe succeeded"}]}
    ]);
    let response = request(&proxy, "/v1/responses", body.clone()).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.json::<Value>().await.unwrap()["output"][0]["content"][0]["text"],
        "OK"
    );
    body["input"][4]["content"]
        .as_array_mut()
        .unwrap()
        .push(json!({"type":"encrypted_content","encrypted_content":"ciphertext"}));
    let response = request(&proxy, "/v1/responses", body).await;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let requests = captured.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].1["messages"][3]["reasoning"], "Waiting");
    assert_eq!(
        requests[1].1["messages"][4]["content"],
        "Message from /root/child to /root:\nProbe succeeded"
    );
    assert!(requests[1].1["messages"][4].get("reasoning").is_none());
}

#[tokio::test]
async fn should_fail_over_on_429_or_invalid_first_stream_event() {
    for reply in [
        (
            StatusCode::TOO_MANY_REQUESTS,
            "application/json",
            json!({"error":{"message":"rate limited"}}).to_string(),
        ),
        sse_reply("data: malformed\n\n".into()),
        sse_reply(String::new()),
    ] {
        let (first, first_state) = upstream(vec![reply]).await;
        let (second, second_state) = upstream(vec![json_reply()]).await;
        let proxy = proxy(vec![
            account(&first, "first", 10, AccountKind::CerebrasApiKey),
            account(&second, "second", 0, AccountKind::CerebrasApiKey),
        ])
        .await;
        let response = request(&proxy, "/v1/responses", request_body()).await;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(first_state.requests.lock().unwrap().len(), 1);
        assert_eq!(second_state.requests.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn should_not_replay_after_stream_output_or_invent_completion_on_eof() {
    for tail in ["", "data: malformed\n\n", "data: [DONE]\n\n"] {
        let mut stream = frame(json!({"choices":[{"index":0,"delta":{"content":"partial"}}]}));
        stream += tail;
        let (first, _) = upstream(vec![sse_reply(stream)]).await;
        let (second, second_state) = upstream(vec![json_reply()]).await;
        let proxy = proxy(vec![
            account(&first, "first", 10, AccountKind::CerebrasApiKey),
            account(&second, "second", 0, AccountKind::CerebrasApiKey),
        ])
        .await;
        let client = reqwest::Client::new();
        let result = client
            .post(format!("{}/v1/responses", proxy.url))
            .bearer_auth("client-secret")
            .json(&json!({"model":"qwen-3.8-27b","input":"hi","stream":true}))
            .timeout(Duration::from_secs(5))
            .send()
            .await;
        if let Ok(response) = result {
            assert!(response.text().await.is_err());
        }
        assert!(second_state.requests.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn should_preserve_billing_errors_and_native_chat_responses() {
    let error = json!({"error":{"message":"add credits","code":"insufficient_quota"}});
    let (upstream, _) = upstream(vec![
        (
            StatusCode::PAYMENT_REQUIRED,
            "application/json",
            error.to_string(),
        ),
        json_reply(),
    ])
    .await;
    let proxy = proxy(vec![account(
        &upstream,
        "cerebras",
        0,
        AccountKind::CerebrasApiKey,
    )])
    .await;
    let response = request(&proxy, "/v1/responses", request_body()).await;
    assert_eq!(response.status(), StatusCode::PAYMENT_REQUIRED);
    assert_eq!(response.json::<Value>().await.unwrap(), error);
    // Use a fresh proxy because billing errors intentionally cool down the account.
    let proxy = self::proxy(vec![account(
        &upstream,
        "cerebras",
        0,
        AccountKind::CerebrasApiKey,
    )])
    .await;
    let body = json!({"model":"qwen-3.8-27b","messages":[{"role":"user","content":"hi"}]});
    let response: Value = request(&proxy, "/v1/chat/completions", body)
        .await
        .json()
        .await
        .unwrap();
    assert_eq!(response, completion());
}

#[tokio::test]
async fn should_reject_unsupported_requests_without_contacting_upstream() {
    let (upstream, state) = upstream(vec![]).await;
    let proxy = proxy(vec![account(
        &upstream,
        "cerebras",
        0,
        AccountKind::CerebrasApiKey,
    )])
    .await;
    for extra in [
        json!({"previous_response_id":"old"}),
        json!({"tools":[{"type":"web_search"}]}),
    ] {
        let mut body = request_body();
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let response = request(&proxy, "/v1/responses", body).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    assert!(state.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn should_retry_original_responses_request_when_failing_over_to_openai() {
    let (cerebras, _) = upstream(vec![(
        StatusCode::TOO_MANY_REQUESTS,
        "application/json",
        json!({"error":{"message":"rate limited"}}).to_string(),
    )])
    .await;
    let captured = Arc::new(Mutex::new(None));
    let result = json!({"id":"resp_openai","object":"response","status":"completed","output":[]});
    let reply = result.clone();
    let record = captured.clone();
    let openai = serve(Router::new().route(
        "/v1/responses",
        post(move |axum::Json(body): axum::Json<Value>| {
            let record = record.clone();
            let reply = reply.clone();
            async move {
                *record.lock().unwrap() = Some(body);
                axum::Json(reply)
            }
        }),
    ))
    .await;
    let proxy = proxy(vec![
        account(&cerebras, "cerebras", 10, AccountKind::CerebrasApiKey),
        account(&openai, "openai", 0, AccountKind::OpenAiApiKey),
    ])
    .await;
    let body = request_body();
    let response = request(&proxy, "/v1/responses", body.clone()).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.json::<Value>().await.unwrap(), result);
    assert_eq!(captured.lock().unwrap().as_ref(), Some(&body));
}
