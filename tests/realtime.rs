use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::extract::ws::{CloseFrame, Message, WebSocketUpgrade};
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use futures_util::{SinkExt, StreamExt};
use tokenproxy::config::{AccountConfig, AccountKind, Config, EffectiveAccount, EffectiveConfig};
use tokenproxy::logging::LogFormat;
use tokenproxy::server::{AppState, app};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Barrier, Notify, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio_tungstenite::tungstenite::{self, client::IntoClientRequest};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const DEADLINE: Duration = Duration::from_secs(15);
const CALLS: &str = "/backend-api/codex/realtime/calls";
const QUERY: &str = "intent=quicksilver&architecture=avas";
const OFFER: &str = "{ \"sdp\":\"v=0\\r\\no=offer\", \"session\":{\"model\":\"gpt-live-1-codex\",\"future_option\":true} }";
const ANSWER: &str = "v=0\r\no=answer\r\ns=mock\r\n";
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Clone, Copy)]
enum Mode {
    Echo,
    Close,
    Abrupt,
    Stall,
    Oversized,
    Interleaved,
}

#[derive(Clone)]
struct Recorded {
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
}

struct Mock {
    name: &'static str,
    mode: Mode,
    status: StatusCode,
    body: String,
    location: Option<String>,
    handshake_status: Option<StatusCode>,
    stall_call: bool,
    stall_handshake: bool,
    broken_body: bool,
    calls: Mutex<Vec<Recorded>>,
    handshakes: Mutex<Vec<Recorded>>,
    messages: AtomicUsize,
    active: AtomicUsize,
    disconnected: Notify,
    release: Notify,
}

impl Mock {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            mode: Mode::Echo,
            status: StatusCode::CREATED,
            body: ANSWER.to_owned(),
            location: None,
            handshake_status: None,
            stall_call: false,
            stall_handshake: false,
            broken_body: false,
            calls: Mutex::new(Vec::new()),
            handshakes: Mutex::new(Vec::new()),
            messages: AtomicUsize::new(0),
            active: AtomicUsize::new(0),
            disconnected: Notify::new(),
            release: Notify::new(),
        }
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

struct Proxy {
    server: Server,
    client: reqwest::Client,
    shutdown: watch::Sender<bool>,
}

async fn serve(router: Router) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    Server { url, task }
}

async fn upstream(state: Arc<Mock>) -> Server {
    serve(
        Router::new()
            .route(CALLS, post(create_mock_call))
            .route("/v1/live/{call_id}", get(mock_socket))
            .with_state(state),
    )
    .await
}

async fn create_mock_call(
    State(state): State<Arc<Mock>>,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let index = {
        let mut calls = state.calls.lock().unwrap();
        let index = calls.len();
        calls.push(Recorded { uri, headers, body });
        index
    };
    if state.stall_call {
        let _ = tokio::time::timeout(DEADLINE, state.release.notified()).await;
    }
    if state.broken_body {
        let stream = futures_util::stream::once(async {
            Err::<Bytes, _>(std::io::Error::other("mock truncated SDP"))
        });
        return (
            StatusCode::CREATED,
            [("location", "/v1/live/rtc_broken")],
            Body::from_stream(stream),
        )
            .into_response();
    }
    let location = state
        .location
        .clone()
        .unwrap_or_else(|| format!("https://api.openai.com/v1/live/rtc_{}_{index}", state.name));
    (
        state.status,
        [
            ("location", location),
            ("content-type", "application/sdp".to_owned()),
            ("x-request-id", "upstream-request-id".to_owned()),
        ],
        state.body.clone(),
    )
        .into_response()
}

async fn mock_socket(
    State(state): State<Arc<Mock>>,
    uri: Uri,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    state.handshakes.lock().unwrap().push(Recorded {
        uri,
        headers,
        body: Bytes::new(),
    });
    if state.stall_handshake {
        let _ = tokio::time::timeout(DEADLINE, state.release.notified()).await;
    }
    if let Some(status) = state.handshake_status {
        return (status, [("retry-after", "7")], "mock handshake rejection").into_response();
    }
    ws.on_upgrade(move |mut socket| async move {
        state.active.fetch_add(1, Ordering::SeqCst);
        let run = async {
            if matches!(state.mode, Mode::Abrupt) {
                return;
            }
            if matches!(state.mode, Mode::Stall) {
                let _ = tokio::time::timeout(DEADLINE, state.release.notified()).await;
                return;
            }
            if socket
                .send(Message::Text(
                    "{\"type\":\"future.unsolicited\",\"value\":42}".into(),
                ))
                .await
                .is_err()
            {
                return;
            }
            if matches!(state.mode, Mode::Oversized) {
                let _ = socket
                    .send(Message::Binary(vec![7; (1 << 20) + 1].into()))
                    .await;
                return;
            }
            if matches!(state.mode, Mode::Close) {
                let _ = socket
                    .send(Message::Close(Some(CloseFrame {
                        code: 1008,
                        reason: "mock policy".into(),
                    })))
                    .await;
                return;
            }
            while let Some(Ok(message)) = socket.next().await {
                match message {
                    Message::Text(_) | Message::Binary(_) => {
                        state.messages.fetch_add(1, Ordering::SeqCst);
                        if matches!(state.mode, Mode::Interleaved) {
                            let _ = socket
                                .send(Message::Text("unsolicited between client events".into()))
                                .await;
                        }
                        if socket.send(message).await.is_err() {
                            break;
                        }
                    }
                    Message::Close(_) => break,
                    Message::Ping(_) | Message::Pong(_) => {}
                }
            }
        };
        run.await;
        state.active.fetch_sub(1, Ordering::SeqCst);
        state.disconnected.notify_waiters();
    })
}

fn account(server: &Server, name: &str, priority: i32) -> EffectiveAccount {
    EffectiveAccount {
        config: AccountConfig {
            id: name.to_owned(),
            kind: AccountKind::ChatgptCodexAuthJson,
            base_url: format!("{}/backend-api/codex", server.url),
            realtime_ws_base_url: Some(format!(
                "{}/v1/live",
                server.url.replace("http://", "ws://")
            )),
            supports_realtime: true,
            priority,
            models: vec!["gpt-live-1-codex".to_owned()],
            auto_use_reset: true,
            ..AccountConfig::default()
        },
        bearer_token: format!("access-{name}"),
        chatgpt_account_id: Some(format!("account-{name}")),
        auth_json: None,
        prompt_cache_key_seed: None,
    }
}

async fn proxy(accounts: Vec<EffectiveAccount>) -> Proxy {
    proxy_with(accounts, 1 << 20, 1000).await
}

async fn proxy_with(accounts: Vec<EffectiveAccount>, max_body: usize, idle_ms: u64) -> Proxy {
    let mut config = Config::default();
    config.accounts = accounts
        .iter()
        .map(|account| account.config.clone())
        .collect();
    config.server.allow_insecure_upstream = true;
    config.server.max_body_bytes = max_body;
    config.timeouts.request_header_ms = 1000;
    config.timeouts.stream_idle_ms = 1000;
    config.timeouts.websocket_connect_ms = 1000;
    config.timeouts.websocket_idle_ms = idle_ms;
    config.retry.max_precommit_retries = 3;
    config.retry.base_backoff_ms = 0;
    config.retry.max_backoff_ms = 0;
    config.retry.honor_retry_after = false;
    let effective = EffectiveConfig {
        config,
        config_update_endpoint: None,
        admin_token: None,
        downstream_token: "downstream-key".to_owned(),
        account_hash_key: "test-hash".to_owned(),
        accounts,
    };
    let (shutdown, _) = watch::channel(false);
    let state =
        AppState::new_with_log_format_and_shutdown(effective, LogFormat::Json, shutdown.clone())
            .unwrap();
    Proxy {
        server: serve(app(state)).await,
        client: client(),
        shutdown,
    }
}

fn client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(DEADLINE)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
}

async fn create(proxy: &Proxy) -> reqwest::Response {
    create_at(&proxy.client, &proxy.server.url).await
}

async fn create_at(client: &reqwest::Client, url: &str) -> reqwest::Response {
    client
        .post(format!("{url}{CALLS}?{QUERY}"))
        .bearer_auth("downstream-key")
        .header("content-type", "application/json")
        .header("openai-alpha", "quicksilver=v2")
        .header("x-session-id", "session-test")
        .header("chatgpt-account-id", "forged-account")
        .body(OFFER)
        .send()
        .await
        .unwrap()
}

async fn call_id(proxy: &Proxy) -> String {
    let response = create(proxy).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    response.headers()["location"]
        .to_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .to_owned()
}

async fn connect_at(url: &str, id: &str) -> Result<Socket, tungstenite::Error> {
    let mut request = format!("{}/v1/live/{id}", url.replace("http://", "ws://"))
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", "Bearer downstream-key".parse().unwrap());
    request
        .headers_mut()
        .insert("chatgpt-account-id", "forged-account".parse().unwrap());
    request
        .headers_mut()
        .insert("openai-alpha", "quicksilver=v2".parse().unwrap());
    request
        .headers_mut()
        .insert("x-session-id", "session-test".parse().unwrap());
    tokio::time::timeout(DEADLINE, tokio_tungstenite::connect_async(request))
        .await
        .unwrap()
        .map(|(socket, _)| socket)
}

async fn next(socket: &mut Socket) -> tungstenite::Message {
    tokio::time::timeout(DEADLINE, socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}

async fn greeting(socket: &mut Socket) {
    assert_eq!(
        next(socket).await.into_text().unwrap(),
        "{\"type\":\"future.unsolicited\",\"value\":42}"
    );
}

fn http_error(error: tungstenite::Error) -> tungstenite::http::Response<Option<Vec<u8>>> {
    match error {
        tungstenite::Error::Http(response) => *response,
        other => panic!("expected HTTP response, got {other:?}"),
    }
}

async fn disconnected(state: &Mock) {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let notified = state.disconnected.notified();
            if state.active.load(Ordering::SeqCst) == 0 {
                return;
            }
            notified.await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn call_setup_preserves_body_status_location_and_authenticates_both_transports() {
    let state = Arc::new(Mock::new("primary"));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", 100)]).await;
    let response = create(&proxy).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(
        response.headers()["location"],
        "https://api.openai.com/v1/live/rtc_primary_0"
    );
    assert_eq!(response.text().await.unwrap(), ANSWER);
    let mut socket = connect_at(&proxy.server.url, "rtc_primary_0")
        .await
        .unwrap();
    greeting(&mut socket).await;
    socket.close(None).await.unwrap();
    disconnected(&state).await;
    let calls = state.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].body.as_ref(), OFFER.as_bytes());
    assert_eq!(calls[0].uri.query(), Some(QUERY));
    let handshakes = state.handshakes.lock().unwrap();
    assert_eq!(handshakes.len(), 1);
    for request in [&calls[0], &handshakes[0]] {
        assert_eq!(request.headers["authorization"], "Bearer access-primary");
        assert_eq!(request.headers["chatgpt-account-id"], "account-primary");
        assert_eq!(request.headers["openai-alpha"], "quicksilver=v2");
        assert_eq!(request.headers["x-session-id"], "session-test");
    }
}

#[tokio::test]
async fn accepts_uuid_location_and_success_status_other_than_created() {
    let mut state = Mock::new("primary");
    state.status = StatusCode::OK;
    state.location = Some("/v1/live/12345678-1234-1234-1234-123456789abc".to_owned());
    let state = Arc::new(state);
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", 100)]).await;
    let response = create(&proxy).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["location"],
        "/v1/live/12345678-1234-1234-1234-123456789abc"
    );
    let mut socket = connect_at(&proxy.server.url, "12345678-1234-1234-1234-123456789abc")
        .await
        .unwrap();
    greeting(&mut socket).await;
}

#[tokio::test]
async fn rejects_unsafe_locations_without_registering_calls() {
    for location in [
        "/v1/live/../secrets",
        "/v1/live/not-a-call",
        "/v1/live/rtc_",
        "/v1/live/rtc_bad%20id",
    ] {
        let mut state = Mock::new("primary");
        state.location = Some(location.to_owned());
        let state = Arc::new(state);
        let upstream = upstream(state.clone()).await;
        let proxy = proxy(vec![account(&upstream, "primary", 100)]).await;
        assert_eq!(
            create(&proxy).await.status(),
            StatusCode::BAD_GATEWAY,
            "{location}"
        );
        assert_eq!(
            http_error(
                connect_at(&proxy.server.url, "rtc_primary_0")
                    .await
                    .unwrap_err()
            )
            .status(),
            StatusCode::NOT_FOUND
        );
        assert!(state.handshakes.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn create_failures_are_preserved_without_retries_resets_or_failover() {
    for status in [
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::INTERNAL_SERVER_ERROR,
        StatusCode::TEMPORARY_REDIRECT,
    ] {
        let mut primary_state = Mock::new("primary");
        primary_state.status = status;
        primary_state.body = "{\"error\":{\"code\":\"usage_limit_reached\"}}".to_owned();
        let primary_state = Arc::new(primary_state);
        let fallback_state = Arc::new(Mock::new("fallback"));
        let primary = upstream(primary_state.clone()).await;
        let fallback = upstream(fallback_state.clone()).await;
        let proxy = proxy(vec![
            account(&primary, "primary", 100),
            account(&fallback, "fallback", 0),
        ])
        .await;
        let response = create(&proxy).await;
        assert_eq!(response.status(), status);
        assert_eq!(response.text().await.unwrap(), primary_state.body);
        assert_eq!(primary_state.calls.lock().unwrap().len(), 1);
        assert!(fallback_state.calls.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn authentication_unknown_call_and_methods_are_checked_before_upstream() {
    let state = Arc::new(Mock::new("primary"));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", 100)]).await;
    assert_eq!(
        client()
            .post(format!("{}{CALLS}", proxy.server.url))
            .body(OFFER)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client()
            .get(format!("{}{CALLS}", proxy.server.url))
            .bearer_auth("downstream-key")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::METHOD_NOT_ALLOWED
    );
    assert_eq!(
        http_error(
            connect_at(&proxy.server.url, "rtc_unknown")
                .await
                .unwrap_err()
        )
        .status(),
        StatusCode::NOT_FOUND
    );
    assert!(state.calls.lock().unwrap().is_empty());
    assert!(state.handshakes.lock().unwrap().is_empty());
}

#[tokio::test]
async fn opt_out_prevents_creating_subscription_calls() {
    let state = Arc::new(Mock::new("primary"));
    let upstream = upstream(state.clone()).await;
    let mut disabled = account(&upstream, "primary", 100);
    disabled.config.supports_realtime = false;
    let proxy = proxy(vec![disabled]).await;
    assert!(!create(&proxy).await.status().is_success());
    assert!(state.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn sideband_relays_unknown_events_binary_ping_and_close_without_response_create() {
    let state = Arc::new(Mock::new("primary"));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", 100)]).await;
    let id = call_id(&proxy).await;
    let mut socket = connect_at(&proxy.server.url, &id).await.unwrap();
    greeting(&mut socket).await;
    for message in [
        tungstenite::Message::Text("{\"type\":\"future.client.event\",\"arbitrary\":[1,2]}".into()),
        tungstenite::Message::Binary(vec![0, 255, 128, 1].into()),
    ] {
        socket.send(message.clone()).await.unwrap();
        assert_eq!(next(&mut socket).await, message);
    }
    socket
        .send(tungstenite::Message::Ping(vec![3, 4].into()))
        .await
        .unwrap();
    assert_eq!(
        next(&mut socket).await,
        tungstenite::Message::Pong(vec![3, 4].into())
    );
    socket.close(None).await.unwrap();
    disconnected(&state).await;
    assert_eq!(state.messages.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn upstream_close_code_and_reason_reach_client() {
    let mut state = Mock::new("primary");
    state.mode = Mode::Close;
    let state = Arc::new(state);
    let upstream = upstream(state).await;
    let proxy = proxy(vec![account(&upstream, "primary", 100)]).await;
    let id = call_id(&proxy).await;
    let mut socket = connect_at(&proxy.server.url, &id).await.unwrap();
    greeting(&mut socket).await;
    match next(&mut socket).await {
        tungstenite::Message::Close(Some(frame)) => {
            assert_eq!(u16::from(frame.code), 1008);
            assert_eq!(frame.reason, "mock policy");
        }
        other => panic!("expected close frame, got {other:?}"),
    }
}

#[tokio::test]
async fn handshake_errors_preserve_status_and_release_call_for_retry() {
    for status in [
        StatusCode::FORBIDDEN,
        StatusCode::TOO_MANY_REQUESTS,
        StatusCode::SERVICE_UNAVAILABLE,
    ] {
        let mut state = Mock::new("primary");
        state.handshake_status = Some(status);
        let state = Arc::new(state);
        let upstream = upstream(state.clone()).await;
        let proxy = proxy(vec![account(&upstream, "primary", 100)]).await;
        let id = call_id(&proxy).await;
        for _ in 0..2 {
            let response = http_error(connect_at(&proxy.server.url, &id).await.unwrap_err());
            assert_eq!(response.status(), status);
            assert_eq!(
                response.body().as_deref(),
                Some(b"mock handshake rejection".as_slice())
            );
        }
        assert_eq!(state.calls.lock().unwrap().len(), 1);
        assert_eq!(state.handshakes.lock().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn duplicate_sideband_is_rejected_and_reconnection_does_not_replay_events() {
    let state = Arc::new(Mock::new("primary"));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", 100)]).await;
    let id = call_id(&proxy).await;
    let mut socket = connect_at(&proxy.server.url, &id).await.unwrap();
    greeting(&mut socket).await;
    assert_eq!(
        http_error(connect_at(&proxy.server.url, &id).await.unwrap_err()).status(),
        StatusCode::CONFLICT
    );
    socket
        .send(tungstenite::Message::Text("one event".into()))
        .await
        .unwrap();
    assert_eq!(next(&mut socket).await.into_text().unwrap(), "one event");
    socket.close(None).await.unwrap();
    disconnected(&state).await;
    let mut reconnected = tokio::time::timeout(DEADLINE, async {
        loop {
            match connect_at(&proxy.server.url, &id).await {
                Ok(socket) => break socket,
                Err(error) => {
                    assert_eq!(http_error(error).status(), StatusCode::CONFLICT);
                    tokio::task::yield_now().await;
                }
            }
        }
    })
    .await
    .unwrap();
    greeting(&mut reconnected).await;
    reconnected
        .send(tungstenite::Message::Text("second event".into()))
        .await
        .unwrap();
    assert_eq!(
        next(&mut reconnected).await.into_text().unwrap(),
        "second event"
    );
    assert_eq!(state.messages.load(Ordering::SeqCst), 2);
    assert_eq!(state.calls.lock().unwrap().len(), 1);
    assert_eq!(state.handshakes.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn silent_media_session_survives_control_idle_timeout_and_shutdown_closes_it() {
    let state = Arc::new(Mock::new("primary"));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy_with(vec![account(&upstream, "primary", 100)], 1 << 20, 50).await;
    let id = call_id(&proxy).await;
    let mut socket = connect_at(&proxy.server.url, &id).await.unwrap();
    greeting(&mut socket).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(200), socket.next())
            .await
            .is_err()
    );
    socket
        .send(tungstenite::Message::Text("still active".into()))
        .await
        .unwrap();
    assert_eq!(next(&mut socket).await.into_text().unwrap(), "still active");
    proxy.shutdown.send(true).unwrap();
    let result = tokio::time::timeout(DEADLINE, socket.next()).await.unwrap();
    assert!(matches!(
        result,
        None | Some(Err(_)) | Some(Ok(tungstenite::Message::Close(_)))
    ));
    disconnected(&state).await;
}

#[tokio::test]
async fn abrupt_upstream_disconnect_ends_downstream_without_replay() {
    let mut state = Mock::new("primary");
    state.mode = Mode::Abrupt;
    let state = Arc::new(state);
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", 100)]).await;
    let id = call_id(&proxy).await;
    let mut socket = connect_at(&proxy.server.url, &id).await.unwrap();
    let result = tokio::time::timeout(DEADLINE, socket.next()).await.unwrap();
    assert!(matches!(
        result,
        None | Some(Err(_)) | Some(Ok(tungstenite::Message::Close(_)))
    ));
    assert_eq!(state.handshakes.lock().unwrap().len(), 1);
    assert_eq!(state.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn call_request_and_response_body_limits_are_enforced() {
    let state = Arc::new(Mock::new("primary"));
    let upstream_server = upstream(state.clone()).await;
    let proxy_server = proxy_with(vec![account(&upstream_server, "primary", 100)], 256, 1000).await;
    let response = client()
        .post(format!("{}{CALLS}", proxy_server.server.url))
        .bearer_auth("downstream-key")
        .header("content-type", "application/json")
        .body("x".repeat(257))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(state.calls.lock().unwrap().is_empty());
    let mut large = Mock::new("large");
    large.body = format!("v=0\r\n{}", "x".repeat(257));
    let large = Arc::new(large);
    let large_server = upstream(large.clone()).await;
    let bounded = proxy_with(vec![account(&large_server, "large", 100)], 256, 1000).await;
    assert_eq!(create(&bounded).await.status(), StatusCode::BAD_GATEWAY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress_concurrent_calls_preserve_exact_message_counts_and_account_binding() {
    let call_count = std::env::var("TOKENPROXY_STRESS_CALLS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(64);
    let messages = std::env::var("TOKENPROXY_STRESS_MESSAGES")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100);
    assert!((1..=1024).contains(&call_count));
    let primary_state = Arc::new(Mock::new("primary"));
    let fallback_state = Arc::new(Mock::new("fallback"));
    let primary = upstream(primary_state.clone()).await;
    let fallback = upstream(fallback_state.clone()).await;
    let proxy = proxy(vec![
        account(&primary, "primary", 100),
        account(&fallback, "fallback", 0),
    ])
    .await;
    let barrier = Arc::new(Barrier::new(call_count));
    let mut tasks = JoinSet::new();
    for call in 0..call_count {
        let url = proxy.server.url.clone();
        let barrier = barrier.clone();
        let client = proxy.client.clone();
        tasks.spawn(async move {
            let response = create_at(&client, &url).await;
            assert_eq!(response.status(), StatusCode::CREATED);
            let id = response.headers()["location"]
                .to_str()
                .unwrap()
                .rsplit('/')
                .next()
                .unwrap();
            let mut socket = connect_at(&url, id).await.unwrap();
            greeting(&mut socket).await;
            barrier.wait().await;
            for sequence in 0..messages {
                let message = if sequence % 2 == 0 {
                    tungstenite::Message::Text(
                        format!(
                            "{{\"type\":\"stress.event\",\"call\":{call},\"sequence\":{sequence}}}"
                        )
                        .into(),
                    )
                } else {
                    tungstenite::Message::Binary(
                        format!("binary:{call}:{sequence}").into_bytes().into(),
                    )
                };
                socket.send(message.clone()).await.unwrap();
                assert_eq!(next(&mut socket).await, message);
            }
            socket.close(None).await.unwrap();
        });
    }
    tokio::time::timeout(Duration::from_secs(60), async {
        while let Some(result) = tasks.join_next().await {
            result.unwrap();
        }
    })
    .await
    .unwrap();
    disconnected(&primary_state).await;
    tokio::time::timeout(DEADLINE, async {
        loop {
            let metrics = client()
                .get(format!("{}/metrics", proxy.server.url))
                .bearer_auth("downstream-key")
                .send()
                .await
                .unwrap()
                .text()
                .await
                .unwrap();
            if metrics
                .lines()
                .any(|line| line == "tokenproxy_active_websocket_sessions 0")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        primary_state.messages.load(Ordering::SeqCst),
        call_count * messages
    );
    assert_eq!(primary_state.calls.lock().unwrap().len(), call_count);
    assert_eq!(primary_state.handshakes.lock().unwrap().len(), call_count);
    for request in primary_state.handshakes.lock().unwrap().iter() {
        assert_eq!(request.headers["authorization"], "Bearer access-primary");
        assert!(request.uri.path().contains("rtc_primary_"));
    }
    assert!(fallback_state.calls.lock().unwrap().is_empty());
    assert!(fallback_state.handshakes.lock().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admission_limit_rejects_new_calls_without_evicting_existing_bindings() {
    let state = Arc::new(Mock::new("primary"));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", 100)]).await;
    let mut first = None;
    for _ in 0..1024 {
        let response = create(&proxy).await;
        assert_eq!(response.status(), StatusCode::CREATED);
        if first.is_none() {
            first = Some(
                response.headers()["location"]
                    .to_str()
                    .unwrap()
                    .rsplit('/')
                    .next()
                    .unwrap()
                    .to_owned(),
            );
        }
        response.bytes().await.unwrap();
    }
    assert_eq!(
        create(&proxy).await.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(state.calls.lock().unwrap().len(), 1024);
    let mut socket = connect_at(&proxy.server.url, &first.unwrap())
        .await
        .unwrap();
    greeting(&mut socket).await;
}

#[tokio::test]
async fn oversized_downstream_messages_are_not_forwarded() {
    let state = Arc::new(Mock::new("primary"));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy_with(vec![account(&upstream, "primary", 100)], 256, 1000).await;
    let id = call_id(&proxy).await;
    let mut socket = connect_at(&proxy.server.url, &id).await.unwrap();
    greeting(&mut socket).await;
    socket
        .send(tungstenite::Message::Binary(vec![0; 257].into()))
        .await
        .unwrap();
    let result = tokio::time::timeout(DEADLINE, socket.next()).await.unwrap();
    assert!(matches!(
        result,
        None | Some(Err(_)) | Some(Ok(tungstenite::Message::Close(_)))
    ));
    disconnected(&state).await;
    assert_eq!(state.messages.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn oversized_upstream_messages_end_sideband_without_forwarding() {
    let mut state = Mock::new("primary");
    state.mode = Mode::Oversized;
    let state = Arc::new(state);
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", 100)]).await;
    let id = call_id(&proxy).await;
    let mut socket = connect_at(&proxy.server.url, &id).await.unwrap();
    greeting(&mut socket).await;
    let result = tokio::time::timeout(DEADLINE, socket.next()).await.unwrap();
    assert!(matches!(
        result,
        None | Some(Err(_)) | Some(Ok(tungstenite::Message::Close(_)))
    ));
    assert_eq!(state.handshakes.lock().unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_upstream_writer_is_bounded_and_ends_downstream() {
    let mut state = Mock::new("primary");
    state.mode = Mode::Stall;
    let state = Arc::new(state);
    let upstream = upstream(state.clone()).await;
    let proxy = proxy_with(vec![account(&upstream, "primary", 100)], 1 << 20, 50).await;
    let id = call_id(&proxy).await;
    let socket = connect_at(&proxy.server.url, &id).await.unwrap();
    let (mut writer, mut reader) = socket.split();
    let sending = tokio::spawn(async move {
        for _ in 0..128 {
            if writer
                .send(tungstenite::Message::Binary(vec![0; 512 * 1024].into()))
                .await
                .is_err()
            {
                return;
            }
        }
    });
    let result = tokio::time::timeout(Duration::from_secs(10), reader.next()).await;
    state.release.notify_one();
    sending.abort();
    assert!(matches!(
        result.unwrap(),
        None | Some(Err(_)) | Some(Ok(tungstenite::Message::Close(_)))
    ));
    disconnected(&state).await;
    assert_eq!(state.handshakes.lock().unwrap().len(), 1);
    assert_eq!(state.messages.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn absolute_location_host_is_not_used_as_connection_destination() {
    let mut state = Mock::new("primary");
    state.location =
        Some("https://unreachable.invalid/v1/live/rtc_primary_0?ignored=true".to_owned());
    let state = Arc::new(state);
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", 100)]).await;
    assert_eq!(create(&proxy).await.status(), StatusCode::CREATED);
    let mut socket = connect_at(&proxy.server.url, "rtc_primary_0")
        .await
        .unwrap();
    greeting(&mut socket).await;
    let handshakes = state.handshakes.lock().unwrap();
    assert_eq!(handshakes.len(), 1);
    assert_eq!(handshakes[0].uri.path(), "/v1/live/rtc_primary_0");
    assert!(handshakes[0].uri.query().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_sideband_claims_admit_exactly_one_connection() {
    const ATTEMPTS: usize = 32;
    let state = Arc::new(Mock::new("primary"));
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", 100)]).await;
    let id = call_id(&proxy).await;
    let barrier = Arc::new(Barrier::new(ATTEMPTS));
    let mut tasks = JoinSet::new();
    for _ in 0..ATTEMPTS {
        let barrier = barrier.clone();
        let url = proxy.server.url.clone();
        let id = id.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            connect_at(&url, &id).await
        });
    }
    let mut accepted = Vec::new();
    let mut rejected = 0;
    tokio::time::timeout(DEADLINE, async {
        while let Some(result) = tasks.join_next().await {
            match result.unwrap() {
                Ok(socket) => accepted.push(socket),
                Err(error) => {
                    assert_eq!(http_error(error).status(), StatusCode::CONFLICT);
                    rejected += 1;
                }
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(accepted.len(), 1);
    assert_eq!(rejected, ATTEMPTS - 1);
    assert_eq!(state.handshakes.lock().unwrap().len(), 1);
    greeting(&mut accepted[0]).await;
    accepted[0].close(None).await.unwrap();
    disconnected(&state).await;
}

#[tokio::test]
async fn delayed_call_response_times_out_without_retry_or_failover() {
    let mut primary_state = Mock::new("primary");
    primary_state.stall_call = true;
    let primary_state = Arc::new(primary_state);
    let fallback_state = Arc::new(Mock::new("fallback"));
    let primary = upstream(primary_state.clone()).await;
    let fallback = upstream(fallback_state.clone()).await;
    let proxy = proxy(vec![
        account(&primary, "primary", 100),
        account(&fallback, "fallback", 0),
    ])
    .await;
    let response = create(&proxy).await;
    primary_state.release.notify_one();
    assert!(response.status().is_server_error());
    assert_eq!(primary_state.calls.lock().unwrap().len(), 1);
    assert!(fallback_state.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn delayed_upstream_handshake_times_out_before_downstream_upgrade() {
    let mut state = Mock::new("primary");
    state.stall_handshake = true;
    let state = Arc::new(state);
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", 100)]).await;
    let id = call_id(&proxy).await;
    let error = connect_at(&proxy.server.url, &id).await.unwrap_err();
    state.release.notify_one();
    assert!(http_error(error).status().is_server_error());
    assert_eq!(state.handshakes.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn ambiguous_call_body_failure_never_retries_or_fails_over() {
    let mut state = Mock::new("primary");
    state.broken_body = true;
    let state = Arc::new(state);
    let fallback_state = Arc::new(Mock::new("fallback"));
    let upstream_server = upstream(state.clone()).await;
    let fallback = upstream(fallback_state.clone()).await;
    let proxy = proxy(vec![
        account(&upstream_server, "primary", 100),
        account(&fallback, "fallback", 0),
    ])
    .await;
    assert!(create(&proxy).await.status().is_server_error());
    assert_eq!(state.calls.lock().unwrap().len(), 1);
    assert!(fallback_state.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn simultaneous_sending_and_receiving_preserves_interleaved_server_events() {
    const MESSAGES: usize = 1000;
    let mut state = Mock::new("primary");
    state.mode = Mode::Interleaved;
    let state = Arc::new(state);
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", 100)]).await;
    let id = call_id(&proxy).await;
    let mut socket = connect_at(&proxy.server.url, &id).await.unwrap();
    greeting(&mut socket).await;
    let (mut writer, mut reader) = socket.split();
    let sending = tokio::spawn(async move {
        for index in 0..MESSAGES {
            writer
                .send(tungstenite::Message::Text(format!("client:{index}").into()))
                .await
                .unwrap();
        }
        writer
    });
    tokio::time::timeout(DEADLINE, async {
        for index in 0..MESSAGES {
            assert_eq!(
                reader.next().await.unwrap().unwrap().into_text().unwrap(),
                "unsolicited between client events"
            );
            assert_eq!(
                reader.next().await.unwrap().unwrap().into_text().unwrap(),
                format!("client:{index}")
            );
        }
    })
    .await
    .unwrap();
    let mut writer = sending.await.unwrap();
    writer.close().await.unwrap();
    disconnected(&state).await;
    assert_eq!(state.messages.load(Ordering::SeqCst), MESSAGES);
}

#[tokio::test]
async fn voice_forbidden_does_not_disable_account_for_future_calls() {
    let mut state = Mock::new("primary");
    state.status = StatusCode::FORBIDDEN;
    let state = Arc::new(state);
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", 100)]).await;
    for _ in 0..2 {
        assert_eq!(create(&proxy).await.status(), StatusCode::FORBIDDEN);
    }
    assert_eq!(state.calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn sideband_forbidden_does_not_disable_new_call_creation() {
    let mut state = Mock::new("primary");
    state.handshake_status = Some(StatusCode::FORBIDDEN);
    let state = Arc::new(state);
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", 100)]).await;
    let id = call_id(&proxy).await;
    assert_eq!(
        http_error(connect_at(&proxy.server.url, &id).await.unwrap_err()).status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(create(&proxy).await.status(), StatusCode::CREATED);
    assert_eq!(state.calls.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn upstream_missing_call_invalidates_binding_without_another_handshake() {
    for status in [StatusCode::NOT_FOUND, StatusCode::GONE] {
        let mut state = Mock::new("primary");
        state.handshake_status = Some(status);
        let state = Arc::new(state);
        let upstream = upstream(state.clone()).await;
        let proxy = proxy(vec![account(&upstream, "primary", 100)]).await;
        let id = call_id(&proxy).await;
        assert_eq!(
            http_error(connect_at(&proxy.server.url, &id).await.unwrap_err()).status(),
            status
        );
        assert_eq!(
            http_error(connect_at(&proxy.server.url, &id).await.unwrap_err()).status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(state.handshakes.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn duplicate_upstream_call_id_is_rejected_without_replacing_original_binding() {
    let mut state = Mock::new("primary");
    state.location = Some("/v1/live/rtc_duplicate".to_owned());
    let state = Arc::new(state);
    let upstream = upstream(state.clone()).await;
    let proxy = proxy(vec![account(&upstream, "primary", 100)]).await;
    assert_eq!(create(&proxy).await.status(), StatusCode::CREATED);
    assert_eq!(create(&proxy).await.status(), StatusCode::BAD_GATEWAY);
    let mut socket = connect_at(&proxy.server.url, "rtc_duplicate")
        .await
        .unwrap();
    greeting(&mut socket).await;
    assert_eq!(state.handshakes.lock().unwrap().len(), 1);
}
