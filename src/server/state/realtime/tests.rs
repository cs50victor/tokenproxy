use std::sync::atomic::{AtomicUsize, Ordering};

use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::Barrier;
use tokio::task::JoinHandle;

use crate::config::{AccountConfig, Config, EffectiveAuthJson, EffectiveConfig};
use crate::server::ConfigStatus;

use super::*;

struct Server {
    base: String,
    task: JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(app: Router) -> Server {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://localhost:{}", listener.local_addr().unwrap().port());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    Server { base, task }
}

fn account(id: &str, base: &str) -> EffectiveAccount {
    EffectiveAccount {
        config: AccountConfig {
            id: id.into(),
            kind: AccountKind::ChatgptCodexAuthJson,
            base_url: format!("{base}/backend-api/codex"),
            supports_realtime: true,
            realtime_ws_base_url: Some(format!("{}/v1/live", base.replacen("http", "ws", 1))),
            ..AccountConfig::default()
        },
        bearer_token: format!("token-{id}"),
        chatgpt_account_id: Some(format!("identity-{id}")),
        auth_json: None,
        prompt_cache_key_seed: None,
    }
}

fn effective(accounts: Vec<EffectiveAccount>) -> EffectiveConfig {
    let mut config = Config::default();
    config.accounts = accounts
        .iter()
        .map(|account| account.config.clone())
        .collect();
    config.server.allow_insecure_upstream = true;
    EffectiveConfig {
        config,
        accounts,
        config_update_endpoint: None,
        admin_token: None,
        downstream_token: "client-key".into(),
        account_hash_key: "test".into(),
    }
}

fn state_from(config: EffectiveConfig) -> AppState {
    AppState::new(config).unwrap()
}

fn store_call(calls: &Calls, id: &str, account: EffectiveAccount) {
    calls
        .insert(
            id.into(),
            Call {
                account,
                caller: "caller".into(),
                expires: Instant::now() + RECONNECT_WINDOW,
                connected: false,
                ended: false,
                _permit: calls.reserve().unwrap(),
            },
        )
        .unwrap();
}

#[test]
fn location_accepts_codex_identifiers_without_trusting_the_host() {
    for (location, expected) in [
        ("/v1/live/rtc_abc", "rtc_abc"),
        (
            "https://api.openai.com/v1/live/rtc_abc/?token=secret",
            "rtc_abc",
        ),
        ("https://untrusted.invalid/rtc_abc", "rtc_abc"),
        (
            "/v1/live/12345678-abcd-1234-abcd-1234567890ab",
            "12345678-abcd-1234-abcd-1234567890ab",
        ),
    ] {
        assert_eq!(call_id_from_location(location).as_deref(), Some(expected));
    }
    for location in [
        "",
        "/rtc_",
        "/v1/live/../",
        "/v1/live/rtc_%2e%2e",
        "/rtc_a#fragment",
        "/v1/live/other",
    ] {
        assert!(call_id_from_location(location).is_none(), "{location}");
    }
}

#[test]
fn expiration_releases_capacity_without_evicting_an_active_call() {
    let calls = Arc::new(Calls::default());
    store_call(&calls, "rtc_idle", account("a", "http://localhost:1"));
    store_call(&calls, "rtc_active", account("a", "http://localhost:1"));
    let lease = calls.claim("rtc_active".into(), "caller").unwrap();
    for call in calls.entries.lock().unwrap().values_mut() {
        call.expires = Instant::now() - Duration::from_secs(1);
    }
    calls.prune();
    assert_eq!(calls.capacity.available_permits(), MAX_CALLS - 1);
    assert!(calls.claim("rtc_idle".into(), "caller").is_err());
    drop(lease);
    assert!(calls.claim("rtc_active".into(), "caller").is_ok());
}

#[test]
fn ended_call_releases_capacity_and_cannot_be_reconnected() {
    let calls = Arc::new(Calls::default());
    store_call(&calls, "rtc_ended", account("a", "http://localhost:1"));
    let lease = calls.claim("rtc_ended".into(), "caller").unwrap();
    lease.forget();
    assert!(calls.claim("rtc_ended".into(), "caller").is_err());
    drop(lease);
    assert_eq!(calls.capacity.available_permits(), MAX_CALLS);
    assert!(calls.claim("rtc_ended".into(), "caller").is_err());
}

#[test]
fn duplicate_upstream_ids_preserve_the_first_owner() {
    let calls = Arc::new(Calls::default());
    store_call(&calls, "rtc_duplicate", account("a", "http://localhost:1"));
    let result = calls.insert(
        "rtc_duplicate".into(),
        Call {
            account: account("b", "http://localhost:2"),
            caller: "caller".into(),
            expires: Instant::now() + RECONNECT_WINDOW,
            connected: false,
            ended: false,
            _permit: calls.reserve().unwrap(),
        },
    );
    assert_eq!(result.unwrap_err().status, StatusCode::BAD_GATEWAY);
    assert_eq!(
        calls
            .claim("rtc_duplicate".into(), "caller")
            .unwrap()
            .account
            .config
            .id,
        "a"
    );
    assert_eq!(calls.capacity.available_permits(), MAX_CALLS - 1);
}

#[tokio::test]
async fn reservation_bounds_inflight_calls_before_upstream_creation() {
    let calls = Arc::new(Calls::default());
    let barrier = Arc::new(Barrier::new(MAX_CALLS * 2));
    let mut tasks = Vec::new();
    for _ in 0..MAX_CALLS * 2 {
        let calls = calls.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            let permit = calls.reserve();
            barrier.wait().await;
            permit.is_ok()
        }));
    }
    let mut admitted = 0;
    for task in tasks {
        admitted += usize::from(task.await.unwrap());
    }
    assert_eq!(admitted, MAX_CALLS);
    assert_eq!(calls.capacity.available_permits(), MAX_CALLS);
}

#[tokio::test]
async fn new_calls_respect_health_but_existing_calls_remain_pinned() {
    let primary = account("a", "http://localhost:1");
    let mut secondary = account("b", "http://localhost:2");
    secondary.config.priority = -1;
    let state = state_from(effective(vec![primary.clone(), secondary]));
    assert_eq!(select_voice_account(&state).await.unwrap().config.id, "a");
    state.store_account_health(
        "a",
        AccountHealth::UsageLimited {
            reset_at_ms: now_unix_ms() + 60_000,
        },
    );
    assert_eq!(select_voice_account(&state).await.unwrap().config.id, "b");
    assert_eq!(
        voice_credentials(&state, &primary, false)
            .await
            .unwrap()
            .config
            .id,
        "a"
    );
}

#[tokio::test]
async fn reload_cannot_substitute_call_identity_or_backend() {
    let primary = account("a", "http://localhost:1");
    for change in 0..7 {
        let state = state_from(effective(vec![primary.clone()]));
        let mut changed = primary.clone();
        match change {
            0 => changed.chatgpt_account_id = Some("different-identity".into()),
            1 => changed.config.base_url = "http://localhost:2/backend-api/codex".into(),
            2 => changed.config.realtime_ws_base_url = Some("ws://localhost:2/v1/live".into()),
            3 => changed.config.kind = AccountKind::OpenAiApiKey,
            4 => changed.config.enabled = false,
            5 => changed.config.supports_realtime = false,
            6 => changed.config.auth_json_path = Some("/tmp/different-auth.json".into()),
            _ => unreachable!(),
        }
        state
            .swap_effective(effective(vec![changed]), ConfigStatus::default())
            .unwrap();
        assert_eq!(
            voice_credentials(&state, &primary, false)
                .await
                .unwrap_err()
                .status,
            StatusCode::CONFLICT,
            "case {change}"
        );
    }
    let state = state_from(effective(vec![primary.clone()]));
    state
        .swap_effective(effective(vec![]), ConfigStatus::default())
        .unwrap();
    assert!(voice_credentials(&state, &primary, false).await.is_err());
}

#[tokio::test]
async fn reload_rotates_tokens_without_reselecting_the_call_account() {
    let received = Arc::new(Mutex::new(Vec::new()));
    let capture = received.clone();
    let server = serve(Router::new().route(
        "/v1/live/rtc_pinned",
        get(move |headers: HeaderMap, ws: WebSocketUpgrade| {
            capture.lock().unwrap().push(headers);
            async move {
                ws.on_upgrade(|mut socket| async move {
                    let _ = socket.send(Message::Text("ready".into())).await;
                })
            }
        }),
    ))
    .await;
    let primary = account("a", &server.base);
    let state = state_from(effective(vec![primary.clone()]));
    store_call(&state.realtime_calls, "rtc_pinned", primary.clone());
    let mut rotated = primary;
    rotated.bearer_token = "rotated-token".into();
    let mut preferred = account("b", &server.base);
    preferred.config.priority = 100;
    state
        .swap_effective(effective(vec![rotated, preferred]), ConfigStatus::default())
        .unwrap();
    let lease = state
        .realtime_calls
        .claim("rtc_pinned".into(), "caller")
        .unwrap();
    let mut socket = connect_sideband(&state, &lease, &HeaderMap::new())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        socket.next().await.unwrap().unwrap().into_text().unwrap(),
        "ready"
    );
    let received = received.lock().unwrap();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0]["authorization"], "Bearer rotated-token");
    assert_eq!(received[0]["chatgpt-account-id"], "identity-a");
}

#[tokio::test]
async fn mismatched_auth_cell_is_rejected_before_refresh() {
    let mut primary = account("a", "http://localhost:1");
    primary.config.auth_json_path = Some("/tmp/tokenproxy-test-unused-auth.json".into());
    let state = state_from(effective(vec![primary.clone()]));
    let mut corrupt = primary.clone();
    corrupt.auth_json = Some(EffectiveAuthJson { text: json!({"tokens": {"access_token":"other", "refresh_token":"other", "account_id":"other"}}).to_string(), upload_name: None });
    let cells = crate::auth::chatgpt_auth_cells(&effective(vec![corrupt])).unwrap();
    *state.chatgpt_auth.write().unwrap() = cells;
    assert_eq!(
        voice_credentials(&state, &primary, false)
            .await
            .unwrap_err()
            .status,
        StatusCode::CONFLICT
    );
}

#[tokio::test]
async fn sideband_refresh_retries_once_on_the_same_account() {
    let refresh_count = Arc::new(AtomicUsize::new(0));
    let counter = refresh_count.clone();
    let authority = serve(Router::new().route(
        "/refresh",
        post(move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Json(json!({"access_token":"new-access", "refresh_token":"new-refresh"})) }
        }),
    ))
    .await;
    let _override =
        crate::auth::use_refresh_endpoint_for_testing(format!("{}/refresh", authority.base));
    for persistent_401 in [false, true] {
        let received = Arc::new(Mutex::new(Vec::new()));
        let capture = received.clone();
        let server = serve(Router::new().route(
            "/v1/live/rtc_refresh",
            get(move |headers: HeaderMap, ws: WebSocketUpgrade| {
                capture.lock().unwrap().push(headers.clone());
                async move {
                    if persistent_401 || headers["authorization"] == "Bearer old-access" {
                        return StatusCode::UNAUTHORIZED.into_response();
                    }
                    ws.on_upgrade(|mut socket| async move {
                        let _ = socket.send(Message::Text("ready".into())).await;
                    })
                }
            }),
        ))
        .await;
        let mut primary = account("a", &server.base);
        primary.bearer_token = "old-access".into();
        primary.auth_json = Some(EffectiveAuthJson { text: json!({"tokens": {"access_token":"old-access", "refresh_token":"old-refresh", "account_id":"identity-a"}}).to_string(), upload_name: None });
        let auth_path = std::env::temp_dir().join(format!(
            "tokenproxy-live-refresh-{}.json",
            uuid::Uuid::new_v4()
        ));
        std::fs::write(&auth_path, &primary.auth_json.as_ref().unwrap().text).unwrap();
        primary.config.auth_json_path = Some(auth_path.clone());
        let state = state_from(effective(vec![primary.clone(), account("b", &server.base)]));
        store_call(&state.realtime_calls, "rtc_refresh", primary);
        let lease = state
            .realtime_calls
            .claim("rtc_refresh".into(), "caller")
            .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            connect_sideband(&state, &lease, &HeaderMap::new()),
        )
        .await
        .unwrap()
        .unwrap();
        if persistent_401 {
            assert_eq!(result.unwrap_err().status(), StatusCode::UNAUTHORIZED);
        } else {
            assert!(result.is_ok());
        }
        let received = received.lock().unwrap();
        assert_eq!(received.len(), 2);
        assert_eq!(received[0]["authorization"], "Bearer old-access");
        assert_eq!(received[1]["authorization"], "Bearer new-access");
        assert!(
            received
                .iter()
                .all(|headers| headers["chatgpt-account-id"] == "identity-a")
        );
        std::fs::remove_file(auth_path).unwrap();
    }
    assert_eq!(refresh_count.load(Ordering::SeqCst), 2);
}

#[test]
fn call_bindings_are_scoped_to_the_authenticated_caller() {
    let primary = account("a", "http://localhost:1");
    let state = state_from(effective(vec![primary, account("b", "http://localhost:2")]));
    let header = |token: &str| {
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        headers
    };
    let a = caller_key(&state, &header("token-a")).unwrap();
    let b = caller_key(&state, &header("token-b")).unwrap();
    assert_ne!(a, b);
    store_call(
        &state.realtime_calls,
        "rtc_private",
        account("a", "http://localhost:1"),
    );
    state
        .realtime_calls
        .entries
        .lock()
        .unwrap()
        .get_mut("rtc_private")
        .unwrap()
        .caller = a;
    assert_eq!(
        state
            .realtime_calls
            .claim("rtc_private".into(), &b)
            .err()
            .unwrap()
            .status,
        StatusCode::NOT_FOUND
    );
}
