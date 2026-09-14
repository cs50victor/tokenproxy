use std::sync::Arc;
use std::time::{Duration, Instant};

use reqwest::{Method, StatusCode, Url};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use tokio::sync::Mutex;

use super::AppState;
use crate::config::{AccountKind, EffectiveAccount};

const RESET_COOLDOWN: Duration = Duration::from_secs(30);
const MAX_RESET_BODY_BYTES: usize = 64 * 1024;

#[derive(Default)]
pub(super) struct ResetAttempt {
    last_attempt: Option<Instant>,
    last_success: Option<Instant>,
    pending_key: Option<String>,
}

#[derive(Clone)]
pub(super) struct ResetContext {
    pub state: AppState,
    pub account: EffectiveAccount,
    started: Instant,
    pub allow_recovery: bool,
}

#[derive(Deserialize)]
struct ResetCredits {
    available_count: u64,
}

#[derive(Deserialize)]
struct ConsumeResponse {
    code: ConsumeCode,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum ConsumeCode {
    Reset,
    AlreadyRedeemed,
    NothingToReset,
    NoCredit,
}

impl ResetContext {
    fn current_account(&self) -> Option<EffectiveAccount> {
        self.state
            .effective()
            .accounts
            .iter()
            .find(|candidate| {
                candidate.config.id == self.account.config.id
                    && candidate.config.auto_use_reset
                    && candidate.config.enabled
                    && candidate.config.kind == AccountKind::ChatgptCodexAuthJson
                    && candidate.config.base_url == self.account.config.base_url
                    && candidate.chatgpt_account_id == self.account.chatgpt_account_id
                    && candidate.auth_json == self.account.auth_json
            })
            .cloned()
    }

    pub fn new(state: &AppState, account: &EffectiveAccount) -> Option<Self> {
        if !account.config.auto_use_reset
            || account.config.kind != AccountKind::ChatgptCodexAuthJson
        {
            return None;
        }
        Some(Self {
            state: state.clone(),
            account: account.clone(),
            started: Instant::now(),
            allow_recovery: true,
        })
    }

    pub async fn recover(&self) -> bool {
        if !self.allow_recovery {
            return false;
        }
        // Reload may revoke permission or replace the identity while a stream is open.
        let Some(current) = self.current_account() else {
            return false;
        };
        let account = self.state.account_for_request(&current).await;
        let Some((url, identity)) = reset_endpoint(&account) else {
            return false;
        };
        let cell = self
            .state
            .reset_attempts
            .lock()
            .await
            .entry(identity.clone())
            .or_insert_with(|| Arc::new(Mutex::new(ResetAttempt::default())))
            .clone();
        let mut attempt = cell.lock().await;
        if self.current_account().is_none() {
            return false;
        }
        if attempt
            .last_success
            .is_some_and(|success| success >= self.started)
        {
            self.clear_usage(&identity).await;
            return true;
        }
        if attempt
            .last_attempt
            .is_some_and(|last| last.elapsed() < RESET_COOLDOWN)
        {
            return false;
        }
        attempt.last_attempt = Some(Instant::now());
        let timeout = Duration::from_millis(
            self.state
                .effective()
                .config
                .timeouts
                .request_header_ms
                .min(10_000),
        );
        let result = tokio::time::timeout(timeout, self.consume(&account, url, &mut attempt)).await;
        let recovered = matches!(result, Ok(Ok(true)));
        if recovered {
            attempt.last_success = Some(Instant::now());
            self.clear_usage(&identity).await;
        }
        let outcome = match result {
            Ok(Ok(true)) => "reset",
            Ok(Ok(false)) => "unavailable",
            Ok(Err(_)) => "failed",
            Err(_) => "timeout",
        };
        // Backend bodies and URLs can contain credentials; log only bounded outcomes.
        let account_hash = crate::usage::account_id_hash(
            &account.config.id,
            &self.state.effective().account_hash_key,
        );
        match self.state.log_format {
            crate::logging::LogFormat::Json => eprintln!(
                "{}",
                serde_json::json!({
                    "event": "auto_use_reset", "account_id_hash": account_hash, "outcome": outcome,
                })
            ),
            crate::logging::LogFormat::Text => {
                eprintln!("auto_use_reset account={account_hash} outcome={outcome}")
            }
        }
        recovered
    }

    async fn consume(
        &self,
        account: &EffectiveAccount,
        url: Url,
        attempt: &mut ResetAttempt,
    ) -> Result<bool, ()> {
        if attempt.pending_key.is_none() {
            let credits: ResetCredits = self
                .request(account, Method::GET, url.clone(), None)
                .await?;
            if credits.available_count == 0 {
                return Ok(false);
            }
            attempt.pending_key = Some(uuid::Uuid::new_v4().to_string());
        }
        let mut consume_url = url;
        consume_url.set_path(&format!("{}/consume", consume_url.path()));
        let body = serde_json::json!({"redeem_request_id": attempt.pending_key});
        // Preserve this key even if the future is cancelled after the server redeems it.
        let response: ConsumeResponse = self
            .request(account, Method::POST, consume_url, Some(&body))
            .await?;
        attempt.pending_key = None;
        Ok(matches!(
            response.code,
            ConsumeCode::Reset | ConsumeCode::AlreadyRedeemed
        ))
    }

    async fn request<T: DeserializeOwned>(
        &self,
        account: &EffectiveAccount,
        method: Method,
        url: Url,
        body: Option<&serde_json::Value>,
    ) -> Result<T, ()> {
        let mut account = account.clone();
        for auth_attempt in 0..2 {
            if method == Method::POST && self.current_account().is_none() {
                return Err(());
            }
            let mut request = self
                .state
                .reset_client
                .request(method.clone(), url.clone())
                .bearer_auth(&account.bearer_token)
                .header("user-agent", "codex-cli")
                .header(
                    "chatgpt-account-id",
                    account.chatgpt_account_id.as_deref().ok_or(())?,
                );
            if let Some(body) = body {
                request = request.json(body);
            }
            let mut response = request.send().await.map_err(|_| ())?;
            if response.status() == StatusCode::UNAUTHORIZED && auth_attempt == 0 {
                let recovered = self
                    .state
                    .recover_chatgpt_unauthorized(&account)
                    .await
                    .map_err(|_| ())?
                    .ok_or(())?;
                if recovered.chatgpt_account_id != account.chatgpt_account_id {
                    return Err(());
                }
                account = recovered;
                continue;
            }
            if !response.status().is_success() {
                return Err(());
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|_| ())? {
                if chunk.len() > MAX_RESET_BODY_BYTES.saturating_sub(bytes.len()) {
                    return Err(());
                }
                bytes.extend_from_slice(&chunk);
            }
            return serde_json::from_slice(&bytes).map_err(|_| ());
        }
        Err(())
    }

    async fn clear_usage(&self, identity: &str) {
        let accounts = self.state.effective();
        let mut windows = self.state.usage_windows.lock().await;
        for account in &accounts.accounts {
            if reset_endpoint(account).is_some_and(|(_, key)| key == identity) {
                windows.remove(&account.config.id);
                self.state
                    .clear_account_health_if_not_auth_failed(&account.config.id);
            }
        }
    }
}

fn reset_endpoint(account: &EffectiveAccount) -> Option<(Url, String)> {
    if account.config.kind != AccountKind::ChatgptCodexAuthJson {
        return None;
    }
    let account_id = account
        .chatgpt_account_id
        .as_deref()
        .filter(|id| !id.is_empty())?;
    let mut url = Url::parse(&account.config.base_url).ok()?;
    let base = url.path().trim_end_matches('/').strip_suffix("/codex")?;
    let path = format!("{base}/wham/rate-limit-reset-credits");
    url.set_path(&path);
    url.set_query(None);
    url.set_fragment(None);
    let identity = format!("{url}\n{account_id}");
    Some((url, identity))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{Body, to_bytes};
    use axum::extract::State;
    use axum::http::{HeaderMap, Request};
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use serde_json::{Value, json};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Notify;
    use tower::ServiceExt;

    use crate::config::{AccountConfig, Config, EffectiveConfig};

    #[derive(Default)]
    struct Backend {
        keys: Mutex<Vec<String>>,
        auth_headers: Mutex<Vec<String>>,
        auth_path: Option<std::path::PathBuf>,
        generation_calls: AtomicUsize,
        consume_delay: Duration,
        block_lookup: bool,
        lookup_started: Notify,
        release_lookup: Notify,
    }

    struct Fixture {
        state: AppState,
        account: EffectiveAccount,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.task.abort();
            if let Some(path) = self.account.config.auth_json_path.as_ref() {
                let _ = std::fs::remove_file(path);
            }
        }
    }

    async fn fixture(backend: Arc<Backend>) -> Fixture {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut account = EffectiveAccount {
            config: AccountConfig {
                id: "test".into(),
                kind: AccountKind::ChatgptCodexAuthJson,
                base_url: format!(
                    "http://{}/backend-api/codex",
                    listener.local_addr().unwrap()
                ),
                auto_use_reset: true,
                supports_responses: true,
                models: vec!["gpt-5.5".into()],
                ..AccountConfig::default()
            },
            bearer_token: "fake".into(),
            chatgpt_account_id: Some("fake-account".into()),
            auth_json: None,
            prompt_cache_key_seed: None,
        };
        if let Some(path) = backend.auth_path.as_ref() {
            let text = r#"{"tokens":{"access_token":"fake","refresh_token":"fake-refresh","account_id":"fake-account"}}"#;
            std::fs::write(path, text).unwrap();
            account.config.auth_json_path = Some(path.clone());
            account.auth_json = Some(crate::config::EffectiveAuthJson {
                text: text.into(),
                upload_name: None,
            });
        }
        let router = Router::new()
            .route("/backend-api/wham/rate-limit-reset-credits", get(|State(backend): State<Arc<Backend>>, headers: HeaderMap| async move {
                backend.auth_headers.lock().await.push(headers["authorization"].to_str().unwrap().into());
                backend.lookup_started.notify_one();
                if backend.block_lookup {
                    backend.release_lookup.notified().await;
                }
                if let Some(path) = backend.auth_path.as_ref()
                    && headers["authorization"] == "Bearer fake"
                {
                    std::fs::write(path, r#"{"tokens":{"access_token":"fresh","refresh_token":"fresh-refresh","account_id":"fake-account"}}"#).unwrap();
                    return (StatusCode::UNAUTHORIZED, Json(json!({"error":"expired"})));
                }
                (StatusCode::OK, Json(json!({"available_count": 3})))
            }))
            .route("/backend-api/wham/rate-limit-reset-credits/consume", post(|State(backend): State<Arc<Backend>>, headers: HeaderMap, Json(body): Json<Value>| async move {
                backend.auth_headers.lock().await.push(headers["authorization"].to_str().unwrap().into());
                if backend.auth_path.is_some() && headers["authorization"] != "Bearer fresh" {
                    return (StatusCode::UNAUTHORIZED, Json(json!({"error":"expired"})));
                }
                let mut keys = backend.keys.lock().await;
                keys.push(body["redeem_request_id"].as_str().unwrap().to_owned());
                let first = keys.len() == 1;
                drop(keys);
                if first {
                    tokio::time::sleep(backend.consume_delay).await;
                }
                (StatusCode::OK, Json(json!({"code": if first { "reset" } else { "already_redeemed" }})))
            }))
            .route("/backend-api/codex/responses", post(|State(backend): State<Arc<Backend>>| async move {
                if backend.generation_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    (StatusCode::TOO_MANY_REQUESTS, Json(json!({"error":{"type":"usage_limit_reached","resets_in_seconds":86400}})))
                } else {
                    (StatusCode::OK, Json(json!({"id":"recovered","output":[]})))
                }
            }))
            .with_state(backend);
        let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let mut config = Config::default();
        config.accounts = vec![account.config.clone()];
        config.timeouts.request_header_ms = 100;
        config.retry.max_precommit_retries = 0;
        let state = AppState::new(EffectiveConfig {
            config,
            accounts: vec![account.clone()],
            config_update_endpoint: None,
            admin_token: None,
            downstream_token: "client".into(),
            account_hash_key: "hash".into(),
        })
        .unwrap();
        Fixture {
            state,
            account,
            task,
        }
    }

    async fn send(state: &AppState) -> StatusCode {
        let response = super::super::app(state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/responses")
                    .header("authorization", "Bearer client")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"model":"gpt-5.5","input":"hi"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        to_bytes(response.into_body(), 65536).await.unwrap();
        status
    }

    async fn expire_cooldown(state: &AppState, account: &EffectiveAccount) {
        let (_, identity) = reset_endpoint(account).unwrap();
        let cell = state.reset_attempts.lock().await[&identity].clone();
        cell.lock().await.last_attempt = Some(Instant::now() - Duration::from_secs(31));
    }

    #[tokio::test]
    async fn reset_401_reloads_auth_and_retries_with_the_current_token() {
        let backend = Arc::new(Backend {
            auth_path: Some(
                std::env::temp_dir()
                    .join(format!("tokenproxy-reset-{}.json", uuid::Uuid::new_v4())),
            ),
            ..Backend::default()
        });
        let fixture = fixture(backend.clone()).await;
        assert!(
            ResetContext::new(&fixture.state, &fixture.account)
                .unwrap()
                .recover()
                .await
        );
        assert_eq!(backend.keys.lock().await.len(), 1);
        let headers = backend.auth_headers.lock().await;
        assert_eq!(headers.first().unwrap(), "Bearer fake");
        assert_eq!(headers.last().unwrap(), "Bearer fresh");
    }

    #[tokio::test]
    async fn ambiguous_redemption_reconciles_through_routing_with_the_same_key_after_reload() {
        let backend = Arc::new(Backend {
            consume_delay: Duration::from_millis(300),
            ..Backend::default()
        });
        let fixture = fixture(backend.clone()).await;
        assert_eq!(send(&fixture.state).await, StatusCode::SERVICE_UNAVAILABLE);
        let effective = fixture.state.effective().as_ref().clone();
        fixture
            .state
            .swap_effective(effective, Default::default())
            .unwrap();
        expire_cooldown(&fixture.state, &fixture.account).await;
        assert_eq!(send(&fixture.state).await, StatusCode::OK);
        let keys = backend.keys.lock().await;
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], keys[1]);
        assert!(uuid::Uuid::parse_str(&keys[0]).is_ok());
    }

    #[tokio::test]
    async fn cancelled_consume_retains_its_key_for_the_next_attempt() {
        let backend = Arc::new(Backend {
            consume_delay: Duration::from_millis(300),
            ..Backend::default()
        });
        let fixture = fixture(backend.clone()).await;
        let context = ResetContext::new(&fixture.state, &fixture.account).unwrap();
        let task = tokio::spawn(async move { context.recover().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while backend.keys.lock().await.is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        expire_cooldown(&fixture.state, &fixture.account).await;
        assert!(
            ResetContext::new(&fixture.state, &fixture.account)
                .unwrap()
                .recover()
                .await
        );
        let keys = backend.keys.lock().await;
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], keys[1]);
    }

    #[tokio::test]
    async fn disabling_resets_during_lookup_prevents_consumption() {
        let backend = Arc::new(Backend {
            block_lookup: true,
            ..Backend::default()
        });
        let fixture = fixture(backend.clone()).await;
        let context = ResetContext::new(&fixture.state, &fixture.account).unwrap();
        let task = tokio::spawn(async move { context.recover().await });
        tokio::time::timeout(Duration::from_secs(1), backend.lookup_started.notified())
            .await
            .unwrap();
        let mut effective = fixture.state.effective().as_ref().clone();
        effective.accounts[0].config.auto_use_reset = false;
        effective.config.accounts[0].auto_use_reset = false;
        fixture
            .state
            .swap_effective(effective, Default::default())
            .unwrap();
        backend.release_lookup.notify_one();
        assert!(!task.await.unwrap());
        assert!(backend.keys.lock().await.is_empty());
    }

    #[tokio::test]
    async fn enabling_resets_recovers_an_already_exhausted_account() {
        let backend = Arc::new(Backend::default());
        let fixture = fixture(backend.clone()).await;
        let enabled = fixture.state.effective().as_ref().clone();
        let mut disabled = enabled.clone();
        disabled.accounts[0].config.auto_use_reset = false;
        disabled.config.accounts[0].auto_use_reset = false;
        fixture
            .state
            .swap_effective(disabled, Default::default())
            .unwrap();
        assert_eq!(send(&fixture.state).await, StatusCode::SERVICE_UNAVAILABLE);
        assert!(backend.keys.lock().await.is_empty());
        fixture
            .state
            .swap_effective(enabled, Default::default())
            .unwrap();
        assert_eq!(send(&fixture.state).await, StatusCode::OK);
        assert_eq!(backend.keys.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn account_replacement_invalidates_an_old_stream_reset_context() {
        let backend = Arc::new(Backend::default());
        let fixture = fixture(backend.clone()).await;
        let context = ResetContext::new(&fixture.state, &fixture.account).unwrap();
        let mut effective = fixture.state.effective().as_ref().clone();
        effective.accounts[0].chatgpt_account_id = Some("other-account".into());
        fixture
            .state
            .swap_effective(effective, Default::default())
            .unwrap();
        assert!(!context.recover().await);
        assert!(backend.keys.lock().await.is_empty());
    }
}
