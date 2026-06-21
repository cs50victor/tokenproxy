use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use axum::http::StatusCode;
use tokio::sync::{Mutex, watch};

use crate::auth::{ChatGptAuthCell, ChatGptAuthSnapshot, chatgpt_auth_cells};
use crate::config::{EffectiveAccount, EffectiveConfig};
use crate::error::{ErrorCode, TokenproxyError};
use crate::logging::{
    LogFormat, RequestLog, RouteSelectionLog, request_log_line, route_selection_log_line,
};
use crate::metrics::Metrics;
use crate::routing::AccountHealth;
use crate::usage::UsageWindow;

mod proxy;

pub use proxy::app;

#[derive(Clone)]
pub struct AppState {
    effective: Arc<EffectiveConfig>,
    // reqwest pools connections per origin internally, so one shared client
    // covers every upstream.
    upstream_client: reqwest::Client,
    request_counter: Arc<AtomicU64>,
    metrics: Metrics,
    usage_windows: Arc<Mutex<BTreeMap<String, Vec<UsageWindow>>>>,
    account_health: Arc<BTreeMap<String, Arc<AccountHealthCell>>>,
    chatgpt_auth: Arc<BTreeMap<String, Arc<ChatGptAuthCell>>>,
    log_format: LogFormat,
    shutdown_tx: watch::Sender<bool>,
}

const TRANSIENT_FAILURE_QUIET_RESET_MS: u64 = 5 * 60 * 1000;
const ACCOUNT_HEALTH_OPEN: u8 = 0;
const ACCOUNT_HEALTH_UNKNOWN: u8 = 1;
const ACCOUNT_HEALTH_THROTTLED: u8 = 2;
const ACCOUNT_HEALTH_USAGE_LIMITED: u8 = 3;
const ACCOUNT_HEALTH_AUTH_FAILED: u8 = 4;

#[derive(Debug)]
struct AccountHealthCell {
    state: AtomicU8,
    deadline_ms: AtomicU64,
    transient_failure_count: AtomicU32,
    last_transient_failure_ms: AtomicU64,
    ewma_connect_ms: AtomicU64,
    ewma_first_event_ms: AtomicU64,
}

impl AccountHealthCell {
    fn new() -> Self {
        Self {
            state: AtomicU8::new(ACCOUNT_HEALTH_OPEN),
            deadline_ms: AtomicU64::new(0),
            transient_failure_count: AtomicU32::new(0),
            last_transient_failure_ms: AtomicU64::new(0),
            ewma_connect_ms: AtomicU64::new(0),
            ewma_first_event_ms: AtomicU64::new(0),
        }
    }

    fn load(&self) -> AccountHealth {
        match self.state.load(Ordering::Acquire) {
            ACCOUNT_HEALTH_UNKNOWN => AccountHealth::Unknown,
            ACCOUNT_HEALTH_THROTTLED => AccountHealth::Throttled {
                next_retry_at_ms: self.deadline_ms.load(Ordering::Acquire),
            },
            ACCOUNT_HEALTH_USAGE_LIMITED => AccountHealth::UsageLimited {
                reset_at_ms: self.deadline_ms.load(Ordering::Acquire),
            },
            ACCOUNT_HEALTH_AUTH_FAILED => AccountHealth::AuthFailed,
            _ => AccountHealth::Open,
        }
    }

    fn transient_failure_count(&self) -> u32 {
        self.transient_failure_count.load(Ordering::Acquire)
    }

    fn record_connect_duration_ms(&self, duration_ms: u64) {
        update_latency_ewma(&self.ewma_connect_ms, duration_ms);
    }

    fn record_first_event_duration_ms(&self, duration_ms: u64) {
        update_latency_ewma(&self.ewma_first_event_ms, duration_ms);
    }

    fn connect_latency_bucket(&self) -> u16 {
        latency_bucket(self.ewma_connect_ms.load(Ordering::Acquire))
    }

    fn first_event_latency_bucket(&self) -> u16 {
        latency_bucket(self.ewma_first_event_ms.load(Ordering::Acquire))
    }

    fn increment_transient_failure_count_at(&self, now_ms: u64) -> u32 {
        loop {
            let previous_last = self.last_transient_failure_ms.load(Ordering::Acquire);
            let previous_count = self.transient_failure_count.load(Ordering::Acquire);
            let next_count = if previous_last == 0
                || now_ms.saturating_sub(previous_last) >= TRANSIENT_FAILURE_QUIET_RESET_MS
            {
                1
            } else {
                previous_count.saturating_add(1)
            };
            if self
                .transient_failure_count
                .compare_exchange(
                    previous_count,
                    next_count,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_ok()
            {
                self.last_transient_failure_ms
                    .store(now_ms, Ordering::Release);
                return next_count;
            }
        }
    }

    fn clear(&self) {
        self.state.store(ACCOUNT_HEALTH_OPEN, Ordering::Release);
        self.deadline_ms.store(0, Ordering::Release);
        self.transient_failure_count.store(0, Ordering::Release);
        self.last_transient_failure_ms.store(0, Ordering::Release);
    }
}

fn update_latency_ewma(cell: &AtomicU64, duration_ms: u64) {
    let sample = duration_ms.max(1);
    let mut current = cell.load(Ordering::Acquire);
    loop {
        let next = if current == 0 {
            sample
        } else {
            current
                .saturating_mul(7)
                .saturating_add(sample)
                .saturating_add(4)
                / 8
        };
        match cell.compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

fn latency_bucket(duration_ms: u64) -> u16 {
    match duration_ms {
        0 => 0,
        1..=25 => 1,
        26..=50 => 2,
        51..=100 => 3,
        101..=250 => 4,
        251..=500 => 5,
        501..=1_000 => 6,
        1_001..=2_500 => 7,
        2_501..=5_000 => 8,
        5_001..=10_000 => 9,
        _ => 10,
    }
}

impl AppState {
    pub fn new_with_log_format_and_shutdown(
        effective: EffectiveConfig,
        log_format: LogFormat,
        shutdown_tx: watch::Sender<bool>,
    ) -> Result<Self, TokenproxyError> {
        let upstream_client = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(effective.config.timeouts.connect_ms))
            .pool_idle_timeout(Duration::from_millis(
                effective.config.timeouts.pool_idle_ms,
            ))
            .build()
            .map_err(|error| {
                TokenproxyError::new(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    ErrorCode::InvalidConfig,
                    format!("failed to build upstream HTTP client: {error}"),
                )
            })?;
        let account_health = Arc::new(
            effective
                .config
                .accounts
                .iter()
                .map(|account| (account.id.clone(), Arc::new(AccountHealthCell::new())))
                .collect(),
        );
        let chatgpt_auth = Arc::new(chatgpt_auth_cells(&effective)?);
        let metrics_enabled = effective.config.observability.metrics;

        Ok(Self {
            effective: Arc::new(effective),
            upstream_client,
            request_counter: Arc::new(AtomicU64::new(1)),
            metrics: Metrics::with_enabled(metrics_enabled),
            usage_windows: Arc::new(Mutex::new(BTreeMap::new())),
            account_health,
            chatgpt_auth,
            log_format,
            shutdown_tx,
        })
    }

    fn next_request_id(&self) -> String {
        let id = self.request_counter.fetch_add(1, Ordering::Relaxed);
        format!("req_{id:016x}")
    }

    fn emit_request_log(&self, log: &RequestLog<'_>) {
        eprintln!("{}", request_log_line(self.log_format, log));
    }

    fn emit_route_selection_log(&self, log: &RouteSelectionLog<'_>) {
        eprintln!("{}", route_selection_log_line(self.log_format, log));
    }

    fn shutdown_receiver(&self) -> watch::Receiver<bool> {
        self.shutdown_tx.subscribe()
    }

    fn routing_accounts(&self) -> &[EffectiveAccount] {
        &self.effective.accounts
    }

    fn account_health_cell(&self, account_id: &str) -> Option<Arc<AccountHealthCell>> {
        self.account_health.get(account_id).cloned()
    }

    fn store_account_health(&self, account_id: &str, health: AccountHealth) {
        if let Some(cell) = self.account_health_cell(account_id) {
            match health {
                AccountHealth::Open => cell.clear(),
                AccountHealth::Unknown => {
                    cell.deadline_ms.store(0, Ordering::Release);
                    cell.transient_failure_count.store(0, Ordering::Release);
                    cell.last_transient_failure_ms.store(0, Ordering::Release);
                    cell.state.store(ACCOUNT_HEALTH_UNKNOWN, Ordering::Release);
                }
                AccountHealth::Throttled { next_retry_at_ms } => {
                    cell.deadline_ms.store(next_retry_at_ms, Ordering::Release);
                    cell.state
                        .store(ACCOUNT_HEALTH_THROTTLED, Ordering::Release);
                }
                AccountHealth::UsageLimited { reset_at_ms } => {
                    cell.deadline_ms.store(reset_at_ms, Ordering::Release);
                    cell.transient_failure_count.store(0, Ordering::Release);
                    cell.last_transient_failure_ms.store(0, Ordering::Release);
                    cell.state
                        .store(ACCOUNT_HEALTH_USAGE_LIMITED, Ordering::Release);
                }
                AccountHealth::AuthFailed => {
                    cell.deadline_ms.store(0, Ordering::Release);
                    cell.transient_failure_count.store(0, Ordering::Release);
                    cell.last_transient_failure_ms.store(0, Ordering::Release);
                    cell.state
                        .store(ACCOUNT_HEALTH_AUTH_FAILED, Ordering::Release);
                }
            }
        }
    }

    fn clear_account_health_if_not_auth_failed(&self, account_id: &str) {
        if let Some(cell) = self.account_health_cell(account_id)
            && cell.state.load(Ordering::Acquire) != ACCOUNT_HEALTH_AUTH_FAILED
        {
            cell.clear();
        }
    }

    fn account_health_snapshot(&self) -> BTreeMap<String, AccountHealth> {
        self.account_health
            .iter()
            .filter_map(|(account_id, cell)| {
                let health = cell.load();
                if health == AccountHealth::Open {
                    None
                } else {
                    Some((account_id.clone(), health))
                }
            })
            .collect()
    }

    async fn account_for_request(&self, account: &EffectiveAccount) -> EffectiveAccount {
        let Some(cell) = self.chatgpt_auth.get(&account.config.id) else {
            return account.clone();
        };
        apply_chatgpt_auth_snapshot(
            account,
            cell.snapshot_for_request(&self.upstream_client).await,
        )
    }

    async fn recover_chatgpt_unauthorized(
        &self,
        account: &EffectiveAccount,
    ) -> Result<Option<EffectiveAccount>, TokenproxyError> {
        let Some(cell) = self.chatgpt_auth.get(&account.config.id) else {
            return Ok(None);
        };
        let snapshot = cell
            .recover_after_unauthorized(&self.upstream_client, &account.bearer_token)
            .await?;
        self.store_account_health(&account.config.id, AccountHealth::Open);
        Ok(Some(apply_chatgpt_auth_snapshot(account, snapshot)))
    }

    fn chatgpt_bearer_authorized(&self, actual: &str) -> bool {
        self.chatgpt_auth
            .values()
            .any(|cell| cell.bearer_matches(actual))
    }
}

fn apply_chatgpt_auth_snapshot(
    account: &EffectiveAccount,
    snapshot: ChatGptAuthSnapshot,
) -> EffectiveAccount {
    let mut account = account.clone();
    account.bearer_token = snapshot.bearer_token;
    account.chatgpt_account_id = snapshot.account_id;
    account
}

#[cfg(test)]
impl AppState {
    fn new(effective: EffectiveConfig) -> Result<Self, TokenproxyError> {
        let (shutdown_tx, _) = watch::channel(false);
        Self::new_with_log_format_and_shutdown(effective, LogFormat::Text, shutdown_tx)
    }

    fn clear_account_health(&self, account_id: &str) {
        if let Some(cell) = self.account_health_cell(account_id) {
            cell.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_reset_transient_failure_count_after_quiet_window() {
        let cell = AccountHealthCell::new();

        assert_eq!(cell.increment_transient_failure_count_at(1_000), 1);
        assert_eq!(cell.increment_transient_failure_count_at(2_000), 2);
        assert_eq!(cell.increment_transient_failure_count_at(302_000), 1);
    }
}
