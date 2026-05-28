use std::cmp::Reverse;

use super::account::{AccountState, Endpoint, RouteRequest, Transport};
use super::health::AccountHealth;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExclusionReason {
    Disabled,
    AuthFailed,
    UsageLimited,
    ThrottledCooldown,
    EndpointUnsupported,
    ModelUnsupported,
    ServiceTierUnsupported,
    WebSocketUnsupported,
    WebSocketContinuationUnsupported,
    PinnedContinuationMismatch,
}

impl ExclusionReason {
    pub fn as_str(self) -> &'static str {
        match self {
            ExclusionReason::Disabled => "disabled",
            ExclusionReason::AuthFailed => "auth_failed",
            ExclusionReason::UsageLimited => "usage_limited",
            ExclusionReason::ThrottledCooldown => "throttled_cooldown",
            ExclusionReason::EndpointUnsupported => "endpoint_unsupported",
            ExclusionReason::ModelUnsupported => "model_unsupported",
            ExclusionReason::ServiceTierUnsupported => "service_tier_unsupported",
            ExclusionReason::WebSocketUnsupported => "websocket_unsupported",
            ExclusionReason::WebSocketContinuationUnsupported => {
                "websocket_continuation_unsupported"
            }
            ExclusionReason::PinnedContinuationMismatch => "pinned_continuation_mismatch",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedAccount {
    pub account_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Selection {
    pub selected: Option<SelectedAccount>,
    pub excluded: Vec<(String, ExclusionReason)>,
}

pub fn select_account(accounts: &[AccountState], request: &RouteRequest, now_ms: u64) -> Selection {
    let mut excluded = Vec::new();
    let mut eligible = Vec::new();

    for account in accounts {
        if let Some(reason) = exclusion_reason(account, request, now_ms) {
            excluded.push((account.config.id.clone(), reason));
            continue;
        }

        eligible.push((score(account, request), account));
    }

    eligible.sort_by_key(|(score, _)| *score);

    Selection {
        selected: eligible.first().map(|(_, account)| SelectedAccount {
            account_id: account.config.id.clone(),
        }),
        excluded,
    }
}

pub fn account_static_compatible(account: &AccountState, request: &RouteRequest) -> bool {
    static_exclusion_reason(account, request).is_none()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct AccountScore {
    continuation_penalty: u8,
    health_penalty: u8,
    priority: Reverse<i32>,
    ewma_connect_ms_bucket: u16,
    ewma_first_event_ms_bucket: u16,
    recent_failure_count: u32,
    stable_hash: u64,
}

fn exclusion_reason(
    account: &AccountState,
    request: &RouteRequest,
    now_ms: u64,
) -> Option<ExclusionReason> {
    if !account.config.enabled {
        return Some(ExclusionReason::Disabled);
    }

    match account.health {
        AccountHealth::Open | AccountHealth::Unknown => {}
        AccountHealth::Throttled { next_retry_at_ms } if now_ms >= next_retry_at_ms => {}
        AccountHealth::Throttled { .. } => return Some(ExclusionReason::ThrottledCooldown),
        AccountHealth::UsageLimited { reset_at_ms } if now_ms >= reset_at_ms => {}
        AccountHealth::UsageLimited { .. } => return Some(ExclusionReason::UsageLimited),
        AccountHealth::AuthFailed => return Some(ExclusionReason::AuthFailed),
    }

    static_exclusion_after_enabled(account, request)
}

fn static_exclusion_reason(
    account: &AccountState,
    request: &RouteRequest,
) -> Option<ExclusionReason> {
    if !account.config.enabled {
        return Some(ExclusionReason::Disabled);
    }

    static_exclusion_after_enabled(account, request)
}

fn static_exclusion_after_enabled(
    account: &AccountState,
    request: &RouteRequest,
) -> Option<ExclusionReason> {
    if !supports_endpoint(account, request.endpoint) {
        return Some(ExclusionReason::EndpointUnsupported);
    }

    if request.endpoint != Endpoint::ResponsesCompact && !supports_model(account, &request.model) {
        return Some(ExclusionReason::ModelUnsupported);
    }

    if !supports_service_tier(account, request.service_tier.as_deref()) {
        return Some(ExclusionReason::ServiceTierUnsupported);
    }

    if request.transport == Transport::WebSocket && !account.config.supports_responses_ws {
        return Some(ExclusionReason::WebSocketUnsupported);
    }

    if request.transport == Transport::WebSocket
        && request.requires_incremental_previous_response_id
        && !account.config.supports_incremental_previous_response_id
    {
        return Some(ExclusionReason::WebSocketContinuationUnsupported);
    }

    if let Some(pinned_account_id) = &request.pinned_account_id {
        let failover_allowed =
            request.allow_failover_from_pinned && request.replay_can_remove_previous_response_id;

        if account.config.id != *pinned_account_id && !failover_allowed {
            return Some(ExclusionReason::PinnedContinuationMismatch);
        }
    }

    None
}

fn score(account: &AccountState, request: &RouteRequest) -> AccountScore {
    AccountScore {
        continuation_penalty: continuation_penalty(account, request),
        health_penalty: health_penalty(&account.health),
        priority: Reverse(account.config.priority),
        ewma_connect_ms_bucket: account.ewma_connect_ms_bucket,
        ewma_first_event_ms_bucket: account.ewma_first_event_ms_bucket,
        recent_failure_count: account.recent_failure_count,
        stable_hash: stable_hash(&[
            &account.config.id,
            &request.caller_hash,
            &request.model_family,
        ]),
    }
}

fn supports_endpoint(account: &AccountState, endpoint: Endpoint) -> bool {
    match endpoint {
        Endpoint::ChatCompletions => account.config.supports_chat_completions,
        Endpoint::Responses => account.config.supports_responses,
        Endpoint::ResponsesCompact => account.config.supports_compact,
    }
}

fn supports_model(account: &AccountState, model: &str) -> bool {
    model.trim().is_empty()
        || account
            .config
            .models
            .iter()
            .any(|candidate| normalize_model(candidate) == normalize_model(model))
}

fn supports_service_tier(account: &AccountState, service_tier: Option<&str>) -> bool {
    let Some(service_tier) = service_tier else {
        return true;
    };
    let normalized = normalize_service_tier(service_tier);

    normalized.is_empty()
        || normalized == "auto"
        || normalized == "default"
        || account
            .config
            .service_tiers
            .iter()
            .any(|candidate| normalize_service_tier(candidate) == normalized)
}

fn continuation_penalty(account: &AccountState, request: &RouteRequest) -> u8 {
    match &request.pinned_account_id {
        Some(pinned_account_id) if *pinned_account_id == account.config.id => 0,
        Some(_) => 10,
        None => 0,
    }
}

fn health_penalty(health: &AccountHealth) -> u8 {
    match health {
        AccountHealth::Open => 0,
        AccountHealth::Unknown => 20,
        AccountHealth::Throttled { .. } | AccountHealth::UsageLimited { .. } => 40,
        AccountHealth::AuthFailed => 100,
    }
}

fn normalize_model(model: &str) -> String {
    model.trim().to_ascii_lowercase()
}

fn normalize_service_tier(service_tier: &str) -> String {
    match service_tier.trim().to_ascii_lowercase().as_str() {
        "fast" => "priority".to_string(),
        normalized => normalized.to_string(),
    }
}

fn stable_hash(parts: &[&str]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;

    for part in parts {
        for byte in part.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
        hash ^= 0xff;
        hash = hash.wrapping_mul(0x100000001b3);
    }

    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routing::account::{AccountConfig, AccountState};

    fn account(id: &str) -> AccountState {
        AccountState {
            config: AccountConfig {
                id: id.to_string(),
                enabled: true,
                priority: 10,
                models: vec!["gpt-5.5".to_string()],
                service_tiers: vec!["priority".to_string()],
                supports_chat_completions: true,
                supports_responses: true,
                supports_responses_ws: true,
                supports_incremental_previous_response_id: true,
                supports_compact: true,
            },
            health: AccountHealth::Open,
            ewma_connect_ms_bucket: 1,
            ewma_first_event_ms_bucket: 1,
            recent_failure_count: 0,
        }
    }

    fn request() -> RouteRequest {
        RouteRequest {
            endpoint: Endpoint::Responses,
            transport: Transport::Http,
            model: "gpt-5.5".to_string(),
            service_tier: None,
            pinned_account_id: None,
            allow_failover_from_pinned: false,
            replay_can_remove_previous_response_id: false,
            requires_incremental_previous_response_id: false,
            caller_hash: "caller".to_string(),
            model_family: "gpt-5".to_string(),
            stream: false,
        }
    }

    #[test]
    fn should_select_highest_priority_eligible_account() {
        let low = account("low");
        let mut high = account("high");
        high.config.priority = 50;

        let selection = select_account(&[low, high], &request(), 1_000);

        assert_eq!(selection.selected.unwrap().account_id, "high");
    }

    #[test]
    fn should_exclude_accounts_by_model_tier_transport_and_health() {
        let mut wrong_model = account("wrong-model");
        wrong_model.config.models = vec!["gpt-4.1".to_string()];

        let mut no_priority = account("no-priority");
        no_priority.config.service_tiers = vec!["default".to_string()];

        let mut no_ws = account("no-ws");
        no_ws.config.supports_responses_ws = false;

        let mut throttled = account("throttled");
        throttled.health = AccountHealth::Throttled {
            next_retry_at_ms: 2_000,
        };

        let mut auth_failed = account("auth-failed");
        auth_failed.health = AccountHealth::AuthFailed;

        let mut usage_limited = account("usage-limited");
        usage_limited.health = AccountHealth::UsageLimited { reset_at_ms: 2_000 };

        let mut req = request();
        req.transport = Transport::WebSocket;
        req.service_tier = Some("fast".to_string());

        let selection = select_account(
            &[
                wrong_model,
                no_priority,
                no_ws,
                throttled,
                auth_failed,
                usage_limited,
            ],
            &req,
            1_000,
        );

        assert_eq!(selection.selected, None);
        assert_eq!(
            selection.excluded,
            vec![
                ("wrong-model".to_string(), ExclusionReason::ModelUnsupported),
                (
                    "no-priority".to_string(),
                    ExclusionReason::ServiceTierUnsupported
                ),
                ("no-ws".to_string(), ExclusionReason::WebSocketUnsupported),
                ("throttled".to_string(), ExclusionReason::ThrottledCooldown),
                ("auth-failed".to_string(), ExclusionReason::AuthFailed),
                ("usage-limited".to_string(), ExclusionReason::UsageLimited),
            ]
        );
    }

    #[test]
    fn should_accept_default_service_tiers_without_account_allowlist_entry() {
        let mut priority_only = account("priority-only");
        priority_only.config.service_tiers = vec!["priority".to_string()];

        let mut default_tier = request();
        default_tier.service_tier = Some("default".to_string());

        assert_eq!(
            select_account(&[priority_only.clone()], &default_tier, 1_000)
                .selected
                .unwrap()
                .account_id,
            "priority-only"
        );

        let mut auto_tier = request();
        auto_tier.service_tier = Some("auto".to_string());

        assert_eq!(
            select_account(&[priority_only.clone()], &auto_tier, 1_000)
                .selected
                .unwrap()
                .account_id,
            "priority-only"
        );

        let mut omitted_tier = request();
        omitted_tier.service_tier = None;

        assert_eq!(
            select_account(&[priority_only], &omitted_tier, 1_000)
                .selected
                .unwrap()
                .account_id,
            "priority-only"
        );
    }

    #[test]
    fn should_reject_explicit_non_default_service_tier_not_named_by_account() {
        let mut priority_only = account("priority-only");
        priority_only.config.service_tiers = vec!["priority".to_string()];
        let mut req = request();
        req.service_tier = Some("flex".to_string());

        let selection = select_account(&[priority_only], &req, 1_000);

        assert_eq!(selection.selected, None);
        assert_eq!(
            selection.excluded,
            vec![(
                "priority-only".to_string(),
                ExclusionReason::ServiceTierUnsupported
            )]
        );
    }

    #[test]
    fn should_require_incremental_support_when_websocket_request_has_previous_response_id() {
        let mut non_incremental = account("non-incremental");
        non_incremental
            .config
            .supports_incremental_previous_response_id = false;
        let mut incremental = account("incremental");
        incremental.config.priority = 1;

        let mut req = request();
        req.transport = Transport::WebSocket;
        req.requires_incremental_previous_response_id = true;

        let selection = select_account(&[non_incremental, incremental], &req, 1_000);

        assert_eq!(selection.selected.unwrap().account_id, "incremental");
        assert_eq!(
            selection.excluded,
            vec![(
                "non-incremental".to_string(),
                ExclusionReason::WebSocketContinuationUnsupported,
            )]
        );
    }

    #[test]
    fn should_keep_pinned_continuation_on_the_same_account() {
        let mut pinned = account("pinned");
        pinned.config.priority = 1;
        let other = account("other");

        let mut req = request();
        req.pinned_account_id = Some("pinned".to_string());

        let selection = select_account(&[other, pinned], &req, 1_000);

        assert_eq!(selection.selected.unwrap().account_id, "pinned");
        assert_eq!(
            selection.excluded,
            vec![(
                "other".to_string(),
                ExclusionReason::PinnedContinuationMismatch
            )]
        );
    }

    #[test]
    fn should_not_model_filter_compact_requests() {
        let mut compact = request();
        compact.endpoint = Endpoint::ResponsesCompact;
        compact.model.clear();
        compact.model_family = "unknown".to_string();

        let selection = select_account(&[account("compact")], &compact, 1_000);

        assert_eq!(selection.selected.unwrap().account_id, "compact");
        assert!(selection.excluded.is_empty());
    }

    #[test]
    fn should_use_deterministic_tie_break_for_equal_scores() {
        let a = account("a");
        let b = account("b");

        let first = select_account(&[a.clone(), b.clone()], &request(), 1_000);
        let second = select_account(&[b, a], &request(), 1_000);

        assert_eq!(first.selected, second.selected);
    }
}
