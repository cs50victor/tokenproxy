use std::cmp::Reverse;

use super::account::{AccountState, Endpoint, RouteRequest, Transport, normalize_service_tier};
use super::health::AccountHealth;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExclusionReason {
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
pub struct Selection {
    pub selected: Option<String>,
    pub excluded: Vec<(String, ExclusionReason)>,
}

pub fn select_account(accounts: &[AccountState], request: &RouteRequest, now_ms: u64) -> Selection {
    let mut excluded = Vec::new();
    let mut selected: Option<(AccountScore, &AccountState)> = None;

    for account in accounts {
        let reason = match account.health {
            AccountHealth::Open | AccountHealth::Unknown => None,
            AccountHealth::Throttled { next_retry_at_ms } if now_ms >= next_retry_at_ms => None,
            AccountHealth::Throttled { .. } => Some(ExclusionReason::ThrottledCooldown),
            AccountHealth::UsageLimited { reset_at_ms } if now_ms >= reset_at_ms => None,
            AccountHealth::UsageLimited { .. } => Some(ExclusionReason::UsageLimited),
            AccountHealth::AuthFailed => Some(ExclusionReason::AuthFailed),
        }
        .or_else(|| static_exclusion_reason(account, request));

        if let Some(reason) = reason {
            excluded.push((account.config.id.clone(), reason));
            continue;
        }

        let score = AccountScore {
            health_penalty: match account.health {
                AccountHealth::Open => 0,
                AccountHealth::Unknown => 20,
                AccountHealth::Throttled { .. } | AccountHealth::UsageLimited { .. } => 40,
                AccountHealth::AuthFailed => 100,
            },
            priority: Reverse(account.config.priority),
            ewma_connect_ms_bucket: account.ewma_connect_ms_bucket,
            ewma_first_event_ms_bucket: account.ewma_first_event_ms_bucket,
            recent_failure_count: account.recent_failure_count,
            stable_hash: stable_hash(&[&account.config.id, &request.model_family]),
        };
        if selected.is_none_or(|(best, _)| score < best) {
            selected = Some((score, account));
        }
    }

    Selection {
        selected: selected.map(|(_, account)| account.config.id.clone()),
        excluded,
    }
}

pub fn account_static_compatible(account: &AccountState, request: &RouteRequest) -> bool {
    static_exclusion_reason(account, request).is_none()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct AccountScore {
    health_penalty: u8,
    priority: Reverse<i32>,
    ewma_connect_ms_bucket: u16,
    ewma_first_event_ms_bucket: u16,
    recent_failure_count: u32,
    stable_hash: u64,
}

fn static_exclusion_reason(
    account: &AccountState,
    request: &RouteRequest,
) -> Option<ExclusionReason> {
    let supports_endpoint = match request.endpoint {
        Endpoint::ChatCompletions => account.config.supports_chat_completions,
        Endpoint::Responses => account.config.supports_responses,
        Endpoint::ResponsesCompact => account.config.supports_compact,
        Endpoint::AnthropicMessages => account.config.supports_anthropic_messages,
    };
    if !supports_endpoint {
        return Some(ExclusionReason::EndpointUnsupported);
    }

    if request.endpoint != Endpoint::ResponsesCompact && !account.config.models.is_empty() {
        let model = request.model.trim();
        if !model.is_empty()
            && !account
                .config
                .models
                .iter()
                .any(|candidate| candidate.trim().eq_ignore_ascii_case(model))
        {
            return Some(ExclusionReason::ModelUnsupported);
        }
    }

    if let Some(service_tier) = request.service_tier.as_deref() {
        let requested = normalize_service_tier(service_tier);
        if !requested.is_empty()
            && !requested.eq_ignore_ascii_case("auto")
            && !requested.eq_ignore_ascii_case("default")
            && !account
                .config
                .service_tiers
                .iter()
                .any(|candidate| normalize_service_tier(candidate).eq_ignore_ascii_case(requested))
        {
            return Some(ExclusionReason::ServiceTierUnsupported);
        }
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

    if let Some(pinned_account_id) = &request.pinned_account_id
        && account.config.id != *pinned_account_id
    {
        return Some(ExclusionReason::PinnedContinuationMismatch);
    }

    None
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
                priority: 10,
                models: vec!["gpt-5.5".to_string()],
                service_tiers: vec!["priority".to_string()],
                supports_chat_completions: true,
                supports_responses: true,
                supports_responses_ws: true,
                supports_incremental_previous_response_id: true,
                supports_compact: true,
                supports_anthropic_messages: true,
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
            requires_incremental_previous_response_id: false,
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

        assert_eq!(selection.selected.unwrap(), "high");
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
                .unwrap(),
            "priority-only"
        );

        let mut auto_tier = request();
        auto_tier.service_tier = Some("auto".to_string());

        assert_eq!(
            select_account(&[priority_only.clone()], &auto_tier, 1_000)
                .selected
                .unwrap(),
            "priority-only"
        );

        let mut omitted_tier = request();
        omitted_tier.service_tier = None;

        assert_eq!(
            select_account(&[priority_only], &omitted_tier, 1_000)
                .selected
                .unwrap(),
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

        assert_eq!(selection.selected.unwrap(), "incremental");
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

        assert_eq!(selection.selected.unwrap(), "pinned");
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

        assert_eq!(selection.selected.unwrap(), "compact");
        assert!(selection.excluded.is_empty());
    }

    #[test]
    fn should_not_model_filter_accounts_without_static_models() {
        let mut discovered_later = account("discovered-later");
        discovered_later.config.models.clear();
        let mut req = request();
        req.model = "gpt-5.4".to_string();

        let selection = select_account(&[discovered_later], &req, 1_000);

        assert_eq!(selection.selected.unwrap(), "discovered-later");
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
