use std::borrow::Cow;

use super::health::AccountHealth;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint {
    ChatCompletions,
    Responses,
    ResponsesCompact,
    AnthropicMessages,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    Http,
    WebSocket,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountConfig {
    pub id: String,
    pub priority: i32,
    pub models: Vec<String>,
    pub service_tiers: Vec<String>,
    pub supports_chat_completions: bool,
    pub supports_responses: bool,
    pub supports_responses_ws: bool,
    pub supports_incremental_previous_response_id: bool,
    pub supports_compact: bool,
    pub supports_anthropic_messages: bool,
}

// Legacy "fast" requests map to the "priority" service tier.
pub(crate) fn normalize_service_tier(service_tier: &str) -> &str {
    let trimmed = service_tier.trim();
    if trimmed.eq_ignore_ascii_case("fast") {
        "priority"
    } else {
        trimmed
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountState {
    pub config: AccountConfig,
    pub health: AccountHealth,
    pub ewma_connect_ms_bucket: u16,
    pub ewma_first_event_ms_bucket: u16,
    pub recent_failure_count: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteRequest {
    pub endpoint: Endpoint,
    pub transport: Transport,
    pub model: String,
    pub service_tier: Option<String>,
    pub pinned_account_id: Option<String>,
    pub requires_incremental_previous_response_id: bool,
    pub model_family: String,
    pub stream: bool,
}

pub fn model_family_label(model: &str) -> String {
    let model = model.trim();
    let model = if model.bytes().any(|byte| byte.is_ascii_uppercase()) {
        Cow::Owned(model.to_ascii_lowercase())
    } else {
        Cow::Borrowed(model)
    };
    if model.is_empty() || model == "unknown" {
        return "unknown".to_string();
    }

    let mut parts = model.split('-');
    let Some(prefix) = parts.next().filter(|part| !part.is_empty()) else {
        return "unknown".to_string();
    };

    let Some(version) = parts.next().filter(|part| !part.is_empty()) else {
        return prefix.to_string();
    };
    let major = version.split('.').next().unwrap_or(version);
    if major.is_empty() {
        prefix.to_string()
    } else {
        format!("{prefix}-{major}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_label_major_model_family() {
        assert_eq!(model_family_label("gpt-5.5"), "gpt-5");
        assert_eq!(model_family_label("GPT-5.5"), "gpt-5");
        assert_eq!(model_family_label("gpt-4o-mini"), "gpt-4o");
        assert_eq!(model_family_label("o3-mini"), "o3-mini");
        assert_eq!(model_family_label("unknown"), "unknown");
        assert_eq!(model_family_label("  "), "unknown");
    }
}
