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

impl AccountConfig {
    pub(crate) fn supports_endpoint(&self, endpoint: Endpoint) -> bool {
        match endpoint {
            Endpoint::ChatCompletions => self.supports_chat_completions,
            Endpoint::Responses => self.supports_responses,
            Endpoint::ResponsesCompact => self.supports_compact,
            Endpoint::AnthropicMessages => self.supports_anthropic_messages,
        }
    }

    pub(crate) fn supports_model(&self, model: &str) -> bool {
        let model = model.trim();
        model.is_empty()
            || self
                .models
                .iter()
                .any(|candidate| candidate.trim().eq_ignore_ascii_case(model))
    }

    pub(crate) fn supports_service_tier(&self, service_tier: Option<&str>) -> bool {
        let Some(service_tier) = service_tier else {
            return true;
        };
        let requested = normalize_service_tier(service_tier);

        requested.is_empty()
            || requested.eq_ignore_ascii_case("auto")
            || requested.eq_ignore_ascii_case("default")
            || self
                .service_tiers
                .iter()
                .any(|candidate| normalize_service_tier(candidate).eq_ignore_ascii_case(requested))
    }
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

impl RouteRequest {
    pub(crate) fn normalized_service_tier(&self) -> Option<String> {
        self.service_tier
            .as_deref()
            .map(|service_tier| normalize_service_tier(service_tier).to_string())
    }
}
