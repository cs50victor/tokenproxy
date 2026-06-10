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
