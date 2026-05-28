#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountHealth {
    Open,
    Unknown,
    Throttled { next_retry_at_ms: u64 },
    UsageLimited { reset_at_ms: u64 },
    AuthFailed,
}

impl AccountHealth {
    pub fn as_str(&self) -> &'static str {
        match self {
            AccountHealth::Open => "open",
            AccountHealth::Unknown => "unknown",
            AccountHealth::Throttled { .. } => "throttled",
            AccountHealth::UsageLimited { .. } => "usage_limited",
            AccountHealth::AuthFailed => "auth_failed",
        }
    }
}
