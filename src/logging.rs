use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    Text,
    Json,
}

pub struct StartupLogLine<'a> {
    pub format: LogFormat,
    pub event: &'a str,
    pub timestamp_local: &'a str,
    pub timestamp_utc: &'a str,
    pub server_id: &'a str,
    pub bind: &'a str,
    pub enabled_accounts: usize,
    pub config: &'a StartupConfigSummary,
}

pub fn startup_log_line(input: StartupLogLine<'_>) -> String {
    match input.format {
        LogFormat::Text => format!(
            "tokenproxy {event}: timestamp_local={timestamp_local} timestamp_utc={timestamp_utc} server_id={server_id} bind={bind} enabled_accounts={enabled_accounts} model_count={} account_status_labels={} max_body_bytes={} connect_ms={} request_header_ms={} stream_idle_ms={} websocket_connect_ms={} websocket_idle_ms={} pool_idle_ms={} max_precommit_retries={} honor_retry_after={} metrics={} request_body_dumps={} allow_openai_request_headers={}",
            input.config.model_count,
            input.config.account_status_labels.join(","),
            input.config.max_body_bytes,
            input.config.connect_ms,
            input.config.request_header_ms,
            input.config.stream_idle_ms,
            input.config.websocket_connect_ms,
            input.config.websocket_idle_ms,
            input.config.pool_idle_ms,
            input.config.max_precommit_retries,
            input.config.honor_retry_after,
            input.config.metrics,
            input.config.request_body_dumps,
            input.config.allow_openai_request_headers,
            event = input.event,
            timestamp_local = input.timestamp_local,
            timestamp_utc = input.timestamp_utc,
            server_id = input.server_id,
            bind = input.bind,
            enabled_accounts = input.enabled_accounts,
        ),
        LogFormat::Json => serde_json::to_string(&StartupLog {
            event: input.event,
            timestamp_local: input.timestamp_local,
            timestamp_utc: input.timestamp_utc,
            server_id: input.server_id,
            bind: input.bind,
            enabled_accounts: input.enabled_accounts,
            model_count: input.config.model_count,
            account_status_labels: &input.config.account_status_labels,
            config: input.config,
        })
        .expect("startup log serializes"),
    }
}

pub fn request_log_line(format: LogFormat, log: &RequestLog<'_>) -> String {
    match format {
        LogFormat::Text => {
            let mut fields = vec![
                format!("timestamp_local={}", log.timestamp_local),
                format!("timestamp_utc={}", log.timestamp_utc),
                format!("tokenproxy_request_id={}", log.tokenproxy_request_id),
                format!("method={}", log.method),
                format!("endpoint={}", log.endpoint),
                format!("transport={}", log.transport),
                format!("status={}", log.status),
                format!("duration_ms={}", log.duration_ms),
            ];
            if let Some(account_id_hash) = log.account_id_hash {
                fields.push(format!("account_id_hash={account_id_hash}"));
            }
            if let Some(upstream_request_id) = log.upstream_request_id {
                fields.push(format!("upstream_request_id={upstream_request_id}"));
            }
            if let Some(cloudflare_ray) = log.cloudflare_ray {
                fields.push(format!("cloudflare_ray={cloudflare_ray}"));
            }
            if let Some(requested_service_tier) = log.requested_service_tier {
                fields.push(format!("requested_service_tier={requested_service_tier}"));
            }
            if let Some(reasoning_effort) = log.reasoning_effort {
                fields.push(format!("reasoning_effort={reasoning_effort}"));
            }
            if let Some(verbosity) = log.verbosity {
                fields.push(format!("verbosity={verbosity}"));
            }
            if let Some(store) = log.store {
                fields.push(format!("store={store}"));
            }
            if let Some(actual_service_tier) = log.actual_service_tier {
                fields.push(format!("actual_service_tier={actual_service_tier}"));
            }
            if let Some(cached_input_tokens) = log.cached_input_tokens {
                fields.push(format!("cached_input_tokens={cached_input_tokens}"));
            }
            if let Some(reasoning_tokens) = log.reasoning_tokens {
                fields.push(format!("reasoning_tokens={reasoning_tokens}"));
            }
            if let Some(error_code) = log.error_code {
                fields.push(format!("error_code={error_code}"));
            }
            format!("tokenproxy request: {}", fields.join(" "))
        }
        LogFormat::Json => serde_json::to_string(log).expect("request log serializes"),
    }
}

pub fn route_selection_log_line(format: LogFormat, log: &RouteSelectionLog<'_>) -> String {
    match format {
        LogFormat::Text => format!(
            "tokenproxy route_selection: level=debug timestamp_local={} timestamp_utc={} endpoint={} transport={} model_family={} selected_account_id_hash={} excluded_account_id_hash={} excluded_reason={}",
            log.timestamp_local,
            log.timestamp_utc,
            log.endpoint,
            log.transport,
            log.model_family,
            log.selected_account_id_hash,
            log.excluded_account_id_hash,
            log.excluded_reason,
        ),
        LogFormat::Json => serde_json::to_string(&serde_json::json!({
            "event": log.event,
            "level": "debug",
            "timestamp_local": log.timestamp_local,
            "timestamp_utc": log.timestamp_utc,
            "endpoint": log.endpoint,
            "transport": log.transport,
            "model_family": log.model_family,
            "selected_account_id_hash": log.selected_account_id_hash,
            "excluded_account_id_hash": log.excluded_account_id_hash,
            "excluded_reason": log.excluded_reason,
        }))
        .expect("route selection log serializes"),
    }
}

pub fn shutdown_forced_log_line(
    format: LogFormat,
    timestamp_local: &str,
    timestamp_utc: &str,
    grace_ms: u128,
) -> String {
    match format {
        LogFormat::Text => format!(
            "tokenproxy shutdown_forced: timestamp_local={timestamp_local} timestamp_utc={timestamp_utc} grace_ms={grace_ms}"
        ),
        LogFormat::Json => serde_json::to_string(&ShutdownForcedLog {
            event: "shutdown_forced",
            timestamp_local,
            timestamp_utc,
            grace_ms,
        })
        .expect("shutdown forced log serializes"),
    }
}

#[derive(Debug, Serialize)]
struct StartupLog<'a> {
    event: &'a str,
    timestamp_local: &'a str,
    timestamp_utc: &'a str,
    server_id: &'a str,
    bind: &'a str,
    enabled_accounts: usize,
    model_count: usize,
    account_status_labels: &'a [String],
    config: &'a StartupConfigSummary,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct StartupConfigSummary {
    pub max_body_bytes: usize,
    pub shutdown_grace_ms: u64,
    pub connect_ms: u64,
    pub request_header_ms: u64,
    pub stream_idle_ms: u64,
    pub websocket_connect_ms: u64,
    pub websocket_idle_ms: u64,
    pub pool_idle_ms: u64,
    pub max_precommit_retries: u8,
    pub honor_retry_after: bool,
    pub metrics: bool,
    pub request_body_dumps: bool,
    pub allow_openai_request_headers: bool,
    pub model_count: usize,
    pub account_status_labels: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct RequestLog<'a> {
    pub event: &'static str,
    pub timestamp_local: &'a str,
    pub timestamp_utc: &'a str,
    pub tokenproxy_request_id: &'a str,
    pub method: &'a str,
    pub endpoint: &'a str,
    pub transport: &'a str,
    pub status: u16,
    pub duration_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id_hash: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upstream_request_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cloudflare_ray: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub requested_service_tier: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verbosity: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub store: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actual_service_tier: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<&'a str>,
}

#[derive(Debug, Serialize)]
pub struct RouteSelectionLog<'a> {
    pub event: &'static str,
    pub timestamp_local: &'a str,
    pub timestamp_utc: &'a str,
    pub endpoint: &'a str,
    pub transport: &'a str,
    pub model_family: &'a str,
    pub selected_account_id_hash: &'a str,
    pub excluded_account_id_hash: &'a str,
    pub excluded_reason: &'a str,
}

#[derive(Debug, Serialize)]
struct ShutdownForcedLog<'a> {
    event: &'static str,
    timestamp_local: &'a str,
    timestamp_utc: &'a str,
    grace_ms: u128,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_render_startup_log_as_json_or_text() {
        let config = startup_config_summary();
        let json = startup_log_line(StartupLogLine {
            format: LogFormat::Json,
            event: "config_loaded",
            timestamp_local: "2026-05-27T04:24:18-07:00",
            timestamp_utc: "2026-05-27T11:24:18Z",
            server_id: "tokenproxy-local",
            bind: "127.0.0.1:8787",
            enabled_accounts: 2,
            config: &config,
        });
        let json: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(json["event"], "config_loaded");
        assert_eq!(json["timestamp_local"], "2026-05-27T04:24:18-07:00");
        assert_eq!(json["timestamp_utc"], "2026-05-27T11:24:18Z");
        assert_eq!(json["enabled_accounts"], 2);
        assert_eq!(json["model_count"], 3);
        assert_eq!(
            json["account_status_labels"],
            serde_json::json!(["unknown"])
        );
        assert_eq!(json["config"]["max_body_bytes"], 10485760);
        assert_eq!(json["config"]["connect_ms"], 3000);
        assert_eq!(json["config"]["allow_openai_request_headers"], false);
        assert!(json.get("token_env").is_none());
        assert!(json.get("bearer_token").is_none());

        let text = startup_log_line(StartupLogLine {
            format: LogFormat::Text,
            event: "config_loaded",
            timestamp_local: "2026-05-27T04:24:18-07:00",
            timestamp_utc: "2026-05-27T11:24:18Z",
            server_id: "tokenproxy-local",
            bind: "127.0.0.1:8787",
            enabled_accounts: 2,
            config: &config,
        });
        assert!(text.contains("tokenproxy config_loaded:"));
        assert!(text.contains("timestamp_local=2026-05-27T04:24:18-07:00"));
        assert!(text.contains("timestamp_utc=2026-05-27T11:24:18Z"));
        assert!(text.contains("enabled_accounts=2"));
        assert!(text.contains("model_count=3"));
        assert!(text.contains("account_status_labels=unknown"));
        assert!(text.contains("max_body_bytes=10485760"));
        assert!(text.contains("connect_ms=3000"));
        assert!(!text.contains("token_env"));
    }

    fn startup_config_summary() -> StartupConfigSummary {
        StartupConfigSummary {
            max_body_bytes: 10_485_760,
            shutdown_grace_ms: 30_000,
            connect_ms: 3_000,
            request_header_ms: 10_000,
            stream_idle_ms: 300_000,
            websocket_connect_ms: 15_000,
            websocket_idle_ms: 300_000,
            pool_idle_ms: 90_000,
            max_precommit_retries: 1,
            honor_retry_after: true,
            metrics: true,
            request_body_dumps: false,
            allow_openai_request_headers: false,
            model_count: 3,
            account_status_labels: vec!["unknown".to_string()],
        }
    }

    #[test]
    fn should_render_structured_request_log_without_secret_fields() {
        let log = RequestLog {
            event: "request",
            timestamp_local: "2026-05-27T04:24:18-07:00",
            timestamp_utc: "2026-05-27T11:24:18Z",
            tokenproxy_request_id: "req_0000000000000001",
            method: "POST",
            endpoint: "/v1/responses",
            transport: "http",
            status: 400,
            duration_ms: 7,
            account_id_hash: Some("acct_1234"),
            upstream_request_id: None,
            cloudflare_ray: Some("abc-LAX"),
            requested_service_tier: Some("priority"),
            reasoning_effort: Some("high"),
            verbosity: Some("low"),
            store: Some("false"),
            actual_service_tier: Some("default"),
            cached_input_tokens: Some(17),
            reasoning_tokens: Some(29),
            error_code: Some("invalid_json"),
        };

        let json = request_log_line(LogFormat::Json, &log);
        let json: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(json["event"], "request");
        assert_eq!(json["timestamp_local"], "2026-05-27T04:24:18-07:00");
        assert_eq!(json["timestamp_utc"], "2026-05-27T11:24:18Z");
        assert_eq!(json["tokenproxy_request_id"], "req_0000000000000001");
        assert_eq!(json["endpoint"], "/v1/responses");
        assert_eq!(json["status"], 400);
        assert_eq!(json["requested_service_tier"], "priority");
        assert_eq!(json["reasoning_effort"], "high");
        assert_eq!(json["verbosity"], "low");
        assert_eq!(json["store"], "false");
        assert_eq!(json["actual_service_tier"], "default");
        assert_eq!(json["cached_input_tokens"], 17);
        assert_eq!(json["reasoning_tokens"], 29);
        assert_eq!(json["error_code"], "invalid_json");
        assert!(json.get("authorization").is_none());
        assert!(json.get("bearer_token").is_none());

        let text = request_log_line(LogFormat::Text, &log);
        assert!(text.contains("timestamp_local=2026-05-27T04:24:18-07:00"));
        assert!(text.contains("timestamp_utc=2026-05-27T11:24:18Z"));
        assert!(text.contains("tokenproxy_request_id=req_0000000000000001"));
        assert!(text.contains("cloudflare_ray=abc-LAX"));
        assert!(text.contains("requested_service_tier=priority"));
        assert!(text.contains("reasoning_effort=high"));
        assert!(text.contains("verbosity=low"));
        assert!(text.contains("store=false"));
        assert!(text.contains("actual_service_tier=default"));
        assert!(text.contains("cached_input_tokens=17"));
        assert!(text.contains("reasoning_tokens=29"));
        assert!(!text.contains("authorization"));
    }

    #[test]
    fn should_render_route_selection_log_without_raw_account_ids() {
        let log = RouteSelectionLog {
            event: "route_selection",
            timestamp_local: "2026-05-27T04:24:18-07:00",
            timestamp_utc: "2026-05-27T11:24:18Z",
            endpoint: "/v1/responses",
            transport: "http",
            model_family: "gpt",
            selected_account_id_hash: "acct_selected",
            excluded_account_id_hash: "acct_excluded",
            excluded_reason: "model_unsupported",
        };

        let json = route_selection_log_line(LogFormat::Json, &log);
        let json: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(json["event"], "route_selection");
        assert_eq!(json["level"], "debug");
        assert_eq!(json["timestamp_local"], "2026-05-27T04:24:18-07:00");
        assert_eq!(json["timestamp_utc"], "2026-05-27T11:24:18Z");
        assert_eq!(json["selected_account_id_hash"], "acct_selected");
        assert_eq!(json["excluded_reason"], "model_unsupported");
        assert!(json.get("selected_account_id").is_none());
        assert!(json.get("excluded_account_id").is_none());

        let text = route_selection_log_line(LogFormat::Text, &log);
        assert!(text.contains("level=debug"));
        assert!(text.contains("timestamp_local=2026-05-27T04:24:18-07:00"));
        assert!(text.contains("timestamp_utc=2026-05-27T11:24:18Z"));
        assert!(text.contains("selected_account_id_hash=acct_selected"));
        assert!(text.contains("excluded_reason=model_unsupported"));
        assert!(!text.contains("selected_account_id=primary"));
    }

    #[test]
    fn should_render_forced_shutdown_log_with_local_and_utc_timestamps() {
        let json = shutdown_forced_log_line(
            LogFormat::Json,
            "2026-05-27T04:24:18-07:00",
            "2026-05-27T11:24:18Z",
            30_000,
        );
        let json: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(json["event"], "shutdown_forced");
        assert_eq!(json["timestamp_local"], "2026-05-27T04:24:18-07:00");
        assert_eq!(json["timestamp_utc"], "2026-05-27T11:24:18Z");
        assert_eq!(json["grace_ms"], 30_000);

        let text = shutdown_forced_log_line(
            LogFormat::Text,
            "2026-05-27T04:24:18-07:00",
            "2026-05-27T11:24:18Z",
            30_000,
        );
        assert!(text.contains("tokenproxy shutdown_forced:"));
        assert!(text.contains("timestamp_local=2026-05-27T04:24:18-07:00"));
        assert!(text.contains("timestamp_utc=2026-05-27T11:24:18Z"));
        assert!(text.contains("grace_ms=30000"));
    }
}
