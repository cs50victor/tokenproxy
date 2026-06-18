use std::collections::BTreeMap;

use axum::http::HeaderMap;
use axum::http::StatusCode;
use serde::Serialize;

use crate::config::AccountConfig;
use crate::observability::sha256_hex;
use crate::routing::AccountHealth;
use crate::time_parse::{
    normalize_rfc3339, rfc3339_after_duration, rfc3339_after_seconds, rfc3339_from_unix_ms,
    unix_ms_from_rfc3339,
};

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct UsageSnapshot {
    pub server_id: String,
    pub observed_at: String,
    pub accounts: Vec<AccountUsage>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct AccountUsage {
    pub account_id_hash: String,
    pub display_name: Option<String>,
    pub health: String,
    pub usage: Vec<UsageWindow>,
    pub cooldown_until: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct UsageWindow {
    pub window: String,
    pub limit: Option<u64>,
    pub remaining: Option<u64>,
    pub remaining_percent: Option<f64>,
    pub rate_limit_pressure: String,
    pub reset_after: Option<String>,
    pub reset_at: Option<String>,
    pub source: String,
    pub observed_at: String,
    pub limited: bool,
}

pub fn usage_snapshot(
    server_id: &str,
    observed_at: &str,
    accounts: &[AccountConfig],
    usage_by_account: &BTreeMap<String, Vec<UsageWindow>>,
    health_by_account: &BTreeMap<String, AccountHealth>,
    account_hash_key: &str,
) -> UsageSnapshot {
    UsageSnapshot {
        server_id: server_id.to_string(),
        observed_at: observed_at.to_string(),
        accounts: accounts
            .iter()
            .map(|account| AccountUsage {
                account_id_hash: account_id_hash(&account.id, account_hash_key),
                display_name: account.display_name.clone(),
                health: if account.enabled {
                    let usage_health = usage_health_from_windows(
                        usage_by_account.get(&account.id).map(Vec::as_slice),
                    );
                    if matches!(usage_health, AccountHealth::UsageLimited { .. }) {
                        usage_health
                    } else {
                        health_by_account
                            .get(&account.id)
                            .cloned()
                            .unwrap_or(AccountHealth::Open)
                    }
                    .as_str()
                    .to_string()
                } else {
                    "disabled".to_string()
                },
                usage: usage_by_account
                    .get(&account.id)
                    .cloned()
                    .unwrap_or_default(),
                cooldown_until: cooldown_until(
                    usage_by_account.get(&account.id),
                    health_by_account.get(&account.id),
                ),
            })
            .collect(),
    }
}

pub fn usage_windows_from_headers(headers: &HeaderMap, observed_at: &str) -> Vec<UsageWindow> {
    let mut windows = Vec::new();
    push_rate_window(headers, observed_at, &mut windows, "requests");
    push_rate_window(headers, observed_at, &mut windows, "tokens");
    windows
}

pub fn usage_windows_from_error_body(
    status: StatusCode,
    body: &[u8],
    observed_at: &str,
) -> Vec<UsageWindow> {
    if status != StatusCode::TOO_MANY_REQUESTS {
        return Vec::new();
    }
    usage_windows_from_usage_limit_error_body(body, observed_at)
}

pub fn usage_windows_from_usage_limit_error_body(
    body: &[u8],
    observed_at: &str,
) -> Vec<UsageWindow> {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return Vec::new();
    };
    usage_windows_from_usage_limit_error_value(&value, observed_at)
}

pub fn usage_windows_from_usage_limit_error_value(
    value: &serde_json::Value,
    observed_at: &str,
) -> Vec<UsageWindow> {
    use serde_json::Value;

    if value.pointer("/error/code").and_then(Value::as_str) != Some("usage_limit_reached") {
        return Vec::new();
    }
    let reset_seconds = value
        .pointer("/error/resets_in_seconds")
        .and_then(Value::as_f64)
        .filter(|seconds| seconds.is_finite() && *seconds >= 0.0);
    let reset_at = value
        .pointer("/error/resets_at")
        .and_then(Value::as_str)
        .and_then(normalize_rfc3339)
        .or_else(|| rfc3339_after_seconds(observed_at, reset_seconds?));

    vec![UsageWindow {
        window: "codex_usage_limit".to_string(),
        limit: None,
        remaining: Some(0),
        remaining_percent: None,
        rate_limit_pressure: "limited".to_string(),
        reset_after: reset_seconds.map(|seconds| format!("{seconds}s")),
        reset_at,
        source: "usage_limit_reached_error".to_string(),
        observed_at: observed_at.to_string(),
        limited: true,
    }]
}

// A usage_limit error without a parseable reset time would otherwise exclude the
// account from routing until process restart; re-probe after a bounded cooldown.
const USAGE_LIMIT_DEFAULT_COOLDOWN_MS: u64 = 300_000;

pub fn usage_health_from_windows(windows: Option<&[UsageWindow]>) -> AccountHealth {
    let Some(windows) = windows else {
        return AccountHealth::Open;
    };
    if !windows.iter().any(|window| window.limited) {
        return AccountHealth::Open;
    }
    let reset_at_ms = earliest_limited_reset(windows)
        .map(|(reset_ms, _)| reset_ms)
        .or_else(|| {
            windows
                .iter()
                .filter(|window| window.limited)
                .filter_map(|window| unix_ms_from_rfc3339(&window.observed_at))
                .min()
                .map(|observed_ms| observed_ms.saturating_add(USAGE_LIMIT_DEFAULT_COOLDOWN_MS))
        })
        .unwrap_or(u64::MAX);
    AccountHealth::UsageLimited { reset_at_ms }
}

fn earliest_limited_reset(windows: &[UsageWindow]) -> Option<(u64, &str)> {
    windows
        .iter()
        .filter(|window| window.limited)
        .filter_map(|window| {
            let reset_at = window.reset_at.as_deref()?;
            Some((unix_ms_from_rfc3339(reset_at)?, reset_at))
        })
        .min_by_key(|(reset_ms, _)| *reset_ms)
}

fn push_rate_window(
    headers: &HeaderMap,
    observed_at: &str,
    windows: &mut Vec<UsageWindow>,
    suffix: &str,
) {
    let limit = header_u64(headers, &format!("x-ratelimit-limit-{suffix}"));
    let remaining = header_u64(headers, &format!("x-ratelimit-remaining-{suffix}"));
    let reset_after = headers
        .get(format!("x-ratelimit-reset-{suffix}"))
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let reset_at = reset_after
        .as_deref()
        .and_then(|reset_after| rfc3339_after_duration(observed_at, reset_after));

    if limit.is_none() && remaining.is_none() && reset_after.is_none() {
        return;
    }

    let limited = remaining == Some(0);
    let remaining_percent = remaining_percent(limit, remaining);

    windows.push(UsageWindow {
        window: format!("openai_{suffix}"),
        limit,
        remaining,
        remaining_percent,
        rate_limit_pressure: if limited || remaining == Some(0) {
            "limited"
        } else if let Some(percent) = remaining_percent {
            if percent < 5.0 {
                "high"
            } else if percent < 20.0 {
                "medium"
            } else if percent < 50.0 {
                "low"
            } else {
                "none"
            }
        } else {
            "unknown"
        }
        .to_string(),
        reset_after,
        reset_at,
        source: "openai_ratelimit_headers".to_string(),
        observed_at: observed_at.to_string(),
        limited,
    });
}

fn cooldown_until(
    windows: Option<&Vec<UsageWindow>>,
    runtime_health: Option<&AccountHealth>,
) -> Option<String> {
    let usage_reset = windows
        .and_then(|windows| earliest_limited_reset(windows))
        .map(|(_, reset_at)| reset_at.to_string());
    usage_reset.or_else(|| match runtime_health {
        Some(AccountHealth::Throttled { next_retry_at_ms }) => {
            rfc3339_from_unix_ms(*next_retry_at_ms)
        }
        Some(AccountHealth::UsageLimited { reset_at_ms }) => rfc3339_from_unix_ms(*reset_at_ms),
        _ => None,
    })
}

fn header_u64(headers: &HeaderMap, name: &str) -> Option<u64> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
}

fn remaining_percent(limit: Option<u64>, remaining: Option<u64>) -> Option<f64> {
    let (Some(limit), Some(remaining)) = (limit, remaining) else {
        return None;
    };
    if limit == 0 {
        return None;
    }
    Some((remaining as f64 / limit as f64) * 100.0)
}

pub fn account_id_hash(account_id: &str, hash_key: &str) -> String {
    let digest =
        sha256_hex(format!("tokenproxy-account-id-hash-v1\0{hash_key}\0{account_id}").as_bytes());
    format!("sha256:{}", &digest[..16])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AccountConfig;
    use crate::time_parse::now_rfc3339;
    use chrono::DateTime;

    #[test]
    fn should_hash_account_ids_with_configured_key() {
        let primary_hash = account_id_hash("primary", "key-a");
        let rotated_hash = account_id_hash("primary", "key-b");

        assert!(primary_hash.starts_with("sha256:"));
        assert_ne!(primary_hash, rotated_hash);
        assert!(!primary_hash.contains("primary"));
    }

    #[test]
    fn should_compute_remaining_percent_only_when_limit_and_remaining_exist() {
        assert_eq!(remaining_percent(Some(500), Some(499)), Some(99.8));
        assert_eq!(remaining_percent(Some(0), Some(0)), None);
        assert_eq!(remaining_percent(None, Some(1)), None);
    }

    #[test]
    fn should_extract_openai_rate_limit_usage_windows_from_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("x-ratelimit-limit-requests", "500".parse().unwrap());
        headers.insert("x-ratelimit-remaining-requests", "499".parse().unwrap());
        headers.insert("x-ratelimit-reset-requests", "120ms".parse().unwrap());

        let windows = usage_windows_from_headers(&headers, "2026-05-27T11:24:18-07:00");

        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].window, "openai_requests");
        assert_eq!(windows[0].remaining_percent, Some(99.8));
        assert_eq!(windows[0].rate_limit_pressure, "none");
        assert_eq!(windows[0].reset_after.as_deref(), Some("120ms"));
        let reset_at = windows[0].reset_at.as_deref().unwrap();
        let reset_at = DateTime::parse_from_rfc3339(reset_at).unwrap();
        let expected = DateTime::parse_from_rfc3339("2026-05-27T11:24:18.12-07:00").unwrap();
        assert_eq!(reset_at, expected);
    }

    #[test]
    fn should_format_observation_time_as_rfc3339_utc() {
        let observed_at = now_rfc3339();

        assert!(observed_at.ends_with('Z'));
        DateTime::parse_from_rfc3339(&observed_at).expect("timestamp parses as RFC3339");
    }

    #[test]
    fn should_parse_reset_after_with_humantime_duration_units() {
        let mut headers = HeaderMap::new();
        headers.insert("x-ratelimit-reset-requests", "1.5s".parse().unwrap());

        let windows = usage_windows_from_headers(&headers, "2026-05-27T11:24:18-07:00");

        assert_eq!(
            windows[0].reset_at.as_deref(),
            Some("2026-05-27T11:24:19.500-07:00")
        );
    }

    #[test]
    fn should_classify_rate_limit_pressure_from_remaining_budget() {
        let cases = [(25, "low"), (10, "medium"), (1, "high"), (0, "limited")];

        for (remaining, expected_pressure) in cases {
            let mut headers = HeaderMap::new();
            headers.insert("x-ratelimit-limit-requests", "100".parse().unwrap());
            headers.insert(
                "x-ratelimit-remaining-requests",
                remaining.to_string().parse().unwrap(),
            );

            let windows = usage_windows_from_headers(&headers, "2026-05-27T11:24:18Z");

            assert_eq!(windows[0].rate_limit_pressure, expected_pressure);
        }
    }

    #[test]
    fn should_extract_usage_limit_window_from_error_body() {
        let windows = usage_windows_from_error_body(
            StatusCode::TOO_MANY_REQUESTS,
            br#"{"error":{"code":"usage_limit_reached","resets_in_seconds":60}}"#,
            "2026-05-27T11:24:18Z",
        );

        assert_eq!(windows.len(), 1);
        assert_eq!(windows[0].window, "codex_usage_limit");
        assert_eq!(windows[0].source, "usage_limit_reached_error");
        assert_eq!(windows[0].remaining, Some(0));
        assert_eq!(windows[0].reset_after.as_deref(), Some("60s"));
        assert_eq!(windows[0].reset_at.as_deref(), Some("2026-05-27T11:25:18Z"));
        assert_eq!(windows[0].rate_limit_pressure, "limited");
        assert!(windows[0].limited);
    }

    #[test]
    fn should_normalize_usage_limit_reset_at_with_chrono() {
        let windows = usage_windows_from_error_body(
            StatusCode::TOO_MANY_REQUESTS,
            br#"{"error":{"code":"usage_limit_reached","resets_at":"2026-05-27T15:07:00+00:00"}}"#,
            "2026-05-27T11:24:18Z",
        );

        assert_eq!(windows[0].reset_at.as_deref(), Some("2026-05-27T15:07:00Z"));
    }

    #[test]
    fn should_omit_invalid_usage_limit_reset_seconds() {
        let windows = usage_windows_from_error_body(
            StatusCode::TOO_MANY_REQUESTS,
            br#"{"error":{"code":"usage_limit_reached","resets_in_seconds":-1}}"#,
            "2026-05-27T11:24:18Z",
        );

        assert_eq!(windows.len(), 1);
        assert!(windows[0].reset_after.is_none());
        assert!(windows[0].reset_at.is_none());
    }

    #[test]
    fn should_convert_limited_window_reset_time_to_routing_health() {
        let windows = vec![UsageWindow {
            window: "codex_usage_limit".to_string(),
            limit: None,
            remaining: Some(0),
            remaining_percent: None,
            rate_limit_pressure: "limited".to_string(),
            reset_after: Some("60s".to_string()),
            reset_at: Some("2026-05-27T11:25:18Z".to_string()),
            source: "usage_limit_reached_error".to_string(),
            observed_at: "2026-05-27T11:24:18Z".to_string(),
            limited: true,
        }];

        assert_eq!(
            usage_health_from_windows(Some(&windows)),
            AccountHealth::UsageLimited {
                reset_at_ms: 1_779_881_118_000
            }
        );
    }

    #[test]
    fn should_apply_bounded_cooldown_when_limited_reset_time_is_unparseable() {
        let windows = vec![UsageWindow {
            window: "codex_usage_limit".to_string(),
            limit: None,
            remaining: Some(0),
            remaining_percent: None,
            rate_limit_pressure: "limited".to_string(),
            reset_after: None,
            reset_at: Some("1969-12-31T23:59:59Z".to_string()),
            source: "usage_limit_reached_error".to_string(),
            observed_at: "2026-05-27T11:24:18Z".to_string(),
            limited: true,
        }];

        let observed_ms = unix_ms_from_rfc3339("2026-05-27T11:24:18Z").unwrap();
        assert_eq!(
            usage_health_from_windows(Some(&windows)),
            AccountHealth::UsageLimited {
                reset_at_ms: observed_ms + USAGE_LIMIT_DEFAULT_COOLDOWN_MS
            }
        );
        assert_eq!(cooldown_until(Some(&windows), None), None);
    }

    #[test]
    fn should_mark_usage_limited_accounts_in_snapshot() {
        let account = AccountConfig {
            id: "primary".to_string(),
            ..AccountConfig::default()
        };
        let reset_at = "2026-05-27T11:25:00Z".to_string();
        let usage_by_account = BTreeMap::from([(
            "primary".to_string(),
            vec![UsageWindow {
                window: "openai_requests".to_string(),
                limit: Some(500),
                remaining: Some(0),
                remaining_percent: Some(0.0),
                rate_limit_pressure: "limited".to_string(),
                reset_after: Some("42s".to_string()),
                reset_at: Some(reset_at.clone()),
                source: "openai_ratelimit_headers".to_string(),
                observed_at: "2026-05-27T11:24:18Z".to_string(),
                limited: true,
            }],
        )]);

        let snapshot = usage_snapshot(
            "tokenproxy-local",
            "2026-05-27T11:24:18Z",
            &[account],
            &usage_by_account,
            &BTreeMap::new(),
            "",
        );

        assert_eq!(snapshot.accounts[0].health, "usage_limited");
        assert_eq!(
            snapshot.accounts[0].cooldown_until.as_deref(),
            Some(reset_at.as_str())
        );
    }

    #[test]
    fn should_use_earliest_limited_reset_for_usage_cooldown() {
        let account = AccountConfig {
            id: "primary".to_string(),
            ..AccountConfig::default()
        };
        let usage_by_account = BTreeMap::from([(
            "primary".to_string(),
            vec![
                UsageWindow {
                    window: "openai_requests".to_string(),
                    limit: Some(500),
                    remaining: Some(0),
                    remaining_percent: Some(0.0),
                    rate_limit_pressure: "limited".to_string(),
                    reset_after: Some("120s".to_string()),
                    reset_at: Some("2026-05-27T11:26:18Z".to_string()),
                    source: "openai_ratelimit_headers".to_string(),
                    observed_at: "2026-05-27T11:24:18Z".to_string(),
                    limited: true,
                },
                UsageWindow {
                    window: "openai_tokens".to_string(),
                    limit: Some(1000),
                    remaining: Some(0),
                    remaining_percent: Some(0.0),
                    rate_limit_pressure: "limited".to_string(),
                    reset_after: Some("60s".to_string()),
                    reset_at: Some("2026-05-27T11:25:18Z".to_string()),
                    source: "openai_ratelimit_headers".to_string(),
                    observed_at: "2026-05-27T11:24:18Z".to_string(),
                    limited: true,
                },
            ],
        )]);

        let snapshot = usage_snapshot(
            "tokenproxy-local",
            "2026-05-27T11:24:18Z",
            &[account],
            &usage_by_account,
            &BTreeMap::new(),
            "",
        );

        assert_eq!(snapshot.accounts[0].health, "usage_limited");
        assert_eq!(
            snapshot.accounts[0].cooldown_until.as_deref(),
            Some("2026-05-27T11:25:18Z")
        );
    }

    #[test]
    fn should_include_runtime_account_health_when_usage_is_not_limited() {
        let account = AccountConfig {
            id: "primary".to_string(),
            ..AccountConfig::default()
        };
        let health_by_account =
            BTreeMap::from([("primary".to_string(), AccountHealth::AuthFailed)]);

        let snapshot = usage_snapshot(
            "tokenproxy-local",
            "2026-05-27T11:24:18Z",
            &[account],
            &BTreeMap::new(),
            &health_by_account,
            "",
        );

        assert_eq!(snapshot.accounts[0].health, "auth_failed");
    }

    #[test]
    fn should_include_runtime_throttle_deadline_as_cooldown() {
        let account = AccountConfig {
            id: "primary".to_string(),
            ..AccountConfig::default()
        };
        let retry_at = DateTime::parse_from_rfc3339("2026-05-27T11:25:18Z")
            .unwrap()
            .timestamp_millis() as u64;
        let health_by_account = BTreeMap::from([(
            "primary".to_string(),
            AccountHealth::Throttled {
                next_retry_at_ms: retry_at,
            },
        )]);

        let snapshot = usage_snapshot(
            "tokenproxy-local",
            "2026-05-27T11:24:18Z",
            &[account],
            &BTreeMap::new(),
            &health_by_account,
            "",
        );

        assert_eq!(snapshot.accounts[0].health, "throttled");
        assert_eq!(
            snapshot.accounts[0].cooldown_until.as_deref(),
            Some("2026-05-27T11:25:18Z")
        );
    }

    #[test]
    fn should_include_disabled_configured_accounts_in_snapshot() {
        let mut disabled = AccountConfig {
            id: "disabled".to_string(),
            display_name: Some("paused account".to_string()),
            ..AccountConfig::default()
        };
        disabled.enabled = false;

        let snapshot = usage_snapshot(
            "tokenproxy-local",
            "2026-05-27T11:24:18Z",
            &[
                AccountConfig {
                    id: "primary".to_string(),
                    ..AccountConfig::default()
                },
                disabled,
            ],
            &BTreeMap::new(),
            &BTreeMap::new(),
            "hash-key",
        );

        assert_eq!(snapshot.accounts.len(), 2);
        assert_eq!(snapshot.accounts[0].health, "open");
        assert_eq!(
            snapshot.accounts[1].display_name.as_deref(),
            Some("paused account")
        );
        assert_eq!(snapshot.accounts[1].health, "disabled");
        assert!(snapshot.accounts[1].usage.is_empty());
        assert!(snapshot.accounts[1].cooldown_until.is_none());
    }
}
