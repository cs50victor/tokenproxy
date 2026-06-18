use axum::http::header::{AUTHORIZATION, CONTENT_LENGTH, HOST, HeaderName, HeaderValue};
use axum::http::{HeaderMap, StatusCode};

use crate::error::{ErrorCode, TokenproxyError};

const DEFAULT_ANTHROPIC_VERSION: &str = "2023-06-01";

const HOP_BY_HOP: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamAuth {
    Bearer,
    AnthropicApiKey,
}

pub fn build_upstream_headers(
    inbound: &HeaderMap,
    upstream_host: &str,
    upstream_token: &str,
    chatgpt_account_id: Option<&str>,
    tokenproxy_request_id: &str,
    auth: UpstreamAuth,
    allow_openai_headers: bool,
) -> Result<HeaderMap, TokenproxyError> {
    let mut output = HeaderMap::new();

    for (name, value) in inbound {
        let lower = name.as_str();
        let forward_openai_header =
            allow_openai_headers && matches!(lower, "openai-organization" | "openai-project");
        if lower == AUTHORIZATION.as_str()
            || lower == HOST.as_str()
            || lower == CONTENT_LENGTH.as_str()
            || HOP_BY_HOP.contains(&lower)
            || lower == "x-api-key"
            || lower == "api-key"
            // reqwest is built without decompression features and tokenproxy strips
            // Content-Encoding from responses, so force identity upstream encoding.
            || lower == "accept-encoding"
            || (lower.starts_with("openai-") && !forward_openai_header)
        {
            continue;
        }
        output.append(name.clone(), value.clone());
    }

    output.insert(HOST, header_value(upstream_host, "upstream host")?);
    match auth {
        UpstreamAuth::Bearer => {
            let mut authorization =
                header_value(&format!("Bearer {upstream_token}"), "authorization")?;
            authorization.set_sensitive(true);
            output.insert(AUTHORIZATION, authorization);
            // ChatGPT Codex auth pairs the OAuth access-token bearer with the
            // workspace id header; see codex-rs model-provider BearerAuthProvider.
            if let Some(account_id) = chatgpt_account_id {
                output.insert(
                    HeaderName::from_static("chatgpt-account-id"),
                    header_value(account_id, "chatgpt account id")?,
                );
            }
        }
        UpstreamAuth::AnthropicApiKey => {
            let mut api_key = header_value(upstream_token, "x-api-key")?;
            api_key.set_sensitive(true);
            output.insert(HeaderName::from_static("x-api-key"), api_key);
            output
                .entry(HeaderName::from_static("anthropic-version"))
                .or_insert(header_value(
                    DEFAULT_ANTHROPIC_VERSION,
                    "anthropic-version",
                )?);
            output
                .entry(HeaderName::from_static("content-type"))
                .or_insert(HeaderValue::from_static("application/json"));
        }
    }
    output.insert(
        HeaderName::from_static("x-tokenproxy-request-id"),
        header_value(tokenproxy_request_id, "tokenproxy request id")?,
    );

    Ok(output)
}

pub fn filter_downstream_response_headers(upstream: &HeaderMap) -> HeaderMap {
    let mut output = HeaderMap::new();
    for (name, value) in upstream {
        // http::HeaderName is lowercase by construction.
        let lower = name.as_str();
        if lower == "set-cookie" || HOP_BY_HOP.contains(&lower) {
            continue;
        }
        if lower == "content-type"
            || lower == "openai-request-id"
            || lower == "request-id"
            || lower == "x-request-id"
            || lower == "anthropic-organization-id"
            || lower == "cf-ray"
            || lower == "retry-after"
            || lower.starts_with("x-ratelimit-")
            || lower.starts_with("anthropic-ratelimit-")
        {
            output.append(name.clone(), value.clone());
        }
    }
    output
}

fn header_value(value: &str, label: &str) -> Result<HeaderValue, TokenproxyError> {
    HeaderValue::from_str(value).map_err(|error| {
        TokenproxyError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidJson,
            format!("invalid {label} header value: {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_replace_auth_host_strip_hop_by_hop_and_add_request_id() {
        let mut inbound = HeaderMap::new();
        inbound.insert("authorization", "Bearer client".parse().unwrap());
        inbound.insert("x-api-key", "client-key".parse().unwrap());
        inbound.insert("host", "tokenproxy.local".parse().unwrap());
        inbound.insert("connection", "keep-alive".parse().unwrap());
        inbound.insert("content-type", "application/json".parse().unwrap());
        inbound.insert("openai-organization", "org_client".parse().unwrap());

        let headers = build_upstream_headers(
            &inbound,
            "api.openai.com",
            "upstream",
            Some("acct_123"),
            "req_1",
            UpstreamAuth::Bearer,
            false,
        )
        .unwrap();

        assert_eq!(headers["authorization"], "Bearer upstream");
        assert!(headers["authorization"].is_sensitive());
        assert_eq!(headers["chatgpt-account-id"], "acct_123");
        assert_eq!(headers["host"], "api.openai.com");
        assert_eq!(headers["x-tokenproxy-request-id"], "req_1");
        assert!(!headers.contains_key("connection"));
        assert!(!headers.contains_key("x-api-key"));
        assert!(!headers.contains_key("openai-organization"));
    }

    #[test]
    fn should_forward_openai_headers_only_when_config_allows_them() {
        let mut inbound = HeaderMap::new();
        inbound.insert("openai-organization", "org_client".parse().unwrap());
        inbound.insert("openai-project", "proj_client".parse().unwrap());
        inbound.insert("openai-unknown", "should-not-forward".parse().unwrap());

        let headers = build_upstream_headers(
            &inbound,
            "api.openai.com",
            "upstream",
            None,
            "req_1",
            UpstreamAuth::Bearer,
            true,
        )
        .unwrap();

        assert_eq!(headers["openai-organization"], "org_client");
        assert_eq!(headers["openai-project"], "proj_client");
        assert!(!headers.contains_key("openai-unknown"));
    }

    #[test]
    fn should_replace_anthropic_key_and_default_version_header() {
        let mut inbound = HeaderMap::new();
        inbound.insert("authorization", "Bearer client".parse().unwrap());
        inbound.insert("x-api-key", "client-key".parse().unwrap());
        inbound.insert("host", "tokenproxy.local".parse().unwrap());
        inbound.insert("content-type", "application/json".parse().unwrap());

        let headers = build_upstream_headers(
            &inbound,
            "api.anthropic.com",
            "upstream-anthropic",
            None,
            "req_1",
            UpstreamAuth::AnthropicApiKey,
            false,
        )
        .unwrap();

        assert_eq!(headers["x-api-key"], "upstream-anthropic");
        assert!(headers["x-api-key"].is_sensitive());
        assert_eq!(headers["anthropic-version"], DEFAULT_ANTHROPIC_VERSION);
        assert_eq!(headers["content-type"], "application/json");
        assert_eq!(headers["host"], "api.anthropic.com");
        assert!(!headers.contains_key("authorization"));
    }

    #[test]
    fn should_preserve_client_anthropic_version_header() {
        let mut inbound = HeaderMap::new();
        inbound.insert("anthropic-version", "2024-01-01".parse().unwrap());

        let headers = build_upstream_headers(
            &inbound,
            "api.anthropic.com",
            "upstream-anthropic",
            None,
            "req_1",
            UpstreamAuth::AnthropicApiKey,
            false,
        )
        .unwrap();

        assert_eq!(headers["anthropic-version"], "2024-01-01");
    }

    #[test]
    fn should_forward_only_safe_downstream_response_headers() {
        let mut upstream = HeaderMap::new();
        upstream.insert("content-type", "text/event-stream".parse().unwrap());
        upstream.insert("openai-request-id", "req_upstream".parse().unwrap());
        upstream.insert("request-id", "req_anthropic".parse().unwrap());
        upstream.insert(
            "anthropic-organization-id",
            "org_anthropic".parse().unwrap(),
        );
        upstream.insert("x-ratelimit-reset-requests", "120ms".parse().unwrap());
        upstream.insert(
            "anthropic-ratelimit-requests-remaining",
            "99".parse().unwrap(),
        );
        upstream.insert("retry-after", "2".parse().unwrap());
        upstream.insert("set-cookie", "session=secret".parse().unwrap());
        upstream.insert("connection", "keep-alive".parse().unwrap());
        upstream.insert("authorization", "Bearer upstream".parse().unwrap());

        let headers = filter_downstream_response_headers(&upstream);

        assert_eq!(headers["content-type"], "text/event-stream");
        assert_eq!(headers["openai-request-id"], "req_upstream");
        assert_eq!(headers["request-id"], "req_anthropic");
        assert_eq!(headers["anthropic-organization-id"], "org_anthropic");
        assert_eq!(headers["x-ratelimit-reset-requests"], "120ms");
        assert_eq!(headers["anthropic-ratelimit-requests-remaining"], "99");
        assert_eq!(headers["retry-after"], "2");
        assert!(!headers.contains_key("set-cookie"));
        assert!(!headers.contains_key("connection"));
        assert!(!headers.contains_key("authorization"));
    }
}
