use axum::http::header::{AUTHORIZATION, HOST, HeaderName, HeaderValue, USER_AGENT};
use axum::http::{HeaderMap, StatusCode};

use crate::error::{ErrorCode, TokenproxyError};

const DEFAULT_ANTHROPIC_VERSION: &str = "2023-06-01";
const DEFAULT_CODEX_USER_AGENT: &str = "codex-cli";
const DEFAULT_CODEX_ORIGINATOR: &str = "codex_cli_rs";
const DEFAULT_CODEX_OPENAI_BETA: &str = "responses_websockets=2026-02-06";

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
    OpenAiBearer,
    ChatGptBearer,
    AnthropicApiKey,
    ForwardInboundBearer,
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
        if should_forward_inbound_header(name.as_str(), auth, allow_openai_headers) {
            output.append(name.clone(), value.clone());
        }
    }

    output.insert(HOST, header_value(upstream_host, "upstream host")?);
    match auth {
        UpstreamAuth::OpenAiBearer | UpstreamAuth::ChatGptBearer => {
            let mut authorization =
                header_value(&format!("Bearer {upstream_token}"), "authorization")?;
            authorization.set_sensitive(true);
            output.insert(AUTHORIZATION, authorization);
            if matches!(auth, UpstreamAuth::ChatGptBearer) {
                apply_chatgpt_codex_default_headers(&mut output, tokenproxy_request_id);
                // ChatGPT Codex auth pairs the OAuth access-token bearer with the
                // workspace id header; see codex-rs model-provider BearerAuthProvider.
                if let Some(account_id) = chatgpt_account_id {
                    output.insert(
                        HeaderName::from_static("chatgpt-account-id"),
                        header_value(account_id, "chatgpt account id")?,
                    );
                }
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
        UpstreamAuth::ForwardInboundBearer => {
            let Some(mut authorization) = output.get(AUTHORIZATION).cloned() else {
                return Err(TokenproxyError::new(
                    StatusCode::UNAUTHORIZED,
                    ErrorCode::Unauthorized,
                    "mainroom_peer forwarding requires a downstream Authorization bearer",
                ));
            };
            authorization.set_sensitive(true);
            output.insert(AUTHORIZATION, authorization);
        }
    }
    output.insert(
        HeaderName::from_static("x-tokenproxy-request-id"),
        header_value(tokenproxy_request_id, "tokenproxy request id")?,
    );

    Ok(output)
}

fn should_forward_inbound_header(
    lower: &str,
    auth: UpstreamAuth,
    allow_openai_headers: bool,
) -> bool {
    // Keep upstream requests synthesized by provider kind. CLIProxyAPI's Codex
    // executors build provider-native requests from scratch and only copy known
    // Codex headers; its generic scrubber also removes proxy, browser, referer,
    // client-identity, and encoding fingerprints. Local capture experiments for
    // this change sent those noisy headers through each account kind and asserted
    // that only this allow-list reached the fake upstream.
    let is_payload_header = matches!(lower, "accept" | "content-type");
    match auth {
        UpstreamAuth::ChatGptBearer => is_payload_header || is_codex_header(lower),
        UpstreamAuth::OpenAiBearer => {
            is_payload_header
                || (allow_openai_headers
                    && matches!(lower, "openai-organization" | "openai-project"))
        }
        UpstreamAuth::AnthropicApiKey => {
            is_payload_header || matches!(lower, "anthropic-version" | "anthropic-beta")
        }
        UpstreamAuth::ForwardInboundBearer => lower == AUTHORIZATION.as_str() || is_payload_header,
    }
}

fn is_codex_header(lower: &str) -> bool {
    // Codex client sources name the x-codex turn/session headers and
    // ChatGPT-account auth path that need to survive proxying; everything else
    // from the edge request is treated as downstream-only context.
    matches!(
        lower,
        "user-agent"
            | "originator"
            | "openai-beta"
            | "x-client-request-id"
            | "x-responsesapi-include-timing-metrics"
            | "version"
            | "session-id"
            | "session_id"
            | "thread-id"
            | "conversation_id"
    ) || lower.starts_with("x-codex-")
}

fn apply_chatgpt_codex_default_headers(headers: &mut HeaderMap, tokenproxy_request_id: &str) {
    headers
        .entry(USER_AGENT)
        .or_insert(HeaderValue::from_static(DEFAULT_CODEX_USER_AGENT));
    headers
        .entry(HeaderName::from_static("originator"))
        .or_insert(HeaderValue::from_static(DEFAULT_CODEX_ORIGINATOR));
    headers
        .entry(HeaderName::from_static("openai-beta"))
        .or_insert(HeaderValue::from_static(DEFAULT_CODEX_OPENAI_BETA));
    if let Ok(request_id) = HeaderValue::from_str(tokenproxy_request_id) {
        headers
            .entry(HeaderName::from_static("x-client-request-id"))
            .or_insert(request_id);
    }
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
            UpstreamAuth::ChatGptBearer,
            false,
        )
        .unwrap();

        assert_eq!(headers["authorization"], "Bearer upstream");
        assert!(headers["authorization"].is_sensitive());
        assert_eq!(headers["chatgpt-account-id"], "acct_123");
        assert_eq!(headers["user-agent"], "codex-cli");
        assert_eq!(headers["originator"], "codex_cli_rs");
        assert_eq!(headers["openai-beta"], "responses_websockets=2026-02-06");
        assert_eq!(headers["x-client-request-id"], "req_1");
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
            UpstreamAuth::OpenAiBearer,
            true,
        )
        .unwrap();

        assert_eq!(headers["openai-organization"], "org_client");
        assert_eq!(headers["openai-project"], "proj_client");
        assert!(!headers.contains_key("openai-unknown"));
        assert!(!headers.contains_key("user-agent"));
    }

    #[test]
    fn should_forward_only_provider_native_headers() {
        let mut inbound = HeaderMap::new();
        // This fixture mirrors the local live-capture experiment: one request
        // carries valid Codex headers plus edge/proxy/browser fingerprint noise.
        inbound.insert("content-type", "application/json".parse().unwrap());
        inbound.insert("accept", "text/event-stream".parse().unwrap());
        inbound.insert("user-agent", "codex_exec/0.141.0 test".parse().unwrap());
        inbound.insert("originator", "codex_exec".parse().unwrap());
        inbound.insert(
            "openai-beta",
            "responses_websockets=2026-02-06".parse().unwrap(),
        );
        inbound.insert("x-client-request-id", "client-req".parse().unwrap());
        inbound.insert("x-codex-window-id", "thread-1:0".parse().unwrap());
        inbound.insert("session-id", "session-1".parse().unwrap());
        inbound.insert("thread-id", "thread-1".parse().unwrap());
        inbound.insert("version", "0.141.0".parse().unwrap());
        inbound.insert("cf-connecting-ip", "203.0.113.1".parse().unwrap());
        inbound.insert("cf-ipcountry", "US".parse().unwrap());
        inbound.insert("cf-ray", "ray-ewr".parse().unwrap());
        inbound.insert("cf-visitor", r#"{"scheme":"https"}"#.parse().unwrap());
        inbound.insert("cdn-loop", "cloudflare".parse().unwrap());
        inbound.insert("forwarded", "for=203.0.113.1".parse().unwrap());
        inbound.insert("via", "1.1 proxy".parse().unwrap());
        inbound.insert("true-client-ip", "203.0.113.1".parse().unwrap());
        inbound.insert("x-real-ip", "203.0.113.1".parse().unwrap());
        inbound.insert("x-client-ip", "203.0.113.1".parse().unwrap());
        inbound.insert("x-cluster-client-ip", "203.0.113.1".parse().unwrap());
        inbound.insert("x-forwarded-host", "victor.mainroom.sh".parse().unwrap());
        inbound.insert("x-forwarded-proto", "https".parse().unwrap());
        inbound.insert("x-mainroom-host", "victor.mainroom.sh".parse().unwrap());
        inbound.insert("x-mainroom-cf-colo", "EWR".parse().unwrap());
        inbound.insert("x-vercel-id", "iad1::test".parse().unwrap());
        inbound.insert("fastly-client-ip", "203.0.113.1".parse().unwrap());
        inbound.insert("x-amzn-trace-id", "Root=1-test".parse().unwrap());
        inbound.insert("sec-ch-ua", r#""Chromium";v="130""#.parse().unwrap());
        inbound.insert("sec-fetch-site", "same-origin".parse().unwrap());
        inbound.insert("priority", "u=1, i".parse().unwrap());
        inbound.insert("referer", "https://victor.mainroom.sh/".parse().unwrap());
        inbound.insert("x-stainless-lang", "js".parse().unwrap());
        inbound.insert("accept-encoding", "gzip, br, zstd".parse().unwrap());

        let headers = build_upstream_headers(
            &inbound,
            "chatgpt.com",
            "upstream",
            Some("acct_123"),
            "req_1",
            UpstreamAuth::ChatGptBearer,
            false,
        )
        .unwrap();

        assert_eq!(headers["content-type"], "application/json");
        assert_eq!(headers["accept"], "text/event-stream");
        assert_eq!(headers["user-agent"], "codex_exec/0.141.0 test");
        assert_eq!(headers["originator"], "codex_exec");
        assert_eq!(headers["openai-beta"], "responses_websockets=2026-02-06");
        assert_eq!(headers["x-client-request-id"], "client-req");
        assert_eq!(headers["x-codex-window-id"], "thread-1:0");
        assert_eq!(headers["session-id"], "session-1");
        assert_eq!(headers["thread-id"], "thread-1");
        assert_eq!(headers["version"], "0.141.0");
        assert_eq!(headers["authorization"], "Bearer upstream");
        assert_eq!(headers["host"], "chatgpt.com");
        assert_eq!(headers["chatgpt-account-id"], "acct_123");
        for name in [
            "cf-connecting-ip",
            "cf-ipcountry",
            "cf-ray",
            "cf-visitor",
            "cdn-loop",
            "forwarded",
            "via",
            "true-client-ip",
            "x-real-ip",
            "x-client-ip",
            "x-cluster-client-ip",
            "x-forwarded-host",
            "x-forwarded-proto",
            "x-mainroom-host",
            "x-mainroom-cf-colo",
            "x-vercel-id",
            "fastly-client-ip",
            "x-amzn-trace-id",
            "sec-ch-ua",
            "sec-fetch-site",
            "priority",
            "referer",
            "x-stainless-lang",
            "accept-encoding",
        ] {
            assert!(!headers.contains_key(name), "{name} should be stripped");
        }
    }

    #[test]
    fn should_preserve_codex_headers_for_chatgpt_bearer() {
        let mut inbound = HeaderMap::new();
        inbound.insert("user-agent", "codex_exec/0.141.0 test".parse().unwrap());
        inbound.insert("originator", "codex_exec".parse().unwrap());
        inbound.insert(
            "openai-beta",
            "responses_websockets=2026-02-06".parse().unwrap(),
        );
        inbound.insert("x-client-request-id", "client-req".parse().unwrap());
        inbound.insert("openai-organization", "org_client".parse().unwrap());
        inbound.insert("openai-project", "proj_client".parse().unwrap());

        let headers = build_upstream_headers(
            &inbound,
            "chatgpt.com",
            "upstream",
            Some("acct_123"),
            "req_1",
            UpstreamAuth::ChatGptBearer,
            false,
        )
        .unwrap();

        assert_eq!(headers["user-agent"], "codex_exec/0.141.0 test");
        assert_eq!(headers["originator"], "codex_exec");
        assert_eq!(headers["openai-beta"], "responses_websockets=2026-02-06");
        assert_eq!(headers["x-client-request-id"], "client-req");
        assert_eq!(headers["chatgpt-account-id"], "acct_123");
        assert!(!headers.contains_key("openai-organization"));
        assert!(!headers.contains_key("openai-project"));
    }

    #[test]
    fn should_add_codex_defaults_for_non_codex_chatgpt_clients() {
        let inbound = HeaderMap::new();

        let headers = build_upstream_headers(
            &inbound,
            "chatgpt.com",
            "upstream",
            Some("acct_123"),
            "req_non_codex",
            UpstreamAuth::ChatGptBearer,
            false,
        )
        .unwrap();

        assert_eq!(headers["user-agent"], DEFAULT_CODEX_USER_AGENT);
        assert_eq!(headers["originator"], DEFAULT_CODEX_ORIGINATOR);
        assert_eq!(headers["openai-beta"], DEFAULT_CODEX_OPENAI_BETA);
        assert_eq!(headers["x-client-request-id"], "req_non_codex");
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
    fn should_forward_inbound_bearer_for_mainroom_peer() {
        let mut inbound = HeaderMap::new();
        inbound.insert("authorization", "Bearer caller".parse().unwrap());
        inbound.insert("x-api-key", "client-key".parse().unwrap());
        inbound.insert("host", "tokenproxy.local".parse().unwrap());
        inbound.insert("content-type", "application/json".parse().unwrap());

        let headers = build_upstream_headers(
            &inbound,
            "peer.mainroom.sh",
            "",
            None,
            "req_1",
            UpstreamAuth::ForwardInboundBearer,
            false,
        )
        .unwrap();

        assert_eq!(headers["authorization"], "Bearer caller");
        assert!(headers["authorization"].is_sensitive());
        assert_eq!(headers["host"], "peer.mainroom.sh");
        assert_eq!(headers["x-tokenproxy-request-id"], "req_1");
        assert!(!headers.contains_key("x-api-key"));
    }

    #[test]
    fn should_reject_mainroom_peer_forwarding_without_inbound_bearer() {
        let error = build_upstream_headers(
            &HeaderMap::new(),
            "peer.mainroom.sh",
            "",
            None,
            "req_1",
            UpstreamAuth::ForwardInboundBearer,
            false,
        )
        .unwrap_err();

        assert_eq!(error.status, StatusCode::UNAUTHORIZED);
        assert!(error.message.contains("mainroom_peer"));
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
