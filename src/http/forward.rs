use axum::http::header::{AUTHORIZATION, CONTENT_LENGTH, HOST, HeaderName, HeaderValue};
use axum::http::{HeaderMap, StatusCode};

use crate::error::{ErrorCode, TokenproxyError};

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

pub fn build_upstream_headers(
    inbound: &HeaderMap,
    upstream_host: &str,
    bearer_token: &str,
    tokenproxy_request_id: &str,
    allow_openai_headers: bool,
) -> Result<HeaderMap, TokenproxyError> {
    let mut output = HeaderMap::new();

    for (name, value) in inbound {
        if should_strip_header(name, allow_openai_headers) {
            continue;
        }
        output.append(name.clone(), value.clone());
    }

    output.insert(HOST, header_value(upstream_host, "upstream host")?);
    output.insert(
        AUTHORIZATION,
        sensitive_bearer_header_value(bearer_token, "authorization")?,
    );
    output.insert(
        HeaderName::from_static("x-tokenproxy-request-id"),
        header_value(tokenproxy_request_id, "tokenproxy request id")?,
    );

    Ok(output)
}

pub fn filter_downstream_response_headers(upstream: &HeaderMap) -> HeaderMap {
    let mut output = HeaderMap::new();
    for (name, value) in upstream {
        let lower = name.as_str().to_ascii_lowercase();
        if lower == "set-cookie" || HOP_BY_HOP.contains(&lower.as_str()) {
            continue;
        }
        if lower == "content-type"
            || lower == "openai-request-id"
            || lower == "x-request-id"
            || lower == "cf-ray"
            || lower == "retry-after"
            || lower.starts_with("x-ratelimit-")
        {
            output.append(name.clone(), value.clone());
        }
    }
    output
}

fn should_strip_header(name: &HeaderName, allow_openai_headers: bool) -> bool {
    let lower = name.as_str().to_ascii_lowercase();
    lower == AUTHORIZATION.as_str()
        || lower == HOST.as_str()
        || lower == CONTENT_LENGTH.as_str()
        || HOP_BY_HOP.contains(&lower.as_str())
        || (lower.starts_with("openai-")
            && !should_forward_openai_header(&lower, allow_openai_headers))
}

fn should_forward_openai_header(lower: &str, allow_openai_headers: bool) -> bool {
    allow_openai_headers && matches!(lower, "openai-organization" | "openai-project")
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

fn sensitive_bearer_header_value(
    bearer_token: &str,
    label: &str,
) -> Result<HeaderValue, TokenproxyError> {
    let mut value = header_value(&format!("Bearer {bearer_token}"), label)?;
    value.set_sensitive(true);
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_replace_auth_host_strip_hop_by_hop_and_add_request_id() {
        let mut inbound = HeaderMap::new();
        inbound.insert("authorization", "Bearer client".parse().unwrap());
        inbound.insert("host", "tokenproxy.local".parse().unwrap());
        inbound.insert("connection", "keep-alive".parse().unwrap());
        inbound.insert("content-type", "application/json".parse().unwrap());
        inbound.insert("openai-organization", "org_client".parse().unwrap());

        let headers =
            build_upstream_headers(&inbound, "api.openai.com", "upstream", "req_1", false).unwrap();

        assert_eq!(headers["authorization"], "Bearer upstream");
        assert!(headers["authorization"].is_sensitive());
        assert_eq!(headers["host"], "api.openai.com");
        assert_eq!(headers["x-tokenproxy-request-id"], "req_1");
        assert!(!headers.contains_key("connection"));
        assert!(!headers.contains_key("openai-organization"));
    }

    #[test]
    fn should_forward_openai_headers_only_when_config_allows_them() {
        let mut inbound = HeaderMap::new();
        inbound.insert("openai-organization", "org_client".parse().unwrap());
        inbound.insert("openai-project", "proj_client".parse().unwrap());
        inbound.insert("openai-unknown", "should-not-forward".parse().unwrap());

        let headers =
            build_upstream_headers(&inbound, "api.openai.com", "upstream", "req_1", true).unwrap();

        assert_eq!(headers["openai-organization"], "org_client");
        assert_eq!(headers["openai-project"], "proj_client");
        assert!(!headers.contains_key("openai-unknown"));
    }

    #[test]
    fn should_forward_only_safe_downstream_response_headers() {
        let mut upstream = HeaderMap::new();
        upstream.insert("content-type", "text/event-stream".parse().unwrap());
        upstream.insert("openai-request-id", "req_upstream".parse().unwrap());
        upstream.insert("x-ratelimit-reset-requests", "120ms".parse().unwrap());
        upstream.insert("retry-after", "2".parse().unwrap());
        upstream.insert("set-cookie", "session=secret".parse().unwrap());
        upstream.insert("connection", "keep-alive".parse().unwrap());
        upstream.insert("authorization", "Bearer upstream".parse().unwrap());

        let headers = filter_downstream_response_headers(&upstream);

        assert_eq!(headers["content-type"], "text/event-stream");
        assert_eq!(headers["openai-request-id"], "req_upstream");
        assert_eq!(headers["x-ratelimit-reset-requests"], "120ms");
        assert_eq!(headers["retry-after"], "2");
        assert!(!headers.contains_key("set-cookie"));
        assert!(!headers.contains_key("connection"));
        assert!(!headers.contains_key("authorization"));
    }
}
