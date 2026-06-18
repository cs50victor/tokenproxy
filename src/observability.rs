use http::HeaderMap;
use serde_json::Value;
use sha2::{Digest, Sha256};

pub fn request_body_dump_record(
    request_id: &str,
    method: &str,
    path: &str,
    headers: &HeaderMap,
    body: &[u8],
    redact_json_pointers: &[String],
) -> Value {
    let mut record = serde_json::json!({
        "type": "request_body_dump",
        "request_id": request_id,
        "method": method,
        "path": path,
        "headers": redacted_headers(headers),
        "body_len": body.len(),
        "body_sha256": sha256_hex(body),
    });

    if let Ok(mut value) = serde_json::from_slice::<Value>(body) {
        redact_json_value(&mut value, redact_json_pointers);
        record["body_json"] = value;
    }

    record
}

pub fn compact_body_hash_record(
    request_id: &str,
    method: &str,
    path: &str,
    response_status: u16,
    request_body: &[u8],
    response_body: &[u8],
) -> Value {
    serde_json::json!({
        "type": "compact_body_hash",
        "request_id": request_id,
        "method": method,
        "path": path,
        "request_body_len": request_body.len(),
        "request_body_sha256": sha256_hex(request_body),
        "response_status": response_status,
        "response_body_len": response_body.len(),
        "response_body_sha256": sha256_hex(response_body),
    })
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let digest = Sha256::digest(bytes);
    let mut output = String::with_capacity(digest.len() * 2);
    for byte in digest {
        output.push(HEX[usize::from(byte >> 4)] as char);
        output.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    output
}

fn redact_json_value(value: &mut Value, redact_json_pointers: &[String]) {
    for pointer in redact_json_pointers {
        if let Some(target) = value.pointer_mut(pointer) {
            *target = Value::String("[redacted]".to_string());
        }
    }
}

fn redacted_headers(headers: &HeaderMap) -> Value {
    let mut redacted = serde_json::Map::new();
    for (name, value) in headers {
        let value = if value.is_sensitive() || is_sensitive_header(name.as_str()) {
            "[redacted]".to_string()
        } else {
            value.to_str().unwrap_or("[non-utf8]").to_string()
        };
        redacted.insert(name.as_str().to_ascii_lowercase(), Value::String(value));
    }
    Value::Object(redacted)
}

fn is_sensitive_header(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "authorization" | "cookie" | "proxy-authorization" | "set-cookie" | "x-api-key"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_hash_request_body_with_sha256() {
        assert_eq!(
            sha256_hex(b"hello world"),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[test]
    fn should_build_redacted_request_body_dump_record() {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", "Bearer client-secret".parse().unwrap());
        headers.insert("cookie", "session=secret".parse().unwrap());
        headers.insert("set-cookie", "session=upstream-secret".parse().unwrap());
        headers.insert("content-type", "application/json".parse().unwrap());

        let record = request_body_dump_record(
            "req_1",
            "POST",
            "/v1/responses",
            &headers,
            br#"{"model":"gpt-5.5","api_key":"secret","refresh_token":"refresh-secret","nested":{"token":"also-secret"}}"#,
            &[
                "/api_key".to_string(),
                "/refresh_token".to_string(),
                "/nested/token".to_string(),
            ],
        );

        assert_eq!(record["type"], "request_body_dump");
        assert_eq!(record["request_id"], "req_1");
        assert_eq!(record["headers"]["authorization"], "[redacted]");
        assert_eq!(record["headers"]["cookie"], "[redacted]");
        assert_eq!(record["headers"]["set-cookie"], "[redacted]");
        assert_eq!(record["headers"]["content-type"], "application/json");
        assert_eq!(record["body_json"]["api_key"], "[redacted]");
        assert_eq!(record["body_json"]["refresh_token"], "[redacted]");
        assert_eq!(record["body_json"]["nested"]["token"], "[redacted]");
        assert_ne!(record["body_sha256"], "");
        assert!(!record.to_string().contains("also-secret"));
        assert!(!record.to_string().contains("refresh-secret"));
        assert!(!record.to_string().contains("client-secret"));
        assert!(!record.to_string().contains("session=secret"));
        assert!(!record.to_string().contains("upstream-secret"));
    }

    #[test]
    fn should_redact_sensitive_header_values_even_when_name_is_safe() {
        use http::HeaderValue;

        let mut headers = HeaderMap::new();
        let mut token: HeaderValue = "secret-value".parse().unwrap();
        token.set_sensitive(true);
        headers.insert("x-trace-token", token);

        let record =
            request_body_dump_record("req_1", "POST", "/v1/responses", &headers, b"{}", &[]);

        assert_eq!(record["headers"]["x-trace-token"], "[redacted]");
        assert!(!record.to_string().contains("secret-value"));
    }

    #[test]
    fn should_build_compact_body_hash_record_without_raw_bodies() {
        let record = compact_body_hash_record(
            "req_1",
            "POST",
            "/v1/responses/compact",
            200,
            br#"{"input":"secret request"}"#,
            br#"{"output":"secret response"}"#,
        );

        assert_eq!(record["type"], "compact_body_hash");
        assert_eq!(record["request_body_len"], 26);
        assert_eq!(record["response_body_len"], 28);
        assert_ne!(record["request_body_sha256"], "");
        assert_ne!(record["response_body_sha256"], "");
        assert!(!record.to_string().contains("secret request"));
        assert!(!record.to_string().contains("secret response"));
    }

    #[test]
    fn should_not_write_raw_non_json_body_into_dump_record() {
        let record = request_body_dump_record(
            "req_1",
            "POST",
            "/v1/responses",
            &HeaderMap::new(),
            b"secret",
            &[],
        );

        assert!(record.get("body_json").is_none());
        assert!(!record.to_string().contains("secret"));
    }
}
