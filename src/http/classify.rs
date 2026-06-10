use axum::http::StatusCode;
use bytes::Bytes;
use serde_json::Value;

use crate::error::{ErrorCode, TokenproxyError};
use crate::model::model_family_label;
use crate::routing::select::normalize_service_tier;
use crate::routing::{Endpoint, RouteRequest, Transport};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassifiedRequest {
    pub route_request: RouteRequest,
    pub request_shape: Option<RequestShape>,
    pub body: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestShape {
    pub service_tier: String,
    pub reasoning_effort: String,
    pub verbosity: String,
    pub store: String,
}

// The router binds this classifier only to POST on the four generation routes;
// every other method or path is answered by a dedicated axum handler.
pub fn classify_request(path: &str, body: Bytes) -> Result<ClassifiedRequest, TokenproxyError> {
    match path {
        "/v1/chat/completions" => {
            let value = parse_json(&body)?;
            let stream = bool_field(&value, "stream").unwrap_or(false);
            let model = required_string(&value, "model")?;
            Ok(ClassifiedRequest {
                route_request: route_request(
                    Endpoint::ChatCompletions,
                    model,
                    string_field(&value, "service_tier"),
                    stream,
                ),
                request_shape: Some(request_shape(&value)),
                body,
            })
        }
        "/v1/responses" => {
            let value = parse_json(&body)?;
            let stream = bool_field(&value, "stream").unwrap_or(false);
            let model = required_string(&value, "model")?;
            Ok(ClassifiedRequest {
                route_request: route_request(
                    Endpoint::Responses,
                    model,
                    string_field(&value, "service_tier"),
                    stream,
                ),
                request_shape: Some(request_shape(&value)),
                body,
            })
        }
        "/v1/responses/compact" => {
            parse_json(&body)?;
            Ok(ClassifiedRequest {
                route_request: route_request(
                    Endpoint::ResponsesCompact,
                    String::new(),
                    None,
                    false,
                ),
                request_shape: None,
                body,
            })
        }
        "/v1/messages" => {
            let value = parse_json(&body)?;
            let stream = bool_field(&value, "stream").unwrap_or(false);
            let model = required_string(&value, "model")?;
            Ok(ClassifiedRequest {
                route_request: route_request(Endpoint::AnthropicMessages, model, None, stream),
                request_shape: None,
                body,
            })
        }
        _ => Err(TokenproxyError::new(
            StatusCode::NOT_FOUND,
            ErrorCode::UnsupportedRoute,
            format!("unsupported route: {path}"),
        )),
    }
}

fn route_request(
    endpoint: Endpoint,
    model: String,
    service_tier: Option<String>,
    stream: bool,
) -> RouteRequest {
    let model_family = if model.is_empty() {
        "unknown".to_string()
    } else {
        model_family_label(&model)
    };
    RouteRequest {
        endpoint,
        transport: Transport::Http,
        model,
        service_tier,
        pinned_account_id: None,
        requires_incremental_previous_response_id: false,
        model_family,
        stream,
    }
}

fn parse_json(body: &[u8]) -> Result<Value, TokenproxyError> {
    serde_json::from_slice(body).map_err(|error| {
        TokenproxyError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidJson,
            format!("invalid JSON request body: {error}"),
        )
    })
}

fn required_string(value: &Value, field: &str) -> Result<String, TokenproxyError> {
    string_field(value, field).ok_or_else(|| {
        TokenproxyError::new(
            StatusCode::BAD_REQUEST,
            ErrorCode::InvalidJson,
            format!("JSON body missing string field {field}"),
        )
    })
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn bool_field(value: &Value, field: &str) -> Option<bool> {
    value.get(field).and_then(Value::as_bool)
}

fn request_shape(value: &Value) -> RequestShape {
    RequestShape {
        service_tier: string_field(value, "service_tier")
            .map(|tier| normalize_service_tier(&tier).to_string())
            .unwrap_or_else(|| "unknown".to_string()),
        reasoning_effort: nested_string_field(value, "reasoning", "effort")
            .unwrap_or_else(|| "unset".to_string()),
        verbosity: nested_string_field(value, "text", "verbosity")
            .unwrap_or_else(|| "unset".to_string()),
        store: optional_bool_label(value, "store").to_string(),
    }
}

fn optional_bool_label(value: &Value, field: &str) -> &'static str {
    match value.get(field).and_then(Value::as_bool) {
        Some(true) => "true",
        Some(false) => "false",
        None => "unset",
    }
}

fn nested_string_field(value: &Value, object: &str, field: &str) -> Option<String> {
    value
        .get(object)
        .and_then(Value::as_object)
        .and_then(|object| object.get(field))
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_classify_supported_json_routes() {
        let classified = classify_request(
            "/v1/responses",
            Bytes::from_static(
                br#"{"model":"gpt-5.5","stream":true,"service_tier":"priority","reasoning":{"effort":"high"},"text":{"verbosity":"low"}}"#,
            ),
        )
        .unwrap();

        let request = classified.route_request;
        assert_eq!(request.endpoint, Endpoint::Responses);
        assert_eq!(request.service_tier.as_deref(), Some("priority"));
        assert!(request.stream);
        assert_eq!(
            classified.request_shape,
            Some(RequestShape {
                service_tier: "priority".to_string(),
                reasoning_effort: "high".to_string(),
                verbosity: "low".to_string(),
                store: "unset".to_string(),
            })
        );
    }

    #[test]
    fn should_classify_anthropic_messages_as_json_pass_through() {
        let classified = classify_request(
            "/v1/messages",
            Bytes::from_static(
                br#"{"model":"claude-sonnet-4.5","stream":true,"max_tokens":1024,"messages":[{"role":"user","content":"hello"}]}"#,
            ),
        )
        .unwrap();

        assert_eq!(classified.request_shape, None);
        let request = classified.route_request;
        assert_eq!(request.endpoint, Endpoint::AnthropicMessages);
        assert_eq!(request.model, "claude-sonnet-4.5");
        assert_eq!(request.service_tier, None);
        assert_eq!(request.model_family, "claude-sonnet");
        assert!(request.stream);
    }

    #[test]
    fn should_capture_request_shape_store_flag() {
        let classified = classify_request(
            "/v1/responses",
            Bytes::from_static(
                br#"{"model":"gpt-5.5","store":true,"service_tier":"priority","input":[]}"#,
            ),
        )
        .unwrap();

        assert_eq!(
            classified.request_shape,
            Some(RequestShape {
                service_tier: "priority".to_string(),
                reasoning_effort: "unset".to_string(),
                verbosity: "unset".to_string(),
                store: "true".to_string(),
            })
        );
    }

    #[test]
    fn should_keep_model_major_version_in_routing_family() {
        let classified = classify_request(
            "/v1/responses",
            Bytes::from_static(br#"{"model":"gpt-5.5","input":[]}"#),
        )
        .unwrap();

        assert_eq!(classified.route_request.model_family.as_str(), "gpt-5");
    }

    #[test]
    fn should_normalize_legacy_fast_service_tier_for_request_shape() {
        let classified = classify_request(
            "/v1/responses",
            Bytes::from_static(br#"{"model":"gpt-5.5","service_tier":"fast","input":[]}"#),
        )
        .unwrap();

        assert_eq!(
            classified.route_request.service_tier.as_deref(),
            Some("fast")
        );
        assert_eq!(
            classified.request_shape.unwrap().service_tier.as_str(),
            "priority"
        );
    }

    #[test]
    fn should_classify_compact_as_json_pass_through() {
        let classified = classify_request(
            "/v1/responses/compact",
            Bytes::from_static(br#"{"input":[{"type":"message","content":"compact"}]}"#),
        )
        .unwrap();

        assert_eq!(
            classified.body,
            Bytes::from_static(br#"{"input":[{"type":"message","content":"compact"}]}"#)
        );
        let request = classified.route_request;
        assert_eq!(request.endpoint, Endpoint::ResponsesCompact);
        assert_eq!(request.model, "");

        let invalid = classify_request(
            "/v1/responses/compact",
            Bytes::from_static(b"opaque upstream payload"),
        )
        .unwrap_err();
        assert_eq!(invalid.status, StatusCode::BAD_REQUEST);
        assert_eq!(invalid.code, ErrorCode::InvalidJson);
    }

    #[test]
    fn should_reject_invalid_json_body() {
        let invalid =
            classify_request("/v1/chat/completions", Bytes::from_static(b"not-json")).unwrap_err();

        assert_eq!(invalid.code, ErrorCode::InvalidJson);
    }

    #[test]
    fn should_reject_unbound_paths_as_unsupported_routes() {
        let error = classify_request("/v1/embeddings", Bytes::new()).unwrap_err();

        assert_eq!(error.status, StatusCode::NOT_FOUND);
        assert_eq!(error.code, ErrorCode::UnsupportedRoute);
    }
}
