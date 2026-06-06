use axum::http::{HeaderMap, Method, StatusCode};
use bytes::Bytes;
use serde_json::Value;

use crate::error::{ErrorCode, TokenproxyError};
use crate::model::model_family_label;
use crate::routing::{Endpoint, RouteRequest, Transport};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteKind {
    ChatCompletions { stream: bool },
    Responses { stream: bool },
    ResponsesCompact,
    AnthropicMessages { stream: bool },
    ResponsesWebSocket,
    Models,
    Health,
    Metrics,
    Usage,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClassifiedRequest {
    pub route: RouteKind,
    pub route_request: Option<RouteRequest>,
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

pub fn classify_request(
    method: &Method,
    path: &str,
    headers: &HeaderMap,
    body: Bytes,
    max_body_bytes: usize,
) -> Result<ClassifiedRequest, TokenproxyError> {
    if body.len() > max_body_bytes {
        return Err(TokenproxyError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            ErrorCode::BodyTooLarge,
            "request body exceeds server.max_body_bytes",
        ));
    }

    match (method, path) {
        (&Method::POST, "/v1/chat/completions") => {
            let value = parse_json(&body)?;
            let stream = bool_field(&value, "stream").unwrap_or(false);
            let model = required_string(&value, "model")?;
            Ok(ClassifiedRequest {
                route: RouteKind::ChatCompletions { stream },
                route_request: Some(RouteRequest {
                    endpoint: Endpoint::ChatCompletions,
                    transport: Transport::Http,
                    model,
                    service_tier: string_field(&value, "service_tier"),
                    pinned_account_id: None,
                    allow_failover_from_pinned: false,
                    replay_can_remove_previous_response_id: false,
                    requires_incremental_previous_response_id: false,
                    caller_hash: "downstream".to_string(),
                    model_family: model_family(&value),
                    stream,
                }),
                request_shape: Some(request_shape(&value)),
                body,
            })
        }
        (&Method::POST, "/v1/responses") => {
            let value = parse_json(&body)?;
            let stream = bool_field(&value, "stream").unwrap_or(false);
            let model = required_string(&value, "model")?;
            Ok(ClassifiedRequest {
                route: RouteKind::Responses { stream },
                route_request: Some(RouteRequest {
                    endpoint: Endpoint::Responses,
                    transport: Transport::Http,
                    model,
                    service_tier: string_field(&value, "service_tier"),
                    pinned_account_id: None,
                    allow_failover_from_pinned: false,
                    replay_can_remove_previous_response_id: value
                        .get("previous_response_id")
                        .and_then(Value::as_str)
                        .is_some(),
                    requires_incremental_previous_response_id: false,
                    caller_hash: "downstream".to_string(),
                    model_family: model_family(&value),
                    stream,
                }),
                request_shape: Some(request_shape(&value)),
                body,
            })
        }
        (&Method::POST, "/v1/responses/compact") => {
            parse_json(&body)?;
            Ok(ClassifiedRequest {
                route: RouteKind::ResponsesCompact,
                route_request: Some(RouteRequest {
                    endpoint: Endpoint::ResponsesCompact,
                    transport: Transport::Http,
                    model: String::new(),
                    service_tier: None,
                    pinned_account_id: None,
                    allow_failover_from_pinned: false,
                    replay_can_remove_previous_response_id: false,
                    requires_incremental_previous_response_id: false,
                    caller_hash: "downstream".to_string(),
                    model_family: "unknown".to_string(),
                    stream: false,
                }),
                request_shape: None,
                body,
            })
        }
        (&Method::POST, "/v1/messages") => {
            let value = parse_json(&body)?;
            let stream = bool_field(&value, "stream").unwrap_or(false);
            let model = required_string(&value, "model")?;
            Ok(ClassifiedRequest {
                route: RouteKind::AnthropicMessages { stream },
                route_request: Some(RouteRequest {
                    endpoint: Endpoint::AnthropicMessages,
                    transport: Transport::Http,
                    model,
                    service_tier: None,
                    pinned_account_id: None,
                    allow_failover_from_pinned: false,
                    replay_can_remove_previous_response_id: false,
                    requires_incremental_previous_response_id: false,
                    caller_hash: "downstream".to_string(),
                    model_family: model_family(&value),
                    stream,
                }),
                request_shape: None,
                body,
            })
        }
        (&Method::GET, "/v1/responses") if is_websocket_upgrade(headers) => Ok(ClassifiedRequest {
            route: RouteKind::ResponsesWebSocket,
            route_request: None,
            request_shape: None,
            body,
        }),
        (&Method::GET, "/v1/responses") => Err(TokenproxyError::new(
            StatusCode::UPGRADE_REQUIRED,
            ErrorCode::UnsupportedMethod,
            "GET /v1/responses requires WebSocket upgrade",
        )),
        (&Method::GET, "/v1/responses/compact") => Err(TokenproxyError::new(
            StatusCode::UPGRADE_REQUIRED,
            ErrorCode::UnsupportedMethod,
            "GET /v1/responses/compact does not support WebSocket transport",
        )),
        (&Method::GET, "/v1/models") => Ok(no_body(RouteKind::Models, body)),
        (&Method::GET, "/healthz") => Ok(no_body(RouteKind::Health, body)),
        (&Method::GET, "/metrics") => Ok(no_body(RouteKind::Metrics, body)),
        (&Method::GET, "/usage") => Ok(no_body(RouteKind::Usage, body)),
        (_, "/v1/chat/completions" | "/v1/responses" | "/v1/messages") => {
            Err(TokenproxyError::new(
                StatusCode::NOT_FOUND,
                ErrorCode::UnsupportedRoute,
                format!("unsupported route: {method} {path}"),
            ))
        }
        (_, "/v1/responses/compact") => Err(TokenproxyError::new(
            StatusCode::METHOD_NOT_ALLOWED,
            ErrorCode::UnsupportedMethod,
            format!("unsupported method: {method} {path}"),
        )),
        (_, "/v1/models" | "/healthz" | "/metrics" | "/usage") => Err(TokenproxyError::new(
            StatusCode::METHOD_NOT_ALLOWED,
            ErrorCode::UnsupportedMethod,
            format!("unsupported method: {method} {path}"),
        )),
        _ => Err(TokenproxyError::new(
            StatusCode::NOT_FOUND,
            ErrorCode::UnsupportedRoute,
            format!("unsupported route: {method} {path}"),
        )),
    }
}

fn no_body(route: RouteKind, body: Bytes) -> ClassifiedRequest {
    ClassifiedRequest {
        route,
        route_request: None,
        request_shape: None,
        body,
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

fn model_family(value: &Value) -> String {
    string_field(value, "model")
        .map(|model| model_family_label(&model))
        .unwrap_or_else(|| "unknown".to_string())
}

fn request_shape(value: &Value) -> RequestShape {
    RequestShape {
        service_tier: string_field(value, "service_tier")
            .map(normalize_service_tier_label)
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

fn normalize_service_tier_label(service_tier: String) -> String {
    if service_tier.trim().eq_ignore_ascii_case("fast") {
        "priority".to_string()
    } else {
        service_tier
    }
}

fn is_websocket_upgrade(headers: &HeaderMap) -> bool {
    let connection = headers
        .get("connection")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    let upgrade = headers
        .get("upgrade")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();

    connection.split(',').any(|part| part.trim() == "upgrade") && upgrade == "websocket"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_classify_supported_json_routes() {
        let classified = classify_request(
            &Method::POST,
            "/v1/responses",
            &HeaderMap::new(),
            Bytes::from_static(
                br#"{"model":"gpt-5.5","stream":true,"service_tier":"priority","reasoning":{"effort":"high"},"text":{"verbosity":"low"}}"#,
            ),
            1024,
        )
        .unwrap();

        assert_eq!(classified.route, RouteKind::Responses { stream: true });
        let request = classified.route_request.unwrap();
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
            &Method::POST,
            "/v1/messages",
            &HeaderMap::new(),
            Bytes::from_static(
                br#"{"model":"claude-sonnet-4.5","stream":true,"max_tokens":1024,"messages":[{"role":"user","content":"hello"}]}"#,
            ),
            1024,
        )
        .unwrap();

        assert_eq!(
            classified.route,
            RouteKind::AnthropicMessages { stream: true }
        );
        assert_eq!(classified.request_shape, None);
        let request = classified.route_request.unwrap();
        assert_eq!(request.endpoint, Endpoint::AnthropicMessages);
        assert_eq!(request.model, "claude-sonnet-4.5");
        assert_eq!(request.service_tier, None);
        assert_eq!(request.model_family, "claude-sonnet");
        assert!(request.stream);
    }

    #[test]
    fn should_capture_request_shape_store_flag() {
        let classified = classify_request(
            &Method::POST,
            "/v1/responses",
            &HeaderMap::new(),
            Bytes::from_static(
                br#"{"model":"gpt-5.5","store":true,"service_tier":"priority","input":[]}"#,
            ),
            1024,
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
            &Method::POST,
            "/v1/responses",
            &HeaderMap::new(),
            Bytes::from_static(br#"{"model":"gpt-5.5","input":[]}"#),
            1024,
        )
        .unwrap();

        assert_eq!(
            classified.route_request.unwrap().model_family.as_str(),
            "gpt-5"
        );
    }

    #[test]
    fn should_normalize_legacy_fast_service_tier_for_request_shape() {
        let classified = classify_request(
            &Method::POST,
            "/v1/responses",
            &HeaderMap::new(),
            Bytes::from_static(br#"{"model":"gpt-5.5","service_tier":"fast","input":[]}"#),
            1024,
        )
        .unwrap();

        assert_eq!(
            classified.route_request.unwrap().service_tier.as_deref(),
            Some("fast")
        );
        assert_eq!(
            classified.request_shape.unwrap().service_tier.as_str(),
            "priority"
        );
    }

    #[test]
    fn should_detect_responses_websocket_upgrade() {
        let mut headers = HeaderMap::new();
        headers.insert("connection", "keep-alive, Upgrade".parse().unwrap());
        headers.insert("upgrade", "websocket".parse().unwrap());

        let classified =
            classify_request(&Method::GET, "/v1/responses", &headers, Bytes::new(), 1024).unwrap();

        assert_eq!(classified.route, RouteKind::ResponsesWebSocket);
    }

    #[test]
    fn should_classify_compact_as_json_pass_through() {
        let classified = classify_request(
            &Method::POST,
            "/v1/responses/compact",
            &HeaderMap::new(),
            Bytes::from_static(br#"{"input":[{"type":"message","content":"compact"}]}"#),
            1024,
        )
        .unwrap();

        assert_eq!(classified.route, RouteKind::ResponsesCompact);
        assert_eq!(
            classified.body,
            Bytes::from_static(br#"{"input":[{"type":"message","content":"compact"}]}"#)
        );
        let request = classified.route_request.unwrap();
        assert_eq!(request.endpoint, Endpoint::ResponsesCompact);
        assert_eq!(request.model, "");

        let invalid = classify_request(
            &Method::POST,
            "/v1/responses/compact",
            &HeaderMap::new(),
            Bytes::from_static(b"opaque upstream payload"),
            1024,
        )
        .unwrap_err();
        assert_eq!(invalid.status, StatusCode::BAD_REQUEST);
        assert_eq!(invalid.code, ErrorCode::InvalidJson);
    }

    #[test]
    fn should_reject_invalid_json_and_oversized_body() {
        let invalid = classify_request(
            &Method::POST,
            "/v1/chat/completions",
            &HeaderMap::new(),
            Bytes::from_static(b"not-json"),
            1024,
        )
        .unwrap_err();
        assert_eq!(invalid.code, ErrorCode::InvalidJson);

        let too_large = classify_request(
            &Method::POST,
            "/v1/chat/completions",
            &HeaderMap::new(),
            Bytes::from_static(br#"{"model":"gpt-5.5"}"#),
            1,
        )
        .unwrap_err();
        assert_eq!(too_large.code, ErrorCode::BodyTooLarge);
    }

    #[test]
    fn should_classify_generation_wrong_methods_as_unsupported_routes() {
        let error = classify_request(
            &Method::GET,
            "/v1/chat/completions",
            &HeaderMap::new(),
            Bytes::new(),
            1024,
        )
        .unwrap_err();

        assert_eq!(error.status, StatusCode::NOT_FOUND);
        assert_eq!(error.code, ErrorCode::UnsupportedRoute);

        let responses_error = classify_request(
            &Method::PUT,
            "/v1/responses",
            &HeaderMap::new(),
            Bytes::new(),
            1024,
        )
        .unwrap_err();

        assert_eq!(responses_error.status, StatusCode::NOT_FOUND);
        assert_eq!(responses_error.code, ErrorCode::UnsupportedRoute);
    }

    #[test]
    fn should_classify_operator_wrong_methods_as_unsupported_methods() {
        let model_error = classify_request(
            &Method::POST,
            "/v1/models",
            &HeaderMap::new(),
            Bytes::new(),
            1024,
        )
        .unwrap_err();

        assert_eq!(model_error.status, StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(model_error.code, ErrorCode::UnsupportedMethod);
    }

    #[test]
    fn should_reject_compact_get_as_unsupported_websocket_transport() {
        let error = classify_request(
            &Method::GET,
            "/v1/responses/compact",
            &HeaderMap::new(),
            Bytes::new(),
            1024,
        )
        .unwrap_err();

        assert_eq!(error.status, StatusCode::UPGRADE_REQUIRED);
        assert_eq!(error.code, ErrorCode::UnsupportedMethod);
    }
}
