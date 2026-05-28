use serde_json::{Map, Value};

use crate::error::{ErrorCode, TokenproxyError};
use crate::responses::state::ReplayState;

const TRANSPORT_ONLY_FIELDS: &[&str] = &["stream", "background"];
const STABLE_TEMPLATE_FIELDS: &[&str] = &[
    "model",
    "instructions",
    "tools",
    "tool_choice",
    "parallel_tool_calls",
    "reasoning",
    "text",
    "truncation",
    "store",
    "service_tier",
];

#[derive(Debug, Clone, PartialEq)]
pub enum ReplayPlan {
    Incremental(Value),
    FullReplay(Value),
}

pub fn is_previous_response_not_found_event(text: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return false;
    };
    value.get("type").and_then(Value::as_str) == Some("error")
        && value
            .pointer("/error/code")
            .and_then(Value::as_str)
            .is_some_and(|code| code == "previous_response_not_found")
}

pub fn previous_response_not_found_retry_payload(
    state: &ReplayState,
    original_request: Value,
    already_retried: bool,
) -> Result<Option<Value>, TokenproxyError> {
    if already_retried
        || state.last_completed_response_id.is_none()
        || state.last_request_template.is_none()
    {
        return Ok(None);
    }
    let normalized = normalize_websocket_create(original_request)?;
    Ok(Some(build_full_replay(state, normalized)?))
}

pub fn normalize_websocket_create(mut value: Value) -> Result<Value, TokenproxyError> {
    if value.get("type").and_then(Value::as_str) != Some("response.create") {
        return Err(TokenproxyError::new(
            axum::http::StatusCode::BAD_REQUEST,
            ErrorCode::WebSocketUnsupportedMessage,
            "WebSocket text frames must use type=response.create",
        ));
    }
    remove_transport_only_fields(&mut value);
    normalize_legacy_service_tier(&mut value);
    Ok(value)
}

pub fn is_compacted_request_window(value: &Value) -> bool {
    value
        .get("input")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items.iter().any(|item| {
                item.get("type").and_then(Value::as_str) == Some("compaction")
                    && item
                        .get("encrypted_content")
                        .and_then(Value::as_str)
                        .is_some_and(|content| !content.is_empty())
            })
        })
}

pub fn plan_next_request(
    state: &ReplayState,
    new_request: Value,
    selected_account_id_hash: &str,
    connection_previous_response_available: bool,
) -> Result<ReplayPlan, TokenproxyError> {
    let mut normalized = normalize_websocket_create(new_request)?;
    let same_account = state.account_id_hash.as_deref() == Some(selected_account_id_hash);

    if state.supports_incremental_previous_response_id
        && same_account
        && connection_previous_response_available
        && let Some(previous_response_id) = &state.last_completed_response_id
    {
        normalized["previous_response_id"] = Value::String(previous_response_id.clone());
        return Ok(ReplayPlan::Incremental(normalized));
    }

    Ok(ReplayPlan::FullReplay(build_full_replay(
        state, normalized,
    )?))
}

pub fn build_full_replay(
    state: &ReplayState,
    new_request: Value,
) -> Result<Value, TokenproxyError> {
    let template = state.last_request_template.as_ref().ok_or_else(|| {
        TokenproxyError::new(
            axum::http::StatusCode::BAD_REQUEST,
            ErrorCode::WebSocketUnsupportedMessage,
            "full replay requires last request template",
        )
    })?;

    let mut output = object_clone(&new_request)?;
    output.remove("previous_response_id");

    for field in STABLE_TEMPLATE_FIELDS {
        if !output.contains_key(*field)
            && let Some(value) = template.get(*field)
        {
            output.insert((*field).to_string(), value.clone());
        }
    }

    remove_transport_only_fields_from_map(&mut output);

    let mut input = Vec::new();
    extend_array_field(&mut input, template, "input");
    input.extend(state.last_completed_output_items.iter().cloned());
    extend_new_input_deduping_tool_outputs(&mut input, &new_request);
    output.insert("input".to_string(), Value::Array(input));

    Ok(Value::Object(output))
}

fn object_clone(value: &Value) -> Result<Map<String, Value>, TokenproxyError> {
    value.as_object().cloned().ok_or_else(|| {
        TokenproxyError::new(
            axum::http::StatusCode::BAD_REQUEST,
            ErrorCode::WebSocketUnsupportedMessage,
            "response.create payload must be a JSON object",
        )
    })
}

fn normalize_legacy_service_tier(value: &mut Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    if object
        .get("service_tier")
        .and_then(Value::as_str)
        .is_some_and(|tier| tier.trim().eq_ignore_ascii_case("fast"))
    {
        object.insert(
            "service_tier".to_string(),
            Value::String("priority".to_string()),
        );
    }
}

fn remove_transport_only_fields(value: &mut Value) {
    if let Some(object) = value.as_object_mut() {
        remove_transport_only_fields_from_map(object);
    }
}

fn remove_transport_only_fields_from_map(object: &mut Map<String, Value>) {
    for field in TRANSPORT_ONLY_FIELDS {
        object.remove(*field);
    }
}

fn extend_array_field(output: &mut Vec<Value>, value: &Value, field: &str) {
    if let Some(items) = value.get(field).and_then(Value::as_array) {
        output.extend(items.iter().cloned());
    }
}

fn extend_new_input_deduping_tool_outputs(output: &mut Vec<Value>, new_request: &Value) {
    if let Some(items) = new_request.get("input").and_then(Value::as_array) {
        for item in items {
            if !is_duplicate_tool_output(output, item) {
                output.push(item.clone());
            }
        }
    }
}

fn is_duplicate_tool_output(existing: &[Value], item: &Value) -> bool {
    if item.get("type").and_then(Value::as_str) != Some("function_call_output") {
        return false;
    }
    let Some(call_id) = item.get("call_id").and_then(Value::as_str) else {
        return false;
    };
    existing.iter().any(|existing_item| {
        existing_item.get("type").and_then(Value::as_str) == Some("function_call_output")
            && existing_item.get("call_id").and_then(Value::as_str) == Some(call_id)
            && existing_item == item
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn should_build_incremental_fast_path_with_previous_response_id() {
        let state = ReplayState {
            account_id_hash: Some("acct".to_string()),
            supports_incremental_previous_response_id: true,
            last_completed_response_id: Some("resp_1".to_string()),
            ..ReplayState::default()
        };

        let plan = plan_next_request(
            &state,
            json!({"type":"response.create","stream":true,"input":[{"type":"message"}]}),
            "acct",
            true,
        )
        .unwrap();

        let ReplayPlan::Incremental(value) = plan else {
            panic!("expected incremental plan");
        };
        assert_eq!(value["previous_response_id"], "resp_1");
        assert!(value.get("stream").is_none());
    }

    #[test]
    fn should_full_replay_when_connection_previous_response_is_unavailable() {
        let state = ReplayState {
            account_id_hash: Some("acct".to_string()),
            supports_incremental_previous_response_id: true,
            last_request_template: Some(json!({
                "type":"response.create",
                "model":"gpt-5.5",
                "input":[{"type":"message","role":"user","content":"old"}]
            })),
            last_completed_response_id: Some("resp_1".to_string()),
            last_completed_output_items: vec![json!({
                "type":"message",
                "phase":"final",
                "content":"old answer"
            })],
            ..ReplayState::default()
        };

        let plan = plan_next_request(
            &state,
            json!({
                "type":"response.create",
                "previous_response_id":"resp_1",
                "input":[{"type":"message","role":"user","content":"next"}]
            }),
            "acct",
            false,
        )
        .unwrap();

        let ReplayPlan::FullReplay(value) = plan else {
            panic!(
                "expected full replay when the upstream connection lost previous-response state"
            );
        };
        assert!(value.get("previous_response_id").is_none());
        assert_eq!(value["input"].as_array().unwrap().len(), 3);
        assert_eq!(value["input"][1]["phase"], "final");
    }

    #[test]
    fn should_normalize_legacy_fast_service_tier_for_websocket_create() {
        let normalized = normalize_websocket_create(json!({
            "type": "response.create",
            "model": "gpt-5.5",
            "service_tier": "fast",
            "input": []
        }))
        .unwrap();

        assert_eq!(normalized["service_tier"].as_str(), Some("priority"));
    }

    #[test]
    fn should_build_full_replay_preserving_phase_and_unknown_fields() {
        let state = ReplayState {
            last_request_template: Some(json!({
                "type":"response.create",
                "model":"gpt-5.5",
                "tools":[{"type":"function","name":"run_test"}],
                "input":[{"type":"message","role":"user","content":"Run tests"}]
            })),
            last_completed_output_items: vec![json!({
                "type":"message",
                "phase":"final",
                "x_unknown": true
            })],
            ..ReplayState::default()
        };

        let value = build_full_replay(
            &state,
            json!({
                "type":"response.create",
                "previous_response_id":"stale",
                "background": true,
                "input":[{"type":"function_call_output","call_id":"call_1","output":"ok"}]
            }),
        )
        .unwrap();

        assert!(value.get("previous_response_id").is_none());
        assert!(value.get("background").is_none());
        assert_eq!(value["input"][1]["phase"], "final");
        assert_eq!(value["input"][1]["x_unknown"], true);
    }

    #[test]
    fn should_prefer_new_stable_fields_when_building_full_replay() {
        let state = ReplayState {
            last_request_template: Some(json!({
                "type":"response.create",
                "model":"gpt-5.5",
                "service_tier":"default",
                "reasoning":{"effort":"low"},
                "text":{"verbosity":"low"},
                "input":[{"type":"message","role":"user","content":"old"}]
            })),
            last_completed_output_items: vec![json!({
                "type":"message",
                "phase":"final",
                "content":"old answer"
            })],
            ..ReplayState::default()
        };

        let value = build_full_replay(
            &state,
            json!({
                "type":"response.create",
                "service_tier":"priority",
                "reasoning":{"effort":"high"},
                "text":{"verbosity":"high"},
                "input":[{"type":"message","role":"user","content":"new"}]
            }),
        )
        .unwrap();

        assert_eq!(value["model"], "gpt-5.5");
        assert_eq!(value["service_tier"], "priority");
        assert_eq!(value["reasoning"]["effort"], "high");
        assert_eq!(value["text"]["verbosity"], "high");
        assert_eq!(value["input"].as_array().unwrap().len(), 3);
    }

    #[test]
    fn should_deduplicate_tool_outputs_by_call_id_in_full_replay() {
        let state = ReplayState {
            last_request_template: Some(json!({
                "type":"response.create",
                "model":"gpt-5.5",
                "input":[
                    {"type":"message","role":"user","content":"Run tests"},
                    {"type":"function_call_output","call_id":"call_1","output":"old"}
                ]
            })),
            ..ReplayState::default()
        };

        let value = build_full_replay(
            &state,
            json!({
                "type":"response.create",
                "input":[
                    {"type":"function_call_output","call_id":"call_1","output":"old"}
                ]
            }),
        )
        .unwrap();

        let input = value["input"].as_array().unwrap();
        assert_eq!(input.len(), 2);
        assert_eq!(input[1]["output"], "old");
    }

    #[test]
    fn should_preserve_different_tool_outputs_with_the_same_call_id_in_full_replay() {
        let state = ReplayState {
            last_request_template: Some(json!({
                "type":"response.create",
                "model":"gpt-5.5",
                "input":[
                    {"type":"message","role":"user","content":"Run tests"},
                    {"type":"function_call_output","call_id":"call_1","output":"old"}
                ]
            })),
            ..ReplayState::default()
        };

        let value = build_full_replay(
            &state,
            json!({
                "type":"response.create",
                "input":[
                    {"type":"function_call_output","call_id":"call_1","output":"new"}
                ]
            }),
        )
        .unwrap();

        let input = value["input"].as_array().unwrap();
        assert_eq!(input.len(), 3);
        assert_eq!(input[1]["output"], "old");
        assert_eq!(input[2]["output"], "new");
    }

    #[test]
    fn should_detect_previous_response_not_found_error_event() {
        assert!(is_previous_response_not_found_event(
            r#"{"type":"error","error":{"code":"previous_response_not_found"}}"#
        ));
        assert!(!is_previous_response_not_found_event(
            r#"{"type":"error","error":{"code":"rate_limit_exceeded"}}"#
        ));
        assert!(!is_previous_response_not_found_event("not-json"));
    }

    #[test]
    fn should_build_single_full_replay_retry_for_previous_response_not_found() {
        let state = ReplayState {
            last_request_template: Some(json!({
                "type":"response.create",
                "model":"gpt-5.5",
                "input":[{"type":"message","role":"user","content":"hello"}]
            })),
            last_completed_output_items: vec![json!({
                "type":"message",
                "phase":"final",
                "content":"hello"
            })],
            last_completed_response_id: Some("resp_stale".to_string()),
            ..ReplayState::default()
        };

        let retry = previous_response_not_found_retry_payload(
            &state,
            json!({
                "type":"response.create",
                "previous_response_id":"stale",
                "input":[{"type":"message","role":"user","content":"again"}]
            }),
            false,
        )
        .unwrap()
        .unwrap();

        assert!(retry.get("previous_response_id").is_none());
        assert_eq!(retry["model"], "gpt-5.5");
        assert_eq!(retry["input"].as_array().unwrap().len(), 3);
        assert!(
            previous_response_not_found_retry_payload(
                &state,
                json!({"type":"response.create","input":[]}),
                true
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn should_not_retry_previous_response_not_found_without_completed_response_id() {
        let state = ReplayState {
            last_request_template: Some(json!({
                "type":"response.create",
                "model":"gpt-5.5",
                "input":[{"type":"message","role":"user","content":"hello"}]
            })),
            last_completed_output_items: vec![json!({
                "type":"message",
                "phase":"final",
                "content":"hello"
            })],
            ..ReplayState::default()
        };

        assert!(
            previous_response_not_found_retry_payload(
                &state,
                json!({
                    "type":"response.create",
                    "input":[{"type":"message","role":"user","content":"again"}]
                }),
                false,
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn should_not_retry_previous_response_not_found_without_request_template() {
        let state = ReplayState {
            last_completed_response_id: Some("resp_stale".to_string()),
            last_completed_output_items: vec![json!({
                "type":"message",
                "phase":"final",
                "content":"hello"
            })],
            ..ReplayState::default()
        };

        assert!(
            previous_response_not_found_retry_payload(
                &state,
                json!({
                    "type":"response.create",
                    "previous_response_id":"resp_stale",
                    "input":[{"type":"message","role":"user","content":"again"}]
                }),
                false,
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn should_detect_compacted_request_window_from_compaction_item() {
        assert!(is_compacted_request_window(&json!({
            "type": "response.create",
            "input": [
                {"type": "message", "role": "user", "content": "kept"},
                {"type": "compaction", "encrypted_content": "gAAAAABpM0Yj"}
            ]
        })));
        assert!(!is_compacted_request_window(&json!({
            "type": "response.create",
            "input": [{"type": "compaction"}]
        })));
        assert!(!is_compacted_request_window(&json!({
            "type": "response.create",
            "input": [{"type": "message", "content": "normal"}]
        })));
    }
}
