use bytes::Bytes;
use eventsource_stream::Event;
use serde_json::Value;

use axum::http::StatusCode;

use crate::error::{ErrorCode, TokenproxyError};

#[derive(Debug, Default)]
pub struct SseRepair {
    output_items: Vec<Value>,
}

impl SseRepair {
    pub fn observe_event(&mut self, event: Event) -> Result<Bytes, TokenproxyError> {
        if event.data == "[DONE]" {
            return Ok(Bytes::from(serialize_sse_event(&event, None)));
        }
        if event.event != "message"
            && event.event != "response.output_item.done"
            && event.event != "response.completed"
        {
            return Ok(Bytes::from(serialize_sse_event(&event, None)));
        }

        let mut value: Value = serde_json::from_str(&event.data).map_err(|error| {
            TokenproxyError::new(
                StatusCode::BAD_GATEWAY,
                ErrorCode::InvalidJson,
                format!("malformed SSE JSON frame: {error}"),
            )
        })?;

        if value.get("type").and_then(Value::as_str) == Some("response.output_item.done") {
            if let Some(item) = value.get("item").cloned() {
                self.output_items.push(item);
            }
            return Ok(Bytes::from(serialize_sse_event(&event, None)));
        }

        if value.get("type").and_then(Value::as_str) == Some("response.completed") {
            let missing_output = value
                .pointer("/response/output")
                .and_then(Value::as_array)
                .is_none_or(Vec::is_empty);

            if missing_output
                && !self.output_items.is_empty()
                && let Some(response) = value.get_mut("response").and_then(Value::as_object_mut)
            {
                response.insert(
                    "output".to_string(),
                    Value::Array(self.output_items.clone()),
                );
                let repaired_json = serde_json::to_string(&value).map_err(|error| {
                    TokenproxyError::new(
                        StatusCode::BAD_GATEWAY,
                        ErrorCode::InvalidJson,
                        format!("failed to serialize repaired SSE JSON frame: {error}"),
                    )
                })?;
                return Ok(Bytes::from(serialize_sse_event(
                    &event,
                    Some(&repaired_json),
                )));
            }
        }

        Ok(Bytes::from(serialize_sse_event(&event, None)))
    }
}

fn serialize_sse_event(event: &Event, data: Option<&str>) -> String {
    let mut output = Vec::new();
    if event.event != "message" && !event.event.is_empty() {
        output.push(format!("event: {}", event.event));
    }
    if !event.id.is_empty() {
        output.push(format!("id: {}", event.id));
    }
    if let Some(retry) = event.retry {
        output.push(format!("retry: {}", retry.as_millis()));
    }

    for line in data.unwrap_or(&event.data).split('\n') {
        output.push(format!("data: {line}"));
    }

    format!("{}\n\n", output.join("\n"))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn should_repair_completed_output_from_prior_output_item_events() {
        let mut repair = SseRepair::default();
        repair
            .observe_event(Event {
                event: "response.output_item.done".to_string(),
                data: r#"{"type":"response.output_item.done","item":{"type":"message","phase":"final","content":"ok"}}"#.to_string(),
                ..Event::default()
            })
            .unwrap();

        let repaired = repair
            .observe_event(Event {
                event: "message".to_string(),
                data: r#"{"type":"response.completed","response":{"id":"resp_1"}}"#.to_string(),
                ..Event::default()
            })
            .unwrap();

        assert!(
            std::str::from_utf8(&repaired)
                .unwrap()
                .contains(r#""phase":"final""#)
        );
    }

    #[test]
    fn should_preserve_sse_metadata_lines_when_repairing_completed_output() {
        let mut repair = SseRepair::default();
        repair
            .observe_event(Event {
                event: "response.output_item.done".to_string(),
                data: r#"{"type":"response.output_item.done","item":{"type":"message","phase":"final","content":"ok"}}"#.to_string(),
                id: "item-1".to_string(),
                ..Event::default()
            })
            .unwrap();

        let repaired = repair
            .observe_event(Event {
                event: "response.completed".to_string(),
                data: r#"{"type":"response.completed","response":{"id":"resp_1"}}"#.to_string(),
                id: "completed-1".to_string(),
                retry: Some(Duration::from_millis(1000)),
            })
            .unwrap();

        let repaired = std::str::from_utf8(&repaired).unwrap();
        assert!(repaired.starts_with("event: response.completed\nid: completed-1\nretry: 1000\n"));
        assert!(
            repaired.contains(r#""output":[{"content":"ok","phase":"final","type":"message"}]"#)
        );
        assert!(repaired.ends_with("\n\n"));
    }

    #[test]
    fn should_serialize_multiline_sse_data() {
        let mut repair = SseRepair::default();

        let event = repair
            .observe_event(Event {
                event: "response.custom".to_string(),
                data: "first\nsecond".to_string(),
                ..Event::default()
            })
            .unwrap();

        assert_eq!(
            std::str::from_utf8(&event).unwrap(),
            "event: response.custom\ndata: first\ndata: second\n\n"
        );
    }

    #[test]
    fn should_repair_completed_output_across_events() {
        let mut repair = SseRepair::default();
        repair
            .observe_event(Event {
                event: "message".to_string(),
                data: r#"{"type":"response.output_item.done","item":{"type":"message","phase":"final"}}"#.to_string(),
                ..Event::default()
            })
            .unwrap();

        let repaired = repair
            .observe_event(Event {
                event: "message".to_string(),
                data: r#"{"type":"response.completed","response":{"id":"resp_1"}}"#.to_string(),
                ..Event::default()
            })
            .unwrap();

        assert!(
            std::str::from_utf8(&repaired)
                .unwrap()
                .contains(r#""phase":"final""#)
        );
    }

    #[test]
    fn should_reject_malformed_sse_json_frame_before_commit() {
        let mut repair = SseRepair::default();

        let error = repair
            .observe_event(Event {
                event: "message".to_string(),
                data: "{not-json}".to_string(),
                ..Event::default()
            })
            .unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_GATEWAY);
        assert_eq!(error.code, ErrorCode::InvalidJson);
        assert!(error.message.contains("malformed SSE JSON frame"));
    }

    #[test]
    fn should_passthrough_unknown_sse_events_without_repair() {
        let mut repair = SseRepair::default();
        let frame = "event: response.custom\nid: custom-1\ndata: {\"type\":\"response.custom\",\"x\":true}\n\n";

        let event = repair
            .observe_event(Event {
                event: "response.custom".to_string(),
                data: r#"{"type":"response.custom","x":true}"#.to_string(),
                id: "custom-1".to_string(),
                ..Event::default()
            })
            .unwrap();

        assert_eq!(std::str::from_utf8(&event).unwrap(), frame);
    }

    #[test]
    fn should_passthrough_unknown_sse_events_without_json_parsing() {
        let mut repair = SseRepair::default();
        let frame = "event: response.custom\nid: custom-1\ndata: opaque-extension-payload\n\n";

        let event = repair
            .observe_event(Event {
                event: "response.custom".to_string(),
                data: "opaque-extension-payload".to_string(),
                id: "custom-1".to_string(),
                ..Event::default()
            })
            .unwrap();

        assert_eq!(std::str::from_utf8(&event).unwrap(), frame);
    }
}
