use bytes::Bytes;
use serde_json::Value;

use axum::http::StatusCode;

use crate::error::{ErrorCode, TokenproxyError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SseRepairEvent {
    Passthrough(String),
    RepairedCompleted(String),
}

#[derive(Debug, Default)]
pub struct SseRepair {
    output_items: Vec<Value>,
    pending: String,
}

impl SseRepair {
    pub fn observe_chunk(&mut self, chunk: &[u8]) -> Result<Vec<Bytes>, TokenproxyError> {
        let text = std::str::from_utf8(chunk).map_err(|error| {
            TokenproxyError::new(
                StatusCode::BAD_GATEWAY,
                ErrorCode::InvalidJson,
                format!("SSE chunk is not UTF-8: {error}"),
            )
        })?;
        self.pending.push_str(text);

        let mut repaired = Vec::new();
        while let Some(index) = self.pending.find("\n\n") {
            let frame = self.pending[..index + 2].to_string();
            self.pending.drain(..index + 2);
            match self.observe_frame(&frame)? {
                SseRepairEvent::Passthrough(frame) | SseRepairEvent::RepairedCompleted(frame) => {
                    repaired.push(Bytes::from(frame));
                }
            }
        }

        Ok(repaired)
    }

    pub fn observe_frame(&mut self, frame: &str) -> Result<SseRepairEvent, TokenproxyError> {
        let event_type = sse_event_type(frame);
        let Some(data) = frame
            .lines()
            .find_map(|line| line.strip_prefix("data:").map(str::trim))
        else {
            return Ok(SseRepairEvent::Passthrough(frame.to_string()));
        };
        if data == "[DONE]" {
            return Ok(SseRepairEvent::Passthrough(frame.to_string()));
        }
        if event_type.is_some_and(|event_type| {
            event_type != "response.output_item.done" && event_type != "response.completed"
        }) {
            return Ok(SseRepairEvent::Passthrough(frame.to_string()));
        }

        let mut value: Value = serde_json::from_str(data).map_err(|error| {
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
            return Ok(SseRepairEvent::Passthrough(frame.to_string()));
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
                let repaired = repair_data_line(frame, &repaired_json);
                return Ok(SseRepairEvent::RepairedCompleted(repaired));
            }
        }

        Ok(SseRepairEvent::Passthrough(frame.to_string()))
    }
}

fn sse_event_type(frame: &str) -> Option<&str> {
    frame
        .lines()
        .find_map(|line| line.strip_prefix("event:").map(str::trim))
        .filter(|event_type| !event_type.is_empty())
}

fn repair_data_line(frame: &str, data: &str) -> String {
    let mut replaced = false;
    let mut output = Vec::new();

    for line in frame.lines() {
        if !replaced && line.strip_prefix("data:").is_some() {
            output.push(format!("data: {data}"));
            replaced = true;
        } else if !line.is_empty() {
            output.push(line.to_string());
        }
    }

    if !replaced {
        output.push(format!("data: {data}"));
    }

    format!("{}\n\n", output.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_repair_completed_output_from_prior_output_item_events() {
        let mut repair = SseRepair::default();
        repair
            .observe_frame(
                r#"event: response.output_item.done
data: {"type":"response.output_item.done","item":{"type":"message","phase":"final","content":"ok"}}"#,
            )
            .unwrap();

        let repaired = repair
            .observe_frame(r#"data: {"type":"response.completed","response":{"id":"resp_1"}}"#)
            .unwrap();

        let SseRepairEvent::RepairedCompleted(frame) = repaired else {
            panic!("expected repaired frame");
        };
        assert!(frame.contains(r#""phase":"final""#));
    }

    #[test]
    fn should_preserve_sse_metadata_lines_when_repairing_completed_output() {
        let mut repair = SseRepair::default();
        repair
            .observe_frame(
                r#"event: response.output_item.done
id: item-1
data: {"type":"response.output_item.done","item":{"type":"message","phase":"final","content":"ok"}}"#,
            )
            .unwrap();

        let repaired = repair
            .observe_frame(
                r#"event: response.completed
id: completed-1
retry: 1000
data: {"type":"response.completed","response":{"id":"resp_1"}}"#,
            )
            .unwrap();

        let SseRepairEvent::RepairedCompleted(frame) = repaired else {
            panic!("expected repaired frame");
        };
        assert!(frame.starts_with("event: response.completed\nid: completed-1\nretry: 1000\n"));
        assert!(frame.contains(r#""output":[{"content":"ok","phase":"final","type":"message"}]"#));
        assert!(frame.ends_with("\n\n"));
    }

    #[test]
    fn should_buffer_partial_sse_chunks_until_frame_boundary() {
        let mut repair = SseRepair::default();

        let first = repair.observe_chunk(br#"data: {"type":"response.output_item.done","item":{"type":"message","phase":"final"}}"#).unwrap();
        assert!(first.is_empty());

        let second = repair.observe_chunk(b"\n\n").unwrap();
        assert_eq!(second.len(), 1);
        assert!(
            std::str::from_utf8(&second[0])
                .unwrap()
                .contains("response.output_item.done")
        );
    }

    #[test]
    fn should_repair_completed_output_across_chunks() {
        let mut repair = SseRepair::default();
        repair
            .observe_chunk(
                br#"data: {"type":"response.output_item.done","item":{"type":"message","phase":"final"}}"#,
            )
            .unwrap();
        repair.observe_chunk(b"\n\n").unwrap();

        let repaired = repair
            .observe_chunk(br#"data: {"type":"response.completed","response":{"id":"resp_1"}}"#)
            .unwrap();
        assert!(repaired.is_empty());
        let repaired = repair.observe_chunk(b"\n\n").unwrap();

        assert_eq!(repaired.len(), 1);
        assert!(
            std::str::from_utf8(&repaired[0])
                .unwrap()
                .contains(r#""phase":"final""#)
        );
    }

    #[test]
    fn should_reject_malformed_sse_json_frame_before_commit() {
        let mut repair = SseRepair::default();

        let error = repair.observe_frame("data: {not-json}\n\n").unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_GATEWAY);
        assert_eq!(error.code, ErrorCode::InvalidJson);
        assert!(error.message.contains("malformed SSE JSON frame"));
    }

    #[test]
    fn should_passthrough_unknown_sse_events_without_repair() {
        let mut repair = SseRepair::default();
        let frame = "event: response.custom\nid: custom-1\ndata: {\"type\":\"response.custom\",\"x\":true}\n\n";

        let event = repair.observe_frame(frame).unwrap();

        assert_eq!(event, SseRepairEvent::Passthrough(frame.to_string()));
    }

    #[test]
    fn should_passthrough_unknown_sse_events_without_json_parsing() {
        let mut repair = SseRepair::default();
        let frame = "event: response.custom\nid: custom-1\ndata: opaque-extension-payload\n\n";

        let event = repair.observe_frame(frame).unwrap();

        assert_eq!(event, SseRepairEvent::Passthrough(frame.to_string()));
    }
}
