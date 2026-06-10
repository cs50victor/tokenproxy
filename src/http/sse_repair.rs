use bytes::Bytes;
use serde_json::Value;

use axum::http::StatusCode;

use crate::error::{ErrorCode, TokenproxyError};

// Bounds a stream that never produces a frame delimiter so one connection
// cannot buffer unbounded memory; generous next to the 10 MiB default
// server.max_body_bytes.
const MAX_PENDING_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Default)]
pub struct SseRepair {
    output_items: Vec<Value>,
    pending: Vec<u8>,
}

impl SseRepair {
    pub fn observe_chunk(&mut self, chunk: &[u8]) -> Result<Vec<Bytes>, TokenproxyError> {
        if self.pending.len() + chunk.len() > MAX_PENDING_BYTES {
            return Err(TokenproxyError::new(
                StatusCode::BAD_GATEWAY,
                ErrorCode::UpstreamFailure,
                "SSE stream exceeded frame buffer limit without a frame boundary",
            ));
        }
        self.pending.extend_from_slice(chunk);

        let mut repaired = Vec::new();
        while let Some(index) = find_frame_boundary(&self.pending) {
            let frame_bytes: Vec<u8> = self.pending.drain(..index + 2).collect();
            // Validate UTF-8 per complete frame: a multi-byte character split
            // across transport chunks must not abort a healthy stream.
            let frame = String::from_utf8(frame_bytes).map_err(|error| {
                TokenproxyError::new(
                    StatusCode::BAD_GATEWAY,
                    ErrorCode::InvalidJson,
                    format!("SSE frame is not UTF-8: {error}"),
                )
            })?;
            repaired.push(Bytes::from(self.observe_frame(frame)?));
        }

        Ok(repaired)
    }

    fn observe_frame(&mut self, frame: String) -> Result<String, TokenproxyError> {
        let event_type = sse_event_type(&frame);
        let Some(data) = frame
            .lines()
            .find_map(|line| line.strip_prefix("data:").map(str::trim))
        else {
            return Ok(frame);
        };
        if data == "[DONE]" {
            return Ok(frame);
        }
        if event_type.is_some_and(|event_type| {
            event_type != "response.output_item.done" && event_type != "response.completed"
        }) {
            return Ok(frame);
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
            return Ok(frame);
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
                return Ok(repair_data_line(&frame, &repaired_json));
            }
        }

        Ok(frame)
    }
}

fn find_frame_boundary(buffer: &[u8]) -> Option<usize> {
    buffer.windows(2).position(|window| window == b"\n\n")
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
data: {"type":"response.output_item.done","item":{"type":"message","phase":"final","content":"ok"}}"#.to_string(),
            )
            .unwrap();

        let repaired = repair
            .observe_frame(
                r#"data: {"type":"response.completed","response":{"id":"resp_1"}}"#.to_string(),
            )
            .unwrap();

        assert!(repaired.contains(r#""phase":"final""#));
    }

    #[test]
    fn should_preserve_sse_metadata_lines_when_repairing_completed_output() {
        let mut repair = SseRepair::default();
        repair
            .observe_frame(
                r#"event: response.output_item.done
id: item-1
data: {"type":"response.output_item.done","item":{"type":"message","phase":"final","content":"ok"}}"#.to_string(),
            )
            .unwrap();

        let repaired = repair
            .observe_frame(
                r#"event: response.completed
id: completed-1
retry: 1000
data: {"type":"response.completed","response":{"id":"resp_1"}}"#
                    .to_string(),
            )
            .unwrap();

        assert!(repaired.starts_with("event: response.completed\nid: completed-1\nretry: 1000\n"));
        assert!(
            repaired.contains(r#""output":[{"content":"ok","phase":"final","type":"message"}]"#)
        );
        assert!(repaired.ends_with("\n\n"));
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
    fn should_accept_multibyte_utf8_split_across_chunk_boundary() {
        let mut repair = SseRepair::default();
        let frame = "data: {\"type\":\"response.custom\",\"text\":\"h\u{e9}llo\"}\n\n".as_bytes();
        let split_at = frame.iter().position(|byte| *byte == 0xc3).unwrap() + 1;
        let (first, second) = frame.split_at(split_at);

        assert!(repair.observe_chunk(first).unwrap().is_empty());
        let frames = repair.observe_chunk(second).unwrap();

        assert_eq!(frames.len(), 1);
        assert!(
            std::str::from_utf8(&frames[0])
                .unwrap()
                .contains("h\u{e9}llo")
        );
    }

    #[test]
    fn should_fail_when_pending_buffer_exceeds_cap_without_frame_boundary() {
        let mut repair = SseRepair::default();
        let chunk = vec![b'a'; 1024 * 1024];
        for _ in 0..16 {
            repair.observe_chunk(&chunk).unwrap();
        }

        let error = repair.observe_chunk(&chunk).unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_GATEWAY);
        assert_eq!(error.code, ErrorCode::UpstreamFailure);
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

        let error = repair
            .observe_frame("data: {not-json}\n\n".to_string())
            .unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_GATEWAY);
        assert_eq!(error.code, ErrorCode::InvalidJson);
        assert!(error.message.contains("malformed SSE JSON frame"));
    }

    #[test]
    fn should_passthrough_unknown_sse_events_without_repair() {
        let mut repair = SseRepair::default();
        let frame = "event: response.custom\nid: custom-1\ndata: {\"type\":\"response.custom\",\"x\":true}\n\n";

        let event = repair.observe_frame(frame.to_string()).unwrap();

        assert_eq!(event, frame);
    }

    #[test]
    fn should_passthrough_unknown_sse_events_without_json_parsing() {
        let mut repair = SseRepair::default();
        let frame = "event: response.custom\nid: custom-1\ndata: opaque-extension-payload\n\n";

        let event = repair.observe_frame(frame.to_string()).unwrap();

        assert_eq!(event, frame);
    }
}
