use std::collections::BTreeMap;

use bytes::Bytes;
use serde_json::{Value, json};
use uuid::Uuid;

use super::request::{PreparedRequest, ToolName};
use super::upstream_error;
use crate::error::TokenproxyError;

pub(crate) struct ResponseConverter {
    id: String,
    model: Value,
    created_at: i64,
    names: BTreeMap<String, ToolName>,
    output: Vec<Value>,
    text_index: Option<usize>,
    reasoning_index: Option<usize>,
    tool_indices: BTreeMap<u64, usize>,
    tool_names: BTreeMap<u64, String>,
    announced_tools: BTreeMap<u64, usize>,
    finish_reason: Option<String>,
    usage: Value,
    sequence: u64,
    started: bool,
    finished: bool,
    received: usize,
    limit: usize,
}

impl ResponseConverter {
    pub(crate) fn new(request: PreparedRequest, limit: usize) -> Self {
        Self {
            id: format!("resp_{}", Uuid::new_v4().simple()),
            model: request.body["model"].clone(),
            created_at: chrono::Utc::now().timestamp(),
            names: request.tools,
            output: Vec::new(),
            text_index: None,
            reasoning_index: None,
            tool_indices: BTreeMap::new(),
            tool_names: BTreeMap::new(),
            announced_tools: BTreeMap::new(),
            finish_reason: None,
            usage: Value::Null,
            sequence: 0,
            started: false,
            finished: false,
            received: 0,
            limit,
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.finished
    }

    pub(crate) fn event(&mut self, data: &str) -> Result<Vec<Bytes>, TokenproxyError> {
        if self.finished {
            return Err(upstream_error("Cerebras sent data after completion"));
        }
        self.received = self.received.saturating_add(data.len());
        if self.received > self.limit {
            return Err(upstream_error("Cerebras response exceeds max_body_bytes"));
        }
        let events = if data.trim() == "[DONE]" {
            self.complete()?
        } else {
            let chunk: Value = serde_json::from_str(data)
                .map_err(|_| upstream_error("invalid Cerebras SSE JSON"))?;
            self.chunk(&chunk)?
        };
        let mut frames = Vec::with_capacity(events.len());
        for mut event in events {
            event["sequence_number"] = json!(self.sequence);
            self.sequence += 1;
            frames.push(Bytes::from(format!(
                "event: {}\ndata: {}\n\n",
                event["type"].as_str().unwrap_or("error"),
                event
            )));
        }
        Ok(frames)
    }

    pub(crate) fn json(mut self, body: Value) -> Result<Value, TokenproxyError> {
        let choices = body["choices"]
            .as_array()
            .ok_or_else(|| upstream_error("Cerebras response lacks choices"))?;
        if choices.len() != 1 {
            return Err(upstream_error("Cerebras response must contain one choice"));
        }
        let choice = &choices[0];
        let chunk = json!({"choices":[{"index":0, "delta":choice["message"], "finish_reason":choice["finish_reason"]}], "usage":body["usage"]});
        let mut chunk = chunk;
        if let Some(calls) = chunk
            .pointer_mut("/choices/0/delta/tool_calls")
            .and_then(Value::as_array_mut)
        {
            for (index, call) in calls.iter_mut().enumerate() {
                call["index"] = json!(index);
            }
        }
        self.chunk(&chunk)?;
        self.complete()?;
        Ok(self.response())
    }

    fn chunk(&mut self, chunk: &Value) -> Result<Vec<Value>, TokenproxyError> {
        if chunk.get("error").is_some() {
            return Err(upstream_error(format!(
                "Cerebras stream error: {}",
                chunk["error"]
            )));
        }
        let choices = chunk["choices"]
            .as_array()
            .ok_or_else(|| upstream_error("Cerebras chunk lacks choices"))?;
        if choices.len() > 1 {
            return Err(upstream_error("Cerebras returned multiple choices"));
        }
        let mut events = Vec::new();
        if let Some(choice) = choices.first() {
            if choice["index"].as_u64() != Some(0) {
                return Err(upstream_error("invalid Cerebras choice index"));
            }
            let delta = choice["delta"]
                .as_object()
                .ok_or_else(|| upstream_error("Cerebras chunk lacks delta"))?;
            if self.finish_reason.is_some() && !delta.is_empty() {
                return Err(upstream_error("Cerebras sent output after finish_reason"));
            }
            if !self.started {
                self.started = true;
                events.push(json!({"type":"response.created", "response":self.response()}));
                events.push(json!({"type":"response.in_progress", "response":self.response()}));
            }
            if delta.get("refusal").is_some_and(|v| !v.is_null()) {
                return Err(upstream_error(
                    "Cerebras returned an unsupported refusal payload",
                ));
            }
            for (key, reasoning) in [
                ("reasoning", true),
                ("reasoning_content", true),
                ("content", false),
            ] {
                if let Some(value) = delta.get(key).filter(|v| !v.is_null()) {
                    let text = value
                        .as_str()
                        .ok_or_else(|| upstream_error("invalid Cerebras text delta"))?;
                    if !text.is_empty() {
                        self.text_delta(text, reasoning, &mut events);
                    }
                }
            }
            if let Some(calls) = delta.get("tool_calls").filter(|v| !v.is_null()) {
                for call in calls
                    .as_array()
                    .ok_or_else(|| upstream_error("invalid Cerebras tool_calls"))?
                {
                    self.tool_delta(call, &mut events)?;
                }
            }
            if let Some(reason) = choice.get("finish_reason").filter(|v| !v.is_null()) {
                let reason = reason
                    .as_str()
                    .ok_or_else(|| upstream_error("invalid Cerebras finish_reason"))?;
                if !matches!(reason, "stop" | "tool_calls" | "length" | "content_filter") {
                    return Err(upstream_error(format!(
                        "unsupported Cerebras finish_reason: {reason}"
                    )));
                }
                self.finish_reason = Some(reason.into());
            }
        }
        if let Some(usage) = chunk.get("usage").filter(|v| !v.is_null()) {
            self.usage = json!({
                "input_tokens":usage["prompt_tokens"], "output_tokens":usage["completion_tokens"], "total_tokens":usage["total_tokens"],
                "input_tokens_details":{"cached_tokens":usage.pointer("/prompt_tokens_details/cached_tokens").and_then(Value::as_u64).unwrap_or(0)},
                "output_tokens_details":{"reasoning_tokens":usage.pointer("/completion_tokens_details/reasoning_tokens").and_then(Value::as_u64).unwrap_or(0)}
            });
        }
        Ok(events)
    }

    fn text_delta(&mut self, delta: &str, reasoning: bool, events: &mut Vec<Value>) {
        let existing = if reasoning {
            self.reasoning_index
        } else {
            self.text_index
        };
        let index = existing.unwrap_or_else(|| {
            let index = self.output.len();
            let id = format!("{}_{}", if reasoning {"rs"} else {"msg"}, Uuid::new_v4().simple());
            let item = if reasoning {
                json!({"id":id, "type":"reasoning", "summary":[]})
            } else {
                json!({"id":id, "type":"message", "role":"assistant", "status":"in_progress", "content":[]})
            };
            events.push(json!({"type":"response.output_item.added", "output_index":index, "item":item}));
            let part = if reasoning {json!({"type":"summary_text", "text":""})} else {json!({"type":"output_text", "text":"", "annotations":[]})};
            if reasoning {
                events.push(json!({"type":"response.reasoning_summary_part.added", "item_id":id, "output_index":index, "summary_index":0, "part":part}));
                self.reasoning_index = Some(index);
            } else {
                events.push(json!({"type":"response.content_part.added", "item_id":id, "output_index":index, "content_index":0, "part":part}));
                self.text_index = Some(index);
            }
            self.output.push(item);
            self.output[index][if reasoning {"summary"} else {"content"}] = json!([part]);
            index
        });
        let key = if reasoning { "summary" } else { "content" };
        if let Some(Value::String(text)) = self.output[index].pointer_mut(&format!("/{key}/0/text"))
        {
            text.push_str(delta);
        }
        let mut event = json!({"type":if reasoning {"response.reasoning_summary_text.delta"} else {"response.output_text.delta"}, "item_id":self.output[index]["id"], "output_index":index, "delta":delta});
        event[if reasoning {
            "summary_index"
        } else {
            "content_index"
        }] = json!(0);
        events.push(event);
    }

    fn tool_delta(&mut self, call: &Value, events: &mut Vec<Value>) -> Result<(), TokenproxyError> {
        let tool_index = call["index"]
            .as_u64()
            .ok_or_else(|| upstream_error("Cerebras tool delta lacks index"))?;
        let index = *self.tool_indices.entry(tool_index).or_insert_with(|| {
            let index = self.output.len();
            self.output.push(json!({"id":format!("fc_{}", Uuid::new_v4().simple()), "type":"function_call", "status":"in_progress", "call_id":"", "name":"", "arguments":""}));
            index
        });
        let announced = self.announced_tools.contains_key(&tool_index);
        if let Some(id) = call.get("id").filter(|v| !v.is_null()) {
            let id = id
                .as_str()
                .ok_or_else(|| upstream_error("invalid Cerebras tool call ID"))?;
            if announced && !id.is_empty() {
                return Err(upstream_error("Cerebras changed an announced tool call ID"));
            }
            if let Some(Value::String(existing)) = self.output[index].get_mut("call_id") {
                existing.push_str(id);
            }
        }
        if let Some(name) = call.pointer("/function/name").filter(|v| !v.is_null()) {
            let name = name
                .as_str()
                .ok_or_else(|| upstream_error("invalid Cerebras function name"))?;
            if announced && !name.is_empty() {
                return Err(upstream_error(
                    "Cerebras changed an announced function name",
                ));
            }
            self.tool_names
                .entry(tool_index)
                .or_default()
                .push_str(name);
        }
        if let Some(arguments) = call.pointer("/function/arguments").filter(|v| !v.is_null()) {
            let delta = arguments
                .as_str()
                .ok_or_else(|| upstream_error("invalid Cerebras function arguments"))?;
            if let Some(Value::String(args)) = self.output[index].get_mut("arguments") {
                args.push_str(delta);
            }
        }
        self.announce_tool(tool_index, false, events)
    }

    fn announce_tool(
        &mut self,
        tool_index: u64,
        final_chunk: bool,
        events: &mut Vec<Value>,
    ) -> Result<(), TokenproxyError> {
        let index = self.tool_indices[&tool_index];
        let name = self
            .tool_names
            .get(&tool_index)
            .map(String::as_str)
            .unwrap_or_default();
        let known = self.names.get(name);
        let has_id = self.output[index]["call_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty());
        if known.is_none() || !has_id {
            if final_chunk {
                return Err(upstream_error(
                    "Cerebras returned an unknown or incomplete tool identity",
                ));
            }
            return Ok(());
        }
        if !final_chunk
            && self
                .names
                .keys()
                .any(|candidate| candidate != name && candidate.starts_with(name))
        {
            return Ok(());
        }
        if let Some(original) = known {
            self.output[index]["name"] = json!(original.name);
            if let Some(namespace) = &original.namespace {
                self.output[index]["namespace"] = json!(namespace);
            }
            if original.namespace.as_deref() == Some("collaboration")
                && matches!(
                    original.name.as_str(),
                    "spawn_agent" | "send_message" | "followup_task"
                )
            {
                self.output[index]["encrypted_function_args"] = json!([]);
            }
        }
        let sent = self.announced_tools.entry(tool_index).or_insert_with(|| {
            let mut item = self.output[index].clone();
            item["arguments"] = json!("");
            events.push(
                json!({"type":"response.output_item.added", "output_index":index, "item":item}),
            );
            0
        });
        let arguments = self.output[index]["arguments"].as_str().unwrap_or_default();
        if arguments.len() > *sent {
            events.push(json!({"type":"response.function_call_arguments.delta", "item_id":self.output[index]["id"], "output_index":index, "delta":&arguments[*sent..]}));
            *sent = arguments.len();
        }
        Ok(())
    }

    fn complete(&mut self) -> Result<Vec<Value>, TokenproxyError> {
        if !self.started || self.finish_reason.is_none() {
            return Err(upstream_error(
                "Cerebras stream ended without a finish_reason",
            ));
        }
        let incomplete = matches!(
            self.finish_reason.as_deref(),
            Some("length" | "content_filter")
        );
        let mut events = Vec::new();
        if !incomplete {
            let tool_indices: Vec<_> = self.tool_indices.keys().copied().collect();
            for tool_index in tool_indices {
                self.announce_tool(tool_index, true, &mut events)?;
            }
        }
        for (index, item) in self.output.iter_mut().enumerate() {
            match item["type"].as_str() {
                Some("function_call") => {
                    if incomplete {
                        item["status"] = json!("incomplete");
                        continue;
                    }
                    if item["call_id"].as_str().is_none_or(str::is_empty)
                        || item["name"].as_str().is_none_or(str::is_empty)
                    {
                        return Err(upstream_error(
                            "Cerebras returned an incomplete tool identity",
                        ));
                    }
                    item["status"] = json!(if incomplete {
                        "incomplete"
                    } else {
                        "completed"
                    });
                    events.push(json!({"type":"response.function_call_arguments.done", "item_id":item["id"], "output_index":index, "arguments":item["arguments"]}));
                }
                Some("message") => {
                    item["status"] = json!(if incomplete {
                        "incomplete"
                    } else {
                        "completed"
                    });
                    events.push(json!({"type":"response.output_text.done", "item_id":item["id"], "output_index":index, "content_index":0, "text":item["content"][0]["text"]}));
                    events.push(json!({"type":"response.content_part.done", "item_id":item["id"], "output_index":index, "content_index":0, "part":item["content"][0]}));
                }
                Some("reasoning") => {
                    events.push(json!({"type":"response.reasoning_summary_text.done", "item_id":item["id"], "output_index":index, "summary_index":0, "text":item["summary"][0]["text"]}));
                    events.push(json!({"type":"response.reasoning_summary_part.done", "item_id":item["id"], "output_index":index, "summary_index":0, "part":item["summary"][0]}));
                }
                _ => {}
            }
            events.push(
                json!({"type":"response.output_item.done", "output_index":index, "item":item}),
            );
        }
        self.finished = true;
        events.push(json!({"type":if incomplete {"response.incomplete"} else {"response.completed"}, "response":self.response()}));
        Ok(events)
    }

    fn response(&self) -> Value {
        let incomplete_reason = if self.finished {
            match self.finish_reason.as_deref() {
                Some("length") => Some("max_output_tokens"),
                Some("content_filter") => Some("content_filter"),
                _ => None,
            }
        } else {
            None
        };
        let status = if !self.finished {
            "in_progress"
        } else if incomplete_reason.is_some() {
            "incomplete"
        } else {
            "completed"
        };
        json!({"id":self.id, "object":"response", "created_at":self.created_at, "status":status, "model":self.model,
            "output":self.output, "usage":self.usage, "error":null, "incomplete_details":incomplete_reason.map(|reason| json!({"reason":reason})), "store":false})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cerebras::prepare;

    fn converter() -> ResponseConverter {
        ResponseConverter::new(
            prepare(json!({"model":"qwen", "input":"hi", "tools":[
                {"type":"namespace","name":"functions","tools":[{"type":"function","name":"run"}]}
            ]}))
            .unwrap(),
            65536,
        )
    }

    fn events(converter: &mut ResponseConverter, data: Value) -> Vec<Value> {
        converter
            .event(&data.to_string())
            .unwrap()
            .iter()
            .map(|frame| {
                let text = std::str::from_utf8(frame).unwrap();
                serde_json::from_str(
                    text.lines()
                        .find_map(|line| line.strip_prefix("data: "))
                        .unwrap(),
                )
                .unwrap()
            })
            .collect()
    }

    #[test]
    fn should_mark_collaboration_messages_as_plaintext() {
        for name in ["spawn_agent", "send_message", "followup_task"] {
            let mut c = ResponseConverter::new(prepare(json!({"model":"qwen","input":"hi", "tools":[
                {"type":"namespace","name":"collaboration","tools":[{"type":"function","name":name}]}
            ]})).unwrap(), 65536);
            let all = events(
                &mut c,
                json!({"choices":[{"index":0,"delta":{"tool_calls":[
                {"index":0,"id":"a","function":{"name":format!("collaboration__{name}"),"arguments":"{\"message\":\"hello\"}"}}
            ]},"finish_reason":"tool_calls"}]}),
            );
            let added = all
                .iter()
                .find(|event| event["type"] == "response.output_item.added")
                .unwrap();
            assert_eq!(added["item"]["encrypted_function_args"], json!([]));
            c.event("[DONE]").unwrap();
            assert_eq!(c.output[0]["encrypted_function_args"], json!([]));
        }
    }

    #[test]
    fn should_stream_reasoning_text_and_interleaved_tool_arguments_with_usage() {
        let mut c = converter();
        let mut all = events(
            &mut c,
            json!({"choices":[{"index":0,"delta":{"reasoning":"plan"}}]}),
        );
        all.extend(events(
            &mut c,
            json!({"choices":[{"index":0,"delta":{"content":"hi", "tool_calls":[
                {"index":0,"id":"a","function":{"name":"functions__run","arguments":"{\"x\":"}},
                {"index":1,"id":"b","function":{"name":"functions__run","arguments":"{}"}}
            ]}}]}),
        ));
        all.extend(events(&mut c, json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}}]},"finish_reason":"tool_calls"}]})));
        all.extend(events(&mut c, json!({"choices":[],"usage":{"prompt_tokens":10,"completion_tokens":8,"total_tokens":18,"prompt_tokens_details":{"cached_tokens":3},"completion_tokens_details":{"reasoning_tokens":2}}})));
        let frames = c.event("[DONE]").unwrap();
        for frame in frames {
            let text = std::str::from_utf8(&frame).unwrap();
            all.push(
                serde_json::from_str(
                    text.lines()
                        .find_map(|line| line.strip_prefix("data: "))
                        .unwrap(),
                )
                .unwrap(),
            );
        }
        let response = &all.last().unwrap()["response"];
        assert_eq!(response["status"], "completed");
        assert_eq!(response["output"][0]["summary"][0]["text"], "plan");
        assert_eq!(response["output"][1]["content"][0]["text"], "hi");
        assert_eq!(response["output"][2]["arguments"], "{\"x\":1}");
        assert_eq!(response["output"][2]["namespace"], "functions");
        assert_eq!(response["output"][2]["name"], "run");
        assert_eq!(response["output"][3]["call_id"], "b");
        assert_eq!(
            response["usage"]["output_tokens_details"]["reasoning_tokens"],
            2
        );
        assert_eq!(
            response["usage"]["input_tokens_details"]["cached_tokens"],
            3
        );
        for (sequence, event) in all.iter().enumerate() {
            assert_eq!(event["sequence_number"], sequence);
        }
        for index in 0..4 {
            let added = all
                .iter()
                .position(|v| {
                    v["type"] == "response.output_item.added" && v["output_index"] == index
                })
                .unwrap();
            let done = all
                .iter()
                .position(|v| {
                    v["type"] == "response.output_item.done" && v["output_index"] == index
                })
                .unwrap();
            assert!(added < done);
            assert_eq!(all[added]["item"]["id"], all[done]["item"]["id"]);
        }
    }

    #[test]
    fn should_convert_nonstream_tool_response() {
        let response = converter().json(json!({"choices":[{"message":{"role":"assistant","content":null,"reasoning":"plan","tool_calls":[
            {"id":"call_a","type":"function","function":{"name":"functions__run","arguments":"{}"}}
        ]},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}})).unwrap();
        assert_eq!(response["output"][1]["call_id"], "call_a");
        assert_eq!(response["output"][1]["namespace"], "functions");
        assert_eq!(response["usage"]["total_tokens"], 7);
    }

    #[test]
    fn should_mark_token_limit_and_content_filter_as_incomplete() {
        for (finish, reason) in [
            ("length", "max_output_tokens"),
            ("content_filter", "content_filter"),
        ] {
            let response = converter()
                .json(json!({"choices":[{"message":{"content":"partial"},"finish_reason":finish}]}))
                .unwrap();
            assert_eq!(response["status"], "incomplete");
            assert_eq!(response["output"][0]["status"], "incomplete");
            assert_eq!(response["incomplete_details"]["reason"], reason);
        }
    }

    #[test]
    fn should_buffer_fragmented_tool_identity_before_announcing_it() {
        let mut c = converter();
        let first = events(
            &mut c,
            json!({"choices":[{"index":0,"delta":{"tool_calls":[
                {"index":0,"id":"call_","function":{"name":"functions__","arguments":"{"}}
            ]}}]}),
        );
        assert!(
            !first
                .iter()
                .any(|e| e["type"] == "response.output_item.added")
        );
        let second = events(
            &mut c,
            json!({"choices":[{"index":0,"delta":{"tool_calls":[
            {"index":0,"id":"1","function":{"name":"run","arguments":"}"}}
        ]},"finish_reason":"tool_calls"}]}),
        );
        let item = &second
            .iter()
            .find(|e| e["type"] == "response.output_item.added")
            .unwrap()["item"];
        assert_eq!(item["call_id"], "call_1");
        assert_eq!(item["name"], "run");
        assert_eq!(item["namespace"], "functions");
        assert_eq!(second.last().unwrap()["delta"], "{}");
        assert!(c.event("[DONE]").is_ok());
    }

    #[test]
    fn should_never_signal_an_executable_tool_when_generation_is_incomplete() {
        for reason in ["length", "content_filter"] {
            let mut c = converter();
            events(
                &mut c,
                json!({"choices":[{"index":0,"delta":{"tool_calls":[
                {"index":0,"id":"a","function":{"name":"functions__run","arguments":"{}"}}
            ]},"finish_reason":reason}]}),
            );
            let frames = c.event("[DONE]").unwrap();
            let text = String::from_utf8(frames.concat()).unwrap();
            assert!(text.contains("response.incomplete"));
            assert!(!text.contains("response.output_item.done"));
            assert!(!text.contains("response.function_call_arguments.done"));
        }
    }

    #[test]
    fn should_reject_refusal_payloads_instead_of_silently_losing_them() {
        assert!(converter().json(json!({"choices":[{"message":{"content":null,"refusal":"refused"},"finish_reason":"stop"}]})).is_err());
    }

    #[test]
    fn should_reject_missing_finish_reason_malformed_chunks_and_oversized_output() {
        assert!(converter().event("[DONE]").is_err());
        assert!(converter().event("not json").is_err());
        assert!(
            converter()
                .event(r#"{"error":{"message":"failed"}}"#)
                .is_err()
        );
        let mut c = converter();
        c.limit = 10;
        assert!(
            c.event(r#"{"choices":[{"index":0,"delta":{"content":"too long"}}]}"#)
                .is_err()
        );
    }
}
