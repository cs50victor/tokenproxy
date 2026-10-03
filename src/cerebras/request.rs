use std::collections::BTreeMap;

use serde_json::{Map, Value, json};

use super::invalid;
use crate::error::TokenproxyError;

#[derive(Clone, Debug)]
pub(super) struct ToolName {
    pub name: String,
    pub namespace: Option<String>,
}

pub(crate) struct PreparedRequest {
    pub body: Value,
    pub(super) tools: BTreeMap<String, ToolName>,
}

pub(crate) fn prepare(request: Value) -> Result<PreparedRequest, TokenproxyError> {
    let object = request
        .as_object()
        .ok_or_else(|| invalid("expected a Responses object"))?;
    for field in ["previous_response_id", "conversation"] {
        if object.get(field).is_some_and(|v| !v.is_null()) {
            return Err(invalid(format!(
                "Cerebras does not support {field}; send the full input history"
            )));
        }
    }
    for field in ["store", "background"] {
        if object
            .get(field)
            .is_some_and(|v| !v.is_null() && v != false)
        {
            return Err(invalid(format!("Cerebras requires {field}=false")));
        }
    }
    if object.get("max_tool_calls").is_some_and(|v| !v.is_null()) {
        return Err(invalid("Cerebras does not support max_tool_calls"));
    }
    if object
        .get("truncation")
        .is_some_and(|v| !v.is_null() && v != "disabled")
    {
        return Err(invalid(
            "Cerebras does not support automatic input truncation",
        ));
    }
    let mut body = Map::new();
    body.insert(
        "model".into(),
        Value::String(string(&request, "model")?.into()),
    );
    for field in [
        "stream",
        "temperature",
        "top_p",
        "parallel_tool_calls",
        "prompt_cache_key",
    ] {
        if let Some(value) = object.get(field).filter(|v| !v.is_null()) {
            body.insert(field.into(), value.clone());
        }
    }
    if let Some(value) = object.get("max_output_tokens").filter(|v| !v.is_null()) {
        body.insert("max_completion_tokens".into(), value.clone());
    }
    if let Some(effort) = request
        .pointer("/reasoning/effort")
        .filter(|v| !v.is_null())
    {
        body.insert("reasoning_effort".into(), effort.clone());
    }
    if let Some(format) = request.pointer("/text/format").filter(|v| !v.is_null()) {
        match format["type"].as_str() {
            Some("text") => {}
            Some("json_object") => {
                body.insert("response_format".into(), json!({"type":"json_object"}));
            }
            Some("json_schema") => {
                let mut schema = format
                    .as_object()
                    .cloned()
                    .ok_or_else(|| invalid("invalid text.format"))?;
                schema.remove("type");
                body.insert(
                    "response_format".into(),
                    json!({"type":"json_schema", "json_schema":schema}),
                );
            }
            _ => return Err(invalid("unsupported Cerebras text.format")),
        }
    }

    let mut names = BTreeMap::new();
    let mut tools = Vec::new();
    if let Some(value) = object.get("tools").filter(|v| !v.is_null()) {
        for tool in value
            .as_array()
            .ok_or_else(|| invalid("tools must be an array"))?
        {
            if tool["type"] == "namespace" {
                let namespace = string(tool, "name")?;
                let members = tool["tools"]
                    .as_array()
                    .ok_or_else(|| invalid("namespace tools must be an array"))?;
                for member in members {
                    tools.push(function_tool(
                        member,
                        Some(namespace),
                        tool["description"].as_str(),
                        &mut names,
                    )?);
                }
            } else {
                tools.push(function_tool(tool, None, None, &mut names)?);
            }
        }
    }
    let has_tools = !tools.is_empty();
    if has_tools {
        body.insert("tools".into(), Value::Array(tools));
    }
    if let Some(choice) = object.get("tool_choice").filter(|v| !v.is_null()) {
        let choice = if choice.is_string() {
            if !matches!(choice.as_str(), Some("auto" | "none" | "required")) {
                return Err(invalid("unsupported Cerebras tool_choice"));
            }
            choice.clone()
        } else if choice["type"] == "function" {
            json!({"type":"function", "function":{"name":resolve_alias(string(choice, "name")?, choice["namespace"].as_str(), &names, false)?}})
        } else {
            return Err(invalid("Cerebras supports only function tool_choice"));
        };
        if has_tools {
            body.insert("tool_choice".into(), choice);
        } else if choice != "auto" && choice != "none" {
            return Err(invalid("tool_choice requires declared function tools"));
        }
    }

    let mut instructions = Vec::new();
    if let Some(value) = object.get("instructions").filter(|v| !v.is_null()) {
        instructions.push(
            value
                .as_str()
                .ok_or_else(|| invalid("instructions must be text"))?
                .to_string(),
        );
    }
    let mut messages: Vec<Value> = Vec::new();
    let mut reasoning = String::new();
    match object.get("input") {
        Some(Value::String(input)) => messages.push(json!({"role":"user", "content":input})),
        Some(Value::Array(input)) => {
            let mut agent_message_boundary = false;
            for item in input {
                let follows_agent_message = agent_message_boundary;
                match item["type"].as_str().unwrap_or("message") {
                    "agent_message" => {
                        if !reasoning.is_empty() {
                            messages.push(json!({"role":"assistant", "content":"", "reasoning":std::mem::take(&mut reasoning)}));
                        }
                        let author = string(item, "author")?;
                        let recipient = string(item, "recipient")?;
                        let content = item["content"]
                            .as_array()
                            .ok_or_else(|| invalid("agent message content must be an array"))?
                            .iter()
                            .map(|part| {
                                if part["type"] != "input_text" {
                                    return Err(invalid("Cerebras requires plaintext agent messages; encrypted task messages cannot cross providers"));
                                }
                                string(part, "text")
                            })
                            .collect::<Result<Vec<_>, _>>()?
                            .join("\n");
                        messages.push(json!({"role":"assistant", "content":format!("Message from {author} to {recipient}:\n{content}")}));
                        agent_message_boundary = true;
                    }
                    "message" => {
                        let role = string(item, "role")?;
                        if matches!(role, "system" | "developer") {
                            instructions.push(text_content(&item["content"])?);
                            continue;
                        }
                        if !matches!(role, "user" | "assistant") {
                            return Err(invalid(format!(
                                "unsupported Cerebras input role: {role}"
                            )));
                        }
                        if role == "user" && !reasoning.is_empty() {
                            return Err(invalid("reasoning cannot cross a user message"));
                        }
                        let content = message_content(&item["content"], role == "user")?;
                        let mut message = json!({"role":role, "content":content});
                        if role == "assistant" && !reasoning.is_empty() {
                            message["reasoning"] = Value::String(std::mem::take(&mut reasoning));
                        }
                        messages.push(message);
                        agent_message_boundary = false;
                    }
                    "function_call" => {
                        agent_message_boundary = false;
                        let name = resolve_alias(
                            string(item, "name")?,
                            item["namespace"].as_str(),
                            &names,
                            true,
                        )?;
                        let call = json!({"id":string(item, "call_id")?, "type":"function", "function":{
                            "name":name, "arguments":string(item, "arguments")?
                        }});
                        if let Some(previous) = messages.last_mut().filter(|m| {
                            !follows_agent_message
                                && m["role"] == "assistant"
                                && (reasoning.is_empty() || m.get("reasoning").is_none())
                        }) {
                            if !reasoning.is_empty() {
                                previous["reasoning"] =
                                    Value::String(std::mem::take(&mut reasoning));
                            }
                            if previous.get("tool_calls").is_none() {
                                previous["tool_calls"] = json!([]);
                            }
                            if let Some(calls) = previous["tool_calls"].as_array_mut() {
                                calls.push(call);
                            }
                        } else {
                            let mut message =
                                json!({"role":"assistant", "content":null, "tool_calls":[call]});
                            if !reasoning.is_empty() {
                                message["reasoning"] =
                                    Value::String(std::mem::take(&mut reasoning));
                            }
                            messages.push(message);
                        }
                    }
                    "function_call_output" => {
                        agent_message_boundary = false;
                        if !reasoning.is_empty() {
                            return Err(invalid(
                                "reasoning must precede an assistant message or function call",
                            ));
                        }
                        messages.push(json!({"role":"tool", "tool_call_id":string(item, "call_id")?, "content":text_content(&item["output"])?}));
                    }
                    "reasoning" => {
                        let content = item
                            .get("content")
                            .filter(|v| !v.is_null())
                            .map(text_content)
                            .transpose()?
                            .unwrap_or_default();
                        let text = if content.is_empty() {
                            item.get("summary")
                                .filter(|v| !v.is_null())
                                .map(text_content)
                                .transpose()?
                                .unwrap_or_default()
                        } else {
                            content
                        };
                        if text.trim().is_empty()
                            && item.get("encrypted_content").is_some_and(|v| !v.is_null())
                        {
                            return Err(invalid(
                                "Cerebras cannot replay encrypted reasoning; start a new conversation or provide plaintext history",
                            ));
                        }
                        if !reasoning.is_empty() && !text.is_empty() {
                            reasoning.push_str(
                                "

",
                            );
                        }
                        reasoning.push_str(&text);
                    }
                    kind => {
                        return Err(invalid(format!("unsupported Cerebras input item: {kind}")));
                    }
                }
            }
        }
        _ => return Err(invalid("Cerebras requires input as text or an array")),
    }
    if !reasoning.is_empty() {
        return Err(invalid(
            "reasoning must precede an assistant message or function call",
        ));
    }
    if !instructions.is_empty() {
        messages.insert(
            0,
            json!({"role":"system", "content":instructions.join("\n\n")}),
        );
    }
    body.insert("messages".into(), Value::Array(messages));
    Ok(PreparedRequest {
        body: Value::Object(body),
        tools: names,
    })
}

fn function_tool(
    tool: &Value,
    namespace: Option<&str>,
    description: Option<&str>,
    names: &mut BTreeMap<String, ToolName>,
) -> Result<Value, TokenproxyError> {
    if tool["type"] != "function" {
        return Err(invalid(
            "Cerebras supports function tools only; disable hosted tools such as web search and use function tools instead of custom grammars",
        ));
    }
    let name = string(tool, "name")?;
    let alias = alias(name, namespace)?;
    if names
        .insert(
            alias.clone(),
            ToolName {
                name: name.into(),
                namespace: namespace.map(str::to_owned),
            },
        )
        .is_some()
    {
        return Err(invalid("duplicate or colliding Cerebras function names"));
    }
    let mut function = Map::new();
    function.insert("name".into(), Value::String(alias));
    for key in ["description", "parameters", "strict"] {
        if let Some(value) = tool.get(key).filter(|v| !v.is_null()) {
            function.insert(key.into(), value.clone());
        }
    }
    if let Some(description) = description.filter(|v| !v.is_empty()) {
        let description = format!(
            "{description}\n\n{}",
            tool["description"].as_str().unwrap_or_default()
        );
        function.insert("description".into(), Value::String(description));
    }
    Ok(json!({"type":"function", "function":function}))
}

fn resolve_alias(
    name: &str,
    namespace: Option<&str>,
    names: &BTreeMap<String, ToolName>,
    allow_history: bool,
) -> Result<String, TokenproxyError> {
    let candidate = alias(name, namespace)?;
    if names.contains_key(&candidate) {
        return Ok(candidate);
    }
    if namespace.is_none() {
        let mut matches = names.iter().filter(|(_, original)| original.name == name);
        if let Some((alias, _)) = matches.next() {
            if matches.next().is_some() {
                return Err(invalid("ambiguous function name; specify its namespace"));
            }
            return Ok(alias.clone());
        }
    }
    if allow_history {
        Ok(candidate)
    } else {
        Err(invalid("tool_choice names an undeclared function"))
    }
}

fn alias(name: &str, namespace: Option<&str>) -> Result<String, TokenproxyError> {
    if name.is_empty() || namespace == Some("") {
        return Err(invalid("function and namespace names must not be empty"));
    }
    let alias = match namespace {
        Some(ns) => format!("{ns}__{name}"),
        None => name.into(),
    };
    if alias.len() <= 64
        && alias
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Ok(alias);
    }
    let hash = crate::observability::sha256_hex(alias.as_bytes());
    Ok(format!("tp_{}", &hash[..48]))
}

fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str, TokenproxyError> {
    value[key]
        .as_str()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| invalid(format!("Cerebras requires nonempty {key}")))
}

fn text_content(content: &Value) -> Result<String, TokenproxyError> {
    if let Some(text) = content.as_str() {
        return Ok(text.into());
    }
    let parts = content
        .as_array()
        .ok_or_else(|| invalid("expected text content"))?;
    let mut output = String::new();
    for part in parts {
        if !matches!(
            part["type"].as_str(),
            Some("input_text" | "output_text" | "text" | "summary_text" | "reasoning_text")
        ) {
            return Err(invalid(
                "unsupported non-text instruction, reasoning, or tool output",
            ));
        }
        output.push_str(
            part["text"]
                .as_str()
                .ok_or_else(|| invalid("text part requires text"))?,
        );
    }
    Ok(output)
}

fn message_content(content: &Value, allow_images: bool) -> Result<Value, TokenproxyError> {
    if content.is_string() {
        return Ok(content.clone());
    }
    let mut output = Vec::new();
    for part in content
        .as_array()
        .ok_or_else(|| invalid("message content must be text or an array"))?
    {
        match part["type"].as_str() {
            Some("input_text" | "output_text") => output.push(json!({"type":"text", "text":part["text"].as_str().ok_or_else(|| invalid("text part requires text"))?})),
            Some("input_image") if allow_images => {
                let mut image = json!({"url":string(part, "image_url")?});
                if let Some(detail) = part.get("detail") { image["detail"] = detail.clone(); }
                output.push(json!({"type":"image_url", "image_url":image}));
            }
            _ => return Err(invalid("Cerebras messages support text and user image URLs only")),
        }
    }
    Ok(Value::Array(output))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_preserve_instructions_and_parallel_tool_history() {
        let request = prepare(json!({"model":"qwen-3.8-27b", "instructions":"A", "input":[
            {"role":"developer", "content":[{"type":"input_text", "text":"B"}]},
            {"role":"user", "content":"question"},
            {"role":"system", "content":"C"},
            {"type":"reasoning", "summary":[{"type":"summary_text", "text":"plan"}]},
            {"type":"function_call", "call_id":"a", "name":"first", "arguments":"{}"},
            {"type":"function_call", "call_id":"b", "name":"second", "arguments":"{}"},
            {"type":"function_call_output", "call_id":"a", "output":"one"},
            {"type":"function_call_output", "call_id":"b", "output":[{"type":"input_text", "text":"two"}]}
        ]})).unwrap();
        assert_eq!(
            request.body["messages"],
            json!([
                {"role":"system", "content":"A\n\nB\n\nC"},
                {"role":"user", "content":"question"},
                {"role":"assistant", "content":null, "reasoning":"plan", "tool_calls":[
                    {"id":"a", "type":"function", "function":{"name":"first", "arguments":"{}"}},
                    {"id":"b", "type":"function", "function":{"name":"second", "arguments":"{}"}}
                ]},
                {"role":"tool", "tool_call_id":"a", "content":"one"},
                {"role":"tool", "tool_call_id":"b", "content":"two"}
            ])
        );
    }

    #[test]
    fn should_translate_parameters_without_forwarding_openai_hints() {
        let result = prepare(json!({"model":"qwen", "input":"hi", "max_output_tokens":256,
            "reasoning":{"effort":"low","summary":"detailed"}, "service_tier":"priority", "store":false,
            "metadata":{"a":"b"}, "prompt_cache_key":"key", "text":{"verbosity":"low", "format":{"type":"json_schema","name":"result","schema":{"type":"object"},"strict":true}}
        })).unwrap();
        assert_eq!(
            result.body,
            json!({"model":"qwen", "messages":[{"role":"user","content":"hi"}],
            "max_completion_tokens":256,"reasoning_effort":"low","prompt_cache_key":"key",
            "response_format":{"type":"json_schema","json_schema":{"name":"result","schema":{"type":"object"},"strict":true}}})
        );
    }

    #[test]
    fn should_translate_namespaces_and_preserve_images() {
        let result = prepare(json!({"model":"qwen", "input":[{"role":"user","content":[{"type":"input_image","image_url":"data:image/png;base64,AAAA","detail":"high"}]}],
            "tools":[{"type":"namespace","name":"functions","tools":[{"type":"function","name":"run","parameters":{"type":"object"}}]}],
            "tool_choice":{"type":"function","namespace":"functions","name":"run"}
        })).unwrap();
        assert_eq!(
            result.body["tools"][0]["function"]["name"],
            "functions__run"
        );
        assert_eq!(
            result.body["tool_choice"]["function"]["name"],
            "functions__run"
        );
        assert_eq!(
            result.body["messages"][0]["content"][0]["image_url"]["url"],
            "data:image/png;base64,AAAA"
        );
        assert_eq!(
            result.tools["functions__run"].namespace.as_deref(),
            Some("functions")
        );
    }

    #[test]
    fn should_reject_features_that_cannot_be_translated_without_data_loss() {
        for extra in [
            json!({"previous_response_id":"resp_old"}),
            json!({"conversation":"conv_old"}),
            json!({"store":true}),
            json!({"background":true}),
            json!({"truncation":"auto"}),
            json!({"tools":[{"type":"web_search"}]}),
            json!({"tools":[{"type":"custom","name":"patch"}]}),
            json!({"input":[{"type":"item_reference","id":"msg_old"}]}),
            json!({"input":[{"type":"reasoning","encrypted_content":"encrypted"}]}),
            json!({"input":[{"role":"user","content":[{"type":"input_file","file_id":"file_old"}]}]}),
        ] {
            let mut request = json!({"model":"qwen", "input":"hi"});
            request
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            assert!(prepare(request.clone()).is_err(), "accepted {request}");
        }
    }

    #[test]
    fn should_preserve_plaintext_agent_messages_between_tool_turns() {
        let converted = prepare(json!({"model":"qwen", "input":[
            {"type":"agent_message","author":"/root","recipient":"/root/child",
             "content":[{"type":"input_text","text":"Message Type: NEW_TASK"},{"type":"input_text","text":"Run the probe."}]},
            {"type":"reasoning","summary":[{"type":"summary_text","text":"Run it now"}]},
            {"type":"function_call","name":"run","call_id":"a","arguments":"{}"},
            {"type":"function_call_output","call_id":"a","output":"OK"}
        ]})).unwrap();
        let messages = &converted.body["messages"];
        assert_eq!(messages[0]["role"], "assistant");
        assert_eq!(
            messages[0]["content"],
            "Message from /root to /root/child:\nMessage Type: NEW_TASK\nRun the probe."
        );
        assert!(messages[0].get("tool_calls").is_none());
        assert!(messages[0].get("reasoning").is_none());
        assert_eq!(messages[1]["tool_calls"][0]["id"], "a");
        assert_eq!(messages[1]["reasoning"], "Run it now");
        assert_eq!(messages[2]["content"], "OK");
    }

    #[test]
    fn should_preserve_reasoning_interrupted_by_an_agent_message() {
        let converted = prepare(json!({"model":"qwen", "input":[
            {"type":"reasoning","summary":[{"type":"summary_text","text":"Waiting"}]},
            {"type":"agent_message","author":"/root/child","recipient":"/root",
             "content":[{"type":"input_text","text":"Probe succeeded"}]}
        ]}))
        .unwrap();
        assert_eq!(converted.body["messages"][0]["reasoning"], "Waiting");
        assert!(converted.body["messages"][1].get("reasoning").is_none());
    }

    #[test]
    fn should_reject_encrypted_agent_messages_without_dropping_content() {
        assert!(
            prepare(json!({"model":"qwen", "input":[
                {"type":"agent_message","author":"/root","recipient":"/root/child", "content":[
                    {"type":"input_text","text":"Header"},
                    {"type":"encrypted_content","encrypted_content":"ciphertext"}
                ]}
            ]}))
            .is_err()
        );
    }

    #[test]
    fn should_resolve_bare_tool_names_only_when_unambiguous() {
        let mut request = json!({"model":"qwen", "input":[{"type":"function_call","name":"run","call_id":"a","arguments":"{}"}],
            "tools":[{"type":"namespace","name":"functions","tools":[{"type":"function","name":"run"}]}],
            "tool_choice":{"type":"function","name":"run"}});
        let converted = prepare(request.clone()).unwrap();
        assert_eq!(
            converted.body["tool_choice"]["function"]["name"],
            "functions__run"
        );
        assert_eq!(
            converted.body["messages"][0]["tool_calls"][0]["function"]["name"],
            "functions__run"
        );
        request["tools"].as_array_mut().unwrap().push(
            json!({"type":"namespace","name":"other","tools":[{"type":"function","name":"run"}]}),
        );
        assert!(prepare(request).is_err());
    }

    #[test]
    fn should_alias_long_tool_names_and_preserve_their_original_identity() {
        let name = "a".repeat(70);
        let request = json!({"model":"qwen", "input":"hi", "tools":[{"type":"function","name":name}], "tool_choice":{"type":"function","name":name}});
        let converted = prepare(request).unwrap();
        let alias = converted.body["tools"][0]["function"]["name"]
            .as_str()
            .unwrap();
        assert!(alias.len() <= 64);
        assert_eq!(converted.tools[alias].name, name);
        assert_eq!(converted.body["tool_choice"]["function"]["name"], alias);
    }

    #[test]
    fn should_prefer_full_reasoning_and_reject_reasoning_across_turns() {
        let reasoning = json!({"type":"reasoning", "content":[{"type":"reasoning_text","text":"full"}], "summary":[{"type":"summary_text","text":"summary"}]});
        let request =
            json!({"model":"qwen", "input":[reasoning, {"role":"assistant","content":"answer"}]});
        assert_eq!(
            prepare(request).unwrap().body["messages"][0]["reasoning"],
            "full"
        );
        let request = json!({"model":"qwen", "input":[reasoning, {"role":"user","content":"next"},{"role":"assistant","content":"answer"}]});
        assert!(prepare(request).is_err());
    }

    #[test]
    fn should_preserve_separate_assistant_reasoning_blocks() {
        let result = prepare(json!({"model":"qwen", "input":[
            {"type":"reasoning","summary":[{"type":"summary_text","text":"first"}]},
            {"role":"assistant","content":"working"},
            {"type":"reasoning","summary":[{"type":"summary_text","text":"second"}]},
            {"type":"function_call","name":"run","call_id":"a","arguments":"{}"}
        ]}))
        .unwrap();
        assert_eq!(result.body["messages"][0]["reasoning"], "first");
        assert_eq!(result.body["messages"][1]["reasoning"], "second");
    }

    #[test]
    fn should_allow_local_compaction_with_empty_tools() {
        let result = prepare(json!({"model":"qwen","input":"summarize","tools":[],"tool_choice":"auto","text":{"format":null}})).unwrap();
        assert!(result.body.get("tools").is_none());
        assert!(result.body.get("tool_choice").is_none());
    }

    #[test]
    fn should_reject_flattened_tool_name_collisions() {
        assert!(
            prepare(json!({"model":"qwen", "input":"hi", "tools":[
                {"type":"function","name":"functions__run"},
                {"type":"namespace","name":"functions","tools":[{"type":"function","name":"run"}]}
            ]}))
            .is_err()
        );
    }
}
