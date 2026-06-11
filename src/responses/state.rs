use serde_json::Value;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ReplayState {
    pub account_id: Option<String>,
    pub account_id_hash: Option<String>,
    pub supports_incremental_previous_response_id: bool,
    pub last_request_template: Option<Value>,
    pub last_completed_response_id: Option<String>,
    pub last_completed_output_items: Vec<Value>,
    pub pending_output_items: Vec<Value>,
    pub in_flight: bool,
    pub requested_service_tier: Option<String>,
    pub reasoning_effort: Option<String>,
    pub verbosity: Option<String>,
    pub store: Option<String>,
    pub actual_service_tier: Option<String>,
    pub cached_input_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
}

impl ReplayState {
    pub fn record_request_template(&mut self, request: Value) {
        self.last_request_template = Some(request);
    }

    pub fn record_completed(&mut self, response_id: String, output_items: Vec<Value>) {
        self.last_completed_response_id = Some(response_id);
        self.last_completed_output_items = output_items;
        self.pending_output_items.clear();
        self.in_flight = false;
    }

    pub fn record_output_item_done(&mut self, item: Value) {
        self.pending_output_items.push(item);
    }

    pub fn invalidate_previous_response(&mut self) {
        self.last_completed_response_id = None;
    }

    pub fn reset_after_compaction(&mut self, compacted_request: Value) {
        self.last_completed_response_id = None;
        self.last_completed_output_items.clear();
        self.pending_output_items.clear();
        self.last_request_template = Some(compacted_request);
        self.in_flight = false;
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn should_invalidate_only_previous_response_cursor() {
        let mut state = ReplayState {
            last_request_template: Some(json!({"type":"response.create","model":"gpt-5.5"})),
            last_completed_response_id: Some("resp_stale".to_string()),
            last_completed_output_items: vec![json!({"type":"message","phase":"final"})],
            pending_output_items: vec![json!({"type":"message","phase":"draft"})],
            ..ReplayState::default()
        };

        state.invalidate_previous_response();

        assert!(state.last_completed_response_id.is_none());
        assert!(state.last_request_template.is_some());
        assert_eq!(state.last_completed_output_items.len(), 1);
        assert_eq!(state.pending_output_items.len(), 1);
    }

    #[test]
    fn should_mark_replay_state_after_compaction_reset() {
        let mut state = ReplayState {
            last_completed_response_id: Some("resp_old".to_string()),
            last_completed_output_items: vec![json!({"type":"message","phase":"final"})],
            pending_output_items: vec![json!({"type":"message","phase":"draft"})],
            in_flight: true,
            ..ReplayState::default()
        };

        state.reset_after_compaction(json!({
            "type": "response.create",
            "input": [{"type": "compaction", "encrypted_content": "gAAAAABpM0Yj"}]
        }));

        assert!(state.last_completed_response_id.is_none());
        assert!(state.last_completed_output_items.is_empty());
        assert!(state.pending_output_items.is_empty());
        assert!(!state.in_flight);
    }
}
