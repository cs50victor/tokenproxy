use axum::extract::ws::{CloseFrame, Message};
use axum::http::StatusCode;
use serde_json::Value;

use crate::error::{ErrorCode, TokenproxyError};
use crate::responses::state::ReplayState;

#[derive(Debug, Clone, PartialEq)]
pub enum WebSocketAction {
    Create(Value),
    ForwardText(String),
    // Pings are auto-ponged by axum; surfaced only so the relay can count them.
    Ping,
    // Unsolicited Pongs are a legal heartbeat (RFC 6455 section 5.5.3); drop them.
    Ignore,
    Close {
        code: u16,
        reason: String,
        event_type: &'static str,
        success: bool,
    },
}

pub fn classify_downstream_message(
    message: Message,
    state: &ReplayState,
) -> Result<WebSocketAction, TokenproxyError> {
    match message {
        Message::Text(text) => {
            if let Ok(value) = serde_json::from_str::<Value>(text.as_str())
                && value.is_object()
                && value.get("type").and_then(Value::as_str) == Some("response.create")
            {
                if state.in_flight {
                    return Err(TokenproxyError::new(
                        StatusCode::CONFLICT,
                        ErrorCode::WebSocketInFlight,
                        "one response is already in flight on this WebSocket",
                    ));
                }
                return Ok(WebSocketAction::Create(value));
            }

            // Tokenproxy is a WebSocket proxy after the initial routable
            // response.create. Codex may send additional protocol text frames
            // that regular upstream accepts, so non-create text must be
            // forwarded instead of rejected here.
            Ok(WebSocketAction::ForwardText(text.to_string()))
        }
        Message::Ping(_) => Ok(WebSocketAction::Ping),
        Message::Pong(_) => Ok(WebSocketAction::Ignore),
        Message::Binary(_) => Ok(WebSocketAction::Close {
            code: 1003,
            reason: "binary frames are unsupported".to_string(),
            event_type: "downstream_binary",
            success: false,
        }),
        Message::Close(frame) => Ok(WebSocketAction::Close {
            code: frame.as_ref().map_or(1000, |frame| frame.code),
            reason: frame
                .as_ref()
                .map_or_else(String::new, |frame: &CloseFrame| frame.reason.to_string()),
            event_type: "downstream_close",
            success: true,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_not_treat_ping_or_pong_heartbeats_as_protocol_errors() {
        let ping = classify_downstream_message(
            Message::Ping(vec![1, 2, 3].into()),
            &ReplayState::default(),
        )
        .unwrap();
        let pong = classify_downstream_message(
            Message::Pong(vec![1, 2, 3].into()),
            &ReplayState::default(),
        )
        .unwrap();

        assert_eq!(ping, WebSocketAction::Ping);
        assert_eq!(pong, WebSocketAction::Ignore);
    }

    #[test]
    fn should_accept_response_create_json_object() {
        let action = classify_downstream_message(
            Message::Text(r#"{"type":"response.create","model":"gpt-5.5"}"#.into()),
            &ReplayState::default(),
        )
        .unwrap();

        let WebSocketAction::Create(value) = action else {
            panic!("expected response.create action");
        };
        assert_eq!(value["type"], "response.create");
        assert_eq!(value["model"], "gpt-5.5");
    }

    #[test]
    fn should_forward_non_create_text_frames() {
        let action = classify_downstream_message(
            Message::Text(r#"["response.create"]"#.into()),
            &ReplayState::default(),
        )
        .unwrap();

        assert_eq!(
            action,
            WebSocketAction::ForwardText(r#"["response.create"]"#.to_string())
        );
    }

    #[test]
    fn should_forward_response_append_json_object() {
        let action = classify_downstream_message(
            Message::Text(r#"{"type":"response.append","input":[]}"#.into()),
            &ReplayState::default(),
        )
        .unwrap();

        assert_eq!(
            action,
            WebSocketAction::ForwardText(r#"{"type":"response.append","input":[]}"#.to_string())
        );
    }

    #[test]
    fn should_forward_json_object_without_type() {
        let action = classify_downstream_message(
            Message::Text(r#"{"model":"gpt-5.5"}"#.into()),
            &ReplayState::default(),
        )
        .unwrap();

        assert_eq!(
            action,
            WebSocketAction::ForwardText(r#"{"model":"gpt-5.5"}"#.to_string())
        );
    }

    #[test]
    fn should_reject_second_response_create_while_in_flight() {
        let state = ReplayState {
            in_flight: true,
            ..ReplayState::default()
        };
        let error = classify_downstream_message(
            Message::Text(r#"{"type":"response.create","model":"gpt-5.5"}"#.into()),
            &state,
        )
        .unwrap_err();

        assert_eq!(error.status, StatusCode::CONFLICT);
        assert_eq!(error.code, ErrorCode::WebSocketInFlight);
    }

    #[test]
    fn should_forward_non_create_text_while_in_flight() {
        let state = ReplayState {
            in_flight: true,
            ..ReplayState::default()
        };
        let action = classify_downstream_message(
            Message::Text(r#"{"type":"response.cancel"}"#.into()),
            &state,
        )
        .unwrap();

        assert_eq!(
            action,
            WebSocketAction::ForwardText(r#"{"type":"response.cancel"}"#.to_string())
        );
    }
}
