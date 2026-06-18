use axum::extract::ws::{CloseFrame, Message};
use axum::http::StatusCode;
use serde_json::Value;

use crate::error::{ErrorCode, TokenproxyError};
use crate::responses::state::ReplayState;

#[derive(Debug, Clone, PartialEq)]
pub enum WebSocketAction {
    Create(Value),
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
            if state.in_flight {
                return Err(TokenproxyError::new(
                    StatusCode::CONFLICT,
                    ErrorCode::WebSocketInFlight,
                    "one response is already in flight on this WebSocket",
                ));
            }
            let value: Value = serde_json::from_str(&text).map_err(|error| {
                TokenproxyError::new(
                    StatusCode::BAD_REQUEST,
                    ErrorCode::WebSocketUnsupportedMessage,
                    format!("invalid WebSocket JSON text frame: {error}"),
                )
            })?;
            if !value.is_object() {
                return Err(TokenproxyError::new(
                    StatusCode::BAD_REQUEST,
                    ErrorCode::WebSocketUnsupportedMessage,
                    "WebSocket text frame must be a JSON object",
                ));
            }
            if value.get("type").and_then(Value::as_str) != Some("response.create") {
                return Err(TokenproxyError::new(
                    StatusCode::BAD_REQUEST,
                    ErrorCode::WebSocketUnsupportedMessage,
                    "unsupported WebSocket message type",
                ));
            }
            Ok(WebSocketAction::Create(value))
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
    fn should_reject_non_object_json_text_frame() {
        let error = classify_downstream_message(
            Message::Text(r#"["response.create"]"#.into()),
            &ReplayState::default(),
        )
        .unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(error.code, ErrorCode::WebSocketUnsupportedMessage);
        assert_eq!(error.message, "WebSocket text frame must be a JSON object");
    }

    #[test]
    fn should_reject_response_append_json_object() {
        let error = classify_downstream_message(
            Message::Text(r#"{"type":"response.append","input":[]}"#.into()),
            &ReplayState::default(),
        )
        .unwrap_err();

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(error.code, ErrorCode::WebSocketUnsupportedMessage);
        assert_eq!(error.message, "unsupported WebSocket message type");
    }
}
