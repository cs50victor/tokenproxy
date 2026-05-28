use axum::extract::ws::{CloseFrame, Message};
use axum::http::StatusCode;
use serde_json::Value;

use crate::error::{ErrorCode, TokenproxyError};
use crate::responses::state::ReplayState;

#[derive(Debug, Clone, PartialEq)]
pub enum WebSocketAction {
    Create(Value),
    Pong(Vec<u8>),
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
        Message::Ping(bytes) => Ok(WebSocketAction::Pong(bytes.to_vec())),
        Message::Binary(_) => Ok(protocol_close(
            "downstream_binary",
            "binary frames are unsupported",
        )),
        Message::Close(frame) => Ok(WebSocketAction::Close {
            code: frame.as_ref().map_or(1000, |frame| frame.code),
            reason: frame
                .as_ref()
                .map_or_else(String::new, |frame: &CloseFrame| frame.reason.to_string()),
            event_type: "downstream_close",
            success: true,
        }),
        _ => Ok(protocol_close(
            "downstream_unsupported_frame",
            "unsupported WebSocket frame",
        )),
    }
}

fn protocol_close(event_type: &'static str, reason: &str) -> WebSocketAction {
    WebSocketAction::Close {
        code: 1003,
        reason: reason.to_string(),
        event_type,
        success: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_classify_ping_as_pong() {
        let action = classify_downstream_message(
            Message::Ping(vec![1, 2, 3].into()),
            &ReplayState::default(),
        )
        .unwrap();

        assert_eq!(action, WebSocketAction::Pong(vec![1, 2, 3]));
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
