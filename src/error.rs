use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    UnsupportedRoute,
    UnsupportedMethod,
    InvalidJson,
    BodyTooLarge,
    NoEligibleAccount,
    Unauthorized,
    UnsupportedMediaType,
    InvalidConfig,
    UpstreamFailure,
    WebSocketInFlight,
    WebSocketUnsupportedMessage,
}

impl ErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            ErrorCode::UnsupportedRoute => "unsupported_route",
            ErrorCode::UnsupportedMethod => "unsupported_method",
            ErrorCode::InvalidJson => "invalid_json",
            ErrorCode::BodyTooLarge => "body_too_large",
            ErrorCode::NoEligibleAccount => "no_eligible_account",
            ErrorCode::Unauthorized => "unauthorized",
            ErrorCode::UnsupportedMediaType => "unsupported_media_type",
            ErrorCode::InvalidConfig => "invalid_config",
            ErrorCode::UpstreamFailure => "upstream_failure",
            ErrorCode::WebSocketInFlight => "websocket_in_flight",
            ErrorCode::WebSocketUnsupportedMessage => "websocket_unsupported_message",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenproxyError {
    pub status: StatusCode,
    pub code: ErrorCode,
    pub message: String,
}

impl TokenproxyError {
    pub fn new(status: StatusCode, code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    pub fn invalid_config(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, ErrorCode::InvalidConfig, message)
    }
}

impl std::fmt::Display for TokenproxyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}: {}", self.code.as_str(), self.message)
    }
}

impl std::error::Error for TokenproxyError {}

#[derive(Debug, Serialize)]
struct ErrorEnvelope<'a> {
    error: ErrorBody<'a>,
}

#[derive(Debug, Serialize)]
struct ErrorBody<'a> {
    message: &'a str,
    #[serde(rename = "type")]
    error_type: &'static str,
    code: &'static str,
    param: Option<&'a str>,
}

impl IntoResponse for TokenproxyError {
    fn into_response(self) -> Response {
        let body = ErrorEnvelope {
            error: ErrorBody {
                message: &self.message,
                error_type: "tokenproxy_error",
                code: self.code.as_str(),
                param: None,
            },
        };

        (self.status, Json(body)).into_response()
    }
}
