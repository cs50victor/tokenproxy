mod request;
mod response;

pub(crate) use request::prepare;
pub(crate) use response::ResponseConverter;

use crate::error::{ErrorCode, TokenproxyError};
use axum::http::StatusCode;

fn invalid(message: impl Into<String>) -> TokenproxyError {
    TokenproxyError::new(StatusCode::BAD_REQUEST, ErrorCode::InvalidJson, message)
}

fn upstream_error(message: impl Into<String>) -> TokenproxyError {
    TokenproxyError::new(StatusCode::BAD_GATEWAY, ErrorCode::UpstreamFailure, message)
}
