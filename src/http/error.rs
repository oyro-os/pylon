//! REST error type that renders as an HTTP status + plain-text body, matching
//! Pusher's HTTP error responses.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};

#[derive(Debug, Clone)]
pub struct RestError {
    pub status: StatusCode,
    pub message: String,
}

impl RestError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: message.into(),
        }
    }
    pub fn payload_too_large(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            message: message.into(),
        }
    }
}

impl IntoResponse for RestError {
    fn into_response(self) -> Response {
        (self.status, self.message).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_to_status() {
        assert_eq!(RestError::bad_request("x").status, StatusCode::BAD_REQUEST);
        assert_eq!(
            RestError::unauthorized("x").status,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            RestError::payload_too_large("x").status,
            StatusCode::PAYLOAD_TOO_LARGE
        );
    }
}
