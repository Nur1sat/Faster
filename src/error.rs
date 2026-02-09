use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("bad request: {0}")]
    BadRequest(String),
    #[error("unauthorized: {0}")]
    Unauthorized(String),
    #[error("forbidden: {0}")]
    Forbidden(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("rate limit exceeded")]
    RateLimited,
    #[error("storage error: {0}")]
    Storage(String),
    #[error("replication error: {0}")]
    Replication(String),
    #[error("internal error: {0}")]
    Internal(String),
}

#[derive(Serialize)]
struct ErrorBody {
    error: String,
}

impl AppError {
    pub fn status(&self) -> StatusCode {
        match self {
            AppError::BadRequest(_) => StatusCode::BAD_REQUEST,
            AppError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            AppError::Forbidden(_) => StatusCode::FORBIDDEN,
            AppError::NotFound(_) => StatusCode::NOT_FOUND,
            AppError::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            AppError::Storage(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Replication(_) => StatusCode::BAD_GATEWAY,
            AppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = self.status();
        let body = Json(ErrorBody {
            error: self.to_string(),
        });
        (status, body).into_response()
    }
}

impl From<std::io::Error> for AppError {
    fn from(value: std::io::Error) -> Self {
        Self::Storage(value.to_string())
    }
}

impl From<serde_json::Error> for AppError {
    fn from(value: serde_json::Error) -> Self {
        Self::Internal(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_mapping_matches_error_variant() {
        let cases = [
            (
                AppError::BadRequest("x".to_string()),
                StatusCode::BAD_REQUEST,
            ),
            (
                AppError::Unauthorized("x".to_string()),
                StatusCode::UNAUTHORIZED,
            ),
            (AppError::Forbidden("x".to_string()), StatusCode::FORBIDDEN),
            (AppError::NotFound("x".to_string()), StatusCode::NOT_FOUND),
            (AppError::RateLimited, StatusCode::TOO_MANY_REQUESTS),
            (
                AppError::Storage("x".to_string()),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            (
                AppError::Replication("x".to_string()),
                StatusCode::BAD_GATEWAY,
            ),
            (
                AppError::Internal("x".to_string()),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ];

        for (err, expected_status) in cases {
            assert_eq!(err.status(), expected_status);
        }
    }
}
