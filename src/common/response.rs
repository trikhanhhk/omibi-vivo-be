use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;

#[derive(Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub message: Option<String>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            message: None,
        }
    }

    pub fn success_message(data: T, message: impl Into<String>) -> Self {
        Self {
            success: true,
            data: Some(data),
            message: Some(message.into()),
        }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            message: Some(message.into()),
        }
    }
}

#[derive(Debug)]
pub enum ApiError {
    BadRequest(String),
    NotFound(String),
    Internal(String),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match self {
            ApiError::BadRequest(msg) => {
                (StatusCode::BAD_REQUEST, Json(ApiResponse::<()>::error(msg))).into_response()
            }
            ApiError::NotFound(msg) => {
                (StatusCode::NOT_FOUND, Json(ApiResponse::<()>::error(msg))).into_response()
            }
            ApiError::Internal(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::<()>::error(msg)),
            )
                .into_response(),
        }
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(err: sqlx::Error) -> Self {
        ApiError::Internal(format!("Database error: {}", err))
    }
}
