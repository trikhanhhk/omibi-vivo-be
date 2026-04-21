use std::{error::Error, fmt};

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

type Source = Box<dyn Error + Send + Sync>;

#[derive(Debug)]
pub enum ApiError {
    BadRequest {
        message: String,
        source: Option<Source>,
    },
    NotFound {
        message: String,
        source: Option<Source>,
    },
    Internal {
        message: String,
        source: Option<Source>,
    },
}

//
// ===== Helper methods =====
//
impl ApiError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest {
            message: message.into(),
            source: None,
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound {
            message: message.into(),
            source: None,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal {
            message: message.into(),
            source: None,
        }
    }

    pub fn internal_with<E>(message: impl Into<String>, err: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Internal {
            message: message.into(),
            source: Some(Box::new(err)),
        }
    }

    fn message(&self) -> &str {
        match self {
            ApiError::BadRequest { message, .. }
            | ApiError::NotFound { message, .. }
            | ApiError::Internal { message, .. } => message,
        }
    }

    fn status_code(&self) -> StatusCode {
        match self {
            ApiError::BadRequest { .. } => StatusCode::BAD_REQUEST,
            ApiError::NotFound { .. } => StatusCode::NOT_FOUND,
            ApiError::Internal { .. } => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    fn source_ref(&self) -> Option<&Source> {
        match self {
            ApiError::BadRequest { source, .. }
            | ApiError::NotFound { source, .. }
            | ApiError::Internal { source, .. } => source.as_ref(),
        }
    }
}

//
// ===== Display =====
//
impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApiError::BadRequest { message, .. } => write!(f, "BadRequest: {}", message),
            ApiError::NotFound { message, .. } => write!(f, "NotFound: {}", message),
            ApiError::Internal { message, .. } => write!(f, "Internal: {}", message),
        }
    }
}

//
// ===== Error trait (with chaining) =====
//
impl Error for ApiError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source_ref().map(|s| s.as_ref() as _)
    }
}

//
// ===== HTTP response mapping =====
//
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let message = self.message().to_string();

        // 👇 optional: log full error chain (rất nên dùng tracing trong thực tế)
        if let Some(source) = self.source() {
            eprintln!("Error: {}", message);
            eprintln!("Caused by: {}", source);
        }

        (status, Json(ApiResponse::<()>::error(message))).into_response()
    }
}

//
// ===== External error mapping =====
//
impl From<sqlx::Error> for ApiError {
    fn from(err: sqlx::Error) -> Self {
        ApiError::Internal {
            message: "Database error".to_string(), // không leak chi tiết
            source: Some(Box::new(err)),
        }
    }
}
