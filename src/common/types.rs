use axum::Json;

use crate::common::response::{ApiError, ApiResponse};

pub type ApiResult<T> = Result<Json<ApiResponse<T>>, ApiError>;

pub type StreamResult = Result<axum::response::Response, ApiError>;
