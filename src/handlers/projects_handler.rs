use axum::{
    Json,
    extract::{Multipart, Path, State},
    http::{
        StatusCode,
        header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, RANGE},
    },
    response::Response,
};
use validator::Validate;

use crate::{
    app::AppState,
    common::{
        response::{ApiError, ApiResponse},
        types::{ApiResult, StreamResult},
    },
    dto::project::{
        create_project_request::CreateProjectRequest, project_detail::ProjectDetail,
        project_list::ProjectList, upload_video_response::UploadVideoResponse,
    },
    models::project::Project,
};

pub async fn get_projects(State(state): State<AppState>) -> ApiResult<ProjectList> {
    let projects = state.project_service.list_projects().await?;

    Ok(Json(ApiResponse::success(ProjectList { projects })))
}

pub async fn create_project(
    State(state): State<AppState>,
    Json(payload): Json<CreateProjectRequest>,
) -> ApiResult<ProjectDetail> {
    // Validate the incoming request
    payload
        .validate()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;

    // Create the project using the service layer
    let project = state.project_service.create_project(payload).await?;

    Ok(Json(ApiResponse::success_message(
        project,
        "Project created successfully".to_string(),
    )))
}

pub async fn get_project_by_id(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> ApiResult<Project> {
    let project = state.project_service.get_project_by_id(id).await?;
    Ok(Json(ApiResponse::success(project)))
}

pub async fn delete_project_by_id(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> ApiResult<()> {
    Ok(Json(ApiResponse::success(
        state.project_service.delete_project_by_id(id).await?,
    )))
}

pub async fn update_project_by_id(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    Json(payload): Json<CreateProjectRequest>,
) -> ApiResult<ProjectDetail> {
    // Validate the incoming request
    payload
        .validate()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;

    // Update the project using the service layer
    let project = state
        .project_service
        .update_project_by_id(id, payload)
        .await?;

    Ok(Json(ApiResponse::success(project)))
}

pub async fn upload_project_video(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    mut multipart: Multipart,
) -> ApiResult<UploadVideoResponse> {
    let mut file_bytes: Option<Vec<u8>> = None;
    let mut content_type = "video/mp4".to_string();
    let mut ext = "mp4".to_string();

    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| ApiError::bad_request(e.to_string()))?
    {
        if field.name() == Some("video") {
            if let Some(ct) = field.content_type() {
                content_type = ct.to_string();
            }
            if let Some(fname) = field.file_name() {
                if let Some(e) = fname.rsplit('.').next() {
                    ext = e.to_string();
                }
            }
            let data = field
                .bytes()
                .await
                .map_err(|e| ApiError::bad_request(e.to_string()))?;
            file_bytes = Some(data.to_vec());
        }
    }

    let bytes =
        file_bytes.ok_or_else(|| ApiError::bad_request("Missing 'video' field in multipart form"))?;

    let response = state
        .project_service
        .upload_video(id, bytes, &content_type, &ext)
        .await?;

    Ok(Json(ApiResponse::success(response)))
}

pub async fn stream_project_video(
    State(state): State<AppState>,
    Path(id): Path<i64>,
    headers: axum::http::HeaderMap,
) -> StreamResult {
    let range_header = headers.get(RANGE).and_then(|v| v.to_str().ok());

    let (body, content_length, content_range) = state
        .project_service
        .stream_video(id, range_header)
        .await?;

    let status = if range_header.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };

    let mut builder = Response::builder()
        .status(status)
        .header(ACCEPT_RANGES, "bytes")
        .header(CONTENT_TYPE, "video/mp4");

    if let Some(len) = content_length {
        builder = builder.header(CONTENT_LENGTH, len.to_string());
    }
    if let Some(cr) = &content_range {
        builder = builder.header(CONTENT_RANGE, cr);
    }

    Ok(builder.body(body).unwrap())
}
