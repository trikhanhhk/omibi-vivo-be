use axum::{
    Json,
    extract::{Path, State},
};
use validator::Validate;

use crate::{
    app::AppState,
    common::{
        response::{ApiError, ApiResponse},
        types::ApiResult,
    },
    dto::project::{
        create_project_request::CreateProjectRequest, project_basic::ProjectBasic,
        project_detail::ProjectDetail, project_list::ProjectList,
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
) -> ApiResult<ProjectBasic> {
    // Validate the incoming request
    payload
        .validate()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

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
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    // Update the project using the service layer
    let project = state
        .project_service
        .update_project_by_id(id, payload)
        .await?;

    Ok(Json(ApiResponse::success(project)))
}
