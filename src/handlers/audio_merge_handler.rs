use axum::{
    Json,
    body::Body,
    extract::{Path, State},
    http::{
        StatusCode,
        header::{CONTENT_DISPOSITION, CONTENT_TYPE},
    },
    response::Response,
};
use validator::Validate;

use crate::{
    app::AppState,
    common::response::{ApiError, ApiResponse},
    dto::audio_merge::merge_audio_request::MergeAudioRequest,
    models::audio_merge_job::AudioMergeJob,
};

/// POST /audio-merge
/// Enqueue a background merge job and return the job record immediately.
pub async fn merge_audio(
    State(state): State<AppState>,
    Json(payload): Json<MergeAudioRequest>,
) -> Result<(StatusCode, Json<ApiResponse<AudioMergeJob>>), ApiError> {
    payload
        .validate()
        .map_err(|e| ApiError::bad_request(e.to_string()))?;

    let job = state
        .audio_merge_service
        .enqueue_merge_audio(payload)
        .await?;
    Ok((StatusCode::ACCEPTED, Json(ApiResponse::success(job))))
}

/// GET /audio-merge/jobs
/// List all merge jobs with pagination.
pub async fn get_merge_jobs(
    State(state): State<AppState>,
) -> Result<Json<ApiResponse<Vec<AudioMergeJob>>>, ApiError> {
    let jobs = state.audio_merge_service.list_jobs().await?;
    Ok(Json(ApiResponse::success(jobs)))
}

/// GET /audio-merge/jobs/:id
/// Return the current status of a merge job.
pub async fn get_merge_job(
    State(state): State<AppState>,
    Path(job_id): Path<i64>,
) -> Result<Json<ApiResponse<AudioMergeJob>>, ApiError> {
    let job = state
        .audio_merge_service
        .get_job(job_id)
        .await?
        .ok_or_else(|| ApiError::not_found("Merge job not found"))?;

    Ok(Json(ApiResponse::success(job)))
}

/// GET /audio-merge/jobs/:id/download
/// Stream the merged audio file if the job is completed.
pub async fn download_merge_audio(
    State(state): State<AppState>,
    Path(job_id): Path<i64>,
) -> Result<Response<Body>, ApiError> {
    let job = state
        .audio_merge_service
        .get_job(job_id)
        .await?
        .ok_or_else(|| ApiError::not_found("Merge job not found"))?;

    let file_name = job.file_name.clone();
    let audio_bytes = state.audio_merge_service.get_audio_bytes(job_id).await?;

    let content_type = infer_content_type(&file_name);
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(CONTENT_TYPE, content_type)
        .header(
            CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}\"", file_name),
        )
        .body(Body::from(audio_bytes))
        .map_err(|e| ApiError::internal(e.to_string()))?;

    Ok(response)
}

fn infer_content_type(file_name: &str) -> &'static str {
    if file_name.ends_with(".mp3") {
        "audio/mpeg"
    } else if file_name.ends_with(".ogg") {
        "audio/ogg"
    } else {
        "audio/wav"
    }
}
