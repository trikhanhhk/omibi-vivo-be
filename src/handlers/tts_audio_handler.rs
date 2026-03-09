use axum::{
    Json,
    extract::{Path, State},
};

use validator::Validate;

use crate::{
    app::AppState,
    common::{
        response::{ApiError, ApiResponse},
        types::{ApiResult, StreamResult},
    },
    dto::tts_audio::create_tts_audio_request::CreateTtsAudioRequest,
    models::tts_audio::TtsAudio,
};

pub async fn get_tts_audio(
    State(state): State<AppState>,
    Path(audio_id): Path<i64>,
) -> ApiResult<TtsAudio> {
    let tts_audio = state.tts_audio_service.get_tts_audio(audio_id).await?;

    Ok(Json(ApiResponse::success(tts_audio)))
}

pub async fn create_tts_audio(
    State(state): State<AppState>,
    Json(payload): Json<CreateTtsAudioRequest>,
) -> ApiResult<TtsAudio> {
    // Validate the incoming request
    payload
        .validate()
        .map_err(|e| ApiError::BadRequest(e.to_string()))?;

    // Create the TTS audio using the service layer
    let tts_audio = state.tts_audio_service.create_tts_audio(payload).await?;

    Ok(Json(ApiResponse::success_message(
        tts_audio,
        "TTS audio generated successfully".to_string(),
    )))
}

pub async fn stream_audio(
    State(state): State<AppState>,
    Path(audio_id): Path<i64>,
) -> StreamResult {
    let response = state.tts_audio_service.stream_audio(audio_id).await?;
    Ok(response)
}
