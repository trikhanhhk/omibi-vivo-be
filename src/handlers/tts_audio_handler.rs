use axum::{Json, extract::State};

use validator::Validate;

use crate::{
    app::AppState,
    common::{
        response::{ApiError, ApiResponse},
        types::ApiResult,
    },
    dto::tts_audio::create_tts_audio_request::CreateTtsAudioRequest,
    models::tts_audio::TtsAudio,
};

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
