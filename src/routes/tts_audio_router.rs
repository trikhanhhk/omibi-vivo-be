use axum::{
    Router,
    routing::{get, post},
};

use crate::{
    app::AppState,
    handlers::tts_audio_handler::{create_tts_audio, get_tts_audio, stream_audio},
};

pub fn routes() -> Router<AppState> {
    Router::new().nest(
        "/api/tts-audio",
        Router::new()
            .route("/", post(create_tts_audio))
            .route("/:audio_id", get(get_tts_audio))
            .route("/:audio_id/audio", get(stream_audio)),
    )
}
