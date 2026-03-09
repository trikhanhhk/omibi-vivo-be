use axum::{Router, routing::post};

use crate::{app::AppState, handlers::tts_audio_handler};

pub fn routes() -> Router<AppState> {
    Router::new().nest(
        "/tts-audio",
        Router::new().route("/", post(tts_audio_handler::create_tts_audio)),
    )
}
