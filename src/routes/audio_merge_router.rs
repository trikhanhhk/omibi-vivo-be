use axum::{
    Router,
    routing::{get, post},
};

use crate::{
    app::AppState,
    handlers::audio_merge_handler::{
        download_merge_audio, get_merge_job, get_merge_jobs, merge_audio,
    },
};

pub fn routes() -> Router<AppState> {
    Router::new().nest(
        "/audio-merge",
        Router::new()
            .route("/", post(merge_audio))
            .route("/jobs", get(get_merge_jobs))
            .route("/jobs/{id}", get(get_merge_job))
            .route("/jobs/{id}/download", get(download_merge_audio)),
    )
}
