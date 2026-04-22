use axum::Router;
use sqlx::PgPool;

use crate::routes::{audio_merge_router, projects_router, tts_audio_router};
use crate::services::{
    audio_merge_service::AudioMergeService, project_service::ProjectService,
    tts_audio_service::TtsAudioService,
};
use crate::storage::minio_storage::MinioStorage;

#[derive(Clone)]
pub struct AppState {
    pub project_service: ProjectService,
    pub tts_audio_service: TtsAudioService,
    pub audio_merge_service: AudioMergeService,
}

impl AppState {
    pub async fn new(pool: PgPool, minio: MinioStorage) -> Self {
        Self {
            project_service: ProjectService::new(pool.clone(), minio.clone()),
            tts_audio_service: TtsAudioService::new(pool.clone(), minio.clone()).await,
            audio_merge_service: AudioMergeService::new(pool, minio).await,
        }
    }
}

pub fn create_app() -> Router<AppState> {
    Router::new()
        .merge(projects_router::routes())
        .merge(tts_audio_router::routes())
        .merge(audio_merge_router::routes())
}
