use tokio::fs::File;
use tokio_util::io::ReaderStream;

use axum::{
    body::Body,
    http::{
        HeaderMap, StatusCode,
        header::{ACCEPT_RANGES, CONTENT_TYPE},
    },
    response::Response,
};
use sqlx::PgPool;

use crate::{
    common::response::ApiError,
    dto::tts_audio::create_tts_audio_request::CreateTtsAudioRequest,
    infra::rabbitmq::{create_channel, setup_queue},
    messaging::tts_publisher::TtsPublisher,
    models::{
        tts_audio::{TtsAudio, TtsAudioStatus},
        tts_job::TtsJob,
    },
    repositories::tts_audio_repository::TtsAudioRepository,
};

#[derive(Clone)]
pub struct TtsAudioService {
    repo: TtsAudioRepository,
    publisher: TtsPublisher,
}

impl TtsAudioService {
    pub async fn new(pool: PgPool) -> Self {
        let channel = create_channel().await;
        setup_queue(&channel).await;
        let publisher = TtsPublisher::new(channel);
        Self {
            repo: TtsAudioRepository::new(pool),
            publisher,
        }
    }

    pub async fn create_tts_audio(
        &self,
        create_project_request: CreateTtsAudioRequest,
    ) -> Result<TtsAudio, ApiError> {
        let audio = self.repo.create(create_project_request).await?;

        // Publish a job to RabbitMQ for asynchronous TTS generation if text is not empty
        let job = TtsJob {
            audio_id: audio.id,
            text: audio.text.clone(),
            tts_model: audio.tts_model.clone(),
        };

        let _ = self.publisher.publish(&job).await;

        Ok(audio)
    }

    pub async fn update_tts_audio(
        &self,
        audio_id: i64,
        update_request: CreateTtsAudioRequest,
    ) -> Result<TtsAudio, ApiError> {
        let _ =
            self.repo.get_by_id(audio_id).await?.ok_or_else(|| {
                ApiError::not_found(format!("Audio with id {} not found", audio_id))
            })?;
        let audio = self.repo.update(audio_id, update_request).await?;

        // Publish a job to RabbitMQ for asynchronous TTS generation if text is not empty
        let job = TtsJob {
            audio_id: audio.id,
            text: audio.text.clone(),
            tts_model: audio.tts_model.clone(),
        };

        let _ = self.publisher.publish(&job).await;

        Ok(audio)
    }

    pub async fn get_tts_audio(&self, audio_id: i64) -> Result<TtsAudio, ApiError> {
        let audio =
            self.repo.get_by_id(audio_id).await?.ok_or_else(|| {
                ApiError::not_found(format!("Audio with id {} not found", audio_id))
            })?;
        Ok(audio)
    }

    pub async fn update_audio_url_and_status(
        &self,
        audio_id: i64,
        audio_url: &str,
        status: crate::models::tts_audio::TtsAudioStatus,
    ) -> Result<(), ApiError> {
        self.repo
            .update_audio_url_and_status(audio_id, audio_url, status)
            .await?;
        Ok(())
    }

    pub async fn update_status(
        &self,
        audio_id: i64,
        status: crate::models::tts_audio::TtsAudioStatus,
    ) -> Result<(), ApiError> {
        self.repo.update_status(audio_id, status).await?;
        Ok(())
    }

    pub async fn stream_audio(&self, audio_id: i64) -> Result<Response, ApiError> {
        let audio =
            self.repo.get_by_id(audio_id).await?.ok_or_else(|| {
                ApiError::not_found(format!("Audio with id {} not found", audio_id))
            })?;
        if !matches!(audio.status, TtsAudioStatus::Completed) {
            return Err(ApiError::bad_request("Audio is not ready for streaming"));
        }

        let path = audio
            .audio_url
            .as_ref()
            .ok_or(ApiError::bad_request("Audio file not ready"))?;

        let full_path = format!("tts-service/{}", path);

        let file = File::open(full_path)
            .await
            .map_err(|_| ApiError::not_found("Audio file not found"))?;

        let stream = ReaderStream::new(file);
        let body = Body::from_stream(stream);

        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, "audio/mpeg".parse().unwrap());
        headers.insert(ACCEPT_RANGES, "bytes".parse().unwrap());

        let response = Response::builder()
            .status(StatusCode::OK)
            .body(body)
            .unwrap();

        Ok(response)
    }
}
