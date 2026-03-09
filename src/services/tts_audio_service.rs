use sqlx::PgPool;

use crate::{
    common::response::ApiError,
    dto::tts_audio::create_tts_audio_request::CreateTtsAudioRequest,
    infra::rabbitmq::{create_channel, setup_queue},
    messaging::tts_publisher::TtsPublisher,
    models::{tts_audio::TtsAudio, tts_job::TtsJob},
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

        self.publisher.publish(&job).await;

        Ok(audio)
    }

    pub async fn update_tts_audio(
        &self,
        audio_id: i64,
        update_request: CreateTtsAudioRequest,
    ) -> Result<TtsAudio, ApiError> {
        let audio = self.repo.update(audio_id, update_request).await?;

        // Publish a job to RabbitMQ for asynchronous TTS generation if text is not empty
        let job = TtsJob {
            audio_id: audio.id,
            text: audio.text.clone(),
            tts_model: audio.tts_model.clone(),
        };

        self.publisher.publish(&job).await;

        Ok(audio)
    }

    pub async fn get_tts_audio(&self, audio_id: i64) -> Result<TtsAudio, ApiError> {
        Ok(self.repo.get_by_id(audio_id).await?)
    }
}
