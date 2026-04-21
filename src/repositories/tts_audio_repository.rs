use sqlx::PgPool;

use crate::{
    dto::tts_audio::create_tts_audio_request::CreateTtsAudioRequest,
    models::tts_audio::{TtsAudio, TtsAudioStatus},
};

#[derive(Clone)]
pub struct TtsAudioRepository {
    pool: PgPool,
}

impl TtsAudioRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create(
        &self,
        create_data: CreateTtsAudioRequest,
    ) -> Result<TtsAudio, sqlx::Error> {
        let tts_audio = sqlx::query_as!(
            TtsAudio,
            r#"
            INSERT INTO tts_audios (tts_name, text, tts_model, status)
            VALUES ($1, $2, $3, 'Processing')
            RETURNING id, tts_name, tts_model, text, audio_url, status as "status: TtsAudioStatus", created_at, updated_at
            "#,
            create_data.tts_name,
            create_data.text,
            create_data.tts_model,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(tts_audio)
    }

    pub async fn get_by_id(&self, audio_id: i64) -> Result<Option<TtsAudio>, sqlx::Error> {
        let tts_audio = sqlx::query_as!(
            TtsAudio,
            r#"
            SELECT id, tts_name, tts_model, text, audio_url, status as "status: TtsAudioStatus", created_at, updated_at
            FROM tts_audios
            WHERE id = $1
            "#,
            audio_id
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(tts_audio)
    }

    pub async fn update(
        &self,
        audio_id: i64,
        update_data: CreateTtsAudioRequest,
    ) -> Result<TtsAudio, sqlx::Error> {
        let tts_audio = sqlx::query_as!(
            TtsAudio,
            r#"
            UPDATE tts_audios
            SET tts_name = $1, text = $2, tts_model = $3, updated_at = NOW()
            WHERE id = $4
            RETURNING id, tts_name, tts_model, text, audio_url, status as "status: TtsAudioStatus", created_at, updated_at
            "#,
            update_data.tts_name,
            update_data.text,
            update_data.tts_model,
            audio_id
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(tts_audio)
    }

    pub async fn update_status(
        &self,
        audio_id: i64,
        status: TtsAudioStatus,
    ) -> Result<TtsAudio, sqlx::Error> {
        let tts_audio = sqlx::query_as!(
            TtsAudio,
            r#"
            UPDATE tts_audios
            SET status = $1, updated_at = NOW()
            WHERE id = $2
            RETURNING id, tts_name, tts_model, text, audio_url, status as "status: TtsAudioStatus", created_at, updated_at
            "#,
            status as TtsAudioStatus,
            audio_id
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(tts_audio)
    }

    pub async fn update_audio_url_and_status(
        &self,
        audio_id: i64,
        audio_url: &str,
        status: TtsAudioStatus,
    ) -> Result<TtsAudio, sqlx::Error> {
        let tts_audio = sqlx::query_as!(
            TtsAudio,
            r#"
            UPDATE tts_audios
            SET audio_url = $1, status = $2, updated_at = NOW()
            WHERE id = $3
            RETURNING id, tts_name, tts_model, text, audio_url, status as "status: TtsAudioStatus", created_at, updated_at
            "#,
            audio_url,
            status as TtsAudioStatus,
            audio_id
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(tts_audio)
    }
}
