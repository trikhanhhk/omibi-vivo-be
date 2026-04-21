use sqlx::PgPool;

use crate::models::audio_merge_job::{AudioMergeJob, AudioMergeStatus};

#[derive(Clone)]
pub struct AudioMergeJobRepository {
    pool: PgPool,
}

impl AudioMergeJobRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create(
        &self,
        file_name: &str,
        model: Option<&str>,
    ) -> Result<AudioMergeJob, sqlx::Error> {
        sqlx::query_as::<_, AudioMergeJob>(
            "INSERT INTO audio_merge_jobs (file_name, model, status)
             VALUES ($1, $2, 'Pending')
             RETURNING id, file_name, model, audio_url, status, created_at, updated_at",
        )
        .bind(file_name)
        .bind(model)
        .fetch_one(&self.pool)
        .await
    }

    pub async fn list(&self) -> Result<Vec<AudioMergeJob>, sqlx::Error> {
        sqlx::query_as::<_, AudioMergeJob>(
            "SELECT id, file_name, model, audio_url, status, created_at, updated_at
             FROM audio_merge_jobs
             ORDER BY created_at DESC",
        )
        .fetch_all(&self.pool)
        .await
    }

    pub async fn get_by_id(&self, id: i64) -> Result<Option<AudioMergeJob>, sqlx::Error> {
        sqlx::query_as::<_, AudioMergeJob>(
            "SELECT id, file_name, model, audio_url, status, created_at, updated_at
             FROM audio_merge_jobs
             WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn update_status(
        &self,
        id: i64,
        status: AudioMergeStatus,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE audio_merge_jobs SET status = $1, updated_at = NOW() WHERE id = $2")
            .bind(status)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn complete(&self, id: i64, audio_url: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE audio_merge_jobs
             SET status = 'Completed', audio_url = $1, updated_at = NOW()
             WHERE id = $2",
        )
        .bind(audio_url)
        .bind(id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
