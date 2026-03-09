use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::FromRow;

#[derive(Serialize, Deserialize, FromRow)]
pub struct Project {
    pub id: i64,
    pub name: String,
    pub description: Option<String>,
    pub thumbnail_url: Option<String>,
    pub audio_url: Option<String>,
    pub video_url: Option<String>,
    pub final_url: Option<String>, // New field for the final output URL (audio+video)
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
