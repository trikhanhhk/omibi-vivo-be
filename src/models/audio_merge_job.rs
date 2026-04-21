use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{Type, prelude::FromRow};

#[derive(Serialize, Deserialize, Debug, Type, PartialEq, Clone)]
#[sqlx(type_name = "audio_merge_status", rename_all = "PascalCase")]
pub enum AudioMergeStatus {
    Pending,
    Processing,
    Completed,
    Failed,
}

#[derive(Serialize, Deserialize, FromRow, Debug)]
pub struct AudioMergeJob {
    pub id: i64,
    pub file_name: String,
    pub model: Option<String>,
    pub audio_url: Option<String>,
    pub status: AudioMergeStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
