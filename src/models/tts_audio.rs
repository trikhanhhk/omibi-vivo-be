use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Type;
use sqlx::prelude::FromRow;

use crate::models::segment::Segment;

#[derive(Serialize, Deserialize, Debug, Type, PartialEq)]
#[sqlx(type_name = "tts_audio_status", rename_all = "PascalCase")]
pub enum TtsAudioStatus {
    Pending,
    Processing,
    Completed,
    Failed,
}

#[derive(Serialize, Deserialize, FromRow)]
pub struct TtsAudio {
    pub id: i64,
    pub tts_name: String,
    pub tts_model: String,
    pub text: String,
    pub audio_url: Option<String>,
    pub status: TtsAudioStatus,
    #[sqlx(skip)]
    pub segments: Option<Vec<Segment>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
