use serde::{Deserialize, Serialize};
use sqlx::prelude::FromRow;

#[derive(Serialize, Deserialize, Debug, FromRow)]
pub struct Segment {
    pub id: i32,
    pub tts_audio_id: i32,
    pub start_time: f32,
    pub end_time: f32,
    pub text: String,
}
