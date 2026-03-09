use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct TtsJob {
    pub audio_id: i64,
    pub text: String,
    pub tts_model: String,
}
