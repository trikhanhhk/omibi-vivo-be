use serde::Deserialize;

/// Message published by Python TTS service on successful synthesis
#[derive(Deserialize, Debug)]
pub struct TtsCompleteMessage {
    pub audio_id: i64,
    pub audio_url: String,
    pub status: String,
}

/// Message published by Python TTS service on failure
#[derive(Deserialize, Debug)]
pub struct TtsErrorMessage {
    pub audio_id: i64,
    pub error: String,
    pub status: String,
}
