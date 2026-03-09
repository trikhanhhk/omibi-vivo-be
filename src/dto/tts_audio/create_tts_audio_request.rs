use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Deserialize, Validate)]
pub struct CreateTtsAudioRequest {
    #[validate(length(min = 3, max = 200))]
    pub tts_name: String,

    #[validate(length(min = 3))]
    pub text: String,
    pub tts_model: String,
}
