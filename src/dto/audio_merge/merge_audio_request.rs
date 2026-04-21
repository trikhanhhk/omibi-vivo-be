use serde::{Deserialize, Serialize};
use validator::Validate;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AudioSegmentRequest {
    pub text: String,
    pub start_time: f64,
    pub end_time: f64,
}

#[derive(Debug, Deserialize)]
pub struct AudioMergeMetadata {
    pub file_name: String,
    pub model: Option<String>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct MergeAudioRequest {
    pub metadata: AudioMergeMetadata,
    #[validate(length(min = 1, message = "At least one segment is required"))]
    pub segments: Vec<AudioSegmentRequest>,
}
