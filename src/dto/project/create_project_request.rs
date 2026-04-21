use serde::Deserialize;
use validator::Validate;

use super::create_project_segment_request::CreateProjectSegmentRequest;

#[derive(Debug, Deserialize, Validate)]
pub struct CreateProjectRequest {
    #[validate(length(min = 3, max = 200))]
    pub name: String,

    #[validate(length(max = 200))]
    pub description: Option<String>,

    pub audio_segments: Option<Vec<CreateProjectSegmentRequest>>,
    pub video_segments: Option<Vec<CreateProjectSegmentRequest>>,
}
