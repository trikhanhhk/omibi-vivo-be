use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct UploadVideoResponse {
    pub video_url: String,
}
