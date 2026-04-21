use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct CreateProjectSegmentRequest {
    pub segment_index: i32,
    pub start_time: f64,
    pub end_time: f64,
    pub text: Option<String>,
    pub url: Option<String>,
}
