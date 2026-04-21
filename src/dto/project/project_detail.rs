use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::models::project_segment::ProjectSegment;

#[derive(Serialize)]
pub struct ProjectDetail {
    pub id: i64,
    pub name: String,
    pub description: Option<String>,
    pub thumbnail_url: Option<String>,
    pub audio_url: Option<String>,
    pub video_url: Option<String>,
    pub final_url: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub audio_segments: Vec<ProjectSegment>,
    pub video_segments: Vec<ProjectSegment>,
}
