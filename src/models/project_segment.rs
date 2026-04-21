use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::Type;
use sqlx::prelude::FromRow;

#[derive(Serialize, Deserialize, Debug, Clone, Type, PartialEq)]
#[sqlx(type_name = "project_segment_type", rename_all = "PascalCase")]
pub enum ProjectSegmentType {
    Audio,
    Video,
}

#[derive(Serialize, Deserialize, Debug, Clone, FromRow)]
pub struct ProjectSegment {
    pub id: i64,
    pub project_id: i64,
    pub segment_type: ProjectSegmentType,
    pub segment_index: i32,
    pub start_time: f64,
    pub end_time: f64,
    pub text: Option<String>,
    pub url: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
