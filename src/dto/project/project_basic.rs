use chrono::{DateTime, Utc};
use serde::Serialize;

#[derive(Serialize)]
pub struct ProjectBasic {
    pub id: i64,
    pub name: String,
    pub description: Option<String>,
    pub thumbnail_url: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
