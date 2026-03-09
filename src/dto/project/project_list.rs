use serde::Serialize;

use crate::dto::project::project_basic::ProjectBasic;

#[derive(Serialize)]
pub struct ProjectList {
    pub projects: Vec<ProjectBasic>,
}
