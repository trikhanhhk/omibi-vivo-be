use serde::Deserialize;
use validator::Validate;

#[derive(Debug, Deserialize, Validate)]
pub struct CreateProjectRequest {
    #[validate(length(min = 3, max = 200))]
    pub name: String,

    #[validate(length(max = 200))]
    pub description: Option<String>,
}
