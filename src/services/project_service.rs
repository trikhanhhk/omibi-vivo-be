use sqlx::PgPool;

use crate::common::response::ApiError;
use crate::dto::project::create_project_request::CreateProjectRequest;
use crate::dto::project::project_basic::ProjectBasic;
use crate::dto::project::project_detail::ProjectDetail;
use crate::models::project::Project;
use crate::repositories::project_repository::ProjectRepository;

#[derive(Clone)]
pub struct ProjectService {
    repo: ProjectRepository,
}

impl ProjectService {
    pub fn new(pool: PgPool) -> Self {
        Self {
            repo: ProjectRepository::new(pool),
        }
    }

    pub async fn list_projects(&self) -> Result<Vec<ProjectBasic>, ApiError> {
        Ok(self.repo.list().await?)
    }

    pub async fn create_project(
        &self,
        CreateProjectRequest { name, description }: CreateProjectRequest,
    ) -> Result<ProjectBasic, ApiError> {
        if name.trim().is_empty() {
            return Err(ApiError::BadRequest(
                "Project name cannot be empty".to_string(),
            ));
        }

        Ok(self.repo.create(name.clone(), description).await?)
    }

    pub async fn get_project_by_id(&self, project_id: i64) -> Result<Project, ApiError> {
        let project = self.repo.get_by_id(project_id).await?;
        if project.is_none() {
            return Err(ApiError::NotFound(format!(
                "Project with id {} not found",
                project_id
            )));
        }
        Ok(project.unwrap())
    }

    pub async fn update_project_by_id(
        &self,
        project_id: i64,
        CreateProjectRequest { name, description }: CreateProjectRequest,
    ) -> Result<ProjectDetail, ApiError> {
        let project = self.repo.get_by_id(project_id).await?;
        if project.is_none() {
            return Err(ApiError::NotFound(format!(
                "Project with id {} not found",
                project_id
            )));
        }

        let existing_project = project.unwrap();

        let updated_name = if name.trim().is_empty() {
            existing_project.name
        } else {
            name.clone()
        };

        Ok(self
            .repo
            .update_by_id(project_id, updated_name, description)
            .await?)
    }

    pub async fn delete_project_by_id(&self, project_id: i64) -> Result<(), ApiError> {
        let project = self.repo.get_by_id(project_id).await?;
        if project.is_none() {
            return Err(ApiError::NotFound(format!(
                "Project with id {} not found",
                project_id
            )));
        }
        self.repo.delete_by_id(project_id).await?;
        Ok(())
    }
}
