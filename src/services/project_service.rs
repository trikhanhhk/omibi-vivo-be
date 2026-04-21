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
        req: CreateProjectRequest,
    ) -> Result<ProjectDetail, ApiError> {
        if req.name.trim().is_empty() {
            return Err(ApiError::bad_request(
                "Project name cannot be empty".to_string(),
            ));
        }

        let audio_segments = req.audio_segments.unwrap_or_default();
        let video_segments = req.video_segments.unwrap_or_default();

        Ok(self
            .repo
            .create(req.name, req.description, audio_segments, video_segments)
            .await?)
    }

    pub async fn get_project_by_id(&self, project_id: i64) -> Result<Project, ApiError> {
        let project = self.repo.get_by_id(project_id).await?.ok_or_else(|| {
            ApiError::not_found(format!("Project with id {} not found", project_id))
        })?;
        Ok(project)
    }

    pub async fn update_project_by_id(
        &self,
        project_id: i64,
        CreateProjectRequest {
            name, description, ..
        }: CreateProjectRequest,
    ) -> Result<ProjectDetail, ApiError> {
        let project = self.repo.get_by_id(project_id).await?.ok_or_else(|| {
            ApiError::not_found(format!("Project with id {} not found", project_id))
        })?;

        let updated_name = if name.trim().is_empty() {
            project.name
        } else {
            name.clone()
        };

        Ok(self
            .repo
            .update_by_id(project_id, updated_name, description)
            .await?)
    }

    pub async fn delete_project_by_id(&self, project_id: i64) -> Result<(), ApiError> {
        self.repo.get_by_id(project_id).await?.ok_or_else(|| {
            ApiError::not_found(format!("Project with id {} not found", project_id))
        })?;
        self.repo.delete_by_id(project_id).await?;
        Ok(())
    }
}
