use axum::body::Body;
use sqlx::PgPool;
use uuid::Uuid;

use crate::common::response::ApiError;
use crate::dto::project::create_project_request::CreateProjectRequest;
use crate::dto::project::project_basic::ProjectBasic;
use crate::dto::project::project_detail::ProjectDetail;
use crate::dto::project::upload_video_response::UploadVideoResponse;
use crate::models::project::Project;
use crate::repositories::project_repository::ProjectRepository;
use crate::storage::minio_storage::MinioStorage;

#[derive(Clone)]
pub struct ProjectService {
    repo: ProjectRepository,
    minio: MinioStorage,
}

impl ProjectService {
    pub fn new(pool: PgPool, minio: MinioStorage) -> Self {
        Self {
            repo: ProjectRepository::new(pool),
            minio,
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

    pub async fn upload_video(
        &self,
        project_id: i64,
        bytes: Vec<u8>,
        content_type: &str,
        ext: &str,
    ) -> Result<UploadVideoResponse, ApiError> {
        // Verify project exists
        self.repo.get_by_id(project_id).await?.ok_or_else(|| {
            ApiError::not_found(format!("Project with id {} not found", project_id))
        })?;

        let key = format!("videos/{}/{}.{}", project_id, Uuid::new_v4(), ext);
        self.minio.upload(&key, bytes, content_type).await?;
        self.repo
            .add_video_segment(project_id, &key)
            .await
            .map_err(|e| ApiError::internal_with("Failed to add video segment", e))?;
        Ok(UploadVideoResponse { video_url: key })
    }

    pub async fn stream_video(
        &self,
        project_id: i64,
        range: Option<&str>,
    ) -> Result<(Body, Option<i64>, Option<String>), ApiError> {
        let project = self.repo.get_by_id(project_id).await?.ok_or_else(|| {
            ApiError::not_found(format!("Project with id {} not found", project_id))
        })?;
        // Prefer the first video segment (live editing view);
        // fall back to video_url only when no segments exist (legacy / already-merged projects)
        let key = project
            .video_segments
            .into_iter()
            .min_by_key(|s| s.segment_index)
            .and_then(|s| s.url)
            .or(project.video_url)
            .ok_or_else(|| ApiError::not_found("No video uploaded for this project".to_string()))?;
        self.minio.download_range(&key, range).await
    }

    pub async fn update_project_by_id(
        &self,
        project_id: i64,
        req: CreateProjectRequest,
    ) -> Result<ProjectDetail, ApiError> {
        let project = self.repo.get_by_id(project_id).await?.ok_or_else(|| {
            ApiError::not_found(format!("Project with id {} not found", project_id))
        })?;

        let updated_name = if req.name.trim().is_empty() {
            project.name
        } else {
            req.name.clone()
        };

        Ok(self
            .repo
            .update_by_id(
                project_id,
                updated_name,
                req.description,
                req.audio_segments,
                req.video_segments,
            )
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
