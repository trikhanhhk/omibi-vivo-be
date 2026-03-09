use sqlx::PgPool;

use crate::dto::project::{project_basic::ProjectBasic, project_detail::ProjectDetail};
use crate::models::project::Project;

#[derive(Clone)]
pub struct ProjectRepository {
    pool: PgPool,
}

impl ProjectRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn create(
        &self,
        name: String,
        description: Option<String>,
    ) -> Result<ProjectBasic, sqlx::Error> {
        let project: ProjectBasic = sqlx::query_as!(
            ProjectBasic,
            r#"
            INSERT INTO projects (name, description)
            VALUES ($1, $2)
            RETURNING id, name, description, thumbnail_url, created_at, updated_at
            "#,
            name,
            description,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(project)
    }

    pub async fn list(&self) -> Result<Vec<ProjectBasic>, sqlx::Error> {
        let projects = sqlx::query_as!(
            ProjectBasic,
            r#"
            SELECT id, name, description, thumbnail_url, created_at, updated_at
            FROM projects
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(projects)
    }

    pub async fn get_by_id(&self, project_id: i64) -> Result<Option<Project>, sqlx::Error> {
        let project = sqlx::query_as!(
            Project,
            r#"
            SELECT id, name, description, thumbnail_url, audio_url, video_url, final_url, created_at, updated_at
            FROM projects
            WHERE id = $1
            "#,
            project_id
        )
        .fetch_optional(&self.pool)
        .await?;

        Ok(project)
    }

    pub async fn update_by_id(
        &self,
        project_id: i64,
        name: String,
        description: Option<String>,
    ) -> Result<ProjectDetail, sqlx::Error> {
        let project = sqlx::query_as!(
            ProjectDetail,
            r#"
            UPDATE projects
            SET name = $1, description = $2, updated_at = NOW()
            WHERE id = $3
            RETURNING id, name, description, thumbnail_url, audio_url, video_url, final_url, created_at, updated_at
            "#,
            name,
            description,
            project_id
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(project)
    }

    pub async fn delete_by_id(&self, project_id: i64) -> Result<(), sqlx::Error> {
        sqlx::query!(
            r#"
            DELETE FROM projects
            WHERE id = $1
            "#,
            project_id
        )
        .execute(&self.pool)
        .await?;

        Ok(())
    }
}
