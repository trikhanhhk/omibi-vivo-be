use sqlx::PgPool;

use crate::dto::project::{
    create_project_segment_request::CreateProjectSegmentRequest, project_basic::ProjectBasic,
    project_detail::ProjectDetail,
};
use crate::models::project::Project;
use crate::models::project_segment::{ProjectSegment, ProjectSegmentType};

#[derive(Clone)]
pub struct ProjectRepository {
    pool: PgPool,
}

impl ProjectRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn fetch_segments(
        &self,
        project_id: i64,
    ) -> Result<(Vec<ProjectSegment>, Vec<ProjectSegment>), sqlx::Error> {
        let segments = sqlx::query_as::<_, ProjectSegment>(
            "SELECT id, project_id, segment_type, segment_index, start_time, end_time, text, url, created_at, updated_at
             FROM project_segments WHERE project_id = $1 ORDER BY segment_type, segment_index",
        )
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?;

        let (audio, video) = segments
            .into_iter()
            .partition(|s| s.segment_type == ProjectSegmentType::Audio);

        Ok((audio, video))
    }

    pub async fn create(
        &self,
        name: String,
        description: Option<String>,
        audio_segments: Vec<CreateProjectSegmentRequest>,
        video_segments: Vec<CreateProjectSegmentRequest>,
    ) -> Result<ProjectDetail, sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        let project = sqlx::query_as::<_, Project>(
            r#"INSERT INTO projects (name, description)
               VALUES ($1, $2)
               RETURNING id, name, description, thumbnail_url, audio_url, video_url, final_url, created_at, updated_at"#,
        )
        .bind(&name)
        .bind(&description)
        .fetch_one(&mut *tx)
        .await?;

        let mut db_audio: Vec<ProjectSegment> = Vec::new();
        for seg in &audio_segments {
            let s = sqlx::query_as::<_, ProjectSegment>(
                r#"INSERT INTO project_segments (project_id, segment_type, segment_index, start_time, end_time, text, url)
                   VALUES ($1, 'Audio', $2, $3, $4, $5, $6)
                   RETURNING id, project_id, segment_type, segment_index, start_time, end_time, text, url, created_at, updated_at"#,
            )
            .bind(project.id)
            .bind(seg.segment_index)
            .bind(seg.start_time)
            .bind(seg.end_time)
            .bind(&seg.text)
            .bind(&seg.url)
            .fetch_one(&mut *tx)
            .await?;
            db_audio.push(s);
        }

        let mut db_video: Vec<ProjectSegment> = Vec::new();
        for seg in &video_segments {
            let s = sqlx::query_as::<_, ProjectSegment>(
                r#"INSERT INTO project_segments (project_id, segment_type, segment_index, start_time, end_time, text, url)
                   VALUES ($1, 'Video', $2, $3, $4, $5, $6)
                   RETURNING id, project_id, segment_type, segment_index, start_time, end_time, text, url, created_at, updated_at"#,
            )
            .bind(project.id)
            .bind(seg.segment_index)
            .bind(seg.start_time)
            .bind(seg.end_time)
            .bind(&seg.text)
            .bind(&seg.url)
            .fetch_one(&mut *tx)
            .await?;
            db_video.push(s);
        }

        tx.commit().await?;

        Ok(ProjectDetail {
            id: project.id,
            name: project.name,
            description: project.description,
            thumbnail_url: project.thumbnail_url,
            audio_url: project.audio_url,
            video_url: project.video_url,
            final_url: project.final_url,
            created_at: project.created_at,
            updated_at: project.updated_at,
            audio_segments: db_audio,
            video_segments: db_video,
        })
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
        let Some(mut project) = sqlx::query_as::<_, Project>(
            "SELECT id, name, description, thumbnail_url, audio_url, video_url, final_url, created_at, updated_at
             FROM projects WHERE id = $1",
        )
        .bind(project_id)
        .fetch_optional(&self.pool)
        .await? else {
            return Ok(None);
        };

        let (audio, video) = self.fetch_segments(project_id).await?;
        project.audio_segments = audio;
        project.video_segments = video;

        Ok(Some(project))
    }

    pub async fn update_by_id(
        &self,
        project_id: i64,
        name: String,
        description: Option<String>,
    ) -> Result<ProjectDetail, sqlx::Error> {
        let mut project = sqlx::query_as::<_, Project>(
            r#"UPDATE projects
               SET name = $1, description = $2, updated_at = NOW()
               WHERE id = $3
               RETURNING id, name, description, thumbnail_url, audio_url, video_url, final_url, created_at, updated_at"#,
        )
        .bind(name)
        .bind(description)
        .bind(project_id)
        .fetch_one(&self.pool)
        .await?;

        let (audio, video) = self.fetch_segments(project_id).await?;
        project.audio_segments = audio;
        project.video_segments = video;

        Ok(ProjectDetail {
            id: project.id,
            name: project.name,
            description: project.description,
            thumbnail_url: project.thumbnail_url,
            audio_url: project.audio_url,
            video_url: project.video_url,
            final_url: project.final_url,
            created_at: project.created_at,
            updated_at: project.updated_at,
            audio_segments: project.audio_segments,
            video_segments: project.video_segments,
        })
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
