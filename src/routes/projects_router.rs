use axum::Router;
use axum::extract::DefaultBodyLimit;
use axum::routing::get;

use crate::{app::AppState, handlers::projects_handler};

pub fn routes() -> Router<AppState> {
    Router::new().nest(
        "/api/projects",
        Router::new()
            .route(
                "/",
                get(projects_handler::get_projects).post(projects_handler::create_project),
            )
            .route(
                "/:id",
                get(projects_handler::get_project_by_id)
                    .delete(projects_handler::delete_project_by_id)
                    .patch(projects_handler::update_project_by_id),
            )
            .route(
                "/:id/upload-video",
                axum::routing::post(projects_handler::upload_project_video)
                    .layer(DefaultBodyLimit::max(500 * 1024 * 1024)), // 500 MB
            )
            .route(
                "/:id/video",
                get(projects_handler::stream_project_video),
            ),
    )
}
