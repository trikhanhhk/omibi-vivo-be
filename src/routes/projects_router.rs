use axum::Router;
use axum::routing::get;

use crate::{app::AppState, handlers::projects_handler};

pub fn routes() -> Router<AppState> {
    Router::new().nest(
        "/projects",
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
            ),
    )
}
