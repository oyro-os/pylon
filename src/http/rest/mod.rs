//! Pusher REST HTTP API: signed-request auth + the five endpoints.

pub mod auth;
pub mod channels;
pub mod events;
pub mod users;

use crate::server::router::AppState;
use axum::routing::{get, post};
use axum::Router;

/// Mount the REST routes onto an existing router.
pub fn merge(router: Router<AppState>) -> Router<AppState> {
    router
        .route("/apps/{app_id}/events", post(events::post_events))
        .route("/apps/{app_id}/batch_events", post(events::post_batch))
        .route("/apps/{app_id}/channels", get(channels::get_channels))
        .route(
            "/apps/{app_id}/channels/{channel_name}",
            get(channels::get_channel),
        )
        .route(
            "/apps/{app_id}/channels/{channel_name}/users",
            get(users::get_users),
        )
}
