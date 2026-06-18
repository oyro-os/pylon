//! Pusher REST HTTP API: signed-request auth + the five endpoints.

pub mod auth;
pub mod channels;
pub mod events;
pub mod health;
pub mod metrics;
pub mod users;

use crate::server::router::AppState;
use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use axum::Router;

/// Mount the REST routes onto an existing router. `body_limit` caps the buffered
/// request body (bytes) for the REST endpoints — applied here, before the
/// handlers run, so an unauthenticated caller cannot force a large buffer ahead
/// of the signature check. Scoped to the REST routes only (the WS upgrade and
/// root routes keep axum's defaults).
pub fn merge(router: Router<AppState>, body_limit: usize) -> Router<AppState> {
    let rest = Router::new()
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
        .route(
            "/apps/{app_id}/users/{user_id}/terminate_connections",
            post(users::terminate_user_connections),
        )
        .layer(DefaultBodyLimit::max(body_limit));
    let probes = Router::new()
        .route("/metrics", get(metrics::get_metrics))
        .route("/health", get(health::get_health))
        .route("/healthz", get(health::get_health))
        .route("/ready", get(health::get_ready))
        .route("/readyz", get(health::get_ready));
    router.merge(rest).merge(probes)
}
