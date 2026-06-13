//! Router assembly + shared application state.

use crate::adapter::Adapter;
use crate::app::AppManager;
use crate::server::config::ServerConfig;
use axum::routing::get;
use axum::Router;
use dashmap::DashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub config: ServerConfig,
    pub apps: Arc<dyn AppManager>,
    pub adapter: Arc<dyn Adapter>,
    pub conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>>,
}

pub fn build_router(state: AppState) -> Router {
    // Cap the REST request body to what the configured limits can legitimately
    // produce (a full batch of max-size events) plus headroom for JSON framing,
    // so the body limit tracks the operator's configured limits rather than a
    // fixed magic number.
    let body_limit = state
        .config
        .max_batch_events
        .saturating_mul(state.config.max_event_payload_bytes)
        .saturating_add(64 * 1024);
    let router = Router::new()
        .route("/", get(crate::http::root))
        .route("/app/{key}", get(crate::ws::upgrade::upgrade));
    crate::http::rest::merge(router, body_limit).with_state(state)
}
