//! pylon binary entrypoint.

use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::server::router::{build_router, AppState};
use pylon::server::shutdown::shutdown_signal;
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pylon::init_tracing();
    let config = ServerConfig::from_env();
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_file(&config.apps_path)?);
    let registry = Arc::new(Registry::new());
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry));
    let state = AppState {
        config: config.clone(),
        apps,
        adapter,
        conn_counts: Arc::new(Default::default()),
        // TEMP(B4a): C1 replaces this with the real HttpTransport-backed dispatcher spawn
        webhooks: pylon::webhook::WebhookHandle::null(),
    };
    let listener = tokio::net::TcpListener::bind((config.bind.as_str(), config.port)).await?;
    tracing::info!("pylon listening on {}:{}", config.bind, config.port);
    axum::serve(listener, build_router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}
