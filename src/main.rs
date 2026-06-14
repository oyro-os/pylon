//! pylon binary entrypoint.

use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::server::router::{build_router, AppState};
use pylon::server::shutdown::shutdown_signal;
use pylon::webhook::dispatcher::SystemClock;
use pylon::webhook::transport::{HttpTransport, WebhookTransport};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pylon::init_tracing();
    let config = ServerConfig::from_env();
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_file(&config.apps_path)?);
    let is_redis = config.adapter == "redis";
    let adapter: Arc<dyn Adapter> = if is_redis {
        Arc::new(pylon::adapter::redis::RedisAdapter::new(&config).await?)
    } else {
        Arc::new(LocalAdapter::new(Arc::new(Registry::new())))
    };

    // Webhook dispatcher: real HTTP transport (reqwest+rustls), system clock.
    let transport: Arc<dyn WebhookTransport> = Arc::new(HttpTransport::new(
        config.webhook_max_retries,
        config.webhook_retry_base_ms,
        config.webhook_timeout_ms,
        config.webhook_max_concurrency,
    ));
    // Cluster-aware `channel_vacated` grace window (Task D1): only the Redis
    // (multi-node) path debounces+rechecks vacated. The local path fires
    // immediately (grace = 0, no occupancy source).
    let (vacated_grace_ms, occupancy): (u64, Option<Arc<dyn pylon::webhook::OccupancySource>>) =
        if is_redis {
            (
                config.webhook_vacated_grace_ms,
                Some(Arc::new(pylon::webhook::AdapterOccupancy(adapter.clone()))),
            )
        } else {
            (0, None)
        };
    let webhooks = pylon::webhook::spawn(
        apps.clone(),
        transport,
        Arc::new(SystemClock),
        config.webhook_batch_ms,
        // Generously sized mailbox (the §8 backpressure safety valve).
        config.webhook_max_concurrency.saturating_mul(100).max(1024),
        vacated_grace_ms,
        occupancy,
    );

    let state = AppState {
        config: config.clone(),
        apps,
        adapter,
        conn_counts: Arc::new(Default::default()),
        webhooks,
    };
    let listener = tokio::net::TcpListener::bind((config.bind.as_str(), config.port)).await?;
    tracing::info!("pylon listening on {}:{}", config.bind, config.port);
    axum::serve(listener, build_router(state))
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}
