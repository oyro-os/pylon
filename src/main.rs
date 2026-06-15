//! pylon binary entrypoint.

use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::channel::registry::Registry;
use pylon::cluster::adapter::ClusterAdapter;
use pylon::server::config::{ServerConfig, TransportMode};
use pylon::server::router::{build_router, AppState};
use pylon::server::shutdown::shutdown_signal;
use pylon::webhook::dispatcher::SystemClock;
use pylon::webhook::transport::{HttpTransport, WebhookTransport};
use pylon::webhook::{OccupancySource, WebhookHandle};
use dashmap::DashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Spawn the webhook dispatcher with the production HTTP transport + system clock.
/// `vacated_grace_ms` + `occupancy` enable the cluster-aware `channel_vacated` grace
/// window (only the multi-node Redis paths pass them; the local paths pass `0` / `None`).
/// Shared by every transport/adapter combination so the dispatcher is built identically.
fn spawn_webhooks(
    config: &ServerConfig,
    apps: Arc<dyn AppManager>,
    vacated_grace_ms: u64,
    occupancy: Option<Arc<dyn OccupancySource>>,
) -> WebhookHandle {
    let transport: Arc<dyn WebhookTransport> = Arc::new(HttpTransport::new(
        config.webhook_max_retries,
        config.webhook_retry_base_ms,
        config.webhook_timeout_ms,
        config.webhook_max_concurrency,
    ));
    pylon::webhook::spawn(
        apps,
        transport,
        Arc::new(SystemClock),
        config.webhook_batch_ms,
        // Generously sized mailbox (the §8 backpressure safety valve).
        config.webhook_max_concurrency.saturating_mul(100).max(1024),
        vacated_grace_ms,
        occupancy,
    )
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    pylon::init_tracing();
    let config = ServerConfig::from_env();
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_file(&config.apps_path)?);
    let is_redis = config.adapter == "redis";

    // The redis+percore combination is the CLUSTERED production path: the node's single
    // `RedisAdapter` is owned by a `ClusterBridge` (on its own runtime), the percore workers
    // drive a `ClusterAdapter` over the shared `LocalAdapter` + the bridge handle, and each
    // connection defers its single-emit cluster edges to the bridge. It has its own startup
    // ordering (the webhook dispatcher's occupancy source reads through the bridge's adapter,
    // so webhooks are attached AFTER the bridge is up), so it runs in a dedicated function.
    // The other three combinations (redis+legacy, local+legacy, local+percore) keep the
    // straight-line wiring below, unchanged.
    if is_redis && config.transport == TransportMode::Percore {
        return run_redis_percore(config, apps).await;
    }
    // Keep the concrete `Arc<RedisAdapter>` so we can start the sweeper AFTER the
    // webhook dispatcher exists (the sweeper needs the `WebhookHandle`; the dispatcher
    // needs the adapter-backed occupancy source — start_sweeper breaks that cycle).
    let redis: Option<Arc<pylon::adapter::redis::RedisAdapter>> = if is_redis {
        Some(Arc::new(
            pylon::adapter::redis::RedisAdapter::new(&config).await?,
        ))
    } else {
        None
    };
    // Keep the CONCRETE local adapter (when not redis) so the percore transport
    // can install its sharded broadcast sink on it. `None` under redis.
    let local: Option<Arc<LocalAdapter>> = match &redis {
        Some(_) => None,
        None => Some(Arc::new(LocalAdapter::new(Arc::new(Registry::new())))),
    };
    let adapter: Arc<dyn Adapter> = match (&redis, &local) {
        (Some(r), _) => r.clone(),
        (None, Some(l)) => l.clone(),
        // Unreachable: exactly one of redis/local is set above.
        (None, None) => Arc::new(LocalAdapter::new(Arc::new(Registry::new()))),
    };

    // Cluster-aware `channel_vacated` grace window (Task D1): only the Redis
    // (multi-node) path debounces+rechecks vacated. The local path fires
    // immediately (grace = 0, no occupancy source).
    let (vacated_grace_ms, occupancy): (u64, Option<Arc<dyn OccupancySource>>) = if is_redis {
        (
            config.webhook_vacated_grace_ms,
            Some(Arc::new(pylon::webhook::AdapterOccupancy(adapter.clone()))),
        )
    } else {
        (0, None)
    };
    let webhooks = spawn_webhooks(&config, apps.clone(), vacated_grace_ms, occupancy);

    // Now that the webhook handle exists, start the Redis sweeper with the SAME
    // handle AppState uses, so vacated webhooks from sweeps and from WS-driven
    // unsubscribes share one dispatcher (grace + cluster re-check).
    if let Some(r) = &redis {
        r.start_sweeper(webhooks.clone());
    }

    // Shared connection counters, used by both transports (axum `AppState` and
    // the percore `DispatchEnv` mirror this type exactly).
    let conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(Default::default());

    match config.transport {
        TransportMode::Legacy => {
            let state = AppState {
                config: config.clone(),
                apps,
                adapter,
                conn_counts,
                webhooks,
                // Legacy transport self-throttles; no SP10 saturation admission.
                saturated: None,
            };
            let listener =
                tokio::net::TcpListener::bind((config.bind.as_str(), config.port)).await?;
            tracing::info!("pylon listening on {}:{}", config.bind, config.port);
            axum::serve(listener, build_router(state))
                .with_graceful_shutdown(shutdown_signal())
                .await?;
        }
        TransportMode::Percore => {
            // The percore worker is a blocking `mio` loop; run it on a dedicated
            // blocking thread and flip the shared shutdown flag when the signal
            // future resolves. Webhooks/adapter background tasks (e.g. the Redis
            // sweeper) were already spawned on this tokio runtime above and keep
            // running independently of the worker thread.
            //
            // REST handoff (SP9 §3.4): the worker accepts every connection but
            // can only drive WS itself. It transfers plain-HTTP (Pusher REST
            // publish) connections over `rest_tx` to a tokio task that serves
            // them with the SAME axum `Router` the legacy transport uses. The
            // task is spawned HERE, on the runtime, BEFORE the blocking worker
            // call — so a runtime handle exists to serve the handed-off fds.
            let (rest_tx, rest_rx) =
                tokio::sync::mpsc::unbounded_channel::<pylon::transport::RestConn>();
            let rest_state = AppState {
                config: config.clone(),
                apps: apps.clone(),
                adapter: adapter.clone(),
                conn_counts: conn_counts.clone(),
                webhooks: webhooks.clone(),
                // SP10: the REST 503 admission gate reads the percore saturation
                // flag (the LocalAdapter's, shared with the sink). `None` for the
                // redis+percore fallback (no concrete local adapter).
                saturated: local.as_ref().map(|l| l.saturation_flag()),
            };
            let rest_router = build_router(rest_state);
            tokio::spawn(pylon::transport::rest::serve(rest_rx, rest_router));

            // Per-core sharded fan-out applies only to the local adapter. With
            // redis+percore the concrete `LocalAdapter` isn't available, so the
            // sink is skipped and broadcasts fall back to the legacy mailbox path.
            if local.is_none() {
                tracing::warn!(
                    "redis adapter with percore transport: sharded broadcast \
                     fan-out is unavailable; using the legacy mailbox path"
                );
            }
            let local_for_sink = local.clone();

            let shutdown = Arc::new(AtomicBool::new(false));
            let worker_shutdown = shutdown.clone();
            let worker_config = config.clone();
            let worker = tokio::task::spawn_blocking(move || {
                pylon::transport::run_percore(
                    worker_config,
                    apps,
                    adapter,
                    conn_counts,
                    webhooks,
                    Some(rest_tx),
                    worker_shutdown,
                    local_for_sink,
                    // The dedicated cluster bridge (SP11 §3.6) wires `clustered:
                    // true` via its own harness; the standalone `main` percore
                    // path is not clustered.
                    false,
                )
            });

            // Wait for Ctrl-C / SIGTERM, then signal the worker to stop and join.
            shutdown_signal().await;
            shutdown.store(true, Ordering::SeqCst);
            worker.await??;
        }
    }
    Ok(())
}

/// The CLUSTERED production path: Redis adapter + percore transport. Mirrors the test
/// harness `tests/common/mod.rs::spawn_percore_cluster` exactly, but for the real binary
/// (bound to `config.bind`/`config.port`, with graceful shutdown).
///
/// One shared `LocalAdapter` `local` is shared by (a) the node's [`ClusterBridge`], which
/// owns the node's single `RedisAdapter` (built with `with_local`, on its own runtime, so
/// its pub/sub recv loop shards remote frames through `local`'s broadcast sink), (b) the
/// REST plane's [`AppState`] (driving that `RedisAdapter` for cluster-wide reads/publishes),
/// and (c) the percore worker fleet's sharded broadcast sink (installed by `run_percore`
/// when `local` is `Some`). The worker drives a [`ClusterAdapter`] = `{ local, bridge
/// handle }` and runs with `clustered = true`, so each connection's handler defers the
/// single-emit cluster edges (count / occupied / vacated / member_added/removed / cache
/// replay) to the bridge.
///
/// Startup ordering breaks the webhooks→adapter→bridge→webhooks cycle: the bridge is started
/// FIRST (building the `RedisAdapter`), THEN the webhook dispatcher is built with an
/// `AdapterOccupancy` over `bridge.adapter()`, and only THEN `bridge.attach_webhooks` wires
/// the deferred handle into the drain loop and starts the Redis sweeper. The bridge is held
/// alive until after the worker joins; its `Drop` tears down the dedicated Redis runtime.
async fn run_redis_percore(config: ServerConfig, apps: Arc<dyn AppManager>) -> anyhow::Result<()> {
    // The single shared LocalAdapter: the bridge's RedisAdapter shares it (so its recv
    // loop's `local.broadcast(Raw)` shards remote frames to this node's workers), the REST
    // plane reads the saturation flag off it, and the worker's ClusterAdapter + the sharded
    // sink install on it.
    let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new())));

    // Start the bridge: builds the node's single `RedisAdapter` (sharing `local`) on its own
    // runtime and returns once Redis is connected, or `Err` if the connect failed.
    let bridge = pylon::cluster::bridge::start(&config, local.clone(), apps.clone())?;

    // REST + occupancy drive the node's single `RedisAdapter` through the bridge.
    let adapter: Arc<dyn Adapter> = bridge.adapter();

    // Webhook dispatcher with the cluster-aware `channel_vacated` grace window (Task D1),
    // same as the redis-legacy path: debounce + re-check the cluster subscription_count
    // (via `AdapterOccupancy` over the RedisAdapter) before firing a surviving vacated.
    let occupancy: Option<Arc<dyn OccupancySource>> =
        Some(Arc::new(pylon::webhook::AdapterOccupancy(adapter.clone())));
    let webhooks = spawn_webhooks(
        &config,
        apps.clone(),
        config.webhook_vacated_grace_ms,
        occupancy,
    );

    // Now the dispatcher exists: wire its handle into the bridge's drain loop AND start the
    // Redis sweeper with the SAME handle (so sweep-driven and command-driven vacated
    // webhooks share one dispatcher). This closes the startup cycle.
    bridge.attach_webhooks(webhooks.clone());

    let conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(Default::default());

    // REST handoff plane: the worker hands plain-HTTP connections to this axum router via
    // `rest_tx`; `rest::serve` drives them on the tokio runtime. The REST `AppState` drives
    // the node's `RedisAdapter` (cluster-wide reads/publishes) and reads the percore
    // saturation flag off the shared `local`.
    let (rest_tx, rest_rx) = tokio::sync::mpsc::unbounded_channel::<pylon::transport::RestConn>();
    let rest_state = AppState {
        config: config.clone(),
        apps: apps.clone(),
        adapter: adapter.clone(),
        conn_counts: conn_counts.clone(),
        webhooks: webhooks.clone(),
        saturated: Some(local.saturation_flag()),
    };
    tokio::spawn(pylon::transport::rest::serve(
        rest_rx,
        build_router(rest_state),
    ));

    // The percore worker drives a `ClusterAdapter` over the shared `local` + the bridge
    // handle: node-local subscribes are synchronous, cross-node edges are fired (never
    // awaited) at the bridge. With `Some(local)` the sharded sink installs on the SAME
    // `local` the bridge's RedisAdapter holds, so cross-node received frames shard to this
    // worker. `clustered = true` flips each connection into deferred single-emit mode.
    let worker_adapter: Arc<dyn Adapter> =
        Arc::new(ClusterAdapter::new(local.clone(), bridge.handle()));
    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = shutdown.clone();
    let worker_config = config.clone();
    let worker_local = local.clone();
    let worker = tokio::task::spawn_blocking(move || {
        pylon::transport::run_percore(
            worker_config,
            apps,
            worker_adapter,
            conn_counts,
            webhooks,
            Some(rest_tx),
            worker_shutdown,
            Some(worker_local),
            // This IS a clustered node: defer the single-emit cluster edges.
            true,
        )
    });

    // Wait for Ctrl-C / SIGTERM, then signal the worker to stop and join. `bridge` stays in
    // scope until AFTER the join, so its `Drop` (which tears down the dedicated Redis
    // runtime) runs only once the worker has stopped firing commands at it.
    shutdown_signal().await;
    shutdown.store(true, Ordering::SeqCst);
    worker.await??;
    drop(bridge);
    Ok(())
}
