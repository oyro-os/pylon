//! Consolidated, transport-parameterized test harness for the WS-driving suites
//! (`integration`, `signin`, `watchlist`, `webhooks`).
//!
//! Each `tests/*.rs` is its own crate, so before SP11 every WS suite carried its
//! OWN copy of `spawn`/`connect`/`next_json`/`established_socket_id`/`auth_token`.
//! This module hoists those into one place (the SP4 consolidation follow-up) AND
//! makes the server transport selectable at runtime via the `PYLON_TEST_TRANSPORT`
//! env var:
//!
//! * unset / `"legacy"` (default) → [`spawn_legacy`]: the axum `build_router`
//!   + `axum::serve` path the suites have always used.
//! * `"percore"` → [`spawn_percore`]: a real per-core `mio` worker fleet
//!   ([`pylon::transport::run_percore`]) with the REST handoff plane wired, bound
//!   to an ephemeral port — the SP11 single-node parity proof.
//!
//! Both paths are exercised by the SAME test bodies; any percore divergence from
//! legacy surfaces as a failed assertion (the headline parity gate).
//!
//! A test file builds a [`SpawnSpec`] (mirroring the constructible `AppState`
//! fields + the concrete `LocalAdapter` the percore sharded fan-out installs on)
//! and calls [`spawn`]. The common case — the standard capacity-2 `APPS` app with
//! a null webhook sink — is the [`spawn_default`] one-liner.

#![allow(dead_code)] // each test crate uses a different subset of these helpers

use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::auth::signature::channel_signature;
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::server::router::{build_router, AppState};
use pylon::webhook::WebhookHandle;
use dashmap::DashMap;
use serde_json::Value;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

pub const SECRET: &str = "app-secret";
pub const KEY: &str = "app-key";

/// The standard single-app config the `integration`/`signin` suites use:
/// capacity 2, client messages + subscription_count enabled.
pub const APPS: &str = r#"[
    {"name":"Test","id":"app","key":"app-key","secret":"app-secret",
     "capacity":2,"client_messages_enabled":true,"subscription_count_enabled":true}
]"#;

pub type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// The constructible pieces of an `AppState` plus the concrete `LocalAdapter` the
/// percore sharded broadcast sink installs on. A test file assembles one of these
/// (usually via [`SpawnSpec::with_apps`]) and hands it to [`spawn`]; the harness
/// then builds either an axum server or a percore worker fleet from the SAME
/// pieces, so the only thing that varies across transports is the I/O plane.
pub struct SpawnSpec {
    pub config: ServerConfig,
    pub apps: Arc<dyn AppManager>,
    /// The concrete local adapter. Held as the concrete type (not `dyn Adapter`)
    /// so [`spawn_percore`] can install the SP9/SP10 sharded broadcast sink on it.
    pub local: Arc<LocalAdapter>,
    pub conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>>,
    pub webhooks: WebhookHandle,
}

impl SpawnSpec {
    /// Build a spec from an apps-JSON string and a config, with a fresh
    /// `LocalAdapter`, empty connection counters, and a null webhook sink.
    pub fn with_apps(config: ServerConfig, apps_json: &str) -> Self {
        let apps: Arc<dyn AppManager> =
            Arc::new(StaticFileAppManager::from_json(apps_json).unwrap());
        let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new())));
        Self {
            config,
            apps,
            local,
            conn_counts: Arc::new(Default::default()),
            webhooks: WebhookHandle::null(),
        }
    }

    /// `dyn Adapter` view of the concrete local adapter (what `AppState` holds).
    fn adapter(&self) -> Arc<dyn Adapter> {
        self.local.clone()
    }
}

/// The transport selected by `PYLON_TEST_TRANSPORT` (default: legacy).
fn selected_transport() -> &'static str {
    // Leak a small owned string so the comparison is cheap and `'static`.
    match std::env::var("PYLON_TEST_TRANSPORT").ok().as_deref() {
        Some("percore") => "percore",
        _ => "legacy",
    }
}

/// Spawn the server for `spec` on the transport chosen by `PYLON_TEST_TRANSPORT`
/// and return its bound `127.0.0.1` address. The default (legacy) path is an
/// in-process axum server; `percore` starts a real per-core worker fleet + REST
/// plane. Identical externally — same `ws://`/`http://` URLs.
pub async fn spawn(spec: SpawnSpec) -> SocketAddr {
    match selected_transport() {
        "percore" => spawn_percore(spec).await,
        _ => spawn_legacy(spec).await,
    }
}

/// Convenience for the common case: the standard capacity-2 [`APPS`] app, a null
/// webhook sink, and the given `config`.
pub async fn spawn_default(config: ServerConfig) -> SocketAddr {
    spawn(SpawnSpec::with_apps(config, APPS)).await
}

/// The legacy transport: `axum::serve(build_router(state))` on `127.0.0.1:0`.
pub async fn spawn_legacy(spec: SpawnSpec) -> SocketAddr {
    let state = AppState {
        config: spec.config,
        apps: spec.apps,
        adapter: spec.local.clone(),
        conn_counts: spec.conn_counts,
        webhooks: spec.webhooks,
        saturated: None,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, build_router(state)).await.unwrap();
    });
    addr
}

/// The percore transport: a real per-core `mio` worker fleet bound to an
/// ephemeral `127.0.0.1` port, with the REST handoff plane wired so REST-driven
/// behaviors (server-to-user triggers, terminate_connections, webhooks-occupied
/// publishes) work exactly as on legacy.
///
/// Mirrors `main.rs`'s `TransportMode::Percore` branch: build the REST `AppState`
/// + handoff channel, spawn `rest::serve` on the tokio runtime, then run
/// [`pylon::transport::run_percore`] on a dedicated blocking thread. The worker
/// installs the sharded broadcast sink on the concrete `LocalAdapter` and serves
/// the full v7 protocol; plain-HTTP connections are handed off to the axum REST
/// router. The returned guard is leaked (the OS reclaims the listener + threads at
/// process exit) — test processes are short-lived, matching how `spawn_legacy`
/// leaks its `tokio::spawn`ed server.
pub async fn spawn_percore(spec: SpawnSpec) -> SocketAddr {
    let SpawnSpec {
        mut config,
        apps,
        local,
        conn_counts,
        webhooks,
    } = spec;

    // Force the percore worker onto an ephemeral 127.0.0.1 port. A throwaway std
    // listener reserves a free port, then is dropped before the worker re-binds
    // it with SO_REUSEPORT (race-free in practice — the OS won't immediately
    // recycle it to another process; mirrors tests/percore.rs::free_port).
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    config.bind = "127.0.0.1".into();
    config.port = port;
    config.transport = pylon::server::config::TransportMode::Percore;
    // A single worker keeps the test deterministic: one accept queue, one slab,
    // so subscribe/broadcast ordering matches the legacy single-task path. (The
    // multi-worker sharded fan-out is proven separately by percore_multiworker.)
    config.workers = 1;

    let adapter: Arc<dyn Adapter> = local.clone();

    // REST handoff plane: the worker hands plain-HTTP connections to this axum
    // router via `rest_tx`; `rest::serve` drives them on the tokio runtime.
    let (rest_tx, rest_rx) =
        tokio::sync::mpsc::unbounded_channel::<pylon::transport::RestConn>();
    let rest_state = AppState {
        config: config.clone(),
        apps: apps.clone(),
        adapter: adapter.clone(),
        conn_counts: conn_counts.clone(),
        webhooks: webhooks.clone(),
        saturated: Some(local.saturation_flag()),
    };
    let rest_router = build_router(rest_state);
    tokio::spawn(pylon::transport::rest::serve(rest_rx, rest_router));

    // Run the blocking `mio` worker fleet on a dedicated thread. The shutdown flag
    // is leaked alongside the join handle: the test process exits long before any
    // graceful-shutdown is needed, and leaking avoids a Drop-ordering race between
    // the worker thread and the tokio runtime tearing down.
    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = shutdown.clone();
    let worker_config = config.clone();
    let local_for_sink = Some(local.clone());
    let handle = std::thread::spawn(move || {
        let _ = pylon::transport::run_percore(
            worker_config,
            apps,
            adapter,
            conn_counts,
            webhooks,
            Some(rest_tx),
            worker_shutdown,
            local_for_sink,
        );
    });
    // Keep the worker alive for the whole test process.
    std::mem::forget((shutdown, handle));

    // Give the worker a moment to bind its SO_REUSEPORT listener before the first
    // client connects (mirrors tests/percore.rs).
    tokio::time::sleep(Duration::from_millis(200)).await;

    format!("127.0.0.1:{port}").parse().unwrap()
}

// ── Shared WS client helpers (identical across every WS suite) ──────────────

pub async fn connect(addr: SocketAddr, query: &str) -> Ws {
    let url = format!("ws://{addr}/app/app-key{query}");
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .expect("connect within 5s")
    .expect("ws handshake");
    ws
}

/// Connect to an arbitrary app key (some suites use multiple keys / no query).
pub async fn connect_key(addr: SocketAddr, key: &str, query: &str) -> Ws {
    let url = format!("ws://{addr}/app/{key}{query}");
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .expect("connect within 5s")
    .expect("ws handshake");
    ws
}

/// Read the next text frame as JSON, failing fast on a hang or unexpected close.
pub async fn next_json(ws: &mut Ws) -> Value {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => return serde_json::from_str(&t).unwrap(),
            Ok(Some(Ok(Message::Close(_)))) => panic!("unexpected close while awaiting a frame"),
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(e))) => panic!("ws error while awaiting a frame: {e}"),
            Ok(None) => panic!("stream ended while awaiting a frame"),
            Err(_) => panic!("timed out awaiting a frame"),
        }
    }
}

/// Read frames until one with the given event name arrives, skipping others
/// (e.g. interleaved `pusher_internal:subscription_count` frames).
pub async fn next_event_named(ws: &mut Ws, event: &str) -> Value {
    loop {
        let f = next_json(ws).await;
        if f["event"] == event {
            return f;
        }
    }
}

/// Try to read a frame within a short window; `None` if none arrived.
pub async fn try_next_json_short(ws: &mut Ws) -> Option<Value> {
    match tokio::time::timeout(Duration::from_millis(300), ws.next()).await {
        Ok(Some(Ok(Message::Text(t)))) => serde_json::from_str(&t).ok(),
        _ => None,
    }
}

pub async fn send_json(ws: &mut Ws, v: Value) {
    ws.send(Message::Text(v.to_string())).await.unwrap();
}

/// `connection_established`'s `data` is a JSON-encoded STRING; extract socket_id.
pub async fn established_socket_id(ws: &mut Ws) -> String {
    let frame = next_json(ws).await;
    assert_eq!(frame["event"], "pusher:connection_established");
    let data: Value = serde_json::from_str(frame["data"].as_str().unwrap()).unwrap();
    data["socket_id"].as_str().unwrap().to_string()
}

/// Build a channel-subscribe auth token for the standard app key/secret.
pub fn auth_token(socket_id: &str, channel: &str, channel_data: Option<&str>) -> String {
    format!(
        "{KEY}:{}",
        channel_signature(SECRET, socket_id, channel, channel_data)
    )
}
