//! C2a — graceful connection drain integration test.
//!
//! Verifies that when the shutdown flag is set on a percore worker:
//!
//!   (a) Every connected WS client receives a WS **Close** frame (opcode close;
//!       we assert the code is 1001 "going away").
//!   (b) The app's per-app `conn_counts` entry returns to 0 (all `on_close` /
//!       `remove` paths ran and decremented the counter).
//!
//! The harness directly constructs a single-worker `DispatchEnv` with a
//! `shutdown_grace_ms` large enough to flush a tiny frame but not so long
//! that the test hangs, connects 3 clients and subscribes them, then sets
//! `shutdown=true` and waits for each client to receive its Close frame within
//! a tight wall-clock budget.

use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::transport::worker::{run, DispatchEnv, Mode, WorkerConfig};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

const APPS: &str = r#"[
    {"name":"Test","id":"app","key":"app-key","secret":"app-secret",
     "capacity":0,"client_messages_enabled":true,"subscription_count_enabled":true}
]"#;
const APP_ID: &str = "app";

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

struct Harness {
    port: u16,
    shutdown: Arc<AtomicBool>,
    conn_counts: Arc<dashmap::DashMap<String, Arc<AtomicUsize>>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Spawn a single dispatch worker with `shutdown_grace_ms` so the drain flushes
/// the Close frame to connected clients before exiting.
async fn spawn_with_grace(grace_ms: u64) -> Harness {
    let config = ServerConfig::default();
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(APPS).unwrap());
    let registry = Arc::new(Registry::new());
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry));
    let conn_counts: Arc<dashmap::DashMap<String, Arc<AtomicUsize>>> =
        Arc::new(Default::default());
    let env = Arc::new(DispatchEnv {
        apps,
        adapter,
        limits: config.limits(),
        activity_timeout: config.activity_timeout,
        pong_timeout: config.pong_timeout,
        strict_protocol: config.strict_protocol,
        conn_counts: conn_counts.clone(),
        webhooks: pylon::webhook::WebhookHandle::null(),
        saturated: None,
        clustered: false,
    });

    let port = free_port();
    let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let shutdown = Arc::new(AtomicBool::new(false));
    let sd = shutdown.clone();
    let handle = std::thread::spawn(move || {
        run(
            WorkerConfig {
                addr,
                max_payload: 1 << 20,
                high_water: 1 << 20,
                mode: Mode::Dispatch(env),
                rest_handoff: None,
                worker_id: 0,
                broadcast: None,
                per_worker_budget: 0,
                inflight_slot: None,
                codel: pylon::transport::conn::CodelParams::DEFAULT,
                budget_factor: None,
                shutdown_grace_ms: grace_ms,
            },
            sd,
        )
        .expect("worker run failed");
    });

    tokio::time::sleep(Duration::from_millis(150)).await;

    Harness {
        port,
        shutdown,
        conn_counts,
        handle: Some(handle),
    }
}

async fn connect(port: u16) -> Ws {
    let url = format!("ws://127.0.0.1:{port}/app/app-key?protocol=7");
    let (ws, _) =
        tokio::time::timeout(Duration::from_secs(5), tokio_tungstenite::connect_async(url))
            .await
            .expect("connect within 5s")
            .expect("ws handshake");
    ws
}

/// Read messages from `ws` until we receive the `pusher:connection_established`
/// Text frame, then return the socket_id. Panics if the stream ends first.
async fn wait_established(ws: &mut Ws) -> String {
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(t))) => {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["event"] == "pusher:connection_established" {
                    let data: Value = serde_json::from_str(v["data"].as_str().unwrap()).unwrap();
                    return data["socket_id"].as_str().unwrap().to_string();
                }
            }
            Some(Ok(_)) => {}
            msg => panic!("unexpected message before connection_established: {msg:?}"),
        }
    }
}

/// Send a subscribe command and drain until we see the `pusher_internal:subscription_succeeded`.
async fn subscribe(ws: &mut Ws, channel: &str) {
    ws.send(Message::Text(
        json!({"event": "pusher:subscribe", "data": {"channel": channel}}).to_string(),
    ))
    .await
    .unwrap();
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(t))) => {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["event"] == "pusher_internal:subscription_succeeded" {
                    return;
                }
            }
            Some(Ok(_)) => {}
            msg => panic!("unexpected message waiting for subscription_succeeded: {msg:?}"),
        }
    }
}

/// Wait for the WS Close frame on `ws` and return its numeric close code (1001
/// expected). Returns `None` if the stream ends without a Close frame.
async fn wait_close(ws: &mut Ws) -> Option<u16> {
    loop {
        match ws.next().await {
            Some(Ok(Message::Close(Some(cf)))) => return Some(u16::from(cf.code)),
            Some(Ok(Message::Close(None))) | None => return None,
            Some(Ok(_)) => {} // ignore other frames while draining
            Some(Err(_)) => return None,
        }
    }
}

/// Main drain test: connect 3 clients, subscribe them, trigger graceful
/// shutdown, assert each receives Close(1001) and conn_counts returns to 0.
#[tokio::test]
async fn graceful_drain_sends_close_1001_and_cleans_up() {
    const N: usize = 3;
    // A 2-second grace window: plenty to flush a tiny Close frame, tight enough
    // that the test completes in reasonable time even under load.
    let h = spawn_with_grace(2000).await;

    // Connect N clients and bring them to the established + subscribed state.
    let mut clients: Vec<Ws> = Vec::with_capacity(N);
    for _ in 0..N {
        let mut ws = connect(h.port).await;
        wait_established(&mut ws).await;
        subscribe(&mut ws, "test-channel").await;
        clients.push(ws);
    }

    // Confirm the counters are live before triggering shutdown.
    {
        let count = h
            .conn_counts
            .get(APP_ID)
            .map(|v| v.load(Ordering::SeqCst))
            .unwrap_or(0);
        assert_eq!(count, N, "all {N} connections should be counted before drain");
    }

    // Trigger the graceful shutdown. Workers will: deregister the listener, queue
    // Close(1001) on every open connection, flush, then call remove() on each.
    h.shutdown.store(true, Ordering::SeqCst);

    // Each client must receive a WS Close frame within the grace window + slack.
    let wall = Duration::from_secs(5);
    let close_codes = tokio::time::timeout(wall, async {
        let mut codes = Vec::with_capacity(N);
        for mut ws in clients {
            codes.push(wait_close(&mut ws).await);
        }
        codes
    })
    .await
    .expect("all clients should receive Close frames within 5s");

    // (a) Every client got a Close frame with code 1001.
    for (i, code) in close_codes.iter().enumerate() {
        assert_eq!(
            *code,
            Some(1001),
            "client {i} should have received Close(1001), got {code:?}"
        );
    }

    // (b) The worker thread should exit (cleanup ran) — join it with a timeout.
    // We take the handle out of h to join without triggering the Drop path.
    // Drop sets shutdown again (no-op) then joins — both are safe here.
    // Give the worker up to 3s to finish its drain loop after the Close frames
    // are delivered (it still needs to call remove() on each connection).
    let join_deadline = std::time::Instant::now() + Duration::from_secs(3);
    // The worker was already signalled; just wait for it to exit by polling its
    // conn_counts: they should all reach 0 within the grace window.
    loop {
        let count = h
            .conn_counts
            .get(APP_ID)
            .map(|v| v.load(Ordering::SeqCst))
            .unwrap_or(0);
        if count == 0 {
            break;
        }
        if std::time::Instant::now() >= join_deadline {
            panic!(
                "conn_counts[{APP_ID}] still {} after drain — remove() did not run for all connections",
                count
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
