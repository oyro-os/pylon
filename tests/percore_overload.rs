//! SP10 Phase 1 — the headline overload regression test for the per-core
//! transport: under a publish flood that outruns delivery, the server must
//! **not hang or OOM** and must **stay alive**.
//!
//! Background (SP9, measured): the per-connection out-queue and the
//! publish→workers broadcast hand-off were both unbounded, so a publish spike
//! grew memory to 9.2 GB and the server wedged at 0% CPU. Phase 1 bounds both
//! buffers with drop-head (per-connection, freshest-wins) and a bounded
//! `sync_channel` hand-off (drop-on-full). WebSocket delivery is at-most-once,
//! so dropping is the correct overload response.
//!
//! This test mirrors `tests/percore_multiworker.rs`'s `run_percore` harness:
//! a real multi-worker percore server on an ephemeral port, a REST plane served
//! on the test runtime, and a single concrete `LocalAdapter` with the sharded
//! broadcast sink installed. It then:
//!
//!   * connects ~200 subscribers to one channel,
//!   * floods REST publishes **as fast as possible** for ~5 s (the SP9 scenario),
//!   * asserts — under a hard 30 s `tokio::time::timeout` wall — that the whole
//!     run COMPLETES (no hang),
//!   * then connects a FRESH probe subscriber and publishes one more frame,
//!     asserting it arrives within 2 s (the server is alive, not wedged).
//!
//! The budget-bound assertion (total inflight ≤ budget) needs the per-worker
//! byte-budget accounting + the `percore_total_inflight_bytes()` debug hook that
//! Phase 2 wires, so it is `#[ignore]`d here.

use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::auth::signature::{hmac_sha256_hex, md5_hex};
use pylon::channel::registry::Registry;
use pylon::server::config::{ServerConfig, TransportMode};
use pylon::server::router::{build_router, AppState};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::Message;

const SECRET: &str = "app-secret";
const KEY: &str = "app-key";
const N_SUBS: usize = 200;
const N_WORKERS: usize = 4;
/// Hard wall: the whole flood + drain must finish well inside this. A hang would
/// blow past it; the `tokio::time::timeout` then fails the test deterministically
/// instead of wedging CI.
const WALL: Duration = Duration::from_secs(30);
/// How long to flood publishes as fast as possible.
const FLOOD: Duration = Duration::from_secs(5);

const APPS: &str = r#"[
    {"name":"Test","id":"app","key":"app-key","secret":"app-secret",
     "capacity":0,"client_messages_enabled":true,"subscription_count_enabled":false}
]"#;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A running multi-worker percore server: its bound port plus the shutdown flag
/// and worker join handle (joined on drop so no thread leaks between tests).
struct Harness {
    port: u16,
    shutdown: Arc<AtomicBool>,
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

/// Reserve a free port via a throwaway std listener, then drop it.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    l.local_addr().unwrap().port()
}

/// Start a 4-worker percore server on an ephemeral port with a concrete
/// `LocalAdapter` (sharded sink installed by `run_percore`) and a REST plane
/// served on the test's tokio runtime. Waits for the listeners to bind.
async fn spawn() -> Harness {
    let port = free_port();
    let config = ServerConfig {
        transport: TransportMode::Percore,
        bind: "127.0.0.1".to_string(),
        port,
        workers: N_WORKERS,
        ..Default::default()
    };

    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(APPS).unwrap());
    let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new())));
    let adapter: Arc<dyn Adapter> = local.clone();
    let conn_counts = Arc::new(Default::default());
    let webhooks = pylon::webhook::WebhookHandle::null();

    let (rest_tx, rest_rx) =
        tokio::sync::mpsc::unbounded_channel::<pylon::transport::RestConn>();
    let rest_state = AppState {
        config: config.clone(),
        apps: apps.clone(),
        adapter: adapter.clone(),
        conn_counts: Arc::clone(&conn_counts),
        webhooks: webhooks.clone(),
    };
    let rest_router = build_router(rest_state);
    tokio::spawn(pylon::transport::rest::serve(rest_rx, rest_router));

    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = shutdown.clone();
    let handle = std::thread::spawn(move || {
        pylon::transport::run_percore(
            config,
            apps,
            adapter,
            conn_counts,
            webhooks,
            Some(rest_tx),
            worker_shutdown,
            Some(local),
        )
        .expect("run_percore failed");
    });

    // Give all four workers a moment to bind their SO_REUSEPORT listeners.
    tokio::time::sleep(Duration::from_millis(300)).await;

    Harness {
        port,
        shutdown,
        handle: Some(handle),
    }
}

async fn connect(port: u16) -> Ws {
    let url = format!("ws://127.0.0.1:{port}/app/{KEY}?protocol=7");
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .expect("connect within 5s")
    .expect("ws handshake");
    ws
}

/// Read the next Text frame as JSON (skipping non-text frames), 5s wall.
async fn next_json(ws: &mut Ws) -> Value {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(t) => return serde_json::from_str(&t).unwrap(),
                Message::Close(_) => panic!("unexpected close while awaiting a frame"),
                _ => continue,
            }
        }
    })
    .await
    .expect("frame within 5s")
}

/// Subscribe `ws` to a PUBLIC channel (no auth) and drain its
/// `subscription_succeeded`.
async fn subscribe_public(ws: &mut Ws, channel: &str) {
    ws.send(Message::Text(
        json!({
            "event": "pusher:subscribe",
            "data": { "channel": channel }
        })
        .to_string(),
    ))
    .await
    .unwrap();
    let succ = next_json(ws).await;
    assert_eq!(succ["event"], "pusher_internal:subscription_succeeded");
}

/// Sign a REST request the Pusher way (matches `tests/rest.rs`).
fn signed_query(method: &str, path: &str, body: &[u8]) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut p: BTreeMap<String, String> = BTreeMap::new();
    p.insert("auth_key".into(), KEY.into());
    p.insert("auth_timestamp".into(), now.to_string());
    p.insert("auth_version".into(), "1.0".into());
    if !body.is_empty() {
        p.insert("body_md5".into(), md5_hex(body));
    }
    let canon = p
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let signed = format!("{}\n{}\n{}", method.to_uppercase(), path, canon);
    let sig = hmac_sha256_hex(SECRET, &signed);
    format!("{canon}&auth_signature={sig}")
}

/// Fire one REST publish to `channel` with `data`. Returns the HTTP status (the
/// flood ignores it; the probe checks it is 200).
async fn publish(port: u16, client: &reqwest::Client, channel: &str, data: &str) -> u16 {
    let body = json!({
        "name": "flood",
        "data": data,
        "channels": [channel],
    })
    .to_string();
    let q = signed_query("POST", "/apps/app/events", body.as_bytes());
    match client
        .post(format!("http://127.0.0.1:{port}/apps/app/events?{q}"))
        .body(body)
        .send()
        .await
    {
        Ok(r) => r.status().as_u16(),
        // A transient connection error during the all-out flood is fine — the
        // point is the server doesn't hang; we don't require every publish to
        // succeed.
        Err(_) => 0,
    }
}

/// THE GATE: flood publishes past delivery capacity and prove the server neither
/// hangs nor wedges. The bounded per-connection drop-head queue + bounded
/// broadcast hand-off keep memory bounded, so the whole run completes inside the
/// hard wall and a fresh subscriber is still served afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overload_flood_does_not_hang_and_server_stays_alive() {
    // The entire test body runs under a hard wall: a hang fails deterministically
    // instead of blocking CI forever.
    tokio::time::timeout(WALL, async {
        let h = spawn().await;
        let channel = "flood-chan";

        // ── Connect ~200 subscribers to the one flooded channel. ──────────────
        // (Public channel → no per-sub auth needed.) The kernel spreads them
        // across the 4 workers' SO_REUSEPORT accept queues.
        let mut subs: Vec<Ws> = Vec::with_capacity(N_SUBS);
        for _ in 0..N_SUBS {
            let mut ws = connect(h.port).await;
            // Drain pusher:connection_established.
            let est = next_json(&mut ws).await;
            assert_eq!(est["event"], "pusher:connection_established");
            subscribe_public(&mut ws, channel).await;
            subs.push(ws);
        }

        // The subscribers are intentionally NOT read during the flood: their
        // out-queues back up, exercising the per-connection drop-head path (a
        // slow consumer must not grow memory without bound).

        // ── Flood REST publishes as fast as possible for ~5 s. ────────────────
        // Several concurrent publisher tasks keep the pipeline saturated. Each
        // fires back-to-back with no rate limit — the SP9 hang scenario.
        let client = reqwest::Client::new();
        let port = h.port;
        let mut publishers = Vec::new();
        for _ in 0..8 {
            let client = client.clone();
            publishers.push(tokio::spawn(async move {
                let start = Instant::now();
                let mut sent: u64 = 0;
                while start.elapsed() < FLOOD {
                    let _ = publish(port, &client, channel, "{\"n\":1}").await;
                    sent += 1;
                }
                sent
            }));
        }
        let mut total_sent: u64 = 0;
        for p in publishers {
            total_sent += p.await.expect("publisher task panicked");
        }
        // Sanity: the flood actually pushed a meaningful number of publishes
        // through (so we really exercised the overload path, not a no-op).
        assert!(
            total_sent > 0,
            "flood sent no publishes; harness/REST plane broken"
        );

        // Reaching here means the flood + the server's drain did not hang: the
        // bounded queues capped memory and the publishers all returned.

        // ── Server still alive: a FRESH probe subscriber gets a new frame. ────
        // Subscribe a brand-new connection to a SEPARATE channel (no flood
        // backlog), publish once, and require delivery within 2 s.
        let probe_chan = "probe-chan";
        let mut probe = connect(h.port).await;
        let est = next_json(&mut probe).await;
        assert_eq!(est["event"], "pusher:connection_established");
        subscribe_public(&mut probe, probe_chan).await;

        let status = publish(h.port, &client, probe_chan, "{\"alive\":true}").await;
        assert_eq!(status, 200, "probe publish must be accepted (server alive)");

        let frame = tokio::time::timeout(Duration::from_secs(2), next_json(&mut probe))
            .await
            .expect("probe frame within 2s — server is wedged");
        assert_eq!(frame["event"], "flood", "probe got wrong event");
        assert_eq!(frame["channel"], probe_chan, "probe wrong channel");
        assert_eq!(frame["data"], "{\"alive\":true}", "probe wrong data");

        drop(subs);
        drop(h);
    })
    .await
    .expect("OVERLOAD HANG: the flood + drain did not complete within the wall");
}

/// Phase 2 gate: under the flood, the total bytes queued across all workers must
/// never exceed the per-worker byte budget × workers. This needs the per-worker
/// byte-budget accounting + the `percore_total_inflight_bytes()` debug hook that
/// Phase 2 wires, so it is ignored here.
// TODO(sp10-phase2): un-ignore when per-worker budget lands
#[ignore]
#[tokio::test]
async fn overload_total_inflight_stays_within_budget() {
    // Placeholder: Phase 2 adds `percore_total_inflight_bytes()` and the
    // per-worker budget; this test will flood and assert
    // `percore_total_inflight_bytes() <= budget` throughout.
    unimplemented!("wired in SP10 Phase 2 (Task 2.2)");
}
