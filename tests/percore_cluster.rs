//! End-to-end 2-node clustered-percore proof (SP11 §3.6 gate).
//!
//! Two REAL percore nodes (each a single-worker `mio` fleet + a [`ClusterBridge`]
//! owning that node's `RedisAdapter`) are spawned on ONE shared Redis key prefix,
//! forming a 2-node cluster. The tests connect real WS clients to each node and
//! prove cross-node delivery rides through Redis:
//!
//! * `cross_node_broadcast_reaches_both_nodes` — a REST publish on node A reaches a
//!   subscriber on node A (local delivery) AND a subscriber on node B (A → Redis
//!   publish → B's recv loop → B's sink → B's worker → the WS client on B).
//! * `cross_node_subscription_count` — subscribing on node B updates the cluster
//!   `subscription_count` that a client on node A observes (the bridge on B
//!   broadcasts the cluster count, which fans to A via Redis).
//!
//! Like `redis_cluster.rs` / `cluster_subscribe.rs`, these talk to a REAL Redis
//! (`PYLON_TEST_REDIS_URL`, default `redis://127.0.0.1:6390`) and isolate every run
//! behind a random key prefix — they NEVER issue FLUSHALL/FLUSHDB. They FAIL LOUD
//! if Redis is unreachable (the bridge `start` panics with a clear message).
//!
//! Cross-node delivery has Redis pub/sub latency, so every cross-node assertion is
//! BOUNDED by a timeout (via the shared WS helpers' built-in per-frame timeout and
//! an explicit poll loop for the count), never masked by a long unconditional sleep.

mod common;

use common::{
    connect, established_socket_id, next_event_named, next_json, send_json,
    spawn_percore_cluster, Ws, KEY, SECRET,
};
use pylon::auth::signature::{hmac_sha256_hex, md5_hex};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;
use uuid::Uuid;

/// The common harness `APPS` app id (key `app-key`, secret `app-secret`).
const APP_ID: &str = "app";

/// A random, run-unique key prefix so the two nodes share one cluster namespace
/// without ever clobbering a shared Redis.
fn random_prefix() -> String {
    format!("pylontest:{}", Uuid::new_v4())
}

/// Build the signed Pusher REST query string for a request, mirroring
/// `tests/rest.rs::signed_query` (HMAC-SHA256 over `METHOD\npath\ncanonical`, with
/// a `body_md5`). Uses the common harness key/secret so it authenticates the
/// `/apps/app/events` publish against the standard `APPS` app.
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

/// POST a signed Pusher event to `addr`'s REST `/apps/{APP_ID}/events`, asserting a
/// 200. `data` is the event payload as a JSON STRING (the Pusher REST `data` field).
async fn publish_event(addr: SocketAddr, name: &str, channel: &str, data: &str) {
    let path = format!("/apps/{APP_ID}/events");
    let body = json!({ "name": name, "data": data, "channels": [channel] }).to_string();
    let q = signed_query("POST", &path, body.as_bytes());
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}{path}?{q}"))
        .body(body)
        .send()
        .await
        .expect("REST publish request must reach the node");
    assert_eq!(
        resp.status(),
        200,
        "REST publish must be accepted (200); got {}",
        resp.status()
    );
}

/// Connect a WS client to `addr`, drain its `connection_established`, subscribe it
/// to the public `channel`, and await `subscription_succeeded`. Returns the live
/// socket so the test can read further frames.
async fn connect_and_subscribe(addr: SocketAddr, channel: &str) -> Ws {
    let mut ws = connect(addr, "?protocol=7").await;
    let _socket_id = established_socket_id(&mut ws).await;
    send_json(
        &mut ws,
        json!({ "event": "pusher:subscribe", "data": { "channel": channel } }),
    )
    .await;
    let succeeded = next_event_named(&mut ws, "pusher_internal:subscription_succeeded").await;
    assert_eq!(
        succeeded["channel"], channel,
        "subscription_succeeded must name the channel"
    );
    ws
}

/// A cross-node broadcast published on node A reaches BOTH a subscriber on node A
/// (local delivery) and a subscriber on node B (via Redis pub/sub → B's sink → B's
/// worker). This is the headline clustered-percore delivery proof.
#[tokio::test]
async fn cross_node_broadcast_reaches_both_nodes() {
    let prefix = random_prefix();
    // Two real percore nodes on one shared Redis prefix → a 2-node cluster.
    let (addr_a, _guard_a) = spawn_percore_cluster(&prefix).await;
    let (addr_b, _guard_b) = spawn_percore_cluster(&prefix).await;

    let channel = "my-chan";
    // Client a on node A, client b on node B, both subscribed to the same channel.
    let mut ws_a = connect_and_subscribe(addr_a, channel).await;
    let mut ws_b = connect_and_subscribe(addr_b, channel).await;

    // Give B's node-local 0→1 edge a moment to drive the bridge's Redis SUBSCRIBE so
    // the published frame isn't lost (bounded; the assertions below time out anyway).
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Publish a distinctive event to the channel via node A's REST endpoint.
    let payload = "{\"hello\":\"cluster\"}";
    publish_event(addr_a, "my-event", channel, payload).await;

    // a (on A) receives it via A's local delivery. `next_event_named` skips any
    // interleaved subscription_count frames and is timeout-bounded by the helper.
    let frame_a = next_event_named(&mut ws_a, "my-event").await;
    assert_eq!(frame_a["channel"], channel, "a frame names the channel");
    assert_eq!(
        frame_a["data"], payload,
        "a must receive the published payload verbatim"
    );

    // b (on B) receives the SAME event cross-node: A → Redis → B's recv loop → B's
    // sink → B's worker → the WS client b.
    let frame_b = next_event_named(&mut ws_b, "my-event").await;
    assert_eq!(frame_b["channel"], channel, "b frame names the channel");
    assert_eq!(
        frame_b["data"], payload,
        "b must receive the cross-node published payload verbatim"
    );
}

/// Read frames from `ws` (timeout-bounded per frame) until a
/// `pusher_internal:subscription_count` frame reports `want`, or `deadline`
/// elapses. Returns whether the wanted count was observed. Tolerates interleaved
/// frames and earlier (smaller) counts — cross-node count updates arrive after a
/// Redis round-trip, so this polls rather than asserting on the first frame.
async fn await_subscription_count(ws: &mut Ws, channel: &str, want: u64, deadline: Duration) -> bool {
    let stop = tokio::time::Instant::now() + deadline;
    while tokio::time::Instant::now() < stop {
        // Bounded read; `next_json` panics on a hard timeout, so race it against the
        // remaining budget and treat an elapsed budget as "not yet / no more frames".
        let remaining = stop.saturating_duration_since(tokio::time::Instant::now());
        let frame = match tokio::time::timeout(remaining, next_json(ws)).await {
            Ok(f) => f,
            Err(_) => return false,
        };
        if frame["event"] == "pusher_internal:subscription_count" && frame["channel"] == channel {
            // `data` is a JSON-encoded STRING: { "subscription_count": N }.
            if let Some(s) = frame["data"].as_str() {
                if let Ok(inner) = serde_json::from_str::<Value>(s) {
                    if inner["subscription_count"].as_u64() == Some(want) {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// The clustered `subscription_count` reflects subscribers ACROSS nodes. Client a
/// subscribes on node A; when client b subscribes the SAME channel on node B, the
/// bridge on B broadcasts the cluster count (2), which fans to A via Redis — so a
/// observes a `subscription_count` of 2.
#[tokio::test]
async fn cross_node_subscription_count() {
    let prefix = random_prefix();
    let (addr_a, _guard_a) = spawn_percore_cluster(&prefix).await;
    let (addr_b, _guard_b) = spawn_percore_cluster(&prefix).await;

    let channel = "counted";

    // a subscribes on A → cluster count becomes 1; a should see a count of 1.
    let mut ws_a = connect_and_subscribe(addr_a, channel).await;
    assert!(
        await_subscription_count(&mut ws_a, channel, 1, Duration::from_secs(5)).await,
        "a must observe cluster subscription_count == 1 after its own subscribe"
    );

    // b subscribes the SAME channel on B → cluster count becomes 2. The bridge on B
    // broadcasts the cluster count, which reaches A via Redis; a observes 2. The
    // `_ws_b` binding keeps B's connection (and thus its cluster membership) open
    // for the duration of a's assertion below.
    let _ws_b = connect_and_subscribe(addr_b, channel).await;
    assert!(
        await_subscription_count(&mut ws_a, channel, 2, Duration::from_secs(5)).await,
        "a must observe an updated cross-node subscription_count == 2 once b subscribes on B"
    );
}
