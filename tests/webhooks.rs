//! End-to-end webhook delivery: a live pylon server with a real `HttpTransport`,
//! a real WS subscribe that occupies a channel, and a local axum receiver that
//! captures the signed POST. Verifies the envelope shape AND the
//! `X-Pusher-Signature` exactly as pusher-http-node's WebHook validator would.
//!
//! The pylon spawn runs the percore worker fleet via `tests/common`'s
//! [`common::spawn`], but wires a REAL `webhook::spawn` dispatcher with a live
//! `HttpTransport` instead of the null sink — so the occupied/vacated webhook
//! fires end-to-end.

mod common;
use common::{spawn, SpawnSpec, Ws};

use futures_util::SinkExt;
use futures_util::StreamExt;
use pylon::adapter::local::LocalAdapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::auth::signature::hmac_sha256_hex;
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::webhook::dispatcher::SystemClock;
use pylon::webhook::transport::{HttpTransport, WebhookTransport};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

const SECRET: &str = "app-secret";
const KEY: &str = "app-key";

/// Spawn a local axum receiver that captures the first POST body + signature header,
/// returning its address and a channel that yields `(raw_body, signature)`.
async fn spawn_receiver() -> (SocketAddr, mpsc::UnboundedReceiver<(String, String)>) {
    use axum::extract::State;
    use axum::http::HeaderMap;
    use axum::routing::post;
    use axum::Router;

    let (tx, rx) = mpsc::unbounded_channel::<(String, String)>();
    let tx = Arc::new(tx);

    async fn handler(
        State(tx): State<Arc<mpsc::UnboundedSender<(String, String)>>>,
        headers: HeaderMap,
        body: String,
    ) -> axum::http::StatusCode {
        let sig = headers
            .get("X-Pusher-Signature")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let _ = tx.send((body, sig));
        axum::http::StatusCode::OK
    }

    let app = Router::new()
        .route("/pusher/webhooks", post(handler))
        .with_state(tx);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (addr, rx)
}

/// Spawn the pylon server pointed at a webhook endpoint with a small batch window.
async fn spawn_pylon(receiver: SocketAddr) -> SocketAddr {
    let apps_json = format!(
        r#"[
            {{"name":"Test","id":"app","key":"{KEY}","secret":"{SECRET}",
              "client_messages_enabled":true,
              "webhooks":[{{"url":"http://{receiver}/pusher/webhooks",
                            "event_types":["channel_occupied","channel_vacated"]}}]}}
        ]"#
    );
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(&apps_json).unwrap());
    let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new())));
    let webhooks = pylon::webhook::spawn(
        apps.clone(),
        |metrics| Arc::new(HttpTransport::new(3, 50, 5000, 100, metrics)) as Arc<dyn WebhookTransport>,
        Arc::new(SystemClock),
        30, // 30ms batch window
        1024,
        0,    // local path: vacated fires immediately (no grace)
        None, // no cluster occupancy source
    );
    let config = ServerConfig {
        webhook_batch_ms: 30,
        ..ServerConfig::default()
    };
    // Route through the transport-parameterized harness with the REAL webhook
    // dispatcher (not the null sink) and the concrete local adapter (so the
    // percore sharded sink installs on it).
    spawn(SpawnSpec {
        config,
        apps,
        local,
        conn_counts: Arc::new(Default::default()),
        webhooks,
    })
    .await
}

async fn connect(addr: SocketAddr) -> Ws {
    let url = format!("ws://{addr}/app/{KEY}?protocol=7");
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(url),
    )
    .await
    .expect("connect within 5s")
    .expect("ws handshake");
    ws
}

async fn next_json(ws: &mut Ws) -> Value {
    loop {
        match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => return serde_json::from_str(&t).unwrap(),
            Ok(Some(Ok(_))) => continue,
            other => panic!("unexpected ws frame: {other:?}"),
        }
    }
}

#[tokio::test]
async fn occupied_webhook_is_posted_and_signature_validates() {
    let (receiver_addr, mut rx) = spawn_receiver().await;
    let pylon_addr = spawn_pylon(receiver_addr).await;

    let mut ws = connect(pylon_addr).await;
    // drain connection_established
    let est = next_json(&mut ws).await;
    assert_eq!(est["event"], "pusher:connection_established");

    // Subscribe to a public channel → 0→1 → channel_occupied webhook.
    ws.send(Message::Text(
        json!({ "event": "pusher:subscribe", "data": { "channel": "my-channel" } }).to_string(),
    ))
    .await
    .unwrap();
    let ack = next_json(&mut ws).await;
    assert_eq!(ack["event"], "pusher_internal:subscription_succeeded");

    // The receiver must get one signed POST within the window + delivery time.
    let (body, signature) = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("webhook POST arrived")
        .expect("channel open");

    // Envelope shape.
    let env: Value = serde_json::from_str(&body).unwrap();
    assert!(env["time_ms"].is_u64());
    let events = env["events"].as_array().unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["name"], "channel_occupied");
    assert_eq!(events[0]["channel"], "my-channel");

    // Signature validates exactly the way pusher-http-node's WebHook does:
    // hex(HMAC_SHA256(secret, raw_body)) == X-Pusher-Signature.
    assert_eq!(signature, hmac_sha256_hex(SECRET, &body));
}

/// Parse a single `metric_name value` series out of a Prometheus text body,
/// returning the parsed `u64` value (or `None` if the line is absent).
fn metric_value(body: &str, line_prefix: &str) -> Option<u64> {
    body.lines()
        .find(|l| l.starts_with(line_prefix))
        .and_then(|l| l.split_whitespace().last())
        .and_then(|v| v.parse().ok())
}

/// End-to-end: driving a webhook (subscribe → `channel_occupied`) must move the
/// `pylon_webhook_*` counters in `GET /metrics`. The receiver returns 2xx so the
/// delivery resolves `ok`. Polls (webhooks are async: batch window + spawned
/// delivery task) rather than assuming a fixed sleep.
#[tokio::test]
async fn metrics_reflect_a_driven_webhook() {
    let (receiver_addr, mut rx) = spawn_receiver().await;
    let pylon_addr = spawn_pylon(receiver_addr).await;

    let mut ws = connect(pylon_addr).await;
    let est = next_json(&mut ws).await;
    assert_eq!(est["event"], "pusher:connection_established");

    // Subscribe to a public channel → 0→1 → channel_occupied webhook fires.
    ws.send(Message::Text(
        json!({ "event": "pusher:subscribe", "data": { "channel": "metrics-room" } }).to_string(),
    ))
    .await
    .unwrap();
    let ack = next_json(&mut ws).await;
    assert_eq!(ack["event"], "pusher_internal:subscription_succeeded");

    // The delivery must actually land (the receiver returns 200) so the spawned
    // transport task bumps delivered_ok before we scrape.
    let _ = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("webhook POST arrived")
        .expect("channel open");

    // Poll /metrics until both the enqueued and the delivered{ok} counters reflect
    // the driven webhook (bounded so a real regression fails fast).
    let client = reqwest::Client::new();
    let mut enqueued = 0u64;
    let mut delivered_ok = 0u64;
    for _ in 0..50 {
        let body = client
            .get(format!("http://{pylon_addr}/metrics"))
            .send()
            .await
            .unwrap()
            .text()
            .await
            .unwrap();
        enqueued = metric_value(&body, "pylon_webhook_enqueued_total").unwrap_or(0);
        delivered_ok =
            metric_value(&body, r#"pylon_webhook_delivered_total{status="ok"}"#).unwrap_or(0);
        if enqueued >= 1 && delivered_ok >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert!(
        enqueued >= 1,
        "pylon_webhook_enqueued_total must be >= 1 after a driven webhook, got {enqueued}"
    );
    assert!(
        delivered_ok >= 1,
        "pylon_webhook_delivered_total{{status=\"ok\"}} must be >= 1 after a 2xx delivery, got {delivered_ok}"
    );
}
