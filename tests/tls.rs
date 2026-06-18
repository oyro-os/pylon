//! Integration tests for native TLS (Part B): end-to-end wss:// handshake,
//! encrypted WS subscribe + event delivery roundtrip, and resilience after a
//! plain client sends garbage to the TLS port.

use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::channel::registry::Registry;
use pylon::protocol::event::ServerEvent;
use pylon::server::config::ServerConfig;
use pylon::webhook::WebhookHandle;
use dashmap::DashMap;
use rcgen::generate_simple_self_signed;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

const APPS: &str = r#"[
    {"name":"TlsTest","id":"tls-app","key":"tls-key","secret":"tls-secret",
     "capacity":100,"client_messages_enabled":false,"subscription_count_enabled":false}
]"#;

const KEY: &str = "tls-key";
const APP_ID: &str = "tls-app";
const CHANNEL: &str = "tls-channel";

// ── cert generation ────────────────────────────────────────────────────────────

/// Generate a self-signed cert+key, write PEM files to temp dir. Returns
/// (cert_der_bytes, cert_pem_path, key_pem_path).
fn gen_cert() -> (Vec<u8>, PathBuf, PathBuf) {
    let cert = generate_simple_self_signed(vec![
        "localhost".to_string(),
        "127.0.0.1".to_string(),
    ])
    .expect("rcgen: generate self-signed cert");

    let dir = std::env::temp_dir();
    let pid = std::process::id();
    // Use a random suffix so parallel test runs don't collide.
    let n: u64 = rand::random();
    let cert_path = dir.join(format!("pylon-tls-test-cert-{pid}-{n}.pem"));
    let key_path = dir.join(format!("pylon-tls-test-key-{pid}-{n}.pem"));

    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.key_pair.serialize_pem()).unwrap();

    let cert_der = cert.cert.der().to_vec();
    (cert_der, cert_path, key_path)
}

/// Build a rustls ClientConfig that trusts ONLY our self-signed cert.
fn tls_client_config(cert_der: &[u8]) -> Arc<rustls::ClientConfig> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut root_store = rustls::RootCertStore::empty();
    let cert = rustls::pki_types::CertificateDer::from(cert_der.to_vec());
    root_store.add(cert).expect("add self-signed cert to root store");
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    Arc::new(config)
}

// ── server harness ─────────────────────────────────────────────────────────────

struct TlsHarness {
    port: u16,
    adapter: Arc<dyn Adapter>,
}

/// Spawn a TLS-enabled percore server on 127.0.0.1. Returns the port and adapter.
async fn spawn_tls_server(cert_path: &std::path::Path, key_path: &std::path::Path) -> TlsHarness {
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };

    let apps: Arc<dyn AppManager> =
        Arc::new(StaticFileAppManager::from_json(APPS).unwrap());
    let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new())));
    let adapter: Arc<dyn Adapter> = local.clone();
    let conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(Default::default());
    let webhooks = WebhookHandle::null();

    let config = ServerConfig {
        bind: "127.0.0.1".into(),
        port,
        workers: 1,
        tls_cert_path: Some(cert_path.to_str().unwrap().to_string()),
        tls_key_path: Some(key_path.to_str().unwrap().to_string()),
        ..ServerConfig::default()
    };

    let tls = pylon::transport::tls::resolve_tls(
        &config.tls_cert_path,
        &config.tls_key_path,
        &config.tls_ca_path,
    )
    .expect("TLS config should load from test cert/key");

    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = shutdown.clone();
    let local_for_sink = Some(local.clone());
    let handle = std::thread::spawn(move || {
        let _ = pylon::transport::run_percore(
            config,
            apps,
            adapter,
            conn_counts,
            webhooks,
            None, // no REST plane in TLS tests (REST handoff is plain-TCP only)
            worker_shutdown,
            local_for_sink,
            false,
            tls,
        );
    });
    std::mem::forget((shutdown, handle));

    tokio::time::sleep(Duration::from_millis(250)).await;
    TlsHarness { port, adapter: local }
}

// ── helper: connect a wss client ───────────────────────────────────────────────

type Ws = tokio_tungstenite::WebSocketStream<
    tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
>;

async fn connect_wss(port: u16, cert_der: &[u8]) -> Ws {
    let client_cfg = tls_client_config(cert_der);
    let connector = tokio_tungstenite::Connector::Rustls(client_cfg);
    let url = format!("wss://127.0.0.1:{port}/app/{KEY}?protocol=7");
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async_tls_with_config(&url, None, false, Some(connector)),
    )
    .await
    .expect("wss connect: timeout")
    .expect("wss connect: error");
    ws
}

/// Receive the next text frame from `ws`, with a 5-second timeout.
async fn next_text(ws: &mut Ws) -> String {
    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("recv: timeout")
        .expect("recv: stream ended")
        .expect("recv: frame error");
    msg.into_text().expect("expected text frame")
}

// ── tests ──────────────────────────────────────────────────────────────────────

/// A wss:// client can complete the TLS handshake + WS upgrade and receive the
/// `connection_established` frame — end-to-end encrypted.
#[tokio::test]
async fn wss_handshake_receives_connection_established() {
    let (cert_der, cert_path, key_path) = gen_cert();
    let harness = spawn_tls_server(&cert_path, &key_path).await;

    let mut ws = connect_wss(harness.port, &cert_der).await;

    let text = next_text(&mut ws).await;
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        v["event"].as_str().unwrap(),
        "pusher:connection_established",
        "expected connection_established, got: {text}"
    );

    let _ = std::fs::remove_file(&cert_path);
    let _ = std::fs::remove_file(&key_path);
}

/// A wss:// client can subscribe to a channel and receive a broadcast event
/// delivered by the adapter — encrypted end-to-end.
#[tokio::test]
async fn wss_subscribe_and_receive_broadcast() {
    let (cert_der, cert_path, key_path) = gen_cert();
    let harness = spawn_tls_server(&cert_path, &key_path).await;

    let mut ws = connect_wss(harness.port, &cert_der).await;

    // 1. Receive connection_established and extract socket_id.
    let text = next_text(&mut ws).await;
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(v["event"].as_str().unwrap(), "pusher:connection_established");
    let data: Value =
        serde_json::from_str(v["data"].as_str().unwrap()).unwrap();
    let _socket_id = data["socket_id"].as_str().unwrap().to_string();

    // 2. Subscribe to a channel.
    let sub = json!({"event":"pusher:subscribe","data":{"channel":CHANNEL}});
    ws.send(Message::Text(sub.to_string())).await.unwrap();

    let text = next_text(&mut ws).await;
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        v["event"].as_str().unwrap(),
        "pusher_internal:subscription_succeeded",
        "expected subscription_succeeded, got: {text}"
    );

    // 3. Broadcast via the adapter.
    futures_executor::block_on(harness.adapter.broadcast(
        APP_ID,
        CHANNEL,
        ServerEvent::ChannelEvent {
            channel: CHANNEL.to_string(),
            event: "tls-event".to_string(),
            data: json!({"hello": "tls"}),
            user_id: None,
        },
        None,
    ));

    // 4. Receive the broadcast over the encrypted socket.
    let text = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let t = next_text(&mut ws).await;
            let v: Value = serde_json::from_str(&t).unwrap();
            if v["event"].as_str() == Some("tls-event") {
                return t;
            }
        }
    })
    .await
    .expect("timeout waiting for tls-event");

    assert!(
        text.contains("tls-event"),
        "expected tls-event delivery over wss, got: {text}"
    );

    let _ = std::fs::remove_file(&cert_path);
    let _ = std::fs::remove_file(&key_path);
}

/// A plain ws:// client connecting to a TLS port gets a handshake error (TLS
/// record parsing failure). The server should not crash and a subsequent wss://
/// client should succeed.
#[tokio::test]
async fn plain_ws_to_tls_port_fails_server_survives() {
    let (cert_der, cert_path, key_path) = gen_cert();
    let harness = spawn_tls_server(&cert_path, &key_path).await;

    // Plain ws:// to a TLS port — should fail (TLS record rejection).
    let plain_url = format!("ws://127.0.0.1:{}/app/{}?protocol=7", harness.port, KEY);
    let plain_result = tokio::time::timeout(
        Duration::from_secs(5),
        tokio_tungstenite::connect_async(&plain_url),
    )
    .await;
    let failed = match plain_result {
        Err(_) => true,        // timeout
        Ok(Err(_)) => true,    // connection or handshake error — expected
        Ok(Ok(_)) => false,    // unexpectedly succeeded
    };
    assert!(failed, "plain ws:// to a TLS port must fail");

    // Server should still be alive: a wss:// client succeeds immediately after.
    let mut ws2 = connect_wss(harness.port, &cert_der).await;

    let text = next_text(&mut ws2).await;
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        v["event"].as_str().unwrap(),
        "pusher:connection_established",
        "server alive after plain failure: expected connection_established, got: {text}"
    );

    let _ = std::fs::remove_file(&cert_path);
    let _ = std::fs::remove_file(&key_path);
}
