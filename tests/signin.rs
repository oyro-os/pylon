//! End-to-end `pusher:signin` handshake over a real WebSocket.
//!
//! Mirrors the in-process harness in `tests/integration.rs` (each `tests/*.rs`
//! is its own crate, so the spawn/connect helpers are replicated here). Drives
//! the already-implemented signin handler: happy path acks `signin_success`,
//! a bad signature yields 4009 + a server-initiated close.

use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::auth::signature::{hmac_sha256_hex, md5_hex, user_signature};
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::server::router::{build_router, AppState};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

const SECRET: &str = "app-secret";
const KEY: &str = "app-key";

const APPS: &str = r#"[
    {"name":"Test","id":"app","key":"app-key","secret":"app-secret",
     "capacity":2,"client_messages_enabled":true,"subscription_count_enabled":true}
]"#;

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn spawn(config: ServerConfig) -> SocketAddr {
    let apps: Arc<dyn AppManager> = Arc::new(StaticFileAppManager::from_json(APPS).unwrap());
    let registry = Arc::new(Registry::new());
    let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry));
    let state = AppState {
        config,
        apps,
        adapter,
        conn_counts: Arc::new(Default::default()),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, build_router(state)).await.unwrap();
    });
    addr
}

async fn connect(addr: SocketAddr, query: &str) -> Ws {
    let url = format!("ws://{addr}/app/app-key{query}");
    let (ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    ws
}

/// Read the next text frame as JSON, failing fast on a hang or unexpected close.
async fn next_json(ws: &mut Ws) -> Value {
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

async fn send_json(ws: &mut Ws, v: Value) {
    ws.send(Message::Text(v.to_string())).await.unwrap();
}

/// connection_established's `data` is a JSON-encoded STRING; extract socket_id.
async fn established_socket_id(ws: &mut Ws) -> String {
    let frame = next_json(ws).await;
    assert_eq!(frame["event"], "pusher:connection_established");
    let data: Value = serde_json::from_str(frame["data"].as_str().unwrap()).unwrap();
    data["socket_id"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn signin_success_acks_user_data() {
    // The user_data string used to SIGN must be byte-identical to the one SENT.
    const USER_DATA: &str = r#"{"id":"u1"}"#;

    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let socket_id = established_socket_id(&mut ws).await;

    let auth = format!("{KEY}:{}", user_signature(SECRET, &socket_id, USER_DATA));
    send_json(
        &mut ws,
        json!({
            "event": "pusher:signin",
            // user_data is a STRING value inside data, not a nested object.
            "data": { "auth": auth, "user_data": USER_DATA }
        }),
    )
    .await;

    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "pusher:signin_success");
    // data is an OBJECT (not double-encoded): user_data is directly readable.
    assert!(
        frame["data"].is_object(),
        "signin_success data must be an object"
    );
    assert_eq!(frame["data"]["user_data"], USER_DATA);
}

#[tokio::test]
async fn signin_bad_auth_errors_4009_then_closes() {
    const USER_DATA: &str = r#"{"id":"u1"}"#;

    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let _socket_id = established_socket_id(&mut ws).await; // skip established

    // Deliberately wrong signature -> 4009.
    send_json(
        &mut ws,
        json!({
            "event": "pusher:signin",
            "data": { "auth": format!("{KEY}:deadbeef"), "user_data": USER_DATA }
        }),
    )
    .await;

    let err = next_json(&mut ws).await;
    assert_eq!(err["event"], "pusher:error");
    assert_eq!(err["data"]["code"], 4009);

    // The server then CLOSES the socket: the next read must be a WS Close
    // (carrying 4009) or the stream ending — never another normal frame.
    match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
        Ok(Some(Ok(Message::Close(frame)))) => {
            if let Some(cf) = frame {
                assert_eq!(u16::from(cf.code), 4009, "close frame should carry 4009");
            }
        }
        Ok(None) | Ok(Some(Err(_))) => { /* stream ended / errored: also acceptable */ }
        Ok(Some(Ok(other))) => panic!("expected close after 4009, got {other:?}"),
        Err(_) => panic!("timed out waiting for close after 4009"),
    }
}

/// Build the signed query string for a Pusher REST request (mirrors the helper
/// in `tests/rest.rs`; each `tests/*.rs` is its own crate so it's replicated).
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

/// A server SDK's `sendToUser(id, ...)` is a REST trigger to `#server-to-user-<id>`.
/// pylon must route that to the user's live connections via the user registry,
/// NOT a channel broadcast (the signed-in connection never subscribes a channel).
#[tokio::test]
async fn server_to_user_trigger_reaches_signed_in_connection() {
    const USER_DATA: &str = r#"{"id":"u1"}"#;

    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let socket_id = established_socket_id(&mut ws).await;

    // Sign in as user u1.
    let auth = format!("{KEY}:{}", user_signature(SECRET, &socket_id, USER_DATA));
    send_json(
        &mut ws,
        json!({
            "event": "pusher:signin",
            "data": { "auth": auth, "user_data": USER_DATA }
        }),
    )
    .await;
    let ack = next_json(&mut ws).await;
    assert_eq!(ack["event"], "pusher:signin_success");

    // Server-to-user trigger: `data` is a JSON-encoded STRING per the Pusher REST API.
    let body = json!({
        "name": "notif",
        "channel": "#server-to-user-u1",
        "data": "{\"msg\":\"hi\"}"
    })
    .to_string();
    let q = signed_query("POST", "/apps/app/events", body.as_bytes());
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/apps/app/events?{q}"))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);

    // The signed-in connection receives a frame identical to a normal channel
    // event: event name, the `#server-to-user-u1` channel, and verbatim `data`.
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "notif");
    assert_eq!(frame["channel"], "#server-to-user-u1");
    assert_eq!(frame["data"], "{\"msg\":\"hi\"}");
}
