//! In-process integration tests driving the server with a real WS client.

use futures_util::{SinkExt, StreamExt};
use pylon::adapter::local::LocalAdapter;
use pylon::adapter::Adapter;
use pylon::app::static_file::StaticFileAppManager;
use pylon::app::AppManager;
use pylon::channel::registry::Registry;
use pylon::server::config::ServerConfig;
use pylon::server::router::{build_router, AppState};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_tungstenite::tungstenite::Message;

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

async fn next_json(ws: &mut Ws) -> Value {
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Text(t) => return serde_json::from_str(&t).unwrap(),
            Message::Close(_) => panic!("unexpected close while awaiting a frame"),
            _ => continue,
        }
    }
}

async fn send_json(ws: &mut Ws, v: Value) {
    ws.send(Message::Text(v.to_string())).await.unwrap();
}

#[tokio::test]
async fn connection_established_on_connect() {
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "pusher:connection_established");
    let data: Value = serde_json::from_str(frame["data"].as_str().unwrap()).unwrap();
    assert!(data["socket_id"].as_str().unwrap().contains('.'));
    assert_eq!(data["activity_timeout"], 120);
}

#[tokio::test]
async fn ping_pong() {
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let _ = next_json(&mut ws).await; // established
    send_json(&mut ws, json!({ "event": "pusher:ping", "data": {} })).await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "pusher:pong");
}

#[tokio::test]
async fn public_subscribe_succeeds() {
    let addr = spawn(ServerConfig::default()).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let _ = next_json(&mut ws).await;
    send_json(
        &mut ws,
        json!({ "event": "pusher:subscribe", "data": { "channel": "room" } }),
    )
    .await;
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "pusher_internal:subscription_succeeded");
    assert_eq!(frame["channel"], "room");
    assert_eq!(frame["data"], ""); // empty-string data for non-presence
}

#[tokio::test]
async fn subscription_count_broadcast_to_all_subscribers() {
    let addr = spawn(ServerConfig::default()).await; // subscription_count_enabled = true
    let mut a = connect(addr, "?protocol=7").await;
    let _ = next_json(&mut a).await; // established
    send_json(
        &mut a,
        json!({ "event": "pusher:subscribe", "data": { "channel": "room" } }),
    )
    .await;
    let _succeeded = next_json(&mut a).await; // subscription_succeeded
    let count1 = next_json(&mut a).await; // count = 1 (a is the only subscriber)
    assert_eq!(count1["event"], "pusher_internal:subscription_count");
    let c1: Value = serde_json::from_str(count1["data"].as_str().unwrap()).unwrap();
    assert_eq!(c1["subscription_count"], 1);

    // a second subscriber joins -> existing subscriber `a` receives an updated count
    let mut b = connect(addr, "?protocol=7").await;
    let _ = next_json(&mut b).await; // established
    send_json(
        &mut b,
        json!({ "event": "pusher:subscribe", "data": { "channel": "room" } }),
    )
    .await;
    let count2 = next_json(&mut a).await;
    assert_eq!(count2["event"], "pusher_internal:subscription_count");
    let c2: Value = serde_json::from_str(count2["data"].as_str().unwrap()).unwrap();
    assert_eq!(c2["subscription_count"], 2);
}

#[tokio::test]
async fn unknown_app_key_errors_4001() {
    let addr = spawn(ServerConfig::default()).await;
    let url = format!("ws://{addr}/app/nope?protocol=7");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "pusher:error");
    assert_eq!(frame["data"]["code"], 4001);
}

#[tokio::test]
async fn unsupported_protocol_errors_4007() {
    let addr = spawn(ServerConfig::default()).await;
    let url = format!("ws://{addr}/app/app-key?protocol=3");
    let (mut ws, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let frame = next_json(&mut ws).await;
    assert_eq!(frame["event"], "pusher:error");
    assert_eq!(frame["data"]["code"], 4007);
}

#[tokio::test]
async fn over_capacity_errors_4004() {
    let addr = spawn(ServerConfig::default()).await; // capacity = 2
    let mut a = connect(addr, "?protocol=7").await;
    let _ = next_json(&mut a).await;
    let mut b = connect(addr, "?protocol=7").await;
    let _ = next_json(&mut b).await;
    let mut c = connect(addr, "?protocol=7").await;
    let frame = next_json(&mut c).await;
    assert_eq!(frame["event"], "pusher:error");
    assert_eq!(frame["data"]["code"], 4004);
}

#[tokio::test]
async fn idle_connection_closed_4201() {
    let config = ServerConfig {
        activity_timeout: 1,
        pong_timeout: 1,
        ..ServerConfig::default()
    };
    let addr = spawn(config).await;
    let mut ws = connect(addr, "?protocol=7").await;
    let est = next_json(&mut ws).await;
    assert_eq!(est["event"], "pusher:connection_established");

    // Stay silent. Server pings after ~1s, then closes ~1s later with 4201.
    // (tokio-tungstenite auto-replies to protocol-level Pings, but pusher:ping is
    //  an application Text frame, so the server gets no pong and must close.)
    let mut saw_ping = false;
    let mut saw_4201 = false;
    for _ in 0..6 {
        match tokio::time::timeout(std::time::Duration::from_secs(3), ws.next()).await {
            Ok(Some(Ok(Message::Text(t)))) => {
                let v: Value = serde_json::from_str(&t).unwrap();
                if v["event"] == "pusher:ping" {
                    saw_ping = true;
                }
                if v["event"] == "pusher:error" {
                    assert_eq!(v["data"]["code"], 4201);
                    saw_4201 = true;
                    break;
                }
            }
            Ok(Some(Ok(Message::Close(_)))) | Ok(None) => break,
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(_))) => break,
            Err(_) => break, // timed out
        }
    }
    assert!(saw_ping, "server should have sent a pusher:ping");
    assert!(saw_4201, "server should have closed with 4201");
}

#[tokio::test]
async fn root_route_identifies_server() {
    let addr = spawn(ServerConfig::default()).await;
    let body = reqwest::get(format!("http://{addr}/"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.to_lowercase().contains("pylon"));
}
