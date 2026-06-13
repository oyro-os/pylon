//! `/app/{key}` WebSocket upgrade: negotiate version, resolve app, check capacity.

use crate::connection::task::{run, ConnectionParams};
use crate::protocol::error::PusherError;
use crate::protocol::negotiate;
use crate::server::router::AppState;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Query, State};
use axum::response::Response;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub async fn upgrade(
    ws: WebSocketUpgrade,
    Path(key): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    State(state): State<AppState>,
) -> Response {
    ws.on_upgrade(move |socket| async move {
        serve(socket, key, params, state).await;
    })
}

async fn serve(socket: WebSocket, key: String, params: HashMap<String, String>, state: AppState) {
    let codec = match negotiate(
        params.get("protocol").map(String::as_str),
        state.config.strict_protocol,
    ) {
        Ok(c) => c,
        Err(e) => return reject(socket, &e).await,
    };

    let app = match state.apps.by_key(&key).await {
        Some(a) => a,
        None => return reject(socket, &PusherError::app_not_found()).await,
    };

    let counter = state
        .conn_counts
        .entry(app.id.clone())
        .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
        .clone();
    let current = counter.fetch_add(1, Ordering::SeqCst);
    if app.capacity != 0 && current >= app.capacity as usize {
        counter.fetch_sub(1, Ordering::SeqCst);
        return reject(socket, &PusherError::over_capacity()).await;
    }

    let cp = ConnectionParams {
        app,
        registry: state.registry.clone(),
        adapter: state.adapter.clone(),
        activity_timeout: state.config.activity_timeout,
        pong_timeout: state.config.pong_timeout,
        conn_count: counter,
    };
    run(socket, codec, cp).await;
}

async fn reject(mut socket: WebSocket, e: &PusherError) {
    let frame = serde_json::json!({ "event": "pusher:error", "data": { "code": e.code, "message": e.message } })
        .to_string();
    let _ = socket.send(Message::Text(frame.into())).await;
    let _ = socket.send(Message::Close(None)).await;
}
