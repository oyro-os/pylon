//! rofrof (Rust) — Pusher-protocol WebSocket server.
//!
//! SCAFFOLD ONLY. This sets up the runtime, config loading and the HTTP root
//! route so `cargo run` works. The protocol itself (WS upgrade, channels,
//! presence, the signed HTTP API) is not implemented yet — see HANDOFF.md for
//! the spec and the planned module layout.

use axum::{routing::get, Router};

mod config;
mod signature;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let apps = config::load_apps("apps.json")?;
    tracing::info!("loaded {} app(s) from apps.json", apps.len());

    // TODO: mount /app/:appKey (ws), POST /apps/:appId/events, and the channel
    // info GET routes. See HANDOFF.md §"Wire protocol".
    let app = Router::new().route(
        "/",
        get(|| async { "The fastest WebSocket server in the world!!!" }),
    );

    let listener = tokio::net::TcpListener::bind("0.0.0.0:7000").await?;
    tracing::info!("listening on 0.0.0.0:7000");

    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("shutting down");
        })
        .await?;

    Ok(())
}
