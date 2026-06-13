#![forbid(unsafe_code)]
//! pylon — a Pusher-compatible real-time WebSocket server.

pub mod adapter;
pub mod app;
pub mod channel;
pub mod connection;
pub mod http;
pub mod protocol;
pub mod server;
pub mod ws;

pub mod config;
pub mod signature;

/// Initialize tracing from `RUST_LOG` (defaults to `info`).
pub fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .try_init();
}
