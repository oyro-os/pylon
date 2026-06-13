pub mod error;
pub mod rest;

pub async fn root() -> &'static str {
    "pylon — Pusher-compatible realtime server"
}
