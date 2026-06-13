//! The version seam: each protocol version implements `Codec`.

use crate::protocol::command::ClientCommand;
use crate::protocol::event::ServerEvent;

/// What a given protocol version supports. Extended in later SPs
/// (encrypted, cache, signin, watchlist).
#[derive(Debug, Clone, Copy, Default)]
pub struct Capabilities {
    pub client_events: bool,
    pub presence: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("missing field: {0}")]
    MissingField(&'static str),
}

pub trait Codec: Send + Sync {
    fn version(&self) -> u8;
    fn capabilities(&self) -> Capabilities;
    fn decode(&self, text: &str) -> Result<ClientCommand, DecodeError>;
    fn encode(&self, event: &ServerEvent) -> String;
}
