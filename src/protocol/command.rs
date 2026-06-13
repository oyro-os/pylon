//! Inbound commands, decoded from any protocol version (version-agnostic).

use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum ClientCommand {
    Ping,
    Subscribe {
        channel: String,
        auth: Option<String>,
        channel_data: Option<String>,
    },
    Unsubscribe {
        channel: String,
    },
    /// `client-*` event. Parsed in SP1, rejected by the handler until SP2.
    ClientEvent {
        event: String,
        channel: String,
        data: Value,
    },
    /// Unrecognized event name (e.g. `pusher:pong`); logged and ignored.
    Unknown(String),
}
