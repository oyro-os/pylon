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
    /// `pusher:signin` — bind this connection to a user. Decoded in SP4 (A4);
    /// handled by the signin handler in A7.
    Signin {
        auth: String,
        user_data: String,
    },
    /// Unrecognized event name (e.g. `pusher:pong`); logged and ignored.
    Unknown(String),
}
