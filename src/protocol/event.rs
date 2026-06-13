//! Outbound events, encoded by the negotiated codec (version-agnostic).

use crate::protocol::error::PusherError;
use crate::protocol::socket_id::SocketId;
use serde_json::{Map, Value};

/// Presence roster. Empty in SP1 (no presence channels yet); filled in SP2.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PresencePayload {
    pub ids: Vec<String>,
    pub hash: Map<String, Value>,
    pub count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ServerEvent {
    ConnectionEstablished {
        socket_id: SocketId,
        activity_timeout: u32,
    },
    Pong,
    SubscriptionSucceeded {
        channel: String,
        presence: Option<PresencePayload>,
    },
    SubscriptionCount {
        channel: String,
        count: usize,
    },
    Error(PusherError),
    /// Generic channel delivery (client events SP2, REST triggers SP2).
    ChannelEvent {
        channel: String,
        event: String,
        data: Value,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_construct_and_match() {
        let e = ServerEvent::SubscriptionSucceeded {
            channel: "c".into(),
            presence: None,
        };
        match e {
            ServerEvent::SubscriptionSucceeded { channel, presence } => {
                assert_eq!(channel, "c");
                assert!(presence.is_none());
            }
            _ => panic!("wrong variant"),
        }
    }
}
