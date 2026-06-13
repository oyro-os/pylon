pub mod frames;

use crate::protocol::codec::{Capabilities, Codec, DecodeError};
use crate::protocol::command::ClientCommand;
use crate::protocol::event::ServerEvent;

pub struct V7Codec;

impl Codec for V7Codec {
    fn version(&self) -> u8 {
        7
    }
    fn capabilities(&self) -> Capabilities {
        Capabilities {
            client_events: true,
            presence: true,
        }
    }
    fn decode(&self, text: &str) -> Result<ClientCommand, DecodeError> {
        frames::decode(text)
    }
    fn encode(&self, event: &ServerEvent) -> String {
        frames::encode(event)
    }
}
