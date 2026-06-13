//! Cross-node broadcast seam. SP1 ships only the in-process `Local` impl;
//! a Redis impl lands in SP7 behind the same trait.

pub mod local;

use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use async_trait::async_trait;

#[async_trait]
pub trait Adapter: Send + Sync {
    async fn broadcast(
        &self,
        app: &str,
        channel: &str,
        event: ServerEvent,
        except: Option<SocketId>,
    );
}
