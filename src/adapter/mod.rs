//! Cross-node channel-state seam. SP2a ships only the in-process `Local` impl;
//! a Redis impl lands in SP7 behind this same trait — no handler changes.

pub mod local;

use crate::channel::outcome::{ChannelSummary, SubscribeOutcome, UnsubscribeOutcome};
use crate::connection::handle::ConnectionHandle;
use crate::presence::member::PresenceMember;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use async_trait::async_trait;

#[async_trait]
pub trait Adapter: Send + Sync {
    async fn subscribe(
        &self,
        app: &str,
        channel: &str,
        handle: ConnectionHandle,
        member: Option<PresenceMember>,
    ) -> SubscribeOutcome;

    async fn unsubscribe(
        &self,
        app: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> UnsubscribeOutcome;

    async fn broadcast(
        &self,
        app: &str,
        channel: &str,
        event: ServerEvent,
        except: Option<SocketId>,
    );

    async fn channels(&self, app: &str, prefix: Option<&str>) -> Vec<ChannelSummary>;

    async fn channel(&self, app: &str, channel: &str) -> ChannelSummary;

    async fn presence_members(&self, app: &str, channel: &str) -> Vec<PresenceMember>;
}
