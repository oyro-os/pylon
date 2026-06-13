use super::Adapter;
use crate::channel::outcome::{ChannelSummary, SubscribeOutcome, UnsubscribeOutcome};
use crate::channel::registry::Registry;
use crate::connection::handle::ConnectionHandle;
use crate::presence::member::PresenceMember;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use async_trait::async_trait;
use std::sync::Arc;

pub struct LocalAdapter {
    registry: Arc<Registry>,
}

impl LocalAdapter {
    pub fn new(registry: Arc<Registry>) -> Self {
        Self { registry }
    }
}

#[async_trait]
impl Adapter for LocalAdapter {
    async fn subscribe(
        &self,
        app: &str,
        channel: &str,
        handle: ConnectionHandle,
        member: Option<PresenceMember>,
    ) -> SubscribeOutcome {
        self.registry.subscribe(app, channel, handle, member)
    }

    async fn unsubscribe(
        &self,
        app: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> UnsubscribeOutcome {
        self.registry.unsubscribe(app, channel, socket_id)
    }

    async fn broadcast(
        &self,
        app: &str,
        channel: &str,
        event: ServerEvent,
        except: Option<SocketId>,
    ) {
        self.registry
            .broadcast(app, channel, &event, except.as_ref());
    }

    async fn channels(&self, app: &str, prefix: Option<&str>) -> Vec<ChannelSummary> {
        self.registry.channels(app, prefix)
    }

    async fn channel(&self, app: &str, channel: &str) -> ChannelSummary {
        self.registry.channel_summary(app, channel)
    }

    async fn presence_members(&self, app: &str, channel: &str) -> Vec<PresenceMember> {
        self.registry.presence_members(app, channel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn subscribe_then_broadcast_delegates_to_registry() {
        let reg = Arc::new(Registry::new());
        let adapter = LocalAdapter::new(reg.clone());
        let (tx, mut rx) = mpsc::unbounded_channel();
        let out = adapter
            .subscribe(
                "app",
                "c",
                ConnectionHandle {
                    socket_id: SocketId::generate(),
                    mailbox: tx,
                },
                None,
            )
            .await;
        assert_eq!(out.subscription_count, 1);
        adapter.broadcast("app", "c", ServerEvent::Pong, None).await;
        assert!(matches!(rx.try_recv(), Ok(ServerEvent::Pong)));
    }

    #[tokio::test]
    async fn presence_members_round_trip() {
        let reg = Arc::new(Registry::new());
        let adapter = LocalAdapter::new(reg.clone());
        let (tx, _rx) = mpsc::unbounded_channel();
        adapter
            .subscribe(
                "app",
                "presence-x",
                ConnectionHandle {
                    socket_id: SocketId::generate(),
                    mailbox: tx,
                },
                Some(PresenceMember {
                    user_id: "u1".into(),
                    user_info: serde_json::json!({}),
                }),
            )
            .await;
        let members = adapter.presence_members("app", "presence-x").await;
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].user_id, "u1");
        assert_eq!(
            adapter.channel("app", "presence-x").await.user_count,
            Some(1)
        );
    }
}
