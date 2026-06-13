use super::Adapter;
use crate::channel::registry::Registry;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::handle::ConnectionHandle;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn broadcast_delegates_to_registry() {
        let reg = Arc::new(Registry::new());
        let adapter = LocalAdapter::new(reg.clone());
        let (tx, mut rx) = mpsc::unbounded_channel();
        reg.subscribe(
            "app",
            "c",
            ConnectionHandle {
                socket_id: SocketId::generate(),
                mailbox: tx,
            },
            None,
        );
        adapter.broadcast("app", "c", ServerEvent::Pong, None).await;
        assert!(matches!(rx.try_recv(), Ok(ServerEvent::Pong)));
    }
}
