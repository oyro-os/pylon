//! Per-connection command dispatch. Version-agnostic: operates on domain types.

use crate::adapter::Adapter;
use crate::app::App;
use crate::channel::kind::ChannelKind;
use crate::connection::handle::ConnectionHandle;
use crate::protocol::command::ClientCommand;
use crate::protocol::error::PusherError;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

pub struct ConnectionContext {
    pub app: App,
    pub socket_id: SocketId,
    pub self_tx: UnboundedSender<ServerEvent>,
    pub adapter: Arc<dyn Adapter>,
    pub limits: crate::server::config::Limits,
    pub subscribed: HashSet<String>,
}

impl ConnectionContext {
    fn handle(&self) -> ConnectionHandle {
        ConnectionHandle {
            socket_id: self.socket_id.clone(),
            mailbox: self.self_tx.clone(),
        }
    }

    fn send_self(&self, event: ServerEvent) {
        let _ = self.self_tx.send(event);
    }

    pub async fn dispatch(&mut self, cmd: ClientCommand) {
        match cmd {
            ClientCommand::Ping => self.send_self(ServerEvent::Pong),
            ClientCommand::Subscribe {
                channel,
                auth,
                channel_data,
            } => self.subscribe(channel, auth, channel_data).await,
            ClientCommand::Unsubscribe { channel } => self.unsubscribe(channel).await,
            ClientCommand::ClientEvent { .. } => {
                // Client events are valid only on private/presence channels (SP2).
                self.send_self(ServerEvent::Error(PusherError::new(
                    4301,
                    "Client events are only supported on private and presence channels",
                )));
            }
            ClientCommand::Unknown(_) => {}
        }
    }

    async fn subscribe(
        &mut self,
        channel: String,
        auth: Option<String>,
        _channel_data: Option<String>,
    ) {
        match ChannelKind::of(&channel) {
            ChannelKind::Public => {
                let out = self
                    .adapter
                    .subscribe(&self.app.id, &channel, self.handle(), None)
                    .await;
                self.subscribed.insert(channel.clone());
                self.send_self(ServerEvent::SubscriptionSucceeded {
                    channel: channel.clone(),
                    presence: None,
                });
                self.maybe_emit_count(&channel, out.subscription_count)
                    .await;
            }
            ChannelKind::Private => {
                let token = match auth.as_deref() {
                    Some(t) => t,
                    None => {
                        return self.send_subscription_error(
                            &channel,
                            "AuthError",
                            "Auth signature required",
                            401,
                        )
                    }
                };
                if let Err(e) = crate::auth::channel::verify(
                    &self.app.key,
                    &self.app.secret,
                    self.socket_id.as_str(),
                    &channel,
                    None,
                    token,
                ) {
                    return self.send_subscription_error(&channel, "AuthError", e.message(), 401);
                }
                let out = self
                    .adapter
                    .subscribe(&self.app.id, &channel, self.handle(), None)
                    .await;
                self.subscribed.insert(channel.clone());
                self.send_self(ServerEvent::SubscriptionSucceeded {
                    channel: channel.clone(),
                    presence: None,
                });
                self.maybe_emit_count(&channel, out.subscription_count)
                    .await;
            }
            // presence / encrypted / cache require auth — SP2/SP3.
            _ => self.send_self(ServerEvent::Error(PusherError::unauthorized())),
        }
    }

    fn send_subscription_error(&self, channel: &str, error_type: &str, error: &str, status: u16) {
        self.send_self(ServerEvent::SubscriptionError {
            channel: channel.to_string(),
            error_type: error_type.to_string(),
            error: error.to_string(),
            status,
        });
    }

    async fn unsubscribe(&mut self, channel: String) {
        if self.subscribed.remove(&channel) {
            let out = self
                .adapter
                .unsubscribe(&self.app.id, &channel, &self.socket_id)
                .await;
            self.maybe_emit_count(&channel, out.subscription_count)
                .await;
        }
    }

    async fn maybe_emit_count(&self, channel: &str, count: usize) {
        if self.app.subscription_count_enabled {
            self.adapter
                .broadcast(
                    &self.app.id,
                    channel,
                    ServerEvent::SubscriptionCount {
                        channel: channel.to_string(),
                        count,
                    },
                    None,
                )
                .await;
        }
    }

    /// On disconnect: leave every channel (emitting count updates).
    pub async fn on_close(&mut self) {
        let channels: Vec<String> = self.subscribed.iter().cloned().collect();
        for channel in channels {
            self.unsubscribe(channel).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::local::LocalAdapter;
    use crate::channel::registry::Registry;
    use serde_json::Value;
    use tokio::sync::mpsc;

    fn app(sub_count: bool) -> App {
        serde_json::from_value::<App>(serde_json::json!({
            "name": "t", "id": "app", "key": "k", "secret": "s",
            "client_messages_enabled": true,
            "subscription_count_enabled": sub_count
        }))
        .unwrap()
    }

    fn ctx(app: App) -> (ConnectionContext, mpsc::UnboundedReceiver<ServerEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let registry = Arc::new(Registry::new());
        let adapter: Arc<dyn Adapter> = Arc::new(LocalAdapter::new(registry));
        let c = ConnectionContext {
            app,
            socket_id: SocketId::generate(),
            self_tx: tx,
            adapter,
            limits: crate::server::config::ServerConfig::default().limits(),
            subscribed: HashSet::new(),
        };
        (c, rx)
    }

    #[tokio::test]
    async fn ping_enqueues_pong() {
        let (mut c, mut rx) = ctx(app(false));
        c.dispatch(ClientCommand::Ping).await;
        assert!(matches!(rx.try_recv(), Ok(ServerEvent::Pong)));
    }

    #[tokio::test]
    async fn public_subscribe_succeeds_and_registers() {
        let (mut c, mut rx) = ctx(app(false));
        c.dispatch(ClientCommand::Subscribe {
            channel: "room".into(),
            auth: None,
            channel_data: None,
        })
        .await;
        assert!(matches!(
            rx.try_recv(),
            Ok(ServerEvent::SubscriptionSucceeded { .. })
        ));
        assert_eq!(c.adapter.channel("app", "room").await.subscription_count, 1);
    }

    #[tokio::test]
    async fn subscription_count_emitted_when_enabled() {
        let (mut c, mut rx) = ctx(app(true));
        c.dispatch(ClientCommand::Subscribe {
            channel: "room".into(),
            auth: None,
            channel_data: None,
        })
        .await;
        assert!(matches!(
            rx.try_recv(),
            Ok(ServerEvent::SubscriptionSucceeded { .. })
        ));
        match rx.try_recv() {
            Ok(ServerEvent::SubscriptionCount { count, .. }) => assert_eq!(count, 1),
            other => panic!("expected SubscriptionCount, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn private_subscribe_without_auth_errors_non_fatally() {
        let (mut c, mut rx) = ctx(app(false));
        c.dispatch(ClientCommand::Subscribe {
            channel: "private-x".into(),
            auth: None,
            channel_data: None,
        })
        .await;
        match rx.try_recv() {
            Ok(ServerEvent::SubscriptionError {
                channel, status, ..
            }) => {
                assert_eq!(channel, "private-x");
                assert_eq!(status, 401);
            }
            other => panic!("expected SubscriptionError, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn private_subscribe_with_valid_auth_succeeds() {
        let (mut c, mut rx) = ctx(app(false));
        let sid = c.socket_id.as_str().to_string();
        let sig = crate::auth::signature::channel_signature("s", &sid, "private-x", None);
        let token = format!("k:{sig}"); // app key "k", secret "s" from the `app()` helper
        c.dispatch(ClientCommand::Subscribe {
            channel: "private-x".into(),
            auth: Some(token),
            channel_data: None,
        })
        .await;
        assert!(matches!(
            rx.try_recv(),
            Ok(ServerEvent::SubscriptionSucceeded { .. })
        ));
    }

    #[tokio::test]
    async fn client_event_rejected_in_sp1() {
        let (mut c, mut rx) = ctx(app(true));
        c.dispatch(ClientCommand::ClientEvent {
            event: "client-x".into(),
            channel: "room".into(),
            data: Value::Null,
        })
        .await;
        match rx.try_recv() {
            Ok(ServerEvent::Error(e)) => assert_eq!(e.code, 4301),
            other => panic!("expected Error 4301, got {other:?}"),
        }
    }
}
