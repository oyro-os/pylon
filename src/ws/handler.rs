//! Per-connection command dispatch. Version-agnostic: operates on domain types.

use crate::adapter::Adapter;
use crate::app::App;
use crate::connection::handle::ConnectionHandle;
use crate::protocol::command::ClientCommand;
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
    pub user: Option<crate::user::AuthenticatedUser>,
}

impl ConnectionContext {
    pub(in crate::ws) fn handle(&self) -> ConnectionHandle {
        ConnectionHandle {
            socket_id: self.socket_id.clone(),
            mailbox: self.self_tx.clone(),
        }
    }

    pub(in crate::ws) fn send_self(&self, event: ServerEvent) {
        let _ = self.self_tx.send(event);
    }

    /// Push a one-change `watchlist_events` frame to every connection watching `user_id`.
    pub(in crate::ws) async fn notify_watchers(&self, user_id: &str, name: &str) {
        let watchers = self.adapter.watchers_of(&self.app.id, user_id).await;
        if watchers.is_empty() {
            return;
        }
        let ev = ServerEvent::WatchlistEvents {
            events: vec![crate::protocol::event::WatchlistChange {
                name: name.to_string(),
                user_ids: vec![user_id.to_string()],
            }],
        };
        for h in watchers {
            let _ = h.mailbox.send(ev.clone());
        }
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
            ClientCommand::ClientEvent {
                event,
                channel,
                data,
            } => self.client_event(event, channel, data).await,
            ClientCommand::Signin { auth, user_data } => self.signin(auth, user_data).await,
            ClientCommand::Unknown(_) => {}
        }
    }

    async fn unsubscribe(&mut self, channel: String) {
        if self.subscribed.remove(&channel) {
            let out = self
                .adapter
                .unsubscribe(&self.app.id, &channel, &self.socket_id)
                .await;
            if let Some(leave) = out.presence {
                if leave.last_for_user {
                    self.adapter
                        .broadcast(
                            &self.app.id,
                            &channel,
                            ServerEvent::MemberRemoved {
                                channel: channel.clone(),
                                user_id: leave.user_id,
                            },
                            None,
                        )
                        .await;
                }
            }
            self.maybe_emit_count(&channel, out.subscription_count)
                .await;
        }
    }

    pub(in crate::ws) async fn maybe_emit_count(&self, channel: &str, count: usize) {
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

    /// On disconnect: leave channels, sign out + emit watchlist offline if this
    /// was the user's last connection, and drop this connection's own watches.
    pub async fn on_close(&mut self) {
        let channels: Vec<String> = self.subscribed.iter().cloned().collect();
        for channel in channels {
            self.unsubscribe(channel).await;
        }
        if let Some(user) = self.user.take() {
            let outcome = self
                .adapter
                .signout_user(&self.app.id, &user.id, &self.socket_id)
                .await;
            if outcome.last_for_user {
                self.notify_watchers(&user.id, "offline").await;
            }
        }
        self.adapter.unwatch(&self.app.id, &self.socket_id).await;
    }
}

#[cfg(test)]
#[path = "handler_tests.rs"]
mod tests;
