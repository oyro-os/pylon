//! Per-(app, channel) registry over `ChannelState`. Stores mailbox handles +
//! presence membership, never sockets. Private store behind `LocalAdapter`.

use crate::channel::outcome::{ChannelSummary, SubscribeOutcome, UnsubscribeOutcome};
use crate::channel::state::ChannelState;
use crate::connection::handle::ConnectionHandle;
use crate::presence::member::PresenceMember;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use dashmap::DashMap;

#[derive(Default)]
pub struct Registry {
    channels: DashMap<(String, String), ChannelState>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe(
        &self,
        app: &str,
        channel: &str,
        handle: ConnectionHandle,
        member: Option<PresenceMember>,
    ) -> SubscribeOutcome {
        let key = (app.to_string(), channel.to_string());
        let mut state = self.channels.entry(key).or_default();
        let presence = state.add(handle, member);
        SubscribeOutcome {
            subscription_count: state.subscription_count(),
            presence,
        }
    }

    pub fn unsubscribe(
        &self,
        app: &str,
        channel: &str,
        socket_id: &SocketId,
    ) -> UnsubscribeOutcome {
        let key = (app.to_string(), channel.to_string());
        let (count, presence, now_empty) = match self.channels.get_mut(&key) {
            Some(mut state) => {
                let presence = state.remove(socket_id);
                (state.subscription_count(), presence, state.is_empty())
            }
            None => (0, None, false),
        };
        if now_empty {
            self.channels.remove_if(&key, |_, s| s.is_empty());
        }
        UnsubscribeOutcome {
            subscription_count: count,
            presence,
        }
    }

    pub fn broadcast(
        &self,
        app: &str,
        channel: &str,
        event: &ServerEvent,
        except: Option<&SocketId>,
    ) {
        if let Some(state) = self.channels.get(&(app.to_string(), channel.to_string())) {
            state.broadcast(event, except);
        }
    }

    pub fn channel_summary(&self, app: &str, channel: &str) -> ChannelSummary {
        match self.channels.get(&(app.to_string(), channel.to_string())) {
            Some(s) => ChannelSummary {
                name: channel.to_string(),
                occupied: !s.is_empty(),
                subscription_count: s.subscription_count(),
                user_count: s.user_count(),
            },
            None => ChannelSummary {
                name: channel.to_string(),
                occupied: false,
                subscription_count: 0,
                user_count: None,
            },
        }
    }

    pub fn channels(&self, app: &str, prefix: Option<&str>) -> Vec<ChannelSummary> {
        self.channels
            .iter()
            .filter(|e| e.key().0 == app && !e.value().is_empty())
            .filter(|e| prefix.is_none_or(|p| e.key().1.starts_with(p)))
            .map(|e| ChannelSummary {
                name: e.key().1.clone(),
                occupied: true,
                subscription_count: e.value().subscription_count(),
                user_count: e.value().user_count(),
            })
            .collect()
    }

    pub fn presence_members(&self, app: &str, channel: &str) -> Vec<PresenceMember> {
        self.channels
            .get(&(app.to_string(), channel.to_string()))
            .map(|s| s.members())
            .unwrap_or_default()
    }

    /// Number of tracked `(app, channel)` entries. Test-only.
    #[cfg(test)]
    pub fn channel_entry_count(&self) -> usize {
        self.channels.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::event::ServerEvent;
    use tokio::sync::mpsc;

    fn handle() -> (ConnectionHandle, mpsc::UnboundedReceiver<ServerEvent>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            ConnectionHandle {
                socket_id: SocketId::generate(),
                mailbox: tx,
            },
            rx,
        )
    }

    #[test]
    fn subscribe_counts_and_broadcasts_excluding_sender() {
        let reg = Registry::new();
        let (h1, mut rx1) = handle();
        let (h2, mut rx2) = handle();
        let sid1 = h1.socket_id.clone();
        assert_eq!(reg.subscribe("app", "c", h1, None).subscription_count, 1);
        assert_eq!(reg.subscribe("app", "c", h2, None).subscription_count, 2);
        reg.broadcast("app", "c", &ServerEvent::Pong, Some(&sid1));
        assert!(rx1.try_recv().is_err());
        assert!(matches!(rx2.try_recv(), Ok(ServerEvent::Pong)));
    }

    #[test]
    fn unsubscribe_prunes_empty_channel() {
        let reg = Registry::new();
        let (h, _rx) = handle();
        let sid = h.socket_id.clone();
        reg.subscribe("app", "c", h, None);
        let out = reg.unsubscribe("app", "c", &sid);
        assert_eq!(out.subscription_count, 0);
        assert_eq!(
            reg.channel_entry_count(),
            0,
            "empty channel entry must be pruned"
        );
    }

    #[test]
    fn channels_query_filters_by_prefix() {
        let reg = Registry::new();
        let (h1, _r1) = handle();
        let (h2, _r2) = handle();
        reg.subscribe("app", "private-a", h1, None);
        reg.subscribe("app", "public-b", h2, None);
        let names: Vec<String> = reg
            .channels("app", Some("private-"))
            .into_iter()
            .map(|s| s.name)
            .collect();
        assert_eq!(names, vec!["private-a".to_string()]);
    }
}
