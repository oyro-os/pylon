//! Per-(app, channel) subscriber registry. Stores mailbox handles, never sockets.

use crate::connection::handle::ConnectionHandle;
use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use dashmap::DashMap;

#[derive(Default)]
pub struct Registry {
    channels: DashMap<(String, String), DashMap<SocketId, ConnectionHandle>>,
}

impl Registry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe(&self, app: &str, channel: &str, handle: ConnectionHandle) {
        let key = (app.to_string(), channel.to_string());
        self.channels
            .entry(key)
            .or_default()
            .insert(handle.socket_id.clone(), handle);
    }

    pub fn unsubscribe(&self, app: &str, channel: &str, socket_id: &SocketId) {
        let key = (app.to_string(), channel.to_string());
        let now_empty = match self.channels.get(&key) {
            Some(subs) => {
                subs.remove(socket_id);
                subs.is_empty()
            }
            None => false,
        };
        if now_empty {
            self.channels.remove_if(&key, |_, subs| subs.is_empty());
        }
    }

    pub fn count(&self, app: &str, channel: &str) -> usize {
        self.channels
            .get(&(app.to_string(), channel.to_string()))
            .map(|s| s.len())
            .unwrap_or(0)
    }

    pub fn broadcast(
        &self,
        app: &str,
        channel: &str,
        event: &ServerEvent,
        except: Option<&SocketId>,
    ) {
        if let Some(subs) = self.channels.get(&(app.to_string(), channel.to_string())) {
            for entry in subs.iter() {
                if Some(entry.key()) == except {
                    continue;
                }
                let _ = entry.value().mailbox.send(event.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn subscribe_and_count() {
        let reg = Registry::new();
        let (h, _rx) = handle();
        reg.subscribe("app", "c", h);
        assert_eq!(reg.count("app", "c"), 1);
    }

    #[test]
    fn broadcast_reaches_subscriber_and_excludes_sender() {
        let reg = Registry::new();
        let (h1, mut rx1) = handle();
        let (h2, mut rx2) = handle();
        let sid1 = h1.socket_id.clone();
        reg.subscribe("app", "c", h1);
        reg.subscribe("app", "c", h2);
        reg.broadcast("app", "c", &ServerEvent::Pong, Some(&sid1));
        assert!(rx1.try_recv().is_err(), "excluded socket must not receive");
        assert!(matches!(rx2.try_recv(), Ok(ServerEvent::Pong)));
    }

    #[test]
    fn unsubscribe_prunes_empty_channel() {
        let reg = Registry::new();
        let (h, _rx) = handle();
        let sid = h.socket_id.clone();
        reg.subscribe("app", "c", h);
        reg.unsubscribe("app", "c", &sid);
        assert_eq!(reg.count("app", "c"), 0);
    }
}
