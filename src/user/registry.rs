//! In-memory user/connection index behind the Adapter seam. A user is "online"
//! while signed in on >= 1 connection. Keyed by (app_id, user_id). The Redis
//! equivalent lands in SP7 behind the same Adapter methods.

use crate::connection::handle::ConnectionHandle;
use crate::protocol::socket_id::SocketId;
use crate::user::{UserJoinOutcome, UserLeaveOutcome};
use dashmap::DashMap;
use std::collections::HashMap;

#[derive(Default)]
pub struct UserRegistry {
    // (app_id, user_id) -> { socket_id -> handle }
    users: DashMap<(String, String), HashMap<SocketId, ConnectionHandle>>,
}

impl UserRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn signin(&self, app: &str, user_id: &str, handle: ConnectionHandle) -> UserJoinOutcome {
        let mut entry = self
            .users
            .entry((app.to_string(), user_id.to_string()))
            .or_default();
        let first_for_user = entry.is_empty();
        entry.insert(handle.socket_id.clone(), handle);
        UserJoinOutcome { first_for_user }
    }

    pub fn signout(&self, app: &str, user_id: &str, socket_id: &SocketId) -> UserLeaveOutcome {
        let key = (app.to_string(), user_id.to_string());
        let last_for_user = {
            let Some(mut entry) = self.users.get_mut(&key) else {
                return UserLeaveOutcome {
                    last_for_user: false,
                };
            };
            entry.remove(socket_id);
            entry.is_empty()
        };
        // Only delete the (app,user) entry if it is STILL empty — a concurrent
        // signin may have repopulated it after we dropped the guard. remove_if
        // re-checks under the shard lock, mirroring channel::Registry.
        self.users.remove_if(&key, |_, sockets| sockets.is_empty());
        UserLeaveOutcome { last_for_user }
    }

    pub fn handles(&self, app: &str, user_id: &str) -> Vec<ConnectionHandle> {
        self.users
            .get(&(app.to_string(), user_id.to_string()))
            .map(|e| e.values().cloned().collect())
            .unwrap_or_default()
    }

    pub fn is_online(&self, app: &str, user_id: &str) -> bool {
        self.users
            .get(&(app.to_string(), user_id.to_string()))
            .is_some_and(|e| !e.is_empty())
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
    fn first_and_subsequent_signin_flags() {
        let r = UserRegistry::new();
        let (h1, _r1) = handle();
        let (h2, _r2) = handle();
        assert!(r.signin("app", "u", h1.clone()).first_for_user);
        assert!(!r.signin("app", "u", h2).first_for_user);
        assert!(r.is_online("app", "u"));
        assert_eq!(r.handles("app", "u").len(), 2);
    }

    #[test]
    fn signout_reports_last_and_clears() {
        let r = UserRegistry::new();
        let (h1, _r1) = handle();
        let (h2, _r2) = handle();
        let s1 = h1.socket_id.clone();
        let s2 = h2.socket_id.clone();
        r.signin("app", "u", h1);
        r.signin("app", "u", h2);
        assert!(!r.signout("app", "u", &s1).last_for_user);
        assert!(r.signout("app", "u", &s2).last_for_user);
        assert!(!r.is_online("app", "u"));
    }
}
