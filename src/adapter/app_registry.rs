//! Process-global per-app connection index, maintained beside `conn_counts`
//! (insert on establish, remove on close), mirroring `UserRegistry`. Needed
//! because enumerating via the channel registry would MISS idle (connected-but-
//! unsubscribed) connections, and `purge_app` eviction must be complete.

use crate::connection::handle::ConnectionHandle;
use crate::protocol::socket_id::SocketId;
use dashmap::DashMap;
use std::collections::HashMap;

/// `app_id -> { socket_id -> handle }`. One entry per app that has >= 1 live
/// connection on this process; the entry is removed when its last socket leaves
/// (`remove_if(empty)`), exactly like `UserRegistry` and `channel::Registry`.
#[derive(Default)]
pub struct AppRegistry {
    apps: DashMap<String, HashMap<SocketId, ConnectionHandle>>,
}

impl AppRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a live connection under its app. Called once at session establish.
    pub fn insert(&self, app_id: &str, handle: ConnectionHandle) {
        self.apps
            .entry(app_id.to_string())
            .or_default()
            .insert(handle.socket_id, handle);
    }

    /// Drop one socket from its app. If that empties the app's set, remove the app
    /// entry (re-checked under the shard lock via `remove_if`, so a concurrent
    /// `insert` that repopulated the set is not clobbered). Called once at close.
    pub fn remove(&self, app_id: &str, socket_id: &SocketId) {
        {
            let Some(mut set) = self.apps.get_mut(app_id) else {
                return;
            };
            set.remove(socket_id);
        }
        self.apps.remove_if(app_id, |_, set| set.is_empty());
    }

    /// Remove the WHOLE app entry and return its handles (for `purge_app`). The
    /// entry is gone after this call, so a concurrent connect re-creates it fresh.
    pub fn drain_app(&self, app_id: &str) -> Vec<ConnectionHandle> {
        self.apps
            .remove(app_id)
            .map(|(_, set)| set.into_values().collect())
            .unwrap_or_default()
    }

    /// Snapshot of the distinct app ids with >= 1 live connection (for the sweep).
    pub fn connected_app_ids(&self) -> Vec<String> {
        self.apps.iter().map(|e| e.key().clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::event::ServerEvent;
    use tokio::sync::mpsc;

    fn handle() -> (ConnectionHandle, mpsc::Receiver<Box<ServerEvent>>) {
        let (tx, rx) = mpsc::channel(1024);
        (
            ConnectionHandle {
                socket_id: SocketId::generate(),
                mailbox: crate::connection::handle::Mailbox::new(tx, None, None),
            },
            rx,
        )
    }

    #[test]
    fn insert_then_drain_returns_all_handles_and_clears() {
        let r = AppRegistry::new();
        let (h1, _r1) = handle();
        let (h2, _r2) = handle();
        let s1 = h1.socket_id;
        let s2 = h2.socket_id;
        r.insert("app", h1);
        r.insert("app", h2);
        let mut got: Vec<SocketId> = r.drain_app("app").iter().map(|h| h.socket_id).collect();
        got.sort();
        let mut want = vec![s1, s2];
        want.sort();
        assert_eq!(got, want, "drain_app must return every registered handle");
        // The app entry is gone after a drain.
        assert!(r.drain_app("app").is_empty());
        assert!(r.connected_app_ids().is_empty());
    }

    #[test]
    fn remove_clears_app_entry_when_last_socket_leaves() {
        let r = AppRegistry::new();
        let (h1, _r1) = handle();
        let (h2, _r2) = handle();
        let s1 = h1.socket_id;
        let s2 = h2.socket_id;
        r.insert("app", h1);
        r.insert("app", h2);
        // Removing one socket leaves the app entry (still has the other).
        r.remove("app", &s1);
        assert_eq!(r.connected_app_ids(), vec!["app".to_string()]);
        // Removing the last socket removes the whole app entry (remove_if-empty).
        r.remove("app", &s2);
        assert!(
            r.connected_app_ids().is_empty(),
            "empty app entry must be removed"
        );
    }

    #[test]
    fn remove_of_absent_socket_is_a_noop() {
        let r = AppRegistry::new();
        let (h1, _r1) = handle();
        r.insert("app", h1);
        // A socket_id never inserted: must not panic and must not drop the app entry.
        r.remove("app", &SocketId::generate());
        assert_eq!(r.connected_app_ids(), vec!["app".to_string()]);
        // A remove against an app that doesn't exist: also a no-op.
        r.remove("missing", &SocketId::generate());
    }

    #[test]
    fn connected_app_ids_snapshots_distinct_apps() {
        let r = AppRegistry::new();
        let (a1, _ra1) = handle();
        let (a2, _ra2) = handle();
        let (b1, _rb1) = handle();
        r.insert("a", a1);
        r.insert("a", a2); // two sockets, one app
        r.insert("b", b1);
        let mut ids = r.connected_app_ids();
        ids.sort();
        assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
    }
}
