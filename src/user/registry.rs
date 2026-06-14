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
    // (app_id, watched_user_id) -> { socket_id -> watcher handle }
    watchers: DashMap<(String, String), HashMap<SocketId, ConnectionHandle>>,
    // (app_id, socket_id) -> watched user_ids (for O(1) disconnect cleanup)
    watching: DashMap<(String, SocketId), Vec<String>>,
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

    /// Record `watched` for this connection. Returns `(online, newly_watched)`:
    /// `online` is the subset of `watched` that is online NODE-LOCALLY right now;
    /// `newly_watched` is the subset whose LOCAL watcher set went 0→1 here (this node
    /// gained its first watcher of that user), which the cross-node adapter uses to
    /// drive the per-user `watch` Redis-subscription lifecycle.
    pub fn watch(
        &self,
        app: &str,
        handle: ConnectionHandle,
        watched: Vec<String>,
    ) -> (Vec<String>, Vec<String>) {
        let sock = handle.socket_id.clone();
        // Idempotent: drop any prior watch state for this connection before
        // recording the new one, so a re-watch can't leak stale `watchers` entries.
        self.unwatch(app, &sock);
        let mut newly_watched = Vec::new();
        for w in &watched {
            let mut entry = self
                .watchers
                .entry((app.to_string(), w.clone()))
                .or_default();
            // 0→1 LOCAL watcher edge for this user on this node.
            if entry.is_empty() {
                newly_watched.push(w.clone());
            }
            entry.insert(sock.clone(), handle.clone());
        }
        let online = watched
            .iter()
            .filter(|w| self.is_online(app, w))
            .cloned()
            .collect();
        self.watching.insert((app.to_string(), sock), watched);
        (online, newly_watched)
    }

    /// Drop this connection's watch state. Returns the users whose LOCAL watcher set
    /// dropped to empty here (1→0 on this node) — the cross-node adapter uses these to
    /// UNSUBSCRIBE the per-user `watch` Redis channel.
    pub fn unwatch(&self, app: &str, socket_id: &SocketId) -> Vec<String> {
        let Some((_, watched)) = self.watching.remove(&(app.to_string(), socket_id.clone())) else {
            return Vec::new();
        };
        let mut now_empty = Vec::new();
        for w in watched {
            let key = (app.to_string(), w.clone());
            // Drop the get_mut guard before the conditional remove (deadlock avoidance),
            // and use remove_if so a concurrent watch that repopulated the set is not
            // clobbered — same pattern as signout / channel::Registry.
            {
                let Some(mut set) = self.watchers.get_mut(&key) else {
                    continue;
                };
                set.remove(socket_id);
            }
            // 1→0 LOCAL watcher edge: report the user only if the set is now empty AND
            // we actually remove it (remove_if re-checks under the shard lock, so a
            // concurrent watch that repopulated the set is not reported as emptied).
            if self
                .watchers
                .remove_if(&key, |_, set| set.is_empty())
                .is_some()
            {
                now_empty.push(w);
            }
        }
        now_empty
    }

    pub fn watchers_of(&self, app: &str, user_id: &str) -> Vec<ConnectionHandle> {
        self.watchers
            .get(&(app.to_string(), user_id.to_string()))
            .map(|e| e.values().cloned().collect())
            .unwrap_or_default()
    }

    /// All local (app, user_id, socket_id) signed-in bindings — for the membership heartbeat.
    pub fn local_bindings(&self) -> Vec<(String, String, SocketId)> {
        self.users
            .iter()
            .flat_map(|e| {
                let (app, user) = e.key().clone();
                e.value()
                    .keys()
                    .cloned()
                    .map(move |sid| (app.clone(), user.clone(), sid))
                    .collect::<Vec<_>>()
            })
            .collect()
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

    #[test]
    fn watch_returns_online_subset_and_watchers_resolve() {
        let r = UserRegistry::new();
        let (online_user, _o) = handle();
        r.signin("app", "b", online_user); // b is online; c is not
        let (watcher, _w) = handle();
        let sock = watcher.socket_id.clone();
        let (online, _newly) = r.watch("app", watcher, vec!["b".into(), "c".into()]);
        assert_eq!(online, vec!["b".to_string()]); // only b currently online
        assert_eq!(r.watchers_of("app", "b").len(), 1);
        let _ = r.unwatch("app", &sock);
        assert!(r.watchers_of("app", "b").is_empty());
    }

    #[test]
    fn rewatch_replaces_prior_watchlist_without_leak() {
        let r = UserRegistry::new();
        let (watcher, _w) = handle();
        let sock = watcher.socket_id.clone();
        // First watch covers a and b.
        r.watch("app", watcher.clone(), vec!["a".into(), "b".into()]);
        assert_eq!(r.watchers_of("app", "a").len(), 1);
        assert_eq!(r.watchers_of("app", "b").len(), 1);
        // Re-watch with a different set must DROP the stale a/b entries (only c remains).
        r.watch("app", watcher, vec!["c".into()]);
        assert!(
            r.watchers_of("app", "a").is_empty(),
            "stale watcher for a leaked"
        );
        assert!(
            r.watchers_of("app", "b").is_empty(),
            "stale watcher for b leaked"
        );
        assert_eq!(r.watchers_of("app", "c").len(), 1);
        // And a final unwatch clears everything.
        let _ = r.unwatch("app", &sock);
        assert!(r.watchers_of("app", "c").is_empty());
    }

    #[test]
    fn local_bindings_enumerates_every_signed_in_connection() {
        let r = UserRegistry::new();
        let (h1, _r1) = handle();
        let (h2, _r2) = handle();
        let (h3, _r3) = handle();
        let s1 = h1.socket_id.clone();
        let s2 = h2.socket_id.clone();
        let s3 = h3.socket_id.clone();
        // Two sockets for user "u" and one for user "v", all under app "app".
        r.signin("app", "u", h1);
        r.signin("app", "u", h2);
        r.signin("app", "v", h3);

        let mut got = r.local_bindings();
        got.sort();
        let mut want = vec![
            ("app".to_string(), "u".to_string(), s1),
            ("app".to_string(), "u".to_string(), s2),
            ("app".to_string(), "v".to_string(), s3),
        ];
        want.sort();
        assert_eq!(
            got, want,
            "local_bindings must enumerate every (app, user, socket) signed-in tuple"
        );
    }

    #[test]
    fn watch_unwatch_report_local_watcher_edges() {
        let r = UserRegistry::new();
        // First watcher of "b": its LOCAL watcher set goes 0→1, so "b" is newly_watched.
        let (w1, _r1) = handle();
        let sock1 = w1.socket_id.clone();
        let (_online, newly) = r.watch("app", w1, vec!["b".into()]);
        assert!(
            newly.contains(&"b".to_string()),
            "first watcher of b must report b as newly_watched"
        );

        // A SECOND, different socket watching "b": the set is already non-empty
        // (1→2), so "b" must NOT be reported as newly_watched again.
        let (w2, _r2) = handle();
        let sock2 = w2.socket_id.clone();
        let (_online2, newly2) = r.watch("app", w2, vec!["b".into()]);
        assert!(
            !newly2.contains(&"b".to_string()),
            "a second watcher of b must NOT re-report b as newly_watched"
        );

        // Unwatching one of the two watchers leaves a watcher behind (2→1): no 1→0 edge.
        let dropped1 = r.unwatch("app", &sock1);
        assert!(
            !dropped1.contains(&"b".to_string()),
            "b still has a watcher → must NOT report b as emptied"
        );

        // Unwatching the LAST watcher of "b" empties the set (1→0): "b" is reported.
        let dropped2 = r.unwatch("app", &sock2);
        assert!(
            dropped2.contains(&"b".to_string()),
            "the last watcher of b leaving must report b as emptied"
        );
        assert!(r.watchers_of("app", "b").is_empty());
    }
}
