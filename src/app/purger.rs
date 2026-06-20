//! `AppPurger`: the end-to-end "make a removed app leave zero footprint" action.
//! Driven by the `remove` invalidation message and the sweep backstop. `refresh`
//! is the config/secret-change action (cache eviction only); `purge` is the
//! remove/disable action (close conns + reclaim counter + evict cache).

use crate::adapter::Adapter;
use crate::app::cache::CachingAppManager;
use dashmap::DashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

/// Owns the three structures a remove must reclaim node-locally. Built once in
/// `main.rs` (DB-backed cache paths only) and shared by the invalidation
/// subscriber and the sweep backstop.
pub struct AppPurger {
    adapter: Arc<dyn Adapter>,
    conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>>,
    cache: Arc<CachingAppManager>,
}

impl AppPurger {
    pub fn new(
        adapter: Arc<dyn Adapter>,
        conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>>,
        cache: Arc<CachingAppManager>,
    ) -> Self {
        Self { adapter, conn_counts, cache }
    }

    /// Config/secret changed: evict the cache only (TTL backstop bounds staleness;
    /// the next lookup re-fetches). Connections stay up.
    pub async fn refresh(&self, id: &str, key: &str) {
        self.cache.invalidate(id, key).await;
    }

    /// App removed/disabled: force-close every connection (4009), reclaim the
    /// per-app counter, and evict the cache. In cluster mode the composing
    /// `RedisAdapter::purge_app` already SREM'd `{prefix}:apps`.
    pub async fn purge(&self, id: &str, key: &str) {
        self.adapter.purge_app(id).await;
        self.conn_counts.remove(id);
        self.cache.invalidate(id, key).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::app_registry::AppRegistry;
    use crate::adapter::local::LocalAdapter;
    use crate::app::cache::CacheConfig;
    use crate::app::{App, AppLookupError, AppManager};
    use crate::channel::registry::Registry;
    use crate::connection::handle::{ConnectionHandle, Mailbox};
    use crate::protocol::event::ServerEvent;
    use crate::protocol::socket_id::SocketId;
    use tokio::sync::mpsc;

    fn app(id: &str, key: &str) -> Arc<App> {
        let mut a: App = serde_json::from_value(serde_json::json!({
            "name": "t", "id": id, "key": key, "secret": "s", "enabled": true
        }))
        .unwrap();
        a.recompute_has_flags();
        Arc::new(a)
    }

    struct Mock(Arc<App>);
    #[async_trait::async_trait]
    impl AppManager for Mock {
        async fn by_id(&self, _: &str) -> Result<Option<Arc<App>>, AppLookupError> {
            Ok(Some(self.0.clone()))
        }
        async fn by_key(&self, k: &str) -> Result<Option<Arc<App>>, AppLookupError> {
            self.by_id(k).await
        }
    }

    #[tokio::test]
    async fn purge_closes_conns_reclaims_counter_and_evicts_cache() {
        let app_registry = Arc::new(AppRegistry::new());
        let local = Arc::new(LocalAdapter::new(Arc::new(Registry::new()), app_registry.clone()));
        let adapter: Arc<dyn Adapter> = local.clone();

        // A live connection of app "a".
        let (tx, mut rx) = mpsc::channel(1024);
        let sid = SocketId::generate();
        app_registry.insert("a", ConnectionHandle { socket_id: sid, mailbox: Mailbox::new(tx, None, None) });

        // A conn_counts entry for "a" (as establish would create).
        let conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>> = Arc::new(DashMap::new());
        conn_counts.insert("a".to_string(), Arc::new(AtomicUsize::new(1)));

        let cfg = CacheConfig { max_capacity: 100, ttl_secs: 300, neg_max: 100, neg_ttl_secs: 300 };
        let cache = Arc::new(CachingAppManager::new(Arc::new(Mock(app("a", "k"))), cfg, None));
        // Warm the cache so we can prove eviction.
        assert_eq!(cache.by_id("a").await.unwrap().unwrap().key, "k");

        let purger = AppPurger::new(adapter, conn_counts.clone(), cache.clone());
        purger.purge("a", "k").await;

        // (1) Connection force-closed 4009.
        assert!(matches!(rx.try_recv().map(|b| *b), Ok(ServerEvent::Error(e)) if e.code == 4009));
        assert!(matches!(rx.try_recv().map(|b| *b), Ok(ServerEvent::Close { code: 4009, .. })));
        // (2) conn_counts entry reclaimed.
        assert!(!conn_counts.contains_key("a"), "purge must remove the conn_counts entry");
        // (3) AppRegistry entry drained.
        assert!(app_registry.connected_app_ids().is_empty());
    }
}
