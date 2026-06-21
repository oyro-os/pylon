use super::l2::RedisAppCache;
use super::{App, AppLookupError, AppManager};
use moka::future::Cache;
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub max_capacity: u64,
    pub ttl_secs: u64,
    pub neg_max: u64,
    pub neg_ttl_secs: u64,
}

/// Outcome of a cache-miss load, distinguishing "not found" (cache as negative)
/// from a real backend error (propagate, never cache).
enum LoadErr {
    NotFound,
    Lookup(AppLookupError),
}

enum LookupBy {
    Id(String),
    Key(String),
}
impl LookupBy {
    async fn load_from_l2(&self, l2: &RedisAppCache) -> Result<Option<Arc<App>>, AppLookupError> {
        match self {
            LookupBy::Id(id) => l2.get_id(id).await,
            LookupBy::Key(k) => l2.get_key(k).await,
        }
    }
    async fn load_from_driver(
        &self,
        d: &dyn AppManager,
    ) -> Result<Option<Arc<App>>, AppLookupError> {
        match self {
            LookupBy::Id(id) => d.by_id(id).await,
            LookupBy::Key(k) => d.by_key(k).await,
        }
    }
}

pub struct CachingAppManager {
    inner: Arc<dyn AppManager>,
    pos: Cache<String, Arc<App>>,
    neg: Cache<String, ()>,
    l2: Option<Arc<RedisAppCache>>,
}

impl CachingAppManager {
    pub fn new(
        inner: Arc<dyn AppManager>,
        cfg: CacheConfig,
        l2: Option<Arc<RedisAppCache>>,
    ) -> Self {
        let pos = Cache::builder()
            .max_capacity(cfg.max_capacity)
            .time_to_live(Duration::from_secs(cfg.ttl_secs))
            .build();
        let neg = Cache::builder()
            .max_capacity(cfg.neg_max)
            .time_to_live(Duration::from_secs(cfg.neg_ttl_secs))
            .build();
        Self {
            inner,
            pos,
            neg,
            l2,
        }
    }

    async fn cached(&self, pkey: String, by: LookupBy) -> Result<Option<Arc<App>>, AppLookupError> {
        if self.neg.get(&pkey).await.is_some() {
            return Ok(None);
        }
        let inner = self.inner.clone();
        let l2 = self.l2.clone();
        let res = self
            .pos
            .try_get_with(pkey.clone(), async move {
                // L2 first — best-effort: errors degrade to the driver, never fail the lookup.
                if let Some(l2) = &l2 {
                    match by.load_from_l2(l2).await {
                        Ok(Some(app)) => return Ok(app),
                        Ok(None) => {}
                        Err(e) => tracing::warn!(error = %e, "app L2 get failed; using driver"),
                    }
                }
                match by.load_from_driver(&*inner).await {
                    Ok(Some(app)) => {
                        if let Some(l2) = &l2 {
                            if let Err(e) = l2.put(&app).await {
                                tracing::warn!(error = %e, "app L2 put failed (ignored)");
                            }
                        }
                        Ok(app)
                    }
                    Ok(None) => Err(LoadErr::NotFound),
                    Err(e) => Err(LoadErr::Lookup(e)),
                }
            })
            .await;

        match res {
            Ok(app) => Ok(Some(app)),
            Err(arc) => match &*arc {
                LoadErr::NotFound => {
                    self.neg.insert(pkey, ()).await;
                    Ok(None)
                }
                LoadErr::Lookup(e) => Err(e.clone()),
            },
        }
    }

    /// Evict an app from L1 (positive + negative, both id and key aliases) and L2.
    /// L2 errors are best-effort (logged). Carries `key` so the key alias evicts
    /// reliably without waiting for TTL.
    pub async fn invalidate(&self, id: &str, key: &str) {
        let id_pkey = format!("id:{id}");
        let key_pkey = format!("key:{key}");
        self.pos.invalidate(&id_pkey).await;
        self.pos.invalidate(&key_pkey).await;
        self.neg.invalidate(&id_pkey).await;
        self.neg.invalidate(&key_pkey).await;
        if let Some(l2) = &self.l2 {
            if let Err(e) = l2.del(id, key).await {
                tracing::warn!(error = %e, "L2 del during invalidate failed (ignored)");
            }
        }
    }
}

#[async_trait::async_trait]
impl AppManager for CachingAppManager {
    async fn by_id(&self, id: &str) -> Result<Option<Arc<App>>, AppLookupError> {
        self.cached(format!("id:{id}"), LookupBy::Id(id.to_string()))
            .await
    }
    async fn by_key(&self, key: &str) -> Result<Option<Arc<App>>, AppLookupError> {
        self.cached(format!("key:{key}"), LookupBy::Key(key.to_string()))
            .await
    }

    fn by_key_cached(&self, key: &str) -> Option<Result<Option<Arc<App>>, AppLookupError>> {
        // SYNC L1-ONLY probe. Use the SAME pkey format as `by_key`. Touch ONLY the
        // in-memory `neg`/`pos` moka caches — never `inner` (the driver) or `l2`
        // (Redis): those are the I/O we offload. `block_on` of an in-memory moka
        // `get` is instant (no reactor/IO is driven — it is exactly what today's
        // establish `block_on` already drives on a hit). The cache never stores
        // errors, so this never yields `Some(Err(_))`.
        let pkey = format!("key:{key}");
        if futures_executor::block_on(self.neg.get(&pkey)).is_some() {
            return Some(Ok(None));
        }
        futures_executor::block_on(self.pos.get(&pkey)).map(|app| Ok(Some(app)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::{App, AppLookupError, AppManager};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn app(id: &str, key: &str) -> Arc<App> {
        let mut a: App = serde_json::from_value(serde_json::json!({
            "name":"t","id":id,"key":key,"secret":"s","enabled":true}))
        .unwrap();
        a.recompute_has_flags();
        Arc::new(a)
    }

    struct Mock {
        app: Option<Arc<App>>,
        fail: bool,
        calls: Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl AppManager for Mock {
        async fn by_id(&self, _id: &str) -> Result<Option<Arc<App>>, AppLookupError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Err(AppLookupError::Backend("boom".into()))
            } else {
                Ok(self.app.clone())
            }
        }
        async fn by_key(&self, _k: &str) -> Result<Option<Arc<App>>, AppLookupError> {
            self.by_id(_k).await
        }
    }
    fn cfg() -> CacheConfig {
        CacheConfig {
            max_capacity: 100,
            ttl_secs: 60,
            neg_max: 100,
            neg_ttl_secs: 60,
        }
    }
    fn mock(app: Option<Arc<App>>, fail: bool) -> (Arc<dyn AppManager>, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Arc::new(Mock {
                app,
                fail,
                calls: calls.clone(),
            }),
            calls,
        )
    }

    #[tokio::test]
    async fn hit_serves_from_l1_without_touching_driver_again() {
        let (m, calls) = mock(Some(app("a", "k")), false);
        let c = CachingAppManager::new(m, cfg(), None);
        assert_eq!(c.by_id("a").await.unwrap().unwrap().key, "k");
        assert_eq!(c.by_id("a").await.unwrap().unwrap().key, "k");
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "second lookup must be an L1 hit"
        );
    }

    #[tokio::test]
    async fn negative_is_cached_separately_and_not_refetched() {
        let (m, calls) = mock(None, false);
        let c = CachingAppManager::new(m, cfg(), None);
        assert!(c.by_id("nope").await.unwrap().is_none());
        assert!(c.by_id("nope").await.unwrap().is_none());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "negative result must be cached"
        );
    }

    #[tokio::test]
    async fn driver_error_propagates_and_is_not_cached() {
        let (m, calls) = mock(None, true);
        let c = CachingAppManager::new(m, cfg(), None);
        assert!(matches!(
            c.by_id("x").await,
            Err(AppLookupError::Backend(_))
        ));
        assert!(matches!(
            c.by_id("x").await,
            Err(AppLookupError::Backend(_))
        ));
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "errors must NOT be cached (driver retried)"
        );
    }

    #[tokio::test]
    async fn concurrent_misses_collapse_to_one_driver_call() {
        let (m, calls) = mock(Some(app("a", "k")), false);
        let c = Arc::new(CachingAppManager::new(m, cfg(), None));
        let mut hs = Vec::new();
        for _ in 0..50 {
            let c = c.clone();
            hs.push(tokio::spawn(async move { c.by_id("a").await }));
        }
        for h in hs {
            h.await.unwrap().unwrap().unwrap();
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "single-flight: 50 concurrent misses => 1 driver call"
        );
    }

    #[tokio::test]
    async fn invalidate_evicts_l1_positive() {
        let (m, calls) = mock(Some(app("a", "k")), false);
        let c = CachingAppManager::new(m, cfg(), None);
        assert_eq!(c.by_id("a").await.unwrap().unwrap().key, "k"); // caches it (driver call 1)
        c.invalidate("a", "k").await;
        assert_eq!(c.by_id("a").await.unwrap().unwrap().key, "k"); // re-fetch (driver call 2)
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "invalidate must evict L1"
        );
    }

    #[tokio::test]
    async fn invalidate_evicts_negative_and_key_alias() {
        let (m, calls) = mock(Some(app("a", "k")), false);
        let c = CachingAppManager::new(m, cfg(), None);
        assert!(c.by_key("k").await.unwrap().is_some()); // caches key alias (call 1)
        c.invalidate("a", "k").await;
        assert!(c.by_key("k").await.unwrap().is_some()); // re-fetch by key (call 2)
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "invalidate must evict the key alias too"
        );
    }

    #[tokio::test]
    async fn l2_hit_avoids_driver() {
        // populate L2, then a CachingAppManager whose driver would PANIC if called serves from L2.
        let url = std::env::var("PYLON_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6390".into());
        let l2 = Arc::new(
            crate::app::l2::RedisAppCache::connect(&url, 2, 60)
                .await
                .unwrap(),
        );
        let a = app(
            &format!("id-{}", uuid::Uuid::new_v4()),
            &format!("key-{}", uuid::Uuid::new_v4()),
        );
        l2.put(&a).await.unwrap();
        let (m, calls) = mock(None, true); // driver returns Err if reached
        let c = CachingAppManager::new(m, cfg(), Some(l2));
        let got = c.by_id(&a.id).await.unwrap().expect("served from L2");
        assert_eq!(got.key, a.key);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "L2 hit must not reach the driver"
        );
    }

    #[tokio::test]
    async fn by_key_cached_returns_none_on_cold_l1_without_touching_driver() {
        let (m, calls) = mock(Some(app("a", "k")), false);
        let c = CachingAppManager::new(m, cfg(), None);
        // Cold: nothing in L1 yet → must offload (None) and NOT call the driver.
        assert!(
            c.by_key_cached("k").is_none(),
            "cold probe must return None"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "cold probe must not call the driver"
        );
    }

    #[tokio::test]
    async fn by_key_cached_returns_some_some_when_l1_warm() {
        let (m, _calls) = mock(Some(app("a", "k")), false);
        let c = CachingAppManager::new(m, cfg(), None);
        // Warm the positive L1 via the normal async path.
        assert_eq!(c.by_key("k").await.unwrap().unwrap().key, "k");
        let probed = c.by_key_cached("k").expect("warm L1 resolves");
        assert_eq!(probed.unwrap().unwrap().key, "k");
    }

    #[tokio::test]
    async fn by_key_cached_returns_some_none_when_neg_cached() {
        let (m, _calls) = mock(None, false);
        let c = CachingAppManager::new(m, cfg(), None);
        // Warm the negative L1: a miss caches "not found".
        assert!(c.by_key("k").await.unwrap().is_none());
        let probed = c.by_key_cached("k").expect("neg-cached resolves");
        assert!(
            probed.unwrap().is_none(),
            "neg-cached probe is Some(Ok(None))"
        );
    }
}
