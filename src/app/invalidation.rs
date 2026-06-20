use super::cache::CachingAppManager;
use fred::interfaces::PubsubInterface;
use fred::prelude::*;
use std::sync::Arc;

/// Cross-node app-cache invalidation over Redis pub/sub.
pub struct AppInvalidator {
    publish_pool: Pool,
}

pub const INVALIDATE_CHANNEL: &str = "pylon:app:invalidate";

#[derive(serde::Serialize, serde::Deserialize)]
struct InvalidateMsg {
    id: String,
    key: String,
}

impl AppInvalidator {
    /// Connect to `url`, subscribe to the invalidation channel (evicting `cache`
    /// on each message), and return a handle that can publish invalidations.
    pub async fn spawn(url: &str, cache: Arc<CachingAppManager>) -> anyhow::Result<Arc<Self>> {
        // `max_attempts = 0` means retry forever; min 100ms, max 30s, base 2.
        let policy = ReconnectPolicy::new_exponential(0, 100, 30_000, 2);
        let mut builder = Builder::from_config(Config::from_url(url)?);
        builder.set_policy(policy);

        let publish_pool = builder.build_pool(2)?;
        publish_pool.init().await?;

        let sub = builder.build_subscriber_client()?;
        sub.init().await?;
        // Keep the resubscribe task handle so it isn't dropped (which would stop it).
        let _mgr = sub.manage_subscriptions();
        sub.subscribe(INVALIDATE_CHANNEL).await?;
        let mut rx = sub.message_rx();
        tokio::spawn(async move {
            // hold `sub` and `_mgr` for the task's lifetime so the subscription stays open
            let _sub = sub;
            let _sub_mgr = _mgr;
            loop {
                match rx.recv().await {
                    Ok(msg) => match msg.value.into_string() {
                        Some(s) => {
                            if let Ok(m) = serde_json::from_str::<InvalidateMsg>(&s) {
                                cache.invalidate(&m.id, &m.key).await;
                            } else {
                                tracing::warn!(payload = %s, "bad app-invalidate message");
                            }
                        }
                        None => {
                            tracing::warn!("dropped non-UTF8 app-invalidate payload");
                        }
                    },
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(skipped = n, "app-invalidate sub stream lagged; dropped messages");
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        Ok(Arc::new(Self { publish_pool }))
    }

    pub async fn publish(&self, id: &str, key: &str) -> anyhow::Result<()> {
        let payload =
            serde_json::to_string(&InvalidateMsg { id: id.into(), key: key.into() })?;
        let _: () = self.publish_pool.next().publish(INVALIDATE_CHANNEL, payload).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::cache::{CacheConfig, CachingAppManager};
    use crate::app::{App, AppLookupError, AppManager};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn redis_url() -> String {
        std::env::var("PYLON_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6390".into())
    }
    fn app(id: &str, key: &str) -> std::sync::Arc<App> {
        let mut a: App = serde_json::from_value(serde_json::json!({
            "name":"t","id":id,"key":key,"secret":"s","enabled":true}))
        .unwrap();
        a.recompute_has_flags();
        std::sync::Arc::new(a)
    }
    struct Mock {
        app: Option<std::sync::Arc<App>>,
        calls: std::sync::Arc<AtomicUsize>,
    }
    #[async_trait::async_trait]
    impl AppManager for Mock {
        async fn by_id(
            &self,
            _: &str,
        ) -> Result<Option<std::sync::Arc<App>>, AppLookupError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.app.clone())
        }
        async fn by_key(
            &self,
            k: &str,
        ) -> Result<Option<std::sync::Arc<App>>, AppLookupError> {
            self.by_id(k).await
        }
    }

    #[tokio::test]
    async fn publish_on_one_node_evicts_another() {
        let cfg = CacheConfig {
            max_capacity: 100,
            ttl_secs: 300,
            neg_max: 100,
            neg_ttl_secs: 300,
        };
        let calls = std::sync::Arc::new(AtomicUsize::new(0));
        // node B: the cache that should get evicted
        let cache_b = std::sync::Arc::new(CachingAppManager::new(
            std::sync::Arc::new(Mock {
                app: Some(app("a", "k")),
                calls: calls.clone(),
            }),
            cfg.clone(),
            None,
        ));
        let _inv_b = AppInvalidator::spawn(&redis_url(), cache_b.clone()).await.unwrap();
        // node A: only publishes
        let cache_a = std::sync::Arc::new(CachingAppManager::new(
            std::sync::Arc::new(Mock {
                app: Some(app("a", "k")),
                calls: std::sync::Arc::new(AtomicUsize::new(0)),
            }),
            cfg,
            None,
        ));
        let inv_a = AppInvalidator::spawn(&redis_url(), cache_a).await.unwrap();

        // warm node B's cache (driver call 1), then invalidate from node A
        assert_eq!(cache_b.by_id("a").await.unwrap().unwrap().key, "k");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        inv_a.publish("a", "k").await.unwrap();
        // wait for the pub/sub round-trip, then re-fetch to prove eviction
        for _ in 0..50 {
            if cache_b_evicted(&cache_b, &calls).await {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        // node B re-fetches => driver called again (calls == 2 if eviction happened in loop)
        let calls_before = calls.load(Ordering::SeqCst);
        if calls_before < 2 {
            // eviction not yet detected by probe; do re-fetch here as the binding assertion
            assert_eq!(cache_b.by_id("a").await.unwrap().unwrap().key, "k");
            assert_eq!(
                calls.load(Ordering::SeqCst),
                2,
                "node B must have been evicted by node A's publish"
            );
        } else {
            assert_eq!(
                calls_before,
                2,
                "node B must have been evicted by node A's publish"
            );
        }
    }
    async fn cache_b_evicted(
        c: &std::sync::Arc<CachingAppManager>,
        calls: &std::sync::Arc<AtomicUsize>,
    ) -> bool {
        // Probe eviction: do a lookup — if the entry was evicted, the driver is hit (calls bump).
        // We look for calls == 2 to confirm the eviction round-trip completed.
        let _ = c.by_id("a").await;
        calls.load(Ordering::SeqCst) >= 2
    }
}
