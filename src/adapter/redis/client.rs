//! fred v10 client wiring for the Redis adapter.
//!
//! Holds the command pool (one [`Pool`] of `pool_size` connections) used for all
//! ordinary commands + PUBLISH, and a dedicated [`SubscriberClient`] for the
//! pub/sub side. The subscriber's resubscribe task ([`SubscriberClient::manage_subscriptions`])
//! is kept alive by storing its [`JoinHandle`] — dropping it would stop the
//! automatic re-subscribe on reconnect.

use fred::clients::SubscriberClient;
use fred::prelude::*;
use tokio::task::JoinHandle;

/// The connected fred clients for one Redis adapter instance.
pub struct RedisClients {
    /// Connection pool for ordinary commands and PUBLISH.
    pub pool: Pool,
    /// Dedicated subscriber client for the pub/sub fan-in.
    pub sub: SubscriberClient,
    /// Background task that re-subscribes the subscriber after a reconnect.
    /// Kept alive for the lifetime of the adapter — never `.await`ed.
    pub sub_manager: JoinHandle<()>,
}

impl RedisClients {
    /// Connect to Redis at `redis_url` with a pool of `pool_size` connections.
    ///
    /// Uses an exponential reconnect policy (min 100ms, max 30s, base 2,
    /// unlimited attempts). Initializes both the pool and the subscriber, and
    /// spawns the subscriber's resubscribe-on-reconnect task.
    pub async fn connect(redis_url: &str, pool_size: u32) -> anyhow::Result<RedisClients> {
        let config = Config::from_url(redis_url)?;
        // `max_attempts = 0` means retry forever; min 100ms, max 30s, base 2.
        let policy = ReconnectPolicy::new_exponential(0, 100, 30_000, 2);

        let mut builder = Builder::from_config(config);
        builder.set_policy(policy);

        let pool = builder.build_pool(pool_size as usize)?;
        let sub = builder.build_subscriber_client()?;

        pool.init().await?;
        sub.init().await?;

        // Keep the resubscribe task handle so it isn't dropped (which would stop it).
        let sub_manager = sub.manage_subscriptions();

        Ok(RedisClients {
            pool,
            sub,
            sub_manager,
        })
    }
}

/// Placeholder for the membership/presence Lua scripts loaded in Phase C.
/// Empty for now; the SHA-cached scripts are registered here later.
#[derive(Default)]
pub struct Scripts {}

impl Scripts {
    pub fn new() -> Self {
        Self::default()
    }
}
