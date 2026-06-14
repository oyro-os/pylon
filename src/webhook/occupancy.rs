//! Cluster-wide occupancy lookup for the cluster-aware `channel_vacated` grace
//! window (Task D1). Before a debounced vacated webhook fires, the dispatcher
//! re-checks the cluster subscription_count for the channel; if it is occupied
//! again anywhere in the cluster, the vacated webhook is suppressed.

use async_trait::async_trait;
use std::sync::Arc;

/// Source of the current cluster-wide subscription_count for a channel. The
/// dispatcher calls this at vacated FIRE time to decide whether the channel has
/// been re-occupied during the grace window.
#[async_trait]
pub trait OccupancySource: Send + Sync {
    /// Current cluster-wide subscription_count for (app, channel).
    async fn subscription_count(&self, app: &str, channel: &str) -> usize;
}

/// Adapter-backed occupancy source: defers to `Adapter::channel(...).subscription_count`.
pub struct AdapterOccupancy(pub Arc<dyn crate::adapter::Adapter>);

#[async_trait]
impl OccupancySource for AdapterOccupancy {
    async fn subscription_count(&self, app: &str, channel: &str) -> usize {
        self.0.channel(app, channel).await.subscription_count
    }
}
