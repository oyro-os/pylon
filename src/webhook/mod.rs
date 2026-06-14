//! Webhooks (SP5): WS-lifecycle-driven, signed, batched HTTP notifications.
//!
//! `WebhookEvent` (the trigger) → `WebhookHandle` (cheap-clone mpsc sender) →
//! `WebhookDispatcher` (actor: window + coalesce + sign) → `WebhookTransport`.

pub mod batch;
pub mod dispatcher;
pub mod event;
pub mod occupancy;
pub mod transport;

use crate::app::AppManager;
use dispatcher::{Clock, WebhookDispatcher};
use event::WebhookEvent;
use std::sync::Arc;
use tokio::sync::mpsc;
use transport::WebhookTransport;

pub use occupancy::{AdapterOccupancy, OccupancySource};

/// Cheap-clone enqueue handle held in `AppState` and `ConnectionContext`. The
/// WS path NEVER blocks on it: `enqueue` is a non-awaiting `try_send` that drops
/// (and logs) on a full mailbox (spec §8).
#[derive(Clone)]
pub struct WebhookHandle {
    tx: mpsc::Sender<WebhookEvent>,
}

impl WebhookHandle {
    /// A handle whose dispatcher is a draining sink (no deliveries). Used by tests
    /// and by any caller that wants webhooks disabled. Spawns a task that drains the
    /// receiver so enqueues never error; must run inside a tokio runtime.
    pub fn null() -> Self {
        let (tx, mut rx) = mpsc::channel(1024);
        tokio::spawn(async move { while rx.recv().await.is_some() {} });
        WebhookHandle { tx }
    }

    /// Non-blocking enqueue. Drops + logs on a full or closed mailbox.
    pub fn enqueue(&self, event: WebhookEvent) {
        match self.tx.try_send(event) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(e)) => {
                tracing::warn!(
                    name = e.name(),
                    app = e.app(),
                    "webhook mailbox full; dropping"
                );
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::warn!("webhook dispatcher gone; dropping trigger");
            }
        }
    }
}

/// Spawn the dispatcher actor and return the enqueue handle. `mailbox_capacity`
/// sizes the bounded channel (the §8 backpressure safety valve).
///
/// `vacated_grace_ms` + `occupancy` enable the cluster-aware `channel_vacated`
/// grace window (Task D1): when both are active (grace > 0 and `occupancy` is
/// `Some`), a surviving `channel_vacated` is debounced by the grace window and
/// re-checked against the cluster subscription_count before firing — suppressed
/// if the channel is occupied again anywhere in the cluster. With `0` / `None`
/// (the local-adapter path) vacated fires immediately, as before.
pub fn spawn(
    apps: Arc<dyn AppManager>,
    transport: Arc<dyn WebhookTransport>,
    clock: Arc<dyn Clock>,
    batch_ms: u64,
    mailbox_capacity: usize,
    vacated_grace_ms: u64,
    occupancy: Option<Arc<dyn OccupancySource>>,
) -> WebhookHandle {
    let (tx, rx) = mpsc::channel(mailbox_capacity);
    let dispatcher = WebhookDispatcher::new(
        rx,
        apps,
        transport,
        clock,
        batch_ms,
        vacated_grace_ms,
        occupancy,
    );
    tokio::spawn(dispatcher.run());
    WebhookHandle { tx }
}
