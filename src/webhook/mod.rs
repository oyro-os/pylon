//! Webhooks (SP5): WS-lifecycle-driven, signed, batched HTTP notifications.
//!
//! `WebhookEvent` (the trigger) → `WebhookHandle` (cheap-clone mpsc sender) →
//! `WebhookDispatcher` (actor: window + coalesce + sign) → `WebhookTransport`.

pub mod batch;
pub mod dispatcher;
pub mod event;
pub mod transport;

use crate::app::AppManager;
use dispatcher::{Clock, WebhookDispatcher};
use event::WebhookEvent;
use std::sync::Arc;
use tokio::sync::mpsc;
use transport::WebhookTransport;

/// Cheap-clone enqueue handle held in `AppState` and `ConnectionContext`. The
/// WS path NEVER blocks on it: `enqueue` is a non-awaiting `try_send` that drops
/// (and logs) on a full mailbox (spec §8).
#[derive(Clone)]
pub struct WebhookHandle {
    tx: mpsc::Sender<WebhookEvent>,
}

impl WebhookHandle {
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
pub fn spawn(
    apps: Arc<dyn AppManager>,
    transport: Arc<dyn WebhookTransport>,
    clock: Arc<dyn Clock>,
    batch_ms: u64,
    mailbox_capacity: usize,
) -> WebhookHandle {
    let (tx, rx) = mpsc::channel(mailbox_capacity);
    let dispatcher = WebhookDispatcher::new(rx, apps, transport, clock, batch_ms);
    tokio::spawn(dispatcher.run());
    WebhookHandle { tx }
}
