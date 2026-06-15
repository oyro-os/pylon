//! Per-core SHARDED broadcast fan-out sink (SP9).
//!
//! The legacy delivery path ([`ChannelState::broadcast`](crate::channel::state))
//! enqueues a broadcast onto every subscriber's per-connection `mpsc` mailbox
//! from ONE thread. With N (e.g. 10k) subscribers on a channel that is N
//! `UnboundedSender::send` calls — each an alloc + a futex wake — serialized on
//! the publishing thread, which walls fan-out long before the CPU is the bound.
//!
//! This sink replaces that with a per-WORKER hand-off: a broadcast notifies each
//! worker exactly ONCE (W messages, not N), and each worker then fans the
//! (already WS-framed) bytes out to its OWN local subscribers by direct
//! slab-enqueue (an `Arc` bump per subscriber, no per-connection mpsc, no
//! per-connection wake). The work to actually copy bytes onto each connection's
//! send queue is thereby spread across all worker cores instead of running
//! serially on the publisher.
//!
//! Only DELIVERY of channel broadcasts moves here; membership/counts still flow
//! through the registry, and DIRECT sends (connection_established, rosters,
//! send_to_user, terminate, …) still use the per-connection mailbox.
//!
//! Safe Rust — the crate root sets `#![deny(unsafe_code)]`; this module adds no
//! `unsafe`.

use crate::protocol::socket_id::SocketId;
use std::sync::Arc;

/// One sharded broadcast hand-off: the WS-framed bytes plus the routing keys
/// every worker needs to find its local subscribers. `frame` is already a
/// complete server→client WebSocket text frame (encoded once by the publisher),
/// shared via `Arc` so each worker's per-connection enqueue is a cheap refcount
/// bump rather than a copy.
pub struct BroadcastMsg {
    pub app: Arc<str>,
    pub channel: Arc<str>,
    pub frame: Arc<[u8]>,
    /// The originating connection's `socket_id`, excluded from delivery (sender
    /// exclusion for client events / count echoes). `None` ⇒ deliver to all.
    pub except: Option<SocketId>,
}

/// One slot per worker. The `Sender` is created in `run_percore` (paired with
/// the `Receiver` handed to the worker); the `Waker` is created BY the worker
/// from its own `mio::Poll` registry at startup and published into the
/// `OnceLock` so the sink can nudge an idle worker to drain promptly.
pub struct WorkerSlot {
    pub tx: std::sync::mpsc::Sender<BroadcastMsg>,
    pub waker: std::sync::OnceLock<Arc<mio::Waker>>,
}

/// Cloneable handle the adapter holds to route broadcasts to every worker. The
/// `Arc<Vec<Arc<WorkerSlot>>>` is shared (one allocation) so cloning the sink
/// onto the adapter is cheap. Each slot is itself an `Arc` SHARED with the
/// owning worker, so the `Waker` a worker publishes into its slot's `OnceLock`
/// at startup is immediately visible to the sink.
#[derive(Clone, Default)]
pub struct BroadcastSink {
    pub workers: Arc<Vec<Arc<WorkerSlot>>>,
}

impl BroadcastSink {
    /// Hand the (already WS-framed) `frame` to EVERY worker; each worker filters
    /// to the subscribers it owns. `send` on a disconnected channel (a worker
    /// thread gone) and a failed `wake` are both ignored — a vanished worker has
    /// no live connections to deliver to.
    pub fn broadcast(
        &self,
        app: Arc<str>,
        channel: Arc<str>,
        frame: Arc<[u8]>,
        except: Option<SocketId>,
    ) {
        for slot in self.workers.iter() {
            let _ = slot.tx.send(BroadcastMsg {
                app: app.clone(),
                channel: channel.clone(),
                frame: frame.clone(),
                except: except.clone(),
            });
            if let Some(w) = slot.waker.get() {
                let _ = w.wake();
            }
        }
    }
}
