//! The [`ClusterBridge`]: a dedicated tokio runtime that hosts a [`RedisAdapter`] so the
//! SYNC percore workers can drive cross-node coordination without ever blocking on Redis.
//!
//! A worker holds a cheap-clone [`ClusterHandle`] and fires fire-and-forget [`ClusterCmd`]s
//! over a bounded `mpsc` channel. The bridge's runtime thread drains that channel and runs
//! each command against the adapter. The control-plane channel is BOUNDED: a full channel
//! means the bridge is momentarily behind, and the worker drops the command rather than
//! block (at-most-once cross-node delivery — acceptable for a best-effort fan-out).
//!
//! The bridge's `RedisAdapter` is built with [`RedisAdapter::with_local`] so it SHARES the
//! workers' [`LocalAdapter`]: the adapter's pub/sub receive loop re-delivers remote frames
//! by calling `local.broadcast(Raw(..))`, which then shards through the workers' broadcast
//! sink straight to the right cores — no extra delivery code on this side.

use crate::adapter::local::LocalAdapter;
use crate::adapter::redis::RedisAdapter;
use crate::protocol::socket_id::SocketId;
use crate::server::config::ServerConfig;
use crate::webhook::WebhookHandle;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::sync::mpsc;

/// Bound on the worker→bridge control-plane channel. A full channel drops the command (see
/// [`ClusterHandle::publish`]); sized generously so only a sustained bridge stall — never a
/// normal burst — ever overflows it.
const CMD_CHANNEL_CAPACITY: usize = 8192;

/// A cross-node coordination command a percore worker fires at the bridge.
///
/// Only [`Publish`](ClusterCmd::Publish) exists today — the broadcast fan-out. Later SP11
/// tasks GROW this enum (Subscribe / PresenceSubscribe / Signin / Watch …) as those paths
/// move onto the bridge; each variant is added with its full drain-loop handling, never a
/// stub.
pub enum ClusterCmd {
    /// Fan a pre-encoded v7 broadcast `frame` out to the rest of the cluster on
    /// `(app, channel)`, excluding `except`. Maps to [`RedisAdapter::cluster_publish_broadcast`].
    Publish {
        app: Arc<str>,
        channel: Arc<str>,
        frame: String,
        except: Option<SocketId>,
    },
}

/// Cheap-clone handle a percore worker uses to fire [`ClusterCmd`]s at the bridge. `Send +
/// Sync`, so each worker can hold its own clone.
#[derive(Clone)]
pub struct ClusterHandle {
    tx: mpsc::Sender<ClusterCmd>,
    node_id: Arc<str>,
}

impl ClusterHandle {
    /// This node's cluster id (minted by the bridge's `RedisAdapter`). Stable for the
    /// bridge's lifetime.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Fire a cross-node broadcast at the bridge. NON-BLOCKING: a `try_send` that drops the
    /// command if the channel is full or closed — the worker must NEVER block on the bridge.
    /// A full channel means the bridge is momentarily behind; a dropped publish is
    /// at-most-once cross-node delivery, which is acceptable for this best-effort fan-out.
    pub fn publish(
        &self,
        app: Arc<str>,
        channel: Arc<str>,
        frame: String,
        except: Option<SocketId>,
    ) {
        let cmd = ClusterCmd::Publish {
            app,
            channel,
            frame,
            except,
        };
        match self.tx.try_send(cmd) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::debug!("cluster bridge channel full; dropping cross-node publish");
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::debug!("cluster bridge gone; dropping cross-node publish");
            }
        }
    }
}

/// Owns the dedicated tokio runtime (on its own OS thread) that hosts the bridge's
/// `RedisAdapter`, plus the shutdown signal and a [`ClusterHandle`] for the workers.
///
/// Dropping the bridge signals shutdown and JOINS the runtime thread, so a test or
/// `run_percore` can tear it down cleanly. Inside the thread, dropping the `RedisAdapter`
/// at the end of the drain loop aborts its background tasks via the adapter's own `Drop`.
pub struct ClusterBridge {
    handle: ClusterHandle,
    /// Set on `Drop` to break the drain loop; the loop also exits if the command channel
    /// closes (all `ClusterHandle`s dropped).
    shutdown: Arc<AtomicBool>,
    /// The runtime thread. `Option` so `Drop` can `take()` and `join()` it exactly once.
    thread: Option<JoinHandle<()>>,
}

impl ClusterBridge {
    /// A cheap clone of the handle for a worker (or a test).
    pub fn handle(&self) -> ClusterHandle {
        self.handle.clone()
    }
}

impl Drop for ClusterBridge {
    /// Signal the drain loop to stop, then join the runtime thread so the bridge (and its
    /// `RedisAdapter`'s background tasks) are fully torn down before this returns.
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        // Closing every sender also wakes the loop's `recv()`; we drop our handle clone so a
        // bridge whose only remaining sender is its own `ClusterHandle` lets the loop end.
        if let Some(thread) = self.thread.take() {
            if let Err(e) = thread.join() {
                tracing::error!(?e, "cluster bridge runtime thread panicked on shutdown");
            }
        }
    }
}

/// Start the bridge: spawn its dedicated multi-thread tokio runtime on a fresh OS thread,
/// connect a `RedisAdapter` (sharing `local`) inside it, start the sweeper, and run the
/// command drain loop. Returns once the runtime thread reports the adapter connected — or
/// `Err` if the connect failed (surfaced across the startup handshake, never a silent hang).
///
/// `local` MUST be the same `LocalAdapter` the percore workers broadcast through, so the
/// adapter's pub/sub receive loop's `local.broadcast(Raw(..))` shards remote frames to the
/// workers' sink.
pub fn start(
    cfg: &ServerConfig,
    local: Arc<LocalAdapter>,
    webhooks: WebhookHandle,
) -> anyhow::Result<ClusterBridge> {
    let (tx, mut rx) = mpsc::channel::<ClusterCmd>(CMD_CHANNEL_CAPACITY);
    let shutdown = Arc::new(AtomicBool::new(false));

    // Owned copy moved into the runtime thread (the caller keeps its borrow).
    let cfg = cfg.clone();
    let thread_shutdown = shutdown.clone();

    // Startup handshake: the runtime thread connects fred asynchronously, but `start` is
    // sync and must return a usable handle or the connect error. A one-shot std channel
    // carries `Result<node_id, anyhow::Error>` from the thread back here.
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel::<anyhow::Result<Arc<str>>>(1);

    let thread = std::thread::Builder::new()
        .name("pylon-cluster-bridge".into())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    // Report the build failure to `start` and exit the thread.
                    let _ = ready_tx.send(Err(anyhow::Error::from(e)));
                    return;
                }
            };

            runtime.block_on(async move {
                // Connect the adapter sharing the workers' `LocalAdapter`. On failure, hand
                // the error back so `start` returns `Err` instead of hanging.
                let adapter = match RedisAdapter::with_local(&cfg, local).await {
                    Ok(a) => a,
                    Err(e) => {
                        let _ = ready_tx.send(Err(e));
                        return;
                    }
                };

                // The sweeper needs the `WebhookHandle`; start it now that the adapter is up.
                adapter.start_sweeper(webhooks);

                // Report ready with the real node id so `start` can build the live handle.
                let node_id: Arc<str> = Arc::from(adapter.node_id());
                if ready_tx.send(Ok(node_id)).is_err() {
                    // `start` was dropped before we became ready (no handle will ever be
                    // used); nothing to do but tear down — `adapter` drops at end of scope.
                    return;
                }
                drop(ready_tx);

                // Command drain loop. Runs until the channel closes (every `ClusterHandle`
                // dropped) OR the shutdown flag is set on bridge `Drop`. `adapter` is kept
                // alive for the whole loop; its `Drop` aborts the background tasks at the end.
                loop {
                    if thread_shutdown.load(Ordering::SeqCst) {
                        break;
                    }
                    // A short timeout makes the loop re-check the shutdown flag promptly even
                    // when idle, without a busy spin.
                    let next = tokio::time::timeout(
                        std::time::Duration::from_millis(100),
                        rx.recv(),
                    )
                    .await;
                    match next {
                        // Channel closed: all handles dropped — nothing more can arrive.
                        Ok(None) => break,
                        // Idle tick: loop back and re-check the shutdown flag.
                        Err(_) => continue,
                        Ok(Some(cmd)) => match cmd {
                            ClusterCmd::Publish {
                                app,
                                channel,
                                frame,
                                except,
                            } => {
                                adapter
                                    .cluster_publish_broadcast(
                                        &app,
                                        &channel,
                                        frame,
                                        except.as_ref(),
                                    )
                                    .await;
                            }
                        },
                    }
                }
                // `adapter` drops here → its background tasks (recv/heartbeats/sweeper) abort.
            });
        })?;

    // Block until the thread reports ready (or fails to connect). A dropped `ready_tx`
    // without a value (e.g. the thread panicked before sending) surfaces as a recv error.
    let node_id = match ready_rx.recv() {
        Ok(Ok(node_id)) => node_id,
        Ok(Err(e)) => {
            // The runtime thread reported a connect/build error; join it and propagate.
            let _ = thread.join();
            return Err(e);
        }
        Err(_) => {
            let _ = thread.join();
            return Err(anyhow::anyhow!(
                "cluster bridge runtime thread exited before reporting ready"
            ));
        }
    };

    let handle = ClusterHandle { tx, node_id };
    Ok(ClusterBridge {
        handle,
        shutdown,
        thread: Some(thread),
    })
}
