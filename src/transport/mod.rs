//! Lean per-core WebSocket transport (SP9).
//!
//! This module owns the raw RFC 6455 frame layer for the new per-connection
//! transport. Unlike `tokio-tungstenite`, it does **not** eagerly allocate a
//! large (128 KiB) read buffer per connection: framing operates over a
//! caller-owned [`bytes::BytesMut`] that grows lazily, and parsed payloads are
//! cheap `Bytes` slices into that buffer.
//!
//! [`frame`] is the RFC 6455 codec; [`conn`] is the per-connection state +
//! non-blocking read/write that the worker event loop drives. The event loop
//! itself is built in later SP9 tasks.

pub mod conn;
pub mod fanout;
pub mod frame;
pub mod handshake;
pub mod rest;
pub mod worker;

use crate::adapter::local::LocalAdapter;
use crate::adapter::Adapter;
use crate::app::AppManager;
use crate::server::config::ServerConfig;
use crate::webhook::WebhookHandle;
use dashmap::DashMap;
use fanout::{BroadcastSink, WorkerSlot};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use worker::{BroadcastWiring, DispatchEnv, Mode, WorkerConfig};

pub use rest::RestConn;

/// Run the per-core (`PYLON_TRANSPORT=percore`) transport as the actual server.
///
/// Takes the already-built shared pieces (the same ones `main`/`AppState`
/// assemble), builds ONE shared [`DispatchEnv`], then spawns
/// `config.worker_count()` worker threads — each pinned to a CPU and each with
/// its OWN `SO_REUSEPORT` listener on `config.bind:config.port`. The kernel
/// load-balances incoming connections across the workers' accept queues, so
/// fan-out parallelizes across cores. The `Arc`'d adapter/env (the `LocalAdapter`
/// registry is `DashMap`-concurrent) and a clone of the REST handoff `Sender`
/// are shared by all workers; cross-worker delivery already works because each
/// per-conn mailbox is `Send + Sync` and every worker drains its own
/// connections. Blocks until `shutdown` is observed by all workers (or a fatal
/// bind/poll error occurs), joining every worker thread before returning.
///
/// REST handling (SP9 §3.4): a worker no longer closes non-WS connections. On a
/// `HeadResult::Rest` head it transfers the accepted fd (plus the head bytes
/// already read) over `rest_handoff` to the tokio/axum REST plane spawned by the
/// caller via [`rest::serve`]. WS connections + the full v7 protocol run on the
/// worker threads. `rest_handoff` is `None` only in tests that exercise a worker
/// without a REST plane.
#[allow(clippy::too_many_arguments)]
pub fn run_percore(
    config: ServerConfig,
    apps: Arc<dyn AppManager>,
    adapter: Arc<dyn Adapter>,
    conn_counts: Arc<DashMap<String, Arc<AtomicUsize>>>,
    webhooks: WebhookHandle,
    rest_handoff: Option<UnboundedSender<RestConn>>,
    shutdown: Arc<AtomicBool>,
    // The CONCRETE local adapter (the same instance wrapped in `adapter` above)
    // when the per-core sharded fan-out applies. `Some` ⇒ install the broadcast
    // sink and give each worker a broadcast inbox so channel deliveries shard
    // across workers; `None` (e.g. the deferred redis+percore combo) ⇒ no sink,
    // broadcasts fall back to the legacy registry mailbox path.
    local: Option<Arc<LocalAdapter>>,
) -> std::io::Result<()> {
    let addr: std::net::SocketAddr = format!("{}:{}", config.bind, config.port)
        .parse()
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;

    let env = Arc::new(DispatchEnv {
        apps,
        adapter,
        limits: config.limits(),
        activity_timeout: config.activity_timeout,
        strict_protocol: config.strict_protocol,
        conn_counts,
        webhooks,
    });

    // WS frame cap: bound a single inbound frame's payload. The configured
    // event-payload limit is small (KiB), so use a 1 MiB frame ceiling that
    // comfortably covers any legitimate Pusher frame while bounding abuse.
    let max_payload = config.max_event_payload_bytes.max(1 << 20);
    // Per-connection outbound high-water before a backpressure close (4 MiB).
    let high_water = 4 << 20;

    let worker_count = config.worker_count();
    // CPU ids to pin to. May be empty if the OS won't report them — workers then
    // run unpinned (still fully functional, just not affinity-bound).
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();

    tracing::info!(%addr, workers = worker_count, "pylon percore: {worker_count} workers on {addr}");

    // Build the per-core sharded broadcast plumbing: one `(Sender, Receiver)`
    // pair + `WorkerSlot` per worker. The `Sender`s live in the sink (installed
    // on the concrete adapter BEFORE any worker spawns, so the first broadcast
    // already routes through it); each `Receiver` + slot is handed to its worker,
    // which fills the slot's `Waker` `OnceLock` at startup. When `local` is
    // `None` (no concrete adapter to install on), broadcasts use the legacy
    // mailbox path and workers get no inbox.
    let mut wirings: Vec<Option<BroadcastWiring>> = Vec::with_capacity(worker_count);
    if let Some(local) = &local {
        // One shared `Arc<WorkerSlot>` per worker: the sink and the worker both
        // hold it, so the `Waker` the worker publishes into the slot at startup
        // is immediately visible to the publisher.
        let mut slots: Vec<Arc<WorkerSlot>> = Vec::with_capacity(worker_count);
        let mut receivers = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let (tx, rx) = std::sync::mpsc::channel();
            slots.push(Arc::new(WorkerSlot {
                tx,
                waker: std::sync::OnceLock::new(),
            }));
            receivers.push(rx);
        }
        let sink = BroadcastSink {
            workers: Arc::new(slots.clone()),
        };
        // Install BEFORE spawning workers so the very first broadcast routes here.
        local.set_broadcast_sink(sink);
        for (slot, rx) in slots.into_iter().zip(receivers) {
            wirings.push(Some(BroadcastWiring { rx, slot }));
        }
    } else {
        for _ in 0..worker_count {
            wirings.push(None);
        }
    }

    let mut handles = Vec::with_capacity(worker_count);
    for (i, wiring) in wirings.into_iter().enumerate() {
        let cfg = WorkerConfig {
            addr,
            max_payload,
            high_water,
            mode: Mode::Dispatch(env.clone()),
            rest_handoff: rest_handoff.clone(),
            worker_id: i,
            broadcast: wiring,
        };
        let shutdown = shutdown.clone();
        let core = core_ids.get(i % core_ids.len().max(1)).copied();
        let handle = std::thread::Builder::new()
            .name(format!("pylon-worker-{i}"))
            .spawn(move || {
                if let Some(core) = core {
                    if core_affinity::set_for_current(core) {
                        tracing::debug!(worker = i, core = ?core, "pinned percore worker to core");
                    } else {
                        tracing::debug!(worker = i, core = ?core, "core pinning unsupported; running unpinned");
                    }
                }
                worker::run(cfg, shutdown)
            })?;
        handles.push(handle);
    }

    // Join all workers. The first fatal worker error is propagated; remaining
    // workers are still joined so we don't leak threads on shutdown.
    let mut first_err = None;
    for handle in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            Err(_) => {
                if first_err.is_none() {
                    first_err = Some(std::io::Error::other("percore worker thread panicked"));
                }
            }
        }
    }
    match first_err {
        Some(e) => Err(e),
        None => Ok(()),
    }
}
