//! Single-worker `mio` event loop for the per-core transport (SP9).
//!
//! [`run`] binds a listener, then drives a non-blocking accept → handshake →
//! frame loop entirely on the calling thread with one [`mio::Poll`]. A
//! [`slab::Slab`] is the connection table: the slab key *is* the connection's
//! [`mio::Token`] value, so a readiness event maps to its [`Connection`] in
//! O(1). The listener uses a reserved token ([`LISTENER`]) that no slab key can
//! collide with.
//!
//! Readiness is managed edge-friendly: a connection is registered
//! `READABLE`-only and only gains `WRITABLE` interest when a [`flush`] returns
//! [`WriteStatus::WouldBlock`]; the interest is dropped back to `READABLE` once
//! the queue drains. This keeps the loop from spinning on a writable socket with
//! nothing to send.
//!
//! For this task the only behaviour is [`Mode::Echo`]: every inbound data frame
//! is re-encoded and queued straight back, pings are answered with pongs, and a
//! close (or any protocol/EOF error) tears the connection down. Real protocol
//! dispatch arrives in a later task. 100% safe Rust — the crate root sets
//! `#![forbid(unsafe_code)]`.

use crate::transport::conn::{ConnError, ConnState, Connection, WriteStatus};
use crate::transport::frame::{self, OpCode};
use crate::transport::handshake::{self, HeadResult};
use bytes::BytesMut;
use mio::net::TcpListener;
use mio::{Events, Interest, Poll, Token};
use std::io::{ErrorKind, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Reserved token for the listener. Slab keys grow from 0, so the maximum
/// `usize` is guaranteed never to collide with a connection token.
const LISTENER: Token = Token(usize::MAX);

/// Configuration for a single worker event loop.
pub struct WorkerConfig {
    /// Address to bind the listener to.
    pub addr: std::net::SocketAddr,
    /// Maximum accepted WebSocket payload size (bytes) per frame.
    pub max_payload: usize,
    /// Per-connection outbound high-water mark (bytes) before backpressure-close.
    pub high_water: usize,
    /// Behaviour applied to inbound frames. [`Mode::Echo`] for this task.
    pub mode: Mode,
}

/// Worker behaviour for inbound frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Echo every data frame back to the sender; answer pings with pongs.
    Echo,
}

/// Per-connection slab entry: the [`Connection`] plus its read remainder.
///
/// `inbuf` is empty or tiny when the connection is idle (it only holds bytes
/// that arrived mid-frame), so it does not reintroduce a large per-connection
/// buffer. During [`ConnState::Handshaking`] it doubles as the head-accumulation
/// buffer until [`handshake::read_head`] returns something other than
/// [`HeadResult::NeedMore`].
struct Entry {
    conn: Connection,
    inbuf: BytesMut,
    /// The [`Token`] this connection is registered under (== `Token(slab_key)`).
    /// Stored so flush-driven interest re-arming can reregister without
    /// threading the key through every call.
    token: Token,
}

/// Run the worker event loop until `shutdown` is set. Blocks the calling thread.
///
/// Returns once `shutdown` is observed `true` (clean stop) or a fatal I/O error
/// occurs while binding/polling.
pub fn run(cfg: WorkerConfig, shutdown: Arc<AtomicBool>) -> std::io::Result<()> {
    let mut poll = Poll::new()?;
    let mut listener = TcpListener::bind(cfg.addr)?;
    poll.registry()
        .register(&mut listener, LISTENER, Interest::READABLE)?;

    let mut events = Events::with_capacity(1024);
    let mut conns: slab::Slab<Entry> = slab::Slab::new();

    loop {
        if shutdown.load(Ordering::SeqCst) {
            return Ok(());
        }

        // The 100ms timeout bounds how long we sleep so `shutdown` is checked
        // even when no readiness events fire.
        if let Err(e) = poll.poll(&mut events, Some(Duration::from_millis(100))) {
            // A signal can interrupt the poll syscall; just retry.
            if e.kind() == ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }

        for event in events.iter() {
            match event.token() {
                LISTENER => accept_ready(&poll, &mut listener, &mut conns, &cfg),
                token => {
                    let key = token.0;
                    // The connection may have been removed earlier in this same
                    // event batch (e.g. a read closed it before its writable
                    // event is processed); skip stale tokens.
                    if !conns.contains(key) {
                        continue;
                    }

                    // A peer hangup / error: tear down regardless of r/w intent.
                    if event.is_error() || event.is_read_closed() || event.is_write_closed() {
                        remove(&poll, &mut conns, key);
                        continue;
                    }

                    if event.is_readable()
                        && handle_readable(&poll, &mut conns, key, &cfg) == Action::Close
                    {
                        remove(&poll, &mut conns, key);
                        continue;
                    }

                    if event.is_writable()
                        && conns.contains(key)
                        && handle_writable(&poll, &mut conns, key) == Action::Close
                    {
                        remove(&poll, &mut conns, key);
                    }
                }
            }
        }
    }
}

/// Outcome of handling a connection event: keep it or close it.
#[derive(Debug, PartialEq, Eq)]
enum Action {
    Keep,
    Close,
}

/// Drain the listener's accept backlog, registering every accepted socket.
fn accept_ready(
    poll: &Poll,
    listener: &mut TcpListener,
    conns: &mut slab::Slab<Entry>,
    cfg: &WorkerConfig,
) {
    loop {
        match listener.accept() {
            Ok((mut stream, _peer)) => {
                let entry = conns.vacant_entry();
                let key = entry.key();
                if let Err(e) =
                    poll.registry()
                        .register(&mut stream, Token(key), Interest::READABLE)
                {
                    // Registration failed: drop the socket, leave the slab slot
                    // unused (vacant_entry didn't consume it).
                    tracing::debug!(error = %e, "failed to register accepted socket");
                    continue;
                }
                entry.insert(Entry {
                    conn: Connection::new(stream, cfg.high_water),
                    inbuf: BytesMut::new(),
                    token: Token(key),
                });
            }
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => break,
            Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => {
                tracing::debug!(error = %e, "listener accept error");
                break;
            }
        }
    }
}

/// Handle a readable event: either advance the handshake or process frames.
fn handle_readable(
    poll: &Poll,
    conns: &mut slab::Slab<Entry>,
    key: usize,
    cfg: &WorkerConfig,
) -> Action {
    let entry = &mut conns[key];
    match entry.conn.state {
        ConnState::Handshaking => handle_handshake(poll, entry),
        ConnState::Open | ConnState::Closing => handle_frames(poll, entry, cfg),
    }
}

/// Accumulate request-head bytes and, once complete, complete the WS upgrade.
fn handle_handshake(poll: &Poll, entry: &mut Entry) -> Action {
    // Pull all available bytes into the head-accumulation buffer (`inbuf`).
    if drain_into(&mut entry.conn, &mut entry.inbuf) == ReadOutcome::Closed {
        return Action::Close;
    }

    match handshake::read_head(&entry.inbuf) {
        HeadResult::NeedMore => Action::Keep,
        HeadResult::WsUpgrade { key: ws_key, .. } => {
            let response = handshake::accept_response(&ws_key).into_boxed_slice();
            if entry.conn.queue(Arc::from(response)).is_err() {
                return Action::Close;
            }
            // A browser never sends data frames before the 101, so any bytes
            // after the head would be a protocol error anyway; clearing is safe.
            entry.inbuf.clear();
            entry.conn.state = ConnState::Open;
            flush_and_arm(poll, entry)
        }
        // TODO(3.4): hand off to the tokio REST control plane (replay the head).
        HeadResult::Rest { .. } => Action::Close,
        HeadResult::Bad(_) => Action::Close,
    }
}

/// Read and echo every complete frame currently buffered.
fn handle_frames(poll: &Poll, entry: &mut Entry, cfg: &WorkerConfig) -> Action {
    let frames = {
        // Split the borrow so `inbuf` (the read remainder) and `conn` can be
        // borrowed at once via a temporary swap-out of the buffer.
        let mut scratch = std::mem::take(&mut entry.inbuf);
        let result = entry.conn.read_frames(&mut scratch, cfg.max_payload);
        entry.inbuf = scratch;
        match result {
            Ok(frames) => frames,
            // EOF or a fatal protocol violation: close. (For Echo we don't
            // bother sending a courtesy close frame.)
            Err(ConnError::Closed) | Err(ConnError::Protocol(_)) => return Action::Close,
            Err(ConnError::Backpressure) => return Action::Close,
        }
    };

    let mut wrote = false;
    for f in frames {
        match cfg.mode {
            Mode::Echo => match f.opcode {
                OpCode::Text | OpCode::Binary | OpCode::Continuation => {
                    let mut out = BytesMut::new();
                    frame::encode(&mut out, f.fin, f.opcode, &f.payload);
                    if entry
                        .conn
                        .queue(Arc::from(out.to_vec().into_boxed_slice()))
                        .is_err()
                    {
                        return Action::Close;
                    }
                    wrote = true;
                }
                OpCode::Ping => {
                    let mut out = BytesMut::new();
                    frame::encode(&mut out, true, OpCode::Pong, &f.payload);
                    if entry
                        .conn
                        .queue(Arc::from(out.to_vec().into_boxed_slice()))
                        .is_err()
                    {
                        return Action::Close;
                    }
                    wrote = true;
                }
                // A peer pong is unsolicited noise here; ignore it.
                OpCode::Pong => {}
                OpCode::Close => return Action::Close,
            },
        }
    }

    if wrote {
        flush_and_arm(poll, entry)
    } else {
        Action::Keep
    }
}

/// Handle a writable event: flush and, when drained, drop writable interest.
fn handle_writable(poll: &Poll, conns: &mut slab::Slab<Entry>, key: usize) -> Action {
    let entry = &mut conns[key];
    flush_and_arm(poll, entry)
}

/// Flush the outbound queue and reconcile writable interest with what remains.
///
/// * [`WriteStatus::Drained`] → re-arm `READABLE`-only (drop `WRITABLE`).
/// * [`WriteStatus::WouldBlock`] → add `WRITABLE` so we get a writable event.
/// * [`WriteStatus::Closed`] → close.
fn flush_and_arm(poll: &Poll, entry: &mut Entry) -> Action {
    // Read the token before the mutable stream borrow below.
    let token = entry.token;
    match entry.conn.flush() {
        WriteStatus::Drained => {
            if poll
                .registry()
                .reregister(&mut entry.conn.stream, token, Interest::READABLE)
                .is_err()
            {
                return Action::Close;
            }
            Action::Keep
        }
        WriteStatus::WouldBlock => {
            if poll
                .registry()
                .reregister(
                    &mut entry.conn.stream,
                    token,
                    Interest::READABLE | Interest::WRITABLE,
                )
                .is_err()
            {
                return Action::Close;
            }
            Action::Keep
        }
        WriteStatus::Closed => Action::Close,
    }
}

/// Read all currently-available bytes off the socket into `buf`, stopping on
/// `WouldBlock` (socket drained) or EOF. Used only during the handshake, where
/// we accumulate the raw head before any framing.
#[derive(PartialEq, Eq)]
enum ReadOutcome {
    Ok,
    Closed,
}

fn drain_into(conn: &mut Connection, buf: &mut BytesMut) -> ReadOutcome {
    let mut chunk = [0u8; 16 * 1024];
    loop {
        match conn.stream.read(&mut chunk) {
            Ok(0) => return ReadOutcome::Closed,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(ref e) if e.kind() == ErrorKind::WouldBlock => return ReadOutcome::Ok,
            Err(ref e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(_) => return ReadOutcome::Closed,
        }
    }
}

/// Remove a connection: deregister its socket and drop the slab entry.
fn remove(poll: &Poll, conns: &mut slab::Slab<Entry>, key: usize) {
    if let Some(mut entry) = conns.try_remove(key) {
        let _ = poll.registry().deregister(&mut entry.conn.stream);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use std::sync::atomic::AtomicBool;
    use tokio_tungstenite::tungstenite::Message;

    /// Reserve a free port via a throwaway std listener, then drop it. The OS
    /// won't immediately hand the same port to a different process, so the
    /// worker re-binding it moments later is race-free in practice.
    fn free_port() -> u16 {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    }

    /// Spawn the worker on its own OS thread bound to `addr` in [`Mode::Echo`],
    /// returning the shutdown flag and the join handle.
    fn spawn_worker(addr: std::net::SocketAddr) -> (Arc<AtomicBool>, std::thread::JoinHandle<()>) {
        let shutdown = Arc::new(AtomicBool::new(false));
        let sd = shutdown.clone();
        let handle = std::thread::spawn(move || {
            run(
                WorkerConfig {
                    addr,
                    max_payload: 1 << 20,
                    high_water: 1 << 20,
                    mode: Mode::Echo,
                },
                sd,
            )
            .expect("worker run failed");
        });
        (shutdown, handle)
    }

    /// THE GATE: a real `tokio-tungstenite` client completes the RFC 6455
    /// handshake against the worker and gets its text frame echoed back.
    #[tokio::test]
    async fn worker_handshakes_and_echoes() {
        let port = free_port();
        let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let (shutdown, handle) = spawn_worker(addr);

        // Give the worker a moment to bind before the client connects.
        tokio::time::sleep(Duration::from_millis(150)).await;

        let url = format!("ws://127.0.0.1:{port}/app/app-key");
        let (mut ws, _resp) = tokio_tungstenite::connect_async(url)
            .await
            .expect("ws connect/handshake");

        ws.send(Message::Text("hello".into()))
            .await
            .expect("send text");

        let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("echo within 5s")
            .expect("stream not ended")
            .expect("frame ok");
        assert_eq!(msg.into_text().unwrap(), "hello");

        // A ping must be answered with a pong carrying the same payload.
        ws.send(Message::Ping(b"ping-payload".to_vec()))
            .await
            .expect("send ping");
        // tungstenite auto-responds to pongs at the protocol layer, so drive the
        // stream until we observe the pong (or our own buffered messages).
        let pong = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match ws.next().await {
                    Some(Ok(Message::Pong(p))) => return Some(p),
                    Some(Ok(_)) => continue,
                    _ => return None,
                }
            }
        })
        .await
        .expect("pong within 5s");
        assert_eq!(pong.as_deref(), Some(&b"ping-payload"[..]));

        shutdown.store(true, Ordering::SeqCst);
        handle.join().unwrap();
    }

    /// A second connection on the same worker also handshakes and echoes,
    /// exercising the slab's multi-connection path (distinct tokens).
    #[tokio::test]
    async fn worker_handles_multiple_connections() {
        let port = free_port();
        let addr: std::net::SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
        let (shutdown, handle) = spawn_worker(addr);
        tokio::time::sleep(Duration::from_millis(150)).await;

        let url = format!("ws://127.0.0.1:{port}/app/app-key");
        let (mut a, _) = tokio_tungstenite::connect_async(url.clone())
            .await
            .expect("connect a");
        let (mut b, _) = tokio_tungstenite::connect_async(url)
            .await
            .expect("connect b");

        a.send(Message::Text("aaa".into())).await.unwrap();
        b.send(Message::Text("bbb".into())).await.unwrap();

        let ma = tokio::time::timeout(Duration::from_secs(5), a.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let mb = tokio::time::timeout(Duration::from_secs(5), b.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(ma.into_text().unwrap(), "aaa");
        assert_eq!(mb.into_text().unwrap(), "bbb");

        shutdown.store(true, Ordering::SeqCst);
        handle.join().unwrap();
    }
}
