//! REST handoff for the per-core transport (SP9 §3.4).
//!
//! The `mio` worker owns the listener and accepts every connection. WebSocket
//! clients are driven on the worker thread; a plain HTTP request (a Pusher REST
//! publish, `POST /apps/{id}/events`) cannot be served there. Instead the worker
//! hands the raw connection — plus the request head bytes it already read — to the
//! tokio runtime, where the axum [`Router`] serves it.
//!
//! The pieces:
//!
//! * [`RestConn`] — the unit of handoff: a `std::net::TcpStream` (ownership of
//!   the accepted fd, moved out of mio) plus the `prefix` bytes already consumed
//!   from the socket during head detection (these MUST be replayed before any
//!   further reads, or the HTTP parser sees a truncated request). For TLS
//!   connections, the live rustls [`ServerConnection`] is also carried so the
//!   async REST plane can continue driving the encrypted session.
//! * [`mio_to_std`] — the single audited `unsafe` site: transfer fd ownership
//!   from a `mio::net::TcpStream` to a `std::net::TcpStream` with no
//!   double-close. The crate root is `#![deny(unsafe_code)]`; this function
//!   opts in locally.
//! * [`Rewind`] — an `AsyncRead`/`AsyncWrite` adapter that yields `prefix`
//!   first, then delegates to the live tokio stream (plain path).
//! * [`TlsRestStream`] — an `AsyncRead`/`AsyncWrite` adapter that drives the
//!   synchronous rustls `ServerConnection` over a tokio `TcpStream`. It replays
//!   `prefix` (the already-decrypted HTTP head bytes) first, then pulls further
//!   plaintext from the TLS session. Waker-driven: uses
//!   `poll_read_ready`/`poll_write_ready` + `try_read`/`try_write` and returns
//!   `Poll::Pending` (never busy-loops) when the TCP socket isn't ready.
//! * [`serve`] — the tokio task: loop on the handoff channel, wrap each
//!   `RestConn` in the appropriate adapter, and serve it with hyper-util's auto
//!   (HTTP/1+2) connection builder against the cloned `Router` (each connection
//!   on its own `tokio::spawn` so a slow REST client never blocks the handoff
//!   loop).

use axum::Router;
use rustls::server::ServerConnection as TlsConn;
use std::io::{self, Read, Write};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::mpsc::UnboundedReceiver;

/// A connection accepted by the `mio` worker but destined for the tokio/axum
/// REST plane. `fd_stream` owns the raw fd (already non-blocking, inherited from
/// mio); `prefix` is the request-head bytes the worker already read off the
/// socket and which must be replayed to the HTTP parser. `tls` carries the live
/// rustls `ServerConnection` for TLS connections (already handshaked; the worker
/// decrypted the prefix bytes from it). `None` for plain-TCP connections.
pub struct RestConn {
    pub fd_stream: std::net::TcpStream,
    pub prefix: Vec<u8>,
    pub tls: Option<Box<TlsConn>>,
}

/// Transfer ownership of the accepted fd from a `mio::net::TcpStream` to a
/// `std::net::TcpStream`.
///
/// This is the sole `unsafe` site in the crate (root is `#![deny(unsafe_code)]`).
/// The caller MUST have deregistered `mio_stream` from its `Poll` and dropped
/// its slab entry first, so mio's registry no longer references the fd.
#[allow(unsafe_code)]
pub fn mio_to_std(mio_stream: mio::net::TcpStream) -> std::net::TcpStream {
    use std::os::fd::{FromRawFd, IntoRawFd};
    // SAFETY: into_raw_fd transfers ownership of the fd out of the mio stream
    // (mio will NOT close it — it forgets the fd); from_raw_fd takes sole
    // ownership into the std stream (which WILL close it on drop). Exactly one
    // owner at all times — no double-close, no use-after-close.
    let raw = mio_stream.into_raw_fd();
    unsafe { std::net::TcpStream::from_raw_fd(raw) }
}

// ── Plain path ─────────────────────────────────────────────────────────────────

/// `AsyncRead`/`AsyncWrite` adapter that replays `prefix` bytes before
/// delegating to the underlying tokio stream.
///
/// `poll_read` drains `prefix` into the caller's buffer first; once `prefix` is
/// exhausted it delegates straight to `inner`. Writes/flush/shutdown delegate
/// unconditionally — the prefix is read-side only.
struct Rewind {
    prefix: Vec<u8>,
    /// Read cursor into `prefix`.
    pos: usize,
    inner: tokio::net::TcpStream,
}

impl Rewind {
    fn new(prefix: Vec<u8>, inner: tokio::net::TcpStream) -> Self {
        Self {
            prefix,
            pos: 0,
            inner,
        }
    }
}

impl AsyncRead for Rewind {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.pos < this.prefix.len() {
            let remaining = &this.prefix[this.pos..];
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            this.pos += n;
            // Drop the buffer once fully consumed so its memory is released.
            if this.pos >= this.prefix.len() {
                this.prefix = Vec::new();
                this.pos = 0;
            }
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for Rewind {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.inner.is_write_vectored()
    }
}

// ── TLS path ───────────────────────────────────────────────────────────────────

/// `AsyncRead`/`AsyncWrite` adapter that drives a synchronous rustls
/// `ServerConnection` over a tokio `TcpStream`.
///
/// The TLS handshake is already **complete** (the mio worker completed it).
/// The `prefix` field holds application-layer bytes the worker already decrypted
/// and which must be fed to hyper before any further TCP reads.
///
/// # Waker/Pending handling
///
/// `poll_read` and `poll_write` use `poll_read_ready`/`poll_write_ready` plus
/// `try_read`/`try_write` — they never busy-loop. When the TCP socket is not
/// ready, `poll_read_ready`/`poll_write_ready` registers the waker and returns
/// `Pending`. When the socket IS ready but a non-blocking read/write returns
/// `WouldBlock`, we re-register the waker by calling `poll_read_ready`/
/// `poll_write_ready` again so the task will be woken when the socket is ready.
///
/// # Ciphertext buffering (C1 fix)
///
/// `out_ct`/`out_pos` is a persistent outbound ciphertext buffer. Rustls
/// produces ciphertext via `write_tls` — once that call returns the bytes are
/// consumed from rustls's internal buffer and live ONLY in `out_ct`. If the TCP
/// send buffer is full we must not discard them; instead we keep them in
/// `out_ct` and resume writing on the next wakeup. `poll_flush_ct` owns the
/// full drain loop: pull from rustls → write to socket → repeat until both
/// `out_ct` is empty AND `!tls.wants_write()`.
struct TlsRestStream {
    tcp: tokio::net::TcpStream,
    tls: Box<TlsConn>,
    prefix: Vec<u8>,
    prefix_pos: usize,
    /// Ciphertext drained from rustls but not yet fully written to the socket.
    out_ct: Vec<u8>,
    /// Write cursor into `out_ct`; bytes `[..out_pos]` have been sent.
    out_pos: usize,
}

impl TlsRestStream {
    /// Poll-style flush: write all buffered ciphertext to the socket, then pull
    /// more from rustls and repeat, until BOTH the socket buffer is empty
    /// (`out_pos == out_ct.len()`) AND `!tls.wants_write()`.
    ///
    /// Returns `Poll::Ready(Ok(()))` only when fully drained. Returns
    /// `Poll::Pending` (with waker registered) when the TCP send buffer is full.
    /// Returns `Poll::Ready(Err(_))` on any fatal I/O error.
    fn poll_flush_ct(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        loop {
            // (a) Write any already-buffered ciphertext to the TCP socket.
            while self.out_pos < self.out_ct.len() {
                match self.tcp.poll_write_ready(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(())) => {}
                }
                match self.tcp.try_write(&self.out_ct[self.out_pos..]) {
                    Ok(0) => {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "tls rest: socket closed",
                        )));
                    }
                    Ok(n) => self.out_pos += n,
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        // try_write returned WouldBlock; poll_write_ready
                        // already registered the waker — loop to re-check.
                        continue;
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(e) => return Poll::Ready(Err(e)),
                }
            }
            // All buffered bytes written; reset the buffer.
            self.out_ct.clear();
            self.out_pos = 0;

            // (b) Pull more ciphertext from rustls.
            if !self.tls.wants_write() {
                // Fully drained: nothing in our buffer AND rustls has nothing.
                return Poll::Ready(Ok(()));
            }
            match self.tls.write_tls(&mut self.out_ct) {
                Ok(0) => {
                    // rustls produced nothing despite wants_write; treat as done.
                    return Poll::Ready(Ok(()));
                }
                Ok(_) => {
                    // More ciphertext appended to out_ct; loop to send it.
                    self.out_pos = 0;
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }
}

impl AsyncRead for TlsRestStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        // 1. Drain the already-decrypted prefix first (the HTTP request head
        //    the mio worker peeked before deciding this is a REST connection).
        if this.prefix_pos < this.prefix.len() {
            let remaining = &this.prefix[this.prefix_pos..];
            let n = remaining.len().min(buf.remaining());
            buf.put_slice(&remaining[..n]);
            this.prefix_pos += n;
            if this.prefix_pos >= this.prefix.len() {
                this.prefix = Vec::new();
                this.prefix_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }

        // 2. Try to pull plaintext already buffered inside rustls (from a prior
        //    `read_tls` that decoded more than one TLS record).
        let mut chunk = [0u8; 16 * 1024];
        match this.tls.reader().read(&mut chunk) {
            Ok(0) => {} // no buffered plaintext; fall through to read ciphertext
            Ok(n) => {
                buf.put_slice(&chunk[..n]);
                return Poll::Ready(Ok(()));
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {}
            Err(e) => return Poll::Ready(Err(e)),
        }

        // 3. Need more ciphertext from the TCP socket. Wait until readable.
        match this.tcp.poll_read_ready(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }

        // 4. Read ciphertext into a temp buffer and feed it to rustls.
        let mut ct_buf = [0u8; 16 * 1024];
        let n = match this.tcp.try_read(&mut ct_buf) {
            Ok(0) => {
                // TCP EOF → clean close.
                return Poll::Ready(Ok(()));
            }
            Ok(n) => n,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // Spurious readiness: socket not actually ready yet. The prior
                // `poll_read_ready` call consumed the readiness event, so we
                // MUST re-register the waker before returning Pending — otherwise
                // the task will never be woken (I1 fix).
                match this.tcp.poll_read_ready(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                    Poll::Ready(Ok(())) => {
                        // Socket became ready again immediately; self-wake so
                        // the runtime re-polls this future without delay.
                        cx.waker().wake_by_ref();
                        return Poll::Pending;
                    }
                }
            }
            Err(e) => return Poll::Ready(Err(e)),
        };

        // Feed the raw ciphertext into rustls.
        let mut cursor = &ct_buf[..n];
        match this.tls.read_tls(&mut cursor) {
            Ok(_) => {}
            Err(e) => return Poll::Ready(Err(e)),
        }
        match this.tls.process_new_packets() {
            Ok(_) => {}
            Err(e) => {
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, e)));
            }
        }

        // After processing, drive any pending TLS writes (e.g. alerts, key-update).
        // Best-effort: a write-side error here doesn't affect the read result.
        let _ = this.poll_flush_ct(cx);

        // 5. Pull the freshly decrypted plaintext out of rustls.
        match this.tls.reader().read(&mut chunk) {
            Ok(0) => Poll::Ready(Ok(())), // TLS close_notify received
            Ok(n) => {
                buf.put_slice(&chunk[..n]);
                Poll::Ready(Ok(()))
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // More ciphertext needed; re-register the waker and wait.
                // We consumed ciphertext this round so tokio will re-poll when
                // the socket has more data — but we need to re-register the
                // waker since poll_read_ready consumed the readiness event.
                // Re-call poll_read_ready to register the waker for the next
                // round. If the socket is already readable again, we'll get
                // Ready and can proceed; if not, we get Pending and wait.
                match this.tcp.poll_read_ready(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
                    // Socket already readable again (more data arrived); signal
                    // the runtime to re-poll this future immediately by returning
                    // Pending after waking ourselves.
                    Poll::Ready(Ok(())) => {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
            }
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

impl AsyncWrite for TlsRestStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        // Hand the plaintext to rustls (in-memory; builds TLS records).
        let n = match this.tls.writer().write(buf) {
            Ok(n) => n,
            Err(e) => return Poll::Ready(Err(e)),
        };

        // Best-effort: drain whatever rustls just produced. The ciphertext is
        // safely buffered in out_ct/rustls even if the socket is not writable
        // yet, so if poll_flush_ct returns Pending we still report the plaintext
        // bytes as accepted — the caller will drive flush to completion.
        // Pending or Ok(()) — either way, plaintext was accepted.
        if let Poll::Ready(Err(e)) = this.poll_flush_ct(cx) {
            return Poll::Ready(Err(e));
        }

        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        // Drain all pending TLS write records to the TCP socket. Returns Ready
        // only when out_ct is empty AND !tls.wants_write().
        match this.poll_flush_ct(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }

        // Flush the underlying TCP socket.
        Pin::new(&mut this.tcp).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();

        // Queue a TLS close_notify alert (idempotent).
        this.tls.send_close_notify();

        // Drain all pending TLS records (including the close_notify) to the
        // TCP socket. Returns Ready only when fully drained.
        match this.poll_flush_ct(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Ready(Ok(())) => {}
        }

        Pin::new(&mut this.tcp).poll_shutdown(cx)
    }
}

// ── Serve ─────────────────────────────────────────────────────────────────────

/// Drive the REST handoff: pull each [`RestConn`] off `rx` and serve it with the
/// cloned axum [`Router`] on its own task. Returns when the channel closes (all
/// senders dropped — i.e. the worker thread is gone).
pub async fn serve(mut rx: UnboundedReceiver<RestConn>, router: Router) {
    while let Some(conn) = rx.recv().await {
        let router = router.clone();
        tokio::spawn(async move {
            if let Err(e) = serve_one(conn, router).await {
                tracing::debug!(error = %e, "percore REST connection ended with error");
            }
        });
    }
}

/// Serve a single handed-off connection.
///
/// For plain-TCP connections: rebuild a tokio stream from the fd, replay the
/// prefix via [`Rewind`], and run hyper-util's auto HTTP/1+2 server against the
/// router.
///
/// For TLS connections: rebuild a tokio stream from the fd, wrap it together with
/// the live rustls session and the prefix in a [`TlsRestStream`], and serve THAT
/// with the same hyper-util auto server. The decrypted prefix bytes are replayed
/// first, then further reads pull plaintext through the TLS session.
async fn serve_one(conn: RestConn, router: Router) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let RestConn { fd_stream, prefix, tls } = conn;
    // It already came from mio (non-blocking), but be explicit for tokio.
    fd_stream.set_nonblocking(true)?;
    let tokio_stream = tokio::net::TcpStream::from_std(fd_stream)?;

    let service = hyper_util::service::TowerToHyperService::new(router);
    let builder = hyper_util::server::conn::auto::Builder::new(hyper_util::rt::TokioExecutor::new());

    match tls {
        None => {
            // Plain path — unchanged from the original implementation.
            let rewind = Rewind::new(prefix, tokio_stream);
            let io = hyper_util::rt::TokioIo::new(rewind);
            builder.serve_connection(io, service).await?;
        }
        Some(tls_conn) => {
            // TLS path: drive the rustls session from the async plane.
            let tls_stream = TlsRestStream {
                tcp: tokio_stream,
                tls: tls_conn,
                prefix,
                prefix_pos: 0,
                out_ct: Vec::new(),
                out_pos: 0,
            };
            let io = hyper_util::rt::TokioIo::new(tls_stream);
            builder.serve_connection(io, service).await?;
        }
    }

    Ok(())
}
