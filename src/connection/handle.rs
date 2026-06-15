use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;

/// The per-worker notifier a [`Mailbox`] uses to wake its owning worker on a
/// cross-connection send. Built in [`establish_session`](crate::transport::worker)
/// from the worker's dirty-token channel + the `MAILBOX_WAKER`.
///
/// `token` is this connection's slab key (== its `mio::Token` value); `dirty` is
/// the worker's dirty-token queue; `waker` unblocks the worker's `mio::Poll`.
#[derive(Clone)]
pub struct MailboxNotify {
    /// This connection's slab key — pushed onto `dirty` so the worker drains
    /// exactly this connection's mailbox (and never scans idle ones).
    pub token: usize,
    /// The worker's dirty-token queue. A `send` here is a cheap unbounded push;
    /// the worker dedups the tokens into a `HashSet` before draining.
    pub dirty: std::sync::mpsc::Sender<usize>,
    /// The worker's `MAILBOX_WAKER`. Woken so an idle (blocked-on-poll) worker
    /// re-polls promptly and drains the just-marked connection.
    pub waker: Arc<mio::Waker>,
}

/// The choke point for every cross-connection delivery to a connection.
///
/// Wraps the connection's mailbox [`UnboundedSender`] plus an optional
/// [`MailboxNotify`]. [`Mailbox::send`] pushes the event onto the mailbox and, if
/// a notifier is present, marks this connection dirty (pushes its slab token onto
/// the worker's dirty queue) and wakes the worker. Because EVERY cross-connection
/// delivery routes through [`ConnectionHandle::mailbox`], this single choke point
/// guarantees the worker is always nudged to drain a connection that received a
/// message — so idle connections are never scanned (O(dirty), not O(N)).
///
/// When no notifier is present (`None` — unit/integration tests that construct a
/// `ConnectionHandle` directly and `try_recv` the matching receiver themselves),
/// `send` simply forwards to the mailbox with no wake; correctness still holds
/// because those tests read the receiver directly rather than via the worker loop.
#[derive(Clone)]
pub struct Mailbox {
    inner: UnboundedSender<ServerEvent>,
    notify: Option<MailboxNotify>,
}

impl Mailbox {
    /// Build a mailbox over `inner`, optionally wired to wake `notify`'s worker.
    pub fn new(inner: UnboundedSender<ServerEvent>, notify: Option<MailboxNotify>) -> Self {
        Self { inner, notify }
    }

    /// Push `event` onto the mailbox, then (if wired) mark this connection dirty
    /// and wake its worker so the send is drained promptly.
    ///
    /// Returns the underlying [`UnboundedSender::send`] result so every existing
    /// `h.mailbox.send(ev)` call site is unchanged. A failed dirty-push or wake is
    /// ignored: the unbounded dirty queue effectively never fails while the worker
    /// lives, and a missed wake at worst defers delivery to the next 50ms idle poll
    /// (the unconditional safety-net drain on the worker still catches it), never a
    /// lost message.
    pub fn send(
        &self,
        event: ServerEvent,
    ) -> Result<(), tokio::sync::mpsc::error::SendError<ServerEvent>> {
        self.inner.send(event)?;
        if let Some(n) = &self.notify {
            // Mark dirty BEFORE the wake so the token is queued by the time the
            // woken worker drains its dirty queue (the wake only unblocks poll).
            let _ = n.dirty.send(n.token);
            let _ = n.waker.wake();
        }
        Ok(())
    }
}

/// Stored in the registry instead of a socket — broadcasting pushes into the
/// mailbox; the owning connection task writes to its own socket.
#[derive(Clone)]
pub struct ConnectionHandle {
    pub socket_id: SocketId,
    pub mailbox: Mailbox,
}
