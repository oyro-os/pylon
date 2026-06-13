use crate::protocol::event::ServerEvent;
use crate::protocol::socket_id::SocketId;
use tokio::sync::mpsc::UnboundedSender;

/// Stored in the registry instead of a socket — broadcasting pushes into the
/// mailbox; the owning connection task writes to its own socket.
#[derive(Clone)]
pub struct ConnectionHandle {
    pub socket_id: SocketId,
    pub mailbox: UnboundedSender<ServerEvent>,
}
