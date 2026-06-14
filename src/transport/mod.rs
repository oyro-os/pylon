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
pub mod frame;
pub mod handshake;
