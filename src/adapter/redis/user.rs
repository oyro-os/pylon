//! Cross-node user/identity operations: the atomic signin/signout connection
//! refcount, the cluster online check, and the per-user pub/sub publish. All
//! return `anyhow::Result` (or are best-effort with logging); callers fall back
//! to the node-local adapter on error.

use super::client::Scripts;
use super::envelope::{Envelope, EnvelopeKind};
use super::keys::{member_token, Keys};
use crate::protocol::socket_id::SocketId;
use fred::clients::Pool;
use fred::interfaces::{HashesInterface, PubsubInterface};
use serde_json::Value;

/// Run USER_SIGNIN. Returns the cluster `first_for_user` edge (HLEN == 1 → the user
/// came online cluster-wide via this connection).
#[allow(clippy::too_many_arguments)]
pub(super) async fn signin(
    scripts: &Scripts,
    pool: &Pool,
    keys: &Keys,
    node_id: &str,
    app: &str,
    user_id: &str,
    socket_id: &SocketId,
    ttl_secs: u64,
) -> anyhow::Result<bool> {
    let token = member_token(node_id, socket_id.as_str());
    let conn: i64 = scripts
        .user_signin
        .evalsha_with_reload::<i64, _, _>(
            pool.next(),
            vec![keys.usr(app, user_id), keys.users(app)],
            vec![
                token,
                (super::now_ms() + ttl_secs * 1000).to_string(),
                ttl_secs.to_string(),
                user_id.to_string(),
            ],
        )
        .await?;
    Ok(conn == 1)
}

/// Run USER_SIGNOUT. Returns the cluster `last_for_user` edge (HLEN == 0 → the
/// user's last cluster connection just dropped).
pub(super) async fn signout(
    scripts: &Scripts,
    pool: &Pool,
    keys: &Keys,
    node_id: &str,
    app: &str,
    user_id: &str,
    socket_id: &SocketId,
) -> anyhow::Result<bool> {
    let token = member_token(node_id, socket_id.as_str());
    let conn: i64 = scripts
        .user_signout
        .evalsha_with_reload::<i64, _, _>(
            pool.next(),
            vec![keys.usr(app, user_id), keys.users(app)],
            vec![token, user_id.to_string()],
        )
        .await?;
    Ok(conn == 0)
}

/// Cluster online check: `HLEN usr > 0`.
pub(super) async fn is_online(
    pool: &Pool,
    keys: &Keys,
    app: &str,
    user_id: &str,
) -> anyhow::Result<bool> {
    let n: i64 = pool.next().hlen(keys.usr(app, user_id)).await?;
    Ok(n > 0)
}

/// Publish a control/notify envelope on a per-user channel. `frame` is the
/// pre-encoded v7 frame for `UserSend`; `Null` for the other kinds. `node_id` is
/// the publisher (self) for live paths, or the DEAD node (token prefix) from the
/// sweeper so every live node — including the sweeper's own — acts on it.
/// Best-effort: logs + continues on any Redis error.
#[allow(clippy::too_many_arguments)]
pub(super) async fn publish(
    pool: &Pool,
    channel: &str,
    node_id: &str,
    app: &str,
    user_id: &str,
    kind: EnvelopeKind,
    frame: Value,
) {
    let env = Envelope {
        node_id: node_id.to_string(),
        app: app.to_string(),
        kind,
        channel: user_id.to_string(),
        event: frame,
        except: None,
    };
    if let Ok(payload) = String::from_utf8(env.encode()) {
        if let Err(e) = pool
            .next()
            .publish::<(), _, _>(channel.to_string(), payload)
            .await
        {
            tracing::warn!(error = %e, app, user_id, "redis user publish failed");
        }
    }
}
