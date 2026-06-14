//! Cross-node presence operations: the atomic join/leave refcount and the cluster
//! roster read. All return `anyhow::Result`; callers fall back to the node-local
//! adapter on error. (members/user_count/reap land in later SP7b tasks.)

use super::client::Scripts;
use super::keys::{member_token, Keys};
use crate::presence::member::PresenceMember;
use crate::protocol::event::PresencePayload;
use crate::protocol::socket_id::SocketId;
use fred::clients::Pool;
use fred::interfaces::HashesInterface;
use serde_json::{Map, Value};
use std::collections::HashMap;

/// Run PRESENCE_JOIN and read the cluster roster. Returns `(first_for_user, roster)`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn join(
    scripts: &Scripts,
    pool: &Pool,
    keys: &Keys,
    node_id: &str,
    app: &str,
    channel: &str,
    member: &PresenceMember,
    socket_id: &SocketId,
) -> anyhow::Result<(bool, PresencePayload)> {
    let token = member_token(node_id, socket_id.as_str());
    let info = serde_json::to_string(&member.user_info)?;
    let conn: i64 = scripts
        .presence_join
        .evalsha_with_reload::<i64, _, _>(
            pool.next(),
            vec![
                keys.presusers(app, channel),
                keys.presinfo(app, channel),
                keys.presmembers(app, channel),
            ],
            vec![member.user_id.clone(), info, token],
        )
        .await?;
    let roster = roster(pool, keys, app, channel).await?;
    Ok((conn == 1, roster))
}

/// Run PRESENCE_LEAVE. Returns `last_for_user`.
#[allow(clippy::too_many_arguments)]
pub(super) async fn leave(
    scripts: &Scripts,
    pool: &Pool,
    keys: &Keys,
    node_id: &str,
    app: &str,
    channel: &str,
    user_id: &str,
    socket_id: &SocketId,
) -> anyhow::Result<bool> {
    let token = member_token(node_id, socket_id.as_str());
    let conn: i64 = scripts
        .presence_leave
        .evalsha_with_reload::<i64, _, _>(
            pool.next(),
            vec![
                keys.presusers(app, channel),
                keys.presinfo(app, channel),
                keys.presmembers(app, channel),
            ],
            vec![user_id.to_string(), token],
        )
        .await?;
    Ok(conn == 0)
}

/// Cluster roster from `presinfo`: sorted ids, id→user_info hash, distinct count.
pub(super) async fn roster(
    pool: &Pool,
    keys: &Keys,
    app: &str,
    channel: &str,
) -> anyhow::Result<PresencePayload> {
    let entries: Vec<(String, String)> = pool.next().hgetall(keys.presinfo(app, channel)).await?;
    let map: HashMap<String, String> = entries.into_iter().collect();
    let mut ids: Vec<String> = map.keys().cloned().collect();
    ids.sort();
    let mut hash = Map::new();
    for id in &ids {
        let info = map
            .get(id)
            .and_then(|s| serde_json::from_str(s).ok())
            .unwrap_or(Value::Null);
        hash.insert(id.clone(), info);
    }
    Ok(PresencePayload {
        count: ids.len(),
        ids,
        hash,
    })
}
