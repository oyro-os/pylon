//! GET /apps/{app_id}/channels/{name}/users — presence channel user ids.
//! POST /apps/{app_id}/users/{user_id}/terminate_connections — terminate all user connections.

use crate::http::error::RestError;
use crate::http::rest::auth::authenticate;
use crate::server::router::AppState;
use axum::body::Bytes;
use axum::extract::{OriginalUri, Path, Query, State};
use axum::Json;
use serde_json::{json, Value};
use std::collections::HashMap;

pub async fn get_users(
    State(state): State<AppState>,
    Path((app_id, channel)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
) -> Result<Json<Value>, RestError> {
    let app = authenticate(&state, &app_id, "GET", uri.path(), &params, &[]).await?;
    let users: Vec<Value> = state
        .adapter
        .presence_members(&app.id, &channel)
        .await
        .into_iter()
        .map(|m| json!({ "id": m.user_id }))
        .collect();
    Ok(Json(json!({ "users": users })))
}

pub async fn terminate_user_connections(
    State(state): State<AppState>,
    Path((app_id, user_id)): Path<(String, String)>,
    OriginalUri(uri): OriginalUri,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Result<Json<Value>, RestError> {
    let app = authenticate(&state, &app_id, "POST", uri.path(), &params, &body).await?;
    state.adapter.terminate_user(&app.id, &user_id).await;
    Ok(Json(json!({})))
}
