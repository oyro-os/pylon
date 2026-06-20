//! Admin endpoints (require `Authorization: Bearer <PYLON_ADMIN_TOKEN>`).
//!
//! These endpoints are disabled (404) when `PYLON_ADMIN_TOKEN` is not configured.

use crate::http::error::RestError;
use crate::server::router::AppState;
use axum::{
    body::Bytes,
    extract::{Path, State},
    http::HeaderMap,
};

#[derive(serde::Deserialize)]
pub struct InvalidateBody {
    pub key: String,
}

/// `POST /admin/apps/{app_id}/invalidate`  body: `{ "key": "<app key>" }`
///
/// Disabled (404) unless `PYLON_ADMIN_TOKEN` is set.
/// Requires `Authorization: Bearer <token>` (constant-time compare).
/// Returns 401 on bad/missing token, 400 on a malformed body, 503 if no
/// invalidator (no L2 Redis), 202 on success.
///
/// The request body is taken as raw [`Bytes`] (not `Json<…>`) so that the
/// disabled-check and token-check run BEFORE the body is parsed. An
/// unauthenticated caller therefore cannot probe whether the admin API is
/// enabled by varying the body — the auth decision is identical regardless of
/// body content, and no parse work is done on its behalf.
pub async fn post_invalidate(
    State(state): State<AppState>,
    Path(app_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<axum::http::StatusCode, RestError> {
    let Some(expected) = state.config.app_admin_token.as_deref() else {
        return Err(RestError::not_found("admin api disabled"));
    };
    let presented = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if !check_token(expected, presented) {
        return Err(RestError::unauthorized("invalid admin token"));
    }
    // Authenticated past this point — only now parse the body.
    let parsed: InvalidateBody = serde_json::from_slice(&body)
        .map_err(|e| RestError::bad_request(format!("invalid body: {e}")))?;
    match &state.invalidator {
        Some(inv) => {
            // Task 5 will replace this with the body's action; for now bridge with Refresh
            // (the safe, non-destructive default) so the compile succeeds.
            inv.publish(&app_id, &parsed.key, crate::app::invalidation::InvalidateAction::Refresh)
                .await
                .map_err(|e| RestError::service_unavailable(format!("invalidate publish failed: {e}")))?;
            Ok(axum::http::StatusCode::ACCEPTED)
        }
        None => Err(RestError::service_unavailable(
            "invalidation requires PYLON_APP_CACHE_REDIS_URL",
        )),
    }
}

// ── Pure-logic helpers extracted for unit-testability ───────────────────────

/// Verify a presented Bearer token against the expected value using constant-time
/// comparison. Returns `true` only if both strings are identical.
///
/// The length guard is NOT a timing side-channel risk because the token length is
/// not secret (it is fixed by the operator). It does prevent a potential
/// "all-zeros" short-circuit in some `ct_eq` implementations.
pub fn check_token(expected: &str, presented: &str) -> bool {
    use subtle::ConstantTimeEq;
    presented.len() == expected.len()
        && presented.as_bytes().ct_eq(expected.as_bytes()).unwrap_u8() == 1
}

#[cfg(test)]
mod tests {
    use super::check_token;

    #[test]
    fn correct_token_passes() {
        assert!(check_token("supersecret", "supersecret"));
    }

    #[test]
    fn wrong_token_fails() {
        assert!(!check_token("supersecret", "wrongtoken!"));
    }

    #[test]
    fn length_mismatch_fails() {
        assert!(!check_token("supersecret", "short"));
    }

    #[test]
    fn empty_presented_fails() {
        assert!(!check_token("supersecret", ""));
    }

    #[test]
    fn both_empty_passes() {
        // Edge-case: empty==empty is technically "correct" but the admin token
        // would never be set to empty string (from_env uses .ok() which treats
        // an unset var as None, not "").
        assert!(check_token("", ""));
    }
}
