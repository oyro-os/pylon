//! GET /health, /healthz, /ready, /readyz — liveness and readiness probes.

use crate::server::router::AppState;
use axum::extract::State;
use axum::http::StatusCode;
use std::sync::atomic::Ordering;

/// Liveness probe. Always `200 ok` — if the process can handle an HTTP request
/// it is alive. Cheap; never panics.
pub async fn get_health() -> (StatusCode, &'static str) {
    (StatusCode::OK, "ok")
}

/// Pure readiness decision — factored out so it is unit-testable without I/O.
///
/// Returns `true` only when the percore fleet is up **and** the node is not
/// draining. All four input combinations:
///
/// | workers_up | draining | ready |
/// |------------|----------|-------|
/// | false      | false    | false |
/// | false      | true     | false |
/// | true       | false    | true  |
/// | true       | true     | false |
pub fn readiness(workers_up: bool, draining: bool) -> bool {
    workers_up && !draining
}

/// Readiness probe.
///
/// - `200 "ready"` — percore fleet is up and not draining (traffic can be sent here).
/// - `503 "draining"` — the fleet is up but a shutdown signal has fired; the LB /
///   k8s controller should stop routing new connections.
/// - `503 "starting"` — the percore fleet has not yet reported its first snapshot
///   (node is still initialising).
///
/// Never panics.
pub async fn get_ready(State(state): State<AppState>) -> (StatusCode, &'static str) {
    let workers_up = crate::transport::percore_metrics_snapshot().is_some();
    let draining = state.draining.load(Ordering::Relaxed);
    if readiness(workers_up, draining) {
        (StatusCode::OK, "ready")
    } else if draining {
        (StatusCode::SERVICE_UNAVAILABLE, "draining")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "starting")
    }
}

#[cfg(test)]
mod tests {
    use super::readiness;

    #[test]
    fn not_up_not_draining_not_ready() {
        assert!(!readiness(false, false));
    }

    #[test]
    fn not_up_draining_not_ready() {
        assert!(!readiness(false, true));
    }

    #[test]
    fn up_draining_not_ready() {
        assert!(!readiness(true, true));
    }

    #[test]
    fn up_not_draining_ready() {
        assert!(readiness(true, false));
    }
}
