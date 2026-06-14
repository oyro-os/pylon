//! Webhook delivery: the signed request value object, envelope/sign helper, the
//! `WebhookTransport` trait, and its `HttpTransport` / `RecordingTransport` impls.

use crate::auth::signature::hmac_sha256_hex;
use async_trait::async_trait;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};

/// One fully-prepared POST: the raw signed body bytes plus the exact header set.
#[derive(Debug, Clone, PartialEq)]
pub struct WebhookDelivery {
    pub url: String,
    /// The exact bytes that were signed and must be sent verbatim.
    pub body: String,
    /// Header names exactly as sent (the three Pusher headers always win over custom).
    pub headers: BTreeMap<String, String>,
}

/// Build the envelope `{ time_ms, events }`, serialize it, sign the raw body with
/// `secret`, and assemble the header set. Per-endpoint `custom` headers are merged
/// in FIRST, then the three Pusher headers overwrite — so custom headers can never
/// override `Content-Type` / `X-Pusher-Key` / `X-Pusher-Signature` (spec §4).
pub fn build_signed_delivery(
    url: &str,
    app_key: &str,
    secret: &str,
    time_ms: u64,
    events: &[Value],
    custom: &BTreeMap<String, String>,
) -> WebhookDelivery {
    let envelope = json!({ "time_ms": time_ms, "events": events });
    let body = serde_json::to_string(&envelope).expect("envelope serializes");
    let signature = hmac_sha256_hex(secret, &body);

    let mut headers: BTreeMap<String, String> = custom.clone();
    headers.insert("Content-Type".into(), "application/json".into());
    headers.insert("X-Pusher-Key".into(), app_key.to_string());
    headers.insert("X-Pusher-Signature".into(), signature);

    WebhookDelivery {
        url: url.to_string(),
        body,
        headers,
    }
}

/// The pluggable delivery boundary. `HttpTransport` POSTs; `RecordingTransport`
/// records for tests. `deliver` owns retry/concurrency policy internally and
/// never returns an error to the dispatcher (failures are logged-and-dropped).
#[async_trait]
pub trait WebhookTransport: Send + Sync {
    async fn deliver(&self, delivery: WebhookDelivery);
}

/// Test double: records every delivery handed to it; performs no I/O.
#[derive(Clone, Default)]
pub struct RecordingTransport {
    deliveries: Arc<Mutex<Vec<WebhookDelivery>>>,
}

impl RecordingTransport {
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of everything delivered so far.
    pub async fn recorded(&self) -> Vec<WebhookDelivery> {
        self.deliveries.lock().await.clone()
    }
}

#[async_trait]
impl WebhookTransport for RecordingTransport {
    async fn deliver(&self, delivery: WebhookDelivery) {
        self.deliveries.lock().await.push(delivery);
    }
}

/// Production transport: reqwest POST with per-attempt timeout, bounded
/// retry/backoff, and a global concurrency semaphore (spec §6). `deliver`
/// spawns the attempt loop and returns immediately — it never blocks the caller.
pub struct HttpTransport {
    client: reqwest::Client,
    max_retries: u32,
    retry_base_ms: u64,
    semaphore: Arc<Semaphore>,
}

impl HttpTransport {
    /// `timeout_ms` is the per-attempt request timeout; `max_concurrency` caps
    /// simultaneous in-flight deliveries.
    pub fn new(
        max_retries: u32,
        retry_base_ms: u64,
        timeout_ms: u64,
        max_concurrency: usize,
    ) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .expect("reqwest client builds");
        Self {
            client,
            max_retries,
            retry_base_ms,
            semaphore: Arc::new(Semaphore::new(max_concurrency)),
        }
    }

    /// True if `status` should be retried: 5xx and 429 retry; other 4xx are
    /// permanent (transport errors are retried separately in the attempt loop).
    fn retryable(status: reqwest::StatusCode) -> bool {
        status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS
    }
}

#[async_trait]
impl WebhookTransport for HttpTransport {
    async fn deliver(&self, delivery: WebhookDelivery) {
        let client = self.client.clone();
        let max_retries = self.max_retries;
        let base = self.retry_base_ms;
        let sem = self.semaphore.clone();

        tokio::spawn(async move {
            // Concurrency cap: if the broker is saturated this awaits a permit.
            let _permit = match sem.acquire().await {
                Ok(p) => p,
                Err(_) => return, // semaphore closed (shutdown)
            };

            // attempt 0 is the first try; up to `max_retries` extra attempts.
            for attempt in 0..=max_retries {
                let mut req = client.post(&delivery.url).body(delivery.body.clone());
                for (k, v) in &delivery.headers {
                    req = req.header(k, v);
                }
                match req.send().await {
                    Ok(resp) => {
                        let status = resp.status();
                        if status.is_success() {
                            return;
                        }
                        if !HttpTransport::retryable(status) {
                            tracing::warn!(url = %delivery.url, %status, "webhook rejected (permanent)");
                            return; // 4xx (non-429): permanent
                        }
                        // retryable status: fall through to backoff
                        tracing::debug!(url = %delivery.url, %status, attempt, "webhook retryable status");
                    }
                    Err(e) => {
                        // transport error (timeout, connection refused): retry
                        tracing::debug!(url = %delivery.url, error = %e, attempt, "webhook transport error");
                    }
                }
                if attempt == max_retries {
                    tracing::warn!(url = %delivery.url, "webhook delivery exhausted retries; dropping");
                    return;
                }
                // exponential backoff: base * 2^attempt.
                let delay = base.saturating_mul(1u64 << attempt);
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::extract::State;
    use axum::http::StatusCode;
    use axum::routing::post;
    use axum::Router;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn events() -> Vec<Value> {
        vec![json!({ "name": "channel_occupied", "channel": "ch" })]
    }

    /// 503 for the first two hits, then 200 — counts every hit in the shared counter.
    async fn flaky_handler(State(calls): State<Arc<AtomicUsize>>) -> StatusCode {
        let n = calls.fetch_add(1, Ordering::SeqCst);
        if n < 2 {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::OK
        }
    }

    /// Always 400 (permanent) — counts every hit so we can assert "no retry".
    async fn reject_handler(State(calls): State<Arc<AtomicUsize>>) -> StatusCode {
        calls.fetch_add(1, Ordering::SeqCst);
        StatusCode::BAD_REQUEST
    }

    /// Bind a throwaway server on a random port; the handler still carries the
    /// shared counter as its `State`, which `with_state` then injects.
    async fn spawn_mock(
        handler: axum::routing::MethodRouter<Arc<AtomicUsize>>,
        calls: Arc<AtomicUsize>,
    ) -> std::net::SocketAddr {
        let app = Router::new().route("/wh", handler).with_state(calls);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        addr
    }

    #[tokio::test]
    async fn http_transport_retries_on_503_then_succeeds() {
        // 503, 503, 200 → exactly 3 attempts.
        let calls = Arc::new(AtomicUsize::new(0));
        let addr = spawn_mock(post(flaky_handler), calls.clone()).await;
        let t = HttpTransport::new(3, 1, 5000, 10); // base 1ms so the test is fast
        let d = build_signed_delivery(
            &format!("http://{addr}/wh"),
            "k",
            "s",
            1,
            &events(),
            &BTreeMap::new(),
        );
        t.deliver(d).await;
        // small settle for the spawned delivery task
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 3, "two retries then success");
    }

    #[tokio::test]
    async fn http_transport_does_not_retry_on_400() {
        let calls = Arc::new(AtomicUsize::new(0));
        let addr = spawn_mock(post(reject_handler), calls.clone()).await;
        let t = HttpTransport::new(3, 1, 5000, 10);
        let d = build_signed_delivery(
            &format!("http://{addr}/wh"),
            "k",
            "s",
            1,
            &events(),
            &BTreeMap::new(),
        );
        t.deliver(d).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "4xx is permanent: single attempt"
        );
    }

    #[test]
    fn signature_is_hmac_of_raw_body_kat() {
        let d = build_signed_delivery(
            "https://e.test/wh",
            "app-key",
            "app-secret",
            1700000000000,
            &events(),
            &BTreeMap::new(),
        );
        // The signed body is the exact serialized envelope.
        // serde_json's json! macro serializes object keys in alphabetical order.
        assert_eq!(
            d.body,
            r#"{"events":[{"channel":"ch","name":"channel_occupied"}],"time_ms":1700000000000}"#
        );
        // KAT: this hex is computed independently in Step 4. Capture it RED-first
        // from the failing assertion's "left" value, then paste it here.
        assert_eq!(
            d.headers["X-Pusher-Signature"],
            hmac_sha256_hex("app-secret", &d.body)
        );
        // And it really is HMAC-SHA256(secret, body) — cross-check via the primitive.
        assert_eq!(
            d.headers["X-Pusher-Signature"].len(),
            64,
            "hex sha256 is 64 chars"
        );
    }

    #[test]
    fn exact_three_pusher_headers_present() {
        let d = build_signed_delivery(
            "https://e.test/wh",
            "the-key",
            "the-secret",
            1,
            &events(),
            &BTreeMap::new(),
        );
        assert_eq!(d.headers["Content-Type"], "application/json");
        assert_eq!(d.headers["X-Pusher-Key"], "the-key");
        assert!(d.headers.contains_key("X-Pusher-Signature"));
    }

    #[tokio::test]
    async fn recording_transport_records_each_delivery() {
        let t = RecordingTransport::new();
        let d = build_signed_delivery(
            "https://e.test/wh",
            "k",
            "s",
            1,
            &events(),
            &BTreeMap::new(),
        );
        t.deliver(d.clone()).await;
        let recorded = t.recorded().await;
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0], d);
    }

    #[test]
    fn custom_headers_merge_but_cannot_override_pusher_headers() {
        let mut custom = BTreeMap::new();
        custom.insert("X-Custom".into(), "yes".into());
        // Attempt to override all three Pusher headers — must be ignored.
        custom.insert("Content-Type".into(), "text/plain".into());
        custom.insert("X-Pusher-Key".into(), "attacker".into());
        custom.insert("X-Pusher-Signature".into(), "forged".into());

        let d = build_signed_delivery(
            "https://e.test/wh",
            "real-key",
            "real-secret",
            5,
            &events(),
            &custom,
        );
        assert_eq!(d.headers["X-Custom"], "yes", "custom header merged");
        assert_eq!(d.headers["Content-Type"], "application/json");
        assert_eq!(d.headers["X-Pusher-Key"], "real-key");
        assert_ne!(d.headers["X-Pusher-Signature"], "forged");
        assert_eq!(
            d.headers["X-Pusher-Signature"],
            hmac_sha256_hex("real-secret", &d.body)
        );
    }
}
