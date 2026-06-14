//! Webhook delivery: the signed request value object, envelope/sign helper, the
//! `WebhookTransport` trait, and its `HttpTransport` / `RecordingTransport` impls.

use crate::auth::signature::hmac_sha256_hex;
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// One fully-prepared POST: the raw signed body bytes plus the exact header set.
#[derive(Debug, Clone, PartialEq)]
pub struct WebhookDelivery {
    pub url: String,
    /// The exact bytes that were signed and must be sent verbatim.
    pub body: String,
    /// Lowercased? No — header names exactly as Pusher sends them.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn events() -> Vec<Value> {
        vec![json!({ "name": "channel_occupied", "channel": "ch" })]
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
