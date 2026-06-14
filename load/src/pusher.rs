use hmac::{Hmac, Mac};
use md5::{Digest, Md5};
use serde_json::{json, Value};
use sha2::Sha256;

/// Lowercase hex HMAC-SHA256(secret, msg).
pub fn hmac_hex(secret: &str, msg: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac key");
    mac.update(msg.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Channel subscribe auth token: "<key>:" + HMAC(secret, "socket_id:channel[:channel_data]").
pub fn channel_auth(
    key: &str,
    secret: &str,
    socket_id: &str,
    channel: &str,
    channel_data: Option<&str>,
) -> String {
    let mut msg = format!("{socket_id}:{channel}");
    if let Some(cd) = channel_data {
        msg.push(':');
        msg.push_str(cd);
    }
    format!("{key}:{}", hmac_hex(secret, &msg))
}

#[cfg(test)]
mod sign_tests {
    use super::*;

    #[test]
    fn channel_auth_known_answer() {
        let token = channel_auth("app-key", "app-secret", "123.456", "private-foo", None);
        assert!(token.starts_with("app-key:"));
        let sig = token.strip_prefix("app-key:").unwrap();
        assert_eq!(sig.len(), 64); // hex sha256
        assert_eq!(
            sig,
            "c53bc505cb3d68dc9905dea8d5ed3c42f9e24aeed9453b7b9b200ff759958c02"
        );
    }
}

/// Build the signed query string for `POST /apps/{id}/events`.
/// Canonical string = "POST\n/apps/{id}/events\n<sorted k=v joined by &>", where the
/// sorted params include auth_key, auth_timestamp, auth_version=1.0, body_md5.
/// Returns the full query string (without leading '?') including auth_signature.
pub fn sign_post_events(
    key: &str,
    secret: &str,
    app_id: &str,
    body: &str,
    timestamp: u64,
) -> String {
    let body_md5 = hex::encode(Md5::digest(body.as_bytes()));
    let mut params = [
        ("auth_key".to_string(), key.to_string()),
        ("auth_timestamp".to_string(), timestamp.to_string()),
        ("auth_version".to_string(), "1.0".to_string()),
        ("body_md5".to_string(), body_md5),
    ];
    params.sort_by(|a, b| a.0.cmp(&b.0));
    let query: String = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let path = format!("/apps/{app_id}/events");
    let to_sign = format!("POST\n{path}\n{query}");
    let sig = hmac_hex(secret, &to_sign);
    format!("{query}&auth_signature={sig}")
}

#[cfg(test)]
mod rest_sign_tests {
    use super::*;

    #[test]
    fn sign_post_events_shape() {
        let q = sign_post_events("app-key", "app-secret", "app", "{}", 1_700_000_000);
        assert!(q.contains("auth_key=app-key"));
        assert!(q.contains("auth_timestamp=1700000000"));
        assert!(q.contains("auth_version=1.0"));
        assert!(q.contains(&format!("body_md5={}", hex::encode(md5::Md5::digest("{}")))));
        assert!(q.contains("&auth_signature="));
        assert!(q.starts_with("auth_key="));
    }
}

/// A parsed Pusher frame: `{event, channel?, data?}` where data is the (possibly
/// double-encoded) payload string.
#[derive(Debug, Clone)]
pub struct Frame {
    pub event: String,
    pub channel: Option<String>,
    pub data: Option<String>,
}

pub fn parse_frame(text: &str) -> Option<Frame> {
    let v: Value = serde_json::from_str(text).ok()?;
    Some(Frame {
        event: v.get("event")?.as_str()?.to_string(),
        channel: v.get("channel").and_then(|c| c.as_str()).map(String::from),
        data: v.get("data").map(|d| match d {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        }),
    })
}

/// Encode a `pusher:subscribe` frame.
pub fn subscribe_frame(channel: &str, auth: Option<&str>, channel_data: Option<&str>) -> String {
    let mut data = json!({ "channel": channel });
    if let Some(a) = auth {
        data["auth"] = json!(a);
    }
    if let Some(cd) = channel_data {
        data["channel_data"] = json!(cd);
    }
    json!({ "event": "pusher:subscribe", "data": data }).to_string()
}

pub fn pong_frame() -> String {
    json!({ "event": "pusher:pong", "data": {} }).to_string()
}

/// Embed publish-nanos into an event payload object so subscribers can compute latency.
pub fn stamp_payload(seq: u64, publish_nanos: u128) -> String {
    json!({ "seq": seq, "t": publish_nanos.to_string() }).to_string()
}

/// Extract publish-nanos from a received data payload (string-encoded JSON object).
pub fn extract_nanos(data: &str) -> Option<u128> {
    let v: Value = serde_json::from_str(data).ok()?;
    v.get("t")?.as_str()?.parse().ok()
}

#[cfg(test)]
mod frame_tests {
    use super::*;

    #[test]
    fn parse_connection_established() {
        let f = parse_frame(
            r#"{"event":"pusher:connection_established","data":"{\"socket_id\":\"1.2\"}"}"#,
        )
        .unwrap();
        assert_eq!(f.event, "pusher:connection_established");
        let inner: Value = serde_json::from_str(f.data.as_ref().unwrap()).unwrap();
        assert_eq!(inner["socket_id"], "1.2");
    }

    #[test]
    fn subscribe_frame_includes_auth() {
        let s = subscribe_frame("private-x", Some("app-key:deadbeef"), None);
        let v: Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["event"], "pusher:subscribe");
        assert_eq!(v["data"]["channel"], "private-x");
        assert_eq!(v["data"]["auth"], "app-key:deadbeef");
    }

    #[test]
    fn stamp_and_extract_roundtrip() {
        let p = stamp_payload(7, 123_456_789);
        assert_eq!(extract_nanos(&p), Some(123_456_789));
    }
}
