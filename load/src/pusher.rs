use hmac::{Hmac, Mac};
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

use md5::{Digest, Md5};

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
    let mut params = vec![
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
