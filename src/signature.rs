//! Pusher authentication primitives.
//!
//! These mirror the verified behaviour of the C++ rofrof (`src/utils/utils.h`
//! and `RofRofController::verifySignature` / `IChannel::verifySignature`). The
//! unit tests below are known-answer tests so the Rust impl stays bug-for-bug
//! compatible with what the reference server actually accepts.

use hmac::{Hmac, Mac};
use md5::{Digest, Md5};
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// Lowercase-hex HMAC-SHA256 of `msg` keyed by `secret`.
pub fn hmac_sha256_hex(secret: &str, msg: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(msg.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Lowercase-hex MD5 (used for the `body_md5` field of the REST signature).
pub fn md5_hex(data: &[u8]) -> String {
    let mut h = Md5::new();
    h.update(data);
    hex::encode(h.finalize())
}

/// Length-checked constant-time string comparison for signatures.
pub fn constant_time_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.ct_eq(b).into()
}

/// Pusher REST API signature.
///
/// `HMAC_SHA256(secret, "<METHOD>\n<path>\n<sorted query params>")`, where the
/// params string is `k=v&k2=v2...` sorted lexicographically by key, excluding
/// `auth_signature` and including `body_md5=<md5(body)>` for requests with a body.
pub fn rest_signature(secret: &str, method: &str, path: &str, sorted_params: &str) -> String {
    let msg = format!("{}\n{}\n{}", method.to_uppercase(), path, sorted_params);
    hmac_sha256_hex(secret, &msg)
}

/// Channel auth string a client signs to join a private/presence channel.
///
/// `HMAC_SHA256(secret, "<socket_id>:<channel>[:<channel_data>]")`. The token the
/// client sends is `"<app_key>:<this signature>"`.
pub fn channel_signature(
    secret: &str,
    socket_id: &str,
    channel: &str,
    channel_data: Option<&str>,
) -> String {
    let msg = match channel_data {
        Some(cd) if !cd.is_empty() => format!("{socket_id}:{channel}:{cd}"),
        _ => format!("{socket_id}:{channel}"),
    };
    hmac_sha256_hex(secret, &msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hmac_known_answer() {
        assert_eq!(
            hmac_sha256_hex("key", "The quick brown fox"),
            "203d1e5cedd2d18f8c5a3beff0bd9c1ebcb97097dfcb288c46b00c9227fde2c0"
        );
    }

    #[test]
    fn md5_known_answer() {
        assert_eq!(md5_hex(b"hello world"), "5eb63bbbe01eeed093cb22bb8f5acdc3");
    }

    #[test]
    fn channel_auth_matches_reference() {
        // Same inputs the C++ IChannel::verifySignature would hash.
        assert_eq!(
            channel_signature("secret", "123.456", "private-foo", None),
            "70492d107085f5eed6c826e9deabe88bd9466b7349d812f7579b263318287644"
        );
    }

    #[test]
    fn constant_time_eq_behaviour() {
        assert!(constant_time_eq("abc", "abc"));
        assert!(!constant_time_eq("abc", "abd"));
        assert!(!constant_time_eq("abc", "abcd"));
    }
}
