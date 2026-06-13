//! Verify Pusher REST API signed requests. The signature is
//! `HMAC_SHA256(secret, "<METHOD>\n<path>\n<sorted-query>")`, where the query is
//! every param except `auth_signature`, keys lowercased and sorted, joined `k=v&…`.

use crate::auth::signature::{constant_time_eq, hmac_sha256_hex, md5_hex};
use std::collections::{BTreeMap, HashMap};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RestAuthError {
    #[error("missing required auth parameter")]
    MissingParam,
    #[error("unsupported auth_version")]
    BadVersion,
    #[error("auth_key does not match app")]
    KeyMismatch,
    #[error("auth_timestamp outside allowed window")]
    Expired,
    #[error("body_md5 mismatch")]
    BadBodyMd5,
    #[error("invalid auth_signature")]
    BadSignature,
}

/// The exact string that is HMAC-signed. `params` must already exclude
/// `auth_signature`; a `BTreeMap` guarantees the keys are sorted.
pub fn signing_string(method: &str, path: &str, params: &BTreeMap<String, String>) -> String {
    let query = params
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    format!("{}\n{}\n{}", method.to_uppercase(), path, query)
}

/// Verify a signed request. `params` is the full decoded query map; `now` is the
/// current unix time (secs); `window` is the allowed clock skew (secs).
#[allow(clippy::too_many_arguments)]
pub fn verify(
    app_key: &str,
    app_secret: &str,
    method: &str,
    path: &str,
    params: &HashMap<String, String>,
    body: &[u8],
    now: u64,
    window: u64,
) -> Result<(), RestAuthError> {
    let get = |k: &str| params.get(k).map(String::as_str);
    if get("auth_version") != Some("1.0") {
        return Err(RestAuthError::BadVersion);
    }
    if get("auth_key") != Some(app_key) {
        return Err(RestAuthError::KeyMismatch);
    }
    let ts: u64 = get("auth_timestamp")
        .ok_or(RestAuthError::MissingParam)?
        .parse()
        .map_err(|_| RestAuthError::MissingParam)?;
    if now.abs_diff(ts) > window {
        return Err(RestAuthError::Expired);
    }
    if !body.is_empty() {
        match get("body_md5") {
            Some(m) if constant_time_eq(m, &md5_hex(body)) => {}
            _ => return Err(RestAuthError::BadBodyMd5),
        }
    }
    let signature = get("auth_signature").ok_or(RestAuthError::MissingParam)?;
    let signed: BTreeMap<String, String> = params
        .iter()
        .map(|(k, v)| (k.to_lowercase(), v.clone()))
        .filter(|(k, _)| k != "auth_signature")
        .collect();
    let expected = hmac_sha256_hex(app_secret, &signing_string(method, path, &signed));
    if constant_time_eq(signature, &expected) {
        Ok(())
    } else {
        Err(RestAuthError::BadSignature)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signed_params(
        secret: &str,
        method: &str,
        path: &str,
        ts: u64,
        body: &[u8],
    ) -> HashMap<String, String> {
        let mut p: BTreeMap<String, String> = BTreeMap::new();
        p.insert("auth_key".into(), "app-key".into());
        p.insert("auth_timestamp".into(), ts.to_string());
        p.insert("auth_version".into(), "1.0".into());
        if !body.is_empty() {
            p.insert("body_md5".into(), md5_hex(body));
        }
        let sig = hmac_sha256_hex(secret, &signing_string(method, path, &p));
        let mut out: HashMap<String, String> = p.into_iter().collect();
        out.insert("auth_signature".into(), sig);
        out
    }

    #[test]
    fn signing_string_matches_pusher_doc_example() {
        let mut p = BTreeMap::new();
        p.insert("auth_key".to_string(), "278d425bdf160c739803".to_string());
        p.insert("auth_timestamp".to_string(), "1353088179".to_string());
        p.insert("auth_version".to_string(), "1.0".to_string());
        p.insert(
            "body_md5".to_string(),
            "ec365a775a4cd0599faeb73354201b6f".to_string(),
        );
        assert_eq!(
            signing_string("POST", "/apps/3/events", &p),
            "POST\n/apps/3/events\nauth_key=278d425bdf160c739803&auth_timestamp=1353088179&auth_version=1.0&body_md5=ec365a775a4cd0599faeb73354201b6f"
        );
    }

    #[test]
    fn accepts_valid_signed_request_with_body() {
        let body = br#"{"name":"e","data":"{}"}"#;
        let p = signed_params("secret", "POST", "/apps/1/events", 1000, body);
        assert_eq!(
            verify(
                "app-key",
                "secret",
                "POST",
                "/apps/1/events",
                &p,
                body,
                1000,
                600
            ),
            Ok(())
        );
    }

    #[test]
    fn accepts_valid_signed_get_without_body() {
        let p = signed_params("secret", "GET", "/apps/1/channels", 1000, b"");
        assert_eq!(
            verify(
                "app-key",
                "secret",
                "GET",
                "/apps/1/channels",
                &p,
                b"",
                1000,
                600
            ),
            Ok(())
        );
    }

    #[test]
    fn rejects_wrong_version() {
        let mut p = signed_params("secret", "GET", "/apps/1/channels", 1000, b"");
        p.insert("auth_version".into(), "2.0".into());
        assert_eq!(
            verify(
                "app-key",
                "secret",
                "GET",
                "/apps/1/channels",
                &p,
                b"",
                1000,
                600
            ),
            Err(RestAuthError::BadVersion)
        );
    }

    #[test]
    fn rejects_expired_timestamp() {
        let p = signed_params("secret", "GET", "/apps/1/channels", 1000, b"");
        // now is 2000, window 600 → |2000-1000| = 1000 > 600
        assert_eq!(
            verify(
                "app-key",
                "secret",
                "GET",
                "/apps/1/channels",
                &p,
                b"",
                2000,
                600
            ),
            Err(RestAuthError::Expired)
        );
    }

    #[test]
    fn rejects_bad_body_md5() {
        let body = br#"{"name":"e","data":"{}"}"#;
        let mut p = signed_params("secret", "POST", "/apps/1/events", 1000, body);
        p.insert("body_md5".into(), md5_hex(b"different"));
        assert_eq!(
            verify(
                "app-key",
                "secret",
                "POST",
                "/apps/1/events",
                &p,
                body,
                1000,
                600
            ),
            Err(RestAuthError::BadBodyMd5)
        );
    }

    #[test]
    fn rejects_bad_signature() {
        let mut p = signed_params("secret", "GET", "/apps/1/channels", 1000, b"");
        p.insert("auth_signature".into(), "deadbeef".into());
        assert_eq!(
            verify(
                "app-key",
                "secret",
                "GET",
                "/apps/1/channels",
                &p,
                b"",
                1000,
                600
            ),
            Err(RestAuthError::BadSignature)
        );
    }
}
