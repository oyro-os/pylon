//! Verify the `auth` token a client sends with `pusher:signin`. Mirrors
//! `auth::channel::verify` but signs "<socket_id>::user::<user_data>".

use crate::auth::signature::{constant_time_eq, user_signature};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum UserAuthError {
    #[error("Bad auth token")]
    Malformed,
    #[error("Auth key mismatch")]
    KeyMismatch,
    #[error("Invalid signature")]
    BadSignature,
}

/// Verify `token` ("<app_key>:<hex_signature>") over the user_data string.
pub fn verify(
    app_key: &str,
    app_secret: &str,
    socket_id: &str,
    user_data: &str,
    token: &str,
) -> Result<(), UserAuthError> {
    let (key, sig) = token.split_once(':').ok_or(UserAuthError::Malformed)?;
    if key != app_key {
        return Err(UserAuthError::KeyMismatch);
    }
    let expected = user_signature(app_secret, socket_id, user_data);
    if constant_time_eq(sig, &expected) {
        Ok(())
    } else {
        Err(UserAuthError::BadSignature)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::signature::user_signature;

    const KEY: &str = "app-key";
    const SECRET: &str = "app-secret";
    const SID: &str = "123.456";
    const UD: &str = r#"{"id":"7"}"#;

    fn token() -> String {
        format!("{KEY}:{}", user_signature(SECRET, SID, UD))
    }

    #[test]
    fn accepts_valid_token() {
        assert_eq!(verify(KEY, SECRET, SID, UD, &token()), Ok(()));
    }

    #[test]
    fn rejects_wrong_key() {
        let t = format!("other:{}", user_signature(SECRET, SID, UD));
        assert_eq!(
            verify(KEY, SECRET, SID, UD, &t),
            Err(UserAuthError::KeyMismatch)
        );
    }

    #[test]
    fn rejects_tampered_user_data() {
        let signed = token();
        assert_eq!(
            verify(KEY, SECRET, SID, r#"{"id":"8"}"#, &signed),
            Err(UserAuthError::BadSignature)
        );
    }

    #[test]
    fn rejects_malformed_token() {
        assert_eq!(
            verify(KEY, SECRET, SID, UD, "no-colon"),
            Err(UserAuthError::Malformed)
        );
    }
}
