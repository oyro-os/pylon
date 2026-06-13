pub mod codec;
pub mod command;
pub mod error;
pub mod event;
pub mod socket_id;
pub mod v7;

use codec::Codec;
use error::PusherError;

pub const MIN_PROTOCOL: u8 = 7;
pub const MAX_PROTOCOL: u8 = 7;

/// Pick a codec for the requested `?protocol=` value. Lenient by default
/// (missing → latest supported); strict mode rejects a missing version with 4008.
pub fn negotiate(protocol: Option<&str>, strict: bool) -> Result<Box<dyn Codec>, PusherError> {
    match protocol {
        None => {
            if strict {
                Err(PusherError::no_protocol())
            } else {
                Ok(codec_for(MAX_PROTOCOL))
            }
        }
        Some(s) => {
            let n: u8 = s.parse().map_err(|_| PusherError::invalid_version())?;
            if (MIN_PROTOCOL..=MAX_PROTOCOL).contains(&n) {
                Ok(codec_for(n))
            } else {
                Err(PusherError::unsupported_protocol())
            }
        }
    }
}

/// The single extension point for new protocol versions.
fn codec_for(_version: u8) -> Box<dyn Codec> {
    Box::new(v7::V7Codec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lenient_missing_defaults_to_latest() {
        assert_eq!(negotiate(None, false).unwrap().version(), 7);
    }
    #[test]
    fn strict_missing_is_4008() {
        assert_eq!(negotiate(None, true).unwrap_err().code, 4008);
    }
    #[test]
    fn unparseable_is_4006() {
        assert_eq!(negotiate(Some("abc"), false).unwrap_err().code, 4006);
    }
    #[test]
    fn supported_version_ok() {
        assert_eq!(negotiate(Some("7"), false).unwrap().version(), 7);
    }
    #[test]
    fn unsupported_version_is_4007() {
        assert_eq!(negotiate(Some("3"), false).unwrap_err().code, 4007);
    }
}
