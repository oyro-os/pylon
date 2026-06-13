//! Channel-name classification. SP1 only acts on `Public`; the auth-required and
//! cache/encrypted kinds are recognized here but handled in SP2/SP3.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChannelKind {
    Public,
    Private,
    Presence,
    PrivateEncrypted,
    Cache,
}

impl ChannelKind {
    pub fn of(name: &str) -> ChannelKind {
        if name.starts_with("private-encrypted-") {
            ChannelKind::PrivateEncrypted
        } else if name.starts_with("presence-") {
            ChannelKind::Presence
        } else if name.starts_with("private-") {
            ChannelKind::Private
        } else if name.starts_with("cache-") {
            ChannelKind::Cache
        } else {
            ChannelKind::Public
        }
    }

    pub fn requires_auth(self) -> bool {
        matches!(
            self,
            Self::Private | Self::Presence | Self::PrivateEncrypted
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_by_prefix() {
        assert_eq!(ChannelKind::of("my-channel"), ChannelKind::Public);
        assert_eq!(ChannelKind::of("private-foo"), ChannelKind::Private);
        assert_eq!(ChannelKind::of("presence-room"), ChannelKind::Presence);
        assert_eq!(
            ChannelKind::of("private-encrypted-x"),
            ChannelKind::PrivateEncrypted
        );
        assert_eq!(ChannelKind::of("cache-feed"), ChannelKind::Cache);
    }

    #[test]
    fn auth_requirement() {
        assert!(!ChannelKind::Public.requires_auth());
        assert!(ChannelKind::Private.requires_auth());
        assert!(ChannelKind::Presence.requires_auth());
        assert!(ChannelKind::PrivateEncrypted.requires_auth());
    }
}
