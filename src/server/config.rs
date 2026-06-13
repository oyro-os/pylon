//! Server-level configuration (defaults + `PYLON_*` env overrides).

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_presence_members: usize,
    pub max_event_payload_bytes: usize,
    pub max_watchlist_size: usize,
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub bind: String,
    pub port: u16,
    pub activity_timeout: u32,
    pub pong_timeout: u32,
    pub strict_protocol: bool,
    pub apps_path: String,
    pub max_presence_members: usize,
    pub max_event_payload_bytes: usize,
    pub max_channels_per_publish: usize,
    pub rest_auth_window_secs: u64,
    pub max_batch_events: usize,
    pub cache_ttl_secs: u64,
    pub max_watchlist_size: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0".into(),
            port: 7000,
            activity_timeout: 120,
            pong_timeout: 30,
            strict_protocol: false,
            apps_path: "apps.json".into(),
            max_presence_members: 100,
            max_event_payload_bytes: 10_240,
            max_channels_per_publish: 100,
            rest_auth_window_secs: 600,
            max_batch_events: 10,
            cache_ttl_secs: 1800,
            max_watchlist_size: 100,
        }
    }
}

impl ServerConfig {
    pub fn from_env() -> Self {
        let mut c = Self::default();
        if let Ok(v) = std::env::var("PYLON_BIND") {
            c.bind = v;
        }
        if let Ok(v) = std::env::var("PYLON_PORT") {
            if let Ok(p) = v.parse() {
                c.port = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_ACTIVITY_TIMEOUT") {
            if let Ok(p) = v.parse() {
                c.activity_timeout = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_PONG_TIMEOUT") {
            if let Ok(p) = v.parse() {
                c.pong_timeout = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_STRICT_PROTOCOL") {
            c.strict_protocol = v == "1" || v.eq_ignore_ascii_case("true");
        }
        if let Ok(v) = std::env::var("PYLON_APPS_PATH") {
            c.apps_path = v;
        }
        if let Ok(v) = std::env::var("PYLON_MAX_PRESENCE_MEMBERS") {
            if let Ok(p) = v.parse() {
                c.max_presence_members = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_MAX_EVENT_PAYLOAD_BYTES") {
            if let Ok(p) = v.parse() {
                c.max_event_payload_bytes = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_MAX_CHANNELS_PER_PUBLISH") {
            if let Ok(p) = v.parse() {
                c.max_channels_per_publish = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_REST_AUTH_WINDOW_SECS") {
            if let Ok(p) = v.parse() {
                c.rest_auth_window_secs = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_MAX_BATCH_EVENTS") {
            if let Ok(p) = v.parse() {
                c.max_batch_events = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_CACHE_TTL_SECS") {
            if let Ok(p) = v.parse() {
                c.cache_ttl_secs = p;
            }
        }
        if let Ok(v) = std::env::var("PYLON_MAX_WATCHLIST_SIZE") {
            if let Ok(p) = v.parse() {
                c.max_watchlist_size = p;
            }
        }
        c
    }

    pub fn limits(&self) -> Limits {
        Limits {
            max_presence_members: self.max_presence_members,
            max_event_payload_bytes: self.max_event_payload_bytes,
            max_watchlist_size: self.max_watchlist_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_follow_spec() {
        let c = ServerConfig::default();
        assert_eq!(c.port, 7000);
        assert_eq!(c.activity_timeout, 120);
        assert_eq!(c.pong_timeout, 30);
        assert!(!c.strict_protocol);
        assert_eq!(c.max_presence_members, 100);
        assert_eq!(c.max_event_payload_bytes, 10_240);
        assert_eq!(c.max_channels_per_publish, 100);
        assert_eq!(c.rest_auth_window_secs, 600);
        assert_eq!(c.max_batch_events, 10);
        assert_eq!(c.cache_ttl_secs, 1800);
        assert_eq!(c.max_watchlist_size, 100);
    }
}
