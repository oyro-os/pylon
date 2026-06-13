//! Server-level configuration (defaults + `PYLON_*` env overrides).

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub bind: String,
    pub port: u16,
    pub activity_timeout: u32,
    pub pong_timeout: u32,
    pub strict_protocol: bool,
    pub apps_path: String,
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
        c
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
    }
}
