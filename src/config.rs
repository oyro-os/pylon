//! App configuration, loaded from `apps.json` (same format as the C++ rofrof).

use serde::Deserialize;

/// A registered Pusher application. Field names match the JSON keys exactly, so
/// serde maps them without renames.
#[derive(Debug, Clone, Deserialize)]
pub struct App {
    pub name: String,
    pub id: String,
    pub key: String,
    pub secret: String,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub client_messages_enabled: bool,
    #[serde(default)]
    pub capacity: u32,
    #[serde(default)]
    pub statistics_enabled: bool,
}

/// Read and parse the app list from a JSON file at `path`.
pub fn load_apps(path: &str) -> anyhow::Result<Vec<App>> {
    let raw = std::fs::read_to_string(path)?;
    parse_apps(&raw)
}

/// Parse the app list from a JSON string. Kept separate from file IO so it can
/// be unit-tested without a bundled (and secret-bearing) fixture on disk.
pub fn parse_apps(raw: &str) -> anyhow::Result<Vec<App>> {
    Ok(serde_json::from_str(raw)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_apps_from_json() {
        // Inline literal with placeholder values — no real credentials in the repo.
        let raw = r#"[
            {"name":"Example","id":"app-id","key":"example-key",
             "secret":"example-secret","capacity":10000,
             "client_messages_enabled":true}
        ]"#;
        let apps = parse_apps(raw).expect("inline apps JSON should parse");
        assert_eq!(apps.len(), 1);
        assert_eq!(apps[0].key, "example-key");
        assert!(apps[0].client_messages_enabled);
        assert_eq!(apps[0].capacity, 10000);
    }
}
