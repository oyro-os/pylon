use super::{App, AppManager};

pub struct StaticFileAppManager {
    apps: Vec<App>,
}

impl StaticFileAppManager {
    pub fn from_json(raw: &str) -> anyhow::Result<Self> {
        Ok(Self {
            apps: serde_json::from_str(raw)?,
        })
    }
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        Self::from_json(&std::fs::read_to_string(path)?)
    }
}

#[async_trait::async_trait]
impl AppManager for StaticFileAppManager {
    async fn by_key(&self, key: &str) -> Option<App> {
        self.apps.iter().find(|a| a.key == key).cloned()
    }
    async fn by_id(&self, id: &str) -> Option<App> {
        self.apps.iter().find(|a| a.id == id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"[
        {"name":"Example","id":"app-id","key":"app-key","secret":"app-secret",
         "capacity":2,"client_messages_enabled":true,"subscription_count_enabled":true}
    ]"#;

    #[tokio::test]
    async fn looks_up_by_key_and_id() {
        let m = StaticFileAppManager::from_json(SAMPLE).unwrap();
        let app = m.by_key("app-key").await.expect("found by key");
        assert_eq!(app.id, "app-id");
        assert_eq!(app.capacity, 2);
        assert!(app.subscription_count_enabled);
        assert!(m.by_id("app-id").await.is_some());
        assert!(m.by_key("nope").await.is_none());
    }
}
