//! App (tenant) definitions and the AppManager seam (static file now; DB in SP6).

pub mod static_file;

use serde::Deserialize;

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
    #[serde(default)]
    pub subscription_count_enabled: bool,
}

#[async_trait::async_trait]
pub trait AppManager: Send + Sync {
    async fn by_key(&self, key: &str) -> Option<App>;
    async fn by_id(&self, id: &str) -> Option<App>;
}
