use super::{App, AppLookupError, AppManager};
use mongodb::{bson::doc, Client, Collection};
use std::sync::Arc;

/// App store backed by MongoDB. The connection URI must include a database name
/// (e.g. `mongodb://host:port/dbname`). Reads the `apps` collection.
#[derive(Clone)]
pub struct MongoAppManager {
    apps: Collection<App>,
}

impl MongoAppManager {
    pub async fn connect(uri: &str) -> anyhow::Result<Self> {
        let client = Client::with_uri_str(uri).await?;
        let db = client.default_database()
            .ok_or_else(|| anyhow::anyhow!("mongo URI must include a database name (mongodb://host/dbname)"))?;
        Ok(Self { apps: db.collection::<App>("apps") })
    }

    async fn find(&self, field: &str, val: &str) -> Result<Option<Arc<App>>, AppLookupError> {
        let filter = doc! { field: val, "enabled": true };
        match self.apps.find_one(filter).await.map_err(|e| AppLookupError::Backend(e.to_string()))? {
            None => Ok(None),
            Some(mut app) => {
                app.recompute_has_flags();
                app.validate().map_err(AppLookupError::Decode)?;
                Ok(Some(Arc::new(app)))
            }
        }
    }
}

#[async_trait::async_trait]
impl AppManager for MongoAppManager {
    async fn by_id(&self, id: &str) -> Result<Option<Arc<App>>, AppLookupError> {
        self.find("id", id).await
    }
    async fn by_key(&self, key: &str) -> Result<Option<Arc<App>>, AppLookupError> {
        self.find("key", key).await
    }
}
