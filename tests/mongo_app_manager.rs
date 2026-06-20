//! MongoDB-backed MongoAppManager integration test. Fail-loud (assumes a Mongo at
//! PYLON_TEST_MONGO_URL or 127.0.0.1:27018), per the repo's redis_cluster.rs convention.
use pylon::app::{AppManager, mongo::MongoAppManager};
use mongodb::{bson::doc, bson::Document, Client};

fn uri() -> String {
    std::env::var("PYLON_TEST_MONGO_URL")
        .unwrap_or_else(|_| "mongodb://127.0.0.1:27018/pylon_test".into())
}

#[tokio::test]
async fn mongo_resolves_by_id_and_key_and_filters_disabled() {
    let client = Client::with_uri_str(&uri()).await
        .expect("connect Mongo (is pylon-test-mongo up on 27018?)");
    let coll = client.default_database().expect("uri has db").collection::<Document>("apps");

    let n = uuid::Uuid::new_v4().to_string();
    let (id, key, off_id, off_key) =
        (format!("id-{n}"), format!("key-{n}"), format!("offid-{n}"), format!("offkey-{n}"));
    coll.insert_many(vec![
        doc! { "id": &id, "key": &key, "secret": "sec", "name": "Mo", "capacity": 7_i32,
               "client_messages_enabled": true, "subscription_count_enabled": true, "enabled": true,
               "webhooks": [ { "url": "https://e.test", "event_types": ["channel_occupied"] } ] },
        doc! { "id": &off_id, "key": &off_key, "secret": "s", "name": "Off", "capacity": 0_i32,
               "client_messages_enabled": false, "subscription_count_enabled": false, "enabled": false,
               "webhooks": [] },
    ]).await.unwrap();

    let m = MongoAppManager::connect(&uri()).await.unwrap();
    let a = m.by_id(&id).await.unwrap().expect("by_id hit");
    assert_eq!(a.key, key);
    assert_eq!(a.capacity, 7);
    assert!(a.client_messages_enabled);
    assert!(a.has_channel_occupied_webhooks);             // recompute ran
    assert_eq!(m.by_key(&key).await.unwrap().unwrap().id, id);
    assert!(m.by_id("nope-xyz").await.unwrap().is_none()); // missing -> None
    assert!(m.by_id(&off_id).await.unwrap().is_none());    // disabled -> None
    assert!(m.by_key(&off_key).await.unwrap().is_none());
}
