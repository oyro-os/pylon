//! Postgres-backed SqlAppManager integration test. Fail-loud (assumes a Postgres at
//! PYLON_TEST_POSTGRES_URL or 127.0.0.1:5433), per the repo's redis_cluster.rs convention.
use pylon::app::{sql::SqlAppManager, AppManager};
use sqlx::any::AnyPoolOptions;

fn url() -> String {
    std::env::var("PYLON_TEST_POSTGRES_URL")
        .unwrap_or_else(|_| "postgres://postgres:pylon@127.0.0.1:5433/pylon_test".into())
}

const DDL: &str = include_str!("../deploy/db/postgres/001_apps.sql");

#[tokio::test]
async fn postgres_resolves_by_id_and_key_and_filters_disabled() {
    sqlx::any::install_default_drivers();
    let setup = AnyPoolOptions::new()
        .max_connections(2)
        .connect(&url())
        .await
        .expect("connect Postgres (is pylon-test-postgres up on 5433?)");
    sqlx::query(DDL)
        .execute(&setup)
        .await
        .expect("create table");

    let n = uuid::Uuid::new_v4().to_string();
    let (id, key, off_id, off_key) = (
        format!("id-{n}"),
        format!("key-{n}"),
        format!("offid-{n}"),
        format!("offkey-{n}"),
    );
    // Postgres uses $1..$N placeholders.
    let ins = "INSERT INTO apps (id,key,secret,name,capacity,client_messages_enabled,\
               subscription_count_enabled,enabled,webhooks) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)";
    sqlx::query(ins)
        .bind(&id)
        .bind(&key)
        .bind("sec")
        .bind("P")
        .bind(7_i64)
        .bind(1_i64)
        .bind(1_i64)
        .bind(1_i64)
        .bind("[{\"url\":\"https://e.test\",\"event_types\":[\"channel_occupied\"]}]")
        .execute(&setup)
        .await
        .unwrap();
    sqlx::query(ins)
        .bind(&off_id)
        .bind(&off_key)
        .bind("s")
        .bind("Off")
        .bind(0_i64)
        .bind(0_i64)
        .bind(0_i64)
        .bind(0_i64)
        .bind("[]")
        .execute(&setup)
        .await
        .unwrap();

    let m = SqlAppManager::connect(&url()).await.unwrap();
    let a = m.by_id(&id).await.unwrap().expect("by_id hit");
    assert_eq!(a.key, key);
    assert_eq!(a.capacity, 7);
    assert!(a.client_messages_enabled);
    assert!(a.has_channel_occupied_webhooks); // recompute ran
    assert_eq!(m.by_key(&key).await.unwrap().unwrap().id, id);
    assert!(m.by_id("nope-xyz").await.unwrap().is_none()); // missing -> None
    assert!(m.by_id(&off_id).await.unwrap().is_none()); // disabled -> None
    assert!(m.by_key(&off_key).await.unwrap().is_none());
}
