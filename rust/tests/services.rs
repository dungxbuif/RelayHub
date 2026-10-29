mod support;

use redis::AsyncCommands;
use support::TestServices;

#[tokio::test]
async fn isolated_services_roundtrip() {
    let services = TestServices::start().await;
    let workspace: uuid::Uuid = sqlx::query_scalar("SELECT $1::uuid")
        .bind(services.workspace_id)
        .fetch_one(&services.postgres)
        .await
        .unwrap();
    assert_eq!(workspace, services.workspace_id);

    sqlx::query(
        "CREATE TABLE relayhub_test_probe (workspace_id uuid PRIMARY KEY, payload text NOT NULL)",
    )
    .execute(&services.postgres)
    .await
    .unwrap();
    sqlx::query("INSERT INTO relayhub_test_probe (workspace_id, payload) VALUES ($1, $2)")
        .bind(services.workspace_id)
        .bind("persisted")
        .execute(&services.postgres)
        .await
        .unwrap();
    let payload: String =
        sqlx::query_scalar("SELECT payload FROM relayhub_test_probe WHERE workspace_id = $1")
            .bind(services.workspace_id)
            .fetch_one(&services.postgres)
            .await
            .unwrap();
    assert_eq!(payload, "persisted");
    let deleted: uuid::Uuid = sqlx::query_scalar(
        "DELETE FROM relayhub_test_probe WHERE workspace_id = $1 RETURNING workspace_id",
    )
    .bind(services.workspace_id)
    .fetch_one(&services.postgres)
    .await
    .unwrap();
    assert_eq!(deleted, services.workspace_id);
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM relayhub_test_probe WHERE workspace_id = $1)",
    )
    .bind(services.workspace_id)
    .fetch_one(&services.postgres)
    .await
    .unwrap();
    assert!(!exists);
    sqlx::query("DROP TABLE relayhub_test_probe")
        .execute(&services.postgres)
        .await
        .unwrap();

    let stream = services
        .jetstream
        .create_stream(async_nats::jetstream::stream::Config {
            name: services.namespace.clone(),
            subjects: vec![format!("{}.*", services.namespace)],
            ..Default::default()
        })
        .await
        .unwrap();
    let ack = services
        .jetstream
        .publish(format!("{}.probe", services.namespace), "harness".into())
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(ack.stream, services.namespace);
    assert_eq!(
        stream
            .get_raw_message(ack.sequence)
            .await
            .unwrap()
            .payload
            .as_ref(),
        b"harness"
    );

    let mut redis = services.redis.clone();
    let key = format!("{}:probe", services.namespace);
    let _: () = redis.set_ex(&key, "harness", 60).await.unwrap();
    assert_eq!(redis.get::<_, String>(&key).await.unwrap(), "harness");
}
