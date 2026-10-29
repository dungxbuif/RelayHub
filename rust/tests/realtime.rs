#[allow(dead_code)]
mod support;

use axum::response::IntoResponse;
use relayhub::{
    events::Events,
    identity::Identity,
    protocol::{EventInput, Frame, Principal},
    realtime::{DisconnectReason, Realtime, RealtimeError},
};
use std::{process::Command, time::Duration};
use support::TestServices;
use uuid::Uuid;

struct Harness {
    services: TestServices,
    realtime: Realtime,
    app: Principal,
    before: Vec<String>,
}
impl Harness {
    async fn start() -> Self {
        let services = TestServices::start().await;
        Identity::new(services.postgres.clone())
            .migrate()
            .await
            .unwrap();
        let events = Events::new(services.postgres.clone());
        events.migrate().await.unwrap();
        let (user, workspace, app, endpoint) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        sqlx::query("INSERT INTO identity_users (id,email) VALUES ($1,$2)")
            .bind(user)
            .bind(format!("{user}@example.test"))
            .execute(&services.postgres)
            .await
            .unwrap();
        sqlx::query("INSERT INTO identity_workspaces (id,owner_id) VALUES ($1,$2)")
            .bind(workspace)
            .bind(user)
            .execute(&services.postgres)
            .await
            .unwrap();
        sqlx::query("INSERT INTO identity_apps (id,workspace_id,name) VALUES ($1,$2,'realtime')")
            .bind(app)
            .bind(workspace)
            .execute(&services.postgres)
            .await
            .unwrap();
        sqlx::query("INSERT INTO event_endpoints (id,workspace_id,app_id) VALUES ($1,$2,$3)")
            .bind(endpoint)
            .bind(workspace)
            .bind(app)
            .execute(&services.postgres)
            .await
            .unwrap();
        let app = Principal::Application {
            workspace_id: workspace,
            app_id: app,
            key_id: Uuid::new_v4(),
        };
        // A pre-existing accepted event catches deletion/mutation as well as realtime inserts.
        events
            .accept_event(
                &app,
                EventInput {
                    endpoint_id: endpoint,
                    event_type: "sentinel".into(),
                    method: "POST".into(),
                    path: "/".into(),
                    headers: vec![],
                    body: b"durable sentinel".to_vec(),
                },
                "sentinel",
            )
            .await
            .unwrap();
        services
            .jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: services.namespace.clone(),
                subjects: vec![format!(
                    "{}.v1.workspaces.*.apps.*.deliveries",
                    services.namespace
                )],
                ..Default::default()
            })
            .await
            .unwrap();
        let realtime = Self::gateway(&services).await;
        let before = snapshot(&services).await;
        Self {
            services,
            realtime,
            app,
            before,
        }
    }
    async fn gateway(services: &TestServices) -> Realtime {
        let output = Command::new("docker")
            .args([
                "compose",
                "--env-file",
                "/dev/null",
                "-f",
                concat!(env!("CARGO_MANIFEST_DIR"), "/tests/compose.yaml"),
                "-p",
                &services.namespace,
                "port",
                "nats",
                "4222",
            ])
            .output()
            .unwrap();
        assert!(output.status.success());
        let address = String::from_utf8(output.stdout).unwrap();
        let client = async_nats::ConnectOptions::new()
            .subscription_capacity(64)
            .connect(format!("nats://{}", address.trim()))
            .await
            .unwrap();
        client.flush().await.unwrap();
        Realtime::new(
            Uuid::new_v4(),
            client,
            services.redis.clone(),
            &services.namespace,
        )
        .unwrap()
    }
    async fn unchanged(&self) {
        assert_eq!(snapshot(&self.services).await, self.before);
        let mut stream = self
            .services
            .jetstream
            .get_stream(&self.services.namespace)
            .await
            .unwrap();
        assert_eq!(stream.info().await.unwrap().state.messages, 0);
    }
}
async fn snapshot(services: &TestServices) -> Vec<String> {
    let mut result = vec![];
    for query in [
        "SELECT row_to_json(t)::text FROM events t ORDER BY row_to_json(t)::text",
        "SELECT row_to_json(t)::text FROM deliveries t ORDER BY row_to_json(t)::text",
        "SELECT row_to_json(t)::text FROM event_outbox t ORDER BY row_to_json(t)::text",
        "SELECT row_to_json(t)::text FROM workspace_quotas t ORDER BY row_to_json(t)::text",
    ] {
        let rows: Vec<String> = sqlx::query_scalar(query)
            .fetch_all(&services.postgres)
            .await
            .unwrap();
        result.extend(rows);
    }
    result
}
fn status(error: RealtimeError) -> u16 {
    error.into_response().status().as_u16()
}
fn body(frame: Frame) -> Vec<u8> {
    match frame {
        Frame::Realtime {
            version: 1,
            channel,
            body,
            ..
        } if channel == "updates" => body,
        _ => panic!("expected realtime frame"),
    }
}

#[tokio::test]
async fn cross_workspace_subscribe_denied() {
    let h = Harness::start().await;
    let mut connection = h.realtime.connect(&h.app).unwrap();
    let other = Principal::Application {
        workspace_id: Uuid::new_v4(),
        app_id: Uuid::new_v4(),
        key_id: Uuid::new_v4(),
    };
    assert_eq!(
        status(connection.subscribe(&other, "updates").await.unwrap_err()),
        403
    );
    connection.subscribe(&h.app, "updates").await.unwrap();
    assert_eq!(
        status(connection.unsubscribe(&other, "updates").await.unwrap_err()),
        403
    );
    for channel in [
        "*",
        ">",
        "",
        "updates..other",
        "workspaces/foreign/apps/foreign/channels/updates",
    ] {
        assert_eq!(
            status(connection.subscribe(&h.app, channel).await.unwrap_err()),
            422
        );
        assert_eq!(
            status(
                h.realtime
                    .publish_realtime(&h.app, channel, b"bad")
                    .await
                    .unwrap_err()
            ),
            422
        );
    }
    assert_eq!(
        h.realtime
            .publish_realtime(&other, "updates", b"foreign")
            .await
            .unwrap()
            .observed_recipients,
        0
    );
    let Principal::Application { workspace_id, .. } = h.app else {
        unreachable!()
    };
    let other_app = Principal::Application {
        workspace_id,
        app_id: Uuid::new_v4(),
        key_id: Uuid::new_v4(),
    };
    assert_eq!(
        h.realtime
            .publish_realtime(&other_app, "updates", b"foreign app")
            .await
            .unwrap()
            .observed_recipients,
        0
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(150), connection.recv())
            .await
            .is_err()
    );
    h.unchanged().await;
}

#[tokio::test]
async fn offline_publish_has_zero_recipients() {
    let h = Harness::start().await;
    let receipt = h
        .realtime
        .publish_realtime(&h.app, "updates", b"offline")
        .await
        .unwrap();
    assert_eq!(receipt.status, 202);
    assert_eq!(receipt.observed_recipients, 0);
    assert_eq!(
        status(
            h.realtime
                .publish_realtime(&h.app, "updates", &vec![0; 1_048_577])
                .await
                .unwrap_err()
        ),
        413
    );
    assert_eq!(
        h.realtime
            .publish_realtime(&h.app, "updates", &vec![255; 1_048_576])
            .await
            .unwrap()
            .observed_recipients,
        0
    );
    h.unchanged().await;
}

#[tokio::test]
async fn no_replay_after_reconnect() {
    let h = Harness::start().await;
    let mut first = h.realtime.connect(&h.app).unwrap();
    first.subscribe(&h.app, "updates").await.unwrap();
    h.realtime
        .publish_realtime(&h.app, "updates", b"old")
        .await
        .unwrap();
    assert_eq!(
        body(
            tokio::time::timeout(Duration::from_secs(2), first.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
        ),
        b"old"
    );
    first.unsubscribe(&h.app, "updates").await.unwrap();
    assert_eq!(
        h.realtime
            .publish_realtime(&h.app, "updates", b"offline")
            .await
            .unwrap()
            .observed_recipients,
        0
    );
    drop(first);
    let mut second = h.realtime.connect(&h.app).unwrap();
    second.subscribe(&h.app, "updates").await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(150), second.recv())
            .await
            .is_err()
    );
    h.realtime
        .publish_realtime(&h.app, "updates", b"new")
        .await
        .unwrap();
    assert_eq!(
        body(
            tokio::time::timeout(Duration::from_secs(2), second.recv())
                .await
                .unwrap()
                .unwrap()
                .unwrap()
        ),
        b"new"
    );
    second.unsubscribe(&h.app, "updates").await.unwrap();
    h.unchanged().await;
}

#[tokio::test]
async fn two_gateway_fanout() {
    let h = Harness::start().await;
    let gateway2 = Harness::gateway(&h.services).await;
    let mut first = h.realtime.connect(&h.app).unwrap();
    let mut second = gateway2.connect(&h.app).unwrap();
    first.subscribe(&h.app, "updates").await.unwrap();
    second.subscribe(&h.app, "updates").await.unwrap();
    // Duplicate subscription must not duplicate fanout or the observed count.
    second.subscribe(&h.app, "updates").await.unwrap();
    let receipt = h
        .realtime
        .publish_realtime(&h.app, "updates", &[0, 255, 1])
        .await
        .unwrap();
    assert_eq!((receipt.status, receipt.observed_recipients), (202, 2));
    for connection in [&mut first, &mut second] {
        assert_eq!(
            body(
                tokio::time::timeout(Duration::from_secs(2), connection.recv())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap()
            ),
            [0, 255, 1]
        );
    }
    second.unsubscribe(&h.app, "updates").await.unwrap();
    assert_eq!(
        gateway2
            .publish_realtime(&h.app, "updates", b"one")
            .await
            .unwrap()
            .observed_recipients,
        1
    );
    assert_eq!(body(first.recv().await.unwrap().unwrap()), b"one");
    h.unchanged().await;
}

#[tokio::test]
async fn slow_reader_is_bounded() {
    let h = Harness::start().await;
    let mut slow = h.realtime.connect(&h.app).unwrap();
    slow.subscribe(&h.app, "updates").await.unwrap();
    slow.subscribe(&h.app, "other").await.unwrap();
    for i in 0..64 {
        h.realtime
            .publish_realtime(&h.app, if i % 2 == 0 { "updates" } else { "other" }, &[i])
            .await
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while slow.buffered_len() < 64 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(slow.buffered_len(), 64);
    h.realtime
        .publish_realtime(&h.app, "updates", b"overflow")
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), slow.closed())
            .await
            .unwrap(),
        DisconnectReason::SlowReader
    );
    assert!(slow.buffered_len() <= 64);
    assert_eq!(slow.recv().await.unwrap_err(), DisconnectReason::SlowReader);
    assert_eq!(DisconnectReason::SlowReader.as_str(), "slow_reader");
    h.unchanged().await;
}

#[tokio::test]
async fn authenticated_socket_routes_frozen_frames_and_revokes() {
    use futures_util::{SinkExt, StreamExt};
    use sha2::{Digest, Sha256};
    use tokio_tungstenite::{
        connect_async,
        tungstenite::{Message, client::IntoClientRequest},
    };
    let h = Harness::start().await;
    let Principal::Application {
        workspace_id,
        app_id,
        key_id,
    } = h.app
    else {
        unreachable!()
    };
    let token = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO identity_credentials(id,workspace_id,app_id,kind,token_hash) VALUES($1,$2,$3,'application',$4)").bind(key_id).bind(workspace_id).bind(app_id).bind(Sha256::digest(token.as_bytes()).to_vec()).execute(&h.services.postgres).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = h
        .realtime
        .router(Identity::new(h.services.postgres.clone()));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut request = format!("ws://{address}/api/v1/realtime/updates")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("sec-websocket-protocol", "relayhub.v1".parse().unwrap());
    assert!(connect_async(request.clone()).await.is_err());
    request
        .headers_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    let (mut socket, _) = connect_async(request.clone()).await.unwrap();
    let frame = Frame::Realtime {
        version: 1,
        request_id: Uuid::new_v4(),
        channel: "updates".into(),
        body: vec![0, 255, 1],
    };
    let receipt: serde_json::Value = reqwest::Client::new()
        .post(format!("http://{address}/api/v1/realtime"))
        .bearer_auth(&token)
        .body(serde_json::to_vec(&frame).unwrap())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        receipt,
        serde_json::json!({"status":202,"observed_recipients":1})
    );
    let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        body(relayhub::protocol::parse_frame(&message.into_data()).unwrap()),
        [0, 255, 1]
    );
    socket
        .send(Message::Text(serde_json::to_string(&frame).unwrap().into()))
        .await
        .unwrap();
    let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        body(relayhub::protocol::parse_frame(&message.into_data()).unwrap()),
        [0, 255, 1]
    );
    sqlx::query("UPDATE identity_credentials SET revoked_at=now() WHERE id=$1")
        .bind(key_id)
        .execute(&h.services.postgres)
        .await
        .unwrap();
    let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        matches!(message, Message::Close(Some(frame)) if u16::from(frame.code)==1008 && frame.reason=="forbidden")
    );
    assert!(connect_async(request).await.is_err());
    server.abort();
    h.unchanged().await;
}

#[tokio::test]
async fn realtime_frames_reject_forged_scope_ack_and_invalid_payloads() {
    let h = Harness::start().await;
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../../contracts/v1/fixtures.json")).unwrap();
    let mut frame = fixture["frames"]
        .as_array()
        .unwrap()
        .iter()
        .find(|frame| frame["type"] == "realtime")
        .unwrap()
        .clone();
    let mut receiver = h.realtime.connect(&h.app).unwrap();
    receiver.subscribe(&h.app, "orders.updated").await.unwrap();
    assert_eq!(
        h.realtime
            .publish_frame(&h.app, &serde_json::to_vec(&frame).unwrap())
            .await
            .unwrap()
            .observed_recipients,
        1
    );
    let received = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(received, Frame::Realtime { body, .. } if body == [0,255,127,128]));
    frame["workspace_id"] = serde_json::json!(Uuid::new_v4());
    assert_eq!(
        status(
            h.realtime
                .publish_frame(&h.app, &serde_json::to_vec(&frame).unwrap())
                .await
                .unwrap_err()
        ),
        422
    );
    frame.as_object_mut().unwrap().remove("workspace_id");
    for (field, value) in [
        ("body", serde_json::json!("!!!")),
        ("version", serde_json::json!(2)),
    ] {
        let mut invalid = frame.clone();
        invalid[field] = value;
        assert_eq!(
            status(
                h.realtime
                    .publish_frame(&h.app, &serde_json::to_vec(&invalid).unwrap())
                    .await
                    .unwrap_err()
            ),
            422
        );
    }
    let ack = Frame::Ack {
        version: 1,
        request_id: Uuid::new_v4(),
        event_id: Uuid::new_v4(),
        delivery_id: Uuid::new_v4(),
        attempt_token: Uuid::new_v4(),
    };
    assert_eq!(
        status(
            h.realtime
                .publish_frame(&h.app, &serde_json::to_vec(&ack).unwrap())
                .await
                .unwrap_err()
        ),
        422
    );
    let Principal::Application {
        workspace_id,
        app_id,
        ..
    } = h.app
    else {
        unreachable!()
    };
    let agent = Principal::Agent {
        workspace_id,
        app_id,
        agent_id: Uuid::new_v4(),
        key_id: Uuid::new_v4(),
    };
    let mut agent_connection = h.realtime.connect(&agent).unwrap();
    agent_connection.subscribe(&agent, "updates").await.unwrap();
    assert_eq!(
        status(
            h.realtime
                .publish_realtime(&agent, "updates", b"denied")
                .await
                .unwrap_err()
        ),
        403
    );
    for principal in [
        Principal::SystemAdmin {
            user_id: Uuid::new_v4(),
        },
        Principal::WorkspaceOwner {
            user_id: Uuid::new_v4(),
            workspace_id,
        },
    ] {
        assert!(matches!(
            h.realtime.connect(&principal),
            Err(RealtimeError::Forbidden)
        ));
    }
    h.unchanged().await;
}

fn service(services: &TestServices, action: &str, name: &str) {
    let output = Command::new("docker")
        .args([
            "compose",
            "--env-file",
            "/dev/null",
            "-f",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/compose.yaml"),
            "-p",
            &services.namespace,
            action,
            name,
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}
#[tokio::test]
async fn redis_loss_preserves_durable_state() {
    let h = Harness::start().await;
    let mut receiver = h.realtime.connect(&h.app).unwrap();
    receiver.subscribe(&h.app, "updates").await.unwrap();
    service(&h.services, "pause", "redis");
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        h.realtime.publish_realtime(&h.app, "updates", b"lost"),
    )
    .await
    .unwrap();
    service(&h.services, "unpause", "redis");
    assert_eq!(status(result.unwrap_err()), 503);
    redis::cmd("FLUSHDB")
        .query_async::<()>(&mut h.services.redis.clone())
        .await
        .unwrap();
    h.unchanged().await;
}
#[tokio::test]
async fn nats_loss_preserves_durable_state() {
    let h = Harness::start().await;
    let mut receiver = h.realtime.connect(&h.app).unwrap();
    receiver.subscribe(&h.app, "updates").await.unwrap();
    service(&h.services, "pause", "nats");
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        h.realtime.publish_realtime(&h.app, "updates", b"uncertain"),
    )
    .await
    .unwrap();
    service(&h.services, "unpause", "nats");
    assert_eq!(status(result.unwrap_err()), 503);
    h.unchanged().await;
}
