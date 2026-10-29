//! Real disposable stores and library gateway subprocesses; not a production host.
//! Requires a local Docker daemon, Compose v2 with --wait, and the pinned images
//! (or registry access). Missing prerequisites fail; no test is ignored.
//! The live host publish/status/drain gate is exercised by remake-acceptance.sh;
//! these tests keep recovery scenarios disposable and deterministic.
use axum::response::IntoResponse;
use futures_util::StreamExt;
use relayhub::{
    delivery::{Deliveries, ProcessingResult},
    events::Events,
    gateway::Gateway,
    identity::Identity,
    outbox::Outbox,
    presence::Presence,
    protocol::{Accepted, DeliveryLease, EventInput, Frame, Principal},
};
use std::{
    io::Write,
    process::{Command, Stdio},
    time::Duration,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

struct Stack {
    project: String,
    owned: bool,
}
impl Stack {
    fn command(&self) -> Command {
        let mut c = Command::new("docker");
        c.args([
            "compose",
            "--env-file",
            "/dev/null",
            "-f",
            concat!(env!("CARGO_MANIFEST_DIR"), "/../deploy/remake/compose.yaml"),
            "-p",
            &self.project,
        ]);
        c
    }
    fn run(&self, args: &[&str]) -> Vec<u8> {
        let output = self
            .command()
            .args(args)
            .output()
            .expect("Docker Compose prerequisite missing");
        assert!(
            output.status.success(),
            "disposable Compose operation failed (output withheld)"
        );
        output.stdout
    }
    fn address(&self, service: &str, port: &str) -> String {
        let result = String::from_utf8(self.run(&["port", service, port])).unwrap();
        let address = result.trim();
        assert!(
            address.starts_with("127.0.0.1:"),
            "rehearsal ports must be loopback-only"
        );
        address.to_owned()
    }
    async fn pool(&self, database: &str) -> sqlx::PgPool {
        sqlx::PgPool::connect(&format!(
            "postgresql://relayhub_test@{}/{database}?sslmode=disable",
            self.address("postgres", "5432")
        ))
        .await
        .expect("disposable PostgreSQL unavailable")
    }
    async fn presence(&self, namespace: &str) -> Presence {
        let connection =
            redis::Client::open(format!("redis://{}/0", self.address("redis", "6379")))
                .unwrap()
                .get_multiplexed_async_connection()
                .await
                .expect("disposable Redis unavailable");
        Presence::new(connection, namespace).unwrap()
    }
    async fn broker(&self) -> async_nats::jetstream::Context {
        async_nats::jetstream::new(
            async_nats::connect(format!("nats://{}", self.address("nats", "4222")))
                .await
                .expect("disposable NATS unavailable"),
        )
    }
}
impl Drop for Stack {
    fn drop(&mut self) {
        if self.owned {
            let result = self
                .command()
                .args(["down", "--volumes", "--remove-orphans", "--timeout", "5"])
                .output();
            if !result.is_ok_and(|o| o.status.success()) {
                // Do not abort on a second panic: the acceptance trap also cleans up.
                eprintln!("disposable recovery cleanup failed for {}", self.project);
                assert!(
                    std::thread::panicking(),
                    "disposable recovery cleanup failed"
                );
            }
        }
    }
}
struct Harness {
    stack: Stack,
    pool: sqlx::PgPool,
    broker: async_nats::jetstream::Context,
    presence: Presence,
    events: Events,
    deliveries: Deliveries,
    app: Principal,
    agent: Principal,
    endpoint: Uuid,
}
impl Harness {
    async fn start() -> Self {
        assert!(
            std::path::Path::new(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../deploy/remake/compose.yaml"
            ))
            .exists(),
            "missing Task 11 staging Compose artifact"
        );
        let stack = Stack {
            project: format!("relayhub_rehearsal_{}", Uuid::new_v4().simple()),
            owned: true,
        };
        stack.run(&[
            "up",
            "-d",
            "--wait",
            "--wait-timeout",
            "60",
            "postgres",
            "nats",
            "redis",
        ]);
        let pool = stack.pool("relayhub_rust_test").await;
        Identity::new(pool.clone()).migrate().await.unwrap();
        let events = Events::new(pool.clone());
        events.migrate().await.unwrap();
        let presence = stack.presence(&stack.project).await;
        let deliveries = Deliveries::new(pool.clone(), presence.clone());
        deliveries.migrate().await.unwrap();
        let (user, workspace, app, endpoint) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        sqlx::query("INSERT INTO identity_users(id,email) VALUES($1,$2)")
            .bind(user)
            .bind(format!("{user}@example.test"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO identity_workspaces(id,owner_id) VALUES($1,$2)")
            .bind(workspace)
            .bind(user)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO identity_apps(id,workspace_id,name) VALUES($1,$2,'rehearsal')")
            .bind(app)
            .bind(workspace)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO event_endpoints(id,workspace_id,app_id) VALUES($1,$2,$3)")
            .bind(endpoint)
            .bind(workspace)
            .bind(app)
            .execute(&pool)
            .await
            .unwrap();
        let broker = stack.broker().await;
        Self {
            stack,
            pool,
            broker,
            presence,
            events,
            deliveries,
            endpoint,
            app: Principal::Application {
                workspace_id: workspace,
                app_id: app,
                key_id: Uuid::new_v4(),
            },
            agent: Principal::Agent {
                workspace_id: workspace,
                app_id: app,
                agent_id: Uuid::new_v4(),
                key_id: Uuid::new_v4(),
            },
        }
    }
    fn gateway(&self) -> Gateway {
        Gateway::new(
            Uuid::new_v4(),
            self.deliveries.clone(),
            self.presence.clone(),
            self.broker.clone(),
        )
    }
    fn outbox(&self, pool: sqlx::PgPool, namespace: &str) -> Outbox {
        Outbox::new(pool, self.broker.clone(), namespace).unwrap()
    }
    async fn accept(&self) -> Accepted {
        let a = self
            .events
            .accept_event(
                &self.app,
                EventInput {
                    endpoint_id: self.endpoint,
                    event_type: "recovery".into(),
                    method: "POST".into(),
                    path: "/hook".into(),
                    headers: vec![],
                    body: vec![0, 255, 1],
                },
                "recovery-key",
            )
            .await
            .unwrap();
        assert_eq!(a.status, 202);
        a
    }
    async fn state(&self, id: Uuid) -> (String, i32) {
        sqlx::query_as("SELECT state,attempt_count FROM deliveries WHERE id=$1")
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }
    async fn stream(&self, namespace: &str) -> async_nats::jetstream::stream::Stream {
        self.broker
            .create_stream(async_nats::jetstream::stream::Config {
                name: namespace.into(),
                subjects: vec![format!("{namespace}.v1.workspaces.*.apps.*.deliveries")],
                storage: async_nats::jetstream::stream::StorageType::File,
                max_age: Duration::from_secs(604800),
                ..Default::default()
            })
            .await
            .unwrap()
    }
}
fn lease(frame: Frame) -> DeliveryLease {
    let Frame::Delivery { lease, .. } = frame else {
        panic!("expected delivery")
    };
    lease
}

#[tokio::test]
async fn broker_restart_preserves_pending() {
    let h = Harness::start().await;
    let stream = h.stream(&h.stack.project).await;
    let consumer = stream
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            durable_name: Some("recovery".into()),
            ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
            ack_wait: Duration::from_secs(1),
            ..Default::default()
        })
        .await
        .unwrap();
    let a = h.accept().await;
    let outbox = h.outbox(h.pool.clone(), &h.stack.project);
    assert_eq!(outbox.dispatch_outbox(1).await.unwrap(), 1);
    let mut messages = consumer.messages().await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(10), messages.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let sequence = first.info().unwrap().stream_sequence;
    drop(messages);
    drop(first); // No ACK before the real broker restart.
    h.stack.run(&["restart", "nats"]);
    h.stack
        .run(&["up", "-d", "--wait", "--wait-timeout", "60", "nats"]);
    let broker = h.stack.broker().await;
    let mut stream = broker.get_stream(&h.stack.project).await.unwrap();
    assert_eq!(stream.info().await.unwrap().state.messages, 1);
    let consumer = stream
        .get_consumer::<async_nats::jetstream::consumer::pull::Config>("recovery")
        .await
        .unwrap();
    let mut messages = consumer.messages().await.unwrap();
    let recovered = tokio::time::timeout(Duration::from_secs(10), messages.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(recovered.info().unwrap().stream_sequence, sequence);
    assert!(recovered.info().unwrap().delivered >= 2);
    let notice: serde_json::Value = serde_json::from_slice(&recovered.payload).unwrap();
    assert_eq!(notice["delivery_id"], a.delivery_id.to_string());
    assert_eq!(notice["event_id"], a.event_id.to_string());
    assert_eq!(h.state(a.delivery_id).await, ("pending".into(), 0));
    assert_eq!(outbox.dispatch_outbox(1).await.unwrap(), 0);
    let gateway = h.gateway();
    let session = gateway.connect(&h.agent).await.unwrap();
    let current = lease(
        gateway
            .pending(&h.agent, session, 16)
            .await
            .unwrap()
            .remove(0),
    );
    h.deliveries
        .settle_delivery(&h.agent, current, ProcessingResult::Success)
        .await
        .unwrap();
    recovered.double_ack().await.unwrap();
    assert_eq!(h.state(a.delivery_id).await, ("succeeded".into(), 1));
}

#[tokio::test]
async fn redis_restart_reconnects() {
    let h = Harness::start().await;
    let a = h.accept().await;
    let old = h.gateway();
    let session = old.connect(&h.agent).await.unwrap();
    h.stack.run(&["restart", "redis"]);
    h.stack
        .run(&["up", "-d", "--wait", "--wait-timeout", "60", "redis"]);
    assert!(old.heartbeat(&h.agent, session).await.is_err());
    // MultiplexedConnection is not a reconnecting manager: rebuild library wiring explicitly.
    let presence = h.stack.presence(&h.stack.project).await;
    assert_eq!(presence.owner(&h.agent).await.unwrap(), None);
    assert_eq!(h.state(a.delivery_id).await, ("pending".into(), 0));
    let deliveries = Deliveries::new(h.pool.clone(), presence.clone());
    let gateway = Gateway::new(
        Uuid::new_v4(),
        deliveries.clone(),
        presence,
        h.broker.clone(),
    );
    let session = gateway.connect(&h.agent).await.unwrap();
    let recovered = lease(
        gateway
            .pending(&h.agent, session, 16)
            .await
            .unwrap()
            .remove(0),
    );
    assert_eq!(recovered.event_id, a.event_id);
    deliveries
        .settle_delivery(&h.agent, recovered, ProcessingResult::Success)
        .await
        .unwrap();
    assert_eq!(h.state(a.delivery_id).await, ("succeeded".into(), 1));
}

#[tokio::test]
async fn postgres_restore_recovers_outbox() {
    let h = Harness::start().await;
    let a = h.accept().await;
    let dump = h.stack.run(&[
        "exec",
        "-T",
        "postgres",
        "pg_dump",
        "-U",
        "relayhub_test",
        "-d",
        "relayhub_rust_test",
        "-Fc",
    ]);
    h.stack.run(&[
        "exec",
        "-T",
        "postgres",
        "createdb",
        "-U",
        "relayhub_test",
        "restored",
    ]);
    let restored = h.stack.pool("restored").await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM pg_tables WHERE schemaname='public'")
            .fetch_one(&restored)
            .await
            .unwrap(),
        0
    );
    let mut restore = h
        .stack
        .command()
        .args([
            "exec",
            "-T",
            "postgres",
            "pg_restore",
            "--exit-on-error",
            "--no-owner",
            "-U",
            "relayhub_test",
            "-d",
            "restored",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    restore.stdin.take().unwrap().write_all(&dump).unwrap();
    assert!(
        restore.wait().unwrap().success(),
        "disposable database restore failed"
    );
    let row: (Uuid, String, bool, i64, i64) = sqlx::query_as("SELECT d.event_id,d.state,o.sent_at IS NULL,q.outstanding_deliveries,q.outstanding_payload_bytes FROM deliveries d JOIN event_outbox o ON o.delivery_id=d.id JOIN workspace_quotas q ON q.workspace_id=d.workspace_id WHERE d.id=$1").bind(a.delivery_id).fetch_one(&restored).await.unwrap();
    assert_eq!(row, (a.event_id, "pending".into(), true, 1, 3));
    let namespace = format!("{}_restore", h.stack.project);
    let mut stream = h.stream(&namespace).await;
    let outbox = h.outbox(restored.clone(), &namespace);
    assert_eq!(outbox.dispatch_outbox(10).await.unwrap(), 1);
    assert_eq!(outbox.dispatch_outbox(10).await.unwrap(), 0);
    assert_eq!(stream.info().await.unwrap().state.messages, 1);
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT sent_at IS NOT NULL FROM event_outbox WHERE delivery_id=$1"
        )
        .bind(a.delivery_id)
        .fetch_one(&restored)
        .await
        .unwrap()
    );
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT sent_at IS NULL FROM event_outbox WHERE delivery_id=$1"
        )
        .bind(a.delivery_id)
        .fetch_one(&h.pool)
        .await
        .unwrap()
    );
    let deliveries = Deliveries::new(restored.clone(), h.presence.clone());
    let gateway = Gateway::new(
        Uuid::new_v4(),
        deliveries.clone(),
        h.presence.clone(),
        h.broker.clone(),
    );
    let session = gateway.connect(&h.agent).await.unwrap();
    let recovered = lease(
        gateway
            .pending(&h.agent, session, 16)
            .await
            .unwrap()
            .remove(0),
    );
    assert_eq!(recovered.event_id, a.event_id);
    deliveries
        .settle_delivery(&h.agent, recovered, ProcessingResult::Success)
        .await
        .unwrap();
    let settled: (String, i32, i64, i64) = sqlx::query_as(
        "SELECT d.state,d.attempt_count,q.outstanding_deliveries,q.outstanding_payload_bytes FROM deliveries d JOIN workspace_quotas q ON q.workspace_id=d.workspace_id WHERE d.id=$1",
    )
    .bind(a.delivery_id)
    .fetch_one(&restored)
    .await
    .unwrap();
    assert_eq!(settled, ("succeeded".into(), 1, 0, 0));
    assert_eq!(h.state(a.delivery_id).await, ("pending".into(), 0));
}

struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[tokio::test]
async fn gateway_kill_fences_old_ack() {
    if let Ok(project) = std::env::var("RELAYHUB_RECOVERY_CHILD") {
        assert!(
            project.starts_with("relayhub_rehearsal_")
                && project
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        );
        let stack = Stack {
            project,
            owned: false,
        };
        let pool = stack.pool("relayhub_rust_test").await;
        let presence = stack.presence(&stack.project).await;
        let deliveries = Deliveries::new(pool, presence.clone());
        let gateway = Gateway::new(Uuid::new_v4(), deliveries, presence, stack.broker().await);
        let agent: Principal =
            serde_json::from_str(&std::env::var("RELAYHUB_RECOVERY_AGENT").unwrap()).unwrap();
        let session = gateway.connect(&agent).await.unwrap();
        let current = lease(
            gateway
                .pending(&agent, session, 16)
                .await
                .unwrap()
                .remove(0),
        );
        let mut socket =
            tokio::net::TcpStream::connect(std::env::var("RELAYHUB_RECOVERY_IPC").unwrap())
                .await
                .unwrap();
        socket
            .write_all(&serde_json::to_vec(&current).unwrap())
            .await
            .unwrap();
        socket.shutdown().await.unwrap();
        std::future::pending::<()>().await;
        return;
    }
    let h = Harness::start().await;
    let a = h.accept().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut child = Child(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "gateway_kill_fences_old_ack", "--test-threads=1"])
            .env("RELAYHUB_RECOVERY_CHILD", &h.stack.project)
            .env(
                "RELAYHUB_RECOVERY_AGENT",
                serde_json::to_string(&h.agent).unwrap(),
            )
            .env(
                "RELAYHUB_RECOVERY_IPC",
                listener.local_addr().unwrap().to_string(),
            )
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(15), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), socket.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    let old: DeliveryLease = serde_json::from_slice(&bytes).unwrap();
    child.0.kill().unwrap();
    assert!(!child.0.wait().unwrap().success());
    assert_eq!(h.state(a.delivery_id).await, ("in_flight".into(), 1));
    let gateway = h.gateway();
    let session = gateway.connect(&h.agent).await.unwrap();
    // Advance time in persisted test state; exercise the actual expiry/retry implementation.
    sqlx::query(
        "UPDATE deliveries SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(a.delivery_id)
    .execute(&h.pool)
    .await
    .unwrap();
    assert!(
        gateway
            .pending(&h.agent, session, 16)
            .await
            .unwrap()
            .is_empty()
    );
    sqlx::query(
        "UPDATE deliveries SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(a.delivery_id)
    .execute(&h.pool)
    .await
    .unwrap();
    let new = lease(
        gateway
            .pending(&h.agent, session, 16)
            .await
            .unwrap()
            .remove(0),
    );
    assert_eq!((new.event_id, new.attempt), (a.event_id, 2));
    assert_ne!(old.attempt_token, new.attempt_token);
    assert_eq!(
        h.deliveries
            .settle_delivery(&h.agent, old, ProcessingResult::Success)
            .await
            .unwrap_err()
            .into_response()
            .status()
            .as_u16(),
        409
    );
    assert_eq!(h.state(a.delivery_id).await, ("in_flight".into(), 2));
    h.deliveries
        .settle_delivery(&h.agent, new, ProcessingResult::Success)
        .await
        .unwrap();
    assert_eq!(h.state(a.delivery_id).await, ("succeeded".into(), 2));
}

#[tokio::test]
async fn old_and_new_namespaces_isolated() {
    let h = Harness::start().await;
    // Seed old work before any new publication/ownership changes.
    h.stack.run(&[
        "exec",
        "-T",
        "postgres",
        "createdb",
        "-U",
        "relayhub_test",
        "old_namespace",
    ]);
    let old_pool = h.stack.pool("old_namespace").await;
    sqlx::raw_sql("CREATE TABLE old_work(id int PRIMARY KEY); INSERT INTO old_work VALUES(1)")
        .execute(&old_pool)
        .await
        .unwrap();
    let a = h.accept().await;
    let old_namespace = format!("{}_old", h.stack.project);
    let mut old_stream = h.stream(&old_namespace).await;
    let mut new_stream = h.stream(&h.stack.project).await;
    let old_subject = format!("{old_namespace}.v1.workspaces.legacy.apps.legacy.deliveries");
    h.broker
        .publish(old_subject, "old-pending-sentinel".into())
        .await
        .unwrap()
        .await
        .unwrap();
    assert_eq!(
        h.outbox(h.pool.clone(), &h.stack.project)
            .dispatch_outbox(1)
            .await
            .unwrap(),
        1
    );
    assert_eq!(old_stream.info().await.unwrap().state.messages, 1);
    assert_eq!(new_stream.info().await.unwrap().state.messages, 1);
    let old_presence = h.stack.presence(&old_namespace).await;
    let old_gateway = Gateway::new(
        Uuid::new_v4(),
        Deliveries::new(h.pool.clone(), old_presence.clone()),
        old_presence.clone(),
        h.broker.clone(),
    );
    let old_session = old_gateway.connect(&h.agent).await.unwrap();
    assert_eq!(h.presence.owner(&h.agent).await.unwrap(), None);
    let gateway = h.gateway();
    let session = gateway.connect(&h.agent).await.unwrap();
    gateway.disconnect(&h.agent, session).await.unwrap();
    assert_eq!(
        old_presence
            .owner(&h.agent)
            .await
            .unwrap()
            .unwrap()
            .session_id,
        old_session
    );
    // A separate old database sentinel stands in for old data; no legacy service is contacted.
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tablename='deliveries'"
        )
        .fetch_one(&old_pool)
        .await
        .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM old_work")
            .fetch_one(&old_pool)
            .await
            .unwrap(),
        1
    );
    assert_eq!(h.state(a.delivery_id).await, ("pending".into(), 0));
    let sentinel = old_stream.get_raw_message(1).await.unwrap();
    assert_eq!(sentinel.payload.as_ref(), b"old-pending-sentinel");
}

#[test]
fn release_rehearsal_uses_the_real_host_artifact() {
    let compose = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../deploy/remake/compose.yaml"
    ));
    let dockerfile = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../deploy/remake/Dockerfile"
    ));
    let acceptance = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../scripts/remake-acceptance.sh"
    ));
    assert!(compose.contains("relayhub:"));
    assert!(compose.contains("entrypoint: [/usr/local/bin/relayhub-server]"));
    assert!(compose.contains("/api/v1/openapi.json"));
    assert!(dockerfile.contains("--bin relayhub-server"));
    assert!(dockerfile.contains("/usr/local/bin/relayhub-server"));
    assert!(
        acceptance.contains("live host authenticated durable publish/status and graceful drain")
    );
    assert!(!acceptance.contains("BLOCKED: production host prerequisites are missing"));
}
