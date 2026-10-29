#[allow(dead_code)]
mod support;

use axum::response::IntoResponse;
use relayhub::{
    events::{AcceptError, Events},
    identity::Identity,
    outbox::Outbox,
    protocol::{Accepted, EventInput, Principal},
};
use sqlx::Row;
use std::{process::Command, sync::Arc, time::Duration};
use support::TestServices;
use uuid::Uuid;

struct Harness {
    services: TestServices,
    events: Events,
    outbox: Outbox,
    principal: Principal,
    workspace: Uuid,
    app: Uuid,
    endpoint: Uuid,
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
        let (principal, workspace, app, endpoint) = tenant(&services.postgres).await;
        let outbox = Outbox::new(
            services.postgres.clone(),
            services.jetstream.clone(),
            &services.namespace,
        )
        .unwrap();
        Self {
            services,
            events,
            outbox,
            principal,
            workspace,
            app,
            endpoint,
        }
    }

    fn input(&self) -> EventInput {
        EventInput {
            endpoint_id: self.endpoint,
            event_type: "test.created".into(),
            method: "POST".into(),
            path: "/hook?value=1".into(),
            headers: vec![
                ("x-value".into(), "one".into()),
                ("x-value".into(), "two".into()),
            ],
            body: vec![0, 255, 1],
        }
    }

    async fn counts(&self) -> (i64, i64, i64) {
        sqlx::query_as("SELECT (SELECT count(*) FROM events), (SELECT count(*) FROM deliveries), (SELECT count(*) FROM event_outbox)")
            .fetch_one(&self.services.postgres).await.unwrap()
    }

    async fn quota(&self) -> (i64, i64) {
        sqlx::query_as("SELECT outstanding_deliveries, outstanding_payload_bytes FROM workspace_quotas WHERE workspace_id = $1")
            .bind(self.workspace).fetch_one(&self.services.postgres).await.unwrap()
    }

    async fn pending_outbox(&self) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM event_outbox WHERE sent_at IS NULL AND discarded_at IS NULL",
        )
        .fetch_one(&self.services.postgres)
        .await
        .unwrap()
    }

    async fn stream(&self, duplicate_window: Duration) -> async_nats::jetstream::stream::Stream {
        self.services
            .jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: self.services.namespace.clone(),
                subjects: vec![format!(
                    "{}.v1.workspaces.*.apps.*.deliveries",
                    self.services.namespace
                )],
                duplicate_window,
                max_age: Duration::from_secs(604800),
                ..Default::default()
            })
            .await
            .unwrap()
    }

    fn broker(&self, action: &str) {
        let result = Command::new("docker")
            .args([
                "compose",
                "--env-file",
                "/dev/null",
                "-f",
                concat!(env!("CARGO_MANIFEST_DIR"), "/tests/compose.yaml"),
                "-p",
                &self.services.namespace,
                action,
                "nats",
            ])
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
    }

    async fn persisted(&self, accepted: &Accepted, input: &EventInput) {
        let row = sqlx::query("SELECT e.payload, e.payload_bytes, d.state, extract(epoch FROM e.expires_at - e.created_at)::bigint AS retention FROM events e JOIN deliveries d ON d.event_id = e.id WHERE e.id = $1 AND d.id = $2")
            .bind(accepted.event_id).bind(accepted.delivery_id).fetch_one(&self.services.postgres).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<EventInput>(&row.get::<Vec<u8>, _>("payload")).unwrap(),
            *input
        );
        assert_eq!(row.get::<i64, _>("payload_bytes"), input.body.len() as i64);
        assert_eq!(row.get::<String, _>("state"), "pending");
        assert_eq!(row.get::<i64, _>("retention"), 604800);
    }
}

async fn tenant(pool: &sqlx::PgPool) -> (Principal, Uuid, Uuid, Uuid) {
    let user = Uuid::new_v4();
    let workspace = Uuid::new_v4();
    let app = Uuid::new_v4();
    let endpoint = Uuid::new_v4();
    sqlx::query("INSERT INTO identity_users (id, email) VALUES ($1, $2)")
        .bind(user)
        .bind(format!("{user}@example.test"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO identity_workspaces (id, owner_id) VALUES ($1, $2)")
        .bind(workspace)
        .bind(user)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO identity_apps (id, workspace_id, name) VALUES ($1, $2, 'test')")
        .bind(app)
        .bind(workspace)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO event_endpoints (id, workspace_id, app_id) VALUES ($1, $2, $3)")
        .bind(endpoint)
        .bind(workspace)
        .bind(app)
        .execute(pool)
        .await
        .unwrap();
    (
        Principal::Application {
            workspace_id: workspace,
            app_id: app,
            key_id: Uuid::new_v4(),
        },
        workspace,
        app,
        endpoint,
    )
}

fn status(error: AcceptError) -> u16 {
    error.into_response().status().as_u16()
}

#[tokio::test]
async fn commit_survives_broker_outage() {
    let h = Harness::start().await;
    let mut stream = h.stream(Duration::from_secs(120)).await;
    h.broker("pause");
    let input = h.input();
    let accepted = h
        .events
        .accept_event(&h.principal, input.clone(), "outage")
        .await
        .unwrap();
    assert_eq!(accepted.status, 202);
    h.persisted(&accepted, &input).await;
    assert!(h.outbox.dispatch_outbox(10).await.is_err());
    assert_eq!(h.counts().await, (1, 1, 1));
    assert_eq!(h.pending_outbox().await, 1);
    assert_eq!(h.quota().await, (1, 3));
    h.broker("unpause");
    let restarted = Outbox::new(
        h.services.postgres.clone(),
        h.services.jetstream.clone(),
        &h.services.namespace,
    )
    .unwrap();
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            if restarted.dispatch_outbox(10).await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(h.pending_outbox().await, 0);
    assert_eq!(stream.info().await.unwrap().state.messages, 1);
    let message = stream.get_raw_message(1).await.unwrap();
    assert_eq!(
        message.subject.as_str(),
        format!(
            "{}.v1.workspaces.{}.apps.{}.deliveries",
            h.services.namespace, h.workspace, h.app
        )
    );
    let notification: serde_json::Value = serde_json::from_slice(&message.payload).unwrap();
    assert_eq!(
        notification["delivery_id"],
        accepted.delivery_id.to_string()
    );
    assert_eq!(notification["event_id"], accepted.event_id.to_string());
    assert_eq!(h.quota().await, (1, 3));
}

#[tokio::test]
async fn publish_crash_deduplicates() {
    let h = Harness::start().await;
    let mut stream = h.stream(Duration::from_secs(1)).await;
    let accepted = h
        .events
        .accept_event(&h.principal, h.input(), "crash")
        .await
        .unwrap();
    // A failing mark transaction models death after PubAck but before database commit.
    sqlx::raw_sql("CREATE FUNCTION fail_outbox_mark() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.sent_at IS NOT NULL THEN RAISE EXCEPTION 'simulated publisher crash'; END IF; RETURN NEW; END $$; CREATE TRIGGER simulate_crash BEFORE UPDATE ON event_outbox FOR EACH ROW EXECUTE FUNCTION fail_outbox_mark();")
        .execute(&h.services.postgres).await.unwrap();
    assert!(h.outbox.dispatch_outbox(1).await.is_err());
    assert_eq!(stream.info().await.unwrap().state.messages, 1);
    assert_eq!(h.pending_outbox().await, 1);
    sqlx::query("DROP TRIGGER simulate_crash ON event_outbox")
        .execute(&h.services.postgres)
        .await
        .unwrap();
    assert_eq!(h.outbox.dispatch_outbox(1).await.unwrap(), 1);
    assert_eq!(stream.info().await.unwrap().state.messages, 1);
    // Outside the broker window, duplicate notifications still identify ONE durable delivery.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    sqlx::query("UPDATE event_outbox SET sent_at = NULL")
        .execute(&h.services.postgres)
        .await
        .unwrap();
    assert_eq!(h.outbox.dispatch_outbox(1).await.unwrap(), 1);
    assert_eq!(stream.info().await.unwrap().state.messages, 2);
    for sequence in [1, 2] {
        let msg = stream.get_raw_message(sequence).await.unwrap();
        let value: serde_json::Value = serde_json::from_slice(&msg.payload).unwrap();
        assert_eq!(value["delivery_id"], accepted.delivery_id.to_string());
    }
    assert_eq!(h.counts().await, (1, 1, 1));
    assert_eq!(h.quota().await, (1, 3));
}

#[tokio::test]
async fn changed_body_same_key_conflicts() {
    let h = Harness::start().await;
    let input = h.input();
    let accepted = h
        .events
        .accept_event(&h.principal, input.clone(), "same")
        .await
        .unwrap();
    assert_eq!(
        h.events
            .accept_event(&h.principal, input.clone(), "same")
            .await
            .unwrap(),
        accepted
    );
    for field in 0..6 {
        let mut changed = input.clone();
        match field {
            0 => changed.body.push(2),
            1 => changed.method = "PUT".into(),
            2 => changed.path = "/other".into(),
            3 => changed.headers.reverse(),
            4 => changed.event_type = "other".into(),
            _ => {
                let other = Uuid::new_v4();
                sqlx::query(
                    "INSERT INTO event_endpoints (id, workspace_id, app_id) VALUES ($1, $2, $3)",
                )
                .bind(other)
                .bind(h.workspace)
                .bind(h.app)
                .execute(&h.services.postgres)
                .await
                .unwrap();
                changed.endpoint_id = other;
            }
        }
        assert_eq!(
            status(
                h.events
                    .accept_event(&h.principal, changed, "same")
                    .await
                    .unwrap_err()
            ),
            409
        );
    }
    assert_eq!(h.counts().await, (1, 1, 1));
    assert_eq!(h.quota().await, (1, 3));
    h.persisted(&accepted, &input).await;
}

#[tokio::test]
async fn concurrent_limit_returns_429() {
    let h = Harness::start().await;
    // Seed real deliveries at the edge, preserving exact counter/state correspondence.
    sqlx::query("INSERT INTO events (id, workspace_id, app_id, endpoint_id, idempotency_key, payload, payload_hash, payload_bytes) SELECT gen_random_uuid(), $1, $2, $3, 'seed-' || n, decode('7b7d','hex'), decode(repeat('00',32),'hex'), 0 FROM generate_series(1,9999) n")
        .bind(h.workspace).bind(h.app).bind(h.endpoint).execute(&h.services.postgres).await.unwrap();
    sqlx::query("INSERT INTO deliveries (id, event_id, workspace_id, app_id) SELECT gen_random_uuid(), id, workspace_id, app_id FROM events")
        .execute(&h.services.postgres).await.unwrap();
    assert_eq!(h.quota().await, (9999, 0));
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..16 {
        let events = h.events.clone();
        let principal = h.principal.clone();
        let input = h.input();
        tasks.spawn(async move {
            events
                .accept_event(&principal, input, &format!("race-{i}"))
                .await
        });
    }
    let mut accepted = 0;
    let mut rejected = 0;
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            Ok(value) => {
                assert_eq!(value.status, 202);
                accepted += 1;
            }
            Err(error) => {
                assert_eq!(status(error), 429);
                rejected += 1;
            }
        }
    }
    assert_eq!((accepted, rejected), (1, 15));
    assert_eq!(h.counts().await, (10000, 10000, 1));
    assert_eq!(h.quota().await, (10000, 3));
}

#[tokio::test]
async fn cross_workspace_key_not_shared() {
    let h = Harness::start().await;
    let (other, workspace, app, endpoint) = tenant(&h.services.postgres).await;
    let accepted = h
        .events
        .accept_event(&h.principal, h.input(), "shared")
        .await
        .unwrap();
    let mut input = h.input();
    input.endpoint_id = endpoint;
    assert_eq!(
        status(
            h.events
                .accept_event(&h.principal, input.clone(), "shared")
                .await
                .unwrap_err()
        ),
        403
    );
    assert_eq!(
        status(
            h.events
                .accept_event(&other, h.input(), "shared")
                .await
                .unwrap_err()
        ),
        403
    );
    let second = h
        .events
        .accept_event(&other, input, "shared")
        .await
        .unwrap();
    assert_ne!(accepted.event_id, second.event_id);
    let row: (Uuid, Uuid) = sqlx::query_as("SELECT workspace_id, app_id FROM events WHERE id = $1")
        .bind(second.event_id)
        .fetch_one(&h.services.postgres)
        .await
        .unwrap();
    assert_eq!(row, (workspace, app));
    // Keys are app-scoped within one workspace too.
    let next_app = Uuid::new_v4();
    let next_endpoint = Uuid::new_v4();
    sqlx::query("INSERT INTO identity_apps (id, workspace_id, name) VALUES ($1,$2,'second')")
        .bind(next_app)
        .bind(h.workspace)
        .execute(&h.services.postgres)
        .await
        .unwrap();
    sqlx::query("INSERT INTO event_endpoints (id, workspace_id, app_id) VALUES ($1,$2,$3)")
        .bind(next_endpoint)
        .bind(h.workspace)
        .bind(next_app)
        .execute(&h.services.postgres)
        .await
        .unwrap();
    let principal = Principal::Application {
        workspace_id: h.workspace,
        app_id: next_app,
        key_id: Uuid::new_v4(),
    };
    let mut input = h.input();
    input.endpoint_id = next_endpoint;
    assert_eq!(
        status(
            h.events
                .accept_event(&h.principal, input.clone(), "shared")
                .await
                .unwrap_err()
        ),
        403
    );
    let third = h
        .events
        .accept_event(&principal, input, "shared")
        .await
        .unwrap();
    assert_ne!(third.event_id, accepted.event_id);
    assert_eq!(h.counts().await, (3, 3, 3));
    assert_eq!(h.quota().await, (2, 6));
}

#[tokio::test]
async fn payload_byte_limit_and_validation_are_transactional() {
    let h = Harness::start().await;
    let mut input = h.input();
    input.body = vec![0; 1_048_577];
    assert_eq!(
        status(
            h.events
                .accept_event(&h.principal, input, "large")
                .await
                .unwrap_err()
        ),
        413
    );
    let mut input = h.input();
    input.path = "/invalid%".into();
    assert_eq!(
        status(
            h.events
                .accept_event(&h.principal, input, "invalid")
                .await
                .unwrap_err()
        ),
        422
    );
    assert_eq!(
        status(
            h.events
                .accept_event(&h.principal, h.input(), "")
                .await
                .unwrap_err()
        ),
        422
    );
    assert_eq!(h.counts().await, (0, 0, 0));
    let mut input = h.input();
    input.body = vec![0; 1_048_576];
    for i in 0..99 {
        assert_eq!(
            h.events
                .accept_event(&h.principal, input.clone(), &format!("bytes-{i}"))
                .await
                .unwrap()
                .status,
            202
        );
    }
    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..12 {
        let events = h.events.clone();
        let principal = h.principal.clone();
        let input = input.clone();
        tasks.spawn(async move {
            events
                .accept_event(&principal, input, &format!("byte-race-{i}"))
                .await
        });
    }
    let mut accepted = 0;
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            Ok(value) => {
                assert_eq!(value.status, 202);
                accepted += 1;
            }
            Err(error) => assert_eq!(status(error), 429),
        }
    }
    assert_eq!(accepted, 1);
    assert_eq!(h.quota().await, (100, 104_857_600));
    assert_eq!(
        status(
            h.events
                .accept_event(&h.principal, h.input(), "over-bytes")
                .await
                .unwrap_err()
        ),
        429
    );
    assert_eq!(h.counts().await, (100, 100, 100));
    // Retries still succeed at capacity without charging twice.
    assert_eq!(
        h.events
            .accept_event(&h.principal, input, "bytes-0")
            .await
            .unwrap()
            .status,
        202
    );
    assert_eq!(h.quota().await, (100, 104_857_600));
}

#[tokio::test]
async fn terminal_and_expiry_release_quota_once() {
    let h = Harness::start().await;
    let accepted = h
        .events
        .accept_event(&h.principal, h.input(), "terminal")
        .await
        .unwrap();
    sqlx::query("UPDATE deliveries SET state = 'succeeded' WHERE id = $1")
        .bind(accepted.delivery_id)
        .execute(&h.services.postgres)
        .await
        .unwrap();
    sqlx::query("UPDATE deliveries SET state = 'succeeded' WHERE id = $1")
        .bind(accepted.delivery_id)
        .execute(&h.services.postgres)
        .await
        .unwrap();
    assert_eq!(h.quota().await, (0, 0));
    assert!(
        sqlx::query("UPDATE deliveries SET state = 'pending' WHERE id = $1")
            .bind(accepted.delivery_id)
            .execute(&h.services.postgres)
            .await
            .is_err()
    );
    let next = h
        .events
        .accept_event(&h.principal, h.input(), "expiry")
        .await
        .unwrap();
    sqlx::query("UPDATE events SET created_at = now() - interval '8 days', expires_at = now() - interval '1 day' WHERE id = $1").bind(next.event_id).execute(&h.services.postgres).await.unwrap();
    assert_eq!(h.events.expire_deliveries(10).await.unwrap(), 1);
    assert_eq!(h.events.expire_deliveries(10).await.unwrap(), 0);
    assert_eq!(h.quota().await, (0, 0));
    let state: String = sqlx::query_scalar("SELECT state FROM deliveries WHERE id = $1")
        .bind(next.delivery_id)
        .fetch_one(&h.services.postgres)
        .await
        .unwrap();
    assert_eq!(state, "expired");
    assert_eq!(h.pending_outbox().await, 0);
    // No broker is needed to discard terminal work.
    assert_eq!(h.outbox.dispatch_outbox(10).await.unwrap(), 0);
}

#[tokio::test]
async fn concurrent_expiry_workers_do_not_deadlock_across_workspaces() {
    let h = Harness::start().await;
    let (_, other_workspace, other_app, other_endpoint) = tenant(&h.services.postgres).await;

    // The first 100 rows alternate A, B; the next 100 alternate B, A.
    // SKIP LOCKED therefore gives concurrent workers opposite quota-lock orders.
    sqlx::query("WITH seeded AS (INSERT INTO events (id, workspace_id, app_id, endpoint_id, idempotency_key, payload, payload_hash, payload_bytes, created_at, expires_at) SELECT gen_random_uuid(), CASE WHEN n <= 100 THEN CASE WHEN n % 2 = 1 THEN $1 ELSE $2 END ELSE CASE WHEN n % 2 = 1 THEN $2 ELSE $1 END END, CASE WHEN n <= 100 THEN CASE WHEN n % 2 = 1 THEN $3 ELSE $4 END ELSE CASE WHEN n % 2 = 1 THEN $4 ELSE $3 END END, CASE WHEN n <= 100 THEN CASE WHEN n % 2 = 1 THEN $5 ELSE $6 END ELSE CASE WHEN n % 2 = 1 THEN $6 ELSE $5 END END, 'expiry-deadlock-' || n, decode('7b7d','hex'), decode(repeat('00',32),'hex'), 0, TIMESTAMPTZ '1999-12-01', TIMESTAMPTZ '2000-01-01' + (n::bigint * interval '1 second') FROM generate_series(1, 200) AS n RETURNING id, workspace_id, app_id) INSERT INTO deliveries (id, event_id, workspace_id, app_id) SELECT gen_random_uuid(), id, workspace_id, app_id FROM seeded")
        .bind(h.workspace)
        .bind(other_workspace)
        .bind(h.app)
        .bind(other_app)
        .bind(h.endpoint)
        .bind(other_endpoint)
        .execute(&h.services.postgres)
        .await
        .unwrap();

    sqlx::raw_sql("CREATE TABLE expiry_gate_seen (pid integer PRIMARY KEY); CREATE FUNCTION expiry_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO expiry_gate_seen (pid) VALUES (pg_backend_pid()) ON CONFLICT DO NOTHING; IF FOUND THEN PERFORM pg_advisory_xact_lock(hashtextextended(NEW.workspace_id::text, 0)); END IF; RETURN NEW; END $$; CREATE TRIGGER aaa_expiry_gate BEFORE UPDATE OF state ON deliveries FOR EACH ROW EXECUTE FUNCTION expiry_gate();")
        .execute(&h.services.postgres)
        .await
        .unwrap();
    let mut gate_connection = h.services.postgres.acquire().await.unwrap();
    for workspace_id in [h.workspace, other_workspace] {
        sqlx::query("SELECT pg_advisory_lock(hashtextextended($1::text, 0))")
            .bind(workspace_id.to_string())
            .execute(&mut *gate_connection)
            .await
            .unwrap();
    }

    let barrier = Arc::new(tokio::sync::Barrier::new(3));
    let first_events = h.events.clone();
    let first_barrier = barrier.clone();
    let first_worker = tokio::spawn(async move {
        first_barrier.wait().await;
        first_events.expire_deliveries(100).await
    });
    let second_events = h.events.clone();
    let second_barrier = barrier.clone();
    let second_worker = tokio::spawn(async move {
        second_barrier.wait().await;
        second_events.expire_deliveries(100).await
    });
    barrier.wait().await;

    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let gate_waiters: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND NOT granted",
            )
            .fetch_one(&h.services.postgres)
            .await
            .unwrap();
            let quota_waiters: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND wait_event_type = 'Lock' AND wait_event <> 'advisory'")
                .fetch_one(&h.services.postgres)
                .await
                .unwrap();
            if gate_waiters >= 2 || (gate_waiters >= 1 && quota_waiters >= 1) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both expiry transactions must reach the deterministic gate");
    for workspace_id in [h.workspace, other_workspace] {
        sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1::text, 0))")
            .bind(workspace_id.to_string())
            .execute(&mut *gate_connection)
            .await
            .unwrap();
    }

    let (first_result, second_result) = tokio::time::timeout(Duration::from_secs(5), async {
        (first_worker.await.unwrap(), second_worker.await.unwrap())
    })
    .await
    .expect("two expiry workers must finish without a lock cycle");
    assert_eq!(first_result.unwrap(), 100);
    assert_eq!(second_result.unwrap(), 100);

    let quota_a: (i64, i64) = sqlx::query_as(
        "SELECT outstanding_deliveries, outstanding_payload_bytes FROM workspace_quotas WHERE workspace_id = $1",
    )
    .bind(h.workspace)
    .fetch_one(&h.services.postgres)
    .await
    .unwrap();
    let quota_b: (i64, i64) = sqlx::query_as(
        "SELECT outstanding_deliveries, outstanding_payload_bytes FROM workspace_quotas WHERE workspace_id = $1",
    )
    .bind(other_workspace)
    .fetch_one(&h.services.postgres)
    .await
    .unwrap();
    assert_eq!(quota_a, (0, 0));
    assert_eq!(quota_b, (0, 0));
}

#[tokio::test]
async fn workers_recover_and_stop_without_broker_dependency() {
    let h = Harness::start().await;
    let mut stream = h.stream(Duration::from_secs(120)).await;
    h.events
        .accept_event(&h.principal, h.input(), "worker")
        .await
        .unwrap();
    let expired = h
        .events
        .accept_event(&h.principal, h.input(), "worker-expiry")
        .await
        .unwrap();
    sqlx::query("UPDATE events SET created_at = now() - interval '8 days', expires_at = now() - interval '1 day' WHERE id = $1").bind(expired.event_id).execute(&h.services.postgres).await.unwrap();
    h.broker("pause");
    let (stop, shutdown) = tokio::sync::watch::channel(false);
    let events = h.events.clone();
    let outbox = h.outbox.clone();
    let workers = tokio::spawn(async move { outbox.run_workers(events, shutdown).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        while h.quota().await != (1, 3) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(h.pending_outbox().await, 1);
    h.broker("unpause");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if h.quota().await == (1, 3) && h.pending_outbox().await == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    stop.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), workers)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stream.info().await.unwrap().state.messages, 1);
}

#[tokio::test]
async fn concurrent_same_key_returns_one_receipt() {
    let h = Harness::start().await;
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let events = h.events.clone();
        let principal = h.principal.clone();
        let input = h.input();
        tasks.spawn(async move {
            events
                .accept_event(&principal, input, "same-race")
                .await
                .unwrap()
        });
    }
    let first = tasks.join_next().await.unwrap().unwrap();
    while let Some(result) = tasks.join_next().await {
        assert_eq!(result.unwrap(), first);
    }
    assert_eq!(h.counts().await, (1, 1, 1));
    assert_eq!(h.quota().await, (1, 3));
}

#[tokio::test]
async fn failed_outbox_insert_rolls_back_acceptance_and_quota() {
    let h = Harness::start().await;
    h.events
        .accept_event(&h.principal, h.input(), "before")
        .await
        .unwrap();
    sqlx::raw_sql("CREATE FUNCTION fail_outbox_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'simulated write failure'; END $$; CREATE TRIGGER simulate_failure BEFORE INSERT ON event_outbox FOR EACH ROW EXECUTE FUNCTION fail_outbox_insert();")
        .execute(&h.services.postgres).await.unwrap();
    assert_eq!(
        status(
            h.events
                .accept_event(&h.principal, h.input(), "failure")
                .await
                .unwrap_err()
        ),
        500
    );
    assert_eq!(h.counts().await, (1, 1, 1));
    assert_eq!(h.quota().await, (1, 3));
    sqlx::query("DROP TRIGGER simulate_failure ON event_outbox")
        .execute(&h.services.postgres)
        .await
        .unwrap();
    h.events
        .accept_event(&h.principal, h.input(), "failure")
        .await
        .unwrap();
    assert_eq!(h.counts().await, (2, 2, 2));
    assert_eq!(h.quota().await, (2, 6));
}

#[tokio::test]
async fn dispatch_requires_puback_and_replicas_do_not_double_publish() {
    let h = Harness::start().await;
    for i in 0..12 {
        h.events
            .accept_event(&h.principal, h.input(), &format!("replica-{i}"))
            .await
            .unwrap();
    }
    // NATS is reachable but no stream accepts the subject: a Core NATS send is insufficient.
    assert!(h.outbox.dispatch_outbox(1).await.is_err());
    assert_eq!(h.pending_outbox().await, 12);
    let mut stream = h.stream(Duration::from_secs(1)).await;
    let second = Outbox::new(
        h.services.postgres.clone(),
        h.services.jetstream.clone(),
        &h.services.namespace,
    )
    .unwrap();
    let (a, b) = tokio::join!(h.outbox.dispatch_outbox(12), second.dispatch_outbox(12));
    assert_eq!(a.unwrap() + b.unwrap(), 12);
    assert_eq!(h.pending_outbox().await, 0);
    assert_eq!(stream.info().await.unwrap().state.messages, 12);
    assert_eq!(h.quota().await, (12, 36));
    assert!(
        Outbox::new(
            h.services.postgres.clone(),
            h.services.jetstream.clone(),
            "bad.*.namespace"
        )
        .is_err()
    );
}

#[tokio::test]
async fn agent_cannot_publish_and_database_rejects_cross_tenant_links() {
    let h = Harness::start().await;
    let agent = Principal::Agent {
        workspace_id: h.workspace,
        app_id: h.app,
        agent_id: Uuid::new_v4(),
        key_id: Uuid::new_v4(),
    };
    assert_eq!(
        status(
            h.events
                .accept_event(&agent, h.input(), "agent")
                .await
                .unwrap_err()
        ),
        403
    );
    assert_eq!(h.counts().await, (0, 0, 0));
    let (_, workspace, _, _) = tenant(&h.services.postgres).await;
    assert!(
        sqlx::query("INSERT INTO event_endpoints (id, workspace_id, app_id) VALUES ($1,$2,$3)")
            .bind(Uuid::new_v4())
            .bind(workspace)
            .bind(h.app)
            .execute(&h.services.postgres)
            .await
            .is_err()
    );
    let accepted = h
        .events
        .accept_event(&h.principal, h.input(), "valid")
        .await
        .unwrap();
    assert!(
        sqlx::query("UPDATE events SET workspace_id=$1 WHERE id=$2")
            .bind(workspace)
            .bind(accepted.event_id)
            .execute(&h.services.postgres)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE deliveries SET workspace_id=$1 WHERE id=$2")
            .bind(workspace)
            .bind(accepted.delivery_id)
            .execute(&h.services.postgres)
            .await
            .is_err()
    );
    h.events.migrate().await.unwrap();
    assert_eq!(h.quota().await, (1, 3));
}
