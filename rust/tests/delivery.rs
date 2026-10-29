#[allow(dead_code)]
mod support;

use axum::response::IntoResponse;
use futures_util::StreamExt;
use relayhub::{
    delivery::{Deliveries, DeliveryError, ProcessingResult},
    events::Events,
    gateway::Gateway,
    identity::Identity,
    outbox::Outbox,
    presence::Presence,
    protocol::{Accepted, EventInput, Frame, Principal},
};
use std::time::Duration;
use support::TestServices;
use uuid::Uuid;

struct Harness {
    services: TestServices,
    events: Events,
    delivery: Deliveries,
    presence: Presence,
    gateway: Gateway,
    app: Principal,
    agent: Principal,
    owner: Principal,
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
        sqlx::query("INSERT INTO identity_apps (id,workspace_id,name) VALUES ($1,$2,'delivery')")
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
        let presence = Presence::new(services.redis.clone(), &services.namespace).unwrap();
        let delivery = Deliveries::new(services.postgres.clone(), presence.clone());
        delivery.migrate().await.unwrap();
        let gateway = Gateway::new(
            Uuid::new_v4(),
            delivery.clone(),
            presence.clone(),
            services.jetstream.clone(),
        );
        Self {
            services,
            events,
            delivery,
            presence,
            gateway,
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
            owner: Principal::WorkspaceOwner {
                user_id: user,
                workspace_id: workspace,
            },
        }
    }
    async fn accept(&self) -> Accepted {
        self.events
            .accept_event(
                &self.app,
                EventInput {
                    endpoint_id: self.endpoint,
                    event_type: "test".into(),
                    method: "POST".into(),
                    path: "/hook".into(),
                    headers: vec![
                        ("x-test".into(), "one".into()),
                        ("x-test".into(), "two".into()),
                    ],
                    body: vec![0, 255, 1],
                },
                &Uuid::new_v4().to_string(),
            )
            .await
            .unwrap()
    }
    async fn state(&self, id: Uuid) -> (String, i32, bool) {
        sqlx::query_as(
            "SELECT state, attempt_count, terminal_at IS NOT NULL FROM deliveries WHERE id=$1",
        )
        .bind(id)
        .fetch_one(&self.services.postgres)
        .await
        .unwrap()
    }
    async fn quota(&self) -> (i64, i64) {
        sqlx::query_as(
            "SELECT outstanding_deliveries, outstanding_payload_bytes FROM workspace_quotas",
        )
        .fetch_one(&self.services.postgres)
        .await
        .unwrap()
    }
    async fn due(&self, id: Uuid) {
        sqlx::query("UPDATE deliveries SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE id=$1").bind(id).execute(&self.services.postgres).await.unwrap();
    }
    async fn expire_lease(&self, id: Uuid) {
        sqlx::query("UPDATE deliveries SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1").bind(id).execute(&self.services.postgres).await.unwrap();
    }
    async fn stream(&self) -> async_nats::jetstream::stream::Stream {
        self.services
            .jetstream
            .create_stream(async_nats::jetstream::stream::Config {
                name: self.services.namespace.clone(),
                subjects: vec![format!(
                    "{}.v1.workspaces.*.apps.*.deliveries",
                    self.services.namespace
                )],
                max_age: Duration::from_secs(604800),
                ..Default::default()
            })
            .await
            .unwrap()
    }
}
fn status(error: DeliveryError) -> u16 {
    error.into_response().status().as_u16()
}

#[tokio::test]
async fn offline_wait_does_not_consume_attempt() {
    let h = Harness::start().await;
    let a = h.accept().await;
    for _ in 0..7 {
        assert_eq!(
            status(
                h.delivery
                    .claim_delivery(&h.agent, a.delivery_id)
                    .await
                    .unwrap_err()
            ),
            503
        );
    }
    assert_eq!(h.state(a.delivery_id).await, ("pending".into(), 0, false));
    assert_eq!(h.quota().await, (1, 3));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM delivery_attempts")
            .fetch_one(&h.services.postgres)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn reconnect_receives_pending() {
    let h = Harness::start().await;
    let a = h.accept().await;
    let first = h.gateway.connect(&h.agent).await.unwrap();
    h.gateway.disconnect(&h.agent, first).await.unwrap();
    let second = Gateway::new(
        Uuid::new_v4(),
        h.delivery.clone(),
        h.presence.clone(),
        h.services.jetstream.clone(),
    );
    let session = second.connect(&h.agent).await.unwrap();
    let frames = second.pending(&h.agent, session, 16).await.unwrap();
    assert_eq!(frames.len(), 1);
    let Frame::Delivery { lease, input, .. } = &frames[0] else {
        panic!("delivery frame")
    };
    assert_eq!(
        (lease.delivery_id, lease.event_id, lease.attempt),
        (a.delivery_id, a.event_id, 1)
    );
    assert_eq!(input.body, vec![0, 255, 1]);
    assert_eq!(input.headers.len(), 2);
    assert_eq!(h.state(a.delivery_id).await, ("in_flight".into(), 1, false));
}

#[tokio::test]
async fn stale_ack_rejected() {
    let h = Harness::start().await;
    let a = h.accept().await;
    h.gateway.connect(&h.agent).await.unwrap();
    let old = h
        .delivery
        .claim_delivery(&h.agent, a.delivery_id)
        .await
        .unwrap();
    h.expire_lease(a.delivery_id).await;
    assert_eq!(
        status(
            h.delivery
                .settle_delivery(&h.agent, old.clone(), ProcessingResult::Success)
                .await
                .unwrap_err()
        ),
        409
    );
    assert_eq!(
        status(
            h.delivery
                .claim_delivery(&h.agent, a.delivery_id)
                .await
                .unwrap_err()
        ),
        409
    );
    h.due(a.delivery_id).await;
    let new = h
        .delivery
        .claim_delivery(&h.agent, a.delivery_id)
        .await
        .unwrap();
    assert_eq!(new.attempt, 2);
    assert_ne!(old.attempt_token, new.attempt_token);
    assert_eq!(
        status(
            h.delivery
                .settle_delivery(&h.agent, old, ProcessingResult::Success)
                .await
                .unwrap_err()
        ),
        409
    );
    assert_eq!(h.state(a.delivery_id).await, ("in_flight".into(), 2, false));
    h.delivery
        .settle_delivery(&h.agent, new, ProcessingResult::Success)
        .await
        .unwrap();
    assert_eq!(h.state(a.delivery_id).await, ("succeeded".into(), 2, true));
    assert_eq!(h.quota().await, (0, 0));
}

#[tokio::test]
async fn two_gateways_single_claim() {
    let h = Harness::start().await;
    let a = h.accept().await;
    h.gateway.connect(&h.agent).await.unwrap();
    let replica = Deliveries::new(h.services.postgres.clone(), h.presence.clone());
    let (x, y) = tokio::join!(
        h.delivery.claim_delivery(&h.agent, a.delivery_id),
        replica.claim_delivery(&h.agent, a.delivery_id)
    );
    assert_eq!(usize::from(x.is_ok()) + usize::from(y.is_ok()), 1);
    assert_eq!(status(x.err().or(y.err()).unwrap()), 409);
    assert_eq!(h.state(a.delivery_id).await, ("in_flight".into(), 1, false));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM delivery_attempts")
            .fetch_one(&h.services.postgres)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn redis_loss_preserves_event() {
    let h = Harness::start().await;
    let a = h.accept().await;
    h.gateway.connect(&h.agent).await.unwrap();
    redis::cmd("FLUSHDB")
        .query_async::<()>(&mut h.services.redis.clone())
        .await
        .unwrap();
    assert_eq!(
        status(
            h.delivery
                .claim_delivery(&h.agent, a.delivery_id)
                .await
                .unwrap_err()
        ),
        503
    );
    assert_eq!(h.state(a.delivery_id).await, ("pending".into(), 0, false));
    assert_eq!(h.quota().await, (1, 3));
    let session = h.gateway.connect(&h.agent).await.unwrap();
    assert_eq!(
        h.gateway
            .pending(&h.agent, session, 16)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM events")
            .fetch_one(&h.services.postgres)
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn retry_exhaustion_enters_dlq() {
    let h = Harness::start().await;
    let a = h.accept().await;
    h.gateway.connect(&h.agent).await.unwrap();
    for attempt in 1..=5 {
        let lease = h
            .delivery
            .claim_delivery(&h.agent, a.delivery_id)
            .await
            .unwrap();
        assert_eq!(lease.attempt, attempt);
        h.delivery
            .settle_delivery(&h.agent, lease, ProcessingResult::RetryableFailure)
            .await
            .unwrap();
        if attempt < 5 {
            assert_eq!(
                h.state(a.delivery_id).await,
                ("retry_wait".into(), i32::from(attempt), false)
            );
            let delay: i64 = sqlx::query_scalar("SELECT round(extract(epoch FROM d.next_attempt_at-a.finished_at))::bigint FROM deliveries d JOIN delivery_attempts a ON a.delivery_id=d.id AND a.attempt=d.attempt_count WHERE d.id=$1").bind(a.delivery_id).fetch_one(&h.services.postgres).await.unwrap();
            assert_eq!(delay, [1, 5, 30, 120][usize::from(attempt - 1)]);
            assert_eq!(
                status(
                    h.delivery
                        .claim_delivery(&h.agent, a.delivery_id)
                        .await
                        .unwrap_err()
                ),
                409
            );
            h.due(a.delivery_id).await;
        }
    }
    assert_eq!(h.state(a.delivery_id).await, ("dlq".into(), 5, true));
    assert_eq!(
        status(
            h.delivery
                .claim_delivery(&h.agent, a.delivery_id)
                .await
                .unwrap_err()
        ),
        410
    );
    assert_eq!(h.quota().await, (0, 0));
}

#[tokio::test]
async fn restart_after_db_settle_before_broker_ack() {
    let h = Harness::start().await;
    let stream = h.stream().await;
    let consumer = stream
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            durable_name: Some("delivery-test".into()),
            ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
            ack_wait: Duration::from_secs(1),
            ..Default::default()
        })
        .await
        .unwrap();
    let a = h.accept().await;
    Outbox::new(
        h.services.postgres.clone(),
        h.services.jetstream.clone(),
        &h.services.namespace,
    )
    .unwrap()
    .dispatch_outbox(1)
    .await
    .unwrap();
    let mut messages = consumer.messages().await.unwrap();
    let message = tokio::time::timeout(Duration::from_secs(5), messages.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    h.gateway.connect(&h.agent).await.unwrap();
    let lease = h
        .delivery
        .claim_delivery(&h.agent, a.delivery_id)
        .await
        .unwrap();
    h.delivery
        .settle_delivery(&h.agent, lease, ProcessingResult::Success)
        .await
        .unwrap();
    drop(message); // Crash window: database committed, no broker ACK.
    let restarted = Gateway::new(
        Uuid::new_v4(),
        h.delivery.clone(),
        h.presence.clone(),
        h.services.jetstream.clone(),
    );
    let duplicate = tokio::time::timeout(Duration::from_secs(5), messages.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(restarted.reconcile_message(&duplicate).await.unwrap());
    let info = h
        .services
        .jetstream
        .get_consumer_from_stream::<async_nats::jetstream::consumer::pull::Config, _, _>(
            "delivery-test",
            &h.services.namespace,
        )
        .await
        .unwrap()
        .cached_info()
        .clone();
    assert_eq!(info.num_ack_pending, 0);
    assert_eq!(h.state(a.delivery_id).await, ("succeeded".into(), 1, true));
    assert_eq!(h.quota().await, (0, 0));
}

#[tokio::test]
async fn lease_expiring_while_settle_waits_for_lock_is_rejected() {
    let h = Harness::start().await;
    let a = h.accept().await;
    h.gateway.connect(&h.agent).await.unwrap();
    let lease = h
        .delivery
        .claim_delivery(&h.agent, a.delivery_id)
        .await
        .unwrap();
    sqlx::query("UPDATE deliveries SET lease_expires_at=clock_timestamp()+interval '400 milliseconds' WHERE id=$1").bind(a.delivery_id).execute(&h.services.postgres).await.unwrap();
    let mut blocker = h.services.postgres.begin().await.unwrap();
    sqlx::query("SELECT id FROM deliveries WHERE id=$1 FOR UPDATE")
        .bind(a.delivery_id)
        .execute(&mut *blocker)
        .await
        .unwrap();
    let delivery = h.delivery.clone();
    let agent = h.agent.clone();
    let settle = tokio::spawn(async move {
        delivery
            .settle_delivery(&agent, lease, ProcessingResult::Success)
            .await
    });
    tokio::time::timeout(Duration::from_secs(3),async {
        loop {
            let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'SELECT d.%')").fetch_one(&h.services.postgres).await.unwrap();
            if waiting {break;}
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    blocker.commit().await.unwrap();
    assert_eq!(status(settle.await.unwrap().unwrap_err()), 409);
    assert_eq!(h.state(a.delivery_id).await, ("in_flight".into(), 1, false));
    assert_eq!(h.quota().await, (1, 3));
}

#[tokio::test]
async fn heartbeat_extends_sixty_seconds_but_never_beyond_five_minutes() {
    let h = Harness::start().await;
    let a = h.accept().await;
    let session = h.gateway.connect(&h.agent).await.unwrap();
    let lease = h
        .delivery
        .claim_delivery(&h.agent, a.delivery_id)
        .await
        .unwrap();
    let seconds:i64=sqlx::query_scalar("SELECT round(extract(epoch FROM lease_expires_at-attempt_started_at))::bigint FROM deliveries WHERE id=$1").bind(a.delivery_id).fetch_one(&h.services.postgres).await.unwrap();
    assert_eq!(seconds, 60);
    sqlx::query("UPDATE deliveries SET attempt_started_at=clock_timestamp()-interval '290 seconds',lease_expires_at=clock_timestamp()+interval '1 second' WHERE id=$1").bind(a.delivery_id).execute(&h.services.postgres).await.unwrap();
    h.gateway.heartbeat(&h.agent, session).await.unwrap();
    h.delivery.heartbeat(&h.agent, &lease).await.unwrap();
    let runtime:i64=sqlx::query_scalar("SELECT round(extract(epoch FROM lease_expires_at-attempt_started_at))::bigint FROM deliveries WHERE id=$1").bind(a.delivery_id).fetch_one(&h.services.postgres).await.unwrap();
    assert_eq!(runtime, 300);
    sqlx::query("UPDATE deliveries SET attempt_started_at=clock_timestamp()-interval '301 seconds' WHERE id=$1").bind(a.delivery_id).execute(&h.services.postgres).await.unwrap();
    assert_eq!(
        status(h.delivery.heartbeat(&h.agent, &lease).await.unwrap_err()),
        409
    );
    assert_eq!(
        status(
            h.delivery
                .settle_delivery(&h.agent, lease, ProcessingResult::Success)
                .await
                .unwrap_err()
        ),
        409
    );
    assert_eq!(h.state(a.delivery_id).await, ("in_flight".into(), 1, false));
}

#[tokio::test]
async fn concurrent_agent_claims_enforce_sixteen_in_flight() {
    let h = Harness::start().await;
    h.gateway.connect(&h.agent).await.unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..24 {
        let id = h.accept().await.delivery_id;
        let delivery = h.delivery.clone();
        let agent = h.agent.clone();
        tasks.spawn(async move { delivery.claim_delivery(&agent, id).await });
    }
    let mut leases = Vec::new();
    let mut rejected = 0;
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            Ok(lease) => leases.push(lease),
            Err(error) => {
                assert_eq!(status(error), 429);
                rejected += 1;
            }
        }
    }
    assert_eq!((leases.len(), rejected), (16, 8));
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM deliveries WHERE state='pending' AND attempt_count=0"
        )
        .fetch_one(&h.services.postgres)
        .await
        .unwrap(),
        8
    );
    h.delivery
        .settle_delivery(&h.agent, leases.pop().unwrap(), ProcessingResult::Success)
        .await
        .unwrap();
    let pending = h
        .delivery
        .read_delivery(&h.owner, h.accept().await.delivery_id)
        .await
        .unwrap();
    assert_eq!(
        h.delivery
            .claim_delivery(&h.agent, pending.delivery_id)
            .await
            .unwrap()
            .attempt,
        1
    );
}

#[tokio::test]
async fn replay_is_authorized_audited_new_generation_and_charged_once() {
    let h = Harness::start().await;
    let a = h.accept().await;
    h.gateway.connect(&h.agent).await.unwrap();
    let lease = h
        .delivery
        .claim_delivery(&h.agent, a.delivery_id)
        .await
        .unwrap();
    let stranger = Principal::WorkspaceOwner {
        user_id: Uuid::new_v4(),
        workspace_id: Uuid::new_v4(),
    };
    assert_eq!(
        status(
            h.delivery
                .read_delivery(&stranger, a.delivery_id)
                .await
                .unwrap_err()
        ),
        403
    );
    for principal in [&stranger, &h.app, &h.agent] {
        assert_eq!(
            status(
                h.delivery
                    .replay_delivery(principal, a.delivery_id)
                    .await
                    .unwrap_err()
            ),
            403
        );
    }
    assert_eq!(
        status(
            h.delivery
                .replay_delivery(&h.owner, a.delivery_id)
                .await
                .unwrap_err()
        ),
        409
    );
    h.delivery
        .settle_delivery(&h.agent, lease, ProcessingResult::Success)
        .await
        .unwrap();
    let (one, two) = tokio::join!(
        h.delivery.replay_delivery(&h.owner, a.delivery_id),
        h.delivery.replay_delivery(&h.owner, a.delivery_id)
    );
    assert_eq!(usize::from(one.is_ok()) + usize::from(two.is_ok()), 1);
    let replay = match (one, two) {
        (Ok(a), Err(e)) | (Err(e), Ok(a)) => {
            assert_eq!(status(e), 409);
            a
        }
        _ => panic!("single replay"),
    };
    assert_eq!((replay.event_id, replay.status), (a.event_id, 202));
    assert_ne!(replay.delivery_id, a.delivery_id);
    let view = h
        .delivery
        .read_delivery(&h.owner, replay.delivery_id)
        .await
        .unwrap();
    assert_eq!(
        (view.generation, view.state, view.attempts),
        (2, "pending".into(), 0)
    );
    assert_eq!(h.state(a.delivery_id).await, ("succeeded".into(), 1, true));
    assert_eq!(h.quota().await, (1, 3));
    let audit: (Uuid, Uuid) = sqlx::query_as(
        "SELECT source_delivery_id,actor_user_id FROM delivery_replays WHERE delivery_id=$1",
    )
    .bind(replay.delivery_id)
    .fetch_one(&h.services.postgres)
    .await
    .unwrap();
    let Principal::WorkspaceOwner { user_id, .. } = h.owner else {
        panic!()
    };
    assert_eq!(audit, (a.delivery_id, user_id));
    let outbox:i64=sqlx::query_scalar("SELECT count(*) FROM event_outbox WHERE delivery_id=$1 AND sent_at IS NULL AND discarded_at IS NULL").bind(replay.delivery_id).fetch_one(&h.services.postgres).await.unwrap();
    assert_eq!(outbox, 1);
}

#[tokio::test]
async fn forged_scope_and_lease_fields_never_settle_work() {
    let h = Harness::start().await;
    let a = h.accept().await;
    h.gateway.connect(&h.agent).await.unwrap();
    let lease = h
        .delivery
        .claim_delivery(&h.agent, a.delivery_id)
        .await
        .unwrap();
    let mut stranger = h.agent.clone();
    if let Principal::Agent { workspace_id, .. } = &mut stranger {
        *workspace_id = Uuid::new_v4();
    }
    assert_eq!(
        status(
            h.delivery
                .claim_delivery(&stranger, a.delivery_id)
                .await
                .unwrap_err()
        ),
        403
    );
    assert_eq!(
        status(
            h.delivery
                .settle_delivery(&stranger, lease.clone(), ProcessingResult::Success)
                .await
                .unwrap_err()
        ),
        403
    );
    assert_eq!(
        status(
            h.delivery
                .claim_delivery(&h.app, a.delivery_id)
                .await
                .unwrap_err()
        ),
        403
    );
    for field in 0..4 {
        let mut forged = lease.clone();
        match field {
            0 => forged.event_id = Uuid::new_v4(),
            1 => forged.attempt_token = Uuid::new_v4(),
            2 => forged.attempt += 1,
            _ => forged.generation += 1,
        };
        assert_eq!(
            status(
                h.delivery
                    .settle_delivery(&h.agent, forged, ProcessingResult::Success)
                    .await
                    .unwrap_err()
            ),
            409
        );
    }
    let mut other_agent = h.agent.clone();
    if let Principal::Agent { agent_id, .. } = &mut other_agent {
        *agent_id = Uuid::new_v4();
    }
    assert_eq!(
        status(
            h.delivery
                .settle_delivery(&other_agent, lease, ProcessingResult::Success)
                .await
                .unwrap_err()
        ),
        409
    );
    assert_eq!(h.state(a.delivery_id).await, ("in_flight".into(), 1, false));
    assert_eq!(h.quota().await, (1, 3));
}

#[tokio::test]
async fn expired_retention_is_not_dlq_and_cannot_be_replayed() {
    let h = Harness::start().await;
    let a = h.accept().await;
    sqlx::query("UPDATE events SET created_at=clock_timestamp()-interval '8 days',expires_at=clock_timestamp()-interval '1 day' WHERE id=$1").bind(a.event_id).execute(&h.services.postgres).await.unwrap();
    assert_eq!(
        status(
            h.delivery
                .claim_delivery(&h.agent, a.delivery_id)
                .await
                .unwrap_err()
        ),
        410
    );
    assert_eq!(h.state(a.delivery_id).await, ("expired".into(), 0, true));
    assert_eq!(
        status(
            h.delivery
                .replay_delivery(&h.owner, a.delivery_id)
                .await
                .unwrap_err()
        ),
        410
    );
    assert_eq!(h.quota().await, (0, 0));
}

#[tokio::test]
async fn ownership_fenced_expiring_and_broker_routes_only_to_current_gateway() {
    let h = Harness::start().await;
    let a = h.accept().await;
    let stream = h.stream().await;
    let consumer = stream
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await
        .unwrap();
    let old = h.gateway.connect(&h.agent).await.unwrap();
    let other = Gateway::new(
        Uuid::new_v4(),
        h.delivery.clone(),
        h.presence.clone(),
        h.services.jetstream.clone(),
    );
    let current = other.connect(&h.agent).await.unwrap();
    h.gateway.disconnect(&h.agent, old).await.unwrap();
    assert_eq!(
        status(h.gateway.heartbeat(&h.agent, old).await.unwrap_err()),
        409
    );
    assert_eq!(
        status(h.gateway.pending(&h.agent, old, 16).await.unwrap_err()),
        409
    );
    other.heartbeat(&h.agent, current).await.unwrap();
    let keys: Vec<String> = redis::cmd("KEYS")
        .arg(format!("{}:v1:presence:*", h.services.namespace))
        .query_async(&mut h.services.redis.clone())
        .await
        .unwrap();
    for key in &keys {
        let ttl: i64 = redis::cmd("TTL")
            .arg(key)
            .query_async(&mut h.services.redis.clone())
            .await
            .unwrap();
        assert!((59..=60).contains(&ttl));
    }
    let mut notices = other.notifications().await.unwrap();
    let mut old_notices = h.gateway.notifications().await.unwrap();
    Outbox::new(
        h.services.postgres.clone(),
        h.services.jetstream.clone(),
        &h.services.namespace,
    )
    .unwrap()
    .dispatch_outbox(1)
    .await
    .unwrap();
    let mut messages = consumer.messages().await.unwrap();
    let message = tokio::time::timeout(Duration::from_secs(5), messages.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(!h.gateway.reconcile_message(&message).await.unwrap());
    let notice = tokio::time::timeout(Duration::from_secs(3), notices.next())
        .await
        .unwrap()
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&notice.payload).unwrap();
    assert_eq!(value["delivery_id"], a.delivery_id.to_string());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), old_notices.next())
            .await
            .is_err()
    );
    assert_eq!(h.state(a.delivery_id).await, ("pending".into(), 0, false));
    assert_eq!(other.pending(&h.agent, current, 16).await.unwrap().len(), 1);
    for key in keys {
        redis::cmd("PEXPIRE")
            .arg(key)
            .arg(1)
            .query_async::<()>(&mut h.services.redis.clone())
            .await
            .unwrap();
    }
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(h.presence.owner(&h.agent).await.unwrap().is_none());
    assert_eq!(h.state(a.delivery_id).await, ("in_flight".into(), 1, false));
}

#[tokio::test]
async fn lease_expiring_while_heartbeat_waits_for_lock_is_rejected() {
    let h = Harness::start().await;
    let a = h.accept().await;
    h.gateway.connect(&h.agent).await.unwrap();
    let lease = h
        .delivery
        .claim_delivery(&h.agent, a.delivery_id)
        .await
        .unwrap();
    sqlx::query("UPDATE deliveries SET lease_expires_at=clock_timestamp()+interval '400 milliseconds' WHERE id=$1").bind(a.delivery_id).execute(&h.services.postgres).await.unwrap();
    let mut blocker = h.services.postgres.begin().await.unwrap();
    sqlx::query("SELECT id FROM deliveries WHERE id=$1 FOR UPDATE")
        .bind(a.delivery_id)
        .execute(&mut *blocker)
        .await
        .unwrap();
    let delivery = h.delivery.clone();
    let agent = h.agent.clone();
    let settle = tokio::spawn(async move { delivery.heartbeat(&agent, &lease).await });
    tokio::time::timeout(Duration::from_secs(3),async {
        loop {
            let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'SELECT d.%')").fetch_one(&h.services.postgres).await.unwrap();
            if waiting {break;}
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    blocker.commit().await.unwrap();
    assert_eq!(status(settle.await.unwrap().unwrap_err()), 409);
    assert_eq!(h.state(a.delivery_id).await, ("in_flight".into(), 1, false));
    assert_eq!(h.quota().await, (1, 3));
}

#[tokio::test]
async fn offline_expired_fifth_attempt_is_reconciled_to_dlq_before_ack() {
    let h = Harness::start().await;
    let stream = h.stream().await;
    let consumer = stream
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await
        .unwrap();
    let a = h.accept().await;
    Outbox::new(
        h.services.postgres.clone(),
        h.services.jetstream.clone(),
        &h.services.namespace,
    )
    .unwrap()
    .dispatch_outbox(1)
    .await
    .unwrap();
    let session = h.gateway.connect(&h.agent).await.unwrap();
    for _ in 0..4 {
        let lease = h
            .delivery
            .claim_delivery(&h.agent, a.delivery_id)
            .await
            .unwrap();
        h.delivery
            .settle_delivery(&h.agent, lease, ProcessingResult::RetryableFailure)
            .await
            .unwrap();
        h.due(a.delivery_id).await;
    }
    assert_eq!(
        h.delivery
            .claim_delivery(&h.agent, a.delivery_id)
            .await
            .unwrap()
            .attempt,
        5
    );
    h.gateway.disconnect(&h.agent, session).await.unwrap();
    h.expire_lease(a.delivery_id).await;
    let mut messages = consumer.messages().await.unwrap();
    let message = tokio::time::timeout(Duration::from_secs(5), messages.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(h.gateway.reconcile_message(&message).await.unwrap());
    assert_eq!(h.state(a.delivery_id).await, ("dlq".into(), 5, true));
    assert_eq!(h.quota().await, (0, 0));
    let outcome: String = sqlx::query_scalar(
        "SELECT outcome FROM delivery_attempts WHERE delivery_id=$1 AND attempt=5",
    )
    .bind(a.delivery_id)
    .fetch_one(&h.services.postgres)
    .await
    .unwrap();
    assert_eq!(outcome, "lease_expired");
}

#[tokio::test]
async fn redis_outage_is_bounded_and_settlement_needs_only_postgres() {
    let h = Harness::start().await;
    let a = h.accept().await;
    let b = h.accept().await;
    h.gateway.connect(&h.agent).await.unwrap();
    let lease = h
        .delivery
        .claim_delivery(&h.agent, a.delivery_id)
        .await
        .unwrap();
    let docker = |action: &str| {
        assert!(
            std::process::Command::new("docker")
                .args([
                    "compose",
                    "--env-file",
                    "/dev/null",
                    "-f",
                    concat!(env!("CARGO_MANIFEST_DIR"), "/tests/compose.yaml"),
                    "-p",
                    &h.services.namespace,
                    action,
                    "redis"
                ])
                .output()
                .unwrap()
                .status
                .success()
        );
    };
    docker("pause");
    let settle = h
        .delivery
        .settle_delivery(&h.agent, lease, ProcessingResult::Success)
        .await;
    let claim = tokio::time::timeout(
        Duration::from_secs(3),
        h.delivery.claim_delivery(&h.agent, b.delivery_id),
    )
    .await;
    docker("unpause");
    settle.unwrap();
    assert_eq!(
        status(
            claim
                .expect("Redis failure must return in bounded time")
                .unwrap_err()
        ),
        503
    );
    assert_eq!(h.state(a.delivery_id).await, ("succeeded".into(), 1, true));
    assert_eq!(h.state(b.delivery_id).await, ("pending".into(), 0, false));
    assert_eq!(h.quota().await, (1, 3));
}

#[tokio::test]
async fn websocket_hello_delivery_ack_and_revocation_use_authenticated_agent() {
    use futures_util::SinkExt;
    use sha2::{Digest, Sha256};
    use tokio_tungstenite::{
        connect_async,
        tungstenite::{Message, client::IntoClientRequest},
    };
    let h = Harness::start().await;
    let a = h.accept().await;
    let Principal::Agent {
        workspace_id,
        app_id,
        agent_id,
        key_id,
    } = h.agent
    else {
        panic!()
    };
    let token = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO identity_credentials(id,workspace_id,app_id,agent_id,kind,token_hash) VALUES($1,$2,$3,$4,'agent',$5)").bind(key_id).bind(workspace_id).bind(app_id).bind(agent_id).bind(Sha256::digest(token.as_bytes()).to_vec()).execute(&h.services.postgres).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = h.gateway.router(Identity::new(h.services.postgres.clone()));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut request = format!("ws://{address}/api/v1/socket")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
        .headers_mut()
        .insert("sec-websocket-protocol", "relayhub.v1".parse().unwrap());
    let (mut socket, _) = connect_async(request.clone()).await.unwrap();
    let hello = Frame::Hello {
        version: 1,
        request_id: Uuid::new_v4(),
        agent_id,
        max_in_flight: 1,
    };
    socket
        .send(Message::Text(serde_json::to_string(&hello).unwrap().into()))
        .await
        .unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(5), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let Frame::Delivery {
        request_id,
        lease,
        input,
        ..
    } = relayhub::protocol::parse_frame(&frame.into_data()).unwrap()
    else {
        panic!()
    };
    assert_eq!(
        (lease.delivery_id, lease.event_id, lease.attempt),
        (a.delivery_id, a.event_id, 1)
    );
    assert_eq!(input.body, vec![0, 255, 1]);
    let heartbeat = Frame::Heartbeat {
        version: 1,
        request_id: Uuid::new_v4(),
        lease: Some(lease.clone()),
    };
    socket
        .send(Message::Text(
            serde_json::to_string(&heartbeat).unwrap().into(),
        ))
        .await
        .unwrap();
    let ack = Frame::Ack {
        version: 1,
        request_id,
        event_id: lease.event_id,
        delivery_id: lease.delivery_id,
        attempt_token: lease.attempt_token,
    };
    socket
        .send(Message::Text(serde_json::to_string(&ack).unwrap().into()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while h.state(a.delivery_id).await.0 != "succeeded" {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    sqlx::query("UPDATE identity_credentials SET revoked_at=now() WHERE id=$1")
        .bind(key_id)
        .execute(&h.services.postgres)
        .await
        .unwrap();
    let close = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(close,Message::Close(Some(frame)) if u16::from(frame.code)==1008));
    assert_eq!(h.quota().await, (0, 0));
    assert!(connect_async(request).await.is_err());
    server.abort();
}

#[tokio::test]
async fn replay_rolls_back_audit_outbox_and_quota_on_failure() {
    let h = Harness::start().await;
    let a = h.accept().await;
    h.gateway.connect(&h.agent).await.unwrap();
    let lease = h
        .delivery
        .claim_delivery(&h.agent, a.delivery_id)
        .await
        .unwrap();
    h.delivery
        .settle_delivery(&h.agent, lease, ProcessingResult::Success)
        .await
        .unwrap();
    sqlx::raw_sql("CREATE FUNCTION reject_replay_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'audit unavailable'; END $$; CREATE TRIGGER reject_replay_audit BEFORE INSERT ON delivery_replays FOR EACH ROW EXECUTE FUNCTION reject_replay_audit();").execute(&h.services.postgres).await.unwrap();
    assert_eq!(
        status(
            h.delivery
                .replay_delivery(&h.owner, a.delivery_id)
                .await
                .unwrap_err()
        ),
        500
    );
    let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM deliveries),(SELECT count(*) FROM event_outbox),(SELECT count(*) FROM delivery_replays)").fetch_one(&h.services.postgres).await.unwrap();
    assert_eq!(counts, (1, 1, 0));
    assert_eq!(h.quota().await, (0, 0));
    assert_eq!(h.state(a.delivery_id).await, ("succeeded".into(), 1, true));
    sqlx::query("DROP TRIGGER reject_replay_audit ON delivery_replays")
        .execute(&h.services.postgres)
        .await
        .unwrap();
    sqlx::query("UPDATE workspace_quotas SET outstanding_deliveries=10000")
        .execute(&h.services.postgres)
        .await
        .unwrap();
    assert_eq!(
        status(
            h.delivery
                .replay_delivery(&h.owner, a.delivery_id)
                .await
                .unwrap_err()
        ),
        429
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM deliveries")
            .fetch_one(&h.services.postgres)
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM delivery_replays")
            .fetch_one(&h.services.postgres)
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn two_gateway_sessions_race_without_double_delivery() {
    let h = Harness::start().await;
    let a = h.accept().await;
    let other = Gateway::new(
        Uuid::new_v4(),
        h.delivery.clone(),
        h.presence.clone(),
        h.services.jetstream.clone(),
    );
    let mut other_agent = h.agent.clone();
    if let Principal::Agent { agent_id, .. } = &mut other_agent {
        *agent_id = Uuid::new_v4();
    }
    let first = h.gateway.connect(&h.agent).await.unwrap();
    let second = other.connect(&other_agent).await.unwrap();
    let (one, two) = tokio::join!(
        h.gateway.pending(&h.agent, first, 16),
        other.pending(&other_agent, second, 16)
    );
    assert_eq!(one.unwrap().len() + two.unwrap().len(), 1);
    assert_eq!(h.state(a.delivery_id).await, ("in_flight".into(), 1, false));
    assert_eq!(h.quota().await, (1, 3));
}

#[tokio::test]
async fn wrong_broker_scope_never_routes_or_settles_delivery() {
    let h = Harness::start().await;
    let a = h.accept().await;
    let stream = h.stream().await;
    let consumer = stream
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await
        .unwrap();
    h.services
        .jetstream
        .publish(
            format!(
                "{}.v1.workspaces.{}.apps.{}.deliveries",
                h.services.namespace,
                Uuid::new_v4(),
                Uuid::new_v4()
            ),
            serde_json::json!({"delivery_id":a.delivery_id,"event_id":a.event_id})
                .to_string()
                .into(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    let mut messages = consumer.messages().await.unwrap();
    let message = tokio::time::timeout(Duration::from_secs(5), messages.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        status(h.gateway.reconcile_message(&message).await.unwrap_err()),
        403
    );
    assert_eq!(h.state(a.delivery_id).await, ("pending".into(), 0, false));
    assert_eq!(h.quota().await, (1, 3));
}

#[tokio::test]
async fn replaced_gateway_cannot_emit_after_claim_wait() {
    let h = Harness::start().await;
    let accepted = h.accept().await;
    let old_session = h.gateway.connect(&h.agent).await.unwrap();

    let mut blocker = h.services.postgres.begin().await.unwrap();
    sqlx::query("SELECT id FROM deliveries WHERE id=$1 FOR UPDATE")
        .bind(accepted.delivery_id)
        .execute(&mut *blocker)
        .await
        .unwrap();

    let old_gateway = h.gateway.clone();
    let old_agent = h.agent.clone();
    let old_pending =
        tokio::spawn(async move { old_gateway.pending(&old_agent, old_session, 1).await });
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let blocked: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%FOR UPDATE OF d%')",
            )
            .fetch_one(&h.services.postgres)
            .await
            .unwrap();
            if blocked {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("old gateway claim must be blocked on the delivery row");

    let replacement = Gateway::new(
        Uuid::new_v4(),
        h.delivery.clone(),
        h.presence.clone(),
        h.services.jetstream.clone(),
    );
    let replacement_session = replacement.connect(&h.agent).await.unwrap();
    blocker.commit().await.unwrap();

    let old_result = tokio::time::timeout(Duration::from_secs(3), old_pending)
        .await
        .expect("blocked claim must finish after the row unlock")
        .unwrap();
    assert!(
        old_result.is_err(),
        "replaced gateway must not receive a lease"
    );
    assert_eq!(
        h.state(accepted.delivery_id).await,
        ("pending".into(), 0, false)
    );

    let replacement_frames = replacement
        .pending(&h.agent, replacement_session, 1)
        .await
        .unwrap();
    assert_eq!(replacement_frames.len(), 1);
    assert_eq!(
        h.state(accepted.delivery_id).await,
        ("in_flight".into(), 1, false)
    );
}

#[tokio::test]
async fn revocation_closes_socket_even_before_hello() {
    use sha2::{Digest, Sha256};
    use tokio_tungstenite::{
        connect_async,
        tungstenite::{Message, client::IntoClientRequest},
    };
    let h = Harness::start().await;
    let Principal::Agent {
        workspace_id,
        app_id,
        agent_id,
        key_id,
    } = h.agent
    else {
        panic!()
    };
    let token = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO identity_credentials(id,workspace_id,app_id,agent_id,kind,token_hash) VALUES($1,$2,$3,$4,'agent',$5)").bind(key_id).bind(workspace_id).bind(app_id).bind(agent_id).bind(Sha256::digest(token.as_bytes()).to_vec()).execute(&h.services.postgres).await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = h.gateway.router(Identity::new(h.services.postgres.clone()));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let mut request = format!("ws://{address}/api/v1/socket")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
        .headers_mut()
        .insert("sec-websocket-protocol", "relayhub.v1".parse().unwrap());
    let (mut socket, _) = connect_async(request.clone()).await.unwrap();
    sqlx::query("UPDATE identity_credentials SET revoked_at=now() WHERE id=$1")
        .bind(key_id)
        .execute(&h.services.postgres)
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), socket.next()).await;
    server.abort();
    let close = result
        .expect("revocation must be checked while awaiting Hello")
        .unwrap()
        .unwrap();
    assert!(matches!(close,Message::Close(Some(frame)) if u16::from(frame.code)==1008));
    assert!(h.presence.owner(&h.agent).await.unwrap().is_none());
}

#[tokio::test]
async fn replay_waiting_for_event_lock_rechecks_retention() {
    let h = Harness::start().await;
    let a = h.accept().await;
    h.gateway.connect(&h.agent).await.unwrap();
    let lease = h
        .delivery
        .claim_delivery(&h.agent, a.delivery_id)
        .await
        .unwrap();
    h.delivery
        .settle_delivery(&h.agent, lease, ProcessingResult::Success)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE events SET expires_at=clock_timestamp()+interval '400 milliseconds' WHERE id=$1",
    )
    .bind(a.event_id)
    .execute(&h.services.postgres)
    .await
    .unwrap();
    let mut blocker = h.services.postgres.begin().await.unwrap();
    sqlx::query("SELECT id FROM events WHERE id=$1 FOR NO KEY UPDATE")
        .bind(a.event_id)
        .execute(&mut *blocker)
        .await
        .unwrap();
    let delivery = h.delivery.clone();
    let owner = h.owner.clone();
    let replay = tokio::spawn(async move { delivery.replay_delivery(&owner, a.delivery_id).await });
    tokio::time::timeout(Duration::from_secs(3),async {
        loop {
            let waiting:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'SELECT expires_at%')").fetch_one(&h.services.postgres).await.unwrap();
            if waiting {break;}
            tokio::task::yield_now().await;
        }
    }).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    blocker.commit().await.unwrap();
    assert_eq!(status(replay.await.unwrap().unwrap_err()), 410);
    assert_eq!(h.quota().await, (0, 0));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM delivery_replays")
            .fetch_one(&h.services.postgres)
            .await
            .unwrap(),
        0
    );
}

type AgentSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

impl Harness {
    async fn socket(
        &self,
        max_in_flight: Option<u16>,
    ) -> (AgentSocket, tokio::task::JoinHandle<()>) {
        use sha2::{Digest, Sha256};
        use tokio_tungstenite::tungstenite::client::IntoClientRequest;
        let Principal::Agent {
            workspace_id,
            app_id,
            agent_id,
            key_id,
        } = self.agent
        else {
            panic!()
        };
        let token = Uuid::new_v4().to_string();
        sqlx::query("INSERT INTO identity_credentials(id,workspace_id,app_id,agent_id,kind,token_hash) VALUES($1,$2,$3,$4,'agent',$5)")
            .bind(key_id).bind(workspace_id).bind(app_id).bind(agent_id)
            .bind(Sha256::digest(token.as_bytes()).to_vec()).execute(&self.services.postgres).await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let router = self
            .gateway
            .router(Identity::new(self.services.postgres.clone()));
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let mut request = format!("ws://{address}/api/v1/socket")
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        request
            .headers_mut()
            .insert("sec-websocket-protocol", "relayhub.v1".parse().unwrap());
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        if let Some(max_in_flight) = max_in_flight {
            send_frame(
                &mut socket,
                Frame::Hello {
                    version: 1,
                    request_id: Uuid::new_v4(),
                    agent_id,
                    max_in_flight,
                },
            )
            .await;
        }
        (socket, server)
    }

    async fn responses(&self) -> relayhub::responses::Responses {
        relayhub::webhooks::Webhooks::new(
            self.services.postgres.clone(),
            self.presence.clone(),
            self.services.redis.clone(),
            &self.services.namespace,
        )
        .unwrap()
        .migrate()
        .await
        .unwrap();
        relayhub::responses::Responses::new(
            self.services.postgres.clone(),
            self.delivery.clone(),
            self.services.redis.clone(),
            &self.services.namespace,
        )
        .unwrap()
    }

    async fn wait_for_claim(&self) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let blocked: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE '%FOR UPDATE OF d%')")
                    .fetch_one(&self.services.postgres).await.unwrap();
                if blocked { break; }
                tokio::task::yield_now().await;
            }
        }).await.expect("claim must reach the locked delivery row");
    }
}

async fn send_frame(socket: &mut AgentSocket, frame: Frame) {
    use futures_util::SinkExt;
    socket
        .send(tokio_tungstenite::tungstenite::Message::Text(
            serde_json::to_string(&frame).unwrap().into(),
        ))
        .await
        .unwrap();
}

async fn next_message(socket: &mut AgentSocket) -> tokio_tungstenite::tungstenite::Message {
    tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .expect("socket must respond")
        .unwrap()
        .unwrap()
}

async fn expect_drain(socket: &mut AgentSocket, deadline_ms: u32) {
    let frame = relayhub::protocol::parse_frame(&next_message(socket).await.into_data()).unwrap();
    assert!(
        matches!(frame, Frame::Drain { version: 1, request_id, deadline_ms: actual } if actual == deadline_ms && !request_id.is_nil()),
        "expected drain, got {frame:?}"
    );
}

async fn expect_close(socket: &mut AgentSocket, code: u16) {
    let message = next_message(socket).await;
    assert!(
        matches!(&message, tokio_tungstenite::tungstenite::Message::Close(Some(frame)) if u16::from(frame.code) == code),
        "no delivery may follow drain: {message:?}"
    );
}

async fn next_delivery(socket: &mut AgentSocket) -> (Uuid, relayhub::protocol::DeliveryLease) {
    let Frame::Delivery {
        request_id, lease, ..
    } = relayhub::protocol::parse_frame(&next_message(socket).await.into_data()).unwrap()
    else {
        panic!("expected delivery")
    };
    (request_id, lease)
}

fn ack(request_id: Uuid, lease: &relayhub::protocol::DeliveryLease) -> Frame {
    Frame::Ack {
        version: 1,
        request_id,
        event_id: lease.event_id,
        delivery_id: lease.delivery_id,
        attempt_token: lease.attempt_token,
    }
}

#[tokio::test]
async fn drain_stops_pending_across_clones_and_validates_deadline() {
    let h = Harness::start().await;
    let accepted = h.accept().await;
    let session = h.gateway.connect(&h.agent).await.unwrap();
    for deadline in [
        Duration::ZERO,
        Duration::from_nanos(1),
        Duration::from_secs(31),
    ] {
        assert_eq!(status(h.gateway.drain(deadline).unwrap_err()), 422);
    }
    h.gateway.clone().drain(Duration::from_secs(30)).unwrap();
    assert!(
        h.gateway
            .pending(&h.agent, session, 16)
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        h.state(accepted.delivery_id).await,
        ("pending".into(), 0, false)
    );
}

#[tokio::test]
async fn drain_rolls_back_claim_racing_with_shutdown() {
    let h = Harness::start().await;
    let accepted = h.accept().await;
    let session = h.gateway.connect(&h.agent).await.unwrap();
    let mut blocker = h.services.postgres.begin().await.unwrap();
    sqlx::query("SELECT id FROM deliveries WHERE id=$1 FOR UPDATE")
        .bind(accepted.delivery_id)
        .execute(&mut *blocker)
        .await
        .unwrap();
    let gateway = h.gateway.clone();
    let agent = h.agent.clone();
    let pending = tokio::spawn(async move { gateway.pending(&agent, session, 1).await });
    h.wait_for_claim().await;
    h.gateway.drain(Duration::from_secs(1)).unwrap();
    blocker.commit().await.unwrap();
    assert!(pending.await.unwrap().unwrap().is_empty());
    assert_eq!(
        h.state(accepted.delivery_id).await,
        ("pending".into(), 0, false)
    );
    let attempts: i64 = sqlx::query_scalar("SELECT count(*) FROM delivery_attempts")
        .fetch_one(&h.services.postgres)
        .await
        .unwrap();
    assert_eq!(attempts, 0);
}

#[tokio::test]
async fn drain_emits_no_delivery_from_a_racing_socket_claim() {
    let h = Harness::start().await;
    let first = h.accept().await;
    let accepted = h.accept().await;
    let mut blocker = h.services.postgres.begin().await.unwrap();
    sqlx::query("SELECT id FROM deliveries WHERE id=$1 FOR UPDATE")
        .bind(accepted.delivery_id)
        .execute(&mut *blocker)
        .await
        .unwrap();
    let (mut socket, server) = h.socket(Some(2)).await;
    h.wait_for_claim().await;
    assert_eq!(
        h.state(first.delivery_id).await,
        ("in_flight".into(), 1, false)
    );
    h.gateway.drain(Duration::from_secs(1)).unwrap();
    expect_drain(&mut socket, 1000).await;
    expect_close(&mut socket, 1000).await;
    blocker.commit().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while h.state(first.delivery_id).await.1 != 0 || h.state(accepted.delivery_id).await.1 != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("all un-emitted claims must roll back after the lock releases");
    assert_eq!(
        h.state(first.delivery_id).await,
        ("pending".into(), 0, false)
    );
    assert_eq!(
        h.state(accepted.delivery_id).await,
        ("pending".into(), 0, false)
    );
    server.abort();
}

#[tokio::test]
async fn drain_keeps_heartbeat_and_ack_for_outstanding_lease() {
    let h = Harness::start().await;
    let accepted = h.accept().await;
    let (mut socket, server) = h.socket(Some(1)).await;
    let (request_id, lease) = next_delivery(&mut socket).await;
    let queued = h.accept().await;
    h.gateway.drain(Duration::from_secs(5)).unwrap();
    expect_drain(&mut socket, 5000).await;
    sqlx::query("UPDATE deliveries SET lease_expires_at=clock_timestamp()+interval '10 seconds' WHERE id=$1").bind(accepted.delivery_id).execute(&h.services.postgres).await.unwrap();
    send_frame(
        &mut socket,
        Frame::Heartbeat {
            version: 1,
            request_id: Uuid::new_v4(),
            lease: Some(lease.clone()),
        },
    )
    .await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let renewed: bool = sqlx::query_scalar("SELECT lease_expires_at>clock_timestamp()+interval '20 seconds' FROM deliveries WHERE id=$1").bind(accepted.delivery_id).fetch_one(&h.services.postgres).await.unwrap();
            if renewed { break; }
            tokio::task::yield_now().await;
        }
    }).await.expect("draining sessions must renew current leases");
    send_frame(&mut socket, ack(request_id, &lease)).await;
    expect_close(&mut socket, 1000).await;
    assert_eq!(
        h.state(accepted.delivery_id).await,
        ("succeeded".into(), 1, true)
    );
    assert_eq!(
        h.state(queued.delivery_id).await,
        ("pending".into(), 0, false)
    );
    server.abort();
}

#[tokio::test]
async fn drain_allows_response_and_rejects_stale_attempts() {
    let mut h = Harness::start().await;
    let responses = h.responses().await;
    h.gateway = h.gateway.with_responses(responses.clone());
    let accepted = h.accept().await;
    h.gateway.connect(&h.agent).await.unwrap();
    let old = h
        .delivery
        .claim_delivery(&h.agent, accepted.delivery_id)
        .await
        .unwrap();
    h.delivery
        .settle_delivery(&h.agent, old.clone(), ProcessingResult::RetryableFailure)
        .await
        .unwrap();
    h.due(accepted.delivery_id).await;
    let (mut socket, server) = h.socket(Some(1)).await;
    let (request_id, lease) = next_delivery(&mut socket).await;
    assert_ne!(old.attempt_token, lease.attempt_token);
    h.gateway.drain(Duration::from_secs(5)).unwrap();
    expect_drain(&mut socket, 5000).await;
    assert_eq!(
        status(
            h.delivery
                .settle_delivery(&h.agent, old.clone(), ProcessingResult::Success)
                .await
                .unwrap_err()
        ),
        409
    );
    let response = relayhub::protocol::HttpResponse {
        status: 201,
        headers: vec![],
        body: b"done".to_vec(),
    };
    assert_eq!(
        responses
            .complete_response(&h.agent, old, response.clone())
            .await
            .unwrap_err()
            .into_response()
            .status()
            .as_u16(),
        409
    );
    send_frame(
        &mut socket,
        Frame::Response {
            version: 1,
            request_id,
            event_id: lease.event_id,
            delivery_id: lease.delivery_id,
            attempt_token: lease.attempt_token,
            response: response.clone(),
        },
    )
    .await;
    expect_close(&mut socket, 1000).await;
    assert_eq!(
        h.state(accepted.delivery_id).await,
        ("succeeded".into(), 2, true)
    );
    let stored: Vec<u8> =
        sqlx::query_scalar("SELECT result FROM webhook_response_candidates WHERE attempt_token=$1")
            .bind(lease.attempt_token)
            .fetch_one(&h.services.postgres)
            .await
            .unwrap();
    assert_eq!(
        serde_json::from_slice::<relayhub::protocol::HttpResponse>(&stored).unwrap(),
        response
    );
    server.abort();
}

#[tokio::test]
async fn drain_deadline_closes_even_when_ack_is_blocked() {
    let h = Harness::start().await;
    let accepted = h.accept().await;
    let (mut socket, server) = h.socket(Some(1)).await;
    let (request_id, lease) = next_delivery(&mut socket).await;
    h.gateway.drain(Duration::from_millis(200)).unwrap();
    expect_drain(&mut socket, 200).await;
    let started = tokio::time::Instant::now();
    let mut blocker = h.services.postgres.begin().await.unwrap();
    sqlx::query("SELECT id FROM deliveries WHERE id=$1 FOR UPDATE")
        .bind(accepted.delivery_id)
        .execute(&mut *blocker)
        .await
        .unwrap();
    send_frame(&mut socket, ack(request_id, &lease)).await;
    h.wait_for_claim().await;
    // A repeated host signal must not extend a socket's advertised deadline.
    h.gateway.drain(Duration::from_secs(30)).unwrap();
    expect_close(&mut socket, 1000).await;
    assert!(started.elapsed() >= Duration::from_millis(150));
    assert!(started.elapsed() < Duration::from_secs(2));
    blocker.commit().await.unwrap();
    assert_eq!(
        h.state(accepted.delivery_id).await,
        ("in_flight".into(), 1, false)
    );
    server.abort();
}

#[tokio::test]
async fn drain_is_observed_by_authenticated_socket_before_hello() {
    let h = Harness::start().await;
    h.gateway.drain(Duration::from_millis(1)).unwrap();
    let (mut socket, server) = h.socket(None).await;
    expect_drain(&mut socket, 1).await;
    expect_close(&mut socket, 1000).await;
    assert_eq!(h.presence.owner(&h.agent).await.unwrap(), None);
    server.abort();
}
