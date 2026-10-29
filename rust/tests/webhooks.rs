#[allow(dead_code)]
mod support;

use axum::{http::Request, response::IntoResponse};
use relayhub::{
    delivery::Deliveries,
    events::Events,
    identity::Identity,
    presence::{Owner, Presence},
    protocol::{Accepted, EndpointPolicy, HttpResponse, OfflinePolicy, Principal, TimeoutPolicy},
    responses::Responses,
    webhooks::{EndpointAuth, HttpRequest, Webhooks},
};
use std::time::Duration;
use support::TestServices;
use uuid::Uuid;

struct Harness {
    services: TestServices,
    webhooks: Webhooks,
    responses: Responses,
    deliveries: Deliveries,
    presence: Presence,
    owner: Principal,
    app: Principal,
    agent: Principal,
    endpoint: Uuid,
}
impl Harness {
    async fn start(policy: EndpointPolicy) -> Self {
        let services = TestServices::start().await;
        Identity::new(services.postgres.clone())
            .migrate()
            .await
            .unwrap();
        Events::new(services.postgres.clone())
            .migrate()
            .await
            .unwrap();
        let presence = Presence::new(services.redis.clone(), &services.namespace).unwrap();
        let deliveries = Deliveries::new(services.postgres.clone(), presence.clone());
        deliveries.migrate().await.unwrap();
        let webhooks = Webhooks::new(
            services.postgres.clone(),
            presence.clone(),
            services.redis.clone(),
            &services.namespace,
        )
        .unwrap();
        webhooks.migrate().await.unwrap();
        let (user, workspace, app, endpoint) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        sqlx::query("INSERT INTO identity_users(id,email) VALUES($1,$2)")
            .bind(user)
            .bind(format!("{user}@example.test"))
            .execute(&services.postgres)
            .await
            .unwrap();
        sqlx::query("INSERT INTO identity_workspaces(id,owner_id) VALUES($1,$2)")
            .bind(workspace)
            .bind(user)
            .execute(&services.postgres)
            .await
            .unwrap();
        sqlx::query("INSERT INTO identity_apps(id,workspace_id,name) VALUES($1,$2,'webhooks')")
            .bind(app)
            .bind(workspace)
            .execute(&services.postgres)
            .await
            .unwrap();
        sqlx::query("INSERT INTO event_endpoints(id,workspace_id,app_id) VALUES($1,$2,$3)")
            .bind(endpoint)
            .bind(workspace)
            .bind(app)
            .execute(&services.postgres)
            .await
            .unwrap();
        let owner = Principal::WorkspaceOwner {
            user_id: user,
            workspace_id: workspace,
        };
        webhooks
            .configure_endpoint(
                &owner,
                endpoint,
                policy,
                EndpointAuth::Bearer("test-only-ingress-secret".into()),
            )
            .await
            .unwrap();
        let responses = Responses::new(
            services.postgres.clone(),
            deliveries.clone(),
            services.redis.clone(),
            &services.namespace,
        )
        .unwrap();
        Self {
            services,
            webhooks,
            responses,
            deliveries,
            presence,
            owner,
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
    fn request(&self, key: &str) -> HttpRequest {
        Request::builder()
            .method("POST")
            .uri(format!("/hooks/{}?source=test", self.endpoint))
            .header("authorization", "Bearer test-only-ingress-secret")
            .header("idempotency-key", key)
            .header("x-test", "one")
            .header("x-test", "two")
            .body(vec![0, 255, 1])
            .unwrap()
    }
    async fn submit(&self, key: &str) -> HttpResponse {
        self.webhooks
            .receive_webhook(self.endpoint, self.request(key))
            .await
            .unwrap()
    }
    async fn online(&self) {
        self.presence
            .register(
                &self.agent,
                &Owner {
                    gateway_id: Uuid::new_v4(),
                    session_id: Uuid::new_v4(),
                },
            )
            .await
            .unwrap();
    }
    async fn offline(&self) {
        redis::cmd("FLUSHDB")
            .query_async::<()>(&mut self.services.redis.clone())
            .await
            .unwrap();
    }
    async fn counts(&self) -> (i64, i64, i64) {
        sqlx::query_as("SELECT (SELECT count(*) FROM events),(SELECT count(*) FROM deliveries),(SELECT count(*) FROM event_outbox)")
            .fetch_one(&self.services.postgres).await.unwrap()
    }
    async fn state(&self, id: Uuid) -> (String, i32, bool) {
        sqlx::query_as(
            "SELECT state,attempt_count,terminal_at IS NOT NULL FROM deliveries WHERE id=$1",
        )
        .bind(id)
        .fetch_one(&self.services.postgres)
        .await
        .unwrap()
    }
    async fn quota(&self) -> (i64, i64) {
        sqlx::query_as(
            "SELECT outstanding_deliveries,outstanding_payload_bytes FROM workspace_quotas",
        )
        .fetch_one(&self.services.postgres)
        .await
        .unwrap()
    }
}
fn sync_policy(offline: OfflinePolicy, timeout: TimeoutPolicy) -> EndpointPolicy {
    EndpointPolicy::Sync {
        timeout_seconds: 1,
        offline,
        timeout,
    }
}
fn accepted(response: &HttpResponse) -> Accepted {
    serde_json::from_slice(&response.body).unwrap()
}
fn app_response() -> HttpResponse {
    HttpResponse {
        status: 201,
        headers: vec![
            ("set-cookie".into(), "a=1".into()),
            ("set-cookie".into(), "b=2".into()),
        ],
        body: vec![0, 255, 7],
    }
}
async fn pending(pool: &sqlx::PgPool, key: &str) -> Uuid {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(id) = sqlx::query_scalar("SELECT d.id FROM deliveries d JOIN events e ON e.id=d.event_id WHERE e.idempotency_key=$1")
                .bind(key).fetch_optional(pool).await.unwrap() { return id; }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("ingress must durably accept work")
}

#[tokio::test]
async fn async_returns_202_after_commit() {
    let h = Harness::start(EndpointPolicy::Async).await;
    let response = h.submit("async").await;
    assert_eq!(response.status, 202);
    let a = accepted(&response);
    assert_eq!(h.counts().await, (1, 1, 1));
    assert_eq!(h.state(a.delivery_id).await, ("pending".into(), 0, false));
    assert_eq!(h.quota().await, (1, 3));
    assert_eq!(accepted(&h.submit("async").await).event_id, a.event_id);
    assert_eq!(h.counts().await, (1, 1, 1));
}

#[tokio::test]
async fn async_receipt_retry_returns_202_after_response() {
    let h = Harness::start(EndpointPolicy::Async).await;
    let first = h.submit("async-response-retry").await;
    assert_eq!(first.status, 202);
    let original = accepted(&first);
    h.online().await;
    let lease = h
        .deliveries
        .claim_delivery(&h.agent, original.delivery_id)
        .await
        .unwrap();
    h.responses
        .complete_response(&h.agent, lease, app_response())
        .await
        .unwrap();

    let retry = h.submit("async-response-retry").await;
    assert_eq!(retry.status, 202);
    let receipt = accepted(&retry);
    assert_eq!(receipt.event_id, original.event_id);
    assert_eq!(receipt.delivery_id, original.delivery_id);
    assert_eq!(
        h.state(original.delivery_id).await,
        ("succeeded".into(), 1, true)
    );
}

#[tokio::test]
async fn sync_online_returns_app_response() {
    let h = Harness::start(sync_policy(OfflinePolicy::Queue, TimeoutPolicy::Continue)).await;
    h.online().await;
    let (webhooks, endpoint, request) = (h.webhooks.clone(), h.endpoint, h.request("sync"));
    let caller = tokio::spawn(async move { webhooks.receive_webhook(endpoint, request).await });
    let id = pending(&h.services.postgres, "sync").await;
    let lease = h.deliveries.claim_delivery(&h.agent, id).await.unwrap();
    h.responses
        .complete_response(&h.agent, lease, app_response())
        .await
        .unwrap();
    assert_eq!(caller.await.unwrap().unwrap(), app_response());
    assert_eq!(h.state(id).await, ("succeeded".into(), 1, true));
    assert_eq!(h.quota().await, (0, 0));
    let persisted: i64 =
        sqlx::query_scalar("SELECT count(*) FROM webhook_responses WHERE result IS NOT NULL")
            .fetch_one(&h.services.postgres)
            .await
            .unwrap();
    assert_eq!(persisted, 1);
}

#[tokio::test]
async fn offline_queue_202() {
    let h = Harness::start(sync_policy(
        OfflinePolicy::Queue,
        TimeoutPolicy::ExpirePending,
    ))
    .await;
    let response = h.submit("offline-queue").await;
    assert_eq!(response.status, 202);
    assert_eq!(h.counts().await, (1, 1, 1));
    assert_eq!(
        h.state(accepted(&response).delivery_id).await,
        ("pending".into(), 0, false)
    );
    assert_eq!(h.quota().await, (1, 3));
}

#[tokio::test]
async fn offline_reject_503_no_enqueue() {
    let h = Harness::start(sync_policy(OfflinePolicy::Reject, TimeoutPolicy::Continue)).await;
    let result = h
        .webhooks
        .receive_webhook(h.endpoint, h.request("offline-reject"))
        .await;
    assert_eq!(result.unwrap_err().into_response().status().as_u16(), 503);
    assert_eq!(h.counts().await, (0, 0, 0));
    let quotas: i64 = sqlx::query_scalar("SELECT count(*) FROM workspace_quotas")
        .fetch_one(&h.services.postgres)
        .await
        .unwrap();
    assert_eq!(quotas, 0);
}

#[tokio::test]
async fn timeout_continue_202() {
    let h = Harness::start(sync_policy(OfflinePolicy::Queue, TimeoutPolicy::Continue)).await;
    h.online().await;
    let start = tokio::time::Instant::now();
    let response = h.submit("continue").await;
    assert_eq!(response.status, 202);
    assert!(start.elapsed() >= Duration::from_secs(1));
    assert_eq!(
        h.state(accepted(&response).delivery_id).await,
        ("pending".into(), 0, false)
    );
    assert_eq!(h.quota().await, (1, 3));
}

#[tokio::test]
async fn expire_pending_504() {
    let h = Harness::start(sync_policy(
        OfflinePolicy::Queue,
        TimeoutPolicy::ExpirePending,
    ))
    .await;
    h.online().await;
    let response = h.submit("expire").await;
    assert_eq!(response.status, 504);
    let id = pending(&h.services.postgres, "expire").await;
    assert_eq!(h.state(id).await, ("cancelled".into(), 0, true));
    assert_eq!(h.quota().await, (0, 0));
    let discarded: bool = sqlx::query_scalar(
        "SELECT discarded_at IS NOT NULL FROM event_outbox WHERE delivery_id=$1",
    )
    .bind(id)
    .fetch_one(&h.services.postgres)
    .await
    .unwrap();
    assert!(discarded);
}

#[tokio::test]
async fn late_response_cannot_answer_new_request() {
    let h = Harness::start(sync_policy(OfflinePolicy::Queue, TimeoutPolicy::Continue)).await;
    h.online().await;
    let first = h.submit("first").await;
    assert_eq!(first.status, 202);
    let a = accepted(&first);
    let old = h
        .deliveries
        .claim_delivery(&h.agent, a.delivery_id)
        .await
        .unwrap();
    let (webhooks, endpoint, request) = (h.webhooks.clone(), h.endpoint, h.request("second"));
    let second = tokio::spawn(async move { webhooks.receive_webhook(endpoint, request).await });
    let id = pending(&h.services.postgres, "second").await;
    h.responses
        .complete_response(&h.agent, old.clone(), app_response())
        .await
        .unwrap();
    assert!(
        h.responses
            .complete_response(&h.agent, old, app_response())
            .await
            .is_err()
    );
    let response = second.await.unwrap().unwrap();
    assert_eq!(response.status, 202);
    assert_eq!(accepted(&response).delivery_id, id);
    assert_eq!(h.state(a.delivery_id).await, ("succeeded".into(), 1, true));
    assert_eq!(h.state(id).await, ("pending".into(), 0, false));
    let outcomes: Vec<String> =
        sqlx::query_scalar("SELECT outcome FROM webhook_response_audit ORDER BY created_at,id")
            .fetch_all(&h.services.postgres)
            .await
            .unwrap();
    assert_eq!(outcomes, vec!["late", "duplicate"]);
}

#[tokio::test]
async fn invalid_ingress_secret_rejected() {
    let h = Harness::start(EndpointPolicy::Async).await;
    let mut request = h.request("invalid");
    request
        .headers_mut()
        .insert("authorization", "Bearer wrong".parse().unwrap());
    let result = h.webhooks.receive_webhook(h.endpoint, request).await;
    assert_eq!(result.unwrap_err().into_response().status().as_u16(), 401);
    assert_eq!(h.counts().await, (0, 0, 0));
}

#[tokio::test]
async fn claimed_timeout_is_not_cancellation() {
    let h = Harness::start(sync_policy(
        OfflinePolicy::Queue,
        TimeoutPolicy::ExpirePending,
    ))
    .await;
    h.online().await;
    let (webhooks, endpoint, request) = (h.webhooks.clone(), h.endpoint, h.request("claimed"));
    let caller = tokio::spawn(async move { webhooks.receive_webhook(endpoint, request).await });
    let id = pending(&h.services.postgres, "claimed").await;
    let lease = h.deliveries.claim_delivery(&h.agent, id).await.unwrap();
    assert_eq!(caller.await.unwrap().unwrap().status, 504);
    assert_eq!(h.state(id).await, ("in_flight".into(), 1, false));
    assert_eq!(h.quota().await, (1, 3));
    h.responses
        .complete_response(&h.agent, lease, app_response())
        .await
        .unwrap();
    assert_eq!(h.state(id).await, ("succeeded".into(), 1, true));
    assert_eq!(h.quota().await, (0, 0));
}

#[tokio::test]
async fn disconnected_caller_keeps_single_durable_acceptance() {
    let h = Harness::start(sync_policy(OfflinePolicy::Queue, TimeoutPolicy::Continue)).await;
    h.online().await;
    let (webhooks, endpoint, request) = (h.webhooks.clone(), h.endpoint, h.request("disconnected"));
    let caller = tokio::spawn(async move { webhooks.receive_webhook(endpoint, request).await });
    let id = pending(&h.services.postgres, "disconnected").await;
    caller.abort();
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert_eq!(h.counts().await, (1, 1, 1));
    assert_eq!(h.state(id).await, ("pending".into(), 0, false));
    assert_eq!(accepted(&h.submit("disconnected").await).delivery_id, id);
    assert_eq!(h.counts().await, (1, 1, 1));
}

#[tokio::test]
async fn scoped_status_and_policy_reject_forged_ownership() {
    let h = Harness::start(EndpointPolicy::Async).await;
    let a = accepted(&h.submit("scope").await);
    assert_eq!(
        h.webhooks
            .event_status(&h.owner, a.event_id)
            .await
            .unwrap()
            .event_id,
        a.event_id
    );
    assert_eq!(
        h.webhooks
            .event_status(&h.app, a.event_id)
            .await
            .unwrap()
            .delivery_id,
        a.delivery_id
    );
    let other_owner = Principal::WorkspaceOwner {
        user_id: Uuid::new_v4(),
        workspace_id: Uuid::new_v4(),
    };
    let mut other_app = h.app.clone();
    if let Principal::Application { app_id, .. } = &mut other_app {
        *app_id = Uuid::new_v4();
    }
    for principal in [&other_owner, &other_app, &h.agent] {
        assert_eq!(
            h.webhooks
                .event_status(principal, a.event_id)
                .await
                .unwrap_err()
                .into_response()
                .status()
                .as_u16(),
            403
        );
        assert_eq!(
            h.webhooks
                .configure_endpoint(
                    principal,
                    h.endpoint,
                    EndpointPolicy::Async,
                    EndpointAuth::Public
                )
                .await
                .unwrap_err()
                .into_response()
                .status()
                .as_u16(),
            403
        );
    }
    assert_eq!(h.counts().await, (1, 1, 1));
}

#[tokio::test]
async fn ingress_routes_enforce_auth_limits_and_strip_credentials() {
    let h = Harness::start(EndpointPolicy::Async).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = h.webhooks.router().layer(axum::Extension(h.app.clone()));
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let client = reqwest::Client::new();
    let url = format!("http://{address}/hooks/{}", h.endpoint);
    assert_eq!(
        client.post(&url).send().await.unwrap().status().as_u16(),
        401
    );
    assert_eq!(
        client
            .post(&url)
            .bearer_auth("test-only-ingress-secret")
            .body(vec![0; 1_048_577])
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        413
    );
    let response = client
        .post(&url)
        .bearer_auth("test-only-ingress-secret")
        .header("cookie", "private=secret")
        .header("connection", "x-private")
        .header("x-private", "hidden")
        .header("x-csrf-token", "csrf-test")
        .header("x-test", "safe")
        .body(vec![0, 255, 1])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 202);
    let a: Accepted = response.json().await.unwrap();
    let payload: Vec<u8> = sqlx::query_scalar("SELECT payload FROM events WHERE id=$1")
        .bind(a.event_id)
        .fetch_one(&h.services.postgres)
        .await
        .unwrap();
    let input: relayhub::protocol::EventInput = serde_json::from_slice(&payload).unwrap();
    assert_eq!(input.body, vec![0, 255, 1]);
    for (name, _) in &input.headers {
        assert!(
            ![
                "authorization",
                "cookie",
                "connection",
                "x-private",
                "x-csrf-token",
                "host",
                "content-length"
            ]
            .contains(&name.as_str())
        );
    }
    assert!(input.headers.contains(&("x-test".into(), "safe".into())));
    let status = client
        .get(format!("http://{address}/api/v1/events/{}", a.event_id))
        .send()
        .await
        .unwrap();
    assert_eq!(status.status().as_u16(), 200);
    assert_eq!(
        status.json::<serde_json::Value>().await.unwrap()["state"],
        "pending"
    );
    assert_eq!(h.counts().await, (1, 1, 1));
    server.abort();
}

#[tokio::test]
async fn response_fences_and_limits_preserve_active_lease() {
    let h = Harness::start(sync_policy(OfflinePolicy::Queue, TimeoutPolicy::Continue)).await;
    let a = accepted(&h.submit("fence").await);
    h.online().await;
    let lease = h
        .deliveries
        .claim_delivery(&h.agent, a.delivery_id)
        .await
        .unwrap();
    let mut oversized = app_response();
    oversized.body = vec![0; 1_048_577];
    assert_eq!(
        h.responses
            .complete_response(&h.agent, lease.clone(), oversized)
            .await
            .unwrap_err()
            .into_response()
            .status()
            .as_u16(),
        413
    );
    let mut stale = lease.clone();
    stale.attempt_token = Uuid::new_v4();
    assert_eq!(
        h.responses
            .complete_response(&h.agent, stale, app_response())
            .await
            .unwrap_err()
            .into_response()
            .status()
            .as_u16(),
        409
    );
    let mut other_agent = h.agent.clone();
    if let Principal::Agent { agent_id, .. } = &mut other_agent {
        *agent_id = Uuid::new_v4();
    }
    assert!(
        h.responses
            .complete_response(&other_agent, lease.clone(), app_response())
            .await
            .is_err()
    );
    assert_eq!(h.state(a.delivery_id).await, ("in_flight".into(), 1, false));
    h.responses
        .complete_response(&h.agent, lease.clone(), app_response())
        .await
        .unwrap();
    assert_eq!(h.state(a.delivery_id).await, ("succeeded".into(), 1, true));
    assert!(
        h.responses
            .complete_response(&h.agent, lease, app_response())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn redis_unavailable_is_bounded_and_preserves_acceptance() {
    let h = Harness::start(sync_policy(OfflinePolicy::Queue, TimeoutPolicy::Continue)).await;
    // Keep this timing assertion specific to Redis, independent of concurrent Argon2 work.
    h.webhooks
        .configure_endpoint(
            &h.owner,
            h.endpoint,
            sync_policy(OfflinePolicy::Queue, TimeoutPolicy::Continue),
            EndpointAuth::Public,
        )
        .await
        .unwrap();
    h.online().await;
    redis::cmd("CLIENT")
        .arg("PAUSE")
        .arg(2000)
        .arg("ALL")
        .query_async::<()>(&mut h.services.redis.clone())
        .await
        .unwrap();
    let response = tokio::time::timeout(Duration::from_millis(1500), h.submit("redis-down"))
        .await
        .expect("presence must be bounded");
    assert_eq!(response.status, 202);
    assert_eq!(h.counts().await, (1, 1, 1));
    assert_eq!(
        h.state(accepted(&response).delivery_id).await,
        ("pending".into(), 0, false)
    );
}

#[tokio::test]
async fn idempotent_retry_keeps_original_policy_after_reconfiguration() {
    let h = Harness::start(EndpointPolicy::Async).await;
    let original = accepted(&h.submit("original-policy").await);
    h.webhooks
        .configure_endpoint(
            &h.owner,
            h.endpoint,
            sync_policy(OfflinePolicy::Queue, TimeoutPolicy::ExpirePending),
            EndpointAuth::Public,
        )
        .await
        .unwrap();
    h.online().await;
    let response = tokio::time::timeout(Duration::from_secs(2), h.submit("original-policy"))
        .await
        .expect("retry must not open a sync wait for an async receipt");
    assert_eq!(response.status, 202);
    assert_eq!(accepted(&response).event_id, original.event_id);
    assert_eq!(h.counts().await, (1, 1, 1));
    assert_eq!(
        h.state(original.delivery_id).await,
        ("pending".into(), 0, false)
    );
}

#[tokio::test]
async fn expired_sync_retry_uses_original_policy_after_reconfiguration_and_offline() {
    let h = Harness::start(sync_policy(
        OfflinePolicy::Queue,
        TimeoutPolicy::ExpirePending,
    ))
    .await;
    h.online().await;
    let first = h.submit("expired-retry").await;
    assert_eq!(first.status, 504);
    let original = pending(&h.services.postgres, "expired-retry").await;
    assert_eq!(h.state(original).await, ("cancelled".into(), 0, true));

    h.webhooks
        .configure_endpoint(
            &h.owner,
            h.endpoint,
            EndpointPolicy::Async,
            EndpointAuth::Public,
        )
        .await
        .unwrap();
    h.offline().await;
    let retry = h.submit("expired-retry").await;
    assert_eq!(retry.status, 504);
    assert_eq!(h.state(original).await, ("cancelled".into(), 0, true));
    assert_eq!(h.counts().await, (1, 1, 1));
}

#[tokio::test]
async fn answered_sync_retry_uses_persisted_response_after_reconfiguration_and_offline() {
    let h = Harness::start(sync_policy(OfflinePolicy::Queue, TimeoutPolicy::Continue)).await;
    h.online().await;
    let (webhooks, endpoint, request) =
        (h.webhooks.clone(), h.endpoint, h.request("answered-retry"));
    let caller = tokio::spawn(async move { webhooks.receive_webhook(endpoint, request).await });
    let id = pending(&h.services.postgres, "answered-retry").await;
    let lease = h.deliveries.claim_delivery(&h.agent, id).await.unwrap();
    h.responses
        .complete_response(&h.agent, lease, app_response())
        .await
        .unwrap();
    let first = caller.await.unwrap().unwrap();
    assert_eq!(first, app_response());

    h.webhooks
        .configure_endpoint(
            &h.owner,
            h.endpoint,
            EndpointPolicy::Async,
            EndpointAuth::Public,
        )
        .await
        .unwrap();
    h.offline().await;
    let retry = h.submit("answered-retry").await;
    assert_eq!(retry, app_response());
    assert_eq!(h.counts().await, (1, 1, 1));
}

#[tokio::test]
async fn concurrent_ingress_does_not_starve_acceptance_pool() {
    let h = Harness::start(EndpointPolicy::Async).await;
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(2)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(h.services.postgres.connect_options().as_ref().clone())
        .await
        .unwrap();
    let webhooks = Webhooks::new(
        pool.clone(),
        h.presence.clone(),
        h.services.redis.clone(),
        &h.services.namespace,
    )
    .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(
            webhooks.receive_webhook(h.endpoint, h.request("parallel-a")),
            webhooks.receive_webhook(h.endpoint, h.request("parallel-b"))
        )
    })
    .await
    .expect("concurrent policy locks must leave capacity for acceptance");
    assert_eq!(result.0.unwrap().status, 202);
    assert_eq!(result.1.unwrap().status, 202);
    assert_eq!(h.counts().await, (2, 2, 2));
    pool.close().await;
}

#[tokio::test]
async fn restart_claim_reconciles_expired_pending_webhook_without_waiter() {
    let h = Harness::start(sync_policy(
        OfflinePolicy::Queue,
        TimeoutPolicy::ExpirePending,
    ))
    .await;
    let response = h.submit("recovery-expiry").await;
    assert_eq!(response.status, 202);
    let receipt = accepted(&response);
    sqlx::query("UPDATE webhook_responses SET deadline_at=clock_timestamp()-interval '1 second' WHERE delivery_id=$1")
        .bind(receipt.delivery_id)
        .execute(&h.services.postgres)
        .await
        .unwrap();
    h.online().await;

    let recovered = Deliveries::new(h.services.postgres.clone(), h.presence.clone());
    let claim = recovered
        .claim_delivery(&h.agent, receipt.delivery_id)
        .await;
    assert!(claim.is_err());
    assert_eq!(
        h.state(receipt.delivery_id).await,
        ("cancelled".into(), 0, true)
    );
    assert_eq!(h.quota().await, (0, 0));
}
