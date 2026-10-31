#[allow(dead_code)]
mod support;

use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use hmac::{Hmac, Mac};
use relayhub::{
    host::Host,
    legacy_crypto::SecretCipher,
    protocol::{Frame, HttpResponse, parse_frame},
};
use reqwest::{Client, Method, RequestBuilder};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{process::Command, time::Duration};
use support::TestServices;
use tokio::task::JoinHandle;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest},
};
use uuid::Uuid;

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
struct Harness {
    services: TestServices,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    server: JoinHandle<Result<(), relayhub::host::HostError>>,
    url: String,
    client: Client,
    workspace: Uuid,
    app_id: Uuid,
    agent: Uuid,
    endpoint: Uuid,
    session: String,
    csrf: String,
    app_token: String,
    agent_token: String,
}
impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
    }
}
impl Harness {
    async fn start() -> Self {
        let services = TestServices::start().await;
        let host = Host::new(
            services.postgres.clone(),
            services.jetstream.clone(),
            services.redis.clone(),
            &services.namespace,
        )
        .unwrap();
        host.migrate().await.unwrap();
        host.migrate().await.unwrap(); // Replica-safe, reentrant startup.
        let user = Uuid::new_v4();
        let workspace = Uuid::new_v4();
        let app = Uuid::new_v4();
        let agent = Uuid::new_v4();
        let session = Uuid::new_v4().to_string();
        let csrf = Uuid::new_v4().to_string();
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
        sqlx::query("INSERT INTO identity_apps(id,workspace_id,name) VALUES($1,$2,'host')")
            .bind(app)
            .bind(workspace)
            .execute(&services.postgres)
            .await
            .unwrap();
        sqlx::query("INSERT INTO identity_sessions(id,user_id,token_hash,csrf_hash,expires_at) VALUES($1,$2,$3,$4,now()+interval '1 day')").bind(Uuid::new_v4()).bind(user).bind(Sha256::digest(session.as_bytes()).to_vec()).bind(Sha256::digest(csrf.as_bytes()).to_vec()).execute(&services.postgres).await.unwrap();
        let app_token = credential(&services.postgres, workspace, app, None).await;
        let agent_token = credential(&services.postgres, workspace, app, Some(agent)).await;
        let endpoint = Uuid::new_v4();
        sqlx::query("INSERT INTO event_endpoints(id,workspace_id,app_id) VALUES($1,$2,$3)")
            .bind(endpoint)
            .bind(workspace)
            .bind(app)
            .execute(&services.postgres)
            .await
            .unwrap();
        // Construct the full router explicitly: merging the old socket here would panic.
        let _ = host.router();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            host.serve(listener, async {
                let _ = stopped.await;
            })
            .await
        });
        Self {
            services,
            stop: Some(stop),
            server,
            url,
            client: Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            workspace,
            app_id: app,
            agent,
            endpoint,
            session,
            csrf,
            app_token,
            agent_token,
        }
    }
    fn request(&self, method: Method, path: &str) -> RequestBuilder {
        self.client.request(method, format!("{}{path}", self.url))
    }
    fn input(&self) -> Value {
        json!({"endpoint_id":self.endpoint,"event_type":"host.test","method":"POST","path":"/","headers":[],"body":STANDARD.encode(b"<script>alert('text only')</script>")})
    }
    fn app_id(&self) -> Uuid {
        self.app_id
    }
    fn browser(&self, method: Method, path: &str) -> RequestBuilder {
        self.request(method, path).header(
            "cookie",
            format!("__Host-relayhub_session={}", self.session),
        )
    }
    fn browser_write(&self, method: Method, path: &str) -> RequestBuilder {
        self.browser(method, path)
            .header("x-csrf-token", &self.csrf)
    }
    async fn publish(&self) -> Value {
        let r = self
            .request(Method::POST, "/api/v1/events")
            .bearer_auth(&self.app_token)
            .header("idempotency-key", Uuid::new_v4().to_string())
            .json(&self.input())
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 202);
        r.json().await.unwrap()
    }
    async fn state(&self, receipt: &Value) -> Value {
        let r = self
            .request(
                Method::GET,
                &format!("/api/v1/events/{}", receipt["event_id"].as_str().unwrap()),
            )
            .bearer_auth(&self.app_token)
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 200);
        r.json().await.unwrap()
    }
    async fn socket(&self) -> Socket {
        let mut request = format!("{}/api/v1/socket", self.url.replace("http:", "ws:"))
            .into_client_request()
            .unwrap();
        request.headers_mut().insert(
            "authorization",
            format!("Bearer {}", self.agent_token).parse().unwrap(),
        );
        request
            .headers_mut()
            .insert("sec-websocket-protocol", "relayhub.v1".parse().unwrap());
        let (mut socket, _) = tokio_tungstenite::connect_async(request).await.unwrap();
        send(
            &mut socket,
            Frame::Hello {
                version: 1,
                request_id: Uuid::new_v4(),
                agent_id: self.agent,
                max_in_flight: 1,
            },
        )
        .await;
        socket
    }
}
async fn credential(
    pool: &sqlx::PgPool,
    workspace: Uuid,
    app: Uuid,
    agent: Option<Uuid>,
) -> String {
    let token = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO identity_credentials(id,workspace_id,app_id,kind,agent_id,token_hash) VALUES($1,$2,$3,$4,$5,$6)").bind(Uuid::new_v4()).bind(workspace).bind(app).bind(if agent.is_some(){"agent"}else{"application"}).bind(agent).bind(Sha256::digest(token.as_bytes()).to_vec()).execute(pool).await.unwrap();
    token
}

#[tokio::test]
async fn legacy_signed_api_key_can_publish_through_rust_host() {
    let services = TestServices::start().await;
    let encryption_key = STANDARD.encode([8u8; 32]);
    let host = Host::new_with_secret_encryption_key(
        services.postgres.clone(),
        services.jetstream.clone(),
        services.redis.clone(),
        &services.namespace,
        &encryption_key,
    )
    .unwrap();
    host.migrate().await.unwrap();
    let user = Uuid::new_v4();
    let workspace = Uuid::new_v4();
    let app = Uuid::new_v4();
    let endpoint = Uuid::new_v4();
    sqlx::query("INSERT INTO identity_users(id,email,is_admin) VALUES($1,$2,true)")
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
    sqlx::query(
        "INSERT INTO identity_apps(id,workspace_id,name) VALUES($1,$2,'legacy signed app')",
    )
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
    let api_key = "rhk_legacy_client";
    let hmac_secret = b"rhs_legacy_secret";
    let encrypted = SecretCipher::from_base64(&encryption_key)
        .unwrap()
        .encrypt(hmac_secret)
        .unwrap();
    let api_key_hash = Sha256::digest(api_key.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    sqlx::query("INSERT INTO identity_credentials(id,workspace_id,app_id,kind,token_hash,hmac_secret_hash,legacy_api_key_hash,encrypted_hmac_secret) VALUES($1,$2,$3,'application',$4,$5,$6,$7)")
        .bind(Uuid::new_v4())
        .bind(workspace)
        .bind(app)
        .bind(Sha256::digest(b"bearer-disabled").to_vec())
        .bind(Sha256::digest(hmac_secret).to_vec())
        .bind(api_key_hash)
        .bind(encrypted)
        .execute(&services.postgres)
        .await
        .unwrap();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let router = host.router();
    let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let payload = json!({
        "endpoint_id": endpoint,
        "event_type": "legacy.signed",
        "method": "POST",
        "path": "/hooks/legacy",
        "headers": [],
        "body": STANDARD.encode(br#"{"ok":true}"#)
    });
    let body = serde_json::to_vec(&payload).unwrap();
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();
    let body_hash = Sha256::digest(&body)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let canonical = format!("{timestamp}\nPOST\n/api/v1/events\n{body_hash}");
    let mut mac = Hmac::<Sha256>::new_from_slice(hmac_secret).unwrap();
    mac.update(canonical.as_bytes());
    let signature = mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let response = Client::new()
        .post(format!("{url}/api/v1/events"))
        .header("idempotency-key", "legacy-signed-1")
        .header("x-relayhub-api-key", api_key)
        .header("x-relayhub-timestamp", timestamp)
        .header("x-relayhub-signature", signature)
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 202);
    server.abort();
}

async fn send(socket: &mut Socket, frame: Frame) {
    socket
        .send(Message::Text(serde_json::to_string(&frame).unwrap().into()))
        .await
        .unwrap();
}
async fn frame(socket: &mut Socket) -> Frame {
    let message = tokio::time::timeout(Duration::from_secs(8), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    parse_frame(&message.into_data()).unwrap()
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
async fn eventually(mut check: impl AsyncFnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(8), async {
        while !check().await {
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn composed_routes_publish_authenticated_durable_receipt_and_outbox() {
    let h = Harness::start().await;
    assert_eq!(
        h.request(Method::POST, "/api/v1/events")
            .header("idempotency-key", "test")
            .json(&h.input())
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        h.request(
            Method::GET,
            &format!("/api/v1/workspaces/{}/apps", h.workspace)
        )
        .bearer_auth(&h.session)
        .send()
        .await
        .unwrap()
        .status(),
        200
    );
    assert_eq!(
        h.request(Method::POST, "/api/v1/realtime")
            .body("{}")
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let ingress = h
        .request(Method::POST, &format!("/hooks/{}", h.endpoint))
        .body("event")
        .send()
        .await
        .unwrap();
    assert_eq!(ingress.status(), 401);
    assert_eq!(
        ingress.json::<Value>().await.unwrap(),
        json!({"error":"invalid_ingress_secret"})
    );
    let key = Uuid::new_v4().to_string();
    let publish = || {
        h.request(Method::POST, "/api/v1/events")
            .bearer_auth(&h.app_token)
            .header("idempotency-key", &key)
            .json(&h.input())
    };
    let r = publish().send().await.unwrap();
    assert_eq!(r.status(), 202);
    assert_eq!(r.headers()["cache-control"], "no-store");
    let receipt: Value = r.json().await.unwrap();
    assert_eq!(
        publish()
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap(),
        receipt
    );
    let id = Uuid::parse_str(receipt["delivery_id"].as_str().unwrap()).unwrap();
    eventually(async || {
        sqlx::query_scalar::<_, bool>(
            "SELECT sent_at IS NOT NULL FROM event_outbox WHERE delivery_id=$1",
        )
        .bind(id)
        .fetch_one(&h.services.postgres)
        .await
        .unwrap()
    })
    .await;
    let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM events),(SELECT count(*) FROM deliveries),(SELECT count(*) FROM event_outbox)").fetch_one(&h.services.postgres).await.unwrap();
    assert_eq!(counts, (1, 1, 1));
    assert_eq!(receipt["status"], 202);
    assert_eq!(h.state(&receipt).await["state"], "pending");
    assert_eq!(
        h.request(
            Method::GET,
            &format!("/api/v1/events/{}", receipt["event_id"].as_str().unwrap())
        )
        .send()
        .await
        .unwrap()
        .status(),
        401
    );
    assert_eq!(
        h.request(Method::POST, "/api/v1/events")
            .bearer_auth(&h.agent_token)
            .header("idempotency-key", "denied")
            .json(&h.input())
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        h.request(Method::POST, "/api/v1/events")
            .bearer_auth(&h.app_token)
            .json(&h.input())
            .send()
            .await
            .unwrap()
            .status(),
        422
    );
    let mut changed = h.input();
    changed["body"] = json!("Y2hhbmdlZA==");
    assert_eq!(
        h.request(Method::POST, "/api/v1/events")
            .bearer_auth(&h.app_token)
            .header("idempotency-key", key)
            .json(&changed)
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    assert_eq!(
        h.request(Method::POST, "/api/v1/events")
            .bearer_auth(&h.app_token)
            .header("content-type", "application/json")
            .header("idempotency-key", "oversized")
            .body("x".repeat(2_097_153))
            .send()
            .await
            .unwrap()
            .status(),
        413
    );
    sqlx::query(
        "UPDATE events SET created_at=now()-interval '8 days',expires_at=now()-interval '1 day'",
    )
    .execute(&h.services.postgres)
    .await
    .unwrap();
    eventually(async || {
        sqlx::query_scalar::<_, String>("SELECT state FROM deliveries WHERE id=$1")
            .bind(id)
            .fetch_one(&h.services.postgres)
            .await
            .unwrap()
            == "expired"
    })
    .await;
}

#[tokio::test]
async fn socket_response_completes_actual_response_and_reconciles_broker() {
    let h = Harness::start().await;
    let receipt = h.publish().await;
    let mut socket = h.socket().await;
    let Frame::Delivery {
        request_id, lease, ..
    } = frame(&mut socket).await
    else {
        panic!("real Gateway delivery required")
    };
    send(
        &mut socket,
        Frame::Response {
            version: 1,
            request_id,
            event_id: lease.event_id,
            delivery_id: lease.delivery_id,
            attempt_token: lease.attempt_token,
            response: HttpResponse {
                status: 201,
                headers: vec![("content-type".into(), "text/plain".into())],
                body: b"completed".to_vec(),
            },
        },
    )
    .await;
    eventually(async || h.state(&receipt).await["state"] == "succeeded").await;
    let status = h.state(&receipt).await;
    assert_eq!(status["response_state"], "answered");
    assert_eq!(status["response"]["body"], "Y29tcGxldGVk");
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM webhook_response_audit WHERE outcome='answered'")
            .fetch_one(&h.services.postgres)
            .await
            .unwrap();
    assert_eq!(count, 1);
    let mut stream = h
        .services
        .jetstream
        .get_stream(&h.services.namespace)
        .await
        .unwrap();
    eventually(async || stream.info().await.unwrap().state.messages == 0).await;
    socket.close(None).await.unwrap();
}

#[tokio::test]
async fn publish_rejects_unsupported_principals_and_untrusted_input() {
    let h = Harness::start().await;
    for token in [&h.agent_token, &h.session] {
        let response = h
            .request(Method::POST, "/api/v1/events")
            .bearer_auth(token)
            .header("idempotency-key", "denied")
            .json(&h.input())
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 403);
    }
    let response = h
        .request(Method::POST, "/api/v1/events")
        .bearer_auth("invalid-secret")
        .json(&h.input())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 401);
    let mut input = h.input();
    input["principal"] = json!({"kind":"system_admin","user_id":Uuid::new_v4()});
    let response = h
        .request(Method::POST, "/api/v1/events")
        .bearer_auth(&h.app_token)
        .header("idempotency-key", "injected")
        .json(&input)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 422);
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        json!({"error":"invalid_input"})
    );
    let response = h
        .request(Method::POST, "/api/v1/events")
        .bearer_auth(&h.app_token)
        .header("idempotency-key", "malformed")
        .header("content-type", "application/json")
        .body("{private-invalid-json")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 422);
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        json!({"error":"invalid_input"})
    );
    let other_app = Uuid::new_v4();
    sqlx::query("INSERT INTO identity_apps(id,workspace_id,name) VALUES($1,$2,'other')")
        .bind(other_app)
        .bind(h.workspace)
        .execute(&h.services.postgres)
        .await
        .unwrap();
    let other_token = credential(&h.services.postgres, h.workspace, other_app, None).await;
    assert_eq!(
        h.request(Method::POST, "/api/v1/events")
            .bearer_auth(other_token)
            .header("idempotency-key", "cross-app")
            .json(&h.input())
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM events")
        .fetch_one(&h.services.postgres)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn dashboard_requires_same_origin_csrf_and_has_cursor_replay_audit() {
    let h = Harness::start().await;
    assert_eq!(
        h.request(Method::GET, "/api/v1/dashboard/deliveries")
            .bearer_auth(&h.app_token)
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        h.browser_write(Method::POST, "/api/v1/dashboard/realtime-token")
            .json(&json!({"app_id":"not-a-uuid","channel":"bad"}))
            .send()
            .await
            .unwrap()
            .status(),
        422
    );
    assert_eq!(
        h.browser(Method::POST, "/api/v1/dashboard/realtime-token")
            .json(&json!({"app_id":h.app_id(),"channel":"dashboard"}))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        h.browser(Method::POST, "/api/v1/dashboard/realtime-token")
            .json(&json!({"app_id":Uuid::new_v4(),"channel":"dashboard"}))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    let receipt = h.publish().await;
    let page = h
        .browser(Method::GET, "/api/v1/dashboard/deliveries?limit=1")
        .send()
        .await
        .unwrap();
    assert_eq!(page.status(), 200);
    let page: Value = page.json().await.unwrap();
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    assert_eq!(page["next_cursor"], receipt["delivery_id"]);

    sqlx::query("UPDATE deliveries SET state='succeeded' WHERE id=$1")
        .bind(Uuid::parse_str(receipt["delivery_id"].as_str().unwrap()).unwrap())
        .execute(&h.services.postgres)
        .await
        .unwrap();
    let replay = h
        .browser_write(
            Method::POST,
            &format!(
                "/api/v1/dashboard/deliveries/{}/replay",
                receipt["delivery_id"].as_str().unwrap()
            ),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(replay.status(), 200);
    let replay: Value = replay.json().await.unwrap();
    assert_eq!(
        replay["audit"]["source_delivery_id"],
        receipt["delivery_id"]
    );
    let audited: i64 =
        sqlx::query_scalar("SELECT count(*) FROM delivery_replays WHERE source_delivery_id=$1")
            .bind(Uuid::parse_str(receipt["delivery_id"].as_str().unwrap()).unwrap())
            .fetch_one(&h.services.postgres)
            .await
            .unwrap();
    assert_eq!(audited, 1);
    let audit = h
        .browser(
            Method::GET,
            &format!(
                "/api/v1/dashboard/deliveries/{}/audit",
                replay["accepted"]["delivery_id"].as_str().unwrap()
            ),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(audit.status(), 200);
    assert_eq!(
        audit
            .json::<Value>()
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn dashboard_client_contract_lists_endpoints_and_publishes_durable_events() {
    let h = Harness::start().await;

    let endpoints = h
        .browser(Method::GET, "/api/v1/dashboard/endpoints")
        .send()
        .await
        .unwrap();
    assert_eq!(endpoints.status(), 200);
    let endpoints: Vec<Value> = endpoints.json().await.unwrap();
    assert!(endpoints.iter().any(|endpoint| {
        endpoint["id"] == json!(h.endpoint)
            && endpoint["app_id"] == json!(h.app_id)
            && endpoint["policy"]["mode"] == "async"
            && endpoint["auth"] == "bearer"
    }));

    let agents = h
        .browser(Method::GET, "/api/v1/dashboard/agents")
        .send()
        .await
        .unwrap();
    assert_eq!(agents.status(), 200);
    let agents: Vec<Value> = agents.json().await.unwrap();
    assert!(agents.iter().all(|agent| agent["online"].is_boolean()));

    let accepted = h
        .browser_write(Method::POST, "/api/v1/dashboard/events")
        .header("idempotency-key", Uuid::new_v4().to_string())
        .json(&h.input())
        .send()
        .await
        .unwrap();
    assert_eq!(accepted.status(), 202);
    let accepted: Value = accepted.json().await.unwrap();
    assert!(accepted["event_id"].as_str().is_some());
    assert!(accepted["delivery_id"].as_str().is_some());

    let updated = h
        .browser_write(
            Method::PATCH,
            &format!("/api/v1/dashboard/endpoints/{}", h.endpoint),
        )
        .json(&json!({
            "policy": {"mode":"sync","timeout_seconds":15,"offline":"queue","timeout":"continue"}
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(updated.status(), 200);
    assert_eq!(
        updated.json::<Value>().await.unwrap()["policy"]["mode"],
        "sync"
    );
}

#[tokio::test]
async fn system_admin_dashboard_lists_without_workspace_scope_and_hides_revoked_agents() {
    let h = Harness::start().await;
    let revoked_agent = Uuid::new_v4();
    sqlx::query("UPDATE identity_users SET is_admin=true WHERE id=(SELECT user_id FROM identity_sessions WHERE token_hash=$1)")
        .bind(Sha256::digest(h.session.as_bytes()).to_vec())
        .execute(&h.services.postgres)
        .await
        .unwrap();
    sqlx::query("INSERT INTO identity_credentials(id,workspace_id,app_id,kind,agent_id,token_hash,revoked_at) VALUES($1,$2,$3,'agent',$4,$5,clock_timestamp())")
        .bind(Uuid::new_v4())
        .bind(h.workspace)
        .bind(h.app_id)
        .bind(revoked_agent)
        .bind(Sha256::digest(Uuid::new_v4().as_bytes()).to_vec())
        .execute(&h.services.postgres)
        .await
        .unwrap();

    let agents = h
        .browser(Method::GET, "/api/v1/dashboard/agents")
        .send()
        .await
        .unwrap();
    assert_eq!(agents.status(), 200);
    let agents: Vec<Value> = agents.json().await.unwrap();
    assert!(
        agents
            .iter()
            .any(|agent| agent["agent_id"] == json!(h.agent))
    );
    assert!(
        !agents
            .iter()
            .any(|agent| agent["agent_id"] == json!(revoked_agent))
    );

    let deliveries = h
        .browser(Method::GET, "/api/v1/dashboard/deliveries")
        .send()
        .await
        .unwrap();
    assert_eq!(deliveries.status(), 200);
}

#[tokio::test]
async fn scoped_realtime_token_cannot_cross_channel_or_durable_routes() {
    let h = Harness::start().await;
    let response = h
        .browser_write(Method::POST, "/api/v1/dashboard/realtime-token")
        .json(&json!({"app_id":h.app_id(),"channel":"dashboard"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let token: Value = response.json().await.unwrap();
    assert_eq!(token["expires_in_seconds"], 60);
    let frame = serde_json::to_vec(&Frame::Realtime {
        version: 1,
        request_id: Uuid::new_v4(),
        channel: "dashboard".into(),
        body: b"ok".to_vec(),
    })
    .unwrap();
    let realtime = h
        .request(Method::POST, "/api/v1/realtime")
        .bearer_auth(token["token"].as_str().unwrap())
        .body(frame.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(realtime.status(), 202);
    let wrong_channel = serde_json::to_vec(&Frame::Realtime {
        version: 1,
        request_id: Uuid::new_v4(),
        channel: "other".into(),
        body: b"no".to_vec(),
    })
    .unwrap();
    assert_eq!(
        h.request(Method::POST, "/api/v1/realtime")
            .bearer_auth(token["token"].as_str().unwrap())
            .body(wrong_channel)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        h.request(Method::POST, "/api/v1/events")
            .bearer_auth(token["token"].as_str().unwrap())
            .header("idempotency-key", "scoped-is-not-durable")
            .json(&h.input())
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let agent_route = h
        .request(Method::GET, "/api/v1/socket")
        .bearer_auth(token["token"].as_str().unwrap())
        .send()
        .await
        .unwrap();
    assert!(!agent_route.status().is_success());
}

#[tokio::test]
async fn scoped_realtime_auth_reports_redis_outage_as_unavailable() {
    let h = Harness::start().await;
    let response = h
        .browser_write(Method::POST, "/api/v1/dashboard/realtime-token")
        .json(&json!({"app_id":h.app_id(),"channel":"dashboard"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let token: Value = response.json().await.unwrap();
    let frame = serde_json::to_vec(&Frame::Realtime {
        version: 1,
        request_id: Uuid::new_v4(),
        channel: "dashboard".into(),
        body: b"redis outage".to_vec(),
    })
    .unwrap();

    service(&h.services, "pause", "redis");
    let response = h
        .request(Method::POST, "/api/v1/realtime")
        .bearer_auth(token["token"].as_str().unwrap())
        .body(frame)
        .send()
        .await
        .unwrap();
    service(&h.services, "unpause", "redis");
    assert_eq!(response.status(), 503);
}

#[tokio::test]
async fn shutdown_drains_gateway_and_allows_active_response() {
    let mut h = Harness::start().await;
    let receipt = h.publish().await;
    let mut socket = h.socket().await;
    let Frame::Delivery {
        request_id, lease, ..
    } = frame(&mut socket).await
    else {
        panic!("delivery required")
    };
    h.stop.take().unwrap().send(()).unwrap();
    assert!(matches!(
        frame(&mut socket).await,
        Frame::Drain {
            deadline_ms: 30_000,
            ..
        }
    ));
    send(
        &mut socket,
        Frame::Response {
            version: 1,
            request_id,
            event_id: lease.event_id,
            delivery_id: lease.delivery_id,
            attempt_token: lease.attempt_token,
            response: HttpResponse {
                status: 200,
                headers: vec![],
                body: b"drained".to_vec(),
            },
        },
    )
    .await;
    eventually(async || {
        sqlx::query_scalar::<_, String>("SELECT state FROM deliveries WHERE id=$1")
            .bind(lease.delivery_id)
            .fetch_one(&h.services.postgres)
            .await
            .unwrap()
            == "succeeded"
    })
    .await;
    assert_eq!(receipt["event_id"], lease.event_id.to_string());
    tokio::time::timeout(Duration::from_secs(36), &mut h.server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[test]
fn server_binary_rejects_invalid_config_without_logging_secrets() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_relayhub-server"))
        .env_clear()
        .env(
            "POSTGRES_URL",
            "postgres://private-user:private-secret@invalid/db",
        )
        .env(
            "NATS_URL",
            "nats://private-user:private-secret@invalid:4222",
        )
        .env("REDIS_ADDR", "127.0.0.1:1")
        .env("HTTP_ADDR", "invalid-listen-address")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(
        String::from_utf8(output.stderr).unwrap().trim(),
        "invalid HTTP_ADDR"
    );
    assert!(output.stdout.is_empty());
}

// Each child owns its environment; parallel tests never mutate the runner's environment.
#[test]
fn config_env_defaults_overrides_and_redis_precedence() {
    use relayhub::host::HostConfig;
    use std::process::Command;
    const CASE: &str = "RELAYHUB_TEST_CONFIG_CASE";
    if let Ok(case) = std::env::var(CASE) {
        let config = HostConfig::from_env().unwrap();
        assert_eq!(config.postgres_url, "postgres://127.0.0.1/test");
        assert_eq!(config.nats_url, "nats://127.0.0.1:4222");
        if case == "defaults" {
            assert_eq!(config.namespace, "relayhub_v1");
            assert_eq!(config.http_addr.to_string(), "0.0.0.0:8080");
            assert_eq!(config.redis_url, "redis://127.0.0.1:6379/0");
        } else {
            assert_eq!(config.namespace, "isolated_test");
            assert_eq!(config.http_addr.to_string(), "127.0.0.1:8081");
            assert_eq!(config.redis_url, "redis://127.0.0.1:6380/1");
        }
        return;
    }
    for case in ["defaults", "overrides"] {
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "config_env_defaults_overrides_and_redis_precedence",
            ])
            .env_clear()
            .env(CASE, case)
            .env("POSTGRES_URL", "postgres://127.0.0.1/test")
            .env("NATS_URL", "nats://127.0.0.1:4222")
            .env("REDIS_ADDR", "127.0.0.1:6379");
        if case == "overrides" {
            command
                .env("REDIS_URL", "redis://127.0.0.1:6380/1")
                .env("RELAYHUB_NAMESPACE", "isolated_test")
                .env("HTTP_ADDR", "127.0.0.1:8081");
        }
        let result = command.output().unwrap();
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stdout)
        );
    }
}

#[tokio::test]
async fn legacy_identity_socket_keeps_no_store() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://127.0.0.1:1/test")
        .unwrap();
    let router = relayhub::identity::Identity::new(pool).router();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let response = Client::builder()
        .no_proxy()
        .build()
        .unwrap()
        .get(format!("http://{address}/api/v1/socket"))
        .send()
        .await
        .unwrap();
    task.abort();
    assert!(!response.status().is_success());
    assert_eq!(
        response
            .headers()
            .get("cache-control")
            .and_then(|v| v.to_str().ok()),
        Some("no-store")
    );
}
