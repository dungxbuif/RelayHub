#[allow(dead_code)] // This task uses only PostgreSQL from the shared acceptance harness.
mod support;

use axum::http::StatusCode;
use base64::{Engine, engine::general_purpose::STANDARD};
use futures_util::StreamExt;
use relayhub::identity::{Action, AuthError, Identity, authorize};
use relayhub::protocol::Principal;
use reqwest::{Client, Response};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;
use support::TestServices;
use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};
use uuid::Uuid;

struct Harness {
    services: TestServices,
    identity: Identity,
    url: String,
    server: tokio::task::JoinHandle<()>,
    client: Client,
    admin: String,
}

impl Harness {
    async fn start() -> Self {
        let services = TestServices::start().await;
        let identity = Identity::with_secret_encryption_key(
            services.postgres.clone(),
            &STANDARD.encode([8u8; 32]),
        )
        .unwrap();
        identity.migrate().await.unwrap();
        let admin = format!("test-admin-{}", Uuid::new_v4());
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO identity_users (id, email, is_admin) VALUES ($1, $2, true)")
            .bind(id)
            .bind(format!("{id}@example.test"))
            .execute(&services.postgres)
            .await
            .unwrap();
        sqlx::query("INSERT INTO identity_sessions (id, user_id, token_hash, csrf_hash, expires_at) VALUES ($1, $2, $3, $4, now() + interval '24 hours')")
            .bind(Uuid::new_v4()).bind(id).bind(hash(&admin)).bind(hash("admin-csrf"))
            .execute(&services.postgres).await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let router = identity.router();
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        Self {
            services,
            identity,
            url,
            server,
            client: Client::new(),
            admin,
        }
    }

    fn pool(&self) -> &sqlx::PgPool {
        &self.services.postgres
    }

    async fn provision(&self, email: &str) -> Value {
        let response = self
            .client
            .post(format!("{}/api/v1/admin/users", self.url))
            .bearer_auth(&self.admin)
            .json(&json!({"email": email}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        response.json().await.unwrap()
    }

    async fn setup(&self, token: &str, password: &str) -> Response {
        self.client
            .post(format!("{}/api/v1/session/setup", self.url))
            .json(&json!({"token": token, "password": password}))
            .send()
            .await
            .unwrap()
    }

    async fn login(&self, email: &str) -> Response {
        self.client
            .post(format!("{}/api/v1/session", self.url))
            .json(&json!({"email": email, "password": "correct horse battery staple"}))
            .send()
            .await
            .unwrap()
    }

    async fn owner(&self, email: &str) -> (Uuid, String) {
        let user = self.provision(email).await;
        assert_eq!(
            self.setup(
                user["setup_token"].as_str().unwrap(),
                "correct horse battery staple"
            )
            .await
            .status(),
            204
        );
        let session: Value = self.login(email).await.json().await.unwrap();
        (
            uuid(&user, "workspace_id"),
            session["token"].as_str().unwrap().to_owned(),
        )
    }

    async fn app(&self, workspace: Uuid, token: &str) -> Uuid {
        let response = self
            .client
            .post(format!("{}/api/v1/workspaces/{workspace}/apps", self.url))
            .bearer_auth(token)
            .json(&json!({"name": "test app"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 201);
        uuid(&response.json().await.unwrap(), "id")
    }

    async fn key(&self, workspace: Uuid, app: Uuid, token: &str, kind: &str) -> Value {
        let response = self
            .client
            .post(format!(
                "{}/api/v1/workspaces/{workspace}/credentials",
                self.url
            ))
            .bearer_auth(token)
            .json(&json!({"app_id": app, "kind": kind}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 201);
        response.json().await.unwrap()
    }

    async fn count(&self, table: &str) -> i64 {
        let query = match table {
            "identity_users" => "SELECT count(*) FROM identity_users",
            "identity_workspaces" => "SELECT count(*) FROM identity_workspaces",
            "identity_setup_tokens" => "SELECT count(*) FROM identity_setup_tokens",
            "identity_apps" => "SELECT count(*) FROM identity_apps",
            "identity_credentials" => "SELECT count(*) FROM identity_credentials",
            _ => panic!("unknown test table"),
        };
        sqlx::query_scalar(query)
            .fetch_one(self.pool())
            .await
            .unwrap()
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
    }
}
fn hash(token: &str) -> Vec<u8> {
    Sha256::digest(token.as_bytes()).to_vec()
}
fn uuid(value: &Value, key: &str) -> Uuid {
    value[key].as_str().unwrap().parse().unwrap()
}

// Removing credential-derived workspace checks must expose a 201 and persisted foreign app.
#[tokio::test]
async fn cross_workspace_returns_403() {
    let h = Harness::start().await;
    let (a, owner_a) = h.owner("a@example.test").await;
    let (b, owner_b) = h.owner("b@example.test").await;
    let app_a = h.app(a, &owner_a).await;
    let key = h.key(a, app_a, &owner_a, "application").await;
    let before = h.count("identity_apps").await;
    for token in [&owner_a, key["token"].as_str().unwrap()] {
        let response = h
            .client
            .post(format!("{}/api/v1/workspaces/{b}/apps", h.url))
            .bearer_auth(token)
            .json(&json!({"name": "foreign"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 403);
    }
    assert_eq!(h.count("identity_apps").await, before);
    let app_b = h.app(b, &owner_b).await;
    let before = h.count("identity_credentials").await;
    let response = h
        .client
        .post(format!("{}/api/v1/workspaces/{a}/credentials", h.url))
        .bearer_auth(&owner_a)
        .json(&json!({"app_id": app_b, "kind": "agent"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 403);
    assert_eq!(h.count("identity_credentials").await, before);
    // A body workspace cannot override the authenticated/path workspace.
    let response = h
        .client
        .post(format!("{}/api/v1/workspaces/{a}/apps", h.url))
        .bearer_auth(&owner_a)
        .json(&json!({"name": "forged", "workspace_id": b}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 422);
    assert_eq!(h.count("identity_apps").await, 2);
}

// Missing periodic database revalidation must leave a socket alive after another instance revokes it.
#[tokio::test]
async fn disabled_key_disconnects_socket() {
    let h = Harness::start().await;
    let (workspace, owner) = h.owner("socket@example.test").await;
    let app = h.app(workspace, &owner).await;
    for kind in ["application", "agent"] {
        let key = h.key(workspace, app, &owner, kind).await;
        let token = key["token"].as_str().unwrap();
        let mut request = format!("{}/api/v1/socket", h.url.replace("http://", "ws://"))
            .into_client_request()
            .unwrap();
        request
            .headers_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        request
            .headers_mut()
            .insert("sec-websocket-protocol", "relayhub.v1".parse().unwrap());
        let (mut socket, response) = connect_async(request).await.unwrap();
        assert_eq!(response.headers()["sec-websocket-protocol"], "relayhub.v1");
        // Independently constructed identity has no in-memory socket registry from h.identity.
        let other = Identity::new(h.pool().clone());
        let principal = other.authenticate(&owner).await.unwrap();
        other
            .revoke_credential(&principal, workspace, uuid(&key, "id"))
            .await
            .unwrap();
        let message = tokio::time::timeout(std::time::Duration::from_secs(3), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            matches!(message, tokio_tungstenite::tungstenite::Message::Close(Some(frame)) if u16::from(frame.code) == 1008)
        );
        assert_eq!(
            h.identity.authenticate(token).await,
            Err(AuthError::Unauthorized)
        );
        let disabled: bool = sqlx::query_scalar(
            "SELECT revoked_at IS NOT NULL FROM identity_credentials WHERE id = $1",
        )
        .bind(uuid(&key, "id"))
        .fetch_one(h.pool())
        .await
        .unwrap();
        assert!(disabled);
    }
}

// Non-atomic consumption must allow concurrent setup/replay or replace the password twice.
#[tokio::test]
async fn password_setup_token_is_single_use() {
    let h = Harness::start().await;
    let user = h.provision("setup@example.test").await;
    let token = user["setup_token"].as_str().unwrap();
    assert_eq!(user["expires_in_seconds"], 86400);
    let row = sqlx::query("SELECT token_hash, extract(epoch from (expires_at - created_at))::bigint AS ttl FROM identity_setup_tokens WHERE user_id = $1")
        .bind(uuid(&user, "user_id")).fetch_one(h.pool()).await.unwrap();
    assert_eq!(row.get::<Vec<u8>, _>("token_hash"), hash(token));
    assert_ne!(row.get::<Vec<u8>, _>("token_hash"), token.as_bytes());
    assert_eq!(row.get::<i64, _>("ttl"), 86400);
    assert_eq!(h.setup(token, "short").await.status(), 422);
    let (a, b) = tokio::join!(
        h.setup(token, "correct horse battery staple"),
        h.setup(token, "correct horse battery staple")
    );
    let mut statuses = [a.status().as_u16(), b.status().as_u16()];
    statuses.sort();
    assert_eq!(statuses, [204, 401]);
    let password: String =
        sqlx::query_scalar("SELECT password_hash FROM identity_users WHERE id = $1")
            .bind(uuid(&user, "user_id"))
            .fetch_one(h.pool())
            .await
            .unwrap();
    assert!(password.starts_with("$argon2id$"));
    assert_ne!(password, "correct horse battery staple");
    assert_eq!(
        h.setup(token, "attacker replacement password")
            .await
            .status(),
        401
    );
    let after: String =
        sqlx::query_scalar("SELECT password_hash FROM identity_users WHERE id = $1")
            .bind(uuid(&user, "user_id"))
            .fetch_one(h.pool())
            .await
            .unwrap();
    assert_eq!(password, after);
    let consumed: bool = sqlx::query_scalar(
        "SELECT consumed_at IS NOT NULL FROM identity_setup_tokens WHERE user_id = $1",
    )
    .bind(uuid(&user, "user_id"))
    .fetch_one(h.pool())
    .await
    .unwrap();
    assert!(consumed);
    assert_eq!(h.login("setup@example.test").await.status(), 200);
}

// Omitting the expiry predicate must set a password after the exact 24-hour boundary.
#[tokio::test]
async fn expired_setup_token_rejected() {
    let h = Harness::start().await;
    let user = h.provision("expired@example.test").await;
    sqlx::query("UPDATE identity_setup_tokens SET created_at = now() - interval '24 hours', expires_at = now() WHERE user_id = $1")
        .bind(uuid(&user, "user_id")).execute(h.pool()).await.unwrap();
    assert_eq!(
        h.setup(
            user["setup_token"].as_str().unwrap(),
            "correct horse battery staple"
        )
        .await
        .status(),
        401
    );
    let state: (bool, bool) = sqlx::query_as("SELECT u.password_hash IS NULL, t.consumed_at IS NULL FROM identity_users u JOIN identity_setup_tokens t ON t.user_id = u.id WHERE u.id = $1")
        .bind(uuid(&user, "user_id")).fetch_one(h.pool()).await.unwrap();
    assert_eq!(state, (true, true));
    assert_eq!(h.login("expired@example.test").await.status(), 401);
}

// Skipping CSRF on any cookie-authenticated mutation must persist an app/key/revocation.
#[tokio::test]
async fn csrf_blocks_cookie_write() {
    let h = Harness::start().await;
    let (workspace, owner) = h.owner("csrf@example.test").await;
    let login = h.login("csrf@example.test").await;
    let cookie = login.headers()["set-cookie"].to_str().unwrap().to_owned();
    assert!(cookie.starts_with("__Host-relayhub_session="));
    for attribute in ["Secure", "HttpOnly", "SameSite=Strict", "Path=/"] {
        assert!(cookie.contains(attribute));
    }
    assert!(!cookie.contains("Domain="));
    let session: Value = login.json().await.unwrap();
    let cookie = cookie.split(';').next().unwrap();
    let url = format!("{}/api/v1/workspaces/{workspace}/apps", h.url);
    let before = h.count("identity_apps").await;
    for csrf in [None, Some("wrong")] {
        let mut request = h
            .client
            .post(&url)
            .header("cookie", cookie)
            .json(&json!({"name": "csrf app"}));
        if let Some(csrf) = csrf {
            request = request.header("x-csrf-token", csrf);
        }
        assert_eq!(request.send().await.unwrap().status(), 403);
    }
    assert_eq!(h.count("identity_apps").await, before);
    assert_eq!(
        h.client
            .post(&url)
            .header("cookie", cookie)
            .header("x-csrf-token", session["csrf_token"].as_str().unwrap())
            .json(&json!({"name": "valid"}))
            .send()
            .await
            .unwrap()
            .status(),
        201
    );
    assert_eq!(h.count("identity_apps").await, before + 1);
    let app = h.app(workspace, &owner).await;
    let key = h.key(workspace, app, &owner, "application").await;
    assert_eq!(
        h.client
            .post(format!(
                "{}/api/v1/workspaces/{workspace}/credentials",
                h.url
            ))
            .header("cookie", cookie)
            .json(&json!({"app_id": app, "kind": "application"}))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        h.client
            .delete(format!(
                "{}/api/v1/workspaces/{workspace}/credentials/{}",
                h.url,
                key["id"].as_str().unwrap()
            ))
            .header("cookie", cookie)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert!(
        h.identity
            .authenticate(key["token"].as_str().unwrap())
            .await
            .is_ok()
    );
    assert_eq!(
        h.client
            .delete(format!("{}/api/v1/session", h.url))
            .header("cookie", cookie)
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert!(
        h.identity
            .authenticate(session["token"].as_str().unwrap())
            .await
            .is_ok()
    );
    assert_eq!(
        h.client
            .post(format!("{}/api/v1/admin/users", h.url))
            .header("cookie", format!("__Host-relayhub_session={}", h.admin))
            .json(&json!({"email": "forged@example.test"}))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(h.count("identity_users").await, 2);
    assert_eq!(
        h.client
            .delete(format!("{}/api/v1/session", h.url))
            .header("cookie", cookie)
            .header("x-csrf-token", session["csrf_token"].as_str().unwrap())
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    assert_eq!(
        h.identity
            .authenticate(session["token"].as_str().unwrap())
            .await,
        Err(AuthError::Unauthorized)
    );
}

// Public/non-admin provisioning and partial transactions must never leave a user/workspace/token.
#[tokio::test]
async fn provisioning_is_admin_only_and_atomic() {
    let h = Harness::start().await;
    let url = format!("{}/api/v1/admin/users", h.url);
    assert_eq!(
        h.client
            .post(&url)
            .json(&json!({"email": "public@example.test"}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let (_, owner) = h.owner("owner@example.test").await;
    assert_eq!(
        h.client
            .post(&url)
            .bearer_auth(owner)
            .json(&json!({"email": "public@example.test"}))
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        h.client
            .post(&url)
            .bearer_auth(&h.admin)
            .json(&json!({"email": "OWNER@example.test"}))
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    assert_eq!(
        h.client
            .post(&url)
            .bearer_auth(&h.admin)
            .json(&json!({"email": "bad", "is_admin": true}))
            .send()
            .await
            .unwrap()
            .status(),
        422
    );
    assert_eq!(
        h.client
            .post(format!("{}/api/v1/signup", h.url))
            .json(&json!({"email": "public@example.test"}))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    assert_eq!(h.count("identity_users").await, 2);
    assert_eq!(h.count("identity_workspaces").await, 1);
    assert_eq!(h.count("identity_setup_tokens").await, 1);
    // Force the second insert to fail in the real database; the first must roll back.
    sqlx::raw_sql("CREATE FUNCTION reject_workspace() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test workspace failure'; END $$; CREATE TRIGGER reject_workspace BEFORE INSERT ON identity_workspaces FOR EACH ROW EXECUTE FUNCTION reject_workspace();")
        .execute(h.pool()).await.unwrap();
    assert_eq!(
        h.client
            .post(&url)
            .bearer_auth(&h.admin)
            .json(&json!({"email": "rollback@example.test"}))
            .send()
            .await
            .unwrap()
            .status(),
        500
    );
    assert_eq!(h.count("identity_users").await, 2);
    assert_eq!(h.count("identity_workspaces").await, 1);
    assert_eq!(h.count("identity_setup_tokens").await, 1);
}

// A credential kind/app chosen in a request must never grant administrative or foreign app access.
#[tokio::test]
async fn principals_and_scopes_cannot_escalate() {
    let h = Harness::start().await;
    let (workspace, owner) = h.owner("scopes@example.test").await;
    let app = h.app(workspace, &owner).await;
    assert!(matches!(
        h.identity.authenticate(&h.admin).await.unwrap(),
        Principal::SystemAdmin { .. }
    ));
    assert!(
        matches!(h.identity.authenticate(&owner).await.unwrap(), Principal::WorkspaceOwner { workspace_id, .. } if workspace_id == workspace)
    );
    for kind in ["application", "agent"] {
        let key = h.key(workspace, app, &owner, kind).await;
        let token = key["token"].as_str().unwrap();
        let principal = h.identity.authenticate(token).await.unwrap();
        match &principal {
            Principal::Application {
                workspace_id,
                app_id,
                key_id,
            } if kind == "application" => {
                assert_eq!(
                    (*workspace_id, *app_id, *key_id),
                    (workspace, app, uuid(&key, "id"))
                );
            }
            Principal::Agent {
                workspace_id,
                app_id,
                agent_id,
                key_id,
            } if kind == "agent" => {
                assert_eq!(
                    (*workspace_id, *app_id, *key_id),
                    (workspace, app, uuid(&key, "id"))
                );
                assert_eq!(*agent_id, uuid(&key, "agent_id"));
            }
            _ => panic!("wrong principal"),
        }
        assert_eq!(
            authorize(&principal, Uuid::new_v4(), Action::Connect { app_id: app }),
            Err(AuthError::Forbidden)
        );
        assert_eq!(
            authorize(&principal, workspace, Action::Connect { app_id: app }),
            Ok(())
        );
        assert_eq!(
            authorize(
                &principal,
                workspace,
                Action::Connect {
                    app_id: Uuid::new_v4()
                }
            ),
            Err(AuthError::Forbidden)
        );
        assert_eq!(
            authorize(&principal, workspace, Action::ManageCredentials),
            Err(AuthError::Forbidden)
        );
        assert_eq!(
            h.client
                .post(format!("{}/api/v1/admin/users", h.url))
                .bearer_auth(token)
                .json(&json!({"email": "no@example.test"}))
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
        assert_eq!(
            h.client
                .post(format!(
                    "{}/api/v1/workspaces/{workspace}/credentials",
                    h.url
                ))
                .bearer_auth(token)
                .json(&json!({"app_id": app, "kind": "application"}))
                .send()
                .await
                .unwrap()
                .status(),
            403
        );
        let stored: Vec<u8> =
            sqlx::query_scalar("SELECT token_hash FROM identity_credentials WHERE id = $1")
                .bind(uuid(&key, "id"))
                .fetch_one(h.pool())
                .await
                .unwrap();
        assert_eq!(stored, hash(token));
        assert_ne!(stored, token.as_bytes());
    }
    let before = h.count("identity_credentials").await;
    for kind in ["system_admin", "workspace_owner"] {
        assert_eq!(
            h.client
                .post(format!(
                    "{}/api/v1/workspaces/{workspace}/credentials",
                    h.url
                ))
                .bearer_auth(&owner)
                .json(&json!({"app_id": app, "kind": kind}))
                .send()
                .await
                .unwrap()
                .status(),
            422
        );
    }
    assert_eq!(h.count("identity_credentials").await, before);
}

// Missing revocation/expiry/session-only checks must permit a credential to act as a dashboard session.
#[tokio::test]
async fn sessions_are_hashed_expiring_and_revocable() {
    let h = Harness::start().await;
    let (workspace, owner) = h.owner("session@example.test").await;
    let response = h
        .client
        .get(format!("{}/api/v1/session", h.url))
        .bearer_auth(&owner)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let principal: Principal = response.json().await.unwrap();
    assert!(
        matches!(principal, Principal::WorkspaceOwner { workspace_id, .. } if workspace_id == workspace)
    );
    assert_eq!(
        h.client
            .post(format!("{}/api/v1/session", h.url))
            .json(&json!({"email": "session@example.test", "password": "wrong password"}))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    let stored: Vec<u8> =
        sqlx::query_scalar("SELECT token_hash FROM identity_sessions WHERE token_hash = $1")
            .bind(hash(&owner))
            .fetch_one(h.pool())
            .await
            .unwrap();
    assert_ne!(stored, owner.as_bytes());
    let app = h.app(workspace, &owner).await;
    let key = h.key(workspace, app, &owner, "application").await;
    assert_eq!(
        h.client
            .get(format!("{}/api/v1/session", h.url))
            .bearer_auth(key["token"].as_str().unwrap())
            .send()
            .await
            .unwrap()
            .status(),
        403
    );
    assert_eq!(
        h.client
            .delete(format!("{}/api/v1/session", h.url))
            .bearer_auth(&owner)
            .send()
            .await
            .unwrap()
            .status(),
        204
    );
    assert_eq!(
        h.identity.authenticate(&owner).await,
        Err(AuthError::Unauthorized)
    );
    let revoked: bool = sqlx::query_scalar(
        "SELECT revoked_at IS NOT NULL FROM identity_sessions WHERE token_hash = $1",
    )
    .bind(hash(&owner))
    .fetch_one(h.pool())
    .await
    .unwrap();
    assert!(revoked);
    let session: Value = h.login("session@example.test").await.json().await.unwrap();
    let token = session["token"].as_str().unwrap();
    sqlx::query("UPDATE identity_sessions SET expires_at = now() WHERE token_hash = $1")
        .bind(hash(token))
        .execute(h.pool())
        .await
        .unwrap();
    assert_eq!(
        h.identity.authenticate(token).await,
        Err(AuthError::Unauthorized)
    );
    assert_eq!(
        h.identity.authenticate("not-a-token").await,
        Err(AuthError::Unauthorized)
    );
}

// Excess login verification is rejected, while an unknown user still follows the dummy-hash path.
#[tokio::test]
async fn login_overload_is_rejected_without_skipping_dummy_hash() {
    let h = Harness::start().await;
    let requests = (0..32).map(|index| {
        let client = h.client.clone();
        let url = format!("{}/api/v1/session", h.url);
        async move {
            client
                .post(url)
                .json(&json!({
                    "email": format!("unknown-{index}@example.test"),
                    "password": "wrong password"
                }))
                .send()
                .await
                .unwrap()
                .status()
        }
    });
    let statuses = futures_util::future::join_all(requests).await;
    assert!(
        statuses
            .iter()
            .any(|status| *status == StatusCode::TOO_MANY_REQUESTS),
        "expected excess verification attempts to receive 429, got {statuses:?}"
    );
    assert!(
        statuses
            .iter()
            .any(|status| *status == StatusCode::UNAUTHORIZED),
        "unknown users must still complete the dummy-hash path, got {statuses:?}"
    );
}

// Absent/incorrect OpenAPI security and schemas would let clients send a workspace authority field.
#[tokio::test]
async fn openapi_describes_identity_routes_and_security() {
    let document = relayhub::identity::openapi();
    assert_eq!(document["openapi"], "3.1.0");
    for path in [
        "/api/v1/admin/users",
        "/api/v1/session",
        "/api/v1/session/setup",
        "/api/v1/workspaces/{id}/apps",
        "/api/v1/workspaces/{id}/credentials",
        "/api/v1/workspaces/{id}/credentials/{credential_id}",
    ] {
        assert!(document["paths"].get(path).is_some(), "missing {path}");
    }
    let create = &document["paths"]["/api/v1/workspaces/{id}/apps"]["post"];
    assert!(create["responses"].get("403").is_some());
    assert!(
        create["security"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s.get("csrfToken").is_some() && s.get("browserSession").is_some())
    );
    assert_eq!(
        document["components"]["schemas"]["CreateApp"]["additionalProperties"],
        false
    );
    assert!(
        document["components"]["schemas"]["CreateApp"]["properties"]
            .get("workspace_id")
            .is_none()
    );
    assert_eq!(
        document["components"]["securitySchemes"]["browserSession"]["name"],
        "__Host-relayhub_session"
    );
}

#[test]
fn openapi_operation_ids_are_unique() {
    let document = relayhub::identity::openapi();
    let mut ids = std::collections::HashSet::new();
    for item in document["paths"].as_object().unwrap().values() {
        for operation in item.as_object().unwrap().values() {
            let id = operation["operationId"].as_str().unwrap();
            assert!(ids.insert(id), "duplicate operationId {id}");
        }
    }
}

#[tokio::test]
async fn migration_status_is_admin_only_and_reports_versions_without_secrets() {
    let h = Harness::start().await;
    let response = h
        .client
        .get(format!("{}/api/v1/admin/migration-status", h.url))
        .bearer_auth(&h.admin)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["service"], "relayhub-rust");
    assert_eq!(body["schema"]["identity"], 17);
    assert_eq!(body["data_migration"]["state"], "not_started");
    assert!(body.get("password").is_none());
    assert!(body.get("database_url").is_none());

    let unauthenticated = h
        .client
        .get(format!("{}/api/v1/admin/migration-status", h.url))
        .send()
        .await
        .unwrap();
    assert_eq!(unauthenticated.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn full_admin_session_uses_the_rust_identity_and_admin_cookie_contract() {
    let h = Harness::start().await;
    let email = format!("admin-{}@example.test", Uuid::new_v4());
    let user = h.provision(&email).await;
    assert_eq!(
        h.setup(
            user["setup_token"].as_str().unwrap(),
            "correct horse battery staple"
        )
        .await
        .status(),
        StatusCode::NO_CONTENT
    );
    sqlx::query("UPDATE identity_users SET is_admin=true WHERE id=$1")
        .bind(uuid(&user, "user_id"))
        .execute(h.pool())
        .await
        .unwrap();

    let response = h
        .client
        .post(format!("{}/api/v1/admin/session", h.url))
        .json(&json!({
            "email": email,
            "password": "correct horse battery staple"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let cookie = response
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    assert!(cookie.starts_with("__Host-relayhub_admin="));
    let body: Value = response.json().await.unwrap();
    assert_eq!(body["user"]["email"], email);
    assert_eq!(body["user"]["role"], "admin");
    assert!(
        body["csrf_token"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );

    let current = h
        .client
        .get(format!("{}/api/v1/admin/session", h.url))
        .header("cookie", cookie.split(';').next().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(current.status(), StatusCode::OK);
    let current: Value = current.json().await.unwrap();
    assert_eq!(current["user"]["email"], email);
}

#[tokio::test]
async fn system_admin_can_manage_full_admin_user_lifecycle_without_password_leakage() {
    let h = Harness::start().await;
    let cookie = format!("__Host-relayhub_admin={}", h.admin);

    let list = h
        .client
        .get(format!("{}/api/v1/admin/users", h.url))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::OK);
    let listed: Value = list.json().await.unwrap();
    assert!(listed.as_array().is_some_and(|users| !users.is_empty()));
    assert!(listed.to_string().find("password").is_none());

    let email = format!("managed-{}@example.test", Uuid::new_v4());
    let created = h
        .client
        .post(format!("{}/api/v1/admin/users", h.url))
        .header("cookie", &cookie)
        .header("x-csrf-token", "admin-csrf")
        .json(&json!({
            "email": email,
            "password": "managed user password",
            "role": "user"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let created: Value = created.json().await.unwrap();
    let user_id = created["id"].as_str().unwrap().to_owned();
    assert_eq!(created["role"], "user");
    assert!(created.get("password").is_none());

    let disabled = h
        .client
        .patch(format!("{}/api/v1/admin/users/{user_id}", h.url))
        .header("cookie", &cookie)
        .header("x-csrf-token", "admin-csrf")
        .json(&json!({"enabled": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(disabled.status(), StatusCode::OK);
    assert_eq!(disabled.json::<Value>().await.unwrap()["enabled"], false);

    let rotated = h
        .client
        .post(format!("{}/api/v1/admin/users/{user_id}/password", h.url))
        .header("cookie", &cookie)
        .header("x-csrf-token", "admin-csrf")
        .json(&json!({"password": "rotated user password"}))
        .send()
        .await
        .unwrap();
    assert_eq!(rotated.status(), StatusCode::OK);
    assert!(rotated.text().await.unwrap().find("password").is_none());
}

#[tokio::test]
async fn full_admin_can_manage_application_inventory_and_one_shot_credentials() {
    let h = Harness::start().await;
    let cookie = format!("__Host-relayhub_admin={}", h.admin);
    let list = h
        .client
        .get(format!("{}/api/v1/apps", h.url))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(list.status(), StatusCode::OK);

    let created = h
        .client
        .post(format!("{}/api/v1/apps", h.url))
        .header("cookie", &cookie)
        .header("x-csrf-token", "admin-csrf")
        .json(&json!({
            "name": "Admin managed app",
            "delivery_mode": "queue",
            "callback_url": null
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let credentials: Value = created.json().await.unwrap();
    let app_id = credentials["app_id"].as_str().unwrap().to_owned();
    assert!(
        credentials["api_key"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );
    assert!(
        credentials["hmac_secret"]
            .as_str()
            .is_some_and(|value| !value.is_empty())
    );

    let updated = h
        .client
        .patch(format!("{}/api/v1/admin/apps/{app_id}", h.url))
        .header("cookie", &cookie)
        .header("x-csrf-token", "admin-csrf")
        .json(&json!({"name": "Renamed app", "delivery_mode": "all"}))
        .send()
        .await
        .unwrap();
    assert_eq!(updated.status(), StatusCode::OK);
    assert_eq!(
        updated.json::<Value>().await.unwrap()["name"],
        "Renamed app"
    );

    let rotated = h
        .client
        .post(format!("{}/api/v1/apps/{app_id}/rotate-secret", h.url))
        .header("cookie", &cookie)
        .header("x-csrf-token", "admin-csrf")
        .send()
        .await
        .unwrap();
    assert_eq!(rotated.status(), StatusCode::OK);
    let rotated: Value = rotated.json().await.unwrap();
    assert_eq!(rotated["app_id"], app_id);
    assert_ne!(rotated["api_key"], credentials["api_key"]);

    let disabled = h
        .client
        .delete(format!("{}/api/v1/apps/{app_id}", h.url))
        .header("cookie", &cookie)
        .header("x-csrf-token", "admin-csrf")
        .send()
        .await
        .unwrap();
    assert_eq!(disabled.status(), StatusCode::OK);
    assert_eq!(disabled.json::<Value>().await.unwrap()["enabled"], false);
}

#[tokio::test]
async fn full_admin_can_manage_functions_and_routing_rules() {
    let h = Harness::start().await;
    let cookie = format!("__Host-relayhub_admin={}", h.admin);
    let created = h
        .client
        .post(format!("{}/api/v1/apps", h.url))
        .header("cookie", &cookie)
        .header("x-relayhub-csrf", "admin-csrf")
        .json(&json!({"name": "Control plane app", "delivery_mode": "all"}))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let app_id = uuid(&created.json::<Value>().await.unwrap(), "app_id");

    let function = h
        .client
        .post(format!("{}/api/v1/admin/apps/{app_id}/functions", h.url))
        .header("cookie", &cookie)
        .header("x-relayhub-csrf", "admin-csrf")
        .json(&json!({"name": "normalize", "timeout_seconds": 5}))
        .send()
        .await
        .unwrap();
    assert_eq!(function.status(), StatusCode::CREATED);
    let function: Value = function.json().await.unwrap();
    assert_eq!(function["name"], "normalize");
    let function_id = uuid(&function, "id");

    let listed = h
        .client
        .get(format!("{}/api/v1/admin/apps/{app_id}/functions", h.url))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(listed.status(), StatusCode::OK);
    assert_eq!(
        listed
            .json::<Value>()
            .await
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );

    let toggled = h
        .client
        .patch(format!(
            "{}/api/v1/admin/apps/{app_id}/functions/{function_id}",
            h.url
        ))
        .header("cookie", &cookie)
        .header("x-relayhub-csrf", "admin-csrf")
        .json(&json!({"enabled": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(toggled.status(), StatusCode::OK);
    assert_eq!(toggled.json::<Value>().await.unwrap()["enabled"], false);

    let rule = h
        .client
        .post(format!("{}/api/v1/routing/rules", h.url))
        .header("cookie", &cookie)
        .header("x-relayhub-csrf", "admin-csrf")
        .json(&json!({"event_type": "order.created", "target_app_id": app_id}))
        .send()
        .await
        .unwrap();
    assert_eq!(rule.status(), StatusCode::CREATED);
    let rule: Value = rule.json().await.unwrap();
    assert_eq!(rule["event_type"], "order.created");
    let rules = h
        .client
        .get(format!("{}/api/v1/routing/rules", h.url))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(rules.status(), StatusCode::OK);
    assert_eq!(
        rules
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
async fn read_only_workspace_user_cannot_mutate_admin_control_plane() {
    let h = Harness::start().await;
    let (workspace, owner) = h.owner("readonly@example.test").await;
    let app = h.app(workspace, &owner).await;
    let login = h
        .client
        .post(format!("{}/api/v1/admin/session", h.url))
        .json(&json!({
            "email": "readonly@example.test",
            "password": "correct horse battery staple"
        }))
        .send()
        .await
        .unwrap();
    assert_eq!(login.status(), StatusCode::OK);
    let cookie = login
        .headers()
        .get("set-cookie")
        .unwrap()
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let csrf = login.json::<Value>().await.unwrap()["csrf_token"]
        .as_str()
        .unwrap()
        .to_owned();
    let response = h
        .client
        .post(format!("{}/api/v1/admin/apps/{app}/functions", h.url))
        .header("cookie", cookie)
        .header("x-relayhub-csrf", csrf)
        .json(&json!({"name":"read-only-must-fail","timeout_seconds":5}))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
}

#[tokio::test]
async fn full_admin_can_manage_queue_configuration_without_fake_delivery_state() {
    let h = Harness::start().await;
    let cookie = format!("__Host-relayhub_admin={}", h.admin);
    let created = h
        .client
        .post(format!("{}/api/v1/apps", h.url))
        .header("cookie", &cookie)
        .header("x-relayhub-csrf", "admin-csrf")
        .json(&json!({"name": "Queue app", "delivery_mode": "queue"}))
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), StatusCode::CREATED);
    let app_id = uuid(&created.json::<Value>().await.unwrap(), "app_id");

    let subscription = h
        .client
        .post(format!(
            "{}/api/v1/admin/apps/{app_id}/subscriptions",
            h.url
        ))
        .header("cookie", &cookie)
        .header("x-relayhub-csrf", "admin-csrf")
        .json(&json!({"name": "worker", "max_in_flight": 5, "max_batch_size": 3}))
        .send()
        .await
        .unwrap();
    assert_eq!(subscription.status(), StatusCode::CREATED);
    let subscription: Value = subscription.json().await.unwrap();
    assert_eq!(subscription["name"], "worker");
    let subscription_id = uuid(&subscription, "id");

    let schedule = h
        .client
        .post(format!(
            "{}/api/v1/admin/apps/{app_id}/subscriptions/{subscription_id}/schedules",
            h.url
        ))
        .header("cookie", &cookie)
        .header("x-relayhub-csrf", "admin-csrf")
        .json(&json!({"name":"hourly","cron_expression":"0 * * * *","timezone":"UTC","event_type":"tick","data":{}}))
        .send()
        .await
        .unwrap();
    assert_eq!(schedule.status(), StatusCode::CREATED);

    let metrics = h
        .client
        .get(format!(
            "{}/api/v1/admin/apps/{app_id}/subscriptions/{subscription_id}/metrics",
            h.url
        ))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(metrics.status(), StatusCode::OK);
    let metrics: Value = metrics.json().await.unwrap();
    assert_eq!(metrics["available"], 0);
    assert_eq!(
        metrics["degraded_components"][0],
        "queue_worker_unavailable"
    );
}

#[tokio::test]
async fn migrated_go_password_hash_can_login_and_is_upgraded_after_authentication() {
    let h = Harness::start().await;
    let id = Uuid::new_v4();
    let email = format!("legacy-{}@example.test", Uuid::new_v4());
    let legacy_hash =
        "pbkdf2-sha256$210000$MDEyMzQ1Njc4OWFiY2RlZg$H_ZMdkjDEDpZ4T7DlMYQs7RkF9UgyowFSsi_pzKxi-s";
    sqlx::query("INSERT INTO identity_users (id,email,is_admin,legacy_password_hash,enabled) VALUES ($1,$2,true,$3,true)")
        .bind(id)
        .bind(&email)
        .bind(legacy_hash)
        .execute(h.pool())
        .await
        .unwrap();

    let response = h
        .client
        .post(format!("{}/api/v1/admin/session", h.url))
        .json(&json!({"email": email, "password": "correct horse battery staple"}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let argon_hash: Option<String> =
        sqlx::query_scalar("SELECT password_hash FROM identity_users WHERE id=$1")
            .bind(id)
            .fetch_one(h.pool())
            .await
            .unwrap();
    assert!(argon_hash.is_some_and(|hash| hash.starts_with("$argon2")));
    let remaining: Option<String> =
        sqlx::query_scalar("SELECT legacy_password_hash FROM identity_users WHERE id=$1")
            .bind(id)
            .fetch_one(h.pool())
            .await
            .unwrap();
    assert!(remaining.is_none());
}

#[tokio::test]
async fn full_admin_read_surface_reports_rust_dashboard_and_event_state() {
    let h = Harness::start().await;
    let cookie = format!("__Host-relayhub_admin={}", h.admin);
    for path in [
        "/api/v1/admin/dashboard",
        "/api/v1/admin/events?limit=25",
        "/api/v1/admin/dlq?limit=25",
        "/api/v1/admin/audit?limit=25",
    ] {
        let response = h
            .client
            .get(format!("{}{}", h.url, path))
            .header("cookie", &cookie)
            .send()
            .await
            .unwrap();
        let status = response.status();
        let raw = response.text().await.unwrap();
        assert_eq!(status, StatusCode::OK, "{path}: {raw}");
        let body: Value = serde_json::from_str(&raw).unwrap();
        if path.ends_with("dashboard") {
            assert!(body.get("durable").is_some());
            assert!(body.get("series").is_some());
            assert!(body.get("instances").is_some());
        } else {
            assert!(body.get("items").is_some());
        }
    }
}
