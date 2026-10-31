//! PostgreSQL-backed identity. Only authenticate() may turn credentials into principals.
use crate::protocol::{Principal, SETUP_TOKEN_TTL_SECONDS, SUBPROTOCOL};
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier, password_hash::SaltString};
use axum::{
    Json, Router,
    extract::{
        Path, State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, patch, post},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use pbkdf2::pbkdf2_hmac;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;
use std::{sync::Arc, time::Duration};
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;
use uuid::Uuid;

const SESSION_SECONDS: u64 = 86400;
const COOKIE: &str = "__Host-relayhub_session";
const ADMIN_COOKIE: &str = "__Host-relayhub_admin";
const PASSWORD_VERIFY_CONCURRENCY: usize = 4;

#[derive(Clone)]
pub struct Identity {
    pool: sqlx::PgPool,
    password_verification: Arc<Semaphore>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthError {
    Unauthorized,
    Forbidden,
    TooManyRequests,
    InvalidInput,
    Conflict,
    Internal,
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let (status, error) = match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "unauthorized"),
            Self::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
            Self::TooManyRequests => (StatusCode::TOO_MANY_REQUESTS, "too_many_requests"),
            Self::InvalidInput => (StatusCode::UNPROCESSABLE_ENTITY, "invalid_input"),
            Self::Conflict => (StatusCode::CONFLICT, "conflict"),
            Self::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
        };
        (status, Json(json!({"error": error}))).into_response()
    }
}
impl std::fmt::Display for AuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for AuthError {}
impl From<sqlx::Error> for AuthError {
    fn from(error: sqlx::Error) -> Self {
        if error
            .as_database_error()
            .is_some_and(|e| e.is_unique_violation())
        {
            Self::Conflict
        } else {
            Self::Internal
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Action {
    ManageApps,
    ManageCredentials,
    Connect { app_id: Uuid },
    Publish { app_id: Uuid },
    Deliver { app_id: Uuid },
}

/// Call with the server-authenticated principal, never a principal deserialized from a request.
pub fn authorize(
    principal: &Principal,
    workspace_id: Uuid,
    action: Action,
) -> Result<(), AuthError> {
    let allowed = match principal {
        Principal::SystemAdmin { .. } => true,
        Principal::WorkspaceOwner {
            workspace_id: own, ..
        } => *own == workspace_id,
        Principal::Application {
            workspace_id: own,
            app_id,
            ..
        } => {
            *own == workspace_id
                && matches!(action, Action::Connect { app_id: target } | Action::Publish { app_id: target } if target == *app_id)
        }
        Principal::Agent {
            workspace_id: own,
            app_id,
            ..
        } => {
            *own == workspace_id
                && matches!(action, Action::Connect { app_id: target } | Action::Deliver { app_id: target } if target == *app_id)
        }
    };
    if allowed {
        Ok(())
    } else {
        Err(AuthError::Forbidden)
    }
}

fn digest(value: &str) -> Vec<u8> {
    Sha256::digest(value.as_bytes()).to_vec()
}
fn secret() -> String {
    // Two OS-random UUIDv4 values supply 244 random bits; no plaintext is persisted.
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}
fn session_principal(principal: &Principal) -> Result<Uuid, AuthError> {
    match principal {
        Principal::SystemAdmin { user_id } | Principal::WorkspaceOwner { user_id, .. } => {
            Ok(*user_id)
        }
        _ => Err(AuthError::Forbidden),
    }
}
fn email(value: &str) -> Result<String, AuthError> {
    let value = value.trim().to_lowercase();
    let Some((local, domain)) = value.split_once('@') else {
        return Err(AuthError::InvalidInput);
    };
    if value.len() > 254
        || local.is_empty()
        || domain.is_empty()
        || domain.contains('@')
        || value.chars().any(char::is_whitespace)
        || value.chars().any(char::is_control)
    {
        return Err(AuthError::InvalidInput);
    }
    Ok(value)
}

impl Identity {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self {
            pool,
            password_verification: Arc::new(Semaphore::new(PASSWORD_VERIFY_CONCURRENCY)),
        }
    }

    pub(crate) fn pool(&self) -> sqlx::PgPool {
        self.pool.clone()
    }

    pub(crate) async fn admin_browser_auth(
        &self,
        headers: &HeaderMap,
        write: bool,
    ) -> Result<Principal, AuthError> {
        let (principal, _) = self
            .request_cookie_auth(headers, write, ADMIN_COOKIE)
            .await?;
        session_principal(&principal)?;
        Ok(principal)
    }

    /// Run once at service startup, before accepting requests. Safe across replicas.
    pub async fn migrate(&self) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(727365011)")
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS identity_schema_migrations (version integer PRIMARY KEY)",
        )
        .execute(&mut *tx)
        .await?;
        let applied: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM identity_schema_migrations WHERE version = 1)",
        )
        .fetch_one(&mut *tx)
        .await?;
        if !applied {
            sqlx::raw_sql(include_str!("../migrations/0001_identity.sql"))
                .execute(&mut *tx)
                .await?;
            sqlx::query("INSERT INTO identity_schema_migrations VALUES (1)")
                .execute(&mut *tx)
                .await?;
        }
        let admin_applied: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM identity_schema_migrations WHERE version = 5)",
        )
        .fetch_one(&mut *tx)
        .await?;
        if !admin_applied {
            sqlx::raw_sql(include_str!("../migrations/0005_admin_console.sql"))
                .execute(&mut *tx)
                .await?;
            sqlx::query("INSERT INTO identity_schema_migrations VALUES (5)")
                .execute(&mut *tx)
                .await?;
        }
        let apps_applied: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM identity_schema_migrations WHERE version = 6)",
        )
        .fetch_one(&mut *tx)
        .await?;
        if !apps_applied {
            sqlx::raw_sql(include_str!("../migrations/0006_admin_apps.sql"))
                .execute(&mut *tx)
                .await?;
            sqlx::query("INSERT INTO identity_schema_migrations VALUES (6)")
                .execute(&mut *tx)
                .await?;
        }
        let migration_applied: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM identity_schema_migrations WHERE version = 7)",
        )
        .fetch_one(&mut *tx)
        .await?;
        if !migration_applied {
            sqlx::raw_sql(include_str!("../migrations/0007_migration_ledger.sql"))
                .execute(&mut *tx)
                .await?;
            sqlx::query("INSERT INTO identity_schema_migrations VALUES (7)")
                .execute(&mut *tx)
                .await?;
        }
        let audit_applied: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM identity_schema_migrations WHERE version = 8)",
        )
        .fetch_one(&mut *tx)
        .await?;
        if !audit_applied {
            sqlx::raw_sql(include_str!("../migrations/0008_admin_audit.sql"))
                .execute(&mut *tx)
                .await?;
            sqlx::query("INSERT INTO identity_schema_migrations VALUES (8)")
                .execute(&mut *tx)
                .await?;
        }
        let control_plane_applied: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM identity_schema_migrations WHERE version = 9)",
        )
        .fetch_one(&mut *tx)
        .await?;
        if !control_plane_applied {
            sqlx::raw_sql(include_str!("../migrations/0009_control_plane.sql"))
                .execute(&mut *tx)
                .await?;
            sqlx::query("INSERT INTO identity_schema_migrations VALUES (9)")
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await
    }

    pub fn router(&self) -> Router {
        self.routes(true)
    }

    /// Identity HTTP routes without the legacy auth-only socket; hosts mount Gateway instead.
    pub fn http_router(&self) -> Router {
        self.routes(false)
    }

    fn routes(&self, include_socket: bool) -> Router {
        let router = Router::new()
            .route("/api/v1/admin/migration-status", get(migration_status))
            .route(
                "/api/v1/admin/session",
                post(admin_session_login)
                    .get(admin_session_current)
                    .delete(admin_session_logout),
            )
            .route(
                "/api/v1/admin/users",
                get(admin_users_list).post(admin_users_create),
            )
            .route(
                "/api/v1/admin/users/{id}",
                patch(admin_users_update).delete(admin_users_disable),
            )
            .route(
                "/api/v1/admin/users/{id}/password",
                post(admin_users_rotate_password),
            )
            .route("/api/v1/apps", get(admin_apps_list).post(admin_apps_create))
            .route("/api/v1/admin/apps/{id}", patch(admin_apps_update))
            .route(
                "/api/v1/apps/{id}",
                axum::routing::delete(admin_apps_disable),
            )
            .route("/api/v1/apps/{id}/rotate-secret", post(admin_apps_rotate))
            .route(
                "/api/v1/session",
                post(login).get(current_session).delete(logout),
            )
            .route("/api/v1/session/setup", post(setup))
            .route(
                "/api/v1/workspaces/{id}/apps",
                post(create_app).get(list_apps),
            )
            .route(
                "/api/v1/workspaces/{id}/credentials",
                post(create_credential),
            )
            .route(
                "/api/v1/workspaces/{id}/credentials/{credential_id}",
                axum::routing::delete(revoke_credential),
            )
            .route("/api/v1/openapi.json", get(|| async { Json(openapi()) }));
        let router = if include_socket {
            router.route("/api/v1/socket", get(socket_upgrade))
        } else {
            router
        };
        router
            .merge(crate::admin::router())
            .merge(crate::control_plane::router())
            .layer(axum::middleware::map_response(
                |mut response: Response| async move {
                    response
                        .headers_mut()
                        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
                    response
                },
            ))
            .with_state(self.clone())
    }

    pub async fn authenticate(&self, token: &str) -> Result<Principal, AuthError> {
        if token.is_empty() || token.len() > 512 {
            return Err(AuthError::Unauthorized);
        }
        let hash = digest(token);
        if let Some(row) = sqlx::query("SELECT u.id, u.is_admin, w.id AS workspace_id FROM identity_sessions s JOIN identity_users u ON u.id = s.user_id LEFT JOIN identity_workspaces w ON w.owner_id = u.id WHERE s.token_hash = $1 AND s.revoked_at IS NULL AND s.expires_at > clock_timestamp() AND u.enabled")
            .bind(&hash).fetch_optional(&self.pool).await? {
            let user_id = row.get("id");
            return if row.get("is_admin") { Ok(Principal::SystemAdmin { user_id }) } else {
                Ok(Principal::WorkspaceOwner { user_id, workspace_id: row.get::<Option<Uuid>, _>("workspace_id").ok_or(AuthError::Unauthorized)? })
            };
        }
        let row = sqlx::query("SELECT id, workspace_id, app_id, kind, agent_id FROM identity_credentials WHERE token_hash = $1 AND revoked_at IS NULL")
            .bind(hash).fetch_optional(&self.pool).await?.ok_or(AuthError::Unauthorized)?;
        let workspace_id = row.get("workspace_id");
        let app_id = row.get("app_id");
        let key_id = row.get("id");
        match row.get::<&str, _>("kind") {
            "application" => Ok(Principal::Application {
                workspace_id,
                app_id,
                key_id,
            }),
            "agent" => Ok(Principal::Agent {
                workspace_id,
                app_id,
                key_id,
                agent_id: row.get("agent_id"),
            }),
            _ => Err(AuthError::Unauthorized),
        }
    }

    async fn request_auth(
        &self,
        headers: &HeaderMap,
        write: bool,
    ) -> Result<(Principal, String), AuthError> {
        if let Some(value) = headers.get(header::AUTHORIZATION) {
            if headers.get_all(header::AUTHORIZATION).iter().count() != 1 {
                return Err(AuthError::Unauthorized);
            }
            let token = value
                .to_str()
                .ok()
                .and_then(|s| s.strip_prefix("Bearer "))
                .ok_or(AuthError::Unauthorized)?;
            return Ok((self.authenticate(token).await?, token.to_owned()));
        }
        self.request_cookie_auth(headers, write, COOKIE).await
    }

    async fn request_cookie_auth(
        &self,
        headers: &HeaderMap,
        write: bool,
        cookie_name: &str,
    ) -> Result<(Principal, String), AuthError> {
        let mut tokens = Vec::new();
        for cookie in headers.get_all(header::COOKIE) {
            for part in cookie
                .to_str()
                .map_err(|_| AuthError::Unauthorized)?
                .split(';')
            {
                if let Some((name, value)) = part.trim().split_once('=')
                    && name == cookie_name
                {
                    tokens.push(value);
                }
            }
        }
        if tokens.len() != 1 {
            return Err(AuthError::Unauthorized);
        }
        let token = tokens[0];
        let principal = self.authenticate(token).await?;
        session_principal(&principal)?;
        if write {
            // The full Admin SPA historically sends X-RelayHub-CSRF; the
            // Rust contract also documents the shorter x-csrf-token name.
            // Accept both during the migration so a UI-only deploy cannot
            // turn every write into a 403.
            let csrf = headers
                .get("x-relayhub-csrf")
                .or_else(|| headers.get("x-csrf-token"))
                .and_then(|v| v.to_str().ok())
                .filter(|v| !v.is_empty() && v.len() <= 512)
                .ok_or(AuthError::Forbidden)?;
            let valid: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM identity_sessions WHERE token_hash = $1 AND csrf_hash = $2 AND revoked_at IS NULL AND expires_at > clock_timestamp())")
                .bind(digest(token)).bind(digest(csrf)).fetch_one(&self.pool).await?;
            if !valid {
                return Err(AuthError::Forbidden);
            }
        }
        Ok((principal, token.to_owned()))
    }

    async fn admin_user(&self, principal: &Principal) -> Result<Value, AuthError> {
        let user_id = session_principal(principal)?;
        let row = sqlx::query("SELECT id, email, is_admin FROM identity_users WHERE id=$1")
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(AuthError::Unauthorized)?;
        Ok(json!({
            "id": row.get::<Uuid, _>("id"),
            "email": row.get::<String, _>("email"),
            "role": if row.get::<bool, _>("is_admin") { "admin" } else { "user" },
        }))
    }

    async fn admin_principal(&self, user_id: Uuid) -> Result<Principal, AuthError> {
        let row = sqlx::query("SELECT is_admin FROM identity_users WHERE id=$1")
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(AuthError::Unauthorized)?;
        if row.get::<bool, _>("is_admin") {
            return Ok(Principal::SystemAdmin { user_id });
        }
        let workspace_id: Uuid =
            sqlx::query_scalar("SELECT id FROM identity_workspaces WHERE owner_id=$1")
                .bind(user_id)
                .fetch_optional(&self.pool)
                .await?
                .ok_or(AuthError::Unauthorized)?;
        Ok(Principal::WorkspaceOwner {
            user_id,
            workspace_id,
        })
    }

    /// Dashboard routes deliberately accept only the same-origin session cookie.
    pub(crate) async fn browser_auth(
        &self,
        headers: &HeaderMap,
        write: bool,
    ) -> Result<Principal, AuthError> {
        if headers.contains_key(header::AUTHORIZATION) {
            return Err(AuthError::Unauthorized);
        }
        let (principal, _) = self.request_auth(headers, write).await?;
        session_principal(&principal)?;
        Ok(principal)
    }

    pub async fn provision_user(
        &self,
        principal: &Principal,
        address: &str,
    ) -> Result<ProvisionedUser, AuthError> {
        if !matches!(principal, Principal::SystemAdmin { .. }) {
            return Err(AuthError::Forbidden);
        }
        let address = email(address)?;
        let user_id = Uuid::new_v4();
        let workspace_id = Uuid::new_v4();
        let setup_token = secret();
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO identity_users (id, email) VALUES ($1, $2)")
            .bind(user_id)
            .bind(address)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO identity_workspaces (id, owner_id) VALUES ($1, $2)")
            .bind(workspace_id)
            .bind(user_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO identity_setup_tokens (user_id, token_hash, expires_at) VALUES ($1, $2, now() + interval '24 hours')")
            .bind(user_id).bind(digest(&setup_token)).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(ProvisionedUser {
            user_id,
            workspace_id,
            setup_token,
            expires_in_seconds: SETUP_TOKEN_TTL_SECONDS,
        })
    }

    pub async fn setup_password(&self, token: &str, password: &str) -> Result<(), AuthError> {
        if !(12..=1024).contains(&password.len()) {
            return Err(AuthError::InvalidInput);
        }
        if token.is_empty() || token.len() > 512 {
            return Err(AuthError::Unauthorized);
        }
        // Reject invalid tokens before spending memory on password hashing.
        let valid: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM identity_setup_tokens WHERE token_hash = $1 AND consumed_at IS NULL AND expires_at > clock_timestamp())")
            .bind(digest(token)).fetch_one(&self.pool).await?;
        if !valid {
            return Err(AuthError::Unauthorized);
        }
        let password = password.to_owned();
        let password_hash = tokio::task::spawn_blocking(move || {
            let salt = SaltString::encode_b64(Uuid::new_v4().as_bytes())
                .map_err(|_| AuthError::Internal)?;
            Argon2::default()
                .hash_password(password.as_bytes(), &salt)
                .map(|h| h.to_string())
                .map_err(|_| AuthError::Internal)
        })
        .await
        .map_err(|_| AuthError::Internal)??;
        let mut tx = self.pool.begin().await?;
        // PostgreSQL rechecks this predicate after a competing UPDATE releases its lock.
        let user_id: Uuid = sqlx::query_scalar("UPDATE identity_setup_tokens SET consumed_at = clock_timestamp() WHERE token_hash = $1 AND consumed_at IS NULL AND expires_at > clock_timestamp() RETURNING user_id")
            .bind(digest(token)).fetch_optional(&mut *tx).await?.ok_or(AuthError::Unauthorized)?;
        sqlx::query("UPDATE identity_users SET password_hash = $1, enabled = true, updated_at = clock_timestamp() WHERE id = $2")
            .bind(password_hash)
            .bind(user_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn revoke_credential(
        &self,
        principal: &Principal,
        workspace_id: Uuid,
        id: Uuid,
    ) -> Result<(), AuthError> {
        authorize(principal, workspace_id, Action::ManageCredentials)?;
        let count = sqlx::query("UPDATE identity_credentials SET revoked_at = COALESCE(revoked_at, clock_timestamp()) WHERE id = $1 AND workspace_id = $2")
            .bind(id).bind(workspace_id).execute(&self.pool).await?.rows_affected();
        if count == 0 {
            return Err(AuthError::Forbidden);
        }
        Ok(())
    }

    /// Authentication lifetime boundary for socket transports; fail closed on DB loss.
    /// Future gateway loops should select on the same credential revalidation.
    pub async fn guard_socket(&self, mut socket: WebSocket, token: String) {
        let mut poll = tokio::time::interval(Duration::from_millis(250));
        loop {
            tokio::select! {
                _ = poll.tick() => {
                    if self.authenticate(&token).await.is_err() {
                        let _ = socket.send(Message::Close(Some(CloseFrame { code: 1008, reason: "credential revoked".into() }))).await;
                        break;
                    }
                },
                message = socket.recv() => {
                    match message {
                        None | Some(Err(_)) | Some(Ok(Message::Close(_))) => break,
                        _ => {},
                    }
                }
            }
        }
    }
}

#[derive(Serialize)]
pub struct ProvisionedUser {
    pub user_id: Uuid,
    pub workspace_id: Uuid,
    pub setup_token: String,
    pub expires_in_seconds: u64,
}
#[derive(Deserialize)]
struct AdminUserCreate {
    email: String,
    password: Option<String>,
    role: Option<String>,
}
#[derive(Deserialize)]
struct AdminUserUpdate {
    role: Option<String>,
    enabled: Option<bool>,
}
#[derive(Deserialize)]
struct AdminPasswordUpdate {
    password: String,
}
#[derive(Deserialize)]
struct AdminAppInput {
    name: Option<String>,
    delivery_mode: Option<String>,
    callback_url: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Setup {
    token: String,
    password: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Login {
    email: String,
    password: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateApp {
    name: String,
}
#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum CredentialKind {
    Application,
    Agent,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateCredential {
    app_id: Uuid,
    kind: CredentialKind,
}

async fn admin_session_auth(
    identity: &Identity,
    headers: &HeaderMap,
    write: bool,
) -> Result<(Principal, String), AuthError> {
    // Keep the v1 bearer provisioning route for agents and CLI tooling. Browser
    // Admin writes use the separate cookie+CSRF contract below.
    let has_admin_cookie = headers.get_all(header::COOKIE).iter().any(|value| {
        value.to_str().ok().is_some_and(|raw| {
            raw.split(';')
                .any(|part| part.trim().starts_with("__Host-relayhub_admin="))
        })
    });
    let (principal, token) = if headers.contains_key(header::AUTHORIZATION) || !has_admin_cookie {
        identity.request_auth(headers, write).await?
    } else {
        identity
            .request_cookie_auth(headers, write, ADMIN_COOKIE)
            .await?
    };
    Ok((principal, token))
}

async fn system_admin_auth(
    identity: &Identity,
    headers: &HeaderMap,
    write: bool,
) -> Result<(Principal, String), AuthError> {
    let (principal, token) = admin_session_auth(identity, headers, write).await?;
    if matches!(principal, Principal::SystemAdmin { .. }) {
        Ok((principal, token))
    } else {
        Err(AuthError::Forbidden)
    }
}

async fn admin_user_by_id(identity: &Identity, id: Uuid) -> Result<Value, AuthError> {
    let row = sqlx::query("SELECT id, email, is_admin, enabled, to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at, to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at FROM identity_users WHERE id=$1")
        .bind(id)
        .fetch_optional(&identity.pool)
        .await?
        .ok_or(AuthError::Conflict)?;
    Ok(json!({
        "id": row.get::<Uuid, _>("id"),
        "email": row.get::<String, _>("email"),
        "role": if row.get::<bool, _>("is_admin") { "admin" } else { "user" },
        "enabled": row.get::<bool, _>("enabled"),
        "created_at": row.get::<String, _>("created_at"),
        "updated_at": row.get::<String, _>("updated_at"),
    }))
}

async fn admin_users_list(
    State(identity): State<Identity>,
    headers: HeaderMap,
) -> Result<Json<Value>, AuthError> {
    system_admin_auth(&identity, &headers, false).await?;
    let rows = sqlx::query("SELECT id, email, is_admin, enabled, to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at, to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at FROM identity_users ORDER BY created_at, id")
        .fetch_all(&identity.pool)
        .await?;
    let users = rows
        .into_iter()
        .map(|row| {
            json!({
                "id": row.get::<Uuid, _>("id"),
                "email": row.get::<String, _>("email"),
                "role": if row.get::<bool, _>("is_admin") { "admin" } else { "user" },
                "enabled": row.get::<bool, _>("enabled"),
                "created_at": row.get::<String, _>("created_at"),
                "updated_at": row.get::<String, _>("updated_at"),
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(Value::Array(users)))
}

async fn admin_users_create(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Json(input): Json<AdminUserCreate>,
) -> Result<Response, AuthError> {
    let (principal, _) = system_admin_auth(&identity, &headers, true).await?;
    if input.password.is_none() && input.role.is_none() {
        return Ok((
            StatusCode::CREATED,
            Json(identity.provision_user(&principal, &input.email).await?),
        )
            .into_response());
    }
    let password = input.password.ok_or(AuthError::InvalidInput)?;
    if !(12..=1024).contains(&password.len()) {
        return Err(AuthError::InvalidInput);
    }
    let role = input.role.unwrap_or_else(|| "admin".to_owned());
    if role != "admin" && role != "user" {
        return Err(AuthError::InvalidInput);
    }
    let address = email(&input.email)?;
    let password_hash = hash_password(password).await?;
    let user_id = Uuid::new_v4();
    let workspace_id = Uuid::new_v4();
    let mut tx = identity.pool.begin().await?;
    sqlx::query("INSERT INTO identity_users (id, email, is_admin, password_hash, enabled, updated_at) VALUES ($1, $2, $3, $4, true, clock_timestamp())")
        .bind(user_id)
        .bind(address)
        .bind(role == "admin")
        .bind(password_hash)
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO identity_workspaces (id, owner_id) VALUES ($1, $2)")
        .bind(workspace_id)
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok((
        StatusCode::CREATED,
        Json(admin_user_by_id(&identity, user_id).await?),
    )
        .into_response())
}

async fn admin_users_update(
    State(identity): State<Identity>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<AdminUserUpdate>,
) -> Result<Json<Value>, AuthError> {
    let (principal, _) = system_admin_auth(&identity, &headers, true).await?;
    if input.role.is_none() && input.enabled.is_none() {
        return Err(AuthError::InvalidInput);
    }
    if input
        .role
        .as_deref()
        .is_some_and(|role| role != "admin" && role != "user")
    {
        return Err(AuthError::InvalidInput);
    }
    let actor = session_principal(&principal)?;
    if input.enabled == Some(false) && actor == id {
        return Err(AuthError::Conflict);
    }
    sqlx::query("UPDATE identity_users SET is_admin=COALESCE($2,is_admin), enabled=COALESCE($3,enabled), updated_at=clock_timestamp() WHERE id=$1")
        .bind(id)
        .bind(input.role.map(|role| role == "admin"))
        .bind(input.enabled)
        .execute(&identity.pool)
        .await?;
    Ok(Json(admin_user_by_id(&identity, id).await?))
}

async fn admin_users_disable(
    State(identity): State<Identity>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Json<Value>, AuthError> {
    let (principal, _) = system_admin_auth(&identity, &headers, true).await?;
    if session_principal(&principal)? == id {
        return Err(AuthError::Conflict);
    }
    sqlx::query(
        "UPDATE identity_users SET enabled=false, updated_at=clock_timestamp() WHERE id=$1",
    )
    .bind(id)
    .execute(&identity.pool)
    .await?;
    Ok(Json(admin_user_by_id(&identity, id).await?))
}

async fn admin_users_rotate_password(
    State(identity): State<Identity>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<AdminPasswordUpdate>,
) -> Result<Json<Value>, AuthError> {
    system_admin_auth(&identity, &headers, true).await?;
    if !(12..=1024).contains(&input.password.len()) {
        return Err(AuthError::InvalidInput);
    }
    let password_hash = hash_password(input.password).await?;
    sqlx::query("UPDATE identity_users SET password_hash=$2, enabled=true, updated_at=clock_timestamp() WHERE id=$1")
        .bind(id)
        .bind(password_hash)
        .execute(&identity.pool)
        .await?;
    sqlx::query("UPDATE identity_sessions SET revoked_at=clock_timestamp() WHERE user_id=$1")
        .bind(id)
        .execute(&identity.pool)
        .await?;
    Ok(Json(admin_user_by_id(&identity, id).await?))
}

async fn admin_workspace(identity: &Identity, principal: &Principal) -> Result<Uuid, AuthError> {
    match principal {
        Principal::WorkspaceOwner { workspace_id, .. } => Ok(*workspace_id),
        Principal::SystemAdmin { user_id } => {
            if let Some(workspace_id) = sqlx::query_scalar::<_, Uuid>(
                "SELECT id FROM identity_workspaces WHERE owner_id=$1",
            )
            .bind(user_id)
            .fetch_optional(&identity.pool)
            .await?
            {
                return Ok(workspace_id);
            }
            let workspace_id = Uuid::new_v4();
            sqlx::query("INSERT INTO identity_workspaces (id, owner_id) VALUES ($1,$2)")
                .bind(workspace_id)
                .bind(user_id)
                .execute(&identity.pool)
                .await?;
            Ok(workspace_id)
        }
        _ => Err(AuthError::Forbidden),
    }
}

fn app_delivery_mode(value: Option<&str>) -> Result<String, AuthError> {
    let value = value.unwrap_or("queue");
    if matches!(value, "queue" | "websocket" | "callback" | "all") {
        Ok(value.to_owned())
    } else {
        Err(AuthError::InvalidInput)
    }
}

async fn admin_app_by_id(identity: &Identity, id: Uuid) -> Result<Value, AuthError> {
    let row = sqlx::query("SELECT id, workspace_id, name, delivery_mode, callback_url, enabled, to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at, to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at FROM identity_apps WHERE id=$1")
        .bind(id)
        .fetch_optional(&identity.pool)
        .await?
        .ok_or(AuthError::Conflict)?;
    Ok(json!({
        "id": row.get::<Uuid, _>("id"),
        "workspace_id": row.get::<Uuid, _>("workspace_id"),
        "name": row.get::<String, _>("name"),
        "delivery_mode": row.get::<String, _>("delivery_mode"),
        "callback_url": row.get::<Option<String>, _>("callback_url"),
        "enabled": row.get::<bool, _>("enabled"),
        "created_at": row.get::<String, _>("created_at"),
        "updated_at": row.get::<String, _>("updated_at"),
    }))
}

async fn issue_application_credentials(
    identity: &Identity,
    workspace_id: Uuid,
    app_id: Uuid,
) -> Result<Value, AuthError> {
    let api_key = secret();
    let hmac_secret = secret();
    sqlx::query("INSERT INTO identity_credentials (id, workspace_id, app_id, kind, token_hash, hmac_secret_hash) VALUES ($1,$2,$3,'application',$4,$5)")
        .bind(Uuid::new_v4())
        .bind(workspace_id)
        .bind(app_id)
        .bind(digest(&api_key))
        .bind(digest(&hmac_secret))
        .execute(&identity.pool)
        .await?;
    Ok(json!({"app_id": app_id, "api_key": api_key, "hmac_secret": hmac_secret}))
}

async fn admin_apps_list(
    State(identity): State<Identity>,
    headers: HeaderMap,
) -> Result<Json<Value>, AuthError> {
    let (principal, _) = admin_session_auth(&identity, &headers, false).await?;
    session_principal(&principal)?;
    let rows = sqlx::query("SELECT id, workspace_id, name, delivery_mode, callback_url, enabled, to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at, to_char(updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at FROM identity_apps WHERE ($1::uuid IS NULL OR workspace_id=$1) ORDER BY created_at, id")
        .bind(match principal { Principal::WorkspaceOwner { workspace_id, .. } => Some(workspace_id), _ => None })
        .fetch_all(&identity.pool)
        .await?;
    let apps = rows
        .into_iter()
        .map(|row| {
            json!({
                "id": row.get::<Uuid, _>("id"),
                "workspace_id": row.get::<Uuid, _>("workspace_id"),
                "name": row.get::<String, _>("name"),
                "delivery_mode": row.get::<String, _>("delivery_mode"),
                "callback_url": row.get::<Option<String>, _>("callback_url"),
                "enabled": row.get::<bool, _>("enabled"),
                "created_at": row.get::<String, _>("created_at"),
                "updated_at": row.get::<String, _>("updated_at"),
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(Value::Array(apps)))
}

async fn admin_apps_create(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Json(input): Json<AdminAppInput>,
) -> Result<Response, AuthError> {
    let (principal, _) = system_admin_auth(&identity, &headers, true).await?;
    let workspace_id = admin_workspace(&identity, &principal).await?;
    let name = input
        .name
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty() && name.chars().count() <= 200)
        .ok_or(AuthError::InvalidInput)?;
    let delivery_mode = app_delivery_mode(input.delivery_mode.as_deref())?;
    if input
        .callback_url
        .as_deref()
        .is_some_and(|url| url.len() > 2048)
    {
        return Err(AuthError::InvalidInput);
    }
    let app_id = Uuid::new_v4();
    sqlx::query("INSERT INTO identity_apps (id, workspace_id, name, delivery_mode, callback_url, updated_at) VALUES ($1,$2,$3,$4,$5,clock_timestamp())")
        .bind(app_id)
        .bind(workspace_id)
        .bind(name)
        .bind(delivery_mode)
        .bind(input.callback_url)
        .execute(&identity.pool)
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(issue_application_credentials(&identity, workspace_id, app_id).await?),
    )
        .into_response())
}

async fn admin_apps_update(
    State(identity): State<Identity>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<AdminAppInput>,
) -> Result<Json<Value>, AuthError> {
    system_admin_auth(&identity, &headers, true).await?;
    let name = input.name.as_deref().map(str::trim);
    if name.is_some_and(|value| value.is_empty() || value.chars().count() > 200) {
        return Err(AuthError::InvalidInput);
    }
    let delivery_mode = input
        .delivery_mode
        .as_deref()
        .map(|value| app_delivery_mode(Some(value)))
        .transpose()?;
    sqlx::query("UPDATE identity_apps SET name=COALESCE($2,name), delivery_mode=COALESCE($3,delivery_mode), callback_url=COALESCE($4,callback_url), updated_at=clock_timestamp() WHERE id=$1")
        .bind(id)
        .bind(name)
        .bind(delivery_mode)
        .bind(input.callback_url)
        .execute(&identity.pool)
        .await?;
    Ok(Json(admin_app_by_id(&identity, id).await?))
}

async fn admin_apps_disable(
    State(identity): State<Identity>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Json<Value>, AuthError> {
    system_admin_auth(&identity, &headers, true).await?;
    sqlx::query("UPDATE identity_apps SET enabled=false, updated_at=clock_timestamp() WHERE id=$1")
        .bind(id)
        .execute(&identity.pool)
        .await?;
    Ok(Json(admin_app_by_id(&identity, id).await?))
}

async fn admin_apps_rotate(
    State(identity): State<Identity>,
    Path(id): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Json<Value>, AuthError> {
    let (principal, _) = system_admin_auth(&identity, &headers, true).await?;
    let workspace_id: Uuid =
        sqlx::query_scalar("SELECT workspace_id FROM identity_apps WHERE id=$1")
            .bind(id)
            .fetch_optional(&identity.pool)
            .await?
            .ok_or(AuthError::Conflict)?;
    authorize(&principal, workspace_id, Action::ManageCredentials)?;
    sqlx::query("UPDATE identity_credentials SET revoked_at=clock_timestamp() WHERE app_id=$1 AND kind='application' AND revoked_at IS NULL")
        .bind(id)
        .execute(&identity.pool)
        .await?;
    Ok(Json(
        issue_application_credentials(&identity, workspace_id, id).await?,
    ))
}

async fn hash_password(password: String) -> Result<String, AuthError> {
    tokio::task::spawn_blocking(move || {
        let salt =
            SaltString::encode_b64(Uuid::new_v4().as_bytes()).map_err(|_| AuthError::Internal)?;
        Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map(|hash| hash.to_string())
            .map_err(|_| AuthError::Internal)
    })
    .await
    .map_err(|_| AuthError::Internal)?
}

async fn migration_status(
    State(identity): State<Identity>,
    headers: HeaderMap,
) -> Result<Json<Value>, AuthError> {
    let (principal, _) = identity.request_auth(&headers, false).await?;
    if !matches!(principal, Principal::SystemAdmin { .. }) {
        return Err(AuthError::Forbidden);
    }
    let schema_version: i32 =
        sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) FROM identity_schema_migrations")
            .fetch_one(&identity.pool)
            .await?;
    let migration = sqlx::query(
        "SELECT id, state, mode FROM relayhub_migration_runs ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_optional(&identity.pool)
    .await?;
    let data_migration = migration.map_or_else(
        || json!({"state": "not_started", "version": 0}),
        |row| {
            json!({
                "state": row.get::<String, _>("state"),
                "mode": row.get::<String, _>("mode"),
                "run_id": row.get::<Uuid, _>("id"),
                "version": 1,
            })
        },
    );
    Ok(Json(json!({
        "service": "relayhub-rust",
        "schema": {
            "identity": schema_version,
        },
        "features": ["identity", "events", "delivery", "webhooks", "realtime", "dashboard"],
        "data_migration": data_migration,
    })))
}

async fn setup(
    State(identity): State<Identity>,
    Json(input): Json<Setup>,
) -> Result<StatusCode, AuthError> {
    identity
        .setup_password(&input.token, &input.password)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
async fn login(
    State(identity): State<Identity>,
    Json(input): Json<Login>,
) -> Result<Response, AuthError> {
    let user_id = verify_password(&identity, &input).await?;
    let token = secret();
    let csrf_token = secret();
    sqlx::query("INSERT INTO identity_sessions (id, user_id, token_hash, csrf_hash, expires_at) VALUES ($1, $2, $3, $4, now() + interval '24 hours')")
        .bind(Uuid::new_v4()).bind(user_id).bind(digest(&token)).bind(digest(&csrf_token)).execute(&identity.pool).await?;
    let cookie = format!(
        "{COOKIE}={token}; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age={SESSION_SECONDS}"
    );
    Ok(([(header::SET_COOKIE, cookie)], Json(json!({"token": token, "csrf_token": csrf_token, "expires_in_seconds": SESSION_SECONDS}))).into_response())
}

async fn verify_password(identity: &Identity, input: &Login) -> Result<Uuid, AuthError> {
    if input.password.len() > 1024 {
        return Err(AuthError::Unauthorized);
    }
    let address = email(&input.email).map_err(|_| AuthError::Unauthorized)?;
    let row = sqlx::query("SELECT id, password_hash, legacy_password_hash FROM identity_users WHERE email = $1 AND enabled")
        .bind(address)
        .fetch_optional(&identity.pool)
        .await?;
    // A fixed valid dummy hash keeps unknown users on the password-verification path.
    let encoded = row
        .as_ref()
        .and_then(|r| r.get::<Option<String>, _>("password_hash"))
        .unwrap_or_else(|| "$argon2id$v=19$m=19456,t=2,p=1$cmVsYXlodWItZHVtbXktc2FsdA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_owned());
    let legacy = row
        .as_ref()
        .and_then(|r| r.get::<Option<String>, _>("legacy_password_hash"));
    // Reject excess verification work immediately; the permit lives in the blocking closure so cancellation cannot release it early.
    let password = input.password.clone();
    let legacy_for_verify = legacy.clone();
    let permit = identity
        .password_verification
        .clone()
        .try_acquire_owned()
        .map_err(|_| AuthError::TooManyRequests)?;
    let verified = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        PasswordHash::new(&encoded).ok().is_some_and(|h| {
            Argon2::default()
                .verify_password(password.as_bytes(), &h)
                .is_ok()
        }) || legacy_for_verify
            .is_some_and(|value| verify_legacy_pbkdf2(&value, password.as_bytes()))
    })
    .await
    .map_err(|_| AuthError::Internal)?;
    if !verified {
        return Err(AuthError::Unauthorized);
    }
    let user_id = row
        .as_ref()
        .map(|value| value.get("id"))
        .ok_or(AuthError::Unauthorized)?;
    if legacy.is_some() {
        let password_hash = hash_password(input.password.clone()).await?;
        sqlx::query("UPDATE identity_users SET password_hash=$1, legacy_password_hash=NULL, updated_at=clock_timestamp() WHERE id=$2")
            .bind(password_hash)
            .bind(user_id)
            .execute(&identity.pool)
            .await?;
    }
    Ok(user_id)
}

fn verify_legacy_pbkdf2(encoded: &str, password: &[u8]) -> bool {
    let mut parts = encoded.split('$');
    let (Some(algorithm), Some(iterations), Some(salt), Some(expected), None) = (
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
        parts.next(),
    ) else {
        return false;
    };
    if algorithm != "pbkdf2-sha256" {
        return false;
    }
    let Ok(iterations) = iterations.parse::<u32>() else {
        return false;
    };
    if !(100_000..=1_000_000).contains(&iterations) {
        return false;
    }
    let Ok(salt) = URL_SAFE_NO_PAD.decode(salt) else {
        return false;
    };
    let Ok(expected) = URL_SAFE_NO_PAD.decode(expected) else {
        return false;
    };
    if expected.len() != 32 {
        return false;
    }
    let mut actual = [0u8; 32];
    pbkdf2_hmac::<Sha256>(password, &salt, iterations, &mut actual);
    actual.ct_eq(&expected).into()
}

async fn admin_session_login(
    State(identity): State<Identity>,
    Json(input): Json<Login>,
) -> Result<Response, AuthError> {
    let user_id = verify_password(&identity, &input).await?;
    let token = secret();
    let csrf_token = secret();
    let expires_at: String = sqlx::query_scalar("INSERT INTO identity_sessions (id, user_id, token_hash, csrf_hash, expires_at) VALUES ($1, $2, $3, $4, now() + interval '24 hours') RETURNING to_char(expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')")
        .bind(Uuid::new_v4())
        .bind(user_id)
        .bind(digest(&token))
        .bind(digest(&csrf_token))
        .fetch_one(&identity.pool)
        .await?;
    let principal = identity.admin_principal(user_id).await?;
    let user = identity.admin_user(&principal).await?;
    let cookie = format!(
        "{ADMIN_COOKIE}={token}; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age={SESSION_SECONDS}"
    );
    Ok((
        [(header::SET_COOKIE, cookie)],
        Json(json!({"csrf_token": csrf_token, "expires_at": expires_at, "user": user})),
    )
        .into_response())
}

async fn admin_session_current(
    State(identity): State<Identity>,
    headers: HeaderMap,
) -> Result<Json<Value>, AuthError> {
    let (principal, token) = identity
        .request_cookie_auth(&headers, false, ADMIN_COOKIE)
        .await?;
    let user = identity.admin_user(&principal).await?;
    let csrf_token = secret();
    let expires_at: String = sqlx::query_scalar("UPDATE identity_sessions SET csrf_hash=$2 WHERE token_hash=$1 RETURNING to_char(expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')")
        .bind(digest(&token))
        .bind(digest(&csrf_token))
        .fetch_optional(&identity.pool)
        .await?
        .ok_or(AuthError::Unauthorized)?;
    Ok(Json(
        json!({"csrf_token": csrf_token, "expires_at": expires_at, "user": user}),
    ))
}

async fn admin_session_logout(
    State(identity): State<Identity>,
    headers: HeaderMap,
) -> Result<Response, AuthError> {
    let (_, token) = identity
        .request_cookie_auth(&headers, true, ADMIN_COOKIE)
        .await?;
    sqlx::query("UPDATE identity_sessions SET revoked_at=clock_timestamp() WHERE token_hash=$1")
        .bind(digest(&token))
        .execute(&identity.pool)
        .await?;
    Ok((
        StatusCode::NO_CONTENT,
        [(
            header::SET_COOKIE,
            format!("{ADMIN_COOKIE}=; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age=0"),
        )],
    )
        .into_response())
}
async fn current_session(
    State(identity): State<Identity>,
    headers: HeaderMap,
) -> Result<Json<Principal>, AuthError> {
    let (principal, _) = identity.request_auth(&headers, false).await?;
    session_principal(&principal)?;
    Ok(Json(principal))
}
async fn logout(
    State(identity): State<Identity>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, AuthError> {
    let (principal, token) = identity.request_auth(&headers, true).await?;
    session_principal(&principal)?;
    sqlx::query(
        "UPDATE identity_sessions SET revoked_at = clock_timestamp() WHERE token_hash = $1",
    )
    .bind(digest(&token))
    .execute(&identity.pool)
    .await?;
    Ok((
        StatusCode::NO_CONTENT,
        [(
            header::SET_COOKIE,
            format!("{COOKIE}=; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age=0"),
        )],
    ))
}
async fn create_app(
    State(identity): State<Identity>,
    Path(workspace): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<CreateApp>,
) -> Result<impl IntoResponse, AuthError> {
    let (principal, _) = identity.request_auth(&headers, true).await?;
    authorize(&principal, workspace, Action::ManageApps)?;
    let name = input.name.trim();
    if name.is_empty() || name.chars().count() > 200 {
        return Err(AuthError::InvalidInput);
    }
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM identity_workspaces WHERE id = $1)")
            .bind(workspace)
            .fetch_one(&identity.pool)
            .await?;
    if !exists {
        return Err(AuthError::Forbidden);
    }
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO identity_apps (id, workspace_id, name) VALUES ($1, $2, $3)")
        .bind(id)
        .bind(workspace)
        .bind(name)
        .execute(&identity.pool)
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(json!({"id": id, "workspace_id": workspace, "name": name})),
    ))
}
async fn list_apps(
    State(identity): State<Identity>,
    Path(workspace): Path<Uuid>,
    headers: HeaderMap,
) -> Result<Json<Value>, AuthError> {
    let (principal, _) = identity.request_auth(&headers, false).await?;
    authorize(&principal, workspace, Action::ManageApps)?;
    let rows =
        sqlx::query("SELECT id, name FROM identity_apps WHERE workspace_id = $1 ORDER BY id")
            .bind(workspace)
            .fetch_all(&identity.pool)
            .await?;
    Ok(Json(json!(rows.into_iter().map(|r| json!({"id": r.get::<Uuid, _>("id"), "workspace_id": workspace, "name": r.get::<String, _>("name")})).collect::<Vec<_>>())))
}
async fn create_credential(
    State(identity): State<Identity>,
    Path(workspace): Path<Uuid>,
    headers: HeaderMap,
    Json(input): Json<CreateCredential>,
) -> Result<impl IntoResponse, AuthError> {
    let (principal, _) = identity.request_auth(&headers, true).await?;
    authorize(&principal, workspace, Action::ManageCredentials)?;
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM identity_apps WHERE id = $1 AND workspace_id = $2)",
    )
    .bind(input.app_id)
    .bind(workspace)
    .fetch_one(&identity.pool)
    .await?;
    if !exists {
        return Err(AuthError::Forbidden);
    }
    let id = Uuid::new_v4();
    let agent_id = if matches!(input.kind, CredentialKind::Agent) {
        Some(Uuid::new_v4())
    } else {
        None
    };
    let kind = if agent_id.is_some() {
        "agent"
    } else {
        "application"
    };
    let token = secret();
    sqlx::query("INSERT INTO identity_credentials (id, workspace_id, app_id, kind, agent_id, token_hash) VALUES ($1, $2, $3, $4, $5, $6)")
        .bind(id).bind(workspace).bind(input.app_id).bind(kind).bind(agent_id).bind(digest(&token)).execute(&identity.pool).await?;
    Ok((
        StatusCode::CREATED,
        Json(
            json!({"id": id, "workspace_id": workspace, "app_id": input.app_id, "kind": input.kind, "agent_id": agent_id, "token": token}),
        ),
    ))
}
async fn revoke_credential(
    State(identity): State<Identity>,
    Path((workspace, id)): Path<(Uuid, Uuid)>,
    headers: HeaderMap,
) -> Result<StatusCode, AuthError> {
    let (principal, _) = identity.request_auth(&headers, true).await?;
    identity
        .revoke_credential(&principal, workspace, id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
async fn socket_upgrade(
    State(identity): State<Identity>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<Response, AuthError> {
    let (principal, token) = identity.request_auth(&headers, false).await?;
    if !matches!(
        principal,
        Principal::Application { .. } | Principal::Agent { .. }
    ) {
        return Err(AuthError::Forbidden);
    }
    let valid_protocol = headers
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.split(',').any(|s| s.trim() == SUBPROTOCOL));
    if !valid_protocol {
        return Err(AuthError::InvalidInput);
    }
    Ok(ws
        .protocols([SUBPROTOCOL])
        .on_upgrade(move |socket| async move { identity.guard_socket(socket, token).await }))
}

/// OpenAPI is colocated with this task's routes so no Task 1 wire contracts change.
pub fn openapi() -> Value {
    let mut schemas = json!({
        "CreateUser": {"type":"object","additionalProperties":false,"required":["email"],"properties":{"email":{"type":"string","format":"email","maxLength":254}}},
        "Setup": {"type":"object","additionalProperties":false,"required":["token","password"],"properties":{"token":{"type":"string","maxLength":512},"password":{"type":"string","minLength":12,"maxLength":1024,"writeOnly":true}}},
        "Login": {"type":"object","additionalProperties":false,"required":["email","password"],"properties":{"email":{"type":"string","format":"email"},"password":{"type":"string","maxLength":1024,"writeOnly":true}}},
        "CreateApp": {"type":"object","additionalProperties":false,"required":["name"],"properties":{"name":{"type":"string","minLength":1,"maxLength":200}}},
        "CreateCredential": {"type":"object","additionalProperties":false,"required":["app_id","kind"],"properties":{"app_id":{"type":"string","format":"uuid"},"kind":{"type":"string","enum":["application","agent"]}}},
        "Error": {"type":"object","required":["error"],"properties":{"error":{"type":"string"}}},
        "ProvisionedUser": {"type":"object","required":["user_id","workspace_id","setup_token","expires_in_seconds"],"properties":{"user_id":{"type":"string","format":"uuid"},"workspace_id":{"type":"string","format":"uuid"},"setup_token":{"type":"string"},"expires_in_seconds":{"type":"integer","const":86400}}},
        "Session": {"type":"object","required":["token","csrf_token","expires_in_seconds"],"properties":{"token":{"type":"string"},"csrf_token":{"type":"string"},"expires_in_seconds":{"type":"integer","const":86400}}},
        "App": {"type":"object","required":["id","workspace_id","name"],"properties":{"id":{"type":"string","format":"uuid"},"workspace_id":{"type":"string","format":"uuid"},"name":{"type":"string"}}},
        "Credential": {"type":"object","required":["id","workspace_id","app_id","kind","agent_id","token"],"properties":{"id":{"type":"string","format":"uuid"},"workspace_id":{"type":"string","format":"uuid"},"app_id":{"type":"string","format":"uuid"},"kind":{"type":"string","enum":["application","agent"]},"agent_id":{"type":["string","null"],"format":"uuid"},"token":{"type":"string"}}}
    });
    // Reuse the existing Principal contract directly, without forking its schema.
    let protocol: Value =
        serde_json::from_str(include_str!("../../contracts/v1/schema.json")).unwrap();
    schemas["Principal"] = protocol["$defs"]["principal"].clone();
    let mut paths = json!({});
    let security = json!([{"bearerAuth":[]},{"browserSession":[],"csrfToken":[]}]);
    for (path, method, operation_id, input, output, status, protected) in [
        (
            "/api/v1/admin/users",
            "post",
            "provisionUser",
            "CreateUser",
            "ProvisionedUser",
            "201",
            true,
        ),
        (
            "/api/v1/session",
            "post",
            "createSession",
            "Login",
            "Session",
            "200",
            false,
        ),
        (
            "/api/v1/session",
            "get",
            "getSession",
            "",
            "Principal",
            "200",
            true,
        ),
        (
            "/api/v1/session",
            "delete",
            "revokeSession",
            "",
            "",
            "204",
            true,
        ),
        (
            "/api/v1/session/setup",
            "post",
            "setupPassword",
            "Setup",
            "",
            "204",
            false,
        ),
        (
            "/api/v1/workspaces/{id}/apps",
            "post",
            "createApp",
            "CreateApp",
            "App",
            "201",
            true,
        ),
        (
            "/api/v1/workspaces/{id}/apps",
            "get",
            "listApps",
            "",
            "Apps",
            "200",
            true,
        ),
        (
            "/api/v1/workspaces/{id}/credentials",
            "post",
            "createCredential",
            "CreateCredential",
            "Credential",
            "201",
            true,
        ),
        (
            "/api/v1/workspaces/{id}/credentials/{credential_id}",
            "delete",
            "revokeCredential",
            "",
            "",
            "204",
            true,
        ),
    ] {
        let mut operation = json!({"operationId":operation_id,"responses":{}});
        operation["security"] = if protected {
            security.clone()
        } else {
            json!([])
        };
        if !input.is_empty() {
            operation["requestBody"] = json!({"required":true,"content":{"application/json":{"schema":{"$ref":format!("#/components/schemas/{input}")}}}});
        }
        operation["responses"][status] = json!({"description":"Success"});
        if !output.is_empty() {
            let schema = if output == "Apps" {
                json!({"type":"array","items":{"$ref":"#/components/schemas/App"}})
            } else {
                json!({"$ref":format!("#/components/schemas/{output}")})
            };
            operation["responses"][status]["content"] =
                json!({"application/json":{"schema":schema}});
        }
        for (code, description) in [
            ("400", "Malformed request"),
            ("401", "Invalid or expired credentials"),
            ("403", "Insufficient scope or invalid CSRF token"),
            ("409", "Duplicate email"),
            ("413", "Request body too large"),
            ("415", "Expected JSON"),
            ("422", "Invalid request fields"),
            ("500", "Internal error"),
        ] {
            operation["responses"][code] = json!({"description":description});
        }
        if path == "/api/v1/session" && method == "post" {
            operation["responses"]["429"] =
                json!({"description":"Password verification capacity is exhausted"});
        }
        if path.contains("{id}") {
            operation["parameters"] = json!([{"name":"id","in":"path","required":true,"schema":{"type":"string","format":"uuid"}}]);
        }
        if path.contains("{credential_id}") {
            operation["parameters"].as_array_mut().unwrap().push(json!({"name":"credential_id","in":"path","required":true,"schema":{"type":"string","format":"uuid"}}));
        }
        paths[path][method] = operation;
    }
    json!({"openapi":"3.1.0","info":{"title":"RelayHub v1 Identity","version":"1.0.0","description":"Only administrators provision accounts. Sessions last 24 hours. Cookie writes require x-csrf-token; cookies require TLS. Setup tokens expire in 24 hours and can be used once. Application keys may connect/publish only for their app; agent keys may connect/receive deliveries only for their app. Secrets are returned once and never stored in plaintext."},"paths":paths,"components":{"schemas":schemas,"securitySchemes":{"bearerAuth":{"type":"http","scheme":"bearer"},"browserSession":{"type":"apiKey","in":"cookie","name":COOKIE},"csrfToken":{"type":"apiKey","in":"header","name":"x-csrf-token"}}}})
}
