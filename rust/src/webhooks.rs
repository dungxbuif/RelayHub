//! Host-facing HTTP adapter. The host supplies Task 2 authentication for status reads.
use crate::{
    delivery::Deliveries,
    events::{AcceptError, Events},
    identity::{Action, authorize},
    presence::Presence,
    protocol::{
        Accepted, EndpointPolicy, EventInput, Headers, HttpResponse, MAX_BODY_BYTES, OfflinePolicy,
        Principal, TimeoutPolicy,
    },
    responses::{REDIS_BUDGET, ResponseError, Responses},
};
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier, password_hash::SaltString};
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Path, State},
    http::{HeaderName, HeaderValue, Request, StatusCode},
    response::{IntoResponse, Response},
    routing::any,
};
use serde::Serialize;
use sqlx::Row;
use std::sync::Arc;
use tokio::sync::Semaphore;
use uuid::Uuid;

pub type HttpRequest = Request<Vec<u8>>;
// Deliberately no Debug: this configuration contains a plaintext bearer secret transiently.
pub enum EndpointAuth {
    Bearer(String),
    Public,
}
#[derive(Clone)]
pub struct Webhooks {
    pool: sqlx::PgPool,
    events: Events,
    presence: Presence,
    responses: Responses,
    admission: Arc<Semaphore>,
}
#[derive(Debug)]
pub enum WebhookError {
    Unauthorized,
    Forbidden,
    Unavailable,
    Accept(AcceptError),
    Response(ResponseError),
    Internal,
}
impl From<sqlx::Error> for WebhookError {
    fn from(_: sqlx::Error) -> Self {
        Self::Internal
    }
}
impl From<AcceptError> for WebhookError {
    fn from(e: AcceptError) -> Self {
        Self::Accept(e)
    }
}
impl From<ResponseError> for WebhookError {
    fn from(e: ResponseError) -> Self {
        Self::Response(e)
    }
}
impl IntoResponse for WebhookError {
    fn into_response(self) -> Response {
        let (status, error) = match self {
            Self::Unauthorized => (StatusCode::UNAUTHORIZED, "invalid_ingress_secret"),
            Self::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
            Self::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "agent_offline"),
            Self::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
            Self::Accept(e) => return e.into_response(),
            Self::Response(e) => return e.into_response(),
        };
        (status, Json(serde_json::json!({"error":error}))).into_response()
    }
}
#[derive(Debug, Serialize)]
pub struct EventStatus {
    pub event_id: Uuid,
    pub delivery_id: Uuid,
    pub generation: i32,
    pub state: String,
    pub attempts: i32,
    pub response_state: String,
    pub response: Option<HttpResponse>,
}

impl Webhooks {
    /// Construct once per PostgreSQL pool, then clone to share the admission budget.
    pub fn new(
        pool: sqlx::PgPool,
        presence: Presence,
        redis: redis::aio::MultiplexedConnection,
        namespace: &str,
    ) -> Result<Self, WebhookError> {
        let capacity = pool.options().get_max_connections() / 2;
        if capacity == 0 {
            return Err(AcceptError::InvalidInput.into());
        }
        let responses = Responses::new(
            pool.clone(),
            Deliveries::new(pool.clone(), presence.clone()),
            redis,
            namespace,
        )?;
        Ok(Self {
            events: Events::new(pool.clone()),
            pool,
            presence,
            responses,
            admission: Arc::new(Semaphore::new(capacity as usize)),
        })
    }
    /// Apply after Task 4 migration/version 3, before accepting public requests.
    pub async fn migrate(&self) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(727365011)")
            .execute(&mut *tx)
            .await?;
        let applied: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM identity_schema_migrations WHERE version=4)",
        )
        .fetch_one(&mut *tx)
        .await?;
        if !applied {
            sqlx::raw_sql(include_str!("../migrations/0004_endpoints.sql"))
                .execute(&mut *tx)
                .await?;
            sqlx::query("INSERT INTO identity_schema_migrations VALUES(4)")
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await
    }
    /// Existing endpoints carry immutable workspace/app ownership. Only owners/admins configure them.
    pub async fn configure_endpoint(
        &self,
        principal: &Principal,
        endpoint: Uuid,
        policy: EndpointPolicy,
        auth: EndpointAuth,
    ) -> Result<(), WebhookError> {
        policy.validate().map_err(|_| AcceptError::InvalidInput)?;
        let mut tx = self.pool.begin().await?;
        let workspace: Uuid =
            sqlx::query_scalar("SELECT workspace_id FROM event_endpoints WHERE id=$1 FOR UPDATE")
                .bind(endpoint)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(WebhookError::Forbidden)?;
        authorize(principal, workspace, Action::ManageApps).map_err(|_| WebhookError::Forbidden)?;
        let (public, hash) = ingress_auth(auth).await?;
        sqlx::query("UPDATE event_endpoints SET policy=$2::jsonb,public_ingress=$3,ingress_secret_hash=$4 WHERE id=$1")
            .bind(endpoint).bind(serde_json::to_string(&policy).map_err(|_|WebhookError::Internal)?).bind(public).bind(hash).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    /// Ownership and credentials are committed together; no partially configured endpoint escapes.
    pub(crate) async fn create_endpoint(
        &self,
        principal: &Principal,
        workspace: Uuid,
        app: Uuid,
        policy: EndpointPolicy,
        auth: EndpointAuth,
    ) -> Result<Uuid, WebhookError> {
        authorize(principal, workspace, Action::ManageApps).map_err(|_| WebhookError::Forbidden)?;
        policy.validate().map_err(|_| AcceptError::InvalidInput)?;
        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM identity_apps WHERE workspace_id=$1 AND id=$2)",
        )
        .bind(workspace)
        .bind(app)
        .fetch_one(&self.pool)
        .await?;
        if !exists {
            return Err(WebhookError::Forbidden);
        }
        let (public, hash) = ingress_auth(auth).await?;
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO event_endpoints(id,workspace_id,app_id,policy,public_ingress,ingress_secret_hash) VALUES($1,$2,$3,$4::jsonb,$5,$6)")
            .bind(id).bind(workspace).bind(app)
            .bind(serde_json::to_string(&policy).map_err(|_| WebhookError::Internal)?)
            .bind(public).bind(hash).execute(&self.pool).await?;
        Ok(id)
    }

    /// Policy edits preserve the one-time ingress secret and immutable stored ownership.
    pub(crate) async fn update_policy(
        &self,
        principal: &Principal,
        workspace: Uuid,
        endpoint: Uuid,
        policy: EndpointPolicy,
    ) -> Result<(), WebhookError> {
        authorize(principal, workspace, Action::ManageApps).map_err(|_| WebhookError::Forbidden)?;
        policy.validate().map_err(|_| AcceptError::InvalidInput)?;
        let changed = sqlx::query(
            "UPDATE event_endpoints SET policy=$3::jsonb WHERE workspace_id=$1 AND id=$2",
        )
        .bind(workspace)
        .bind(endpoint)
        .bind(serde_json::to_string(&policy).map_err(|_| WebhookError::Internal)?)
        .execute(&self.pool)
        .await?
        .rows_affected();
        if changed != 1 {
            return Err(WebhookError::Forbidden);
        }
        Ok(())
    }

    /// Dropping the caller's future must not abandon acceptance or resubmit durable work.
    pub async fn receive_webhook(
        &self,
        endpoint_id: Uuid,
        request: HttpRequest,
    ) -> Result<HttpResponse, WebhookError> {
        let service = self.clone();
        tokio::spawn(async move { service.receive(endpoint_id, request).await })
            .await
            .map_err(|_| WebhookError::Internal)?
    }
    async fn receive(
        &self,
        endpoint_id: Uuid,
        request: HttpRequest,
    ) -> Result<HttpResponse, WebhookError> {
        if request.body().len() > MAX_BODY_BYTES {
            return Err(AcceptError::PayloadTooLarge.into());
        }
        if request.method() == axum::http::Method::CONNECT
            || request.headers().contains_key("upgrade")
        {
            return Err(AcceptError::InvalidInput.into());
        }
        // Each admission uses one policy-lock connection and one acceptance connection.
        let admission = self
            .admission
            .acquire()
            .await
            .map_err(|_| WebhookError::Internal)?;
        let mut policy_lock = self.pool.begin().await?;
        let row = sqlx::query("SELECT workspace_id,app_id,policy::text,public_ingress,ingress_secret_hash FROM event_endpoints WHERE id=$1 FOR SHARE")
            .bind(endpoint_id).fetch_optional(&mut *policy_lock).await?.ok_or(WebhookError::Unauthorized)?;
        if !row.get::<bool, _>("public_ingress") {
            let hash: String = row
                .get::<Option<String>, _>("ingress_secret_hash")
                .ok_or(WebhookError::Unauthorized)?;
            let values: Vec<_> = request.headers().get_all("authorization").iter().collect();
            if values.len() != 1 {
                return Err(WebhookError::Unauthorized);
            }
            let token = values[0]
                .to_str()
                .ok()
                .and_then(|s| s.strip_prefix("Bearer "))
                .filter(|s| !s.is_empty() && s.len() <= 512)
                .ok_or(WebhookError::Unauthorized)?
                .to_owned();
            let valid = tokio::task::spawn_blocking(move || {
                PasswordHash::new(&hash).is_ok_and(|hash| {
                    Argon2::default()
                        .verify_password(token.as_bytes(), &hash)
                        .is_ok()
                })
            })
            .await
            .map_err(|_| WebhookError::Internal)?;
            if !valid {
                return Err(WebhookError::Unauthorized);
            }
        }
        let workspace: Uuid = row.get("workspace_id");
        let app: Uuid = row.get("app_id");
        let policy: EndpointPolicy =
            serde_json::from_str(row.get("policy")).map_err(|_| WebhookError::Internal)?;
        let keys: Vec<_> = request
            .headers()
            .get_all("idempotency-key")
            .iter()
            .collect();
        if keys.len() > 1 {
            return Err(AcceptError::InvalidInput.into());
        }
        let key = match keys.first() {
            Some(value) => value
                .to_str()
                .map_err(|_| AcceptError::InvalidInput)?
                .to_owned(),
            None => Uuid::new_v4().to_string(),
        };
        // Resolve a previously accepted receipt before applying current endpoint presence policy.
        let existing: Option<Uuid> = sqlx::query_scalar(
            "SELECT d.id FROM events e JOIN deliveries d ON d.event_id=e.id WHERE e.workspace_id=$1 AND e.app_id=$2 AND e.idempotency_key=$3 ORDER BY d.generation DESC LIMIT 1",
        )
        .bind(workspace)
        .bind(app)
        .bind(&key)
        .fetch_optional(&mut *policy_lock)
        .await?;
        let online = if existing.is_none() && matches!(policy, EndpointPolicy::Sync { .. }) {
            matches!(tokio::time::timeout(REDIS_BUDGET,self.presence.owners(workspace,app)).await,Ok(Ok(owners)) if !owners.is_empty())
        } else {
            false
        };
        if existing.is_none()
            && !online
            && matches!(
                policy,
                EndpointPolicy::Sync {
                    offline: OfflinePolicy::Reject,
                    ..
                }
            )
        {
            return Err(WebhookError::Unavailable);
        }
        let headers: Headers = request
            .headers()
            .iter()
            .map(|(name, value)| {
                value
                    .to_str()
                    .map(|value| (name.to_string(), value.to_owned()))
                    .map_err(|_| AcceptError::InvalidInput)
            })
            .collect::<Result<_, _>>()?;
        let input = EventInput {
            endpoint_id,
            event_type: "webhook".into(),
            method: request.method().to_string(),
            path: request
                .uri()
                .path_and_query()
                .map(|v| v.as_str())
                .unwrap_or("/")
                .to_owned(),
            headers: safe_headers(headers, true),
            body: request.into_body(),
        };
        // This server-created capability follows verified endpoint auth and stored ownership only.
        let principal = Principal::Application {
            workspace_id: workspace,
            app_id: app,
            key_id: endpoint_id,
        };
        let accepted = self.events.accept_event(&principal, input, &key).await?;
        policy_lock.commit().await?;
        drop(admission);
        if existing.is_some() {
            return self.resolve_existing(accepted).await;
        }
        if let EndpointPolicy::Sync { timeout, .. } = policy
            && online
        {
            if let Some(response) = self.responses.wait(accepted.delivery_id).await? {
                return Ok(response);
            }
            if timeout == TimeoutPolicy::ExpirePending {
                return json_response(
                    504,
                    &serde_json::json!({"event_id":accepted.event_id,"delivery_id":accepted.delivery_id,"error":"sync_timeout","outcome":"unknown_if_claimed"}),
                );
            }
        }
        json_response(202, &accepted)
    }

    async fn resolve_existing(&self, accepted: Accepted) -> Result<HttpResponse, WebhookError> {
        self.responses.expire_wait(accepted.delivery_id).await?;
        let row = sqlx::query("SELECT r.policy::text,r.state AS response_state,r.result,d.state AS delivery_state,r.deadline_at IS NOT NULL AS has_deadline FROM webhook_responses r JOIN deliveries d ON d.id=r.delivery_id WHERE r.delivery_id=$1")
            .bind(accepted.delivery_id)
            .fetch_one(&self.pool)
            .await?;
        let policy: EndpointPolicy =
            serde_json::from_str(row.get("policy")).map_err(|_| WebhookError::Internal)?;
        if matches!(policy, EndpointPolicy::Async) {
            return json_response(202, &accepted);
        }
        if row.get::<&str, _>("response_state") == "answered" {
            let result: Vec<u8> = row.get("result");
            return serde_json::from_slice(&result).map_err(|_| WebhookError::Internal);
        }
        let timed_out = row.get::<&str, _>("response_state") == "timed_out";
        if timed_out {
            return if timed_out
                && matches!(
                    policy,
                    EndpointPolicy::Sync {
                        timeout: TimeoutPolicy::ExpirePending,
                        ..
                    }
                ) {
                timeout_response(&accepted)
            } else {
                json_response(202, &accepted)
            };
        }
        if matches!(
            row.get::<&str, _>("delivery_state"),
            "cancelled" | "expired"
        ) {
            return if matches!(
                policy,
                EndpointPolicy::Sync {
                    timeout: TimeoutPolicy::ExpirePending,
                    ..
                }
            ) {
                timeout_response(&accepted)
            } else {
                json_response(202, &accepted)
            };
        }
        if matches!(policy, EndpointPolicy::Sync { .. }) && row.get::<bool, _>("has_deadline") {
            if let Some(response) = self.responses.wait(accepted.delivery_id).await? {
                return Ok(response);
            }
            if matches!(
                policy,
                EndpointPolicy::Sync {
                    timeout: TimeoutPolicy::ExpirePending,
                    ..
                }
            ) {
                return timeout_response(&accepted);
            }
        }
        json_response(202, &accepted)
    }

    pub async fn event_status(
        &self,
        principal: &Principal,
        event_id: Uuid,
    ) -> Result<EventStatus, WebhookError> {
        let row = sqlx::query("SELECT p.workspace_id,p.app_id,d.id FROM events e JOIN event_endpoints p ON p.id=e.endpoint_id JOIN deliveries d ON d.event_id=e.id WHERE e.id=$1 ORDER BY d.generation DESC LIMIT 1")
            .bind(event_id).fetch_optional(&self.pool).await?.ok_or(WebhookError::Forbidden)?;
        authorize(
            principal,
            row.get("workspace_id"),
            Action::Publish {
                app_id: row.get("app_id"),
            },
        )
        .map_err(|_| WebhookError::Forbidden)?;
        let delivery_id: Uuid = row.get("id");
        self.responses.expire_wait(delivery_id).await?;
        let row = sqlx::query("SELECT d.generation,d.state,d.attempt_count,r.state AS response_state,r.result FROM deliveries d JOIN webhook_responses r ON r.delivery_id=d.id WHERE d.id=$1")
            .bind(delivery_id).fetch_one(&self.pool).await?;
        let result: Option<Vec<u8>> = row.get("result");
        Ok(EventStatus {
            event_id,
            delivery_id,
            generation: row.get("generation"),
            state: row.get("state"),
            attempts: row.get("attempt_count"),
            response_state: row.get("response_state"),
            response: result
                .map(|bytes| serde_json::from_slice(&bytes))
                .transpose()
                .map_err(|_| WebhookError::Internal)?,
        })
    }

    pub(crate) fn ingress_router(&self) -> Router {
        Router::new()
            .route("/hooks/{endpoint_id}", any(ingress))
            .with_state(self.clone())
    }

    /// Mount status behind existing Task 2 middleware injecting Extension<Principal>.
    /// The legacy `/api/v1/events/{event_id}` contract is mounted by the compatibility
    /// adapter; keeping a second handler here would make Axum reject startup because
    /// both handlers own the same method/path. The dashboard status view remains at
    /// `/api/v1/dashboard/events/{event}`.
    pub fn router(&self) -> Router {
        self.ingress_router()
    }
}
fn json_response(status: u16, value: &impl Serialize) -> Result<HttpResponse, WebhookError> {
    Ok(HttpResponse {
        status,
        headers: vec![("content-type".into(), "application/json".into())],
        body: serde_json::to_vec(value).map_err(|_| WebhookError::Internal)?,
    })
}

fn timeout_response(accepted: &Accepted) -> Result<HttpResponse, WebhookError> {
    json_response(
        504,
        &serde_json::json!({
            "event_id": accepted.event_id,
            "delivery_id": accepted.delivery_id,
            "error": "sync_timeout",
            "outcome": "unknown_if_claimed"
        }),
    )
}

/// Strip hop-by-hop fields, Connection-nominated fields, and credentials before forwarding.
pub(crate) fn safe_headers(headers: Headers, ingress: bool) -> Headers {
    let nominated: Vec<String> = headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("connection"))
        .flat_map(|(_, value)| {
            value
                .split(',')
                .map(|name| name.trim().to_ascii_lowercase())
        })
        .collect();
    headers
        .into_iter()
        .filter(|(name, _)| {
            let name = name.to_ascii_lowercase();
            ![
                "connection",
                "keep-alive",
                "proxy-authenticate",
                "proxy-authorization",
                "te",
                "trailer",
                "transfer-encoding",
                "upgrade",
                "content-length",
                "authorization",
            ]
            .contains(&name.as_str())
                && !nominated.contains(&name)
                && !(ingress
                    && ([
                        "host",
                        "cookie",
                        "x-csrf-token",
                        "x-api-key",
                        "idempotency-key",
                        "forwarded",
                        "x-real-ip",
                    ]
                    .contains(&name.as_str())
                        || name.starts_with("x-forwarded-")
                        || name.starts_with("x-relayhub-")))
        })
        .collect()
}
async fn ingress(
    State(service): State<Webhooks>,
    Path(endpoint): Path<Uuid>,
    request: Request<Body>,
) -> Result<Response, WebhookError> {
    let (parts, body) = request.into_parts();
    let body = to_bytes(body, MAX_BODY_BYTES)
        .await
        .map_err(|_| AcceptError::PayloadTooLarge)?;
    let response = service
        .receive_webhook(endpoint, Request::from_parts(parts, body.to_vec()))
        .await?;
    let mut output = Response::new(Body::from(response.body));
    *output.status_mut() =
        StatusCode::from_u16(response.status).map_err(|_| WebhookError::Internal)?;
    for (name, value) in response.headers {
        output.headers_mut().append(
            HeaderName::from_bytes(name.as_bytes()).map_err(|_| WebhookError::Internal)?,
            HeaderValue::from_str(&value).map_err(|_| WebhookError::Internal)?,
        );
    }
    Ok(output)
}
async fn ingress_auth(auth: EndpointAuth) -> Result<(bool, Option<String>), WebhookError> {
    Ok(match auth {
        EndpointAuth::Public => (true, None),
        EndpointAuth::Bearer(secret) => {
            if secret.is_empty() || secret.len() > 512 {
                return Err(AcceptError::InvalidInput.into());
            }
            let hash = tokio::task::spawn_blocking(move || {
                let salt = SaltString::encode_b64(Uuid::new_v4().as_bytes())
                    .map_err(|_| WebhookError::Internal)?;
                Argon2::default()
                    .hash_password(secret.as_bytes(), &salt)
                    .map(|hash| hash.to_string())
                    .map_err(|_| WebhookError::Internal)
            })
            .await
            .map_err(|_| WebhookError::Internal)??;
            (false, Some(hash))
        }
    })
}
