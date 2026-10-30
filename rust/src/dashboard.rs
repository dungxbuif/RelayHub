//! Same-origin dashboard adapters. Durable modules remain the source of truth.
use crate::{
    delivery::{Deliveries, DeliveryError},
    events::{AcceptError, Events},
    identity::{Action, AuthError, Identity, authorize},
    presence::Presence,
    protocol::{Accepted, EndpointPolicy, EventInput, Principal},
    realtime::{Realtime, RealtimeError},
    webhooks::{EndpointAuth, EventStatus, WebhookError, Webhooks},
};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, patch, post},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

const MAX_PAGE: i64 = 100;

#[derive(Clone)]
pub struct Dashboard {
    pool: sqlx::PgPool,
    identity: Identity,
    events: Events,
    webhooks: Webhooks,
    deliveries: Deliveries,
    presence: Presence,
    realtime: Realtime,
}
impl Dashboard {
    pub(crate) fn new(
        pool: sqlx::PgPool,
        identity: Identity,
        events: Events,
        webhooks: Webhooks,
        deliveries: Deliveries,
        presence: Presence,
        realtime: Realtime,
    ) -> Self {
        Self {
            pool,
            identity,
            events,
            webhooks,
            deliveries,
            presence,
            realtime,
        }
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route(
                "/api/v1/dashboard/endpoints",
                get(list_endpoints).post(create_endpoint),
            )
            .route(
                "/api/v1/dashboard/endpoints/{endpoint}",
                patch(update_endpoint).put(update_endpoint),
            )
            .route("/api/v1/dashboard/events", post(publish_event))
            .route("/api/v1/dashboard/agents", get(list_agents))
            .route("/api/v1/dashboard/deliveries", get(list_deliveries))
            .route(
                "/api/v1/dashboard/deliveries/{delivery}",
                get(delivery_status),
            )
            .route(
                "/api/v1/dashboard/deliveries/{delivery}/replay",
                post(replay),
            )
            .route(
                "/api/v1/dashboard/deliveries/{delivery}/audit",
                get(replay_audit),
            )
            .route("/api/v1/dashboard/events/{event}", get(event_status))
            .route("/api/v1/dashboard/realtime-token", post(realtime_token))
            .with_state(self.clone())
    }
}

#[derive(Debug)]
enum DashboardError {
    Auth(AuthError),
    Accept(AcceptError),
    Webhook(WebhookError),
    Delivery(DeliveryError),
    Realtime(RealtimeError),
    Database,
}
impl From<AuthError> for DashboardError {
    fn from(error: AuthError) -> Self {
        Self::Auth(error)
    }
}
impl From<AcceptError> for DashboardError {
    fn from(error: AcceptError) -> Self {
        Self::Accept(error)
    }
}
impl From<WebhookError> for DashboardError {
    fn from(error: WebhookError) -> Self {
        Self::Webhook(error)
    }
}
impl From<DeliveryError> for DashboardError {
    fn from(error: DeliveryError) -> Self {
        Self::Delivery(error)
    }
}
impl From<RealtimeError> for DashboardError {
    fn from(error: RealtimeError) -> Self {
        Self::Realtime(error)
    }
}
impl From<sqlx::Error> for DashboardError {
    fn from(_: sqlx::Error) -> Self {
        Self::Database
    }
}
impl IntoResponse for DashboardError {
    fn into_response(self) -> Response {
        match self {
            Self::Auth(error) => error.into_response(),
            Self::Accept(error) => error.into_response(),
            Self::Webhook(error) => error.into_response(),
            Self::Delivery(error) => error.into_response(),
            Self::Realtime(error) => error.into_response(),
            Self::Database => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error":"internal_error"})),
            )
                .into_response(),
        }
    }
}

async fn browser(
    identity: &Identity,
    headers: &HeaderMap,
    write: bool,
) -> Result<Principal, DashboardError> {
    Ok(identity.browser_auth(headers, write).await?)
}
fn owner_workspace(principal: &Principal) -> Result<Option<Uuid>, DashboardError> {
    match principal {
        Principal::WorkspaceOwner { workspace_id, .. } => Ok(Some(*workspace_id)),
        Principal::SystemAdmin { .. } => Ok(None),
        _ => Err(DashboardError::Auth(AuthError::Forbidden)),
    }
}
async fn app_workspace(
    pool: &sqlx::PgPool,
    principal: &Principal,
    app: Uuid,
) -> Result<Uuid, DashboardError> {
    let workspace: Uuid = sqlx::query_scalar("SELECT workspace_id FROM identity_apps WHERE id=$1")
        .bind(app)
        .fetch_optional(pool)
        .await?
        .ok_or(DashboardError::Auth(AuthError::Forbidden))?;
    authorize(principal, workspace, Action::ManageApps)?;
    Ok(workspace)
}
async fn endpoint_workspace(
    pool: &sqlx::PgPool,
    principal: &Principal,
    endpoint: Uuid,
) -> Result<Uuid, DashboardError> {
    let workspace: Uuid =
        sqlx::query_scalar("SELECT workspace_id FROM event_endpoints WHERE id=$1")
            .bind(endpoint)
            .fetch_optional(pool)
            .await?
            .ok_or(DashboardError::Auth(AuthError::Forbidden))?;
    authorize(principal, workspace, Action::ManageApps)?;
    Ok(workspace)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CreateEndpoint {
    app_id: Uuid,
    policy: EndpointPolicy,
    auth: EndpointAuthInput,
}
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum EndpointAuthInput {
    Public,
    Bearer { secret: String },
}
impl From<EndpointAuthInput> for EndpointAuth {
    fn from(auth: EndpointAuthInput) -> Self {
        match auth {
            EndpointAuthInput::Public => Self::Public,
            EndpointAuthInput::Bearer { secret } => Self::Bearer(secret),
        }
    }
}
#[derive(Serialize)]
struct EndpointCreated {
    id: Uuid,
    app_id: Uuid,
    policy: EndpointPolicy,
    auth: &'static str,
}
async fn create_endpoint(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Json(input): Json<CreateEndpoint>,
) -> Result<(StatusCode, Json<EndpointCreated>), DashboardError> {
    let principal = browser(&dashboard.identity, &headers, true).await?;
    let workspace = app_workspace(&dashboard.pool, &principal, input.app_id).await?;
    let policy = input.policy.clone();
    let auth = match &input.auth {
        EndpointAuthInput::Public => "public",
        EndpointAuthInput::Bearer { .. } => "bearer",
    };
    let id = dashboard
        .webhooks
        .create_endpoint(
            &principal,
            workspace,
            input.app_id,
            input.policy,
            input.auth.into(),
        )
        .await?;
    Ok((
        StatusCode::CREATED,
        Json(EndpointCreated {
            id,
            app_id: input.app_id,
            policy,
            auth,
        }),
    ))
}

#[derive(Serialize)]
struct EndpointView {
    id: Uuid,
    app_id: Uuid,
    policy: EndpointPolicy,
    auth: &'static str,
}

async fn list_endpoints(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
) -> Result<Json<Vec<EndpointView>>, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, false).await?;
    let workspace = owner_workspace(&principal)?;
    let rows = sqlx::query(
        "SELECT id, app_id, policy::text, public_ingress FROM event_endpoints WHERE ($1::uuid IS NULL OR workspace_id=$1) ORDER BY id",
    )
    .bind(workspace)
    .fetch_all(&dashboard.pool)
    .await?;
    let endpoints = rows
        .into_iter()
        .map(|row| {
            Ok(EndpointView {
                id: row.get("id"),
                app_id: row.get("app_id"),
                policy: serde_json::from_str(row.get("policy"))
                    .map_err(|_| DashboardError::Database)?,
                auth: if row.get::<bool, _>("public_ingress") {
                    "public"
                } else {
                    "bearer"
                },
            })
        })
        .collect::<Result<Vec<_>, DashboardError>>()?;
    Ok(Json(endpoints))
}

async fn publish_event(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Json(input): Json<EventInput>,
) -> Result<(StatusCode, Json<Accepted>), DashboardError> {
    let principal = browser(&dashboard.identity, &headers, true).await?;
    if headers.get_all("idempotency-key").iter().count() != 1 {
        return Err(AcceptError::InvalidInput.into());
    }
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .ok_or(AcceptError::InvalidInput)?;
    let accepted = dashboard
        .events
        .accept_event(&principal, input, key)
        .await?;
    Ok((StatusCode::ACCEPTED, Json(accepted)))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UpdateEndpoint {
    policy: EndpointPolicy,
}
async fn update_endpoint(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Path(endpoint): Path<Uuid>,
    Json(input): Json<UpdateEndpoint>,
) -> Result<(StatusCode, Json<EndpointView>), DashboardError> {
    let principal = browser(&dashboard.identity, &headers, true).await?;
    let workspace = endpoint_workspace(&dashboard.pool, &principal, endpoint).await?;
    dashboard
        .webhooks
        .update_policy(&principal, workspace, endpoint, input.policy)
        .await?;
    let row = sqlx::query(
        "SELECT app_id, policy::text, public_ingress FROM event_endpoints WHERE id=$1 AND workspace_id=$2",
    )
    .bind(endpoint)
    .bind(workspace)
    .fetch_one(&dashboard.pool)
    .await?;
    Ok((
        StatusCode::OK,
        Json(EndpointView {
            id: endpoint,
            app_id: row.get("app_id"),
            policy: serde_json::from_str(row.get("policy"))
                .map_err(|_| DashboardError::Database)?,
            auth: if row.get::<bool, _>("public_ingress") {
                "public"
            } else {
                "bearer"
            },
        }),
    ))
}

#[derive(Serialize)]
struct AgentView {
    workspace_id: Uuid,
    agent_id: Uuid,
    app_id: Uuid,
    gateway_id: Option<Uuid>,
    session_id: Option<Uuid>,
}
async fn list_agents(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
) -> Result<Json<Vec<AgentView>>, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, false).await?;
    let workspace = owner_workspace(&principal)?;
    let rows = sqlx::query("SELECT id, workspace_id, app_id, agent_id FROM identity_credentials WHERE kind='agent' AND revoked_at IS NULL AND ($1::uuid IS NULL OR workspace_id=$1) ORDER BY id LIMIT 1000")
        .bind(workspace).fetch_all(&dashboard.pool).await?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let workspace_id: Uuid = row.get("workspace_id");
        let agent_id: Uuid = row.get("agent_id");
        let app_id: Uuid = row.get("app_id");
        let agent = Principal::Agent {
            workspace_id,
            app_id,
            agent_id,
            key_id: row.get("id"),
        };
        let owner = dashboard
            .presence
            .owner(&agent)
            .await
            .map_err(DashboardError::Delivery)?;
        result.push(AgentView {
            workspace_id,
            agent_id,
            app_id,
            gateway_id: owner.as_ref().map(|o| o.gateway_id),
            session_id: owner.map(|o| o.session_id),
        });
    }
    Ok(Json(result))
}

#[derive(Deserialize, Default)]
struct DeliveryQuery {
    cursor: Option<Uuid>,
    limit: Option<i64>,
}
#[derive(Serialize)]
struct DeliveryItem {
    delivery_id: Uuid,
    event_id: Uuid,
    app_id: Uuid,
    generation: i32,
    state: String,
    attempts: i32,
}
#[derive(Serialize)]
struct DeliveryPage {
    items: Vec<DeliveryItem>,
    next_cursor: Option<Uuid>,
}
async fn list_deliveries(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Query(query): Query<DeliveryQuery>,
) -> Result<Json<DeliveryPage>, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, false).await?;
    let workspace = owner_workspace(&principal)?;
    let limit = query.limit.unwrap_or(50).clamp(1, MAX_PAGE);
    let rows = if let Some(cursor) = query.cursor {
        sqlx::query("SELECT id,event_id,app_id,generation,state,attempt_count FROM deliveries WHERE ($1::uuid IS NULL OR workspace_id=$1) AND id>$2 ORDER BY id LIMIT $3")
            .bind(workspace).bind(cursor).bind(limit).fetch_all(&dashboard.pool).await?
    } else {
        sqlx::query("SELECT id,event_id,app_id,generation,state,attempt_count FROM deliveries WHERE ($1::uuid IS NULL OR workspace_id=$1) ORDER BY id LIMIT $2")
            .bind(workspace).bind(limit).fetch_all(&dashboard.pool).await?
    };
    let items = rows
        .into_iter()
        .map(|row| DeliveryItem {
            delivery_id: row.get("id"),
            event_id: row.get("event_id"),
            app_id: row.get("app_id"),
            generation: row.get("generation"),
            state: row.get("state"),
            attempts: row.get("attempt_count"),
        })
        .collect::<Vec<_>>();
    Ok(Json(DeliveryPage {
        next_cursor: items.last().map(|item| item.delivery_id),
        items,
    }))
}

async fn delivery_status(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Path(delivery): Path<Uuid>,
) -> Result<Json<crate::delivery::DeliveryView>, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, false).await?;
    Ok(Json(
        dashboard
            .deliveries
            .read_delivery(&principal, delivery)
            .await?,
    ))
}

#[derive(Serialize)]
struct ReplayAudit {
    source_delivery_id: Uuid,
    replay_delivery_id: Uuid,
    actor_user_id: Uuid,
}
#[derive(Serialize)]
struct ReplayResult {
    accepted: Accepted,
    audit: ReplayAudit,
}

#[derive(Serialize)]
struct ReplayAuditView {
    delivery_id: Uuid,
    source_delivery_id: Uuid,
    actor_user_id: Uuid,
    created_at: String,
}

async fn replay_audit(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Path(delivery): Path<Uuid>,
) -> Result<Json<Vec<ReplayAuditView>>, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, false).await?;
    let row = sqlx::query(
        "SELECT r.delivery_id, r.source_delivery_id, r.actor_user_id, r.created_at::text, d.workspace_id FROM delivery_replays r JOIN deliveries d ON d.id=r.delivery_id WHERE r.delivery_id=$1",
    )
    .bind(delivery)
    .fetch_optional(&dashboard.pool)
    .await?
    .ok_or(DashboardError::Auth(AuthError::Forbidden))?;
    authorize(&principal, row.get("workspace_id"), Action::ManageApps)?;
    Ok(Json(vec![ReplayAuditView {
        delivery_id: row.get("delivery_id"),
        source_delivery_id: row.get("source_delivery_id"),
        actor_user_id: row.get("actor_user_id"),
        created_at: row.get("created_at"),
    }]))
}
async fn replay(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Path(delivery): Path<Uuid>,
) -> Result<Json<ReplayResult>, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, true).await?;
    let actor_user_id = match principal {
        Principal::SystemAdmin { user_id } | Principal::WorkspaceOwner { user_id, .. } => user_id,
        _ => return Err(DashboardError::Auth(AuthError::Forbidden)),
    };
    let accepted = dashboard
        .deliveries
        .replay_delivery(&principal, delivery)
        .await?;
    Ok(Json(ReplayResult {
        audit: ReplayAudit {
            source_delivery_id: delivery,
            replay_delivery_id: accepted.delivery_id,
            actor_user_id,
        },
        accepted,
    }))
}

async fn event_status(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Path(event): Path<Uuid>,
) -> Result<Json<EventStatus>, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, false).await?;
    Ok(Json(
        dashboard.webhooks.event_status(&principal, event).await?,
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RealtimeTokenRequest {
    app_id: Uuid,
    channel: String,
}
#[derive(Serialize)]
struct RealtimeTokenResponse {
    token: String,
    workspace_id: Uuid,
    channel: String,
    expires_in_seconds: u64,
}
async fn realtime_token(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Json(input): Json<RealtimeTokenRequest>,
) -> Result<Json<RealtimeTokenResponse>, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, true).await?;
    let workspace = app_workspace(&dashboard.pool, &principal, input.app_id).await?;
    let token = dashboard
        .realtime
        .issue_scoped_token(workspace, input.app_id, &input.channel)
        .await?;
    Ok(Json(RealtimeTokenResponse {
        token,
        workspace_id: workspace,
        channel: input.channel,
        expires_in_seconds: crate::realtime::SCOPED_TOKEN_TTL_SECONDS,
    }))
}
