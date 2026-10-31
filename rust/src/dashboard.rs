//! Same-origin dashboard adapters. Durable modules remain the source of truth.
use crate::{
    delivery::{Deliveries, DeliveryError},
    events::{AcceptError, Events},
    identity::{Action, AuthError, Identity, authorize},
    presence::Presence,
    protocol::{Accepted, EndpointPolicy, EventInput, Principal},
    realtime::{
        PushNotificationInput as RealtimePushNotificationInput, Realtime, RealtimeError,
        push_config, send_push,
    },
    webhooks::{EndpointAuth, EventStatus, WebhookError, Webhooks},
};
use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, patch, post},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
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
            .route("/api/v1/admin/studio/token", post(studio_token))
            .route("/api/v1/admin/studio/publish", post(studio_publish))
            .route("/api/v1/admin/connections", get(admin_connections))
            .route(
                "/api/v1/admin/connections/{connection}",
                axum::routing::delete(admin_disconnect_connection),
            )
            .route(
                "/api/v1/admin/apps/{app_id}/connections",
                get(admin_app_connections),
            )
            .route(
                "/api/v1/admin/apps/{app_id}/connections/{connection}",
                get(admin_app_connection).delete(admin_app_disconnect_connection),
            )
            .route(
                "/api/v1/admin/apps/{app_id}/realtime/channels/{channel}/history",
                get(admin_realtime_history),
            )
            .route(
                "/api/v1/admin/apps/{app_id}/realtime/files/{file_id}",
                get(admin_realtime_file),
            )
            .route(
                "/api/v1/admin/apps/{app_id}/realtime/files/{file_id}/download",
                get(admin_realtime_file_download),
            )
            .route(
                "/api/v1/admin/apps/{app_id}/realtime/push/devices",
                get(admin_push_devices).post(admin_push_register),
            )
            .route(
                "/api/v1/admin/apps/{app_id}/realtime/push/devices/{device_id}",
                axum::routing::delete(admin_push_delete),
            )
            .route(
                "/api/v1/admin/apps/{app_id}/realtime/push/channels/{channel}/devices/{device_id}",
                axum::routing::put(admin_push_bind).delete(admin_push_unbind),
            )
            .route(
                "/api/v1/admin/apps/{app_id}/realtime/push/channels/{channel}/notifications",
                post(admin_push_notify),
            )
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
    let admin_cookie = headers
        .get_all(axum::http::header::COOKIE)
        .iter()
        .any(|value| {
            value.to_str().ok().is_some_and(|raw| {
                raw.split(';')
                    .any(|part| part.trim().starts_with("__Host-relayhub_admin="))
            })
        });
    if admin_cookie {
        Ok(identity.admin_browser_auth(headers, write).await?)
    } else {
        Ok(identity.browser_auth(headers, write).await?)
    }
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
    online: bool,
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
            online: owner.is_some(),
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
        sqlx::query("SELECT id,event_id,target_app_id,generation,state,attempt_count FROM deliveries WHERE ($1::uuid IS NULL OR workspace_id=$1) AND id>$2 ORDER BY id LIMIT $3")
            .bind(workspace).bind(cursor).bind(limit).fetch_all(&dashboard.pool).await?
    } else {
        sqlx::query("SELECT id,event_id,target_app_id,generation,state,attempt_count FROM deliveries WHERE ($1::uuid IS NULL OR workspace_id=$1) ORDER BY id LIMIT $2")
            .bind(workspace).bind(limit).fetch_all(&dashboard.pool).await?
    };
    let items = rows
        .into_iter()
        .map(|row| DeliveryItem {
            delivery_id: row.get("id"),
            event_id: row.get("event_id"),
            app_id: row.get("target_app_id"),
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StudioTokenRequest {
    app_id: Uuid,
    protocol: String,
    client_id: Option<String>,
    channels: Option<Vec<String>>,
}

async fn studio_token(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Json(input): Json<StudioTokenRequest>,
) -> Result<Json<Value>, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, true).await?;
    let workspace = app_workspace(&dashboard.pool, &principal, input.app_id).await?;
    if !matches!(
        input.protocol.as_str(),
        "realtime_v2" | "realtime" | "stream"
    ) {
        return Err(DashboardError::Realtime(RealtimeError::InvalidInput));
    }
    let channel = input
        .channels
        .and_then(|channels| channels.into_iter().next())
        .unwrap_or_else(|| "studio".to_owned());
    let token = if input.protocol == "realtime_v2" {
        let client_id = input
            .client_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or(DashboardError::Realtime(RealtimeError::InvalidInput))?;
        dashboard
            .realtime
            .issue_v2_scoped_token(workspace, input.app_id, client_id, &[channel.clone()])
            .await?
    } else {
        dashboard
            .realtime
            .issue_scoped_token(workspace, input.app_id, &channel)
            .await?
    };
    let expires_at: String = sqlx::query_scalar(
        "SELECT to_char((clock_timestamp() + interval '60 seconds') AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')",
    )
    .fetch_one(&dashboard.pool)
    .await?;
    Ok(Json(json!({
        "token": token,
        "expires_at": expires_at,
        "protocol": input.protocol,
        "client_id": input.client_id,
        "channel": channel,
    })))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StudioPublishRequest {
    app_id: Uuid,
    channel: String,
    data: Value,
}

async fn studio_publish(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Json(input): Json<StudioPublishRequest>,
) -> Result<(StatusCode, Json<Value>), DashboardError> {
    let principal = browser(&dashboard.identity, &headers, true).await?;
    let workspace = app_workspace(&dashboard.pool, &principal, input.app_id).await?;
    let application = Principal::Application {
        workspace_id: workspace,
        app_id: input.app_id,
        key_id: Uuid::nil(),
    };
    let body = serde_json::to_vec(&input.data)
        .map_err(|_| DashboardError::Auth(AuthError::InvalidInput))?;
    let receipt = dashboard
        .realtime
        .publish_realtime(&application, &input.channel, &body)
        .await?;
    Ok((StatusCode::ACCEPTED, Json(json!(receipt))))
}

async fn admin_connections(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Value>, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, false).await?;
    let app_id = query
        .get("app_id")
        .ok_or(DashboardError::Auth(AuthError::InvalidInput))?
        .parse::<Uuid>()
        .map_err(|_| DashboardError::Auth(AuthError::InvalidInput))?;
    app_workspace(&dashboard.pool, &principal, app_id).await?;
    Ok(Json(
        json!({"connections": dashboard.realtime.list_connections(app_id, 100).await?}),
    ))
}

async fn admin_disconnect_connection(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Path(connection): Path<String>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Result<StatusCode, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, true).await?;
    let app_id = query
        .get("app_id")
        .ok_or(DashboardError::Auth(AuthError::InvalidInput))?
        .parse::<Uuid>()
        .map_err(|_| DashboardError::Auth(AuthError::InvalidInput))?;
    app_workspace(&dashboard.pool, &principal, app_id).await?;
    dashboard
        .realtime
        .disconnect_connection(app_id, &connection)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn admin_app_connections(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Path(app_id): Path<Uuid>,
) -> Result<Json<Value>, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, false).await?;
    app_workspace(&dashboard.pool, &principal, app_id).await?;
    Ok(Json(
        json!({"connections": dashboard.realtime.list_connections(app_id, 100).await?}),
    ))
}

async fn admin_app_connection(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Path((app_id, connection)): Path<(Uuid, String)>,
) -> Result<Json<Value>, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, false).await?;
    app_workspace(&dashboard.pool, &principal, app_id).await?;
    let item = dashboard
        .realtime
        .list_connections(app_id, 500)
        .await?
        .into_iter()
        .find(|value| {
            value.get("connection_id").and_then(Value::as_str) == Some(connection.as_str())
        })
        .ok_or(DashboardError::Auth(AuthError::Conflict))?;
    Ok(Json(item))
}

async fn admin_app_disconnect_connection(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Path((app_id, connection)): Path<(Uuid, String)>,
) -> Result<StatusCode, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, true).await?;
    app_workspace(&dashboard.pool, &principal, app_id).await?;
    dashboard
        .realtime
        .disconnect_connection(app_id, &connection)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn admin_realtime_history(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Path((app_id, channel)): Path<(Uuid, String)>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Value>, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, false).await?;
    let workspace = app_workspace(&dashboard.pool, &principal, app_id).await?;
    let app_principal = Principal::Application {
        workspace_id: workspace,
        app_id,
        key_id: Uuid::nil(),
    };
    let limit = query
        .get("limit")
        .and_then(|value| value.parse().ok())
        .unwrap_or(100);
    Ok(Json(
        dashboard
            .realtime
            .history(
                &app_principal,
                &channel,
                query.get("cursor").map(String::as_str),
                limit,
            )
            .await?,
    ))
}

async fn admin_realtime_file(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Path((app_id, file_id)): Path<(Uuid, String)>,
) -> Result<Json<Value>, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, false).await?;
    app_workspace(&dashboard.pool, &principal, app_id).await?;
    let row = sqlx::query("SELECT id,app_id,channel,name,mime_type,size_bytes,sha256,status,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(expires_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS expires_at,to_char(completed_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS completed_at FROM realtime_files WHERE id=$1 AND app_id=$2")
        .bind(&file_id).bind(app_id).fetch_optional(&dashboard.pool).await?
        .ok_or(DashboardError::Auth(AuthError::Conflict))?;
    Ok(Json(json!({
        "id": row.get::<String, _>("id"), "app_id": row.get::<Uuid, _>("app_id"),
        "channel": row.get::<String, _>("channel"), "name": row.get::<String, _>("name"),
        "mime_type": row.get::<String, _>("mime_type"), "size_bytes": row.get::<i64, _>("size_bytes"),
        "sha256": row.get::<String, _>("sha256"), "status": row.get::<String, _>("status"),
        "created_at": row.get::<String, _>("created_at"), "expires_at": row.get::<String, _>("expires_at"),
        "completed_at": row.get::<Option<String>, _>("completed_at"),
    })))
}

async fn admin_realtime_file_download(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Path((app_id, file_id)): Path<(Uuid, String)>,
) -> Result<Response, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, false).await?;
    app_workspace(&dashboard.pool, &principal, app_id).await?;
    let row = sqlx::query("SELECT status,object_key,name,mime_type FROM realtime_files WHERE id=$1 AND app_id=$2 AND expires_at>clock_timestamp()")
        .bind(&file_id).bind(app_id).fetch_optional(&dashboard.pool).await?
        .ok_or(DashboardError::Auth(AuthError::Conflict))?;
    if row.get::<String, _>("status") != "ready" {
        return Err(DashboardError::Auth(AuthError::Conflict));
    }
    let store = crate::object_store::ObjectStore::from_env()
        .map_err(|_| DashboardError::Realtime(RealtimeError::Unavailable))?;
    let object_key = row.get::<String, _>("object_key");
    let upstream = store
        .get(&object_key)
        .await
        .map_err(|_| DashboardError::Realtime(RealtimeError::Unavailable))?;
    let bytes = upstream
        .bytes()
        .await
        .map_err(|_| DashboardError::Realtime(RealtimeError::Unavailable))?;
    Response::builder()
        .status(StatusCode::OK)
        .header("cache-control", "no-store")
        .header("content-type", row.get::<String, _>("mime_type"))
        .header(
            "content-disposition",
            format!(
                "attachment; filename=\"{}\"",
                row.get::<String, _>("name").replace('"', "")
            ),
        )
        .body(Body::from(bytes))
        .map_err(|_| DashboardError::Realtime(RealtimeError::Unavailable))
}

async fn admin_push_devices(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Path(app_id): Path<Uuid>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<Value>, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, false).await?;
    app_workspace(&dashboard.pool, &principal, app_id).await?;
    let rows = if let Some(channel) = query.get("channel") {
        sqlx::query("SELECT d.id,d.app_id,d.provider,to_char(d.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(d.updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at FROM realtime_push_bindings b JOIN realtime_push_devices d ON d.id=b.device_id AND d.app_id=b.app_id WHERE b.app_id=$1 AND b.channel=$2 ORDER BY d.created_at,d.id")
            .bind(app_id).bind(channel).fetch_all(&dashboard.pool).await?
    } else {
        sqlx::query("SELECT id,app_id,provider,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at FROM realtime_push_devices WHERE app_id=$1 ORDER BY created_at,id")
            .bind(app_id).fetch_all(&dashboard.pool).await?
    };
    Ok(Json(json!({"devices": rows.into_iter().map(|row| json!({
        "id": row.get::<Uuid,_>("id"), "app_id": row.get::<Uuid,_>("app_id"),
        "provider": row.get::<String,_>("provider"), "created_at": row.get::<String,_>("created_at"),
        "updated_at": row.get::<String,_>("updated_at")
    })).collect::<Vec<_>>() })))
}

async fn admin_push_register(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Path(app_id): Path<Uuid>,
    Json(input): Json<PushRegisterInput>,
) -> Result<Json<Value>, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, true).await?;
    app_workspace(&dashboard.pool, &principal, app_id).await?;
    let provider = input.provider.trim().to_ascii_lowercase();
    if !matches!(provider.as_str(), "fcm" | "apns")
        || input.token.len() < 16
        || input.token.len() > 4096
        || input.token.chars().any(char::is_control)
    {
        return Err(DashboardError::Auth(AuthError::InvalidInput));
    }
    let token_hash = Sha256::digest(input.token.as_bytes()).to_vec();
    let encrypted = dashboard.identity.encrypt_secret(input.token.as_bytes())?;
    let row = sqlx::query("INSERT INTO realtime_push_devices(id,app_id,provider,token_hash,encrypted_token) VALUES($1,$2,$3,$4,$5) ON CONFLICT(app_id,provider,token_hash) DO UPDATE SET encrypted_token=EXCLUDED.encrypted_token,updated_at=clock_timestamp() RETURNING id,app_id,provider,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at")
        .bind(Uuid::new_v4()).bind(app_id).bind(provider).bind(token_hash).bind(encrypted).fetch_one(&dashboard.pool).await?;
    Ok(Json(
        json!({"id":row.get::<Uuid,_>("id"),"app_id":row.get::<Uuid,_>("app_id"),"provider":row.get::<String,_>("provider"),"created_at":row.get::<String,_>("created_at"),"updated_at":row.get::<String,_>("updated_at")}),
    ))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PushRegisterInput {
    provider: String,
    token: String,
}

async fn admin_push_delete(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Path((app_id, device_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, true).await?;
    app_workspace(&dashboard.pool, &principal, app_id).await?;
    let deleted = sqlx::query("DELETE FROM realtime_push_devices WHERE app_id=$1 AND id=$2")
        .bind(app_id)
        .bind(device_id)
        .execute(&dashboard.pool)
        .await?;
    if deleted.rows_affected() != 1 {
        return Err(DashboardError::Auth(AuthError::Conflict));
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn admin_push_bind(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Path((app_id, channel, device_id)): Path<(Uuid, String, Uuid)>,
) -> Result<StatusCode, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, true).await?;
    app_workspace(&dashboard.pool, &principal, app_id).await?;
    if channel.is_empty() || channel.len() > 128 || channel.chars().any(char::is_control) {
        return Err(DashboardError::Auth(AuthError::InvalidInput));
    }
    sqlx::query("INSERT INTO realtime_push_bindings(app_id,channel,device_id) SELECT $1,$2,id FROM realtime_push_devices WHERE app_id=$1 AND id=$3 ON CONFLICT DO NOTHING")
        .bind(app_id).bind(channel).bind(device_id).execute(&dashboard.pool).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn admin_push_unbind(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Path((app_id, channel, device_id)): Path<(Uuid, String, Uuid)>,
) -> Result<StatusCode, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, true).await?;
    app_workspace(&dashboard.pool, &principal, app_id).await?;
    let deleted = sqlx::query(
        "DELETE FROM realtime_push_bindings WHERE app_id=$1 AND channel=$2 AND device_id=$3",
    )
    .bind(app_id)
    .bind(channel)
    .bind(device_id)
    .execute(&dashboard.pool)
    .await?;
    if deleted.rows_affected() != 1 {
        return Err(DashboardError::Auth(AuthError::Conflict));
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn admin_push_notify(
    State(dashboard): State<Dashboard>,
    headers: HeaderMap,
    Path((app_id, channel)): Path<(Uuid, String)>,
    Json(input): Json<PushNotificationInput>,
) -> Result<Json<Value>, DashboardError> {
    let principal = browser(&dashboard.identity, &headers, true).await?;
    app_workspace(&dashboard.pool, &principal, app_id).await?;
    if channel.is_empty()
        || channel.len() > 128
        || input.title.len() > 100
        || input.body.len() > 500
        || input.data.to_string().len() > 4096
        || input.title.chars().any(char::is_control)
        || input.body.chars().any(char::is_control)
    {
        return Err(DashboardError::Auth(AuthError::InvalidInput));
    }
    let rows = sqlx::query("SELECT d.id,d.provider,d.encrypted_token FROM realtime_push_bindings b JOIN realtime_push_devices d ON d.id=b.device_id AND d.app_id=b.app_id WHERE b.app_id=$1 AND b.channel=$2 ORDER BY d.id LIMIT 100")
        .bind(app_id).bind(&channel).fetch_all(&dashboard.pool).await?;
    let mut outcomes = Vec::with_capacity(rows.len());
    for row in rows {
        let outcome_id = Uuid::new_v4();
        let device_id: Uuid = row.get("id");
        let provider: String = row.get("provider");
        let mut status = "failed";
        let mut reason = Some("provider_unavailable".to_owned());
        let mut provider_message_id = None;
        if let Some((endpoint, authorization, topic, project)) = push_config(&provider) {
            if let Ok(token) = dashboard
                .identity
                .decrypt_secret(row.get::<String, _>("encrypted_token").as_str())
            {
                let request = RealtimePushNotificationInput {
                    title: input.title.clone(),
                    body: input.body.clone(),
                    data: input.data.clone(),
                };
                if let Ok(message_id) = send_push(
                    &provider,
                    &endpoint,
                    &authorization,
                    topic.as_deref(),
                    project.as_deref(),
                    &token,
                    &request,
                )
                .await
                {
                    status = "delivered";
                    reason = None;
                    provider_message_id = message_id;
                } else {
                    reason = Some("provider_error".to_owned());
                }
            }
        }
        sqlx::query("INSERT INTO realtime_push_outcomes(id,app_id,device_id,channel,provider,status,provider_message_id,reason) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
            .bind(outcome_id).bind(app_id).bind(device_id).bind(&channel).bind(&provider).bind(status).bind(&provider_message_id).bind(&reason).execute(&dashboard.pool).await?;
        outcomes.push(json!({"id":outcome_id,"app_id":app_id,"device_id":device_id,"channel":channel,"provider":provider,"status":status,"provider_message_id":provider_message_id,"reason":reason}));
    }
    Ok(Json(json!({"outcomes": outcomes})))
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PushNotificationInput {
    title: String,
    body: String,
    #[serde(default)]
    data: Value,
}
