//! Durable queue Admin configuration surface.
//!
//! Subscription and schedule configuration is persisted in Rust. Delivery
//! pull/settlement remains explicitly unavailable until the queue worker and
//! its lease protocol are migrated; those handlers return bounded empty or
//! unavailable results rather than silently claiming work was processed.
use crate::identity::{Action, AuthError, Identity, authorize};
use crate::protocol::Principal;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::Row;
use uuid::Uuid;

#[derive(Debug, Deserialize, Default)]
struct SubscriptionInput {
    name: Option<String>,
    enabled: Option<bool>,
    max_in_flight: Option<i32>,
    max_batch_size: Option<i32>,
    ordering_mode: Option<String>,
    max_dispatch_rate: Option<i32>,
    success_callback_url: Option<String>,
    failure_callback_url: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct SubscriptionPatch {
    policy_version: Option<i32>,
    name: Option<String>,
    enabled: Option<bool>,
    max_in_flight: Option<i32>,
    max_batch_size: Option<i32>,
    ordering_mode: Option<String>,
    max_dispatch_rate: Option<i32>,
    success_callback_url: Option<String>,
    failure_callback_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ScheduleInput {
    name: String,
    enabled: Option<bool>,
    cron_expression: String,
    timezone: String,
    event_type: String,
    data: Value,
    ordering_key: Option<String>,
    priority: Option<i32>,
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize, Default)]
struct DrainInput {
    timeout_seconds: Option<u64>,
}

#[derive(Debug, Deserialize, Default)]
struct BatchInput {
    items: Vec<Value>,
}

#[derive(Debug, Deserialize, Default)]
struct DeadLetterInput {
    delivery_ids: Vec<Uuid>,
    confirm: bool,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Default)]
struct CursorQuery {
    limit: Option<u32>,
    cursor: Option<Uuid>,
}

const SUB_COLUMNS: &str = "id,workspace_id,app_id,name,enabled,to_char(paused_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS paused_at,to_char(draining_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS draining_at,to_char(drained_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS drained_at,max_in_flight,max_batch_size,ordering_mode,max_dispatch_rate,success_callback_url,failure_callback_url,policy_version,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at";
const SCHEDULE_COLUMNS: &str = "id,subscription_id,name,enabled,cron_expression,timezone,event_type,data_text,ordering_key,priority,metadata_text,to_char(next_run_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS next_run_at,to_char(last_run_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS last_run_at,policy_version";

pub fn router() -> Router<Identity> {
    Router::new()
        .route(
            "/api/v1/admin/apps/{app_id}/subscriptions",
            get(list_subscriptions).post(create_subscription),
        )
        .route(
            "/api/v1/admin/apps/{app_id}/subscriptions/{subscription_id}",
            get(get_subscription)
                .patch(update_subscription)
                .delete(delete_subscription),
        )
        .route(
            "/api/v1/admin/apps/{app_id}/subscriptions/{subscription_id}/pause",
            post(pause_subscription),
        )
        .route(
            "/api/v1/admin/apps/{app_id}/subscriptions/{subscription_id}/resume",
            post(resume_subscription),
        )
        .route(
            "/api/v1/admin/apps/{app_id}/subscriptions/{subscription_id}/pull",
            post(pull),
        )
        .route(
            "/api/v1/admin/apps/{app_id}/subscriptions/{subscription_id}/settle",
            post(settle),
        )
        .route(
            "/api/v1/admin/apps/{app_id}/subscriptions/{subscription_id}/leases/extend",
            post(extend_leases),
        )
        .route(
            "/api/v1/admin/apps/{app_id}/subscriptions/{subscription_id}/metrics",
            get(metrics),
        )
        .route(
            "/api/v1/admin/apps/{app_id}/subscriptions/{subscription_id}/drain",
            get(get_drain).post(start_drain),
        )
        .route(
            "/api/v1/admin/apps/{app_id}/subscriptions/{subscription_id}/schedules",
            get(list_schedules).post(create_schedule),
        )
        .route(
            "/api/v1/admin/apps/{app_id}/subscriptions/{subscription_id}/schedules/{schedule_id}",
            get(get_schedule)
                .put(update_schedule)
                .delete(delete_schedule),
        )
        .route(
            "/api/v1/admin/apps/{app_id}/subscriptions/{subscription_id}/callbacks",
            get(list_callbacks),
        )
        .route(
            "/api/v1/admin/apps/{app_id}/subscriptions/{subscription_id}/dead-letters",
            get(list_dead_letters),
        )
        .route(
            "/api/v1/admin/apps/{app_id}/subscriptions/{subscription_id}/dead-letters/export",
            get(export_dead_letters),
        )
        .route(
            "/api/v1/admin/apps/{app_id}/subscriptions/{subscription_id}/dead-letters/replay",
            post(replay_dead_letters),
        )
        .route(
            "/api/v1/admin/apps/{app_id}/subscriptions/{subscription_id}/dead-letters/delete",
            post(delete_dead_letters),
        )
}

async fn app_workspace(
    identity: &Identity,
    principal: &Principal,
    app_id: Uuid,
) -> Result<Uuid, AuthError> {
    let workspace: Uuid =
        sqlx::query_scalar("SELECT workspace_id FROM identity_apps WHERE id=$1 AND enabled")
            .bind(app_id)
            .fetch_optional(&identity.pool())
            .await?
            .ok_or(AuthError::Conflict)?;
    authorize(principal, workspace, Action::ManageApps)?;
    Ok(workspace)
}

async fn subscription_scope(
    identity: &Identity,
    principal: &Principal,
    app_id: Uuid,
    subscription_id: Uuid,
) -> Result<Uuid, AuthError> {
    let workspace = app_workspace(identity, principal, app_id).await?;
    let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM queue_subscriptions WHERE id=$1 AND app_id=$2 AND workspace_id=$3)")
        .bind(subscription_id).bind(app_id).bind(workspace).fetch_one(&identity.pool()).await?;
    if valid {
        Ok(workspace)
    } else {
        Err(AuthError::Conflict)
    }
}

fn name(value: Option<String>) -> Result<String, AuthError> {
    let value = value.ok_or(AuthError::InvalidInput)?;
    let value = value.trim();
    if value.is_empty() || value.len() > 64 || value.chars().any(char::is_control) {
        return Err(AuthError::InvalidInput);
    }
    Ok(value.to_owned())
}

fn positive(value: Option<i32>, default: i32, max: i32) -> Result<i32, AuthError> {
    let value = value.unwrap_or(default);
    if (1..=max).contains(&value) {
        Ok(value)
    } else {
        Err(AuthError::InvalidInput)
    }
}

fn sub_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({
        "id": row.get::<Uuid,_>("id"), "app_id": row.get::<Uuid,_>("app_id"),
        "name": row.get::<String,_>("name"), "enabled": row.get::<bool,_>("enabled"),
        "paused_at": row.get::<Option<String>,_>("paused_at"),
        "draining_at": row.get::<Option<String>,_>("draining_at"),
        "drained_at": row.get::<Option<String>,_>("drained_at"),
        "max_in_flight": row.get::<i32,_>("max_in_flight"), "max_batch_size": row.get::<i32,_>("max_batch_size"),
        "ordering_mode": row.get::<String,_>("ordering_mode"), "max_dispatch_rate": row.get::<Option<i32>,_>("max_dispatch_rate"),
        "success_callback_url": row.get::<Option<String>,_>("success_callback_url"), "failure_callback_url": row.get::<Option<String>,_>("failure_callback_url"),
        "policy_version": row.get::<i32,_>("policy_version"), "created_at": row.get::<String,_>("created_at"), "updated_at": row.get::<String,_>("updated_at"),
    })
}

// sqlx's Postgres timestamp types are intentionally not enabled in the minimal
// dependency set. Keep timestamp fields as bounded strings in the wire object.
async fn fetch_subscription(
    identity: &Identity,
    id: Uuid,
    app_id: Uuid,
) -> Result<sqlx::postgres::PgRow, AuthError> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {SUB_COLUMNS} FROM queue_subscriptions WHERE id=$1 AND app_id=$2"
    )))
    .bind(id)
    .bind(app_id)
    .fetch_optional(&identity.pool())
    .await?
    .ok_or(AuthError::Conflict)
}

async fn list_subscriptions(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path(app_id): Path<Uuid>,
) -> Result<Json<Value>, AuthError> {
    let principal = identity.admin_browser_auth(&headers, false).await?;
    app_workspace(&identity, &principal, app_id).await?;
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {SUB_COLUMNS} FROM queue_subscriptions WHERE app_id=$1 ORDER BY created_at,id"
    )))
    .bind(app_id)
    .fetch_all(&identity.pool())
    .await?;
    Ok(Json(
        json!({"items": rows.iter().map(sub_json).collect::<Vec<_>>() }),
    ))
}

async fn create_subscription(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path(app_id): Path<Uuid>,
    Json(input): Json<SubscriptionInput>,
) -> Result<(StatusCode, Json<Value>), AuthError> {
    let principal = identity.admin_browser_auth(&headers, true).await?;
    let workspace = app_workspace(&identity, &principal, app_id).await?;
    let sub_name = name(input.name)?;
    let in_flight = positive(input.max_in_flight, 10, 10_000)?;
    let batch = positive(input.max_batch_size, 10, 100)?;
    let ordering = input.ordering_mode.unwrap_or_else(|| "none".to_owned());
    if ordering != "none" && ordering != "key" {
        return Err(AuthError::InvalidInput);
    }
    let row = sqlx::query(sqlx::AssertSqlSafe(format!("INSERT INTO queue_subscriptions(id,workspace_id,app_id,name,enabled,max_in_flight,max_batch_size,ordering_mode,max_dispatch_rate,success_callback_url,failure_callback_url) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING {SUB_COLUMNS}")))
        .bind(Uuid::new_v4()).bind(workspace).bind(app_id).bind(sub_name).bind(input.enabled.unwrap_or(true)).bind(in_flight).bind(batch).bind(ordering).bind(input.max_dispatch_rate).bind(input.success_callback_url).bind(input.failure_callback_url).fetch_one(&identity.pool()).await?;
    Ok((StatusCode::CREATED, Json(sub_json(&row))))
}

async fn get_subscription(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, subscription_id)): Path<(Uuid, Uuid)>,
) -> Result<Json<Value>, AuthError> {
    let principal = identity.admin_browser_auth(&headers, false).await?;
    subscription_scope(&identity, &principal, app_id, subscription_id).await?;
    Ok(Json(sub_json(
        &fetch_subscription(&identity, subscription_id, app_id).await?,
    )))
}

async fn update_subscription(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, subscription_id)): Path<(Uuid, Uuid)>,
    Json(input): Json<SubscriptionPatch>,
) -> Result<Json<Value>, AuthError> {
    let principal = identity.admin_browser_auth(&headers, true).await?;
    subscription_scope(&identity, &principal, app_id, subscription_id).await?;
    if let Some(version) = input.policy_version {
        if version < 1 {
            return Err(AuthError::InvalidInput);
        }
    }
    let ordering = input.ordering_mode.clone();
    if ordering
        .as_deref()
        .is_some_and(|v| v != "none" && v != "key")
    {
        return Err(AuthError::InvalidInput);
    }
    let in_flight = input
        .max_in_flight
        .map(|v| positive(Some(v), 10, 10_000))
        .transpose()?;
    let batch = input
        .max_batch_size
        .map(|v| positive(Some(v), 10, 100))
        .transpose()?;
    let row = sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE queue_subscriptions SET name=COALESCE($2,name),enabled=COALESCE($3,enabled),max_in_flight=COALESCE($4,max_in_flight),max_batch_size=COALESCE($5,max_batch_size),ordering_mode=COALESCE($6,ordering_mode),max_dispatch_rate=COALESCE($7,max_dispatch_rate),success_callback_url=COALESCE($8,success_callback_url),failure_callback_url=COALESCE($9,failure_callback_url),policy_version=policy_version+1,updated_at=clock_timestamp() WHERE id=$1 AND app_id=$10 RETURNING {SUB_COLUMNS}")))
        .bind(subscription_id).bind(input.name.map(|v| v.trim().to_owned())).bind(input.enabled).bind(in_flight).bind(batch).bind(ordering).bind(input.max_dispatch_rate).bind(input.success_callback_url).bind(input.failure_callback_url).bind(app_id).fetch_optional(&identity.pool()).await?.ok_or(AuthError::Conflict)?;
    Ok(Json(sub_json(&row)))
}

async fn set_paused(
    identity: &Identity,
    app_id: Uuid,
    id: Uuid,
    paused: bool,
) -> Result<Value, AuthError> {
    let row = sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE queue_subscriptions SET paused_at=CASE WHEN $2 THEN clock_timestamp() ELSE NULL END,enabled=CASE WHEN $2 THEN false ELSE true END,updated_at=clock_timestamp() WHERE id=$1 AND app_id=$3 RETURNING {SUB_COLUMNS}")))
        .bind(id).bind(paused).bind(app_id).fetch_optional(&identity.pool()).await?.ok_or(AuthError::Conflict)?;
    Ok(sub_json(&row))
}

async fn pause_subscription(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, id)): Path<(Uuid, Uuid)>,
) -> Result<Json<Value>, AuthError> {
    let p = identity.admin_browser_auth(&headers, true).await?;
    subscription_scope(&identity, &p, app_id, id).await?;
    Ok(Json(set_paused(&identity, app_id, id, true).await?))
}
async fn resume_subscription(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, id)): Path<(Uuid, Uuid)>,
) -> Result<Json<Value>, AuthError> {
    let p = identity.admin_browser_auth(&headers, true).await?;
    subscription_scope(&identity, &p, app_id, id).await?;
    Ok(Json(set_paused(&identity, app_id, id, false).await?))
}

async fn delete_subscription(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, AuthError> {
    let p = identity.admin_browser_auth(&headers, true).await?;
    subscription_scope(&identity, &p, app_id, id).await?;
    if headers
        .get("x-relayhub-confirm")
        .and_then(|v| v.to_str().ok())
        != Some("true")
    {
        return Err(AuthError::InvalidInput);
    }
    sqlx::query("DELETE FROM queue_subscriptions WHERE id=$1 AND app_id=$2")
        .bind(id)
        .bind(app_id)
        .execute(&identity.pool())
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn pull(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, id)): Path<(Uuid, Uuid)>,
    Json(_input): Json<Value>,
) -> Result<Json<Value>, AuthError> {
    let p = identity.admin_browser_auth(&headers, true).await?;
    subscription_scope(&identity, &p, app_id, id).await?;
    Ok(Json(
        json!({"items": [], "degraded_components": ["queue_worker_unavailable"]}),
    ))
}
async fn batch_unavailable(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, id)): Path<(Uuid, Uuid)>,
    Json(input): Json<BatchInput>,
) -> Result<Json<Value>, AuthError> {
    let p = identity.admin_browser_auth(&headers, true).await?;
    subscription_scope(&identity, &p, app_id, id).await?;
    Ok(Json(
        json!({"items": input.items.into_iter().map(|item| json!({"receipt": item.get("receipt").cloned().unwrap_or(Value::Null), "status": "queue_worker_unavailable"})).collect::<Vec<_>>(), "degraded_components": ["queue_worker_unavailable"]}),
    ))
}
async fn settle(
    State(identity): State<Identity>,
    headers: HeaderMap,
    path: Path<(Uuid, Uuid)>,
    Json(input): Json<BatchInput>,
) -> Result<Json<Value>, AuthError> {
    batch_unavailable(State(identity), headers, path, Json(input)).await
}
async fn extend_leases(
    State(identity): State<Identity>,
    headers: HeaderMap,
    path: Path<(Uuid, Uuid)>,
    Json(input): Json<BatchInput>,
) -> Result<Json<Value>, AuthError> {
    batch_unavailable(State(identity), headers, path, Json(input)).await
}

async fn metrics(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, id)): Path<(Uuid, Uuid)>,
) -> Result<Json<Value>, AuthError> {
    let p = identity.admin_browser_auth(&headers, false).await?;
    subscription_scope(&identity, &p, app_id, id).await?;
    Ok(Json(
        json!({"available":0,"in_flight":0,"acknowledged":0,"dead_letter":0,"oldest_available_at":null,"degraded_components":["queue_worker_unavailable"]}),
    ))
}
async fn start_drain(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, id)): Path<(Uuid, Uuid)>,
    Json(input): Json<DrainInput>,
) -> Result<(StatusCode, Json<Value>), AuthError> {
    let p = identity.admin_browser_auth(&headers, true).await?;
    subscription_scope(&identity, &p, app_id, id).await?;
    let timeout = input.timeout_seconds.unwrap_or(30).clamp(1, 3600) as i64;
    let row=sqlx::query("UPDATE queue_subscriptions SET draining_at=clock_timestamp(),drained_at=NULL,updated_at=clock_timestamp() WHERE id=$1 AND app_id=$2 RETURNING to_char(draining_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS started_at").bind(id).bind(app_id).fetch_one(&identity.pool()).await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            json!({"subscription_id":id,"status":"draining","in_flight":0,"started_at":row.get::<String,_>("started_at"),"deadline_at":format!("{}", timeout)}),
        ),
    ))
}
async fn get_drain(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, id)): Path<(Uuid, Uuid)>,
) -> Result<Json<Value>, AuthError> {
    let p = identity.admin_browser_auth(&headers, false).await?;
    subscription_scope(&identity, &p, app_id, id).await?;
    let row = fetch_subscription(&identity, id, app_id).await?;
    Ok(Json(
        json!({"subscription_id":id,"status":if row.get::<Option<String>,_>("drained_at").is_some(){"drained"}else if row.get::<Option<String>,_>("draining_at").is_some(){"draining"}else{"active"},"in_flight":0,"started_at":row.get::<Option<String>,_>("draining_at"),"completed_at":row.get::<Option<String>,_>("drained_at")}),
    ))
}

fn schedule_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({"id":row.get::<Uuid,_>("id"),"name":row.get::<String,_>("name"),"enabled":row.get::<bool,_>("enabled"),"cron_expression":row.get::<String,_>("cron_expression"),"timezone":row.get::<String,_>("timezone"),"event_type":row.get::<String,_>("event_type"),"next_run_at":row.get::<String,_>("next_run_at"),"last_run_at":row.get::<Option<String>,_>("last_run_at"),"policy_version":row.get::<i32,_>("policy_version")})
}
async fn list_schedules(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, id)): Path<(Uuid, Uuid)>,
) -> Result<Json<Value>, AuthError> {
    let p = identity.admin_browser_auth(&headers, false).await?;
    subscription_scope(&identity, &p, app_id, id).await?;
    let rows=sqlx::query(sqlx::AssertSqlSafe(format!("SELECT {SCHEDULE_COLUMNS} FROM queue_schedules WHERE subscription_id=$1 ORDER BY created_at,id"))).bind(id).fetch_all(&identity.pool()).await?;
    Ok(Json(
        json!({"items":rows.iter().map(schedule_json).collect::<Vec<_>>() }),
    ))
}
async fn create_schedule(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, id)): Path<(Uuid, Uuid)>,
    Json(input): Json<ScheduleInput>,
) -> Result<(StatusCode, Json<Value>), AuthError> {
    let p = identity.admin_browser_auth(&headers, true).await?;
    let workspace = subscription_scope(&identity, &p, app_id, id).await?;
    if input.name.trim().is_empty()
        || input.name.len() > 64
        || input.cron_expression.trim().is_empty()
        || input.timezone.trim().is_empty()
        || input.event_type.trim().is_empty()
    {
        return Err(AuthError::InvalidInput);
    }
    let row=sqlx::query(sqlx::AssertSqlSafe(format!("INSERT INTO queue_schedules(id,workspace_id,app_id,subscription_id,name,enabled,cron_expression,timezone,event_type,data_text,ordering_key,priority,metadata_text) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING {SCHEDULE_COLUMNS}"))).bind(Uuid::new_v4()).bind(workspace).bind(app_id).bind(id).bind(input.name.trim()).bind(input.enabled.unwrap_or(true)).bind(input.cron_expression.trim()).bind(input.timezone.trim()).bind(input.event_type.trim()).bind(input.data.to_string()).bind(input.ordering_key).bind(input.priority.unwrap_or(0)).bind(input.metadata.unwrap_or_else(||json!({})).to_string()).fetch_one(&identity.pool()).await?;
    Ok((StatusCode::CREATED, Json(schedule_json(&row))))
}
async fn update_schedule(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, sub_id, schedule_id)): Path<(Uuid, Uuid, Uuid)>,
    Json(input): Json<ScheduleInput>,
) -> Result<Json<Value>, AuthError> {
    let p = identity.admin_browser_auth(&headers, true).await?;
    subscription_scope(&identity, &p, app_id, sub_id).await?;
    let row=sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE queue_schedules SET name=$2,enabled=COALESCE($3,enabled),cron_expression=$4,timezone=$5,event_type=$6,data_text=$7,ordering_key=$8,priority=COALESCE($9,priority),metadata_text=$10,policy_version=policy_version+1,updated_at=clock_timestamp() WHERE id=$1 AND subscription_id=$11 RETURNING {SCHEDULE_COLUMNS}"))).bind(schedule_id).bind(input.name.trim()).bind(input.enabled).bind(input.cron_expression.trim()).bind(input.timezone.trim()).bind(input.event_type.trim()).bind(input.data.to_string()).bind(input.ordering_key).bind(input.priority).bind(input.metadata.unwrap_or_else(||json!({})).to_string()).bind(sub_id).fetch_optional(&identity.pool()).await?.ok_or(AuthError::Conflict)?;
    Ok(Json(schedule_json(&row)))
}
async fn get_schedule(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, sub_id, schedule_id)): Path<(Uuid, Uuid, Uuid)>,
) -> Result<Json<Value>, AuthError> {
    let p = identity.admin_browser_auth(&headers, false).await?;
    subscription_scope(&identity, &p, app_id, sub_id).await?;
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {SCHEDULE_COLUMNS} FROM queue_schedules WHERE id=$1 AND subscription_id=$2"
    )))
    .bind(schedule_id)
    .bind(sub_id)
    .fetch_optional(&identity.pool())
    .await?
    .ok_or(AuthError::Conflict)?;
    Ok(Json(schedule_json(&row)))
}
async fn delete_schedule(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, sub_id, schedule_id)): Path<(Uuid, Uuid, Uuid)>,
) -> Result<StatusCode, AuthError> {
    let p = identity.admin_browser_auth(&headers, true).await?;
    subscription_scope(&identity, &p, app_id, sub_id).await?;
    if headers
        .get("x-relayhub-confirm")
        .and_then(|v| v.to_str().ok())
        != Some("true")
    {
        return Err(AuthError::InvalidInput);
    }
    sqlx::query("DELETE FROM queue_schedules WHERE id=$1 AND subscription_id=$2")
        .bind(schedule_id)
        .bind(sub_id)
        .execute(&identity.pool())
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
async fn list_callbacks(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, id)): Path<(Uuid, Uuid)>,
) -> Result<Json<Value>, AuthError> {
    let p = identity.admin_browser_auth(&headers, false).await?;
    subscription_scope(&identity, &p, app_id, id).await?;
    Ok(Json(
        json!({"items":[],"degraded_components":["queue_worker_unavailable"]}),
    ))
}
async fn list_dead_letters(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, id)): Path<(Uuid, Uuid)>,
    Query(_query): Query<CursorQuery>,
) -> Result<Json<Value>, AuthError> {
    let p = identity.admin_browser_auth(&headers, false).await?;
    subscription_scope(&identity, &p, app_id, id).await?;
    Ok(Json(json!({"items":[]})))
}
async fn export_dead_letters(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, id)): Path<(Uuid, Uuid)>,
    Query(_query): Query<CursorQuery>,
) -> Result<Json<Value>, AuthError> {
    list_dead_letters(
        State(identity),
        headers,
        Path((app_id, id)),
        Query(CursorQuery::default()),
    )
    .await
}
async fn replay_dead_letters(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, id)): Path<(Uuid, Uuid)>,
    Json(input): Json<DeadLetterInput>,
) -> Result<Json<Value>, AuthError> {
    let p = identity.admin_browser_auth(&headers, true).await?;
    subscription_scope(&identity, &p, app_id, id).await?;
    if !input.confirm || input.delivery_ids.len() > 100 {
        return Err(AuthError::InvalidInput);
    }
    Ok(Json(
        json!({"replayed":0,"degraded_components":["queue_worker_unavailable"]}),
    ))
}
async fn delete_dead_letters(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, id)): Path<(Uuid, Uuid)>,
    Json(input): Json<DeadLetterInput>,
) -> Result<Json<Value>, AuthError> {
    let p = identity.admin_browser_auth(&headers, true).await?;
    subscription_scope(&identity, &p, app_id, id).await?;
    if !input.confirm || input.delivery_ids.len() > 100 {
        return Err(AuthError::InvalidInput);
    }
    Ok(Json(
        json!({"deleted":0,"degraded_components":["queue_worker_unavailable"]}),
    ))
}
