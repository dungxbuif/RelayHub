//! Signed application-facing Queue v2 API.
//!
//! The Admin queue handlers use browser sessions.  These handlers use the
//! legacy signed application contract and share the same PostgreSQL queue
//! tables, so the public SDK does not need the old Go process during cutover.
use axum::{
    Json, Router,
    body::Bytes,
    extract::{OriginalUri, Path, Query, State},
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;
use uuid::Uuid;

use crate::identity::{AuthError, Identity};
use crate::protocol::Principal;

const SUB_COLUMNS: &str = "id,workspace_id,app_id,name,enabled,to_char(paused_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS paused_at,to_char(draining_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS draining_at,to_char(drain_deadline_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS drain_deadline_at,to_char(drained_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS drained_at,mode,event_types,max_attempts,default_visibility_seconds,max_visibility_seconds,max_total_lease_seconds,retention_seconds,max_in_flight,max_batch_size,retry_delay_seconds,ordering_mode,deduplication_seconds,max_dispatch_rate,success_callback_url,failure_callback_url,result_callback_metadata,policy_version,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at";
const SCHEDULE_COLUMNS: &str = "id,subscription_id,name,enabled,cron_expression,timezone,event_type,data_text,ordering_key,priority,metadata_text,to_char(next_run_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS next_run_at,to_char(last_run_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS last_run_at,policy_version";

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct SubscriptionInput {
    name: Option<String>,
    enabled: Option<bool>,
    event_types: Option<Vec<String>>,
    max_attempts: Option<i32>,
    default_visibility_seconds: Option<i32>,
    max_visibility_seconds: Option<i32>,
    max_total_lease_seconds: Option<i32>,
    retention_seconds: Option<i32>,
    max_in_flight: Option<i32>,
    max_batch_size: Option<i32>,
    retry_delay_seconds: Option<i32>,
    ordering_mode: Option<String>,
    deduplication_seconds: Option<i32>,
    max_dispatch_rate: Option<i32>,
    success_callback_url: Option<String>,
    failure_callback_url: Option<String>,
    result_callback_metadata: Option<Value>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct SubscriptionPatch {
    policy_version: Option<i32>,
    name: Option<String>,
    enabled: Option<bool>,
    event_types: Option<Vec<String>>,
    max_attempts: Option<i32>,
    default_visibility_seconds: Option<i32>,
    max_visibility_seconds: Option<i32>,
    max_total_lease_seconds: Option<i32>,
    retention_seconds: Option<i32>,
    max_in_flight: Option<i32>,
    max_batch_size: Option<i32>,
    retry_delay_seconds: Option<i32>,
    ordering_mode: Option<String>,
    deduplication_seconds: Option<i32>,
    max_dispatch_rate: Option<i32>,
    success_callback_url: Option<String>,
    failure_callback_url: Option<String>,
    result_callback_metadata: Option<Value>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct PullInput {
    max_messages: Option<u32>,
    wait_seconds: Option<u32>,
    visibility_seconds: Option<u32>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct SettlementInput {
    items: Vec<SettlementItem>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SettlementItem {
    receipt: String,
    disposition: String,
    delay_seconds: Option<u32>,
    reason: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct LeaseInput {
    items: Vec<LeaseItem>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LeaseItem {
    receipt: String,
    extension_seconds: u32,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct DrainInput {
    timeout_seconds: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
struct DeadLetterInput {
    delivery_ids: Vec<Uuid>,
}

#[derive(Debug, Deserialize, Default)]
struct CursorQuery {
    limit: Option<u32>,
    cursor: Option<Uuid>,
    format: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LegacyPayload {
    event_type: String,
    body: String,
}

pub fn router(identity: Identity) -> Router {
    Router::new()
        .route("/api/v2/subscriptions", get(list).post(create))
        .route(
            "/api/v2/subscriptions/{subscription_id}",
            get(get_one).put(update).delete(remove),
        )
        .route("/api/v2/subscriptions/{subscription_id}/pause", post(pause))
        .route(
            "/api/v2/subscriptions/{subscription_id}/resume",
            post(resume),
        )
        .route("/api/v2/subscriptions/{subscription_id}/pull", post(pull))
        .route(
            "/api/v2/subscriptions/{subscription_id}/settle",
            post(settle),
        )
        .route(
            "/api/v2/subscriptions/{subscription_id}/leases/extend",
            post(extend),
        )
        .route(
            "/api/v2/subscriptions/{subscription_id}/metrics",
            get(metrics),
        )
        .route(
            "/api/v2/subscriptions/{subscription_id}/drain",
            get(drain_status).post(drain),
        )
        .route(
            "/api/v2/subscriptions/{subscription_id}/schedules",
            get(list_schedules).post(create_schedule),
        )
        .route(
            "/api/v2/subscriptions/{subscription_id}/schedules/{schedule_id}",
            get(get_schedule)
                .put(update_schedule)
                .delete(delete_schedule),
        )
        .route(
            "/api/v2/subscriptions/{subscription_id}/dead-letters",
            get(dead_letters),
        )
        .route(
            "/api/v2/subscriptions/{subscription_id}/dead-letters/export",
            get(export_dead_letters),
        )
        .route(
            "/api/v2/subscriptions/{subscription_id}/dead-letters/replay",
            post(replay_dead_letters),
        )
        .route(
            "/api/v2/subscriptions/{subscription_id}/dead-letters/delete",
            post(delete_dead_letters),
        )
        .with_state(identity)
}

fn target(uri: &Uri) -> &str {
    uri.path_and_query()
        .map(|value| value.as_str())
        .unwrap_or(uri.path())
}

async fn app_scope(
    identity: &Identity,
    headers: &HeaderMap,
    method: &str,
    uri: &Uri,
    body: &[u8],
) -> Result<(Uuid, Uuid), AuthError> {
    let principal = identity
        .authenticate_public_request(headers, method, target(uri), body)
        .await?;
    match principal {
        Principal::Application {
            workspace_id,
            app_id,
            ..
        } => Ok((workspace_id, app_id)),
        _ => Err(AuthError::Forbidden),
    }
}

async fn subscription_scope(
    identity: &Identity,
    workspace_id: Uuid,
    app_id: Uuid,
    subscription_id: Uuid,
) -> Result<(), AuthError> {
    let valid: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM queue_subscriptions WHERE id=$1 AND app_id=$2 AND workspace_id=$3)")
        .bind(subscription_id)
        .bind(app_id)
        .bind(workspace_id)
        .fetch_one(&identity.pool())
        .await?;
    if valid {
        Ok(())
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

fn bounded(value: Option<i32>, default: i32, min: i32, max: i32) -> Result<i32, AuthError> {
    let value = value.unwrap_or(default);
    if (min..=max).contains(&value) {
        Ok(value)
    } else {
        Err(AuthError::InvalidInput)
    }
}

fn positive(value: Option<i32>, default: i32, max: i32) -> Result<i32, AuthError> {
    bounded(value, default, 1, max)
}

fn event_types(value: Option<Vec<String>>) -> Result<Vec<String>, AuthError> {
    let mut values = value.unwrap_or_default();
    if values.len() > 100 {
        return Err(AuthError::InvalidInput);
    }
    for value in &mut values {
        *value = value.trim().to_owned();
        if value.is_empty() || value.len() > 128 || value.chars().any(char::is_control) {
            return Err(AuthError::InvalidInput);
        }
    }
    values.sort();
    values.dedup();
    Ok(values)
}

fn metadata(value: Option<Value>) -> Result<String, AuthError> {
    let value = value.unwrap_or_else(|| json!({}));
    if !value.is_object() || value.to_string().len() > 4096 {
        return Err(AuthError::InvalidInput);
    }
    Ok(value.to_string())
}

fn sub_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({
        "id": row.get::<Uuid,_>("id"), "app_id": row.get::<Uuid,_>("app_id"),
        "name": row.get::<String,_>("name"), "enabled": row.get::<bool,_>("enabled"),
        "paused_at": row.get::<Option<String>,_>("paused_at"), "draining_at": row.get::<Option<String>,_>("draining_at"),
        "drain_deadline_at": row.get::<Option<String>,_>("drain_deadline_at"), "drained_at": row.get::<Option<String>,_>("drained_at"),
        "mode": row.get::<String,_>("mode"), "event_types": row.get::<Vec<String>,_>("event_types"),
        "max_attempts": row.get::<i32,_>("max_attempts"), "default_visibility_seconds": row.get::<i32,_>("default_visibility_seconds"),
        "max_visibility_seconds": row.get::<i32,_>("max_visibility_seconds"), "max_total_lease_seconds": row.get::<i32,_>("max_total_lease_seconds"),
        "retention_seconds": row.get::<i32,_>("retention_seconds"), "max_in_flight": row.get::<i32,_>("max_in_flight"),
        "max_batch_size": row.get::<i32,_>("max_batch_size"), "retry_delay_seconds": row.get::<i32,_>("retry_delay_seconds"),
        "ordering_mode": row.get::<String,_>("ordering_mode"), "deduplication_seconds": row.get::<i32,_>("deduplication_seconds"),
        "max_dispatch_rate": row.get::<Option<i32>,_>("max_dispatch_rate"),
        "success_callback_url": row.get::<Option<String>,_>("success_callback_url"), "failure_callback_url": row.get::<Option<String>,_>("failure_callback_url"),
        "result_callback_metadata": serde_json::from_str::<Value>(&row.get::<String,_>("result_callback_metadata")).unwrap_or_else(|_| json!({})),
        "policy_version": row.get::<i32,_>("policy_version"), "created_at": row.get::<String,_>("created_at"), "updated_at": row.get::<String,_>("updated_at"),
    })
}

async fn fetch_sub(
    identity: &Identity,
    app_id: Uuid,
    id: Uuid,
) -> Result<sqlx::postgres::PgRow, AuthError> {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {SUB_COLUMNS} FROM queue_subscriptions WHERE app_id=$1 AND id=$2"
    )))
    .bind(app_id)
    .bind(id)
    .fetch_optional(&identity.pool())
    .await?
    .ok_or(AuthError::Conflict)
}

async fn list(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "GET", &uri, &body).await?;
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

async fn create(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), AuthError> {
    let (workspace, app_id) = app_scope(&identity, &headers, "POST", &uri, &body).await?;
    let input: SubscriptionInput =
        serde_json::from_slice(&body).map_err(|_| AuthError::InvalidInput)?;
    let sub_name = name(input.name)?;
    let events = event_types(input.event_types)?;
    let max_attempts = bounded(input.max_attempts, 10, 1, 100)?;
    let default_visibility = bounded(input.default_visibility_seconds, 60, 1, 900)?;
    let max_visibility = bounded(input.max_visibility_seconds, 300, default_visibility, 3600)?;
    let max_total_lease = bounded(input.max_total_lease_seconds, 3600, max_visibility, 86400)?;
    let retention = bounded(input.retention_seconds, 604800, 60, 2592000)?;
    let in_flight = positive(input.max_in_flight, 10, 10_000)?;
    let batch = positive(input.max_batch_size, 10, 100)?;
    let retry_delay = bounded(input.retry_delay_seconds, 5, 0, 86400)?;
    let ordering = input.ordering_mode.unwrap_or_else(|| "none".into());
    if ordering != "none" && ordering != "key" {
        return Err(AuthError::InvalidInput);
    }
    let dedup = bounded(input.deduplication_seconds, 0, 0, 86400)?;
    let metadata = metadata(input.result_callback_metadata)?;
    let row = sqlx::query(sqlx::AssertSqlSafe(format!("INSERT INTO queue_subscriptions(id,workspace_id,app_id,name,enabled,mode,event_types,max_attempts,default_visibility_seconds,max_visibility_seconds,max_total_lease_seconds,retention_seconds,max_in_flight,max_batch_size,retry_delay_seconds,ordering_mode,deduplication_seconds,max_dispatch_rate,success_callback_url,failure_callback_url,result_callback_metadata) VALUES($1,$2,$3,$4,$5,'pull',$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20) RETURNING {SUB_COLUMNS}")))
        .bind(Uuid::new_v4()).bind(workspace).bind(app_id).bind(sub_name).bind(input.enabled.unwrap_or(true)).bind(events).bind(max_attempts).bind(default_visibility).bind(max_visibility).bind(max_total_lease).bind(retention).bind(in_flight).bind(batch).bind(retry_delay).bind(ordering).bind(dedup).bind(input.max_dispatch_rate).bind(input.success_callback_url).bind(input.failure_callback_url).bind(metadata).fetch_one(&identity.pool()).await?;
    Ok((StatusCode::CREATED, Json(sub_json(&row))))
}

async fn get_one(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "GET", &uri, &body).await?;
    Ok(Json(sub_json(&fetch_sub(&identity, app_id, id).await?)))
}

async fn update(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "PUT", &uri, &body).await?;
    let input: SubscriptionPatch =
        serde_json::from_slice(&body).map_err(|_| AuthError::InvalidInput)?;
    let current = fetch_sub(&identity, app_id, id).await?;
    if input
        .policy_version
        .is_some_and(|v| v != current.get::<i32, _>("policy_version"))
    {
        return Err(AuthError::Conflict);
    }
    if input
        .name
        .as_deref()
        .is_some_and(|v| v.trim().is_empty() || v.len() > 64)
    {
        return Err(AuthError::InvalidInput);
    }
    if input
        .ordering_mode
        .as_deref()
        .is_some_and(|v| v != "none" && v != "key")
    {
        return Err(AuthError::InvalidInput);
    }
    let event_types = input
        .event_types
        .map(|v| event_types(Some(v)))
        .transpose()?;
    let max_attempts = input
        .max_attempts
        .map(|v| bounded(Some(v), 10, 1, 100))
        .transpose()?;
    let default_visibility = input
        .default_visibility_seconds
        .map(|v| bounded(Some(v), 60, 1, 900))
        .transpose()?;
    let max_visibility = input
        .max_visibility_seconds
        .map(|v| bounded(Some(v), 300, 1, 3600))
        .transpose()?;
    let max_total_lease = input
        .max_total_lease_seconds
        .map(|v| bounded(Some(v), 3600, 1, 86400))
        .transpose()?;
    let retention = input
        .retention_seconds
        .map(|v| bounded(Some(v), 604800, 60, 2592000))
        .transpose()?;
    let in_flight = input
        .max_in_flight
        .map(|v| positive(Some(v), 10, 10_000))
        .transpose()?;
    let batch = input
        .max_batch_size
        .map(|v| positive(Some(v), 10, 100))
        .transpose()?;
    let retry_delay = input
        .retry_delay_seconds
        .map(|v| bounded(Some(v), 5, 0, 86400))
        .transpose()?;
    let dedup = input
        .deduplication_seconds
        .map(|v| bounded(Some(v), 0, 0, 86400))
        .transpose()?;
    let metadata = input
        .result_callback_metadata
        .map(|v| metadata(Some(v)))
        .transpose()?;
    let row = sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE queue_subscriptions SET name=COALESCE($2,name),enabled=COALESCE($3,enabled),event_types=COALESCE($4,event_types),max_attempts=COALESCE($5,max_attempts),default_visibility_seconds=COALESCE($6,default_visibility_seconds),max_visibility_seconds=COALESCE($7,max_visibility_seconds),max_total_lease_seconds=COALESCE($8,max_total_lease_seconds),retention_seconds=COALESCE($9,retention_seconds),max_in_flight=COALESCE($10,max_in_flight),max_batch_size=COALESCE($11,max_batch_size),retry_delay_seconds=COALESCE($12,retry_delay_seconds),ordering_mode=COALESCE($13,ordering_mode),deduplication_seconds=COALESCE($14,deduplication_seconds),max_dispatch_rate=COALESCE($15,max_dispatch_rate),success_callback_url=COALESCE($16,success_callback_url),failure_callback_url=COALESCE($17,failure_callback_url),result_callback_metadata=COALESCE($18,result_callback_metadata),policy_version=policy_version+1,updated_at=clock_timestamp() WHERE id=$1 AND app_id=$19 AND policy_version=$20 RETURNING {SUB_COLUMNS}")))
        .bind(id).bind(input.name.map(|v| v.trim().to_owned())).bind(input.enabled).bind(event_types).bind(max_attempts).bind(default_visibility).bind(max_visibility).bind(max_total_lease).bind(retention).bind(in_flight).bind(batch).bind(retry_delay).bind(input.ordering_mode).bind(dedup).bind(input.max_dispatch_rate).bind(input.success_callback_url).bind(input.failure_callback_url).bind(metadata).bind(app_id).bind(input.policy_version.unwrap_or(current.get("policy_version"))).fetch_optional(&identity.pool()).await?.ok_or(AuthError::Conflict)?;
    Ok(Json(sub_json(&row)))
}

async fn remove(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<StatusCode, AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "DELETE", &uri, &body).await?;
    let changed = sqlx::query("DELETE FROM queue_subscriptions WHERE id=$1 AND app_id=$2")
        .bind(id)
        .bind(app_id)
        .execute(&identity.pool())
        .await?
        .rows_affected();
    if changed == 1 {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(AuthError::Conflict)
    }
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

async fn pause(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "POST", &uri, &body).await?;
    Ok(Json(set_paused(&identity, app_id, id, true).await?))
}

async fn resume(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "POST", &uri, &body).await?;
    Ok(Json(set_paused(&identity, app_id, id, false).await?))
}

async fn pull(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "POST", &uri, &body).await?;
    let input: PullInput = serde_json::from_slice(&body).map_err(|_| AuthError::InvalidInput)?;
    let max_messages = input.max_messages.unwrap_or(10).clamp(1, 100);
    let visibility = input.visibility_seconds.unwrap_or(60).clamp(1, 3600);
    let _wait = input.wait_seconds.unwrap_or(0).min(30);
    let mut tx = identity.pool().begin().await?;
    let policy = sqlx::query("SELECT enabled,paused_at IS NOT NULL AS paused,draining_at IS NOT NULL AS draining,max_in_flight,max_batch_size,max_attempts,default_visibility_seconds,max_visibility_seconds,max_total_lease_seconds,ordering_mode FROM queue_subscriptions WHERE id=$1 AND app_id=$2 FOR UPDATE")
        .bind(id).bind(app_id).fetch_optional(&mut *tx).await?.ok_or(AuthError::Conflict)?;
    if !policy.get::<bool, _>("enabled")
        || policy.get::<bool, _>("paused")
        || policy.get::<bool, _>("draining")
    {
        return Err(AuthError::Conflict);
    }
    let limit = max_messages.min(policy.get::<i32, _>("max_batch_size").max(1) as u32);
    let max_attempts = policy.get::<i32, _>("max_attempts");
    sqlx::query("UPDATE queue_deliveries SET status=CASE WHEN attempts >= $2 THEN 'dead_letter' ELSE 'available' END,receipt_hash=NULL,lease_started_at=NULL,lease_expires_at=NULL,lease_max_expires_at=NULL,last_reason=CASE WHEN attempts >= $2 THEN 'max_attempts_exhausted' ELSE 'visibility_timeout' END,updated_at=clock_timestamp() WHERE subscription_id=$1 AND status='in_flight' AND lease_expires_at<=clock_timestamp()")
        .bind(id).bind(max_attempts).execute(&mut *tx).await?;
    sqlx::query("UPDATE queue_deliveries SET status='dead_letter',last_reason='retention_expired',updated_at=clock_timestamp() WHERE subscription_id=$1 AND status='available' AND expires_at<=clock_timestamp()")
        .bind(id).execute(&mut *tx).await?;
    let in_flight: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM queue_deliveries WHERE subscription_id=$1 AND status='in_flight'",
    )
    .bind(id)
    .fetch_one(&mut *tx)
    .await?;
    let limit = limit.min((policy.get::<i32, _>("max_in_flight") as i64 - in_flight).max(0) as u32);
    if limit == 0 {
        tx.commit().await?;
        return Ok(Json(json!({"items": []})));
    }
    let rows = sqlx::query("SELECT d.id,d.event_id,d.generation,d.attempts,e.app_id,e.payload,to_char(e.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at FROM queue_deliveries d JOIN events e ON e.id=d.event_id WHERE d.subscription_id=$1 AND d.status='available' AND d.available_at<=clock_timestamp() AND d.expires_at>clock_timestamp() ORDER BY d.priority DESC,d.available_at,d.created_at,d.id FOR UPDATE OF d SKIP LOCKED LIMIT $2")
        .bind(id).bind(i64::from(limit)).fetch_all(&mut *tx).await?;
    let lease_seconds =
        visibility.min(policy.get::<i32, _>("max_visibility_seconds").max(1) as u32);
    let max_total_lease = policy.get::<i32, _>("max_total_lease_seconds").max(1) as u32;
    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        let delivery_id: Uuid = row.get("id");
        let event_id: Uuid = row.get("event_id");
        let generation: i32 = row.get("generation");
        let attempt = row.get::<i32, _>("attempts") + 1;
        let receipt = format!("rcpt_{}", Uuid::new_v4().simple());
        let digest = Sha256::digest(receipt.as_bytes()).to_vec();
        sqlx::query("UPDATE queue_deliveries SET status='in_flight',attempts=$2,receipt_hash=$3,lease_started_at=clock_timestamp(),lease_expires_at=LEAST(clock_timestamp()+$4*interval '1 second',clock_timestamp()+$5*interval '1 second',expires_at),lease_max_expires_at=clock_timestamp()+$5*interval '1 second',updated_at=clock_timestamp() WHERE id=$1")
            .bind(delivery_id).bind(attempt).bind(digest).bind(i64::from(lease_seconds)).bind(i64::from(max_total_lease)).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO queue_delivery_attempts(delivery_id,generation,attempt,outcome,occurred_at) VALUES($1,$2,$3,'leased',clock_timestamp())")
            .bind(delivery_id).bind(generation).bind(attempt).execute(&mut *tx).await?;
        let payload: LegacyPayload = serde_json::from_slice(&row.get::<Vec<u8>, _>("payload"))
            .map_err(|_| AuthError::Internal)?;
        let data = STANDARD
            .decode(payload.body)
            .ok()
            .and_then(|value| serde_json::from_slice::<Value>(&value).ok())
            .unwrap_or_else(|| json!({}));
        let event = json!({"id":event_id,"type":payload.event_type,"source_app_id":row.get::<Uuid,_>("app_id"),"target_app_ids":[app_id],"data":data,"created_at":row.get::<String,_>("created_at").to_string()});
        let expires: String = sqlx::query_scalar("SELECT to_char(lease_expires_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') FROM queue_deliveries WHERE id=$1")
            .bind(delivery_id).fetch_one(&mut *tx).await?;
        items.push(json!({"id":delivery_id,"subscription_id":id,"receipt":receipt,"attempt":attempt,"generation":generation,"lease_expires_at":expires,"event":event}));
    }
    tx.commit().await?;
    Ok(Json(json!({"items":items})))
}

async fn settle(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "POST", &uri, &body).await?;
    let input: SettlementInput =
        serde_json::from_slice(&body).map_err(|_| AuthError::InvalidInput)?;
    if input.items.len() > 100 {
        return Err(AuthError::InvalidInput);
    }
    let mut tx = identity.pool().begin().await?;
    let max_attempts: i32 = sqlx::query_scalar(
        "SELECT max_attempts FROM queue_subscriptions WHERE id=$1 AND app_id=$2",
    )
    .bind(id)
    .bind(app_id)
    .fetch_optional(&mut *tx)
    .await?
    .ok_or(AuthError::Conflict)?;
    let mut results = Vec::with_capacity(input.items.len());
    for item in input.items {
        if item.receipt.is_empty() || item.receipt.len() > 512 {
            return Err(AuthError::InvalidInput);
        }
        if !matches!(
            item.disposition.as_str(),
            "acked" | "available" | "dead_letter"
        ) {
            return Err(AuthError::InvalidInput);
        }
        let digest = Sha256::digest(item.receipt.as_bytes()).to_vec();
        let delay = i64::from(item.delay_seconds.unwrap_or(0).min(86400));
        let row = sqlx::query("UPDATE queue_deliveries SET status=CASE WHEN $4='available' AND attempts >= $5 THEN 'dead_letter' ELSE $4 END,available_at=CASE WHEN $4='available' THEN clock_timestamp()+$6*interval '1 second' ELSE available_at END,receipt_hash=NULL,lease_started_at=NULL,lease_expires_at=NULL,lease_max_expires_at=NULL,last_reason=NULLIF($7,''),updated_at=clock_timestamp() WHERE id IN (SELECT id FROM queue_deliveries WHERE subscription_id=$1 AND app_id=$2 AND receipt_hash=$3 AND status='in_flight' AND lease_expires_at>clock_timestamp()) RETURNING id,event_id,generation,attempts,status,last_reason")
            .bind(id).bind(app_id).bind(digest).bind(&item.disposition).bind(max_attempts).bind(delay).bind(item.reason.clone().unwrap_or_default()).fetch_optional(&mut *tx).await?;
        let status = row
            .as_ref()
            .map(|r| r.get::<String, _>("status"))
            .unwrap_or_else(|| "invalid_receipt".into());
        if let Some(row) = row {
            sqlx::query("INSERT INTO queue_delivery_attempts(delivery_id,generation,attempt,outcome,reason,occurred_at) VALUES($1,$2,$3,$4,NULLIF($5,''),clock_timestamp())")
                .bind(row.get::<Uuid,_>("id")).bind(row.get::<i32,_>("generation")).bind(row.get::<i32,_>("attempts")).bind(&status).bind(item.reason.clone().unwrap_or_default()).execute(&mut *tx).await?;
        }
        results.push(json!({"receipt":item.receipt,"status":status}));
    }
    tx.commit().await?;
    Ok(Json(json!({"items":results})))
}

async fn extend(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "POST", &uri, &body).await?;
    let input: LeaseInput = serde_json::from_slice(&body).map_err(|_| AuthError::InvalidInput)?;
    if input.items.len() > 100 {
        return Err(AuthError::InvalidInput);
    }
    let mut tx = identity.pool().begin().await?;
    let mut items = Vec::with_capacity(input.items.len());
    for item in input.items {
        if item.receipt.is_empty() || item.extension_seconds == 0 || item.extension_seconds > 3600 {
            return Err(AuthError::InvalidInput);
        }
        let digest = Sha256::digest(item.receipt.as_bytes()).to_vec();
        let changed = sqlx::query("UPDATE queue_deliveries SET lease_expires_at=LEAST(lease_expires_at+$4*interval '1 second',lease_max_expires_at),updated_at=clock_timestamp() WHERE subscription_id=$1 AND app_id=$2 AND receipt_hash=$3 AND status='in_flight' AND lease_expires_at>clock_timestamp()")
            .bind(id).bind(app_id).bind(digest).bind(i64::from(item.extension_seconds)).execute(&mut *tx).await?.rows_affected() == 1;
        items.push(json!({"receipt":item.receipt,"status":if changed {"extended"} else {"invalid_receipt"}}));
    }
    tx.commit().await?;
    Ok(Json(json!({"items":items})))
}

async fn metrics(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "GET", &uri, &body).await?;
    let row = sqlx::query("SELECT count(*) FILTER (WHERE status='available' AND available_at<=clock_timestamp()) AS available,count(*) FILTER (WHERE status='in_flight') AS in_flight,count(*) FILTER (WHERE status='acked') AS acknowledged,count(*) FILTER (WHERE status='dead_letter') AS dead_letter,to_char(min(available_at) FILTER (WHERE status='available' AND available_at<=clock_timestamp()) AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS oldest_available_at FROM queue_deliveries WHERE subscription_id=$1 AND app_id=$2")
        .bind(id).bind(app_id).fetch_one(&identity.pool()).await?;
    Ok(Json(
        json!({"available":row.get::<i64,_>("available"),"in_flight":row.get::<i64,_>("in_flight"),"acknowledged":row.get::<i64,_>("acknowledged"),"dead_letter":row.get::<i64,_>("dead_letter"),"oldest_available_at":row.get::<Option<String>,_>("oldest_available_at")}),
    ))
}

async fn drain(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "POST", &uri, &body).await?;
    let input: DrainInput = serde_json::from_slice(&body).map_err(|_| AuthError::InvalidInput)?;
    let timeout = input.timeout_seconds.unwrap_or(30).clamp(1, 3600) as i64;
    let row = sqlx::query("UPDATE queue_subscriptions SET draining_at=clock_timestamp(),drain_deadline_at=clock_timestamp()+$3*interval '1 second',drained_at=NULL,updated_at=clock_timestamp() WHERE id=$1 AND app_id=$2 RETURNING to_char(draining_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS started_at,to_char(drain_deadline_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS deadline_at")
        .bind(id).bind(app_id).bind(timeout).fetch_optional(&identity.pool()).await?.ok_or(AuthError::Conflict)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(
            json!({"subscription_id":id,"status":"draining","in_flight":0,"started_at":row.get::<String,_>("started_at"),"deadline_at":row.get::<String,_>("deadline_at")}),
        ),
    ))
}

async fn drain_status(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "GET", &uri, &body).await?;
    let row = fetch_sub(&identity, app_id, id).await?;
    let in_flight: i64 = sqlx::query_scalar("SELECT count(*) FROM queue_deliveries WHERE subscription_id=$1 AND app_id=$2 AND status='in_flight'")
        .bind(id).bind(app_id).fetch_one(&identity.pool()).await?;
    let status = if row.get::<Option<String>, _>("drained_at").is_some() {
        "drained"
    } else if row.get::<Option<String>, _>("draining_at").is_some() {
        "draining"
    } else {
        "active"
    };
    Ok(Json(
        json!({"subscription_id":id,"status":status,"in_flight":in_flight,"started_at":row.get::<Option<String>,_>("draining_at"),"deadline_at":row.get::<Option<String>,_>("drain_deadline_at"),"completed_at":row.get::<Option<String>,_>("drained_at")}),
    ))
}

fn schedule_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({
        "id":row.get::<Uuid,_>("id"), "subscription_id":row.get::<Uuid,_>("subscription_id"),
        "name":row.get::<String,_>("name"), "enabled":row.get::<bool,_>("enabled"),
        "cron_expression":row.get::<String,_>("cron_expression"), "timezone":row.get::<String,_>("timezone"),
        "event_type":row.get::<String,_>("event_type"),
        "data":serde_json::from_str::<Value>(&row.get::<String,_>("data_text")).unwrap_or_else(|_|json!({})),
        "ordering_key":row.get::<Option<String>,_>("ordering_key"), "priority":row.get::<i32,_>("priority"),
        "metadata":serde_json::from_str::<Value>(&row.get::<String,_>("metadata_text")).unwrap_or_else(|_|json!({})),
        "next_run_at":row.get::<String,_>("next_run_at"), "last_run_at":row.get::<Option<String>,_>("last_run_at"),
        "policy_version":row.get::<i32,_>("policy_version"),
    })
}

async fn list_schedules(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "GET", &uri, &body).await?;
    let rows = sqlx::query(sqlx::AssertSqlSafe(format!("SELECT {SCHEDULE_COLUMNS} FROM queue_schedules WHERE subscription_id=$1 AND app_id=$2 ORDER BY created_at,id")))
        .bind(id).bind(app_id).fetch_all(&identity.pool()).await?;
    Ok(Json(
        json!({"items":rows.iter().map(schedule_json).collect::<Vec<_>>() }),
    ))
}

async fn create_schedule(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), AuthError> {
    let (workspace, app_id) = app_scope(&identity, &headers, "POST", &uri, &body).await?;
    let input: ScheduleInput =
        serde_json::from_slice(&body).map_err(|_| AuthError::InvalidInput)?;
    subscription_scope(&identity, workspace, app_id, id).await?;
    if input.name.trim().is_empty()
        || input.name.len() > 64
        || input.cron_expression.trim().is_empty()
        || input.timezone.trim().is_empty()
        || input.event_type.trim().is_empty()
        || !input.data.is_object()
    {
        return Err(AuthError::InvalidInput);
    }
    let row = sqlx::query(sqlx::AssertSqlSafe(format!("INSERT INTO queue_schedules(id,workspace_id,app_id,subscription_id,name,enabled,cron_expression,timezone,event_type,data_text,ordering_key,priority,metadata_text) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) RETURNING {SCHEDULE_COLUMNS}")))
        .bind(Uuid::new_v4()).bind(workspace).bind(app_id).bind(id).bind(input.name.trim()).bind(input.enabled.unwrap_or(true)).bind(input.cron_expression.trim()).bind(input.timezone.trim()).bind(input.event_type.trim()).bind(input.data.to_string()).bind(input.ordering_key).bind(input.priority.unwrap_or(0)).bind(input.metadata.unwrap_or_else(||json!({})).to_string()).fetch_one(&identity.pool()).await?;
    Ok((StatusCode::CREATED, Json(schedule_json(&row))))
}

async fn get_schedule(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path((id, schedule_id)): Path<(Uuid, Uuid)>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "GET", &uri, &body).await?;
    let row = sqlx::query(sqlx::AssertSqlSafe(format!("SELECT {SCHEDULE_COLUMNS} FROM queue_schedules WHERE id=$1 AND subscription_id=$2 AND app_id=$3")))
        .bind(schedule_id).bind(id).bind(app_id).fetch_optional(&identity.pool()).await?.ok_or(AuthError::Conflict)?;
    Ok(Json(schedule_json(&row)))
}

async fn update_schedule(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path((id, schedule_id)): Path<(Uuid, Uuid)>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "PUT", &uri, &body).await?;
    let input: ScheduleInput =
        serde_json::from_slice(&body).map_err(|_| AuthError::InvalidInput)?;
    if input.name.trim().is_empty()
        || input.name.len() > 64
        || input.cron_expression.trim().is_empty()
        || input.timezone.trim().is_empty()
        || input.event_type.trim().is_empty()
        || !input.data.is_object()
    {
        return Err(AuthError::InvalidInput);
    }
    let row = sqlx::query(sqlx::AssertSqlSafe(format!("UPDATE queue_schedules SET name=$2,enabled=COALESCE($3,enabled),cron_expression=$4,timezone=$5,event_type=$6,data_text=$7,ordering_key=$8,priority=COALESCE($9,priority),metadata_text=$10,policy_version=policy_version+1,updated_at=clock_timestamp() WHERE id=$1 AND subscription_id=$11 AND app_id=$12 RETURNING {SCHEDULE_COLUMNS}")))
        .bind(schedule_id).bind(input.name.trim()).bind(input.enabled).bind(input.cron_expression.trim()).bind(input.timezone.trim()).bind(input.event_type.trim()).bind(input.data.to_string()).bind(input.ordering_key).bind(input.priority).bind(input.metadata.unwrap_or_else(||json!({})).to_string()).bind(id).bind(app_id).fetch_optional(&identity.pool()).await?.ok_or(AuthError::Conflict)?;
    Ok(Json(schedule_json(&row)))
}

async fn delete_schedule(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path((id, schedule_id)): Path<(Uuid, Uuid)>,
    body: Bytes,
) -> Result<StatusCode, AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "DELETE", &uri, &body).await?;
    let changed =
        sqlx::query("DELETE FROM queue_schedules WHERE id=$1 AND subscription_id=$2 AND app_id=$3")
            .bind(schedule_id)
            .bind(id)
            .bind(app_id)
            .execute(&identity.pool())
            .await?
            .rows_affected();
    if changed == 1 {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(AuthError::Conflict)
    }
}

fn dead_letter_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({
        "delivery_id":row.get::<Uuid,_>("id"), "subscription_id":row.get::<Uuid,_>("subscription_id"),
        "event_id":row.get::<Uuid,_>("event_id"), "attempts":row.get::<i32,_>("attempts"),
        "generation":row.get::<i32,_>("generation"), "reason":row.get::<String,_>("reason"),
        "updated_at":row.get::<String,_>("updated_at"),
    })
}

async fn read_dead_letters(
    identity: &Identity,
    app_id: Uuid,
    id: Uuid,
    query: &CursorQuery,
) -> Result<Vec<Value>, AuthError> {
    let limit = query.limit.unwrap_or(50).clamp(1, 100) as i64;
    let rows = if let Some(cursor) = query.cursor {
        sqlx::query("SELECT id,subscription_id,event_id,attempts,generation,COALESCE(last_reason,'') AS reason,to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at FROM queue_deliveries WHERE subscription_id=$1 AND app_id=$2 AND status='dead_letter' AND id>$3 ORDER BY id LIMIT $4")
            .bind(id).bind(app_id).bind(cursor).bind(limit).fetch_all(&identity.pool()).await?
    } else {
        sqlx::query("SELECT id,subscription_id,event_id,attempts,generation,COALESCE(last_reason,'') AS reason,to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at FROM queue_deliveries WHERE subscription_id=$1 AND app_id=$2 AND status='dead_letter' ORDER BY id LIMIT $3")
            .bind(id).bind(app_id).bind(limit).fetch_all(&identity.pool()).await?
    };
    Ok(rows.iter().map(dead_letter_json).collect())
}

async fn dead_letters(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(id): Path<Uuid>,
    Query(query): Query<CursorQuery>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "GET", &uri, &body).await?;
    let items = read_dead_letters(&identity, app_id, id, &query).await?;
    let next = items
        .last()
        .and_then(|item| item.get("delivery_id"))
        .cloned()
        .unwrap_or(Value::String(String::new()));
    Ok(Json(json!({"items":items,"next_cursor":next})))
}

async fn export_dead_letters(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(id): Path<Uuid>,
    Query(query): Query<CursorQuery>,
    body: Bytes,
) -> Result<Response, AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "GET", &uri, &body).await?;
    let items = read_dead_letters(&identity, app_id, id, &query).await?;
    if query.format.as_deref() == Some("ndjson") {
        let body = items
            .into_iter()
            .map(|item| item.to_string())
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        return Ok((
            [
                (axum::http::header::CONTENT_TYPE, "application/x-ndjson"),
                (
                    axum::http::header::CONTENT_DISPOSITION,
                    "attachment; filename=relayhub-dlq.ndjson",
                ),
            ],
            body,
        )
            .into_response());
    }
    Ok(Json(json!({"items":items})).into_response())
}

async fn replay_dead_letters(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "POST", &uri, &body).await?;
    let input: DeadLetterInput =
        serde_json::from_slice(&body).map_err(|_| AuthError::InvalidInput)?;
    if input.delivery_ids.len() > 100 {
        return Err(AuthError::InvalidInput);
    }
    let count = sqlx::query("UPDATE queue_deliveries SET status='available',generation=generation+1,attempts=0,available_at=clock_timestamp(),receipt_hash=NULL,lease_started_at=NULL,lease_expires_at=NULL,lease_max_expires_at=NULL,last_reason=NULL,updated_at=clock_timestamp() WHERE subscription_id=$1 AND app_id=$2 AND status='dead_letter' AND id=ANY($3)")
        .bind(id).bind(app_id).bind(&input.delivery_ids).execute(&identity.pool()).await?.rows_affected();
    Ok(Json(json!({"replayed":count})))
}

async fn delete_dead_letters(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(id): Path<Uuid>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, app_id) = app_scope(&identity, &headers, "POST", &uri, &body).await?;
    let input: DeadLetterInput =
        serde_json::from_slice(&body).map_err(|_| AuthError::InvalidInput)?;
    if input.delivery_ids.len() > 100 {
        return Err(AuthError::InvalidInput);
    }
    let count = sqlx::query("DELETE FROM queue_deliveries WHERE subscription_id=$1 AND app_id=$2 AND status='dead_letter' AND id=ANY($3)")
        .bind(id).bind(app_id).bind(&input.delivery_ids).execute(&identity.pool()).await?.rows_affected();
    Ok(Json(json!({"deleted":count})))
}
