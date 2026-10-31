//! Compatibility adapters for the public Go HTTP contract.
//!
//! These handlers keep the signed application API stable while the Rust data
//! plane is authoritative.  They deliberately translate wire IDs and payloads
//! at the edge; no legacy process is involved in serving the request.
use axum::{
    Json, Router,
    body::Bytes,
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Digest;
use sqlx::Row;
use std::collections::HashSet;
use uuid::Uuid;

use crate::{
    events::Events,
    identity::{AuthError, Identity},
    protocol::Principal,
};

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AppPatch {
    name: Option<String>,
    callback_url: Option<Option<String>>,
    delivery_mode: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EventInput {
    r#type: String,
    #[serde(default)]
    target_app_ids: Vec<String>,
    data: Value,
    queue: Option<QueueInput>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct QueueInput {
    available_at: Option<String>,
    delay_seconds: Option<i64>,
    ordering_key: Option<String>,
    priority: Option<i32>,
    deduplication_key: Option<String>,
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct LegacyPayload {
    event_type: String,
    body: String,
    queue: Option<Value>,
}

pub fn router(identity: Identity) -> Router {
    Router::new()
        .route("/api/v1/events/{event_id}", get(get_event))
        .route("/api/v1/jobs/{job_id}", get(get_job))
        .with_state(identity)
}

fn wire_event_id(id: Uuid) -> String {
    format!("evt_{}", id.simple())
}

fn wire_job_id(id: Uuid) -> String {
    format!("job_{}", id.simple())
}

fn hex_digest(value: impl AsRef<[u8]>) -> String {
    sha2::Sha256::digest(value.as_ref())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn parse_wire_id(value: &str, prefix: &str) -> Option<Uuid> {
    value
        .strip_prefix(prefix)
        .and_then(|suffix| Uuid::parse_str(suffix).ok())
        .or_else(|| Uuid::parse_str(value).ok())
}

fn request_target(uri: &Uri) -> &str {
    uri.path_and_query()
        .map(|value| value.as_str())
        .unwrap_or(uri.path())
}

async fn app_principal(
    identity: &Identity,
    headers: &HeaderMap,
    method: &str,
    uri: &Uri,
    body: &[u8],
) -> Result<(Uuid, Uuid), AuthError> {
    let principal = identity
        .authenticate_public_request(headers, method, request_target(uri), body)
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

async fn app_row(identity: &Identity, app_id: Uuid) -> Result<Value, AuthError> {
    let row = sqlx::query("SELECT id,workspace_id,name,delivery_mode,callback_url,enabled,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at FROM identity_apps WHERE id=$1")
        .bind(app_id)
        .fetch_optional(&identity.pool())
        .await?
        .ok_or(AuthError::Conflict)?;
    Ok(json!({
        "id": row.get::<Uuid, _>("id"),
        "name": row.get::<String, _>("name"),
        "callback_url": row.get::<Option<String>, _>("callback_url"),
        "delivery_mode": row.get::<String, _>("delivery_mode"),
        "enabled": row.get::<bool, _>("enabled"),
        "created_at": row.get::<String, _>("created_at"),
        "updated_at": row.get::<String, _>("updated_at"),
    }))
}

pub(crate) async fn get_app(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(app_value): Path<String>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, authenticated_app) = app_principal(&identity, &headers, "GET", &uri, &body).await?;
    let app_id = resolve_app(&identity, &app_value).await?;
    if app_id != authenticated_app {
        return Err(AuthError::Forbidden);
    }
    Ok(Json(app_row(&identity, app_id).await?))
}

pub(crate) async fn update_app(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(app_value): Path<String>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, authenticated_app) = app_principal(&identity, &headers, "PATCH", &uri, &body).await?;
    let app_id = resolve_app(&identity, &app_value).await?;
    if app_id != authenticated_app {
        return Err(AuthError::Forbidden);
    }
    let input: AppPatch = serde_json::from_slice(&body).map_err(|_| AuthError::InvalidInput)?;
    if input.name.is_none() && input.callback_url.is_none() && input.delivery_mode.is_none() {
        return Err(AuthError::InvalidInput);
    }
    if input
        .name
        .as_deref()
        .is_some_and(|value| value.trim().is_empty() || value.trim().len() > 200)
        || input
            .delivery_mode
            .as_deref()
            .is_some_and(|value| !matches!(value, "queue" | "websocket" | "callback" | "all"))
        || input
            .callback_url
            .as_ref()
            .and_then(|value| value.as_ref())
            .is_some_and(|value| value.len() > 2048)
    {
        return Err(AuthError::InvalidInput);
    }
    sqlx::query("UPDATE identity_apps SET name=COALESCE($2,name),callback_url=CASE WHEN $3 THEN $4 ELSE callback_url END,delivery_mode=COALESCE($5,delivery_mode),updated_at=clock_timestamp() WHERE id=$1")
        .bind(app_id)
        .bind(input.name.map(|value| value.trim().to_owned()))
        .bind(input.callback_url.is_some())
        .bind(input.callback_url.flatten())
        .bind(input.delivery_mode)
        .execute(&identity.pool())
        .await?;
    Ok(Json(app_row(&identity, app_id).await?))
}

async fn resolve_app(identity: &Identity, value: &str) -> Result<Uuid, AuthError> {
    if let Ok(id) = Uuid::parse_str(value) {
        return Ok(id);
    }
    sqlx::query_scalar("SELECT id FROM identity_apps WHERE legacy_source_id=$1")
        .bind(value)
        .fetch_optional(&identity.pool())
        .await?
        .ok_or(AuthError::Conflict)
}

async fn app_label(identity: &Identity, app_id: Uuid) -> Result<String, AuthError> {
    Ok(sqlx::query_scalar::<_, Option<String>>(
        "SELECT legacy_source_id FROM identity_apps WHERE id=$1",
    )
    .bind(app_id)
    .fetch_one(&identity.pool())
    .await?
    .unwrap_or_else(|| app_id.to_string()))
}

async fn resolve_targets(
    identity: &Identity,
    workspace_id: Uuid,
    source_app: Uuid,
    input: &[String],
    event_type: &str,
) -> Result<Vec<(Uuid, String)>, AuthError> {
    let labels = if input.is_empty() {
        sqlx::query_scalar::<_, Uuid>("SELECT target_app_id FROM routing_rules WHERE workspace_id=$1 AND enabled AND deleted_at IS NULL AND (source_app_id IS NULL OR source_app_id=$2) AND event_type=$3 ORDER BY created_at,id")
            .bind(workspace_id)
            .bind(source_app)
            .bind(event_type)
            .fetch_all(&identity.pool())
            .await?
            .into_iter()
            .map(|id| id.to_string())
            .collect::<Vec<_>>()
    } else {
        input.to_vec()
    };
    if labels.is_empty() || labels.len() > 100 {
        return Err(AuthError::InvalidInput);
    }
    let mut seen = HashSet::new();
    let mut targets = Vec::with_capacity(labels.len());
    for label in labels {
        let id = resolve_app(identity, label.trim()).await?;
        if !seen.insert(id) || id == source_app {
            return Err(AuthError::InvalidInput);
        }
        let enabled: bool =
            sqlx::query_scalar("SELECT enabled FROM identity_apps WHERE id=$1 AND workspace_id=$2")
                .bind(id)
                .bind(workspace_id)
                .fetch_optional(&identity.pool())
                .await?
                .ok_or(AuthError::InvalidInput)?;
        if !enabled {
            return Err(AuthError::InvalidInput);
        }
        targets.push((id, label.trim().to_owned()));
    }
    Ok(targets)
}

pub async fn publish_event_compat(
    State((identity, _events)): State<(Identity, Events)>,
    headers: HeaderMap,
    uri: OriginalUri,
    body: Bytes,
) -> Response {
    let (workspace, source_app) =
        match app_principal(&identity, &headers, "POST", &uri, &body).await {
            Ok(value) => value,
            Err(error) => return error.into_response(),
        };
    let key = match headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
    {
        Some(value) if !value.is_empty() && value.len() <= 256 => value,
        _ => return AuthError::InvalidInput.into_response(),
    };
    let input: EventInput = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => return AuthError::InvalidInput.into_response(),
    };
    if input.r#type.trim().is_empty()
        || input.r#type.len() > 128
        || !input.data.is_object()
        || input.data.to_string().len() > 1_048_576
    {
        return AuthError::InvalidInput.into_response();
    }
    if let Some(queue) = &input.queue {
        if (queue.available_at.is_some() && queue.delay_seconds.is_some())
            || queue
                .delay_seconds
                .is_some_and(|value| !(0..=2_592_000).contains(&value))
            || queue
                .ordering_key
                .as_deref()
                .is_some_and(|value| value.len() > 128)
            || queue
                .priority
                .is_some_and(|value| !(-10..=10).contains(&value))
            || queue
                .deduplication_key
                .as_deref()
                .is_some_and(|value| value.len() > 256)
            || queue
                .metadata
                .as_ref()
                .is_some_and(|value| !value.is_object() || value.to_string().len() > 16_384)
        {
            return AuthError::InvalidInput.into_response();
        }
    }
    let targets = match resolve_targets(
        &identity,
        workspace,
        source_app,
        &input.target_app_ids,
        input.r#type.trim(),
    )
    .await
    {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let response = match publish_legacy(&identity, source_app, workspace, key, input, targets).await
    {
        Ok((value, replay)) => {
            let mut response = (StatusCode::ACCEPTED, Json(value)).into_response();
            if replay {
                response
                    .headers_mut()
                    .insert("Idempotent-Replayed", "true".parse().unwrap());
            }
            response
        }
        Err(error) => error.into_response(),
    };
    response
}

async fn publish_legacy(
    identity: &Identity,
    source_app: Uuid,
    workspace: Uuid,
    key: &str,
    input: EventInput,
    targets: Vec<(Uuid, String)>,
) -> Result<(Value, bool), AuthError> {
    if let Some(existing) = sqlx::query("SELECT id,payload,legacy_target_app_ids FROM events WHERE app_id=$1 AND idempotency_key=$2")
        .bind(source_app)
        .bind(key)
        .fetch_optional(&identity.pool())
        .await?
    {
        let event_id: Uuid = existing.get("id");
        let same_request = serde_json::from_slice::<LegacyPayload>(&existing.get::<Vec<u8>, _>("payload"))
            .ok()
            .and_then(|payload| {
                let data = STANDARD
                    .decode(payload.body)
                    .ok()
                    .and_then(|value| serde_json::from_slice::<Value>(&value).ok())?;
                let stored_targets = existing.get::<Vec<String>, _>("legacy_target_app_ids");
                Some(
                    payload.event_type == input.r#type.trim()
                        && data == input.data
                        && payload.queue.unwrap_or(Value::Null)
                            == serde_json::to_value(&input.queue).unwrap_or(Value::Null)
                        && stored_targets == targets.iter().map(|(_, label)| label.clone()).collect::<Vec<_>>(),
                )
            })
            .unwrap_or(false);
        if !same_request {
            return Err(AuthError::Conflict);
        }
        return Ok((publication_json(identity, event_id).await?, true));
    }
    let endpoint = sqlx::query_scalar::<_, Uuid>(
        "SELECT id FROM event_endpoints WHERE workspace_id=$1 AND app_id=$2 ORDER BY id LIMIT 1",
    )
    .bind(workspace)
    .bind(source_app)
    .fetch_optional(&identity.pool())
    .await?
    .unwrap_or_else(Uuid::new_v4);
    let mut tx = identity.pool().begin().await?;
    if sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM event_endpoints WHERE id=$1)")
        .bind(endpoint)
        .fetch_one(&mut *tx)
        .await?
        == false
    {
        sqlx::query("INSERT INTO event_endpoints(id,workspace_id,app_id) VALUES($1,$2,$3)")
            .bind(endpoint)
            .bind(workspace)
            .bind(source_app)
            .execute(&mut *tx)
            .await?;
    }
    let event_id = Uuid::new_v4();
    let event_wire = wire_event_id(event_id);
    let body_json = serde_json::to_vec(&input.data).map_err(|_| AuthError::InvalidInput)?;
    let queue_available_at = input
        .queue
        .as_ref()
        .and_then(|queue| queue.available_at.clone());
    let queue_delay_seconds = input.queue.as_ref().and_then(|queue| queue.delay_seconds);
    let queue_ordering_key = input
        .queue
        .as_ref()
        .and_then(|queue| queue.ordering_key.clone());
    let queue_priority = input
        .queue
        .as_ref()
        .and_then(|queue| queue.priority)
        .unwrap_or(0);
    let queue_metadata = input
        .queue
        .as_ref()
        .and_then(|queue| queue.metadata.clone())
        .unwrap_or_else(|| json!({}))
        .to_string();
    let queue_dedup_hash = input
        .queue
        .as_ref()
        .and_then(|queue| queue.deduplication_key.as_deref())
        .filter(|value| !value.is_empty())
        .map(hex_digest)
        .unwrap_or_default();
    let queue_json = input.queue.as_ref().map(|queue| json!(queue));
    let payload = json!({
        "endpoint_id": endpoint,
        "event_type": input.r#type.trim(),
        "method": "POST",
        "path": format!("/legacy/events/{event_wire}"),
        "headers": [],
        "body": STANDARD.encode(&body_json),
        "queue": queue_json,
    });
    let payload_bytes = serde_json::to_vec(&payload).map_err(|_| AuthError::InvalidInput)?;
    let hash = sha2::Sha256::digest(&payload_bytes).to_vec();
    sqlx::query("INSERT INTO events(id,workspace_id,app_id,endpoint_id,idempotency_key,payload,payload_hash,payload_bytes,expires_at,legacy_source_id,legacy_target_app_ids) VALUES($1,$2,$3,$4,$5,$6,$7,$8,clock_timestamp()+interval '7 days',$9,$10)")
        .bind(event_id)
        .bind(workspace)
        .bind(source_app)
        .bind(endpoint)
        .bind(key)
        .bind(payload_bytes)
        .bind(hash)
        .bind(body_json.len() as i64)
        .bind(&event_wire)
        .bind(targets.iter().map(|(_, label)| label.clone()).collect::<Vec<_>>())
        .execute(&mut *tx)
        .await?;
    for (target_app, _) in &targets {
        let policy = sqlx::query("SELECT delivery_mode,callback_url FROM identity_apps WHERE id=$1 AND workspace_id=$2 AND enabled")
            .bind(target_app)
            .bind(workspace)
            .fetch_one(&mut *tx)
            .await?;
        let mode: String = policy.get("delivery_mode");
        let callback_enabled = matches!(mode.as_str(), "callback" | "all")
            && policy
                .get::<Option<String>, _>("callback_url")
                .is_some_and(|url| !url.trim().is_empty());
        let mut sinks = Vec::with_capacity(2);
        if matches!(mode.as_str(), "queue" | "websocket" | "all") {
            sinks.push("stream");
        }
        if callback_enabled {
            sinks.push("callback");
        }
        if sinks.is_empty() {
            return Err(AuthError::InvalidInput);
        }
        for sink in sinks {
            let delivery_id = Uuid::new_v4();
            let job_wire = wire_job_id(delivery_id);
            sqlx::query("INSERT INTO deliveries(id,event_id,workspace_id,app_id,target_app_id,generation,state,attempt_count,created_at,legacy_source_id,legacy_public_job_id,legacy_sink) VALUES($1,$2,$3,$4,$5,1,'pending',0,clock_timestamp(),$6,$7,$8)")
                .bind(delivery_id)
                .bind(event_id)
                .bind(workspace)
                .bind(source_app)
                .bind(target_app)
                .bind(format!("compat:delivery:{job_wire}"))
                .bind(job_wire)
                .bind(sink)
                .execute(&mut *tx)
                .await?;
            if sink == "stream" {
                sqlx::query(
                    "INSERT INTO event_outbox(delivery_id,workspace_id,app_id) VALUES($1,$2,$3)",
                )
                .bind(delivery_id)
                .bind(workspace)
                .bind(source_app)
                .execute(&mut *tx)
                .await?;
            }
        }
        sqlx::query("INSERT INTO queue_deliveries(id,subscription_id,app_id,event_id,available_at,metadata,created_at,updated_at,expires_at,ordering_key,priority) SELECT gen_random_uuid(),s.id,s.app_id,$1,CASE WHEN $4::bigint IS NULL THEN COALESCE($5::timestamptz,clock_timestamp()) ELSE clock_timestamp()+$4*interval '1 second' END,$6::jsonb,clock_timestamp(),clock_timestamp(),LEAST(e.expires_at,clock_timestamp()+make_interval(secs=>s.retention_seconds)),$7,$8 FROM queue_subscriptions s JOIN events e ON e.id=$1 WHERE s.app_id=$2 AND s.enabled AND s.paused_at IS NULL AND (cardinality(s.event_types)=0 OR $3=ANY(s.event_types)) AND ($9='' OR s.deduplication_seconds=0 OR NOT EXISTS (SELECT 1 FROM queue_deduplication_keys k WHERE k.subscription_id=s.id AND k.key_hash=$9 AND k.expires_at>clock_timestamp())) ON CONFLICT(subscription_id,event_id) DO NOTHING")
            .bind(event_id)
            .bind(target_app)
            .bind(input.r#type.trim())
            .bind(queue_delay_seconds)
            .bind(&queue_available_at)
            .bind(&queue_metadata)
            .bind(&queue_ordering_key)
            .bind(queue_priority)
            .bind(&queue_dedup_hash)
            .execute(&mut *tx)
            .await?;
        if !queue_dedup_hash.is_empty() {
            sqlx::query("INSERT INTO queue_deduplication_keys(subscription_id,key_hash,event_id,expires_at) SELECT s.id,$2,$1,clock_timestamp()+make_interval(secs=>s.deduplication_seconds) FROM queue_subscriptions s JOIN queue_deliveries d ON d.subscription_id=s.id AND d.event_id=$1 WHERE s.app_id=$3 AND s.deduplication_seconds>0 ON CONFLICT(subscription_id,key_hash) DO NOTHING")
                .bind(event_id)
                .bind(&queue_dedup_hash)
                .bind(target_app)
                .execute(&mut *tx)
                .await?;
        }
    }
    tx.commit().await?;
    Ok((publication_json(identity, event_id).await?, false))
}

async fn publication_json(identity: &Identity, event_id: Uuid) -> Result<Value, AuthError> {
    let event = event_json(identity, event_id).await?;
    let jobs =
        sqlx::query("SELECT legacy_public_job_id FROM deliveries WHERE event_id=$1 ORDER BY id")
            .bind(event_id)
            .fetch_all(&identity.pool())
            .await?
            .into_iter()
            .filter_map(|row| row.get::<Option<String>, _>("legacy_public_job_id"))
            .map(|id| json!({"id":id,"event_id":event.get("id").cloned().unwrap_or(Value::Null)}))
            .collect::<Vec<_>>();
    Ok(json!({"event":event,"jobs":jobs}))
}

async fn event_json(identity: &Identity, event_id: Uuid) -> Result<Value, AuthError> {
    let row = sqlx::query("SELECT id,app_id,payload,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,legacy_target_app_ids FROM events WHERE id=$1")
        .bind(event_id)
        .fetch_optional(&identity.pool())
        .await?
        .ok_or(AuthError::Conflict)?;
    let payload: LegacyPayload = serde_json::from_slice(&row.get::<Vec<u8>, _>("payload"))
        .map_err(|_| AuthError::Internal)?;
    let data = STANDARD
        .decode(payload.body)
        .ok()
        .and_then(|value| serde_json::from_slice::<Value>(&value).ok())
        .unwrap_or_else(|| json!({}));
    let source = app_label(identity, row.get("app_id")).await?;
    let targets = row.get::<Vec<String>, _>("legacy_target_app_ids");
    let queue = payload.queue.unwrap_or(Value::Null);
    Ok(json!({
        "id": wire_event_id(row.get("id")),
        "type": payload.event_type,
        "source_app_id": source,
        "target_app_ids": targets,
        "data": data,
        "queue": queue,
        "created_at": row.get::<String, _>("created_at"),
    }))
}

async fn get_event(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(event_value): Path<String>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, app_id) = app_principal(&identity, &headers, "GET", &uri, &body).await?;
    let event_id = parse_wire_id(&event_value, "evt_").ok_or(AuthError::Conflict)?;
    let row = sqlx::query("SELECT app_id,legacy_target_app_ids FROM events WHERE id=$1")
        .bind(event_id)
        .fetch_optional(&identity.pool())
        .await?
        .ok_or(AuthError::Conflict)?;
    let targets = row.get::<Vec<String>, _>("legacy_target_app_ids");
    let target_visible = if row.get::<Uuid, _>("app_id") == app_id {
        true
    } else {
        let mut visible = false;
        for target in &targets {
            if resolve_app(&identity, target).await.ok() == Some(app_id) {
                visible = true;
                break;
            }
        }
        visible
    };
    let readable = target_visible;
    if !readable {
        return Err(AuthError::Conflict);
    }
    Ok(Json(event_json(&identity, event_id).await?))
}

async fn get_job(
    State(identity): State<Identity>,
    headers: HeaderMap,
    uri: OriginalUri,
    Path(job_value): Path<String>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let (_, app_id) = app_principal(&identity, &headers, "GET", &uri, &body).await?;
    let job_id = job_value.trim_start_matches("job_");
    let row = sqlx::query("SELECT d.id,d.event_id,d.app_id,d.target_app_id,d.state,d.attempt_count,to_char(d.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,d.legacy_public_job_id FROM deliveries d WHERE d.legacy_public_job_id=$1 OR d.id::text=$2")
        .bind(&job_value)
        .bind(job_id)
        .fetch_optional(&identity.pool())
        .await?
        .ok_or(AuthError::Conflict)?;
    if row.get::<Uuid, _>("app_id") != app_id && row.get::<Uuid, _>("target_app_id") != app_id {
        return Err(AuthError::Conflict);
    }
    let source = app_label(&identity, row.get("app_id")).await?;
    let target = app_label(&identity, row.get("target_app_id")).await?;
    let status = match row.get::<String, _>("state").as_str() {
        "succeeded" => "acked",
        "dlq" => "dead_letter",
        "in_flight" => "leased",
        _ => "pending",
    };
    Ok(Json(json!({
        "id": row.get::<Option<String>, _>("legacy_public_job_id").unwrap_or_else(|| wire_job_id(row.get("id"))),
        "event_id": wire_event_id(row.get("event_id")),
        "source_app_id": source,
        "target_app_id": target,
        "status": status,
        "attempts": row.get::<i32, _>("attempt_count"),
        "created_at": row.get::<String, _>("created_at"),
    })))
}
