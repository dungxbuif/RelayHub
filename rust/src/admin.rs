//! Rust-owned read projections and guarded controls for the full Admin console.
//!
//! These handlers deliberately read the Rust durable tables. They do not proxy
//! to the legacy Go service, and every result is scoped from the authenticated
//! Admin principal rather than from a request-supplied workspace id.
use crate::{
    identity::{Action, AuthError, Identity, authorize},
    protocol::{EventInput, Principal},
};
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::HeaderMap,
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::Row;
use uuid::Uuid;

#[derive(Debug, Deserialize, Default)]
struct ListQuery {
    limit: Option<u32>,
    cursor: Option<Uuid>,
    #[serde(rename = "type")]
    event_type: Option<String>,
    source_app_id: Option<Uuid>,
    target_app_id: Option<Uuid>,
    actor_type: Option<String>,
    action: Option<String>,
    resource_type: Option<String>,
    resource_id: Option<String>,
    outcome: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplayInput {
    delivery_ids: Vec<Uuid>,
}

pub fn router() -> Router<Identity> {
    Router::new()
        .route("/api/v1/admin/dashboard", get(dashboard))
        .route("/api/v1/admin/metrics", get(metrics))
        .route("/api/v1/admin/events", get(events))
        .route("/api/v1/admin/events/{event_id}", get(event_detail))
        .route(
            "/api/v1/admin/events/{event_id}/timeline",
            get(event_timeline),
        )
        .route("/api/v1/admin/dlq", get(dead_letters))
        .route("/api/v1/admin/dlq/replay", post(replay_dead_letters))
        .route("/api/v1/admin/dlq/{delivery_id}", get(dead_letter_detail))
        .route(
            "/api/v1/admin/dlq/{delivery_id}/replay",
            post(replay_dead_letter),
        )
        .route("/api/v1/admin/audit", get(audit))
}

async fn metrics(
    State(identity): State<Identity>,
    headers: HeaderMap,
) -> Result<Json<Value>, AuthError> {
    let Json(mut dashboard) = dashboard(State(identity), headers).await?;
    if let Some(object) = dashboard.as_object_mut() {
        object.remove("durable");
    }
    Ok(Json(dashboard))
}

fn workspace_scope(principal: &Principal) -> Result<Option<Uuid>, AuthError> {
    match principal {
        Principal::WorkspaceOwner { workspace_id, .. } => Ok(Some(*workspace_id)),
        Principal::SystemAdmin { .. } => Ok(None),
        _ => Err(AuthError::Forbidden),
    }
}

fn bounded_limit(value: Option<u32>) -> i64 {
    value.unwrap_or(25).clamp(1, 100) as i64
}

fn missing_table(error: &sqlx::Error) -> bool {
    matches!(error, sqlx::Error::Database(error) if error.code().as_deref() == Some("42P01"))
}

fn redacted_value(value: Value, depth: usize) -> Value {
    if depth > 5 {
        return Value::String("[TRUNCATED]".to_owned());
    }
    match value {
        Value::Object(entries) => Value::Object(
            entries
                .into_iter()
                .map(|(key, value)| {
                    let sensitive = key.to_ascii_lowercase();
                    let value = if [
                        "token",
                        "secret",
                        "password",
                        "authorization",
                        "cookie",
                        "signature",
                        "api_key",
                        "apikey",
                        "credential",
                    ]
                    .iter()
                    .any(|needle| sensitive.contains(needle))
                    {
                        Value::String("[REDACTED]".to_owned())
                    } else {
                        redacted_value(value, depth + 1)
                    };
                    (key, value)
                })
                .take(100)
                .collect(),
        ),
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .take(100)
                .map(|v| redacted_value(v, depth + 1))
                .collect(),
        ),
        Value::String(value) if value.len() > 2_048 => {
            Value::String(format!("{}…[TRUNCATED]", &value[..2_048]))
        }
        other => other,
    }
}

fn event_type_and_data(payload: &[u8]) -> (String, Value) {
    let input = serde_json::from_slice::<EventInput>(payload).ok();
    let event_type = input
        .as_ref()
        .map(|input| input.event_type.clone())
        .unwrap_or_else(|| "unknown".to_owned());
    let data = input
        .and_then(|input| serde_json::from_slice::<Value>(&input.body).ok())
        .map(|value| redacted_value(value, 0))
        .unwrap_or_else(|| json!({"encoding": "base64", "available": false}));
    (event_type, data)
}

async fn dashboard(
    State(identity): State<Identity>,
    headers: HeaderMap,
) -> Result<Json<Value>, AuthError> {
    let principal = identity.admin_browser_auth(&headers, false).await?;
    let workspace = workspace_scope(&principal)?;
    let pool = identity.pool();
    let counts = sqlx::query(
        "SELECT count(*) FILTER (WHERE state='pending') AS pending, count(*) FILTER (WHERE state='retry_wait') AS retrying, count(*) FILTER (WHERE state='dlq') AS dead_letter, to_char(min(created_at) FILTER (WHERE terminal_at IS NULL) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS oldest_pending FROM deliveries WHERE ($1::uuid IS NULL OR workspace_id=$1)",
    )
    .bind(workspace)
    .fetch_one(&pool)
    .await;
    let generated_at: String = sqlx::query_scalar(
        "SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')",
    )
    .fetch_one(&pool)
    .await?;
    let (pending, retrying, dead_letter, oldest_pending_at) = match counts {
        Ok(row) => (
            row.get::<i64, _>("pending"),
            row.get::<i64, _>("retrying"),
            row.get::<i64, _>("dead_letter"),
            row.get::<Option<String>, _>("oldest_pending"),
        ),
        Err(error) if missing_table(&error) => (0, 0, 0, None),
        Err(_) => return Err(AuthError::Internal),
    };
    Ok(Json(json!({
        "generated_at": generated_at,
        "window_seconds": 900,
        "step_seconds": 60,
        "series": [],
        "instances": [],
        "active_connections": 0,
        "degraded_components": ["instance_metrics_unavailable"],
        "durable": {
            "pending": pending,
            "retrying": retrying,
            "dead_letter": dead_letter,
            "oldest_pending_at": oldest_pending_at,
        },
    })))
}

async fn events(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, AuthError> {
    let principal = identity.admin_browser_auth(&headers, false).await?;
    let workspace = workspace_scope(&principal)?;
    let rows = sqlx::query(
        "SELECT e.id, e.app_id, e.payload, to_char(e.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at, (SELECT count(*) FROM deliveries d WHERE d.event_id=e.id) AS delivery_count FROM events e WHERE ($1::uuid IS NULL OR e.workspace_id=$1) AND ($2::uuid IS NULL OR e.app_id=$2) AND ($3::uuid IS NULL OR e.id < $3) AND ($4::text IS NULL OR convert_from(e.payload, 'UTF8')::jsonb->>'event_type'=$4) ORDER BY e.created_at DESC, e.id DESC LIMIT $5",
    )
    .bind(workspace)
    .bind(query.source_app_id)
    .bind(query.cursor)
    .bind(query.event_type.as_deref())
    .bind(bounded_limit(query.limit))
    .fetch_all(&identity.pool())
    .await;
    let rows = match rows {
        Ok(rows) => rows,
        Err(error) if missing_table(&error) => Vec::new(),
        Err(_) => return Err(AuthError::Internal),
    };
    let items = rows
        .into_iter()
        .map(|row| {
            let payload: Vec<u8> = row.get("payload");
            let (event_type, _) = event_type_and_data(&payload);
            json!({
                "id": row.get::<Uuid, _>("id"),
                "type": event_type,
                "source_app_id": row.get::<Uuid, _>("app_id"),
                "target_count": 1,
                "delivery_count": row.get::<i64, _>("delivery_count"),
                "created_at": row.get::<String, _>("created_at"),
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({"items": items})))
}

async fn event_timeline(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path(event_id): Path<Uuid>,
) -> Result<Json<Value>, AuthError> {
    let principal = identity.admin_browser_auth(&headers, false).await?;
    let workspace = workspace_scope(&principal)?;
    let event = sqlx::query(
        "SELECT e.id,e.app_id,e.payload,to_char(e.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,(SELECT count(*) FROM deliveries d WHERE d.event_id=e.id) AS delivery_count FROM events e WHERE e.id=$1 AND ($2::uuid IS NULL OR e.workspace_id=$2)",
    )
    .bind(event_id)
    .bind(workspace)
    .fetch_optional(&identity.pool())
    .await?;
    let Some(event) = event else {
        return Err(AuthError::Conflict);
    };
    let payload: Vec<u8> = event.get("payload");
    let (event_type, data) = event_type_and_data(&payload);
    let event_json = json!({
        "id": event.get::<Uuid, _>("id"),
        "type": event_type,
        "source_app_id": event.get::<Uuid, _>("app_id"),
        "target_count": 1,
        "delivery_count": event.get::<i64, _>("delivery_count"),
        "created_at": event.get::<String, _>("created_at"),
        "target_app_ids": [event.get::<Uuid, _>("app_id")],
        "data": data,
    });
    let deliveries = sqlx::query(
        "SELECT d.id,d.event_id,d.app_id,d.generation,d.state,d.attempt_count,to_char(d.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(COALESCE(max(a.finished_at),d.created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at,COALESCE((SELECT da.outcome FROM delivery_attempts da WHERE da.delivery_id=d.id ORDER BY da.attempt DESC LIMIT 1),'') AS reason FROM deliveries d LEFT JOIN delivery_attempts a ON a.delivery_id=d.id WHERE d.event_id=$1 GROUP BY d.id ORDER BY d.created_at,d.id",
    )
    .bind(event_id)
    .fetch_all(&identity.pool())
    .await;
    let deliveries = match deliveries {
        Ok(rows) => rows
            .into_iter()
            .map(|row| {
                json!({
                    "delivery_id": row.get::<Uuid, _>("id"),
                    "job_id": row.get::<Uuid, _>("id"),
                    "event_id": row.get::<Uuid, _>("event_id"),
                    "target_app_id": row.get::<Uuid, _>("app_id"),
                    "sink": "agent",
                    "status": row.get::<String, _>("state"),
                    "reason": row.get::<String, _>("reason"),
                    "generation": row.get::<i32, _>("generation"),
                    "attempts": row.get::<i32, _>("attempt_count"),
                    "created_at": row.get::<String, _>("created_at"),
                    "updated_at": row.get::<String, _>("updated_at"),
                })
            })
            .collect::<Vec<_>>(),
        Err(error) if missing_table(&error) => Vec::new(),
        Err(_) => return Err(AuthError::Internal),
    };
    let attempts = sqlx::query(
        "SELECT a.delivery_id,d.generation,a.attempt,a.outcome,to_char(a.started_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS started_at,to_char(COALESCE(a.finished_at,clock_timestamp()) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at FROM delivery_attempts a JOIN deliveries d ON d.id=a.delivery_id WHERE d.event_id=$1 ORDER BY a.started_at,a.delivery_id,a.attempt",
    )
    .bind(event_id)
    .fetch_all(&identity.pool())
    .await;
    let attempts = match attempts {
        Ok(rows) => rows
            .into_iter()
            .map(|row| {
                json!({
                    "delivery_id": row.get::<Uuid, _>("delivery_id"),
                    "generation": row.get::<i32, _>("generation"),
                    "attempt": row.get::<i32, _>("attempt"),
                    "outcome": row.get::<String, _>("outcome"),
                    "started_at": row.get::<String, _>("started_at"),
                    "updated_at": row.get::<String, _>("updated_at"),
                })
            })
            .collect::<Vec<_>>(),
        Err(error) if missing_table(&error) => Vec::new(),
        Err(_) => return Err(AuthError::Internal),
    };
    let mut items = vec![json!({
        "id": format!("event:{event_id}"),
        "type": "event.created",
        "occurred_at": event_json["created_at"],
        "event_id": event_id,
        "outcome": "accepted",
    })];
    for delivery in &deliveries {
        items.push(json!({
            "id": format!("delivery:{}", delivery["delivery_id"]),
            "type": format!("delivery.{}", delivery["status"]),
            "occurred_at": delivery["updated_at"],
            "event_id": event_id,
            "delivery_id": delivery["delivery_id"],
            "job_id": delivery["job_id"],
            "generation": delivery["generation"],
            "outcome": delivery["status"],
            "reason": delivery["reason"],
        }));
    }
    Ok(Json(json!({
        "event": event_json,
        "deliveries": deliveries,
        "attempts": attempts,
        "items": items,
    })))
}

async fn event_detail(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path(event_id): Path<Uuid>,
) -> Result<Json<Value>, AuthError> {
    let principal = identity.admin_browser_auth(&headers, false).await?;
    let workspace = workspace_scope(&principal)?;
    let row = sqlx::query(
        "SELECT e.id,e.app_id,e.payload,to_char(e.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,(SELECT count(*) FROM deliveries d WHERE d.event_id=e.id) AS delivery_count FROM events e WHERE e.id=$1 AND ($2::uuid IS NULL OR e.workspace_id=$2)",
    )
    .bind(event_id)
    .bind(workspace)
    .fetch_optional(&identity.pool())
    .await?;
    let Some(row) = row else {
        return Err(AuthError::Conflict);
    };
    let payload: Vec<u8> = row.get("payload");
    let (event_type, data) = event_type_and_data(&payload);
    Ok(Json(json!({
        "id": row.get::<Uuid, _>("id"),
        "type": event_type,
        "source_app_id": row.get::<Uuid, _>("app_id"),
        "target_count": 1,
        "delivery_count": row.get::<i64, _>("delivery_count"),
        "created_at": row.get::<String, _>("created_at"),
        "target_app_ids": [row.get::<Uuid, _>("app_id")],
        "data": data,
    })))
}

async fn dead_letters(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, AuthError> {
    let principal = identity.admin_browser_auth(&headers, false).await?;
    let workspace = workspace_scope(&principal)?;
    let rows = sqlx::query(
        "SELECT d.id,d.event_id,d.app_id,d.attempt_count,to_char(d.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(COALESCE(max(a.finished_at),d.created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at,COALESCE((SELECT da.outcome FROM delivery_attempts da WHERE da.delivery_id=d.id ORDER BY da.attempt DESC LIMIT 1),'attempts_exhausted') AS reason FROM deliveries d LEFT JOIN delivery_attempts a ON a.delivery_id=d.id WHERE d.state='dlq' AND ($1::uuid IS NULL OR d.workspace_id=$1) AND ($2::uuid IS NULL OR d.app_id=$2) AND ($3::uuid IS NULL OR d.id < $3) GROUP BY d.id ORDER BY d.created_at DESC,d.id DESC LIMIT $4",
    )
    .bind(workspace)
    .bind(query.source_app_id.or(query.target_app_id))
    .bind(query.cursor)
    .bind(bounded_limit(query.limit))
    .fetch_all(&identity.pool())
    .await;
    let rows = match rows {
        Ok(rows) => rows,
        Err(error) if missing_table(&error) => Vec::new(),
        Err(_) => return Err(AuthError::Internal),
    };
    let items = rows
        .into_iter()
        .map(|row| {
            json!({
                "delivery_id": row.get::<Uuid, _>("id"),
                "job_id": row.get::<Uuid, _>("id"),
                "event_id": row.get::<Uuid, _>("event_id"),
                "source_app_id": row.get::<Uuid, _>("app_id"),
                "target_app_id": row.get::<Uuid, _>("app_id"),
                "sink": "agent",
                "reason": row.get::<String, _>("reason"),
                "attempts": row.get::<i32, _>("attempt_count"),
                "created_at": row.get::<String, _>("created_at"),
                "updated_at": row.get::<String, _>("updated_at"),
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({"items": items})))
}

async fn dead_letter_detail(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path(delivery_id): Path<Uuid>,
) -> Result<Json<Value>, AuthError> {
    let principal = identity.admin_browser_auth(&headers, false).await?;
    let workspace = workspace_scope(&principal)?;
    let row = sqlx::query(
        "SELECT d.id,d.event_id,d.app_id,d.attempt_count,to_char(d.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(COALESCE(max(a.finished_at),d.created_at) AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at,COALESCE((SELECT da.outcome FROM delivery_attempts da WHERE da.delivery_id=d.id ORDER BY da.attempt DESC LIMIT 1),'attempts_exhausted') AS reason FROM deliveries d LEFT JOIN delivery_attempts a ON a.delivery_id=d.id WHERE d.id=$1 AND d.state='dlq' AND ($2::uuid IS NULL OR d.workspace_id=$2) GROUP BY d.id",
    )
    .bind(delivery_id)
    .bind(workspace)
    .fetch_optional(&identity.pool())
    .await?;
    let Some(row) = row else {
        return Err(AuthError::Conflict);
    };
    let event_type: String = sqlx::query_scalar(
        "SELECT COALESCE(convert_from(payload,'UTF8')::jsonb->>'event_type','unknown') FROM events WHERE id=$1",
    )
    .bind(row.get::<Uuid, _>("event_id"))
    .fetch_optional(&identity.pool())
    .await?
    .flatten()
    .unwrap_or_else(|| "unknown".to_owned());
    Ok(Json(json!({
        "delivery_id": row.get::<Uuid, _>("id"),
        "job_id": row.get::<Uuid, _>("id"),
        "event_id": row.get::<Uuid, _>("event_id"),
        "source_app_id": row.get::<Uuid, _>("app_id"),
        "target_app_id": row.get::<Uuid, _>("app_id"),
        "sink": "agent",
        "reason": row.get::<String, _>("reason"),
        "attempts": row.get::<i32, _>("attempt_count"),
        "created_at": row.get::<String, _>("created_at"),
        "updated_at": row.get::<String, _>("updated_at"),
        "event_type": event_type,
    })))
}

async fn replay_dead_letter(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path(delivery_id): Path<Uuid>,
) -> Result<Json<Value>, AuthError> {
    let _ = identity.admin_browser_auth(&headers, true).await?;
    Ok(Json(
        json!({"items": [replay_one(&identity, &headers, delivery_id).await?]}),
    ))
}

async fn replay_dead_letters(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Json(input): Json<ReplayInput>,
) -> Result<Json<Value>, AuthError> {
    let _ = identity.admin_browser_auth(&headers, true).await?;
    if input.delivery_ids.is_empty() || input.delivery_ids.len() > 100 {
        return Err(AuthError::InvalidInput);
    }
    let mut items = Vec::with_capacity(input.delivery_ids.len());
    for delivery_id in input.delivery_ids {
        items.push(replay_one(&identity, &headers, delivery_id).await?);
    }
    Ok(Json(json!({"items": items})))
}

async fn replay_one(
    identity: &Identity,
    headers: &HeaderMap,
    source: Uuid,
) -> Result<Value, AuthError> {
    let principal = identity.admin_browser_auth(headers, true).await?;
    let actor = match principal {
        Principal::SystemAdmin { user_id } | Principal::WorkspaceOwner { user_id, .. } => user_id,
        _ => return Err(AuthError::Forbidden),
    };
    let mut tx = identity.pool().begin().await?;
    let row = sqlx::query("SELECT id,event_id,workspace_id,app_id,generation,state FROM deliveries WHERE id=$1 FOR UPDATE")
        .bind(source)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(AuthError::Conflict)?;
    authorize(&principal, row.get("workspace_id"), Action::ManageApps)?;
    if row.get::<String, _>("state") != "dlq" {
        if let Some(existing) = sqlx::query_scalar::<_, Uuid>(
            "SELECT delivery_id FROM delivery_replays WHERE source_delivery_id=$1",
        )
        .bind(source)
        .fetch_optional(&mut *tx)
        .await?
        {
            return Ok(
                json!({"delivery_id": existing, "event_id": row.get::<Uuid, _>("event_id"), "job_id": existing, "from_generation": row.get::<i32, _>("generation"), "generation": row.get::<i32, _>("generation"), "status": "pending"}),
            );
        }
        return Err(AuthError::Conflict);
    }
    if sqlx::query_scalar::<_, bool>(
        "SELECT expires_at <= clock_timestamp() FROM events WHERE id=$1",
    )
    .bind(row.get::<Uuid, _>("event_id"))
    .fetch_one(&mut *tx)
    .await?
    {
        return Err(AuthError::Conflict);
    }
    if sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM deliveries WHERE event_id=$1 AND terminal_at IS NULL)",
    )
    .bind(row.get::<Uuid, _>("event_id"))
    .fetch_one(&mut *tx)
    .await?
    {
        return Err(AuthError::Conflict);
    }
    let next: i32 =
        sqlx::query_scalar("SELECT max(generation)+1 FROM deliveries WHERE event_id=$1")
            .bind(row.get::<Uuid, _>("event_id"))
            .fetch_one(&mut *tx)
            .await?;
    let replay = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO deliveries(id,event_id,workspace_id,app_id,generation) VALUES($1,$2,$3,$4,$5)",
    )
    .bind(replay)
    .bind(row.get::<Uuid, _>("event_id"))
    .bind(row.get::<Uuid, _>("workspace_id"))
    .bind(row.get::<Uuid, _>("app_id"))
    .bind(next)
    .execute(&mut *tx)
    .await?;
    sqlx::query("INSERT INTO event_outbox(delivery_id,workspace_id,app_id) VALUES($1,$2,$3)")
        .bind(replay)
        .bind(row.get::<Uuid, _>("workspace_id"))
        .bind(row.get::<Uuid, _>("app_id"))
        .execute(&mut *tx)
        .await?;
    sqlx::query("INSERT INTO delivery_replays(delivery_id,source_delivery_id,actor_user_id) VALUES($1,$2,$3)")
        .bind(replay)
        .bind(source)
        .bind(actor)
        .execute(&mut *tx)
        .await?;
    if table_exists(&mut tx, "admin_audit_log").await? {
        sqlx::query("INSERT INTO admin_audit_log(workspace_id,actor_type,actor_id,action,resource_type,resource_id,outcome,metadata) VALUES($1,'admin',$2,'delivery.replay','delivery',$3,'success',$4::jsonb)")
            .bind(row.get::<Uuid, _>("workspace_id"))
            .bind(actor.to_string())
            .bind(source.to_string())
            .bind(json!({"new_delivery_id": replay, "from_generation": row.get::<i32, _>("generation"), "generation": next}).to_string())
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(
        json!({"delivery_id": replay, "event_id": row.get::<Uuid, _>("event_id"), "job_id": replay, "from_generation": row.get::<i32, _>("generation"), "generation": next, "status": "pending"}),
    )
}

async fn table_exists(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    name: &str,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
        .bind(format!("public.{name}"))
        .fetch_one(&mut **tx)
        .await
}

async fn audit(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, AuthError> {
    let principal = identity.admin_browser_auth(&headers, false).await?;
    let workspace = workspace_scope(&principal)?;
    let rows = sqlx::query("SELECT id, to_char(occurred_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS occurred_at,actor_type,actor_id,action,resource_type,resource_id,outcome,metadata::text AS metadata FROM admin_audit_log WHERE ($1::uuid IS NULL OR workspace_id=$1) AND ($2::text IS NULL OR actor_type=$2) AND ($3::text IS NULL OR action=$3) AND ($4::text IS NULL OR resource_type=$4) AND ($5::text IS NULL OR resource_id=$5) AND ($6::text IS NULL OR outcome=$6) ORDER BY occurred_at DESC,id DESC LIMIT $7")
        .bind(workspace)
        .bind(query.actor_type.as_deref())
        .bind(query.action.as_deref())
        .bind(query.resource_type.as_deref())
        .bind(query.resource_id.as_deref())
        .bind(query.outcome.as_deref())
        .bind(bounded_limit(query.limit))
        .fetch_all(&identity.pool())
        .await;
    let rows = match rows {
        Ok(rows) => rows,
        Err(error) if missing_table(&error) => Vec::new(),
        Err(_) => return Err(AuthError::Internal),
    };
    let items = rows
        .into_iter()
        .map(|row| {
            json!({
                "id": row.get::<i64, _>("id"),
                "occurred_at": row.get::<String, _>("occurred_at"),
                "actor_type": row.get::<String, _>("actor_type"),
                "actor_id": row.get::<Option<String>, _>("actor_id"),
                "action": row.get::<String, _>("action"),
                "resource_type": row.get::<String, _>("resource_type"),
                "resource_id": row.get::<Option<String>, _>("resource_id"),
                "outcome": row.get::<String, _>("outcome"),
                "metadata": serde_json::from_str::<Value>(&row.get::<String, _>("metadata")).unwrap_or_else(|_| json!({})),
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({"items": items})))
}
