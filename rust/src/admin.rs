//! Rust-owned read projections for the full Admin console.
use crate::identity::{AuthError, Identity};
use axum::{
    Json, Router,
    extract::{Query, State},
    routing::get,
};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::Row;

#[derive(Debug, Deserialize)]
struct ListQuery {
    limit: Option<u32>,
}

pub fn router() -> Router<Identity> {
    Router::new()
        .route("/api/v1/admin/dashboard", get(dashboard))
        .route("/api/v1/admin/events", get(events))
}

async fn dashboard(
    State(identity): State<Identity>,
    headers: axum::http::HeaderMap,
) -> Result<Json<Value>, AuthError> {
    let principal = identity.admin_browser_auth(&headers, false).await?;
    let workspace = match principal {
        crate::protocol::Principal::WorkspaceOwner { workspace_id, .. } => Some(workspace_id),
        crate::protocol::Principal::SystemAdmin { .. } => None,
        _ => return Err(AuthError::Forbidden),
    };
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
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("42P01") => {
            (0, 0, 0, None)
        }
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
    headers: axum::http::HeaderMap,
    Query(query): Query<ListQuery>,
) -> Result<Json<Value>, AuthError> {
    let principal = identity.admin_browser_auth(&headers, false).await?;
    let workspace = match principal {
        crate::protocol::Principal::WorkspaceOwner { workspace_id, .. } => Some(workspace_id),
        crate::protocol::Principal::SystemAdmin { .. } => None,
        _ => return Err(AuthError::Forbidden),
    };
    let limit = query.limit.unwrap_or(25).clamp(1, 100) as i64;
    let pool = identity.pool();
    let rows = sqlx::query(
        "SELECT e.id, e.app_id, e.payload, to_char(e.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at, (SELECT count(*) FROM deliveries d WHERE d.event_id=e.id) AS delivery_count FROM events e WHERE ($1::uuid IS NULL OR e.workspace_id=$1) ORDER BY e.created_at DESC, e.id DESC LIMIT $2",
    )
    .bind(workspace)
    .bind(limit)
    .fetch_all(&pool)
    .await;
    let rows = match rows {
        Ok(rows) => rows,
        Err(sqlx::Error::Database(error)) if error.code().as_deref() == Some("42P01") => Vec::new(),
        Err(_) => return Err(AuthError::Internal),
    };
    let items = rows
        .into_iter()
        .map(|row| {
            let payload: Vec<u8> = row.get("payload");
            let event_type = serde_json::from_slice::<Value>(&payload)
                .ok()
                .and_then(|value| {
                    value
                        .get("event_type")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .unwrap_or_else(|| "unknown".to_owned());
            json!({
                "id": row.get::<uuid::Uuid, _>("id"),
                "type": event_type,
                "source_app_id": row.get::<uuid::Uuid, _>("app_id"),
                "target_count": 1,
                "delivery_count": row.get::<i64, _>("delivery_count"),
                "created_at": row.get::<String, _>("created_at"),
            })
        })
        .collect::<Vec<_>>();
    Ok(Json(json!({"items": items})))
}
