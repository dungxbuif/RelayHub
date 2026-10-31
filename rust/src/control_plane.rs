//! Rust-owned Admin control-plane resources that do not belong to identity.
//!
//! The first slice ports the durable function catalog and routing-rule CRUD.
//! Invocation is persisted and explicitly reports that the handler runtime is
//! not available until the Rust agent execution protocol is migrated.
use crate::identity::{Action, AuthError, Identity, authorize};
use crate::protocol::Principal;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, patch, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::Row;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FunctionCreate {
    name: String,
    timeout_seconds: i32,
    enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FunctionUpdate {
    name: Option<String>,
    timeout_seconds: Option<i32>,
    enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FunctionInvoke {
    input: Value,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleCreate {
    source_app_id: Option<Uuid>,
    event_type: String,
    target_app_id: Uuid,
    realtime_channel: Option<String>,
    enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuleUpdate {
    source_app_id: Option<Option<Uuid>>,
    event_type: Option<String>,
    target_app_id: Option<Uuid>,
    realtime_channel: Option<Option<String>>,
    enabled: Option<bool>,
}

pub fn router() -> Router<Identity> {
    Router::new()
        .route(
            "/api/v1/admin/apps/{app_id}/functions",
            get(list_functions).post(create_function),
        )
        .route(
            "/api/v1/admin/apps/{app_id}/functions/{function_id}",
            patch(update_function).delete(delete_function),
        )
        .route(
            "/api/v1/admin/apps/{app_id}/functions/{function_id}/invoke",
            post(invoke_function),
        )
        .route("/api/v1/routing/rules", get(list_rules).post(create_rule))
        .route(
            "/api/v1/routing/rules/{rule_id}",
            patch(update_rule).delete(delete_rule),
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

fn validate_function_name(name: &str) -> Result<String, AuthError> {
    let name = name.trim();
    if name.is_empty() || name.len() > 64 || name.chars().any(char::is_control) {
        return Err(AuthError::InvalidInput);
    }
    Ok(name.to_owned())
}

fn validate_timeout(timeout_seconds: i32) -> Result<i32, AuthError> {
    if (1..=30).contains(&timeout_seconds) {
        Ok(timeout_seconds)
    } else {
        Err(AuthError::InvalidInput)
    }
}

fn function_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({
        "id": row.get::<Uuid, _>("id"),
        "app_id": row.get::<Uuid, _>("app_id"),
        "name": row.get::<String, _>("name"),
        "timeout_seconds": row.get::<i32, _>("timeout_seconds"),
        "enabled": row.get::<bool, _>("enabled"),
        "created_at": row.get::<String, _>("created_at"),
        "updated_at": row.get::<String, _>("updated_at"),
    })
}

async fn list_functions(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path(app_id): Path<Uuid>,
) -> Result<Json<Value>, AuthError> {
    let principal = identity.admin_browser_auth(&headers, false).await?;
    app_workspace(&identity, &principal, app_id).await?;
    let rows = sqlx::query("SELECT id,app_id,name,timeout_seconds,enabled,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at FROM functions WHERE app_id=$1 ORDER BY created_at,id")
        .bind(app_id).fetch_all(&identity.pool()).await?;
    Ok(Json(Value::Array(rows.iter().map(function_json).collect())))
}

async fn create_function(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path(app_id): Path<Uuid>,
    Json(input): Json<FunctionCreate>,
) -> Result<(StatusCode, Json<Value>), AuthError> {
    let principal = identity.admin_browser_auth(&headers, true).await?;
    let workspace = app_workspace(&identity, &principal, app_id).await?;
    let name = validate_function_name(&input.name)?;
    let timeout = validate_timeout(input.timeout_seconds)?;
    let row = sqlx::query("INSERT INTO functions(id,workspace_id,app_id,name,timeout_seconds,enabled) VALUES($1,$2,$3,$4,$5,$6) RETURNING id,app_id,name,timeout_seconds,enabled,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at")
        .bind(Uuid::new_v4()).bind(workspace).bind(app_id).bind(name).bind(timeout).bind(input.enabled.unwrap_or(true))
        .fetch_one(&identity.pool()).await?;
    Ok((StatusCode::CREATED, Json(function_json(&row))))
}

async fn update_function(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, function_id)): Path<(Uuid, Uuid)>,
    Json(input): Json<FunctionUpdate>,
) -> Result<Json<Value>, AuthError> {
    let principal = identity.admin_browser_auth(&headers, true).await?;
    app_workspace(&identity, &principal, app_id).await?;
    let name = input
        .name
        .as_deref()
        .map(validate_function_name)
        .transpose()?;
    let timeout = input.timeout_seconds.map(validate_timeout).transpose()?;
    let row = sqlx::query("UPDATE functions SET name=COALESCE($2,name),timeout_seconds=COALESCE($3,timeout_seconds),enabled=COALESCE($4,enabled),updated_at=clock_timestamp() WHERE id=$1 AND app_id=$5 RETURNING id,app_id,name,timeout_seconds,enabled,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at")
        .bind(function_id).bind(name).bind(timeout).bind(input.enabled).bind(app_id)
        .fetch_optional(&identity.pool()).await?.ok_or(AuthError::Conflict)?;
    Ok(Json(function_json(&row)))
}

async fn delete_function(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, function_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, AuthError> {
    let principal = identity.admin_browser_auth(&headers, true).await?;
    app_workspace(&identity, &principal, app_id).await?;
    let deleted = sqlx::query("DELETE FROM functions WHERE id=$1 AND app_id=$2")
        .bind(function_id)
        .bind(app_id)
        .execute(&identity.pool())
        .await?;
    if deleted.rows_affected() != 1 {
        return Err(AuthError::Conflict);
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn invoke_function(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path((app_id, function_id)): Path<(Uuid, Uuid)>,
    Json(input): Json<FunctionInvoke>,
) -> Result<Json<Value>, AuthError> {
    let principal = identity.admin_browser_auth(&headers, true).await?;
    let workspace = app_workspace(&identity, &principal, app_id).await?;
    let function = sqlx::query("SELECT enabled FROM functions WHERE id=$1 AND app_id=$2")
        .bind(function_id)
        .bind(app_id)
        .fetch_optional(&identity.pool())
        .await?
        .ok_or(AuthError::Conflict)?;
    if !function.get::<bool, _>("enabled") {
        return Err(AuthError::Conflict);
    }
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 256)
        .ok_or(AuthError::InvalidInput)?;
    let invocation = Uuid::new_v4();
    let input_text = serde_json::to_string(&input.input).map_err(|_| AuthError::InvalidInput)?;
    let existing = sqlx::query("SELECT id,state,result_text,error_text FROM function_invocations WHERE function_id=$1 AND idempotency_key=$2")
        .bind(function_id).bind(key).fetch_optional(&identity.pool()).await?;
    if let Some(row) = existing {
        return Ok(Json(json!({
            "invocation_id": row.get::<Uuid, _>("id"),
            "ok": row.get::<String, _>("state") == "succeeded",
            "result": row.get::<Option<String>, _>("result_text").and_then(|v| serde_json::from_str::<Value>(&v).ok()),
            "error": row.get::<Option<String>, _>("error_text"),
        })));
    }
    sqlx::query("INSERT INTO function_invocations(id,function_id,workspace_id,idempotency_key,input_text,state,error_text) VALUES($1,$2,$3,$4,$5,'failed','function_runtime_unavailable')")
        .bind(invocation).bind(function_id).bind(workspace).bind(key).bind(input_text).execute(&identity.pool()).await?;
    Ok(Json(json!({
        "invocation_id": invocation,
        "ok": false,
        "error": "function_runtime_unavailable",
    })))
}

fn rule_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({
        "id": row.get::<Uuid, _>("id"),
        "source_app_id": row.get::<Option<Uuid>, _>("source_app_id"),
        "event_type": row.get::<String, _>("event_type"),
        "target_app_id": row.get::<Uuid, _>("target_app_id"),
        "realtime_channel": row.get::<Option<String>, _>("realtime_channel"),
        "enabled": row.get::<bool, _>("enabled"),
        "created_at": row.get::<String, _>("created_at"),
        "updated_at": row.get::<String, _>("updated_at"),
    })
}

async fn list_rules(
    State(identity): State<Identity>,
    headers: HeaderMap,
) -> Result<Json<Value>, AuthError> {
    let principal = identity.admin_browser_auth(&headers, false).await?;
    let workspace = match principal {
        Principal::WorkspaceOwner { workspace_id, .. } => Some(workspace_id),
        Principal::SystemAdmin { .. } => None,
        _ => return Err(AuthError::Forbidden),
    };
    let rows = sqlx::query("SELECT id,source_app_id,event_type,target_app_id,realtime_channel,enabled,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at FROM routing_rules WHERE deleted_at IS NULL AND ($1::uuid IS NULL OR workspace_id=$1) ORDER BY created_at,id")
        .bind(workspace).fetch_all(&identity.pool()).await?;
    Ok(Json(Value::Array(rows.iter().map(rule_json).collect())))
}

async fn create_rule(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Json(input): Json<RuleCreate>,
) -> Result<(StatusCode, Json<Value>), AuthError> {
    let principal = identity.admin_browser_auth(&headers, true).await?;
    let workspace = app_workspace(&identity, &principal, input.target_app_id).await?;
    if let Some(source) = input.source_app_id {
        if app_workspace(&identity, &principal, source).await? != workspace {
            return Err(AuthError::Forbidden);
        }
    }
    let event_type = input.event_type.trim();
    if event_type.is_empty() || event_type.len() > 200 {
        return Err(AuthError::InvalidInput);
    }
    let row = sqlx::query("INSERT INTO routing_rules(id,workspace_id,source_app_id,event_type,target_app_id,realtime_channel,enabled) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id,source_app_id,event_type,target_app_id,realtime_channel,enabled,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at")
        .bind(Uuid::new_v4()).bind(workspace).bind(input.source_app_id).bind(event_type).bind(input.target_app_id).bind(input.realtime_channel).bind(input.enabled.unwrap_or(true)).fetch_one(&identity.pool()).await?;
    Ok((StatusCode::CREATED, Json(rule_json(&row))))
}

async fn update_rule(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path(rule_id): Path<Uuid>,
    Json(input): Json<RuleUpdate>,
) -> Result<Json<Value>, AuthError> {
    let principal = identity.admin_browser_auth(&headers, true).await?;
    let old =
        sqlx::query("SELECT workspace_id FROM routing_rules WHERE id=$1 AND deleted_at IS NULL")
            .bind(rule_id)
            .fetch_optional(&identity.pool())
            .await?
            .ok_or(AuthError::Conflict)?;
    let old_workspace: Uuid = old.get("workspace_id");
    authorize(&principal, old_workspace, Action::ManageApps)?;
    if let Some(target) = input.target_app_id {
        if app_workspace(&identity, &principal, target).await? != old_workspace {
            return Err(AuthError::Forbidden);
        }
    }
    if let Some(source) = input.source_app_id.flatten() {
        if app_workspace(&identity, &principal, source).await? != old_workspace {
            return Err(AuthError::Forbidden);
        }
    }
    let event_type = input
        .event_type
        .as_deref()
        .map(str::trim)
        .map(str::to_owned);
    if event_type
        .as_deref()
        .is_some_and(|v| v.is_empty() || v.len() > 200)
    {
        return Err(AuthError::InvalidInput);
    }
    let row = sqlx::query("UPDATE routing_rules SET source_app_id=COALESCE($2,source_app_id),event_type=COALESCE($3,event_type),target_app_id=COALESCE($4,target_app_id),realtime_channel=COALESCE($5,realtime_channel),enabled=COALESCE($6,enabled),updated_at=clock_timestamp() WHERE id=$1 RETURNING id,source_app_id,event_type,target_app_id,realtime_channel,enabled,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at")
        .bind(rule_id).bind(input.source_app_id.flatten()).bind(event_type).bind(input.target_app_id).bind(input.realtime_channel.flatten()).bind(input.enabled).fetch_one(&identity.pool()).await?;
    Ok(Json(rule_json(&row)))
}

async fn delete_rule(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path(rule_id): Path<Uuid>,
) -> Result<StatusCode, AuthError> {
    let principal = identity.admin_browser_auth(&headers, true).await?;
    let row =
        sqlx::query("SELECT workspace_id FROM routing_rules WHERE id=$1 AND deleted_at IS NULL")
            .bind(rule_id)
            .fetch_optional(&identity.pool())
            .await?
            .ok_or(AuthError::Conflict)?;
    authorize(&principal, row.get("workspace_id"), Action::ManageApps)?;
    sqlx::query("UPDATE routing_rules SET deleted_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id=$1")
        .bind(rule_id).execute(&identity.pool()).await?;
    Ok(StatusCode::NO_CONTENT)
}
