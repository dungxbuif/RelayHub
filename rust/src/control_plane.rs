//! Rust-owned Admin control-plane resources that do not belong to identity.
//!
//! Durable function catalog, invocation, and routing-rule CRUD.
//! Invocation is served by the Rust-owned connection-fenced function socket.
use crate::identity::{Action, AuthError, Identity, authorize};
use crate::protocol::Principal;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{
        Path, Query, State, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, patch, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::Row;
use std::time::Duration;
use tokio::time::Instant;
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
            "/api/v1/functions",
            get(list_public_functions).post(create_public_function),
        )
        .route(
            "/api/v1/functions/{function_id}",
            axum::routing::delete(delete_public_function),
        )
        .route(
            "/api/v1/functions/{function_id}/invoke",
            post(invoke_public_function),
        )
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
        .route(
            "/api/v1/functions/socket",
            axum::routing::get(function_socket_upgrade),
        )
        .route("/api/v1/routing/rules", get(list_rules).post(create_rule))
        .route(
            "/api/v1/routing/rules/{rule_id}",
            patch(update_rule).delete(delete_rule),
        )
}

fn application_id(principal: &Principal) -> Result<(Uuid, Uuid), AuthError> {
    match principal {
        Principal::Application {
            workspace_id,
            app_id,
            ..
        } => Ok((*workspace_id, *app_id)),
        _ => Err(AuthError::Forbidden),
    }
}

async fn public_application(
    identity: &Identity,
    headers: &HeaderMap,
    method: &str,
    target: &str,
    body: &[u8],
) -> Result<(Uuid, Uuid), AuthError> {
    let principal = identity
        .authenticate_public_request(headers, method, target, body)
        .await?;
    application_id(&principal)
}

async fn list_public_functions(
    State(identity): State<Identity>,
    headers: HeaderMap,
) -> Result<Json<Value>, AuthError> {
    let (_, app_id) =
        public_application(&identity, &headers, "GET", "/api/v1/functions", &[]).await?;
    let rows = sqlx::query("SELECT id,app_id,name,timeout_seconds,enabled,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at FROM functions WHERE app_id=$1 ORDER BY name,id")
        .bind(app_id)
        .fetch_all(&identity.pool())
        .await?;
    Ok(Json(Value::Array(rows.iter().map(function_json).collect())))
}

async fn create_public_function(
    State(identity): State<Identity>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<Value>), AuthError> {
    let (workspace, app_id) =
        public_application(&identity, &headers, "POST", "/api/v1/functions", &body).await?;
    let input: FunctionCreate =
        serde_json::from_slice(&body).map_err(|_| AuthError::InvalidInput)?;
    let name = validate_function_name(&input.name)?;
    let timeout = validate_timeout(input.timeout_seconds)?;
    let row = sqlx::query("INSERT INTO functions(id,workspace_id,app_id,name,timeout_seconds,enabled) VALUES($1,$2,$3,$4,$5,$6) RETURNING id,app_id,name,timeout_seconds,enabled,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at")
        .bind(Uuid::new_v4())
        .bind(workspace)
        .bind(app_id)
        .bind(name)
        .bind(timeout)
        .bind(input.enabled.unwrap_or(true))
        .fetch_one(&identity.pool())
        .await?;
    Ok((StatusCode::CREATED, Json(function_json(&row))))
}

async fn delete_public_function(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path(function_id): Path<Uuid>,
) -> Result<StatusCode, AuthError> {
    let target = format!("/api/v1/functions/{function_id}");
    let (_, app_id) = public_application(&identity, &headers, "DELETE", &target, &[]).await?;
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

async fn invoke_public_function(
    State(identity): State<Identity>,
    headers: HeaderMap,
    Path(function_id): Path<Uuid>,
    body: Bytes,
) -> Result<Json<Value>, AuthError> {
    let target = format!("/api/v1/functions/{function_id}/invoke");
    let (workspace, caller_app_id) =
        public_application(&identity, &headers, "POST", &target, &body).await?;
    let input: FunctionInvoke =
        serde_json::from_slice(&body).map_err(|_| AuthError::InvalidInput)?;
    let function =
        sqlx::query("SELECT enabled,timeout_seconds,workspace_id FROM functions WHERE id=$1")
            .bind(function_id)
            .fetch_optional(&identity.pool())
            .await?
            .ok_or(AuthError::Conflict)?;
    if !function.get::<bool, _>("enabled") || function.get::<Uuid, _>("workspace_id") != workspace {
        return Err(AuthError::Conflict);
    }
    let key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty() && value.len() <= 256)
        .ok_or(AuthError::InvalidInput)?;
    let invocation = Uuid::new_v4();
    let input_text = serde_json::to_string(&input.input).map_err(|_| AuthError::InvalidInput)?;
    if let Some(row) = sqlx::query("SELECT id,state,result_text,error_text FROM function_invocations WHERE function_id=$1 AND idempotency_key=$2")
        .bind(function_id)
        .bind(key)
        .fetch_optional(&identity.pool())
        .await?
    {
        return Ok(Json(function_result(&row)));
    }
    sqlx::query("INSERT INTO function_invocations(id,function_id,workspace_id,idempotency_key,input_text,state) VALUES($1,$2,$3,$4,$5,'queued')")
        .bind(invocation)
        .bind(function_id)
        .bind(workspace)
        .bind(key)
        .bind(input_text)
        .execute(&identity.pool())
        .await?;
    let deadline =
        Instant::now() + Duration::from_secs(function.get::<i32, _>("timeout_seconds") as u64);
    loop {
        let row = sqlx::query(
            "SELECT state,result_text,error_text FROM function_invocations WHERE id=$1",
        )
        .bind(invocation)
        .fetch_one(&identity.pool())
        .await?;
        if row.get::<String, _>("state") != "queued" {
            return Ok(Json(json!({
                "invocation_id": invocation,
                "ok": row.get::<String, _>("state") == "succeeded",
                "result": row.get::<Option<String>, _>("result_text").and_then(|value| serde_json::from_str::<Value>(&value).ok()),
                "error": row.get::<Option<String>, _>("error_text").and_then(|value| serde_json::from_str::<Value>(&value).ok()),
            })));
        }
        if Instant::now() >= deadline {
            sqlx::query("UPDATE function_invocations SET state='failed',error_text='function_timeout',completed_at=clock_timestamp() WHERE id=$1 AND state='queued'")
                .bind(invocation)
                .execute(&identity.pool())
                .await?;
            return Ok(Json(
                json!({"invocation_id":invocation,"ok":false,"error":"function_timeout"}),
            ));
        }
        let _ = caller_app_id;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn function_result(row: &sqlx::postgres::PgRow) -> Value {
    let state = row.get::<String, _>("state");
    json!({
        "invocation_id": row.get::<Uuid, _>("id"),
        "ok": state == "succeeded",
        "result": row.get::<Option<String>, _>("result_text").and_then(|value| serde_json::from_str::<Value>(&value).ok()),
        "error": row.get::<Option<String>, _>("error_text").and_then(|value| serde_json::from_str::<Value>(&value).ok()),
    })
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
    let function =
        sqlx::query("SELECT enabled,timeout_seconds FROM functions WHERE id=$1 AND app_id=$2")
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
    sqlx::query("INSERT INTO function_invocations(id,function_id,workspace_id,idempotency_key,input_text,state) VALUES($1,$2,$3,$4,$5,'queued')")
        .bind(invocation).bind(function_id).bind(workspace).bind(key).bind(input_text).execute(&identity.pool()).await?;
    let deadline =
        Instant::now() + Duration::from_secs(function.get::<i32, _>("timeout_seconds") as u64);
    loop {
        let row = sqlx::query(
            "SELECT state,result_text,error_text FROM function_invocations WHERE id=$1",
        )
        .bind(invocation)
        .fetch_one(&identity.pool())
        .await?;
        let state: String = row.get("state");
        if state != "queued" {
            return Ok(Json(json!({
                "invocation_id": invocation,
                "ok": state == "succeeded",
                "result": row.get::<Option<String>, _>("result_text").and_then(|value| serde_json::from_str::<Value>(&value).ok()),
                "error": row.get::<Option<String>, _>("error_text").map(|value| serde_json::from_str::<Value>(&value).unwrap_or(Value::String(value))),
            })));
        }
        if Instant::now() >= deadline {
            sqlx::query("UPDATE function_invocations SET state='failed',error_text='function_timeout',completed_at=clock_timestamp() WHERE id=$1 AND state='queued'")
                .bind(invocation)
                .execute(&identity.pool())
                .await?;
            return Ok(Json(json!({
                "invocation_id": invocation,
                "ok": false,
                "error": "function_timeout",
            })));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[derive(Debug, Deserialize)]
struct FunctionSocketQuery {
    token: String,
}

async fn function_socket_upgrade(
    State(identity): State<Identity>,
    Query(query): Query<FunctionSocketQuery>,
    ws: WebSocketUpgrade,
) -> Result<Response, AuthError> {
    let principal = identity.authenticate(&query.token).await?;
    let app_id = match principal {
        Principal::Application { app_id, .. } | Principal::Agent { app_id, .. } => app_id,
        _ => return Err(AuthError::Forbidden),
    };
    let connection_id = format!("conn_{}", Uuid::new_v4().simple());
    Ok(ws
        .on_upgrade(move |socket| function_socket(socket, identity, app_id, connection_id))
        .into_response())
}

async fn claim_function(
    identity: &Identity,
    app_id: Uuid,
    connection_id: &str,
) -> Result<Option<Value>, sqlx::Error> {
    let mut tx = identity.pool().begin().await?;
    sqlx::query("DELETE FROM function_runtime_claims WHERE claimed_at < clock_timestamp()-interval '60 seconds'")
        .execute(&mut *tx)
        .await?;
    let row = sqlx::query("SELECT i.id,i.input_text,f.name,f.timeout_seconds,to_char((clock_timestamp() + f.timeout_seconds * interval '1 second') AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS deadline FROM function_invocations i JOIN functions f ON f.id=i.function_id WHERE i.state='queued' AND f.app_id=$1 AND f.enabled AND NOT EXISTS (SELECT 1 FROM function_runtime_claims c WHERE c.invocation_id=i.id) ORDER BY i.created_at,i.id FOR UPDATE OF i SKIP LOCKED LIMIT 1")
        .bind(app_id)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(row) = row else {
        tx.commit().await?;
        return Ok(None);
    };
    let invocation_id: Uuid = row.get("id");
    let inserted = sqlx::query("INSERT INTO function_runtime_claims(invocation_id,app_id,connection_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING")
        .bind(invocation_id)
        .bind(app_id)
        .bind(connection_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    tx.commit().await?;
    if inserted != 1 {
        return Ok(None);
    }
    let input = serde_json::from_str::<Value>(row.get::<String, _>("input_text").as_str())
        .unwrap_or_else(|_| json!({}));
    Ok(Some(json!({
        "type": "rpc.invoke",
        "invocation_id": invocation_id,
        "function": row.get::<String, _>("name"),
        "input": input,
        "deadline": row.get::<String, _>("deadline"),
    })))
}

async fn complete_function(
    identity: &Identity,
    app_id: Uuid,
    connection_id: &str,
    frame: Value,
) -> Result<(), sqlx::Error> {
    if frame.get("type").and_then(Value::as_str) != Some("rpc.result") {
        return Ok(());
    }
    let invocation_id = frame
        .get("invocation_id")
        .and_then(Value::as_str)
        .and_then(|value| Uuid::parse_str(value).ok());
    let Some(invocation_id) = invocation_id else {
        return Ok(());
    };
    let ok = frame.get("ok").and_then(Value::as_bool).unwrap_or(false);
    let result_text = if ok {
        frame.get("result").map(Value::to_string)
    } else {
        None
    };
    let error_text = if ok {
        None
    } else {
        frame.get("error").map(Value::to_string)
    };
    let mut tx = identity.pool().begin().await?;
    let state = if ok { "succeeded" } else { "failed" };
    let changed = sqlx::query("UPDATE function_invocations SET state=$2,result_text=$3,error_text=$4,completed_at=clock_timestamp() WHERE id=$1 AND state='queued' AND EXISTS (SELECT 1 FROM function_runtime_claims c WHERE c.invocation_id=$1 AND c.app_id=$5 AND c.connection_id=$6)")
        .bind(invocation_id)
        .bind(state)
        .bind(result_text)
        .bind(error_text)
        .bind(app_id)
        .bind(connection_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
    if changed == 1 {
        sqlx::query("DELETE FROM function_runtime_claims WHERE invocation_id=$1 AND app_id=$2 AND connection_id=$3")
            .bind(invocation_id)
            .bind(app_id)
            .bind(connection_id)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await
}

async fn function_socket(
    mut socket: WebSocket,
    identity: Identity,
    app_id: Uuid,
    connection_id: String,
) {
    let ready = json!({
        "type": "ready",
        "app_id": app_id,
        "connection_id": connection_id,
        "capabilities": ["functions"],
    });
    if socket
        .send(Message::Text(ready.to_string().into()))
        .await
        .is_err()
    {
        return;
    }
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            _ = tick.tick() => {
                match claim_function(&identity, app_id, &connection_id).await {
                    Ok(Some(frame)) => {
                        if socket.send(Message::Text(frame.to_string().into())).await.is_err() { break; }
                    }
                    Ok(None) => {}
                    Err(_) => break,
                }
            }
            message = socket.recv() => {
                let Some(Ok(message)) = message else { break; };
                match message {
                    Message::Text(text) => {
                        let frame = serde_json::from_str::<Value>(&text).unwrap_or_else(|_| json!({}));
                        if complete_function(&identity, app_id, &connection_id, frame).await.is_err() { break; }
                    }
                    Message::Binary(bytes) => {
                        let frame = serde_json::from_slice::<Value>(&bytes).unwrap_or_else(|_| json!({}));
                        if complete_function(&identity, app_id, &connection_id, frame).await.is_err() { break; }
                    }
                    Message::Close(_) => break,
                    Message::Ping(_) | Message::Pong(_) => {}
                }
            }
        }
    }
    let _ = sqlx::query("DELETE FROM function_runtime_claims WHERE app_id=$1 AND connection_id=$2")
        .bind(app_id)
        .bind(connection_id)
        .execute(&identity.pool())
        .await;
}

/// Compatibility handler for the pre-v2 function clients that still connect
/// to `/ws` without a negotiated realtime subprotocol. The actual function
/// protocol remains the Rust-owned, connection-fenced implementation above.
pub(crate) fn legacy_function_socket_response(
    identity: Identity,
    principal: Principal,
    ws: WebSocketUpgrade,
) -> Response {
    let app_id = match principal {
        Principal::Application { app_id, .. } | Principal::Agent { app_id, .. } => app_id,
        _ => return AuthError::Forbidden.into_response(),
    };
    let connection_id = format!("conn_{}", Uuid::new_v4().simple());
    ws.on_upgrade(move |socket| function_socket(socket, identity, app_id, connection_id))
        .into_response()
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
