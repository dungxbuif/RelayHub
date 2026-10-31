//! Ephemeral Core NATS fanout. Receipts describe routing, never durable acceptance or processing.
use crate::{
    identity::{Action, AuthError, authorize},
    protocol::{
        Frame, MAX_BODY_BYTES, PRESENCE_EXPIRY_SECONDS, PRESENCE_HEARTBEAT_SECONDS, Principal,
        ProtocolError, VERSION, parse_frame,
    },
};
use axum::{
    Json,
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use base64::{
    Engine,
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
};
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Digest;
use sqlx::Row;
use std::{
    collections::HashMap,
    future::Future,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use uuid::Uuid;

pub const OUTBOUND_BUFFER_FRAMES: usize = 64;
const IO_TIMEOUT: Duration = Duration::from_secs(2);
pub const SCOPED_TOKEN_TTL_SECONDS: u64 = 60;
const STREAM_SUBPROTOCOL: &str = "relayhub.stream.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RealtimeError {
    Forbidden,
    InvalidInput,
    PayloadTooLarge,
    Unavailable,
}
impl IntoResponse for RealtimeError {
    fn into_response(self) -> Response {
        let (status, error) = match self {
            Self::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
            Self::InvalidInput => (StatusCode::UNPROCESSABLE_ENTITY, "invalid_input"),
            Self::PayloadTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large"),
            Self::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "realtime_unavailable"),
        };
        (status, Json(serde_json::json!({"error": error}))).into_response()
    }
}
impl std::fmt::Display for RealtimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for RealtimeError {}

/// The count is a snapshot of unexpired presence, not proof of delivery or processing.
#[derive(Debug, Serialize)]
pub struct PublishReceipt {
    pub status: u16,
    pub observed_recipients: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisconnectReason {
    SlowReader,
    TransportUnavailable,
    Closed,
}
impl DisconnectReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SlowReader => "slow_reader",
            Self::TransportUnavailable => "realtime_unavailable",
            Self::Closed => "closed",
        }
    }
}

#[derive(Clone)]
pub struct Realtime {
    gateway: Uuid,
    nats: async_nats::Client,
    redis: redis::aio::MultiplexedConnection,
    namespace: String,
    pool: Option<sqlx::PgPool>,
    object_store: Option<crate::object_store::ObjectStore>,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ScopedTokenClaims {
    workspace_id: Uuid,
    app_id: Uuid,
    channel: String,
    publish_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StreamTokenClaims {
    workspace_id: Uuid,
    app_id: Uuid,
    scopes: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct RealtimeV2TokenClaims {
    workspace_id: Uuid,
    app_id: Uuid,
    client_id: String,
    channels: Vec<String>,
}
struct Subscription {
    id: Uuid,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}
struct Outbound {
    subscription: Uuid,
    frame: Frame,
}

/// One authenticated connection; every channel shares its single 64-frame outbound queue.
pub struct Connection {
    realtime: Realtime,
    principal: Principal,
    session: Uuid,
    subscriptions: HashMap<String, Subscription>,
    sender: mpsc::Sender<Outbound>,
    receiver: mpsc::Receiver<Outbound>,
    reason: watch::Sender<Option<DisconnectReason>>,
}

async fn bounded<T, E>(future: impl Future<Output = Result<T, E>>) -> Result<T, RealtimeError> {
    tokio::time::timeout(IO_TIMEOUT, future)
        .await
        .map_err(|_| RealtimeError::Unavailable)?
        .map_err(|_| RealtimeError::Unavailable)
}
fn scope(principal: &Principal, publish: bool) -> Result<(Uuid, Uuid), RealtimeError> {
    let (workspace, app) = match principal {
        Principal::Application {
            workspace_id,
            app_id,
            ..
        }
        | Principal::Agent {
            workspace_id,
            app_id,
            ..
        } => (*workspace_id, *app_id),
        _ => return Err(RealtimeError::Forbidden),
    };
    authorize(
        principal,
        workspace,
        if publish {
            Action::Publish { app_id: app }
        } else {
            Action::Connect { app_id: app }
        },
    )
    .map_err(|_| RealtimeError::Forbidden)?;
    Ok((workspace, app))
}
fn validate_channel(channel: &str) -> Result<(), RealtimeError> {
    if channel.len() > 128
        || channel.split('.').any(|part| {
            part.is_empty()
                || !part
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
        })
    {
        return Err(RealtimeError::InvalidInput);
    }
    Ok(())
}
impl Realtime {
    pub fn router(&self, identity: crate::identity::Identity) -> axum::Router {
        axum::Router::new()
            .route("/api/v1/realtime", axum::routing::post(publish_http))
            .route(
                "/api/v1/realtime/channels/{channel}/publish",
                axum::routing::post(legacy_channel_publish),
            )
            .route("/api/v1/socket/token", axum::routing::post(socket_token))
            .route(
                "/api/v2/realtime/channels/{channel}/history",
                axum::routing::get(v2_history_http),
            )
            .route("/api/v1/stream", axum::routing::get(legacy_stream_upgrade))
            .route("/api/v1/realtime/{channel}", axum::routing::get(upgrade))
            .route("/api/v2/realtime/files", axum::routing::post(file_create))
            .route(
                "/api/v2/realtime/files/{file_id}/complete",
                axum::routing::post(file_complete),
            )
            .route(
                "/api/v2/realtime/files/{file_id}/download",
                axum::routing::get(file_download),
            )
            .route(
                "/api/v2/realtime/push/devices",
                axum::routing::post(push_register),
            )
            .route(
                "/api/v2/realtime/push/devices/{device_id}",
                axum::routing::delete(push_delete),
            )
            .route(
                "/api/v2/realtime/push/channels/{channel}/devices/{device_id}",
                axum::routing::put(push_bind).delete(push_unbind),
            )
            .route(
                "/api/v2/realtime/push/channels/{channel}/notifications",
                axum::routing::post(push_notify),
            )
            .route("/ws", axum::routing::get(upgrade_v2))
            .layer(axum::extract::DefaultBodyLimit::max(2 * MAX_BODY_BYTES))
            .with_state((self.clone(), identity))
    }
    // async-nats flush only drains local writes. This echo fences preceding SUB/PUB commands.
    async fn barrier(&self) -> Result<(), RealtimeError> {
        bounded(async {
            let subject = format!("{}.v1.realtime.barrier.{}", self.namespace, Uuid::new_v4());
            let mut echo = self
                .nats
                .subscribe(subject.clone())
                .await
                .map_err(|_| RealtimeError::Unavailable)?;
            self.nats
                .publish(subject, Vec::new().into())
                .await
                .map_err(|_| RealtimeError::Unavailable)?;
            echo.next().await.ok_or(RealtimeError::Unavailable)?;
            Ok::<_, RealtimeError>(())
        })
        .await
    }
    /// Supply a private Core NATS client, configured with bounded subscription capacity.
    pub fn new(
        gateway: Uuid,
        nats: async_nats::Client,
        redis: redis::aio::MultiplexedConnection,
        namespace: &str,
    ) -> Result<Self, RealtimeError> {
        if namespace.is_empty()
            || namespace.len() > 128
            || !namespace
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
        {
            return Err(RealtimeError::InvalidInput);
        }
        Ok(Self {
            gateway,
            nats,
            redis,
            namespace: namespace.to_owned(),
            pool: None,
            object_store: crate::object_store::ObjectStore::from_env().ok(),
        })
    }

    pub fn new_with_pool(
        gateway: Uuid,
        nats: async_nats::Client,
        redis: redis::aio::MultiplexedConnection,
        namespace: &str,
        pool: sqlx::PgPool,
    ) -> Result<Self, RealtimeError> {
        let mut realtime = Self::new(gateway, nats, redis, namespace)?;
        realtime.pool = Some(pool);
        Ok(realtime)
    }
    pub fn connect(&self, principal: &Principal) -> Result<Connection, RealtimeError> {
        scope(principal, false)?;
        let (sender, receiver) = mpsc::channel(OUTBOUND_BUFFER_FRAMES);
        let (reason, _) = watch::channel(None);
        Ok(Connection {
            realtime: self.clone(),
            principal: principal.clone(),
            session: Uuid::new_v4(),
            subscriptions: HashMap::new(),
            sender,
            receiver,
            reason,
        })
    }
    pub async fn issue_scoped_token(
        &self,
        workspace_id: Uuid,
        app_id: Uuid,
        channel: &str,
    ) -> Result<String, RealtimeError> {
        validate_channel(channel)?;
        let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let claims = serde_json::to_string(&ScopedTokenClaims {
            workspace_id,
            app_id,
            channel: channel.to_owned(),
            publish_only: true,
        })
        .map_err(|_| RealtimeError::Unavailable)?;
        redis::cmd("SET")
            .arg(self.scoped_token_key(&token))
            .arg(claims)
            .arg("EX")
            .arg(SCOPED_TOKEN_TTL_SECONDS)
            .arg("NX")
            .query_async::<Option<String>>(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?
            .ok_or(RealtimeError::Unavailable)?;
        Ok(token)
    }

    pub async fn issue_stream_token(
        &self,
        workspace_id: Uuid,
        app_id: Uuid,
        scopes: &[String],
        ttl_seconds: i64,
    ) -> Result<String, RealtimeError> {
        if scopes.is_empty()
            || scopes.len() > 8
            || scopes.iter().any(|scope| {
                !matches!(
                    scope.as_str(),
                    "ws:connect" | "ws:subscribe" | "ws:read" | "stream:connect"
                )
            })
            || !(1..=900).contains(&ttl_seconds)
        {
            return Err(RealtimeError::InvalidInput);
        }
        let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let claims = serde_json::to_string(&StreamTokenClaims {
            workspace_id,
            app_id,
            scopes: scopes.to_owned(),
        })
        .map_err(|_| RealtimeError::Unavailable)?;
        redis::cmd("SET")
            .arg(self.stream_token_key(&token))
            .arg(claims)
            .arg("EX")
            .arg(ttl_seconds)
            .arg("NX")
            .query_async::<Option<String>>(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?
            .ok_or(RealtimeError::Unavailable)?;
        Ok(token)
    }

    async fn authenticate_stream_token(
        &self,
        token: &str,
    ) -> Result<StreamTokenClaims, RealtimeError> {
        let claims: Option<String> = redis::cmd("GET")
            .arg(self.stream_token_key(token))
            .query_async(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
        claims
            .and_then(|value| serde_json::from_str::<StreamTokenClaims>(&value).ok())
            .ok_or(RealtimeError::Forbidden)
    }

    pub async fn issue_v2_scoped_token(
        &self,
        workspace_id: Uuid,
        app_id: Uuid,
        client_id: &str,
        channels: &[String],
    ) -> Result<String, RealtimeError> {
        self.issue_v2_scoped_token_with_ttl(
            workspace_id,
            app_id,
            client_id,
            channels,
            SCOPED_TOKEN_TTL_SECONDS as i64,
        )
        .await
    }

    pub async fn issue_v2_scoped_token_with_ttl(
        &self,
        workspace_id: Uuid,
        app_id: Uuid,
        client_id: &str,
        channels: &[String],
        ttl_seconds: i64,
    ) -> Result<String, RealtimeError> {
        if client_id.is_empty()
            || client_id.len() > 128
            || !client_id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"_.:-".contains(&c))
            || channels.is_empty()
            || channels.len() > 100
            || !(1..=900).contains(&ttl_seconds)
        {
            return Err(RealtimeError::InvalidInput);
        }
        for channel in channels {
            validate_channel(channel)?;
        }
        let token = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
        let claims = serde_json::to_string(&RealtimeV2TokenClaims {
            workspace_id,
            app_id,
            client_id: client_id.to_owned(),
            channels: channels.to_owned(),
        })
        .map_err(|_| RealtimeError::Unavailable)?;
        redis::cmd("SET")
            .arg(self.v2_scoped_token_key(&token))
            .arg(claims)
            .arg("EX")
            .arg(ttl_seconds)
            .arg("NX")
            .query_async::<Option<String>>(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?
            .ok_or(RealtimeError::Unavailable)?;
        Ok(token)
    }

    async fn authenticate_v2_scoped(
        &self,
        token: &str,
    ) -> Result<RealtimeV2TokenClaims, RealtimeError> {
        if token.is_empty() || token.len() > 128 {
            return Err(RealtimeError::Forbidden);
        }
        let claims: Option<String> = redis::cmd("GET")
            .arg(self.v2_scoped_token_key(token))
            .query_async(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
        claims
            .and_then(|value| serde_json::from_str::<RealtimeV2TokenClaims>(&value).ok())
            .ok_or(RealtimeError::Forbidden)
    }
    pub async fn authenticate_scoped(
        &self,
        token: &str,
        channel: &str,
        publish: bool,
    ) -> Result<Principal, RealtimeError> {
        if token.is_empty() || token.len() > 128 {
            return Err(RealtimeError::Forbidden);
        }
        let claims: Option<String> = redis::cmd("GET")
            .arg(self.scoped_token_key(token))
            .query_async(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
        let claims = claims
            .and_then(|value| serde_json::from_str::<ScopedTokenClaims>(&value).ok())
            .ok_or(RealtimeError::Forbidden)?;
        if claims.channel != channel || !claims.publish_only || !publish {
            return Err(RealtimeError::Forbidden);
        }
        Ok(Principal::Application {
            workspace_id: claims.workspace_id,
            app_id: claims.app_id,
            key_id: Uuid::nil(),
        })
    }
    fn scoped_token_key(&self, token: &str) -> String {
        format!("{}:v1:realtime-token:{token}", self.namespace)
    }
    fn stream_token_key(&self, token: &str) -> String {
        format!("{}:v1:stream-token:{token}", self.namespace)
    }
    fn v2_scoped_token_key(&self, token: &str) -> String {
        format!("{}:v2:realtime-token:{token}", self.namespace)
    }
    fn v2_subject(
        &self,
        workspace: Uuid,
        app: Uuid,
        channel: &str,
    ) -> Result<String, RealtimeError> {
        validate_channel(channel)?;
        Ok(format!(
            "{}.v2.workspaces.{workspace}.apps.{app}.realtime.{}",
            self.namespace,
            URL_SAFE_NO_PAD.encode(channel)
        ))
    }
    fn v2_history_key(&self, workspace: Uuid, app: Uuid, channel: &str) -> String {
        format!(
            "{}:v2:realtime-history:{workspace}:{app}:{}",
            self.namespace,
            URL_SAFE_NO_PAD.encode(channel)
        )
    }
    fn v2_history_sequence_key(&self, workspace: Uuid, app: Uuid, channel: &str) -> String {
        format!(
            "{}:v2:realtime-history-seq:{workspace}:{app}:{}",
            self.namespace,
            URL_SAFE_NO_PAD.encode(channel)
        )
    }
    fn v2_presence_key(&self, workspace: Uuid, app: Uuid, channel: &str) -> String {
        format!(
            "{}:v2:realtime-presence:{workspace}:{app}:{}",
            self.namespace,
            URL_SAFE_NO_PAD.encode(channel)
        )
    }
    fn v2_actions_key(&self, app: Uuid, channel: &str, message_id: &str) -> String {
        // Keep the Redis key compatible with the Go realtime action store.
        format!("rh:{{actions:{app}:{channel}:{message_id}}}:state")
    }
    fn subject(
        &self,
        principal: &Principal,
        channel: &str,
        publish: bool,
    ) -> Result<String, RealtimeError> {
        let (workspace, app) = scope(principal, publish)?;
        validate_channel(channel)?;
        Ok(format!(
            "{}.v1.workspaces.{workspace}.apps.{app}.realtime.{}",
            self.namespace,
            URL_SAFE_NO_PAD.encode(channel)
        ))
    }
    fn presence_key(&self, subject: &str) -> String {
        format!("{}:v1:realtime:{subject}", self.namespace)
    }
    fn connection_index_key(&self, app: Uuid) -> String {
        format!("{}:v1:realtime-connections:{app}", self.namespace)
    }
    fn connection_record_key(&self, workspace: Uuid, app: Uuid, session: Uuid) -> String {
        format!(
            "{}:v1:realtime-connection:{workspace}:{app}:{session}",
            self.namespace
        )
    }
    fn disconnect_key(&self, workspace: Uuid, app: Uuid, session: Uuid) -> String {
        format!(
            "{}:v1:realtime-disconnect:{workspace}:{app}:{session}",
            self.namespace
        )
    }
    fn history_key(&self, subject: &str) -> String {
        format!("{}:v1:realtime-history:{subject}", self.namespace)
    }
    fn history_sequence_key(&self, subject: &str) -> String {
        format!("{}:v1:realtime-history-seq:{subject}", self.namespace)
    }
    fn connection_id(session: Uuid) -> String {
        format!("conn_{}", session.simple())
    }
    async fn register(&self, key: &str, member: &str) -> Result<(), RealtimeError> {
        bounded(redis::cmd("EVAL")
            .arg("local t=redis.call('TIME'); redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',t[1]); redis.call('ZADD',KEYS[1],t[1]+ARGV[2],ARGV[1]); redis.call('EXPIRE',KEYS[1],ARGV[2]); return 1")
            .arg(1).arg(key).arg(member).arg(PRESENCE_EXPIRY_SECONDS)
            .query_async::<i64>(&mut self.redis.clone())).await?;
        Ok(())
    }
    async fn remove(&self, key: &str, member: &str) {
        let _ = bounded(
            redis::cmd("ZREM")
                .arg(key)
                .arg(member)
                .query_async::<i64>(&mut self.redis.clone()),
        )
        .await;
    }
    async fn register_connection(
        &self,
        principal: &Principal,
        session: Uuid,
        channel: &str,
    ) -> Result<(), RealtimeError> {
        self.register_connection_with_protocol(principal, session, channel, "relayhub.realtime.v1")
            .await
    }
    async fn register_connection_with_protocol(
        &self,
        principal: &Principal,
        session: Uuid,
        channel: &str,
        protocol: &str,
    ) -> Result<(), RealtimeError> {
        let (workspace, app) = scope(principal, false)?;
        let record_key = self.connection_record_key(workspace, app, session);
        let index_key = self.connection_index_key(app);
        let existing: Option<String> = redis::cmd("GET")
            .arg(&record_key)
            .query_async(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
        let mut record = existing
            .and_then(|value| serde_json::from_str::<Value>(&value).ok())
            .unwrap_or_else(|| {
                json!({
                    "app_id": app,
                    "client_id": "",
                    "connection_id": Self::connection_id(session),
                    "instance_id": self.gateway,
                    "protocol": protocol,
                    "channels": [],
                })
            });
        let channels = record
            .get_mut("channels")
            .and_then(Value::as_array_mut)
            .ok_or(RealtimeError::Unavailable)?;
        if !channels.iter().any(|value| value.as_str() == Some(channel)) {
            channels.push(Value::String(channel.to_owned()));
        }
        let now_seconds = redis::cmd("TIME")
            .query_async::<Vec<String>>(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?
            .first()
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or(RealtimeError::Unavailable)?;
        record["connected_at"] = record
            .get("connected_at")
            .cloned()
            .unwrap_or_else(|| json!(now_seconds.saturating_mul(1000)));
        record["last_seen_at"] = json!(now_seconds.saturating_mul(1000));
        let value = serde_json::to_string(&record).map_err(|_| RealtimeError::Unavailable)?;
        redis::cmd("SET")
            .arg(&record_key)
            .arg(value)
            .arg("EX")
            .arg(PRESENCE_EXPIRY_SECONDS)
            .query_async::<()>(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
        redis::cmd("ZADD")
            .arg(index_key)
            .arg(now_seconds)
            .arg(record_key)
            .query_async::<()>(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
        Ok(())
    }
    async fn heartbeat_connection(
        &self,
        principal: &Principal,
        session: Uuid,
    ) -> Result<bool, RealtimeError> {
        let (workspace, app) = scope(principal, false)?;
        let record_key = self.connection_record_key(workspace, app, session);
        let disconnect_key = self.disconnect_key(workspace, app, session);
        let requested: bool = redis::cmd("EXISTS")
            .arg(&disconnect_key)
            .query_async(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
        if requested {
            let _: () = redis::cmd("DEL")
                .arg(disconnect_key)
                .query_async(&mut self.redis.clone())
                .await
                .map_err(|_| RealtimeError::Unavailable)?;
            return Ok(true);
        }
        let existing: Option<String> = redis::cmd("GET")
            .arg(&record_key)
            .query_async(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
        let Some(mut record) =
            existing.and_then(|value| serde_json::from_str::<Value>(&value).ok())
        else {
            return Ok(true);
        };
        let now_seconds = redis::cmd("TIME")
            .query_async::<Vec<String>>(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?
            .first()
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or(RealtimeError::Unavailable)?;
        record["last_seen_at"] = json!(now_seconds.saturating_mul(1000));
        let value = serde_json::to_string(&record).map_err(|_| RealtimeError::Unavailable)?;
        redis::cmd("SET")
            .arg(record_key)
            .arg(value)
            .arg("EX")
            .arg(PRESENCE_EXPIRY_SECONDS)
            .query_async::<()>(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
        Ok(false)
    }
    async fn remove_connection(&self, principal: &Principal, session: Uuid) {
        if let Ok((workspace, app)) = scope(principal, false) {
            let record_key = self.connection_record_key(workspace, app, session);
            let _ = redis::cmd("DEL")
                .arg(&record_key)
                .query_async::<()>(&mut self.redis.clone())
                .await;
            let _ = redis::cmd("ZREM")
                .arg(self.connection_index_key(app))
                .arg(record_key)
                .query_async::<()>(&mut self.redis.clone())
                .await;
        }
    }
    pub async fn list_connections(
        &self,
        app: Uuid,
        limit: u32,
    ) -> Result<Vec<Value>, RealtimeError> {
        let keys: Vec<String> = redis::cmd("ZRANGE")
            .arg(self.connection_index_key(app))
            .arg(0)
            .arg(i64::from(limit.min(500).saturating_sub(1)))
            .query_async(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
        let mut result = Vec::with_capacity(keys.len());
        for key in keys {
            let value: Option<String> = redis::cmd("GET")
                .arg(key)
                .query_async(&mut self.redis.clone())
                .await
                .map_err(|_| RealtimeError::Unavailable)?;
            if let Some(value) = value.and_then(|raw| serde_json::from_str::<Value>(&raw).ok()) {
                result.push(value);
            }
        }
        Ok(result)
    }
    pub async fn disconnect_connection(
        &self,
        app: Uuid,
        connection_id: &str,
    ) -> Result<(), RealtimeError> {
        let session = connection_id
            .strip_prefix("conn_")
            .and_then(|value| Uuid::parse_str(value).ok())
            .ok_or(RealtimeError::InvalidInput)?;
        let keys: Vec<String> = redis::cmd("ZRANGE")
            .arg(self.connection_index_key(app))
            .arg(0)
            .arg(-1)
            .query_async(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
        for key in keys {
            let value: Option<String> = redis::cmd("GET")
                .arg(&key)
                .query_async(&mut self.redis.clone())
                .await
                .map_err(|_| RealtimeError::Unavailable)?;
            if value
                .as_deref()
                .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
                .and_then(|record| {
                    record
                        .get("connection_id")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .as_deref()
                == Some(connection_id)
            {
                let parts: Vec<&str> = key.rsplitn(3, ':').collect();
                let workspace = parts
                    .get(2)
                    .and_then(|value| Uuid::parse_str(value).ok())
                    .ok_or(RealtimeError::Unavailable)?;
                let _: () = redis::cmd("SETEX")
                    .arg(self.disconnect_key(workspace, app, session))
                    .arg(60)
                    .arg("1")
                    .query_async(&mut self.redis.clone())
                    .await
                    .map_err(|_| RealtimeError::Unavailable)?;
                return Ok(());
            }
        }
        Err(RealtimeError::Unavailable)
    }
    pub async fn history(
        &self,
        principal: &Principal,
        channel: &str,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<Value, RealtimeError> {
        let subject = self.subject(principal, channel, false)?;
        let start = cursor
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(0);
        let raw: Vec<String> = redis::cmd("LRANGE")
            .arg(self.history_key(&subject))
            .arg(0)
            .arg(-1)
            .query_async(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
        let items: Vec<Value> = raw
            .into_iter()
            .filter_map(|value| serde_json::from_str::<Value>(&value).ok())
            .filter(|value| {
                value
                    .get("seq")
                    .and_then(Value::as_i64)
                    .is_some_and(|seq| seq > start)
            })
            .map(|mut value| {
                if let Some(object) = value.as_object_mut() {
                    object.remove("seq");
                }
                value
            })
            .take(limit.clamp(1, 100) as usize)
            .collect();
        let next_cursor = items.last().and_then(|value| value.get("cursor")).cloned();
        Ok(
            json!({"type":"history.result","channel":channel,"items":items,"next_cursor":next_cursor,"continuity_cursor":next_cursor}),
        )
    }
    pub async fn publish_realtime(
        &self,
        principal: &Principal,
        channel: &str,
        payload: &[u8],
    ) -> Result<PublishReceipt, RealtimeError> {
        let subject = self.subject(principal, channel, true)?;
        if payload.len() > MAX_BODY_BYTES {
            return Err(RealtimeError::PayloadTooLarge);
        }
        if self.nats.connection_state() != async_nats::connection::State::Connected {
            return Err(RealtimeError::Unavailable);
        }
        let observed_recipients = bounded(redis::cmd("EVAL")
            .arg("local t=redis.call('TIME'); redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',t[1]); return redis.call('ZCARD',KEYS[1])")
            .arg(1).arg(self.presence_key(&subject)).query_async::<u64>(&mut self.redis.clone())).await?;
        let sequence: i64 = redis::cmd("INCR")
            .arg(self.history_sequence_key(&subject))
            .query_async(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
        let data = serde_json::from_slice::<Value>(payload)
            .unwrap_or_else(|_| Value::String(URL_SAFE_NO_PAD.encode(payload)));
        let history = json!({
            "type": "channel.message",
            "message_id": format!("msg_{}", Uuid::new_v4().simple()),
            "channel": channel,
            "published_at": sequence,
            "cursor": sequence.to_string(),
            "data": data,
            "seq": sequence,
        });
        redis::cmd("RPUSH")
            .arg(self.history_key(&subject))
            .arg(serde_json::to_string(&history).map_err(|_| RealtimeError::Unavailable)?)
            .query_async::<()>(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
        redis::cmd("LTRIM")
            .arg(self.history_key(&subject))
            .arg(-1000)
            .arg(-1)
            .query_async::<()>(&mut self.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
        // Raw binary keeps the frozen 1 MiB limit within NATS' default max_payload.
        bounded(self.nats.publish(subject, payload.to_vec().into())).await?;
        self.barrier().await?;
        Ok(PublishReceipt {
            status: 202,
            observed_recipients,
        })
    }
    /// Only the frozen realtime frame is accepted here; ACK/response frames have no meaning.
    pub async fn publish_frame(
        &self,
        principal: &Principal,
        bytes: &[u8],
    ) -> Result<PublishReceipt, RealtimeError> {
        if bytes.len() > 2 * MAX_BODY_BYTES {
            return Err(RealtimeError::PayloadTooLarge);
        }
        let frame = parse_frame(bytes).map_err(|error| match error {
            ProtocolError::BodyTooLarge => RealtimeError::PayloadTooLarge,
            _ => RealtimeError::InvalidInput,
        })?;
        let Frame::Realtime { channel, body, .. } = frame else {
            return Err(RealtimeError::InvalidInput);
        };
        self.publish_realtime(principal, &channel, &body).await
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SocketTokenInput {
    #[serde(default)]
    scopes: Vec<String>,
    ttl_seconds: i64,
    #[serde(default)]
    protocol: String,
    #[serde(default)]
    client_id: String,
    #[serde(default)]
    channels: Value,
}

fn socket_channels(value: &Value) -> Option<Vec<String>> {
    match value {
        Value::Object(entries) => Some(entries.keys().cloned().collect()),
        Value::Array(entries) => entries
            .iter()
            .map(|value| value.as_str().map(str::to_owned))
            .collect(),
        Value::Null => None,
        _ => None,
    }
}

async fn socket_token(
    State((realtime, identity)): axum::extract::State<(Realtime, crate::identity::Identity)>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let principal = match identity
        .authenticate_public_request(&headers, "POST", "/api/v1/socket/token", &body)
        .await
    {
        Ok(principal @ Principal::Application { .. }) => principal,
        Ok(_) => return crate::identity::AuthError::Forbidden.into_response(),
        Err(error) => return error.into_response(),
    };
    let input: SocketTokenInput = match serde_json::from_slice::<SocketTokenInput>(&body) {
        Ok(input) if (1..=900).contains(&input.ttl_seconds) => input,
        _ => return realtime_bad_input(),
    };
    let (workspace_id, app_id) = match principal {
        Principal::Application {
            workspace_id,
            app_id,
            ..
        } => (workspace_id, app_id),
        _ => unreachable!(),
    };
    let token = if input.protocol == "realtime.v2" {
        let channels = match socket_channels(&input.channels) {
            Some(channels) if !channels.is_empty() => channels,
            _ => return realtime_bad_input(),
        };
        match realtime
            .issue_v2_scoped_token_with_ttl(
                workspace_id,
                app_id,
                &input.client_id,
                &channels,
                input.ttl_seconds,
            )
            .await
        {
            Ok(token) => token,
            Err(error) => return error.into_response(),
        }
    } else if input.protocol.is_empty() {
        if input.scopes.is_empty() {
            return realtime_bad_input();
        }
        match realtime
            .issue_stream_token(workspace_id, app_id, &input.scopes, input.ttl_seconds)
            .await
        {
            Ok(token) => token,
            Err(error) => return error.into_response(),
        }
    } else {
        return realtime_bad_input();
    };
    let expires_at: Result<String, sqlx::Error> = sqlx::query_scalar(
        "SELECT to_char((clock_timestamp()+$1*interval '1 second') AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')",
    )
    .bind(input.ttl_seconds)
    .fetch_one(&identity.pool())
    .await;
    match expires_at {
        Ok(expires_at) => (
            StatusCode::CREATED,
            Json(json!({"token":token,"expires_at":expires_at})),
        )
            .into_response(),
        Err(_) => realtime_disabled("realtime_unavailable"),
    }
}

async fn legacy_channel_publish(
    State((realtime, identity)): axum::extract::State<(Realtime, crate::identity::Identity)>,
    headers: axum::http::HeaderMap,
    Path(channel): Path<String>,
    body: Bytes,
) -> Response {
    let target = format!("/api/v1/realtime/channels/{channel}/publish");
    let principal = match identity
        .authenticate_public_request(&headers, "POST", &target, &body)
        .await
    {
        Ok(principal) => principal,
        Err(error) => return error.into_response(),
    };
    let input: Value = match serde_json::from_slice(&body) {
        Ok(Value::Object(mut object)) => object.remove("data").unwrap_or(Value::Null),
        _ => return realtime_bad_input(),
    };
    if !input.is_object() {
        return realtime_bad_input();
    }
    let payload = match serde_json::to_vec(&input) {
        Ok(payload) => payload,
        Err(_) => return realtime_bad_input(),
    };
    match realtime
        .publish_realtime(&principal, &channel, &payload)
        .await
    {
        Ok(_) => StatusCode::ACCEPTED.into_response(),
        Err(error) => error.into_response(),
    }
}
impl Connection {
    fn authorize(&self, principal: &Principal) -> Result<(), RealtimeError> {
        if *principal != self.principal {
            return Err(RealtimeError::Forbidden);
        }
        scope(principal, false)?;
        Ok(())
    }
    pub async fn subscribe(
        &mut self,
        principal: &Principal,
        channel: &str,
    ) -> Result<(), RealtimeError> {
        self.authorize(principal)?;
        let subject = self.realtime.subject(principal, channel, false)?;
        if self.reason.borrow().is_some() {
            return Err(RealtimeError::Unavailable);
        }
        if self.subscriptions.contains_key(channel) {
            return Ok(());
        }
        let mut subscriber = bounded(self.realtime.nats.subscribe(subject.clone())).await?;
        self.realtime.barrier().await?;
        let key = self.realtime.presence_key(&subject);
        let member = format!("{}:{}", self.realtime.gateway, self.session);
        self.realtime.register(&key, &member).await?;
        self.realtime
            .register_connection(&self.principal, self.session, channel)
            .await?;
        let (stop, mut stopping) = watch::channel(false);
        let id = Uuid::new_v4();
        let sender = self.sender.clone();
        let reason = self.reason.clone();
        let mut closed = reason.subscribe();
        let realtime = self.realtime.clone();
        let connection_principal = self.principal.clone();
        let connection_session = self.session;
        let channel_name = channel.to_owned();
        let task = tokio::spawn(async move {
            let mut heartbeat =
                tokio::time::interval(Duration::from_secs(PRESENCE_HEARTBEAT_SECONDS));
            heartbeat.tick().await;
            loop {
                tokio::select! {
                    biased;
                    _ = stopping.changed() => break,
                    _ = closed.changed() => break,
                    _ = sender.closed() => break,
                    _ = heartbeat.tick() => {
                        if realtime.nats.connection_state() != async_nats::connection::State::Connected || realtime.register(&key, &member).await.is_err() || matches!(realtime.heartbeat_connection(&connection_principal, connection_session).await, Ok(true) | Err(_)) {
                            reason.send_if_modified(|value| { if value.is_none() { *value=Some(DisconnectReason::TransportUnavailable); true } else { false } });
                            break;
                        }
                    },
                    message = subscriber.next() => {
                        let Some(message) = message else {
                            reason.send_replace(Some(DisconnectReason::TransportUnavailable));
                            break;
                        };
                        if message.payload.len() > MAX_BODY_BYTES { continue; }
                        let frame = Frame::Realtime { version: VERSION, request_id: Uuid::new_v4(), channel: channel_name.clone(), body: message.payload.to_vec() };
                        match sender.try_send(Outbound { subscription: id, frame }) {
                            Ok(()) => {},
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                reason.send_if_modified(|value| { if value.is_none() { *value=Some(DisconnectReason::SlowReader); true } else { false } });
                                break;
                            },
                            Err(mpsc::error::TrySendError::Closed(_)) => break,
                        }
                    }
                }
            }
            let _ = bounded(subscriber.unsubscribe()).await;
            realtime.remove(&key, &member).await;
        });
        self.subscriptions
            .insert(channel.to_owned(), Subscription { id, stop, task });
        Ok(())
    }
    pub async fn unsubscribe(
        &mut self,
        principal: &Principal,
        channel: &str,
    ) -> Result<(), RealtimeError> {
        self.authorize(principal)?;
        validate_channel(channel)?;
        if let Some(subscription) = self.subscriptions.remove(channel) {
            subscription.stop.send_replace(true);
            let _ = subscription.task.await;
        }
        Ok(())
    }
    pub fn buffered_len(&self) -> usize {
        self.receiver.len()
    }
    pub async fn closed(&mut self) -> DisconnectReason {
        let mut reason = self.reason.subscribe();
        loop {
            if let Some(value) = *reason.borrow_and_update() {
                return value;
            }
            if reason.changed().await.is_err() {
                return DisconnectReason::Closed;
            }
        }
    }
    pub async fn recv(&mut self) -> Result<Option<Frame>, DisconnectReason> {
        let mut reason = self.reason.subscribe();
        loop {
            if let Some(value) = *reason.borrow_and_update() {
                return Err(value);
            }
            tokio::select! {
                biased;
                _ = reason.changed() => {},
                item = self.receiver.recv() => {
                    let Some(item) = item else { return Ok(None); };
                    let Frame::Realtime { channel, .. } = &item.frame else { unreachable!() };
                    if self.subscriptions.get(channel).is_some_and(|s| s.id == item.subscription) { return Ok(Some(item.frame)); }
                }
            }
        }
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.reason.send_replace(Some(DisconnectReason::Closed));
    }
}

fn disconnect_close(reason: DisconnectReason) -> (u16, &'static str) {
    match reason {
        DisconnectReason::SlowReader => (1008, "slow_reader"),
        DisconnectReason::TransportUnavailable => (1011, "realtime_unavailable"),
        DisconnectReason::Closed => (1000, "closed"),
    }
}

async fn authenticated(
    identity: &crate::identity::Identity,
    realtime: &Realtime,
    headers: &axum::http::HeaderMap,
    channel: &str,
    publish: bool,
) -> Result<(String, Principal), Box<Response>> {
    use crate::identity::AuthError;
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(|| Box::new(AuthError::Unauthorized.into_response()))?
        .to_owned();
    let principal = match tokio::time::timeout(IO_TIMEOUT, identity.authenticate(&token)).await {
        Ok(Ok(principal)) => principal,
        Ok(Err(AuthError::Unauthorized)) => realtime
            .authenticate_scoped(&token, channel, publish)
            .await
            .map_err(|error| Box::new(error.into_response()))?,
        Ok(Err(error)) => return Err(Box::new(error.into_response())),
        Err(_) => return Err(Box::new(AuthError::Unauthorized.into_response())),
    };
    Ok((token, principal))
}
async fn authenticated_token(
    identity: &crate::identity::Identity,
    realtime: &Realtime,
    token: &str,
    channel: &str,
    publish: bool,
) -> Result<Principal, RealtimeError> {
    match identity.authenticate(token).await {
        Ok(principal) => Ok(principal),
        Err(crate::identity::AuthError::Unauthorized) => {
            realtime.authenticate_scoped(token, channel, publish).await
        }
        Err(_) => Err(RealtimeError::Forbidden),
    }
}
async fn publish_http(
    axum::extract::State((realtime, identity)): axum::extract::State<(
        Realtime,
        crate::identity::Identity,
    )>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    if !headers.contains_key(axum::http::header::AUTHORIZATION) {
        return crate::identity::AuthError::Unauthorized.into_response();
    }
    let channel = match parse_frame(&body) {
        Ok(Frame::Realtime { channel, .. }) => channel,
        _ => return RealtimeError::InvalidInput.into_response(),
    };
    let principal = match authenticated(&identity, &realtime, &headers, &channel, true).await {
        Ok((_, principal)) => principal,
        Err(error) => return *error,
    };
    match realtime.publish_frame(&principal, &body).await {
        Ok(receipt) => (StatusCode::ACCEPTED, Json(receipt)).into_response(),
        Err(error) => error.into_response(),
    }
}
async fn upgrade(
    axum::extract::State((realtime, identity)): axum::extract::State<(
        Realtime,
        crate::identity::Identity,
    )>,
    axum::extract::Path(channel): axum::extract::Path<String>,
    headers: axum::http::HeaderMap,
    ws: axum::extract::WebSocketUpgrade,
) -> Response {
    let (token, principal) =
        match authenticated(&identity, &realtime, &headers, &channel, false).await {
            Ok(auth) => auth,
            Err(error) => return *error,
        };
    if !headers
        .get(axum::http::header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(',')
                .any(|s| s.trim() == crate::protocol::SUBPROTOCOL)
        })
    {
        return RealtimeError::InvalidInput.into_response();
    }
    let mut connection = match realtime.connect(&principal) {
        Ok(connection) => connection,
        Err(error) => return error.into_response(),
    };
    if let Err(error) = connection.subscribe(&principal, &channel).await {
        return error.into_response();
    }
    ws.protocols([crate::protocol::SUBPROTOCOL])
        .max_message_size(2 * MAX_BODY_BYTES)
        .on_upgrade(move |socket| {
            socket_loop(socket, connection, identity, token, principal, channel)
        })
}

#[derive(Debug, Deserialize)]
struct RealtimeV2Query {
    token: String,
}

fn v2_channel_allowed(claims: &RealtimeV2TokenClaims, channel: &str) -> bool {
    claims.channels.iter().any(|grant| {
        grant == channel
            || grant == "*"
            || grant
                .strip_suffix('*')
                .is_some_and(|prefix| !prefix.is_empty() && channel.starts_with(prefix))
    })
}

fn v2_visible(frame: &Value, connection_id: &str, client_id: &str) -> bool {
    if frame.get("type").and_then(Value::as_str) != Some("channel.message") {
        return true;
    }
    match frame
        .get("audience")
        .and_then(|value| value.get("type"))
        .and_then(Value::as_str)
        .unwrap_or("all")
    {
        "all" => true,
        "others" => {
            frame.get("publisher_connection_id").and_then(Value::as_str) != Some(connection_id)
        }
        "connection" => {
            frame
                .get("audience")
                .and_then(|value| value.get("connection_id"))
                .and_then(Value::as_str)
                == Some(connection_id)
        }
        "client" => {
            frame
                .get("audience")
                .and_then(|value| value.get("client_id"))
                .and_then(Value::as_str)
                == Some(client_id)
        }
        _ => false,
    }
}

async fn upgrade_v2(
    axum::extract::State((realtime, identity)): axum::extract::State<(
        Realtime,
        crate::identity::Identity,
    )>,
    axum::extract::Query(query): axum::extract::Query<RealtimeV2Query>,
    headers: axum::http::HeaderMap,
    ws: axum::extract::WebSocketUpgrade,
) -> Response {
    let wants_v2 = headers
        .get(axum::http::header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|candidate| candidate.trim() == V2_PROTOCOL)
        });
    if !wants_v2 {
        let principal = match identity.authenticate(&query.token).await {
            Ok(principal) => principal,
            Err(error) => return error.into_response(),
        };
        return crate::control_plane::legacy_function_socket_response(identity, principal, ws);
    }
    let claims = match realtime.authenticate_v2_scoped(&query.token).await {
        Ok(claims) => claims,
        Err(error) => return error.into_response(),
    };
    let connection_id = format!("conn_{}", Uuid::new_v4().simple());
    ws.protocols([V2_PROTOCOL])
        .max_message_size(2 * MAX_BODY_BYTES)
        .on_upgrade(move |socket| v2_socket_loop(socket, realtime, claims, connection_id))
        .into_response()
}

async fn send_v2(socket: &mut axum::extract::ws::WebSocket, value: &Value) -> Result<(), ()> {
    let text = serde_json::to_string(value).map_err(|_| ())?;
    tokio::time::timeout(
        IO_TIMEOUT,
        socket.send(axum::extract::ws::Message::Text(text.into())),
    )
    .await
    .map_err(|_| ())?
    .map_err(|_| ())
}

async fn v2_history(
    realtime: &Realtime,
    claims: &RealtimeV2TokenClaims,
    channel: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<Value, RealtimeError> {
    if !v2_channel_allowed(claims, channel) {
        return Err(RealtimeError::Forbidden);
    }
    validate_channel(channel)?;
    let start = cursor
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    let raw: Vec<String> = redis::cmd("LRANGE")
        .arg(realtime.v2_history_key(claims.workspace_id, claims.app_id, channel))
        .arg(0)
        .arg(-1)
        .query_async(&mut realtime.redis.clone())
        .await
        .map_err(|_| RealtimeError::Unavailable)?;
    let mut items = Vec::new();
    for value in raw {
        let mut frame =
            serde_json::from_str::<Value>(&value).map_err(|_| RealtimeError::Unavailable)?;
        let seq = frame.get("seq").and_then(Value::as_i64).unwrap_or_default();
        if seq <= start {
            continue;
        }
        if let Some(object) = frame.as_object_mut() {
            object.remove("seq");
        }
        items.push(frame);
        if items.len() >= limit.clamp(1, 100) {
            break;
        }
    }
    let next_cursor = items
        .last()
        .and_then(|value| value.get("cursor"))
        .cloned()
        .unwrap_or(Value::Null);
    Ok(json!({
        "type": "history.result",
        "channel": channel,
        "items": items,
        "next_cursor": next_cursor,
        "continuity_cursor": next_cursor,
    }))
}

#[derive(Debug, Deserialize, Default)]
struct V2HistoryQuery {
    cursor: Option<String>,
    limit: Option<usize>,
}

async fn v2_history_http(
    State((realtime, _identity)): axum::extract::State<(Realtime, crate::identity::Identity)>,
    headers: axum::http::HeaderMap,
    Path(channel): Path<String>,
    Query(query): Query<V2HistoryQuery>,
) -> Response {
    let Some(token) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
    else {
        return crate::identity::AuthError::Unauthorized.into_response();
    };
    let claims = match realtime.authenticate_v2_scoped(token).await {
        Ok(claims) => claims,
        Err(error) => return error.into_response(),
    };
    match v2_history(
        &realtime,
        &claims,
        &channel,
        query.cursor.as_deref(),
        query.limit.unwrap_or(50),
    )
    .await
    {
        Ok(value) => Json(value).into_response(),
        Err(error) => error.into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct StreamQuery {
    token: String,
}

#[derive(Debug, Clone)]
struct StreamLease {
    delivery_id: Uuid,
    attempt_token: Uuid,
    attempt: i32,
}

async fn legacy_stream_upgrade(
    State((realtime, _identity)): axum::extract::State<(Realtime, crate::identity::Identity)>,
    headers: axum::http::HeaderMap,
    Query(query): Query<StreamQuery>,
    ws: axum::extract::WebSocketUpgrade,
) -> Result<Response, AuthError> {
    let claims = realtime
        .authenticate_stream_token(&query.token)
        .await
        .map_err(|_| AuthError::Unauthorized)?;
    if !claims.scopes.iter().any(|scope| scope == "stream:connect") {
        return Err(AuthError::Forbidden);
    }
    if !headers
        .get(axum::http::header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|item| item.trim() == STREAM_SUBPROTOCOL)
        })
    {
        return Err(AuthError::InvalidInput);
    }
    let connection_id = Uuid::new_v4();
    Ok(ws
        .protocols([STREAM_SUBPROTOCOL])
        .max_message_size(2 * MAX_BODY_BYTES)
        .on_upgrade(move |socket| {
            legacy_stream_socket(socket, realtime, _identity, claims, connection_id)
        })
        .into_response())
}

async fn legacy_stream_claim(
    realtime: &Realtime,
    claims: &StreamTokenClaims,
    connection_id: Uuid,
) -> Result<Option<(String, StreamLease, Value)>, RealtimeError> {
    let Some(pool) = realtime.pool.as_ref() else {
        return Err(RealtimeError::Unavailable);
    };
    let mut tx = pool.begin().await.map_err(|_| RealtimeError::Unavailable)?;
    let row = sqlx::query("SELECT d.id,d.event_id,d.attempt_count,e.app_id,e.payload,e.legacy_target_app_ids,to_char(e.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at FROM deliveries d JOIN events e ON e.id=d.event_id WHERE d.target_app_id=$1 AND d.state IN ('pending','retry_wait') AND (d.next_attempt_at IS NULL OR d.next_attempt_at<=clock_timestamp()) AND e.expires_at>clock_timestamp() ORDER BY d.created_at,d.id FOR UPDATE OF d SKIP LOCKED LIMIT 1")
        .bind(claims.app_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|_| RealtimeError::Unavailable)?;
    let Some(row) = row else {
        tx.commit().await.map_err(|_| RealtimeError::Unavailable)?;
        return Ok(None);
    };
    let delivery_id: Uuid = row.get("id");
    let attempt = row.get::<i32, _>("attempt_count") + 1;
    let attempt_token = Uuid::new_v4();
    sqlx::query("UPDATE deliveries SET state='in_flight',attempt_count=$2,attempt_token=$3,agent_id=$4,attempt_started_at=clock_timestamp(),lease_expires_at=clock_timestamp()+interval '60 seconds',next_attempt_at=NULL WHERE id=$1")
        .bind(delivery_id)
        .bind(attempt)
        .bind(attempt_token)
        .bind(connection_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| RealtimeError::Unavailable)?;
    sqlx::query("INSERT INTO delivery_attempts(delivery_id,attempt,attempt_token,agent_id,started_at,lease_expires_at) VALUES($1,$2,$3,$4,clock_timestamp(),clock_timestamp()+interval '60 seconds')")
        .bind(delivery_id)
        .bind(attempt)
        .bind(attempt_token)
        .bind(connection_id)
        .execute(&mut *tx)
        .await
        .map_err(|_| RealtimeError::Unavailable)?;
    let payload: Value = serde_json::from_slice(&row.get::<Vec<u8>, _>("payload"))
        .map_err(|_| RealtimeError::Unavailable)?;
    let event_type = payload
        .get("event_type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let data = payload
        .get("body")
        .and_then(Value::as_str)
        .and_then(|value| {
            URL_SAFE_NO_PAD
                .decode(value)
                .ok()
                .or_else(|| STANDARD.decode(value).ok())
        })
        .and_then(|value| serde_json::from_slice::<Value>(&value).ok())
        .unwrap_or_else(|| json!({}));
    let targets = row
        .get::<Vec<String>, _>("legacy_target_app_ids")
        .into_iter()
        .map(Value::String)
        .collect::<Vec<_>>();
    let delivery_wire = sqlx::query_scalar::<_, Option<String>>(
        "SELECT legacy_public_job_id FROM deliveries WHERE id=$1",
    )
    .bind(delivery_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|_| RealtimeError::Unavailable)?
    .unwrap_or_else(|| format!("job_{}", delivery_id.simple()));
    tx.commit().await.map_err(|_| RealtimeError::Unavailable)?;
    let event = json!({
        "id": row.get::<Uuid,_>("event_id").to_string(),
        "type": event_type,
        "source_app_id": row.get::<Uuid,_>("app_id").to_string(),
        "target_app_ids": targets,
        "data": data,
        "created_at": row.get::<String,_>("created_at"),
    });
    let frame = json!({"type":"event.delivery","delivery_id":delivery_wire,"attempt":attempt,"event":event});
    Ok(Some((
        delivery_wire,
        StreamLease {
            delivery_id,
            attempt_token,
            attempt,
        },
        frame,
    )))
}

async fn legacy_stream_settle(
    realtime: &Realtime,
    claims: &StreamTokenClaims,
    connection_id: Uuid,
    lease: &StreamLease,
    disposition: &str,
    delay_ms: u64,
) -> Result<bool, RealtimeError> {
    let Some(pool) = realtime.pool.as_ref() else {
        return Err(RealtimeError::Unavailable);
    };
    let mut tx = pool.begin().await.map_err(|_| RealtimeError::Unavailable)?;
    let row = sqlx::query("SELECT state,attempt_count FROM deliveries WHERE id=$1 AND target_app_id=$2 AND agent_id=$3 AND attempt_token=$4 FOR UPDATE")
        .bind(lease.delivery_id).bind(claims.app_id).bind(connection_id).bind(lease.attempt_token)
        .fetch_optional(&mut *tx).await.map_err(|_| RealtimeError::Unavailable)?;
    if row.as_ref().is_none_or(|row| {
        row.get::<String, _>("state") != "in_flight"
            || row.get::<i32, _>("attempt_count") != lease.attempt
    }) {
        return Ok(false);
    }
    if disposition == "progress" {
        sqlx::query("UPDATE deliveries SET lease_expires_at=LEAST(clock_timestamp()+interval '60 seconds',attempt_started_at+interval '300 seconds') WHERE id=$1")
            .bind(lease.delivery_id).execute(&mut *tx).await.map_err(|_| RealtimeError::Unavailable)?;
    } else {
        let (state, outcome) = if disposition == "ack" {
            ("succeeded", "success")
        } else {
            ("retry_wait", "retryable_failure")
        };
        sqlx::query("UPDATE deliveries SET state=$2,terminal_at=CASE WHEN $2='succeeded' THEN clock_timestamp() ELSE NULL END,attempt_token=NULL,agent_id=NULL,attempt_started_at=NULL,lease_expires_at=NULL,next_attempt_at=CASE WHEN $2='retry_wait' THEN clock_timestamp()+$3*interval '1 millisecond' ELSE NULL END WHERE id=$1")
            .bind(lease.delivery_id).bind(state).bind(delay_ms as i64).execute(&mut *tx).await.map_err(|_| RealtimeError::Unavailable)?;
        sqlx::query("UPDATE delivery_attempts SET finished_at=clock_timestamp(),outcome=$2 WHERE delivery_id=$1 AND attempt=$3 AND attempt_token=$4 AND finished_at IS NULL")
            .bind(lease.delivery_id).bind(outcome).bind(lease.attempt).bind(lease.attempt_token).execute(&mut *tx).await.map_err(|_| RealtimeError::Unavailable)?;
    }
    tx.commit().await.map_err(|_| RealtimeError::Unavailable)?;
    Ok(true)
}

async fn legacy_stream_claim_function(
    identity: &crate::identity::Identity,
    app_id: Uuid,
    connection_id: &str,
) -> Result<Option<(String, Value)>, sqlx::Error> {
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
    let wire_id = format!("inv_{}", invocation_id.simple());
    Ok(Some((
        wire_id,
        json!({
            "type": "function.invoke",
            "invocation_id": format!("inv_{}", invocation_id.simple()),
            "function": row.get::<String, _>("name"),
            "input": input,
            "deadline": row.get::<String, _>("deadline"),
        }),
    )))
}

async fn legacy_stream_complete_function(
    identity: &crate::identity::Identity,
    app_id: Uuid,
    connection_id: &str,
    invocation_wire_id: &str,
    frame: &Value,
) -> Result<bool, sqlx::Error> {
    if frame.get("type").and_then(Value::as_str) != Some("function.result") {
        return Ok(false);
    }
    let invocation_id = frame
        .get("invocation_id")
        .and_then(Value::as_str)
        .and_then(|value| value.strip_prefix("inv_"))
        .and_then(|value| Uuid::parse_str(value).ok())
        .or_else(|| {
            Uuid::parse_str(
                invocation_wire_id
                    .strip_prefix("inv_")
                    .unwrap_or(invocation_wire_id),
            )
            .ok()
        });
    let Some(invocation_id) = invocation_id else {
        return Ok(false);
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
    tx.commit().await?;
    Ok(changed == 1)
}

async fn legacy_stream_socket(
    mut socket: axum::extract::ws::WebSocket,
    realtime: Realtime,
    identity: crate::identity::Identity,
    claims: StreamTokenClaims,
    connection_id: Uuid,
) {
    let ready = json!({"type":"ready","protocol_version":1,"app_id":claims.app_id,"connection_id":format!("conn_{}",connection_id.simple()),"heartbeat_interval_ms":20000,"max_in_flight_limit":16});
    if socket
        .send(axum::extract::ws::Message::Text(ready.to_string().into()))
        .await
        .is_err()
    {
        return;
    }
    let Some(Ok(axum::extract::ws::Message::Text(first))) = socket.recv().await else {
        return;
    };
    let first = serde_json::from_str::<Value>(&first).unwrap_or_else(|_| json!({}));
    if first.get("type").and_then(Value::as_str) != Some("consumer.start") {
        let _ = socket.send(axum::extract::ws::Message::Text(json!({"type":"error","code":"invalid_frame","message":"consumer.start required","retryable":false}).to_string().into())).await;
        return;
    }
    let max_in_flight = first
        .get("max_in_flight")
        .and_then(Value::as_u64)
        .unwrap_or(16)
        .clamp(1, 16) as usize;
    if socket
        .send(axum::extract::ws::Message::Text(
            json!({"type":"consumer.started","consumer":"default","max_in_flight":max_in_flight})
                .to_string()
                .into(),
        ))
        .await
        .is_err()
    {
        return;
    }
    let mut assigned: std::collections::HashMap<String, StreamLease> =
        std::collections::HashMap::new();
    let mut assigned_functions: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    let mut ticker = tokio::time::interval(Duration::from_millis(100));
    loop {
        tokio::select! {
            _ = ticker.tick() => {
                while assigned.len() < max_in_flight {
                    match legacy_stream_claim(&realtime, &claims, connection_id).await {
                        Ok(Some((id, lease, frame))) => {
                            assigned.insert(id, lease);
                            if socket.send(axum::extract::ws::Message::Text(frame.to_string().into())).await.is_err() { return; }
                        }
                        Ok(None) => break,
                        Err(_) => return,
                    }
                }
                if assigned_functions.len() < max_in_flight {
                    match legacy_stream_claim_function(&identity, claims.app_id, &format!("conn_{}", connection_id.simple())).await {
                        Ok(Some((invocation_id, frame))) => {
                            assigned_functions.insert(invocation_id);
                            if socket.send(axum::extract::ws::Message::Text(frame.to_string().into())).await.is_err() { return; }
                        }
                        Ok(None) => {}
                        Err(_) => return,
                    }
                }
            }
            message = socket.recv() => {
                let Some(Ok(message)) = message else { return; };
                let raw = match message {
                    axum::extract::ws::Message::Text(value) => value.to_string(),
                    axum::extract::ws::Message::Binary(value) => String::from_utf8_lossy(&value).into_owned(),
                    axum::extract::ws::Message::Ping(value) => { let _ = socket.send(axum::extract::ws::Message::Pong(value)).await; continue; },
                    axum::extract::ws::Message::Close(_) => return,
                    axum::extract::ws::Message::Pong(_) => continue,
                };
                let frame = serde_json::from_str::<Value>(&raw).unwrap_or_else(|_| json!({}));
                let kind = frame.get("type").and_then(Value::as_str).unwrap_or("");
                if kind == "ping" {
                    let _ = socket.send(axum::extract::ws::Message::Text(json!({"type":"pong"}).to_string().into())).await;
                    continue;
                }
                if kind == "function.result" {
                    let invocation_id = frame.get("invocation_id").and_then(Value::as_str).unwrap_or("").to_owned();
                    if !assigned_functions.contains(&invocation_id) {
                        let _ = socket.send(axum::extract::ws::Message::Text(json!({"type":"error","code":"function_not_assigned","message":"Function invocation is not assigned to this connection.","retryable":false}).to_string().into())).await;
                        continue;
                    }
                    match legacy_stream_complete_function(&identity, claims.app_id, &format!("conn_{}", connection_id.simple()), &invocation_id, &frame).await {
                        Ok(true) => { assigned_functions.remove(&invocation_id); }
                        Ok(false) => {
                            let _ = socket.send(axum::extract::ws::Message::Text(json!({"type":"error","code":"function_not_assigned","message":"Function invocation is not assigned to this connection.","retryable":false}).to_string().into())).await;
                        }
                        Err(_) => return,
                    }
                    continue;
                }
                let id = frame.get("delivery_id").and_then(Value::as_str).unwrap_or("").to_owned();
                let Some(lease) = assigned.get(&id).cloned() else { continue; };
                let result = match kind {
                    "delivery.ack" => legacy_stream_settle(&realtime, &claims, connection_id, &lease, "ack", 0).await.map(|ok| (ok,"acked")),
                    "delivery.progress" => legacy_stream_settle(&realtime, &claims, connection_id, &lease, "progress", 0).await.map(|ok| (ok,"progress")),
                    "delivery.nack" => legacy_stream_settle(&realtime, &claims, connection_id, &lease, "nack", frame.get("delay_ms").and_then(Value::as_u64).unwrap_or(0).min(300000)).await.map(|ok| (ok,"retrying")),
                    _ => continue,
                };
                match result {
                    Ok((true, state)) => { if state != "progress" { assigned.remove(&id); } let _ = socket.send(axum::extract::ws::Message::Text(json!({"type":"delivery.accepted","delivery_id":id,"state":state}).to_string().into())).await; },
                    Ok((false, _)) => { let _ = socket.send(axum::extract::ws::Message::Text(json!({"type":"error","code":"stale_delivery","message":"Delivery assignment is no longer current.","retryable":true}).to_string().into())).await; },
                    Err(_) => return,
                }
            }
        }
    }
}

async fn v2_publish(
    realtime: &Realtime,
    claims: &RealtimeV2TokenClaims,
    connection_id: &str,
    channel: &str,
    data: Option<Value>,
    encryption: Option<Value>,
    audience: Value,
) -> Result<Value, RealtimeError> {
    if !v2_channel_allowed(claims, channel) {
        return Err(RealtimeError::Forbidden);
    }
    validate_channel(channel)?;
    if let Some(encryption) = &encryption {
        let valid = channel.starts_with("private:")
            && data.is_none()
            && encryption.get("algorithm").and_then(Value::as_str) == Some("aes-256-gcm")
            && encryption
                .get("key_id")
                .and_then(Value::as_str)
                .is_some_and(valid_client_id)
            && encryption
                .get("nonce")
                .and_then(Value::as_str)
                .and_then(|value| URL_SAFE_NO_PAD.decode(value).ok())
                .is_some_and(|value| value.len() == 12)
            && encryption
                .get("ciphertext")
                .and_then(Value::as_str)
                .and_then(|value| URL_SAFE_NO_PAD.decode(value).ok())
                .is_some_and(|value| (16..=48 * 1024).contains(&value.len()));
        if !valid {
            return Err(RealtimeError::InvalidInput);
        }
    } else if data.as_ref().is_none_or(|value| {
        !value.is_object()
            || serde_json::to_vec(value)
                .map(|body| body.len() > MAX_BODY_BYTES)
                .unwrap_or(true)
    }) {
        return Err(RealtimeError::InvalidInput);
    }
    let audience_type = audience
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("all");
    if !matches!(audience_type, "all" | "others" | "connection" | "client") {
        return Err(RealtimeError::InvalidInput);
    }
    let message_id = format!("msg_{}", Uuid::new_v4().simple());
    let published_at = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis().to_string())
        .unwrap_or_else(|_| "0".to_owned());
    let mut frame = json!({
        "type": "channel.message",
        "protocol": crate::realtime::V2_PROTOCOL,
        "app_id": claims.app_id,
        "channel": channel,
        "publisher_app_id": claims.app_id,
        "publisher_client_id": claims.client_id,
        "publisher_connection_id": connection_id,
        "message_id": message_id,
        "published_at": published_at,
        "audience": audience,
    });
    if let Some(data) = data {
        frame["data"] = data;
    }
    if let Some(encryption) = encryption {
        frame["encryption"] = encryption;
    }
    redis::cmd("HSET")
        .arg(realtime.v2_actions_key(claims.app_id, channel, &message_id))
        .arg("_exists")
        .arg("1")
        .arg("_encrypted")
        .arg(frame.get("encryption").is_some())
        .query_async::<i64>(&mut realtime.redis.clone())
        .await
        .map_err(|_| RealtimeError::Unavailable)?;
    redis::cmd("PEXPIRE")
        .arg(realtime.v2_actions_key(claims.app_id, channel, &message_id))
        .arg(7 * 24 * 60 * 60 * 1000_i64)
        .query_async::<bool>(&mut realtime.redis.clone())
        .await
        .map_err(|_| RealtimeError::Unavailable)?;
    if audience_type == "all" {
        let sequence: i64 = redis::cmd("INCR")
            .arg(realtime.v2_history_sequence_key(claims.workspace_id, claims.app_id, channel))
            .query_async(&mut realtime.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
        let mut history = frame.clone();
        history["seq"] = json!(sequence);
        history["cursor"] = json!(sequence.to_string());
        redis::cmd("RPUSH")
            .arg(realtime.v2_history_key(claims.workspace_id, claims.app_id, channel))
            .arg(serde_json::to_string(&history).map_err(|_| RealtimeError::Unavailable)?)
            .query_async::<()>(&mut realtime.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
        redis::cmd("LTRIM")
            .arg(realtime.v2_history_key(claims.workspace_id, claims.app_id, channel))
            .arg(-1000)
            .arg(-1)
            .query_async::<()>(&mut realtime.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
    }
    let subject = realtime.v2_subject(claims.workspace_id, claims.app_id, channel)?;
    realtime
        .nats
        .publish(
            subject,
            serde_json::to_vec(&frame)
                .map_err(|_| RealtimeError::Unavailable)?
                .into(),
        )
        .await
        .map_err(|_| RealtimeError::Unavailable)?;
    realtime.barrier().await?;
    Ok(json!({"type":"channel.publish.result","accepted":true,"message_id":message_id}))
}

fn valid_client_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"_.:-".contains(&c))
}

async fn v2_action_put(
    realtime: &Realtime,
    claims: &RealtimeV2TokenClaims,
    channel: &str,
    message_id: &str,
    action_type: &str,
    idempotency_key: &str,
    data: &Value,
) -> Result<Value, RealtimeError> {
    if !v2_channel_allowed(claims, channel)
        || !valid_client_id(idempotency_key)
        || !matches!(action_type, "reaction" | "annotation")
        || !data.is_object()
        || data.to_string().len() > 4096
        || !message_id.starts_with("msg_")
        || !valid_client_id(message_id)
    {
        return Err(RealtimeError::InvalidInput);
    }
    let key = realtime.v2_actions_key(claims.app_id, channel, message_id);
    let exists: Option<String> = redis::cmd("HGET")
        .arg(&key)
        .arg("_exists")
        .query_async(&mut realtime.redis.clone())
        .await
        .map_err(|_| RealtimeError::Unavailable)?;
    if exists.as_deref() != Some("1") {
        return Err(RealtimeError::Unavailable);
    }
    let idem_field = format!("idem:{}:{}", claims.client_id, idempotency_key);
    if let Some(action_id) = redis::cmd("HGET")
        .arg(&key)
        .arg(&idem_field)
        .query_async::<Option<String>>(&mut realtime.redis.clone())
        .await
        .map_err(|_| RealtimeError::Unavailable)?
    {
        if let Some(existing) = redis::cmd("HGET")
            .arg(&key)
            .arg(format!("action:{action_id}"))
            .query_async::<Option<String>>(&mut realtime.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?
        {
            let action =
                serde_json::from_str::<Value>(&existing).map_err(|_| RealtimeError::Unavailable)?;
            return Ok(json!({
                "type":"message.action.updated",
                "app_id":claims.app_id,
                "channel":channel,
                "message_id":message_id,
                "action":action,
            }));
        }
    }
    let count = redis::cmd("HGET")
        .arg(&key)
        .arg("_count")
        .query_async::<Option<String>>(&mut realtime.redis.clone())
        .await
        .map_err(|_| RealtimeError::Unavailable)?
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);
    if count >= 100 {
        return Err(RealtimeError::Unavailable);
    }
    let action_id = format!("action_{}", Uuid::new_v4().simple());
    let action = json!({
        "id":action_id,
        "app_id":claims.app_id,
        "channel":channel,
        "message_id":message_id,
        "client_id":claims.client_id,
        "type":action_type,
        "idempotency_key":idempotency_key,
        "data":data,
        "created_at":chrono_millis().to_string(),
    });
    let raw = serde_json::to_string(&action).map_err(|_| RealtimeError::Unavailable)?;
    redis::cmd("HSET")
        .arg(&key)
        .arg(format!("action:{action_id}"))
        .arg(&raw)
        .arg(format!("owner:{action_id}"))
        .arg(&claims.client_id)
        .arg(&idem_field)
        .arg(&action_id)
        .arg("_count")
        .arg(count + 1)
        .query_async::<i64>(&mut realtime.redis.clone())
        .await
        .map_err(|_| RealtimeError::Unavailable)?;
    redis::cmd("PEXPIRE")
        .arg(&key)
        .arg(7 * 24 * 60 * 60 * 1000_i64)
        .query_async::<bool>(&mut realtime.redis.clone())
        .await
        .map_err(|_| RealtimeError::Unavailable)?;
    Ok(json!({
        "type":"message.action.updated",
        "app_id":claims.app_id,
        "channel":channel,
        "message_id":message_id,
        "action":action,
    }))
}

async fn v2_action_list(
    realtime: &Realtime,
    claims: &RealtimeV2TokenClaims,
    channel: &str,
    message_id: &str,
) -> Result<Value, RealtimeError> {
    if !v2_channel_allowed(claims, channel)
        || !message_id.starts_with("msg_")
        || !valid_client_id(message_id)
    {
        return Err(RealtimeError::InvalidInput);
    }
    let key = realtime.v2_actions_key(claims.app_id, channel, message_id);
    let exists: Option<String> = redis::cmd("HGET")
        .arg(&key)
        .arg("_exists")
        .query_async(&mut realtime.redis.clone())
        .await
        .map_err(|_| RealtimeError::Unavailable)?;
    if exists.as_deref() != Some("1") {
        return Err(RealtimeError::Unavailable);
    }
    let values: std::collections::HashMap<String, String> = redis::cmd("HGETALL")
        .arg(&key)
        .query_async(&mut realtime.redis.clone())
        .await
        .map_err(|_| RealtimeError::Unavailable)?;
    let mut actions = values
        .into_iter()
        .filter_map(|(field, value)| {
            field
                .strip_prefix("action:")
                .and_then(|_| serde_json::from_str::<Value>(&value).ok())
        })
        .collect::<Vec<_>>();
    actions.sort_by_key(|value| {
        value
            .get("created_at")
            .and_then(Value::as_str)
            .map(str::to_owned)
    });
    Ok(
        json!({"type":"message.actions.result","app_id":claims.app_id,"channel":channel,"message_id":message_id,"actions":actions}),
    )
}

async fn v2_action_remove(
    realtime: &Realtime,
    claims: &RealtimeV2TokenClaims,
    channel: &str,
    message_id: &str,
    action_id: &str,
) -> Result<Value, RealtimeError> {
    if !v2_channel_allowed(claims, channel)
        || !message_id.starts_with("msg_")
        || !valid_client_id(message_id)
        || !valid_client_id(action_id)
    {
        return Err(RealtimeError::InvalidInput);
    }
    let key = realtime.v2_actions_key(claims.app_id, channel, message_id);
    let field = format!("action:{action_id}");
    let raw: Option<String> = redis::cmd("HGET")
        .arg(&key)
        .arg(&field)
        .query_async(&mut realtime.redis.clone())
        .await
        .map_err(|_| RealtimeError::Unavailable)?;
    let Some(raw) = raw else {
        return Err(RealtimeError::Unavailable);
    };
    let owner: Option<String> = redis::cmd("HGET")
        .arg(&key)
        .arg(format!("owner:{action_id}"))
        .query_async(&mut realtime.redis.clone())
        .await
        .map_err(|_| RealtimeError::Unavailable)?;
    if owner.as_deref() != Some(claims.client_id.as_str()) {
        return Err(RealtimeError::Forbidden);
    }
    let mut action = serde_json::from_str::<Value>(&raw).map_err(|_| RealtimeError::Unavailable)?;
    if action.get("removed_at").is_none() {
        action["removed_at"] = json!(chrono_millis().to_string());
        redis::cmd("HSET")
            .arg(&key)
            .arg(&field)
            .arg(serde_json::to_string(&action).map_err(|_| RealtimeError::Unavailable)?)
            .query_async::<i64>(&mut realtime.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
    }
    Ok(
        json!({"type":"message.action.removed","app_id":claims.app_id,"channel":channel,"message_id":message_id,"action":action}),
    )
}

async fn v2_publish_file(
    realtime: &Realtime,
    claims: &RealtimeV2TokenClaims,
    connection_id: &str,
    channel: &str,
    file_id: &str,
    audience: Value,
) -> Result<(), RealtimeError> {
    if !v2_channel_allowed(claims, channel)
        || !file_id.starts_with("file_")
        || !valid_client_id(file_id)
    {
        return Err(RealtimeError::InvalidInput);
    }
    let audience_type = audience
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("all");
    if !matches!(audience_type, "all" | "others" | "connection" | "client") {
        return Err(RealtimeError::InvalidInput);
    }
    let pool = realtime.pool.as_ref().ok_or(RealtimeError::Unavailable)?;
    let row = sqlx::query("SELECT id,app_id,channel,name,mime_type,size_bytes,sha256,status,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(expires_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS expires_at,to_char(completed_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS completed_at FROM realtime_files WHERE id=$1 AND app_id=$2 AND channel=$3 AND status='ready' AND expires_at>clock_timestamp()")
        .bind(file_id)
        .bind(claims.app_id)
        .bind(channel)
        .fetch_optional(pool)
        .await
        .map_err(|_| RealtimeError::Unavailable)?
        .ok_or(RealtimeError::Unavailable)?;
    let message_id = format!("msg_{}", Uuid::new_v4().simple());
    let frame = json!({
        "type":"channel.message",
        "protocol":V2_PROTOCOL,
        "app_id":claims.app_id,
        "channel":channel,
        "publisher_app_id":claims.app_id,
        "publisher_client_id":claims.client_id,
        "publisher_connection_id":connection_id,
        "message_id":message_id,
        "published_at":chrono_millis().to_string(),
        "audience":audience,
        "file":{
            "id":row.get::<String,_>("id"),
            "app_id":row.get::<Uuid,_>("app_id"),
            "channel":row.get::<String,_>("channel"),
            "name":row.get::<String,_>("name"),
            "mime_type":row.get::<String,_>("mime_type"),
            "size_bytes":row.get::<i64,_>("size_bytes"),
            "sha256":row.get::<String,_>("sha256"),
            "status":row.get::<String,_>("status"),
            "created_at":row.get::<String,_>("created_at"),
            "expires_at":row.get::<String,_>("expires_at"),
            "completed_at":row.get::<Option<String>,_>("completed_at"),
        }
    });
    let key = realtime.v2_actions_key(claims.app_id, channel, &message_id);
    redis::cmd("HSET")
        .arg(&key)
        .arg("_exists")
        .arg("1")
        .arg("_encrypted")
        .arg("0")
        .query_async::<i64>(&mut realtime.redis.clone())
        .await
        .map_err(|_| RealtimeError::Unavailable)?;
    redis::cmd("PEXPIRE")
        .arg(&key)
        .arg(7 * 24 * 60 * 60 * 1000_i64)
        .query_async::<bool>(&mut realtime.redis.clone())
        .await
        .map_err(|_| RealtimeError::Unavailable)?;
    if audience_type == "all" {
        let sequence: i64 = redis::cmd("INCR")
            .arg(realtime.v2_history_sequence_key(claims.workspace_id, claims.app_id, channel))
            .query_async(&mut realtime.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
        let mut history = frame.clone();
        history["seq"] = json!(sequence);
        history["cursor"] = json!(sequence.to_string());
        redis::cmd("RPUSH")
            .arg(realtime.v2_history_key(claims.workspace_id, claims.app_id, channel))
            .arg(serde_json::to_string(&history).map_err(|_| RealtimeError::Unavailable)?)
            .query_async::<()>(&mut realtime.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
        redis::cmd("LTRIM")
            .arg(realtime.v2_history_key(claims.workspace_id, claims.app_id, channel))
            .arg(-1000)
            .arg(-1)
            .query_async::<()>(&mut realtime.redis.clone())
            .await
            .map_err(|_| RealtimeError::Unavailable)?;
    }
    realtime
        .nats
        .publish(
            realtime.v2_subject(claims.workspace_id, claims.app_id, channel)?,
            serde_json::to_vec(&frame)
                .map_err(|_| RealtimeError::Unavailable)?
                .into(),
        )
        .await
        .map_err(|_| RealtimeError::Unavailable)?;
    realtime.barrier().await
}

async fn app_identity(
    identity: &crate::identity::Identity,
    headers: &axum::http::HeaderMap,
    method: &str,
    target: &str,
    body: &[u8],
) -> Result<Uuid, crate::identity::AuthError> {
    let principal = identity
        .authenticate_public_request(headers, method, target, body)
        .await?;
    match principal {
        Principal::Application { app_id, .. } | Principal::Agent { app_id, .. } => Ok(app_id),
        _ => Err(crate::identity::AuthError::Forbidden),
    }
}

fn realtime_bad_input() -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({"error":"invalid_request"})),
    )
        .into_response()
}
fn realtime_disabled(error: &'static str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error":error})),
    )
        .into_response()
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileCreateInput {
    channel: String,
    name: String,
    mime_type: String,
    size_bytes: i64,
    sha256: String,
}

async fn file_create(
    State((realtime, identity)): State<(Realtime, crate::identity::Identity)>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let app_id =
        match app_identity(&identity, &headers, "POST", "/api/v2/realtime/files", &body).await {
            Ok(app_id) => app_id,
            Err(error) => return error.into_response(),
        };
    let input: FileCreateInput = match serde_json::from_slice(&body) {
        Ok(input) => input,
        Err(_) => return realtime_bad_input(),
    };
    let channel = input.channel.trim();
    let name = input.name.trim();
    let mime_type = input.mime_type.trim().to_ascii_lowercase();
    let sha256 = input.sha256.trim().to_ascii_lowercase();
    if validate_channel(channel).is_err()
        || name.is_empty()
        || name.len() > 255
        || name
            .chars()
            .any(|value| value == '/' || value == '\\' || value.is_control())
        || !matches!(
            mime_type.as_str(),
            "image/jpeg"
                | "image/png"
                | "image/gif"
                | "image/webp"
                | "application/pdf"
                | "text/plain"
                | "application/zip"
        )
        || !(1..=25 * 1024 * 1024).contains(&input.size_bytes)
        || crate::object_store::ObjectStore::from_env().is_err()
        || !valid_sha256(&sha256)
    {
        return if crate::object_store::ObjectStore::from_env().is_err() {
            realtime_disabled("file_messaging_disabled")
        } else {
            realtime_bad_input()
        };
    }
    let Some(store) = realtime.object_store.as_ref() else {
        return realtime_disabled("file_messaging_disabled");
    };
    let Some(pool) = realtime.pool.as_ref() else {
        return realtime_disabled("file_messaging_unavailable");
    };
    let file_id = format!("file_{}", Uuid::new_v4().simple());
    let object_key = format!("realtime/{app_id}/{file_id}");
    let row = sqlx::query("INSERT INTO realtime_files(id,app_id,channel,name,mime_type,size_bytes,sha256,object_key,status,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'pending',clock_timestamp()+interval '24 hours') RETURNING id,app_id,channel,name,mime_type,size_bytes,sha256,status,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(expires_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS expires_at,to_char(completed_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS completed_at")
        .bind(&file_id).bind(app_id).bind(channel).bind(name).bind(&mime_type).bind(input.size_bytes).bind(&sha256).bind(&object_key).fetch_one(pool).await;
    let row = match row {
        Ok(row) => row,
        Err(_) => return realtime_disabled("file_messaging_unavailable"),
    };
    let (upload_url, required_headers) = match store.presign_upload(
        &object_key,
        &mime_type,
        &sha256,
        Duration::from_secs(15 * 60),
    ) {
        Ok(value) => value,
        Err(_) => return realtime_disabled("file_messaging_unavailable"),
    };
    (StatusCode::CREATED, Json(json!({"file":file_json(&row),"upload_url":upload_url,"required_headers":required_headers}))).into_response()
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

async fn file_complete(
    State((realtime, identity)): State<(Realtime, crate::identity::Identity)>,
    headers: axum::http::HeaderMap,
    Path(file_id): Path<String>,
) -> Response {
    let target = format!("/api/v2/realtime/files/{file_id}/complete");
    let app_id = match app_identity(&identity, &headers, "POST", &target, &[]).await {
        Ok(app_id) => app_id,
        Err(error) => return error.into_response(),
    };
    let Some(store) = realtime.object_store.as_ref() else {
        return realtime_disabled("file_messaging_disabled");
    };
    let Some(pool) = realtime.pool.as_ref() else {
        return realtime_disabled("file_messaging_unavailable");
    };
    let row = sqlx::query("SELECT id,app_id,channel,name,mime_type,size_bytes,sha256,object_key,status,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(expires_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS expires_at,to_char(completed_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS completed_at FROM realtime_files WHERE id=$1 AND app_id=$2")
        .bind(&file_id).bind(app_id).fetch_optional(pool).await;
    let Some(row) = (match row {
        Ok(row) => row,
        Err(_) => return realtime_disabled("file_messaging_unavailable"),
    }) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error":"file_not_found"})),
        )
            .into_response();
    };
    if row.get::<String, _>("status") == "ready" {
        return Json(json!(file_json(&row))).into_response();
    }
    let object_key = row.get::<String, _>("object_key");
    let info = match store.stat(&object_key).await {
        Ok(info) => info,
        Err(_) => {
            return (
                StatusCode::CONFLICT,
                Json(json!({"error":"file_not_ready"})),
            )
                .into_response();
        }
    };
    if info.size != row.get::<i64, _>("size_bytes")
        || !info
            .sha256
            .eq_ignore_ascii_case(row.get::<String, _>("sha256").as_str())
    {
        return (
            StatusCode::CONFLICT,
            Json(json!({"error":"file_not_ready"})),
        )
            .into_response();
    }
    let completed = sqlx::query("UPDATE realtime_files SET status='ready',completed_at=clock_timestamp() WHERE id=$1 AND app_id=$2 AND status='pending' AND expires_at>clock_timestamp() RETURNING id,app_id,channel,name,mime_type,size_bytes,sha256,object_key,status,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(expires_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS expires_at,to_char(completed_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS completed_at")
        .bind(&file_id).bind(app_id).fetch_optional(pool).await;
    match completed {
        Ok(Some(row)) => Json(json!(file_json(&row))).into_response(),
        Ok(None) => (
            StatusCode::CONFLICT,
            Json(json!({"error":"file_not_ready"})),
        )
            .into_response(),
        Err(_) => realtime_disabled("file_messaging_unavailable"),
    }
}

async fn file_download(
    State((realtime, identity)): State<(Realtime, crate::identity::Identity)>,
    headers: axum::http::HeaderMap,
    Path(file_id): Path<String>,
) -> Response {
    let target = format!("/api/v2/realtime/files/{file_id}/download");
    let app_id = match app_identity(&identity, &headers, "GET", &target, &[]).await {
        Ok(app_id) => app_id,
        Err(error) => return error.into_response(),
    };
    let Some(store) = realtime.object_store.as_ref() else {
        return realtime_disabled("file_messaging_disabled");
    };
    let Some(pool) = realtime.pool.as_ref() else {
        return realtime_disabled("file_messaging_unavailable");
    };
    let row = sqlx::query("SELECT id,app_id,channel,name,mime_type,size_bytes,sha256,object_key,status,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(expires_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS expires_at,to_char(completed_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS completed_at FROM realtime_files WHERE id=$1 AND app_id=$2 AND status='ready' AND expires_at>clock_timestamp()")
        .bind(&file_id).bind(app_id).fetch_optional(pool).await;
    let Some(row) = (match row {
        Ok(row) => row,
        Err(_) => return realtime_disabled("file_messaging_unavailable"),
    }) else {
        return (
            StatusCode::CONFLICT,
            Json(json!({"error":"file_not_ready"})),
        )
            .into_response();
    };
    let object_key = row.get::<String, _>("object_key");
    let download_url = match store.presign_download(&object_key, Duration::from_secs(15 * 60)) {
        Ok(url) => url,
        Err(_) => return realtime_disabled("file_messaging_unavailable"),
    };
    Json(json!({"file":file_json(&row),"download_url":download_url})).into_response()
}

fn file_json(row: &sqlx::postgres::PgRow) -> Value {
    json!({"id":row.get::<String,_>("id"),"app_id":row.get::<Uuid,_>("app_id"),"channel":row.get::<String,_>("channel"),"name":row.get::<String,_>("name"),"mime_type":row.get::<String,_>("mime_type"),"size_bytes":row.get::<i64,_>("size_bytes"),"sha256":row.get::<String,_>("sha256"),"status":row.get::<String,_>("status"),"created_at":row.get::<String,_>("created_at"),"expires_at":row.get::<String,_>("expires_at"),"completed_at":row.get::<Option<String>,_>("completed_at")})
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PushRegisterInput {
    provider: String,
    token: String,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PushNotificationInput {
    pub(crate) title: String,
    pub(crate) body: String,
    #[serde(default)]
    pub(crate) data: Value,
}

async fn push_register(
    State((_, identity)): State<(Realtime, crate::identity::Identity)>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    let app_id = match app_identity(
        &identity,
        &headers,
        "POST",
        "/api/v2/realtime/push/devices",
        &body,
    )
    .await
    {
        Ok(app_id) => app_id,
        Err(error) => return error.into_response(),
    };
    let input: PushRegisterInput = match serde_json::from_slice(&body) {
        Ok(input) => input,
        Err(_) => return realtime_bad_input(),
    };
    let provider = input.provider.trim().to_ascii_lowercase();
    let token = input.token.trim();
    if !matches!(provider.as_str(), "apns" | "fcm")
        || !(16..=4096).contains(&token.len())
        || token.chars().any(char::is_control)
    {
        return realtime_bad_input();
    }
    let hash = sha2::Sha256::digest(token.as_bytes()).to_vec();
    let encrypted = match identity.encrypt_secret(token.as_bytes()) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    let id = Uuid::new_v4();
    let row = sqlx::query("INSERT INTO realtime_push_devices(id,app_id,provider,token_hash,encrypted_token) VALUES($1,$2,$3,$4,$5) ON CONFLICT(app_id,provider,token_hash) DO UPDATE SET encrypted_token=EXCLUDED.encrypted_token,updated_at=clock_timestamp() RETURNING id,app_id,provider,to_char(created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,to_char(updated_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS updated_at")
        .bind(id)
        .bind(app_id)
        .bind(provider)
        .bind(hash)
        .bind(encrypted)
        .fetch_one(&identity.pool())
        .await;
    match row {
        Ok(row) => (
            StatusCode::CREATED,
            Json(json!({"id":row.get::<Uuid,_>("id"),"app_id":row.get::<Uuid,_>("app_id"),"provider":row.get::<String,_>("provider"),"created_at":row.get::<String,_>("created_at"),"updated_at":row.get::<String,_>("updated_at")})),
        )
            .into_response(),
        Err(_) => realtime_disabled("push_unavailable"),
    }
}

async fn push_delete(
    State((_, identity)): State<(Realtime, crate::identity::Identity)>,
    headers: axum::http::HeaderMap,
    Path(device_id): Path<Uuid>,
) -> Response {
    let app_id = match app_identity(
        &identity,
        &headers,
        "DELETE",
        &format!("/api/v2/realtime/push/devices/{device_id}"),
        &[],
    )
    .await
    {
        Ok(app_id) => app_id,
        Err(error) => return error.into_response(),
    };
    match sqlx::query("DELETE FROM realtime_push_devices WHERE app_id=$1 AND id=$2")
        .bind(app_id)
        .bind(device_id)
        .execute(&identity.pool())
        .await
    {
        Ok(result) if result.rows_affected() == 1 => StatusCode::NO_CONTENT.into_response(),
        Ok(_) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error":"device_not_found"})),
        )
            .into_response(),
        Err(_) => realtime_disabled("push_unavailable"),
    }
}

async fn push_bind(
    State((_, identity)): State<(Realtime, crate::identity::Identity)>,
    headers: axum::http::HeaderMap,
    Path((channel, device_id)): Path<(String, Uuid)>,
) -> Response {
    push_bind_inner(identity, headers, channel, device_id, true).await
}
async fn push_unbind(
    State((_, identity)): State<(Realtime, crate::identity::Identity)>,
    headers: axum::http::HeaderMap,
    Path((channel, device_id)): Path<(String, Uuid)>,
) -> Response {
    push_bind_inner(identity, headers, channel, device_id, false).await
}
async fn push_bind_inner(
    identity: crate::identity::Identity,
    headers: axum::http::HeaderMap,
    channel: String,
    device_id: Uuid,
    bind: bool,
) -> Response {
    if channel.is_empty() || channel.len() > 128 || channel.chars().any(char::is_control) {
        return realtime_bad_input();
    }
    let method = if bind { "PUT" } else { "DELETE" };
    let target = format!("/api/v2/realtime/push/channels/{channel}/devices/{device_id}");
    let app_id = match app_identity(&identity, &headers, method, &target, &[]).await {
        Ok(app_id) => app_id,
        Err(error) => return error.into_response(),
    };
    let result = if bind {
        sqlx::query("INSERT INTO realtime_push_bindings(app_id,channel,device_id) SELECT $1,$2,id FROM realtime_push_devices WHERE app_id=$1 AND id=$3 ON CONFLICT DO NOTHING")
            .bind(app_id).bind(&channel).bind(device_id).execute(&identity.pool()).await
    } else {
        sqlx::query(
            "DELETE FROM realtime_push_bindings WHERE app_id=$1 AND channel=$2 AND device_id=$3",
        )
        .bind(app_id)
        .bind(&channel)
        .bind(device_id)
        .execute(&identity.pool())
        .await
    };
    match result {
        Ok(result) if !bind || result.rows_affected() == 1 => {
            StatusCode::NO_CONTENT.into_response()
        }
        Ok(_) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error":"device_not_found"})),
        )
            .into_response(),
        Err(_) => realtime_disabled("push_unavailable"),
    }
}

async fn push_notify(
    State((_, identity)): State<(Realtime, crate::identity::Identity)>,
    headers: axum::http::HeaderMap,
    Path(channel): Path<String>,
    body: Bytes,
) -> Response {
    let target = format!("/api/v2/realtime/push/channels/{channel}/notifications");
    let app_id = match app_identity(&identity, &headers, "POST", &target, &body).await {
        Ok(app_id) => app_id,
        Err(error) => return error.into_response(),
    };
    let input: PushNotificationInput = match serde_json::from_slice(&body) {
        Ok(input) => input,
        Err(_) => return realtime_bad_input(),
    };
    if channel.is_empty()
        || channel.len() > 128
        || input.title.len() > 100
        || input.body.len() > 500
        || input.data.to_string().len() > 4096
        || input.title.chars().any(char::is_control)
        || input.body.chars().any(char::is_control)
    {
        return realtime_bad_input();
    }
    if !input.data.is_null() && !input.data.is_object() {
        return realtime_bad_input();
    }
    if contains_secret_key(&input.data) {
        return realtime_bad_input();
    }
    let rows = sqlx::query("SELECT d.id,d.provider,d.encrypted_token FROM realtime_push_bindings b JOIN realtime_push_devices d ON d.id=b.device_id AND d.app_id=b.app_id WHERE b.app_id=$1 AND b.channel=$2 ORDER BY d.id LIMIT 100")
        .bind(app_id).bind(&channel).fetch_all(&identity.pool()).await;
    let rows = match rows {
        Ok(rows) => rows,
        Err(_) => return realtime_disabled("push_unavailable"),
    };
    let mut outcomes = Vec::with_capacity(rows.len());
    for row in rows {
        let id = Uuid::new_v4();
        let device_id: Uuid = row.get("id");
        let provider: String = row.get("provider");
        let mut status = "failed";
        let mut reason = Some("provider_unavailable".to_owned());
        let mut provider_message_id = None;
        if let Some((endpoint, authorization, topic, project)) = push_config(&provider) {
            let encrypted = row.get::<String, _>("encrypted_token");
            let token = identity.decrypt_secret(&encrypted).ok();
            if let Some(token) = token {
                match send_push(
                    &provider,
                    &endpoint,
                    &authorization,
                    topic.as_deref(),
                    project.as_deref(),
                    &token,
                    &input,
                )
                .await
                {
                    Ok(message_id) => {
                        status = "delivered";
                        reason = None;
                        provider_message_id = message_id;
                    }
                    Err(_) => reason = Some("provider_error".to_owned()),
                }
            }
        }
        if sqlx::query("INSERT INTO realtime_push_outcomes(id,app_id,device_id,channel,provider,status,provider_message_id,reason) VALUES($1,$2,$3,$4,$5,$6,$7,$8)")
            .bind(id).bind(app_id).bind(device_id).bind(&channel).bind(&provider).bind(status).bind(&provider_message_id).bind(&reason).execute(&identity.pool()).await.is_err() {
            return realtime_disabled("push_unavailable");
        }
        outcomes.push(json!({"id":id,"app_id":app_id,"device_id":device_id,"channel":channel,"provider":provider,"status":status,"provider_message_id":provider_message_id,"reason":reason}));
    }
    Json(json!({"outcomes":outcomes})).into_response()
}

fn contains_secret_key(value: &Value) -> bool {
    match value {
        Value::Object(entries) => entries.iter().any(|(key, child)| {
            let key = key.to_ascii_lowercase();
            key.contains("secret")
                || key.contains("token")
                || key.contains("password")
                || key.contains("authorization")
                || contains_secret_key(child)
        }),
        Value::Array(items) => items.iter().any(contains_secret_key),
        _ => false,
    }
}

pub(crate) fn push_config(
    provider: &str,
) -> Option<(String, String, Option<String>, Option<String>)> {
    let env = |name: &str| {
        std::env::var(name)
            .ok()
            .filter(|value| !value.trim().is_empty())
    };
    match provider {
        "apns" => Some((
            env("RELAYHUB_APNS_ENDPOINT")?,
            env("RELAYHUB_APNS_AUTHORIZATION")?,
            Some(env("RELAYHUB_APNS_TOPIC")?),
            None,
        )),
        "fcm" => Some((
            env("RELAYHUB_FCM_ENDPOINT")?,
            env("RELAYHUB_FCM_AUTHORIZATION")?,
            None,
            Some(env("RELAYHUB_FCM_PROJECT")?),
        )),
        _ => None,
    }
}

pub(crate) async fn send_push(
    provider: &str,
    endpoint: &str,
    authorization: &str,
    topic: Option<&str>,
    project: Option<&str>,
    token: &[u8],
    input: &PushNotificationInput,
) -> Result<Option<String>, ()> {
    let (url, payload) = match provider {
        "apns" => (
            format!(
                "{}/3/device/{}",
                endpoint.trim_end_matches('/'),
                urlencoding(token)
            ),
            json!({"aps":{"alert":{"title":input.title,"body":input.body}},"data":input.data}),
        ),
        "fcm" => (
            format!(
                "{}/v1/projects/{}/messages:send",
                endpoint.trim_end_matches('/'),
                project.ok_or(())?
            ),
            json!({"message":{"token":String::from_utf8(token.to_vec()).map_err(|_| ())?,"notification":{"title":input.title,"body":input.body},"data":input.data}}),
        ),
        _ => return Err(()),
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|_| ())?;
    let mut request = client
        .post(url)
        .header("authorization", authorization)
        .header("content-type", "application/json")
        .json(&payload);
    if provider == "apns" {
        request = request.header("apns-topic", topic.ok_or(())?);
    }
    let response = request.send().await.map_err(|_| ())?;
    let provider_id = response
        .headers()
        .get("apns-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if !response.status().is_success() {
        return Err(());
    }
    if provider == "fcm" {
        let body = response.json::<Value>().await.map_err(|_| ())?;
        Ok(body.get("name").and_then(Value::as_str).map(str::to_owned))
    } else {
        Ok(provider_id)
    }
}

fn urlencoding(value: &[u8]) -> String {
    value
        .iter()
        .flat_map(|byte| {
            if byte.is_ascii_alphanumeric() || b"-_.~".contains(byte) {
                vec![*byte as char].into_iter().collect::<Vec<_>>()
            } else {
                format!("%{byte:02X}").chars().collect::<Vec<_>>()
            }
        })
        .collect()
}

async fn v2_socket_loop(
    mut socket: axum::extract::ws::WebSocket,
    realtime: Realtime,
    claims: RealtimeV2TokenClaims,
    connection_id: String,
) {
    use axum::extract::ws::Message;
    let principal = Principal::Application {
        workspace_id: claims.workspace_id,
        app_id: claims.app_id,
        key_id: Uuid::nil(),
    };
    let session = connection_id
        .strip_prefix("conn_")
        .and_then(|value| Uuid::parse_str(value).ok())
        .unwrap_or_else(Uuid::new_v4);
    let (outbound, mut received) = mpsc::channel::<Value>(OUTBOUND_BUFFER_FRAMES);
    let ready = json!({
        "type":"ready",
        "protocol": V2_PROTOCOL,
        "app_id": claims.app_id,
        "client_id": claims.client_id,
        "connection_id": connection_id,
        "capabilities":["subscribe","unsubscribe","publish","publish.batch","history","rewind","namespace-grants","presence","occupancy","message.actions","encryption.aes-256-gcm","audience.all","audience.others","audience.connection","audience.client"]
    });
    if send_v2(&mut socket, &ready).await.is_err() {
        return;
    }
    let mut subscriptions: HashMap<String, JoinHandle<()>> = HashMap::new();
    let result = loop {
        tokio::select! {
            Some(frame) = received.recv() => {
                if send_v2(&mut socket, &frame).await.is_err() { break (); }
            }
            message = socket.recv() => {
                let Some(Ok(message)) = message else { break (); };
                let bytes = match message {
                    Message::Text(text) => text.as_bytes().to_vec(),
                    Message::Binary(bytes) => bytes.to_vec(),
                    Message::Close(_) => break (),
                    Message::Ping(_) | Message::Pong(_) => continue,
                };
                let frame: Value = match serde_json::from_slice(&bytes) {
                    Ok(value) => value,
                    Err(_) => { if send_v2(&mut socket, &json!({"type":"error","code":"invalid_json","message":"Frame must contain one JSON object."})).await.is_err() { break (); } continue; }
                };
                let kind = frame.get("type").and_then(Value::as_str).unwrap_or("");
                match kind {
                    "ping" => {
                        if send_v2(&mut socket, &json!({"type":"pong"})).await.is_err() { break (); }
                    }
                    "subscribe" => {
                        let Some(channels) = frame.get("channels").and_then(Value::as_array) else {
                            if send_v2(&mut socket, &json!({"type":"error","code":"invalid_channels","message":"channels is required."})).await.is_err() { break (); }
                            continue;
                        };
                        let rewind = frame.get("rewind").cloned();
                        let mut accepted = Vec::new();
                        let mut failed = None;
                        for channel in channels.iter().filter_map(Value::as_str) {
                            if !v2_channel_allowed(&claims, channel) { failed = Some(("forbidden", channel.to_owned())); break; }
                            if subscriptions.contains_key(channel) { accepted.push(channel.to_owned()); continue; }
                            let subject = match realtime.v2_subject(claims.workspace_id, claims.app_id, channel) { Ok(value) => value, Err(_) => { failed = Some(("invalid_channel", channel.to_owned())); break; } };
                            let mut subscriber = match realtime.nats.subscribe(subject).await { Ok(value) => value, Err(_) => { failed = Some(("realtime_unavailable", channel.to_owned())); break; } };
                            if realtime.register_connection_with_protocol(&principal, session, channel, V2_PROTOCOL).await.is_err() {
                                failed = Some(("realtime_unavailable", channel.to_owned()));
                                break;
                            }
                            let tx = outbound.clone();
                            let channel_name = channel.to_owned();
                            let local_connection = connection_id.clone();
                            let local_client = claims.client_id.clone();
                            let task = tokio::spawn(async move {
                                while let Some(message) = subscriber.next().await {
                                    let Ok(frame) = serde_json::from_slice::<Value>(&message.payload) else { continue; };
                                    if frame.get("channel").and_then(Value::as_str) != Some(channel_name.as_str()) || !v2_visible(&frame, &local_connection, &local_client) { continue; }
                                    if tx.send(frame).await.is_err() { break; }
                                }
                            });
                            subscriptions.insert(channel.to_owned(), task);
                            accepted.push(channel.to_owned());
                        }
                        if let Some((code, channel)) = failed {
                            if send_v2(&mut socket, &json!({"type":"error","code":code,"channel":channel})).await.is_err() { break (); }
                        } else {
                            if send_v2(&mut socket, &json!({"type":"subscribed","channels":accepted})).await.is_err() { break (); }
                            if let Some(rewind) = rewind {
                                let limit = rewind.get("limit").and_then(Value::as_u64).unwrap_or(50) as usize;
                                let cursor = rewind.get("cursor").and_then(Value::as_str);
                                for channel in accepted {
                                    if let Ok(history) = v2_history(&realtime, &claims, &channel, cursor, limit).await {
                                        if send_v2(&mut socket, &history).await.is_err() { break; }
                                    }
                                }
                            }
                        }
                    }
                    "unsubscribe" => {
                        let channels = frame.get("channels").and_then(Value::as_array).cloned().unwrap_or_default();
                        let mut removed = Vec::new();
                        for channel in channels.iter().filter_map(Value::as_str) {
                            if let Some(task) = subscriptions.remove(channel) { task.abort(); removed.push(channel.to_owned()); }
                        }
                        if send_v2(&mut socket, &json!({"type":"unsubscribed","channels":removed})).await.is_err() { break (); }
                    }
                    "history.get" => {
                        let Some(channel) = frame.get("channel").and_then(Value::as_str) else { if send_v2(&mut socket, &json!({"type":"error","code":"invalid_history"})).await.is_err() { break (); } continue; };
                        match v2_history(&realtime, &claims, channel, frame.get("cursor").and_then(Value::as_str), frame.get("limit").and_then(Value::as_u64).unwrap_or(50) as usize).await {
                            Ok(history) => { if send_v2(&mut socket, &history).await.is_err() { break (); } }
                            Err(error) => { if send_v2(&mut socket, &json!({"type":"error","code":format!("{error:?}").to_lowercase()})).await.is_err() { break (); } }
                        }
                    }
                    "channel.publish" => {
                        let Some(channel) = frame.get("channel").and_then(Value::as_str) else { if send_v2(&mut socket, &json!({"type":"error","code":"invalid_publish"})).await.is_err() { break (); } continue; };
                        let data = frame.get("data").cloned();
                        let encryption = frame.get("encryption").cloned();
                        let audience = frame.get("audience").cloned().unwrap_or_else(|| json!({"type":"all"}));
                        match v2_publish(&realtime, &claims, &connection_id, channel, data, encryption, audience).await {
                            Ok(result) => { if send_v2(&mut socket, &result).await.is_err() { break (); } }
                            Err(error) => { if send_v2(&mut socket, &json!({"type":"error","code":format!("{error:?}").to_lowercase()})).await.is_err() { break (); } }
                        }
                    }
                    "channel.publish.batch" => {
                        let mut outcomes = Vec::new();
                        if let Some(items) = frame.get("items").and_then(Value::as_array) {
                            for item in items.iter().take(50) {
                                let id = item.get("id").cloned().unwrap_or(Value::Null);
                                let channel = item.get("channel").and_then(Value::as_str).unwrap_or("");
                                let data = item.get("data").cloned();
                                let encryption = item.get("encryption").cloned();
                                let audience = item.get("audience").cloned().unwrap_or_else(|| json!({"type":"all"}));
                                match v2_publish(&realtime, &claims, &connection_id, channel, data, encryption, audience).await {
                                    Ok(result) => outcomes.push(json!({"id":id,"accepted":true,"message_id":result.get("message_id")})),
                                    Err(error) => outcomes.push(json!({"id":id,"accepted":false,"code":format!("{error:?}").to_lowercase()})),
                                }
                            }
                        }
                        if send_v2(&mut socket, &json!({"type":"channel.publish.batch.result","outcomes":outcomes})).await.is_err() { break (); }
                    }
                    "presence.update" => {
                        let Some(channel) = frame.get("channel").and_then(Value::as_str) else { continue; };
                        if !v2_channel_allowed(&claims, channel) || !subscriptions.contains_key(channel) { if send_v2(&mut socket, &json!({"type":"error","code":"not_subscribed"})).await.is_err() { break (); } continue; }
                        let data = frame.get("data").cloned().unwrap_or_else(|| json!({}));
                        let key = realtime.v2_presence_key(claims.workspace_id, claims.app_id, channel);
                        let _: Result<(), _> = redis::cmd("ZADD").arg(&key).arg(chrono_millis()).arg(&connection_id).query_async(&mut realtime.redis.clone()).await;
                        let occupancy: i64 = redis::cmd("ZCARD").arg(&key).query_async(&mut realtime.redis.clone()).await.unwrap_or(1);
                        let presence = json!({"type":"presence.update","protocol":V2_PROTOCOL,"app_id":claims.app_id,"channel":channel,"publisher_client_id":claims.client_id,"publisher_connection_id":connection_id,"message_id":format!("msg_{}",Uuid::new_v4().simple()),"published_at":chrono_millis().to_string(),"occupancy":occupancy,"data":data});
                        let subject = match realtime.v2_subject(claims.workspace_id, claims.app_id, channel) { Ok(value) => value, Err(_) => continue };
                        let _ = realtime.nats.publish(subject, serde_json::to_vec(&presence).unwrap_or_default().into()).await;
                        let _ = realtime.barrier().await;
                    }
                    "message.action.put" => {
                        let result = match (frame.get("channel").and_then(Value::as_str), frame.get("message_id").and_then(Value::as_str), frame.get("action_type").and_then(Value::as_str), frame.get("idempotency_key").and_then(Value::as_str), frame.get("data")) {
                            (Some(channel), Some(message_id), Some(action_type), Some(idempotency_key), Some(data)) => v2_action_put(&realtime, &claims, channel, message_id, action_type, idempotency_key, data).await,
                            _ => Err(RealtimeError::InvalidInput),
                        };
                        if let Ok(result) = &result {
                            let _ = realtime.nats.publish(realtime.v2_subject(claims.workspace_id, claims.app_id, result.get("channel").and_then(Value::as_str).unwrap_or("")).unwrap_or_default(), serde_json::to_vec(result).unwrap_or_default().into()).await;
                        }
                        if send_v2(&mut socket, &result.unwrap_or_else(|error| json!({"type":"error","code":format!("{error:?}").to_lowercase()}))).await.is_err() { break (); }
                    }
                    "message.actions.get" => {
                        let result = match (frame.get("channel").and_then(Value::as_str), frame.get("message_id").and_then(Value::as_str)) {
                            (Some(channel), Some(message_id)) => v2_action_list(&realtime, &claims, channel, message_id).await,
                            _ => Err(RealtimeError::InvalidInput),
                        };
                        if send_v2(&mut socket, &result.unwrap_or_else(|error| json!({"type":"error","code":format!("{error:?}").to_lowercase()}))).await.is_err() { break (); }
                    }
                    "message.action.remove" => {
                        let result = match (frame.get("channel").and_then(Value::as_str), frame.get("message_id").and_then(Value::as_str), frame.get("action_id").and_then(Value::as_str)) {
                            (Some(channel), Some(message_id), Some(action_id)) => v2_action_remove(&realtime, &claims, channel, message_id, action_id).await,
                            _ => Err(RealtimeError::InvalidInput),
                        };
                        if send_v2(&mut socket, &result.unwrap_or_else(|error| json!({"type":"error","code":format!("{error:?}").to_lowercase()}))).await.is_err() { break (); }
                    }
                    "file.publish" => {
                        let result = match (frame.get("channel").and_then(Value::as_str), frame.get("file_id").and_then(Value::as_str)) {
                            (Some(channel), Some(file_id)) => v2_publish_file(&realtime, &claims, &connection_id, channel, file_id, frame.get("audience").cloned().unwrap_or_else(|| json!({"type":"all"}))).await,
                            _ => Err(RealtimeError::InvalidInput),
                        };
                        if let Err(error) = result {
                            if send_v2(&mut socket, &json!({"type":"error","code":format!("{error:?}").to_lowercase()})).await.is_err() { break (); }
                        }
                    }
                    _ => {
                        if send_v2(&mut socket, &json!({"type":"error","code":"unknown_type","message":"Frame type is not supported."})).await.is_err() { break (); }
                    }
                }
            }
        }
    };
    let _ = result;
    for (_, task) in subscriptions {
        task.abort();
    }
    realtime.remove_connection(&principal, session).await;
}

fn chrono_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as i64)
        .unwrap_or_default()
}

const V2_PROTOCOL: &str = "relayhub.realtime.v2";

async fn socket_loop(
    mut socket: axum::extract::ws::WebSocket,
    mut connection: Connection,
    identity: crate::identity::Identity,
    token: String,
    principal: Principal,
    channel: String,
) {
    use axum::extract::ws::{CloseFrame, Message};
    let mut auth = Box::pin(tokio::time::sleep(Duration::ZERO));
    let (code, reason) = loop {
        tokio::select! {
            biased;
            _ = &mut auth => {
                match tokio::time::timeout(IO_TIMEOUT, authenticated_token(&identity, &connection.realtime, &token, &channel, false)).await {
                    Ok(Ok(ref current)) if *current == principal => {},
                    Err(_) => {
                        auth.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(250));
                        continue;
                    },
                    Ok(Err(error)) => break match error {
                        RealtimeError::Unavailable => (1011, "realtime_unavailable"),
                        _ => (1008, "forbidden"),
                    },
                    Ok(Ok(_)) => break (1008, "forbidden"),
                }
                auth.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(250));
            },
            frame = connection.recv() => {
                match frame {
                    Ok(Some(frame)) => {
                        let Ok(text) = serde_json::to_string(&frame) else { break (1011, "realtime_unavailable"); };
                        if bounded(socket.send(Message::Text(text.into()))).await.is_err() { break (1008, "slow_reader"); }
                    },
                    Err(reason) => break disconnect_close(reason),
                    Ok(None) => break (1000, "closed"),
                }
            },
            message = socket.recv() => {
                let Some(Ok(message)) = message else { break (1000, "closed"); };
                let bytes = match message {
                    Message::Text(text) => text.as_bytes().to_vec(),
                    Message::Binary(bytes) => bytes.to_vec(),
                    Message::Close(_) => break (1000, "closed"),
                    Message::Ping(_) | Message::Pong(_) => continue,
                };
                match tokio::time::timeout(IO_TIMEOUT, authenticated_token(&identity, &connection.realtime, &token, &channel, true)).await {
                    Ok(Ok(ref current)) if *current == principal => {}
                    Ok(Err(RealtimeError::Unavailable)) => break (1011, "realtime_unavailable"),
                    _ => break (1008, "forbidden"),
                }
                if let Err(error) = connection.realtime.publish_frame(&principal, &bytes).await {
                    break match error {
                        RealtimeError::Forbidden => (1008, "forbidden"),
                        RealtimeError::PayloadTooLarge => (1009, "payload_too_large"),
                        RealtimeError::InvalidInput => (1008, "invalid_input"),
                        RealtimeError::Unavailable => (1011, "realtime_unavailable"),
                    };
                }
            }
        }
    };
    let _ = bounded(socket.send(Message::Close(Some(CloseFrame {
        code,
        reason: reason.into(),
    }))))
    .await;
    let realtime = connection.realtime.clone();
    let session = connection.session;
    let _ = connection.unsubscribe(&principal, &channel).await;
    realtime.remove_connection(&principal, session).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disconnect_transport_failure_uses_internal_error_close() {
        assert_eq!(
            disconnect_close(DisconnectReason::TransportUnavailable),
            (1011, "realtime_unavailable")
        );
        assert_eq!(
            disconnect_close(DisconnectReason::SlowReader),
            (1008, "slow_reader")
        );
        assert_eq!(disconnect_close(DisconnectReason::Closed), (1000, "closed"));
    }
}
