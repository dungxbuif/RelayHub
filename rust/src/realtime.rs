//! Ephemeral Core NATS fanout. Receipts describe routing, never durable acceptance or processing.
use crate::{
    identity::{Action, authorize},
    protocol::{
        Frame, MAX_BODY_BYTES, PRESENCE_EXPIRY_SECONDS, PRESENCE_HEARTBEAT_SECONDS, Principal,
        ProtocolError, VERSION, parse_frame,
    },
};
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::StreamExt;
use serde::Serialize;
use std::{collections::HashMap, future::Future, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
use uuid::Uuid;

pub const OUTBOUND_BUFFER_FRAMES: usize = 64;
const IO_TIMEOUT: Duration = Duration::from_secs(2);

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
            .route("/api/v1/realtime/{channel}", axum::routing::get(upgrade))
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
        })
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
        let (stop, mut stopping) = watch::channel(false);
        let id = Uuid::new_v4();
        let sender = self.sender.clone();
        let reason = self.reason.clone();
        let mut closed = reason.subscribe();
        let realtime = self.realtime.clone();
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
                        if realtime.nats.connection_state() != async_nats::connection::State::Connected || realtime.register(&key, &member).await.is_err() {
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

async fn authenticated(
    identity: &crate::identity::Identity,
    headers: &axum::http::HeaderMap,
) -> Result<(String, Principal), crate::identity::AuthError> {
    use crate::identity::AuthError;
    let token = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or(AuthError::Unauthorized)?
        .to_owned();
    let principal = tokio::time::timeout(IO_TIMEOUT, identity.authenticate(&token))
        .await
        .map_err(|_| AuthError::Unauthorized)??;
    Ok((token, principal))
}
async fn publish_http(
    axum::extract::State((realtime, identity)): axum::extract::State<(
        Realtime,
        crate::identity::Identity,
    )>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let principal = match authenticated(&identity, &headers).await {
        Ok((_, principal)) => principal,
        Err(error) => return error.into_response(),
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
    let (token, principal) = match authenticated(&identity, &headers).await {
        Ok(auth) => auth,
        Err(error) => return error.into_response(),
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
                if !matches!(tokio::time::timeout(IO_TIMEOUT, identity.authenticate(&token)).await, Ok(Ok(ref current)) if *current == principal) {
                    break (1008, "forbidden");
                }
                auth.as_mut().reset(tokio::time::Instant::now() + Duration::from_millis(250));
            },
            frame = connection.recv() => {
                match frame {
                    Ok(Some(frame)) => {
                        let Ok(text) = serde_json::to_string(&frame) else { break (1011, "realtime_unavailable"); };
                        if bounded(socket.send(Message::Text(text.into()))).await.is_err() { break (1008, "slow_reader"); }
                    },
                    Err(reason) => break (1008, reason.as_str()),
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
                if !matches!(tokio::time::timeout(IO_TIMEOUT, identity.authenticate(&token)).await, Ok(Ok(ref current)) if *current == principal) { break (1008, "forbidden"); }
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
    let _ = connection.unsubscribe(&principal, &channel).await;
}
