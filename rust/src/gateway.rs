//! Library gateway: authenticated socket handler, reconnect scan and broker routing step.
use crate::{
    delivery::{Deliveries, DeliveryError, ProcessingResult},
    identity::{AuthError, Identity},
    presence::{Owner, Presence, agent_scope},
    protocol::{
        Frame, HEARTBEAT_SECONDS, MAX_IN_FLIGHT, PRESENCE_EXPIRY_SECONDS, Principal, SUBPROTOCOL,
        VERSION, parse_frame,
    },
    responses::{ResponseError, Responses},
};
use axum::{
    Router,
    extract::{
        State, WebSocketUpgrade,
        ws::{CloseFrame, Message, WebSocket},
    },
    http::{HeaderMap, header},
    response::Response,
    routing::get,
};
use futures_util::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use serde::Deserialize;
use sqlx::Row;
use std::{
    collections::HashMap,
    future::poll_fn,
    task::{Poll, ready},
    time::Duration,
};
use tokio::{
    sync::{Mutex, watch},
    time::Instant,
};
use uuid::Uuid;

#[derive(Clone)]
pub struct Gateway {
    id: Uuid,
    delivery: Deliveries,
    presence: Presence,
    jetstream: async_nats::jetstream::Context,
    shutdown: watch::Sender<Option<Duration>>,
    responses: Option<Responses>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Notice {
    delivery_id: Uuid,
    event_id: Uuid,
}
impl Gateway {
    /// Stop admission on every clone. The first signal wins; deadlines are 1–30,000 ms.
    /// Each socket closes after its outstanding leases settle or its advertised grace expires.
    pub fn drain(&self, deadline: Duration) -> Result<(), DeliveryError> {
        if deadline < Duration::from_millis(1) || deadline > Duration::from_secs(30) {
            return Err(DeliveryError::InvalidInput);
        }
        self.shutdown.send_if_modified(|current| {
            if current.is_some() {
                return false;
            }
            *current = Some(Duration::from_millis(deadline.as_millis() as u64));
            true
        });
        Ok(())
    }
    /// Bind the existing fenced response service before building the socket router.
    pub fn with_responses(mut self, responses: Responses) -> Self {
        self.responses = Some(responses);
        self
    }
    pub fn new(
        id: Uuid,
        delivery: Deliveries,
        presence: Presence,
        jetstream: async_nats::jetstream::Context,
    ) -> Self {
        Self {
            id,
            delivery,
            presence,
            jetstream,
            shutdown: watch::channel(None).0,
            responses: None,
        }
    }
    pub async fn connect(&self, agent: &Principal) -> Result<Uuid, DeliveryError> {
        let session = Uuid::new_v4();
        self.presence
            .register(
                agent,
                &Owner {
                    gateway_id: self.id,
                    session_id: session,
                },
            )
            .await?;
        Ok(session)
    }
    pub async fn disconnect(&self, agent: &Principal, session: Uuid) -> Result<(), DeliveryError> {
        self.presence
            .remove(
                agent,
                &Owner {
                    gateway_id: self.id,
                    session_id: session,
                },
            )
            .await
    }
    async fn owned(&self, agent: &Principal, session: Uuid) -> Result<(), DeliveryError> {
        if self.presence.owner(agent).await?
            == Some(Owner {
                gateway_id: self.id,
                session_id: session,
            })
        {
            Ok(())
        } else {
            Err(DeliveryError::Conflict)
        }
    }

    async fn rollback_frames(
        &self,
        agent: &Principal,
        frames: &[Frame],
    ) -> Result<(), DeliveryError> {
        for frame in frames {
            if let Frame::Delivery { lease, .. } = frame {
                self.delivery.rollback_claim(agent, lease).await?;
            }
        }
        Ok(())
    }
    pub async fn heartbeat(&self, agent: &Principal, session: Uuid) -> Result<(), DeliveryError> {
        self.presence
            .heartbeat(
                agent,
                &Owner {
                    gateway_id: self.id,
                    session_id: session,
                },
            )
            .await
    }
    /// A bounded reconnect/notification handler. Only an owning gateway may send these frames.
    pub async fn pending(
        &self,
        agent: &Principal,
        session: Uuid,
        limit: u16,
    ) -> Result<Vec<Frame>, DeliveryError> {
        let mut shutdown = self.shutdown.subscribe();
        let pending = self.pending_inner(agent, session, limit);
        tokio::pin!(pending);
        tokio::select! {
            result = &mut pending => result,
            _ = wait_for_drain(&mut shutdown) => {
                // Allow an ambiguous COMMIT to finish and roll back, but never retain a stuck task.
                tokio::time::timeout(Duration::from_secs(2), pending).await.unwrap_or_else(|_| {
                    // shortcut: an unobserved commit recovers through lease expiry; use durable claim intents if attempts must be refunded after DB outages.
                    tracing::warn!("drain claim cleanup timed out; remaining leases recover on expiry");
                    Err(DeliveryError::Unavailable)
                })
            }
        }
    }

    async fn pending_inner(
        &self,
        agent: &Principal,
        session: Uuid,
        limit: u16,
    ) -> Result<Vec<Frame>, DeliveryError> {
        self.owned(agent, session).await?;
        if self.shutdown.borrow().is_some() {
            return Ok(Vec::new());
        }
        let expected_owner = Owner {
            gateway_id: self.id,
            session_id: session,
        };
        let mut frames = Vec::new();
        for id in self.delivery.pending_ids(agent).await? {
            if frames.len() >= usize::from(limit.min(MAX_IN_FLIGHT)) {
                break;
            }
            if let Err(error) = self.owned(agent, session).await {
                self.rollback_frames(agent, &frames).await?;
                return Err(error);
            }
            if self.shutdown.borrow().is_some() {
                break;
            }
            match self
                .delivery
                .claim_delivery_owned(agent, id, Some(&expected_owner))
                .await
            {
                Ok(lease) => {
                    if self.shutdown.borrow().is_some() {
                        self.delivery.rollback_claim(agent, &lease).await?;
                        break;
                    }
                    if self.presence.owner(agent).await?.as_ref() != Some(&expected_owner) {
                        self.delivery.rollback_claim(agent, &lease).await?;
                        self.rollback_frames(agent, &frames).await?;
                        return Err(DeliveryError::Conflict);
                    }
                    let input = match self.delivery.input(&lease).await {
                        Ok(input) => input,
                        Err(error) => {
                            self.delivery.rollback_claim(agent, &lease).await?;
                            self.rollback_frames(agent, &frames).await?;
                            return Err(error);
                        }
                    };
                    if self.presence.owner(agent).await?.as_ref() != Some(&expected_owner) {
                        self.delivery.rollback_claim(agent, &lease).await?;
                        self.rollback_frames(agent, &frames).await?;
                        return Err(DeliveryError::Conflict);
                    }
                    frames.push(Frame::Delivery {
                        version: VERSION,
                        request_id: Uuid::new_v4(),
                        lease,
                        input,
                    });
                }
                Err(DeliveryError::Conflict) => {
                    if self.presence.owner(agent).await?.as_ref() != Some(&expected_owner) {
                        self.rollback_frames(agent, &frames).await?;
                        return Err(DeliveryError::Conflict);
                    }
                }
                Err(DeliveryError::Gone) => {}
                Err(DeliveryError::TooManyRequests) => break,
                Err(error) => {
                    self.rollback_frames(agent, &frames).await?;
                    return Err(error);
                }
            }
        }
        if let Err(error) = self.owned(agent, session).await {
            self.rollback_frames(agent, &frames).await?;
            return Err(error);
        }
        if self.shutdown.borrow().is_some() {
            self.rollback_frames(agent, &frames).await?;
            frames.clear();
        }
        Ok(frames)
    }
    /// Subscribe before routing messages; the host feeds notices to its owning socket sessions.
    /// pending() also provides DB recovery when notifications or ephemeral ownership are lost.
    pub async fn notifications(&self) -> Result<async_nats::Subscriber, DeliveryError> {
        let subscriber = self
            .jetstream
            .client()
            .subscribe(self.presence.subject(self.id))
            .await
            .map_err(|_| DeliveryError::Unavailable)?;
        self.jetstream
            .client()
            .flush()
            .await
            .map_err(|_| DeliveryError::Unavailable)?;
        Ok(subscriber)
    }
    /// One JetStream consumer step. Only committed terminal state permits a broker ACK.
    /// Nonterminal messages are delayed, never ACKed merely because a gateway was notified.
    pub async fn reconcile_message(
        &self,
        message: &async_nats::jetstream::Message,
    ) -> Result<bool, DeliveryError> {
        let notice: Notice =
            serde_json::from_slice(&message.payload).map_err(|_| DeliveryError::InvalidInput)?;
        let row = sqlx::query(
            "SELECT d.workspace_id,d.target_app_id,d.event_id FROM deliveries d WHERE d.id=$1",
        )
        .bind(notice.delivery_id)
        .fetch_optional(&self.delivery.pool)
        .await?
        .ok_or(DeliveryError::Forbidden)?;
        let workspace: Uuid = row.get("workspace_id");
        let app: Uuid = row.get("target_app_id");
        if row.get::<Uuid, _>("event_id") != notice.event_id
            || message.subject.as_str() != self.presence.delivery_subject(workspace, app)
        {
            return Err(DeliveryError::Forbidden);
        }
        if self.delivery.reconcile(notice.delivery_id).await? {
            message
                .double_ack()
                .await
                .map_err(|_| DeliveryError::Unavailable)?;
            return Ok(true);
        }
        let mut gateways = Vec::new();
        for owner in self.presence.owners(workspace, app).await? {
            if !gateways.contains(&owner.gateway_id) {
                self.jetstream
                    .client()
                    .publish(
                        self.presence.subject(owner.gateway_id),
                        message.payload.clone(),
                    )
                    .await
                    .map_err(|_| DeliveryError::Unavailable)?;
                gateways.push(owner.gateway_id);
            }
        }
        self.jetstream
            .client()
            .flush()
            .await
            .map_err(|_| DeliveryError::Unavailable)?;
        message
            .ack_with(async_nats::jetstream::AckKind::Nak(Some(
                Duration::from_secs(1),
            )))
            .await
            .map_err(|_| DeliveryError::Unavailable)?;
        Ok(false)
    }
    /// Mount instead of Task 2's auth-only /socket route. Read/replay remain Principal APIs.
    pub fn router(&self, identity: Identity) -> Router {
        Router::new()
            .route("/api/v1/socket", get(upgrade))
            .with_state((self.clone(), identity))
    }
    async fn socket(&self, socket: WebSocket, identity: Identity, token: String, agent: Principal) {
        let (sink, mut source) = socket.split();
        let sink = Mutex::new(sink);
        let mut session = None;
        let mut shutdown = self.shutdown.subscribe();
        let mut drain_deadline = None;
        let result = {
            let work =
                self.socket_loop(&sink, &mut source, &identity, &token, &agent, &mut session);
            tokio::pin!(work);
            tokio::select! {
                biased;
                grace = wait_for_drain(&mut shutdown) => {
                    let until = Instant::now() + grace;
                    drain_deadline = Some(until);
                    let notice = tokio::time::timeout_at(until, Self::send_drain(&sink, grace));
                    tokio::pin!(notice);
                    // Keep work polled while Drain waits for a writer already flushing a frame.
                    let (notice, finished) = tokio::select! {
                        result = &mut work => (notice.await, Some(result)),
                        result = &mut notice => (result, None),
                    };
                    match notice {
                        Ok(Ok(())) => match finished {
                            Some(result) => result,
                            None => tokio::time::timeout_at(until, work).await.unwrap_or(Ok(())),
                        },
                        _ => Err(DeliveryError::Unavailable),
                    }
                },
                result = &mut work => result,
            }
        };
        let code = if result.is_ok() { 1000 } else { 1008 };
        let _ = tokio::time::timeout_at(
            drain_deadline.unwrap_or_else(|| Instant::now() + Duration::from_secs(2)),
            async {
                sink.lock()
                    .await
                    .send(Message::Close(Some(CloseFrame {
                        code,
                        reason: "session ended".into(),
                    })))
                    .await
            },
        )
        .await;
        // Close must not wait for Redis. Removal still compares the exact gateway/session owner.
        if let Some(session) = session {
            let _ = tokio::time::timeout(Duration::from_secs(2), self.disconnect(&agent, session))
                .await;
        }
    }
    async fn send_drain(
        sink: &Mutex<SplitSink<WebSocket, Message>>,
        deadline: Duration,
    ) -> Result<(), DeliveryError> {
        let frame = Frame::Drain {
            version: VERSION,
            request_id: Uuid::new_v4(),
            deadline_ms: deadline.as_millis() as u32,
        };
        let text = serde_json::to_string(&frame).map_err(|_| DeliveryError::Internal)?;
        sink.lock()
            .await
            .send(Message::Text(text.into()))
            .await
            .map_err(|_| DeliveryError::Unavailable)
    }

    async fn send_delivery(
        &self,
        socket: &mut SplitSink<WebSocket, Message>,
        frame: &Frame,
    ) -> Result<bool, DeliveryError> {
        let text = serde_json::to_string(frame).map_err(|_| DeliveryError::Internal)?;
        let admitted = poll_fn(|cx| -> Poll<Result<bool, DeliveryError>> {
            // Serialize sink admission with drain's write lock, without holding a guard across await.
            let shutdown = self.shutdown.borrow();
            if shutdown.is_some() {
                return Poll::Ready(Ok(false));
            }
            ready!(socket.poll_ready_unpin(cx)).map_err(|_| DeliveryError::Unavailable)?;
            socket
                .start_send_unpin(Message::Text(text.clone().into()))
                .map_err(|_| DeliveryError::Unavailable)?;
            Poll::Ready(Ok(true))
        })
        .await?;
        if admitted {
            socket
                .flush()
                .await
                .map_err(|_| DeliveryError::Unavailable)?;
        }
        Ok(admitted)
    }

    async fn socket_loop(
        &self,
        sink: &Mutex<SplitSink<WebSocket, Message>>,
        source: &mut SplitStream<WebSocket>,
        identity: &Identity,
        token: &str,
        agent: &Principal,
        session: &mut Option<Uuid>,
    ) -> Result<(), DeliveryError> {
        let mut shutdown = self.shutdown.subscribe();
        async {
            let (_, _, agent_id) = agent_scope(agent)?;
            let refresh = || async {
                tokio::time::sleep(Duration::from_millis(250)).await;
                identity.authenticate(token).await.map_err(|_| DeliveryError::Forbidden)
            };
            let auth = refresh();
            tokio::pin!(auth);
            let hello_deadline = tokio::time::sleep(Duration::from_secs(HEARTBEAT_SECONDS));
            tokio::pin!(hello_deadline);
            let first = loop {
                tokio::select! {
                    _ = wait_for_drain(&mut shutdown) => return Ok(()),
                    _ = &mut hello_deadline => return Err(DeliveryError::InvalidInput),
                    principal = &mut auth => {
                        if principal? != *agent { return Err(DeliveryError::Forbidden); }
                        auth.set(refresh());
                    },
                    message = source.next() => break message.ok_or(DeliveryError::InvalidInput)?.map_err(|_| DeliveryError::InvalidInput)?,
                }
            };
            let Frame::Hello { agent_id: hello_agent, max_in_flight, .. } = decode(first)? else {
                return Err(DeliveryError::InvalidInput);
            };
            if hello_agent != agent_id || identity.authenticate(token).await.map_err(|_| DeliveryError::Forbidden)? != *agent {
                return Err(DeliveryError::Forbidden);
            }
            let sid = self.connect(agent).await?;
            *session = Some(sid);
            let mut outstanding = HashMap::new();
            let mut scan = tokio::time::interval(Duration::from_secs(1));
            let mut last_heartbeat = Instant::now();
            let mut draining = false;
            loop {
                if draining && outstanding.is_empty() {
                    return Ok(());
                }
                let step = async {
                    tokio::select! {
                        _ = wait_for_drain(&mut shutdown), if !draining => {
                            draining = true;
                        },
                        principal = &mut auth => {
                            if principal? != *agent { return Err(DeliveryError::Forbidden); }
                            auth.set(refresh());
                            if last_heartbeat.elapsed() >= Duration::from_secs(PRESENCE_EXPIRY_SECONDS) {
                                return Err(DeliveryError::Unavailable);
                            }
                        },
                        _ = scan.tick() => {
                            self.owned(agent, sid).await?;
                            // Expired requests must stop occupying the socket's advertised capacity.
                            let live: Vec<Uuid> = sqlx::query_scalar("SELECT attempt_token FROM deliveries WHERE state='in_flight' AND attempt_token=ANY($1) AND lease_expires_at>clock_timestamp()")
                                .bind(outstanding.values().map(|l: &crate::protocol::DeliveryLease| l.attempt_token).collect::<Vec<_>>()).fetch_all(&self.delivery.pool).await?;
                            outstanding.retain(|_, lease| live.contains(&lease.attempt_token));
                            if !draining {
                                let gateway = self.clone();
                                let principal = agent.clone();
                                let limit = max_in_flight.saturating_sub(outstanding.len() as u16);
                                let mut pending = Box::pin(async move { gateway.pending(&principal, sid, limit).await });
                                let frames = tokio::select! {
                                    biased;
                                    _ = wait_for_drain(&mut shutdown) => {
                                        // Finish claims rather than cancel a transaction whose commit may have succeeded.
                                        let gateway = self.clone();
                                        let principal = agent.clone();
                                        tokio::spawn(async move {
                                            let result = tokio::time::timeout(Duration::from_secs(2), async {
                                                let frames = pending.await?;
                                                gateway.rollback_frames(&principal, &frames).await
                                            }).await;
                                            if !matches!(result, Ok(Ok(()))) {
                                                tracing::warn!(?result, "drain claim cleanup incomplete; remaining leases recover on expiry");
                                            }
                                        });
                                        draining = true;
                                        return Ok(false);
                                    },
                                    frames = &mut pending => frames?,
                                };
                                for (index, frame) in frames.iter().enumerate() {
                                    if let Err(error) = self.owned(agent, sid).await {
                                        self.rollback_frames(agent, &frames[index..]).await?;
                                        return Err(error);
                                    }
                                    let sent = tokio::time::timeout(Duration::from_secs(2), async {
                                        self.send_delivery(&mut *sink.lock().await, frame).await
                                    })
                                        .await.unwrap_or(Err(DeliveryError::Unavailable));
                                    match sent {
                                        Ok(true) => {
                                            if let Frame::Delivery { request_id, lease, .. } = frame {
                                                outstanding.insert(*request_id, lease.clone());
                                            }
                                        },
                                        Ok(false) => {
                                            self.rollback_frames(agent, &frames[index..]).await?;
                                            break;
                                        },
                                        Err(error) => {
                                            // The current frame may have reached the peer; only undo later claims.
                                            self.rollback_frames(agent, &frames[index + 1..]).await?;
                                            return Err(error);
                                        },
                                    }
                                }
                            }
                        },
                        message = source.next() => {
                            let Some(message) = message else { return Ok(true) };
                            let message = message.map_err(|_| DeliveryError::Unavailable)?;
                            if matches!(message, Message::Close(_)) { return Ok(true) }
                            if matches!(message, Message::Ping(_) | Message::Pong(_)) { return Ok(false) }
                            self.owned(agent, sid).await?;
                            let frame = decode(message)?;
                            match frame {
                                Frame::Ack { request_id, event_id, delivery_id, attempt_token, .. }
                                | Frame::Response { request_id, event_id, delivery_id, attempt_token, .. } => {
                                    let lease = outstanding.get(&request_id).ok_or(DeliveryError::Conflict)?;
                                    if (lease.event_id, lease.delivery_id, lease.attempt_token) != (event_id, delivery_id, attempt_token) {
                                        return Err(DeliveryError::Conflict);
                                    }
                                    if let Frame::Response { response, .. } = frame {
                                        self.responses.as_ref().ok_or(DeliveryError::InvalidInput)?
                                            .complete_response(agent, lease.clone(), response).await
                                            .map_err(|error| match error {
                                                ResponseError::Delivery(error) => error,
                                                ResponseError::InvalidInput | ResponseError::PayloadTooLarge => DeliveryError::InvalidInput,
                                                ResponseError::Internal => DeliveryError::Internal,
                                            })?;
                                    } else {
                                        self.delivery.settle_delivery(agent, lease.clone(), ProcessingResult::Success).await?;
                                    }
                                    outstanding.remove(&request_id);
                                },
                                Frame::Heartbeat { lease, .. } => {
                                    self.heartbeat(agent, sid).await?;
                                    if let Some(lease) = lease {
                                        if !outstanding.values().any(|v| v == &lease) { return Err(DeliveryError::Conflict) }
                                        self.delivery.heartbeat(agent, &lease).await?;
                                    }
                                    last_heartbeat = Instant::now();
                                },
                                _ => return Err(DeliveryError::InvalidInput),
                            }
                        }
                    }
                    Ok(false)
                };
                let closed = step.await?;
                if closed { return Ok(()) }
            }
        }.await
    }
}
async fn wait_for_drain(shutdown: &mut watch::Receiver<Option<Duration>>) -> Duration {
    loop {
        if let Some(deadline) = *shutdown.borrow_and_update() {
            return deadline;
        }
        // The socket owns a Gateway clone, so the sender stays alive.
        shutdown.changed().await.expect("gateway shutdown sender");
    }
}
fn decode(message: Message) -> Result<Frame, DeliveryError> {
    match message {
        Message::Text(s) => parse_frame(s.as_bytes()),
        Message::Binary(b) => parse_frame(&b),
        _ => return Err(DeliveryError::InvalidInput),
    }
    .map_err(|_| DeliveryError::InvalidInput)
}
async fn upgrade(
    State((gateway, identity)): State<(Gateway, Identity)>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> Result<Response, AuthError> {
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(AuthError::Unauthorized)?
        .to_owned();
    let principal = identity.authenticate(&token).await?;
    agent_scope(&principal).map_err(|_| AuthError::Forbidden)?;
    if !headers
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.split(',').any(|s| s.trim() == SUBPROTOCOL))
    {
        return Err(AuthError::InvalidInput);
    }
    Ok(ws
        .protocols([SUBPROTOCOL])
        .max_message_size(2 * crate::protocol::MAX_BODY_BYTES)
        .on_upgrade(move |socket| async move {
            gateway.socket(socket, identity, token, principal).await
        }))
}
