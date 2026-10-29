//! Library gateway: authenticated socket handler, reconnect scan and broker routing step.
use crate::{
    delivery::{Deliveries, DeliveryError, ProcessingResult},
    identity::{AuthError, Identity},
    presence::{Owner, Presence, agent_scope},
    protocol::{
        Frame, HEARTBEAT_SECONDS, MAX_IN_FLIGHT, PRESENCE_EXPIRY_SECONDS, Principal, SUBPROTOCOL,
        VERSION, parse_frame,
    },
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
use serde::Deserialize;
use sqlx::Row;
use std::{collections::HashMap, time::Duration};
use uuid::Uuid;

#[derive(Clone)]
pub struct Gateway {
    id: Uuid,
    delivery: Deliveries,
    presence: Presence,
    jetstream: async_nats::jetstream::Context,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Notice {
    delivery_id: Uuid,
    event_id: Uuid,
}
impl Gateway {
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
        self.owned(agent, session).await?;
        let mut frames = Vec::new();
        for id in self.delivery.pending_ids(agent).await? {
            if frames.len() >= usize::from(limit.min(MAX_IN_FLIGHT)) {
                break;
            }
            self.owned(agent, session).await?;
            match self.delivery.claim_delivery(agent, id).await {
                Ok(lease) => {
                    let input = self.delivery.input(&lease).await?;
                    frames.push(Frame::Delivery {
                        version: VERSION,
                        request_id: Uuid::new_v4(),
                        lease,
                        input,
                    });
                }
                Err(DeliveryError::Conflict | DeliveryError::Gone) => {}
                Err(DeliveryError::TooManyRequests) => break,
                Err(error) => return Err(error),
            }
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
            "SELECT d.workspace_id,d.app_id,d.event_id FROM deliveries d WHERE d.id=$1",
        )
        .bind(notice.delivery_id)
        .fetch_optional(&self.delivery.pool)
        .await?
        .ok_or(DeliveryError::Forbidden)?;
        let workspace: Uuid = row.get("workspace_id");
        let app: Uuid = row.get("app_id");
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
    async fn socket(
        &self,
        mut socket: WebSocket,
        identity: Identity,
        token: String,
        agent: Principal,
    ) {
        let session_result = self
            .socket_loop(&mut socket, &identity, &token, &agent)
            .await;
        if let Some(session) = session_result.0 {
            let _ = self.disconnect(&agent, session).await;
        }
        let code = if session_result.1.is_ok() { 1000 } else { 1008 };
        let _ = tokio::time::timeout(
            Duration::from_secs(2),
            socket.send(Message::Close(Some(CloseFrame {
                code,
                reason: "session ended".into(),
            }))),
        )
        .await;
    }
    async fn socket_loop(
        &self,
        socket: &mut WebSocket,
        identity: &Identity,
        token: &str,
        agent: &Principal,
    ) -> (Option<Uuid>, Result<(), DeliveryError>) {
        let mut session = None;
        let result=async {
            let (_,_,agent_id)=agent_scope(agent)?;
            let mut auth=tokio::time::interval(Duration::from_millis(250));
            let hello_deadline=tokio::time::sleep(Duration::from_secs(HEARTBEAT_SECONDS));
            tokio::pin!(hello_deadline);
            let first=loop {
                tokio::select! {
                    _=&mut hello_deadline=>return Err(DeliveryError::InvalidInput),
                    _=auth.tick()=>{
                        if identity.authenticate(token).await.map_err(|_|DeliveryError::Forbidden)?!=*agent {return Err(DeliveryError::Forbidden);}
                    },
                    message=socket.recv()=>break message.ok_or(DeliveryError::InvalidInput)?.map_err(|_|DeliveryError::InvalidInput)?,
                }
            };
            let Frame::Hello {agent_id:hello_agent,max_in_flight,..}=decode(first)? else {return Err(DeliveryError::InvalidInput)};
            if hello_agent!=agent_id || identity.authenticate(token).await.map_err(|_|DeliveryError::Forbidden)?!=*agent {return Err(DeliveryError::Forbidden);}
            let sid=self.connect(agent).await?; session=Some(sid);
            let mut outstanding=HashMap::new();
            let mut scan=tokio::time::interval(Duration::from_secs(1));
            let mut last_heartbeat=tokio::time::Instant::now();
            loop {
                tokio::select! {
                    _=auth.tick()=>{
                        if identity.authenticate(token).await.map_err(|_|DeliveryError::Forbidden)?!=*agent {return Err(DeliveryError::Forbidden);}
                        if last_heartbeat.elapsed()>=Duration::from_secs(PRESENCE_EXPIRY_SECONDS) {return Err(DeliveryError::Unavailable);}
                    },
                    _=scan.tick()=>{
                        self.owned(agent,sid).await?;
                        // Expired requests must stop occupying the socket's advertised capacity.
                        let live:Vec<Uuid>=sqlx::query_scalar("SELECT attempt_token FROM deliveries WHERE state='in_flight' AND attempt_token=ANY($1) AND lease_expires_at>clock_timestamp()")
                            .bind(outstanding.values().map(|l:&crate::protocol::DeliveryLease|l.attempt_token).collect::<Vec<_>>()).fetch_all(&self.delivery.pool).await?;
                        outstanding.retain(|_,lease|live.contains(&lease.attempt_token));
                        for frame in self.pending(agent,sid,max_in_flight.saturating_sub(outstanding.len() as u16)).await? {
                            if let Frame::Delivery {request_id,lease,..}=&frame {outstanding.insert(*request_id,lease.clone());}
                            let text=serde_json::to_string(&frame).map_err(|_|DeliveryError::Internal)?;
                            tokio::time::timeout(Duration::from_secs(2),socket.send(Message::Text(text.into()))).await.map_err(|_|DeliveryError::Unavailable)?.map_err(|_|DeliveryError::Unavailable)?;
                        }
                    },
                    message=socket.recv()=>{
                        let Some(message)=message else {return Ok(())};
                        let message=message.map_err(|_|DeliveryError::Unavailable)?;
                        if matches!(message,Message::Close(_)) {return Ok(())}
                        if matches!(message,Message::Ping(_)|Message::Pong(_)) {continue;}
                        self.owned(agent,sid).await?;
                        match decode(message)? {
                            Frame::Ack {request_id,event_id,delivery_id,attempt_token,..}=>{
                                let lease=outstanding.get(&request_id).ok_or(DeliveryError::Conflict)?;
                                if (lease.event_id,lease.delivery_id,lease.attempt_token)!=(event_id,delivery_id,attempt_token) {return Err(DeliveryError::Conflict);}
                                self.delivery.settle_delivery(agent,lease.clone(),ProcessingResult::Success).await?;
                                outstanding.remove(&request_id);
                            },
                            Frame::Heartbeat {lease,..}=>{
                                self.heartbeat(agent,sid).await?;
                                if let Some(lease)=lease {
                                    if !outstanding.values().any(|v|v==&lease) {return Err(DeliveryError::Conflict);}
                                    self.delivery.heartbeat(agent,&lease).await?;
                                }
                                last_heartbeat=tokio::time::Instant::now();
                            },
                            _=>return Err(DeliveryError::InvalidInput),
                        }
                    }
                }
            }
        }.await;
        (session, result)
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
