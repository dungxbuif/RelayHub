//! Durable response candidates are promoted by the Task 4 settlement transaction.
use crate::{
    delivery::{Deliveries, DeliveryError, ProcessingResult},
    identity::{Action, authorize},
    presence::agent_scope,
    protocol::{DeliveryLease, HttpResponse, MAX_BODY_BYTES, Principal, ProtocolError},
    webhooks::safe_headers,
};
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use sqlx::Row;
use std::time::Duration;
use uuid::Uuid;

pub(crate) const REDIS_BUDGET: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub struct Responses {
    pool: sqlx::PgPool,
    deliveries: Deliveries,
    redis: redis::aio::MultiplexedConnection,
    namespace: String,
}
#[derive(Debug)]
pub enum ResponseError {
    Delivery(DeliveryError),
    PayloadTooLarge,
    InvalidInput,
    Internal,
}
impl From<sqlx::Error> for ResponseError {
    fn from(_: sqlx::Error) -> Self {
        Self::Internal
    }
}
impl From<DeliveryError> for ResponseError {
    fn from(e: DeliveryError) -> Self {
        Self::Delivery(e)
    }
}
impl IntoResponse for ResponseError {
    fn into_response(self) -> Response {
        match self {
            Self::Delivery(e) => e.into_response(),
            Self::PayloadTooLarge => (
                StatusCode::PAYLOAD_TOO_LARGE,
                Json(serde_json::json!({"error":"payload_too_large"})),
            )
                .into_response(),
            Self::InvalidInput => (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(serde_json::json!({"error":"invalid_response"})),
            )
                .into_response(),
            Self::Internal => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error":"internal_error"})),
            )
                .into_response(),
        }
    }
}

impl Responses {
    pub fn new(
        pool: sqlx::PgPool,
        deliveries: Deliveries,
        redis: redis::aio::MultiplexedConnection,
        namespace: &str,
    ) -> Result<Self, ResponseError> {
        // Reuse the presence namespace validation; Redis keys contain no credentials or payloads.
        crate::presence::Presence::new(redis.clone(), namespace)?;
        Ok(Self {
            pool,
            deliveries,
            redis,
            namespace: namespace.to_owned(),
        })
    }
    fn key(&self, delivery: Uuid) -> String {
        format!("{}:v1:response:{delivery}", self.namespace)
    }

    /// The caller supplies an authenticated agent and the exact lease issued by Task 4.
    pub async fn complete_response(
        &self,
        agent: &Principal,
        lease: DeliveryLease,
        mut response: HttpResponse,
    ) -> Result<(), ResponseError> {
        let (_, _, agent_id) = agent_scope(agent)?;
        lease.validate().map_err(|_| ResponseError::InvalidInput)?;
        response.validate().map_err(|e| match e {
            ProtocolError::BodyTooLarge => ResponseError::PayloadTooLarge,
            _ => ResponseError::InvalidInput,
        })?;
        if response.status < 200 {
            return Err(ResponseError::InvalidInput);
        }
        response.headers = safe_headers(response.headers, false);
        let bytes = serde_json::to_vec(&response).map_err(|_| ResponseError::Internal)?;
        if bytes.len() > 2 * MAX_BODY_BYTES {
            return Err(ResponseError::PayloadTooLarge);
        }
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT d.*, e.expires_at>clock_timestamp() AND d.lease_expires_at>clock_timestamp() AND d.attempt_started_at+interval '300 seconds'>clock_timestamp() AS live FROM deliveries d JOIN events e ON e.id=d.event_id WHERE d.id=$1 FOR UPDATE OF d")
            .bind(lease.delivery_id).fetch_optional(&mut *tx).await?.ok_or(DeliveryError::Forbidden)?;
        authorize(
            agent,
            row.get("workspace_id"),
            Action::Deliver {
                app_id: row.get("app_id"),
            },
        )
        .map_err(|_| DeliveryError::Forbidden)?;
        let candidate: Option<Vec<u8>> = sqlx::query_scalar(
            "SELECT result FROM webhook_response_candidates WHERE attempt_token=$1",
        )
        .bind(lease.attempt_token)
        .fetch_optional(&mut *tx)
        .await?;
        let matching = row.get::<Uuid, _>("event_id") == lease.event_id
            && row.get::<i32, _>("generation") as u32 == lease.generation
            && row.get::<i32, _>("attempt_count") == i32::from(lease.attempt)
            && row.get::<Option<Uuid>, _>("attempt_token") == Some(lease.attempt_token)
            && row.get::<Option<Uuid>, _>("agent_id") == Some(agent_id);
        let live = matching
            && row.get::<&str, _>("state") == "in_flight"
            && row.get::<Option<bool>, _>("live") == Some(true);
        let outcome = if candidate.is_some() && matching {
            "duplicate"
        } else if live {
            "staged"
        } else {
            "rejected"
        };
        let audit = Uuid::new_v4();
        sqlx::query("INSERT INTO webhook_response_audit(id,delivery_id,attempt_token,agent_id,outcome) VALUES($1,$2,$3,$4,$5)")
            .bind(audit).bind(lease.delivery_id).bind(lease.attempt_token).bind(agent_id).bind(outcome).execute(&mut *tx).await?;
        if !live || candidate.as_ref().is_some_and(|prior| prior != &bytes) {
            tx.commit().await?;
            return Err(DeliveryError::Conflict.into());
        }
        if candidate.is_none() {
            sqlx::query("INSERT INTO webhook_response_candidates(attempt_token,audit_id,result) VALUES($1,$2,$3)")
                .bind(lease.attempt_token).bind(audit).bind(bytes).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        // A crash here can be retried with the same candidate; no delivery has been settled yet.
        if let Err(error) = self
            .deliveries
            .settle_delivery(agent, lease.clone(), ProcessingResult::Success)
            .await
        {
            sqlx::query("UPDATE webhook_response_audit SET outcome='rejected' WHERE id=$1 AND outcome='staged'")
                .bind(audit).execute(&self.pool).await?;
            return Err(error.into());
        }
        let _ = tokio::time::timeout(
            REDIS_BUDGET,
            redis::cmd("SET")
                .arg(self.key(lease.delivery_id))
                .arg("ready")
                .arg("EX")
                .arg(60)
                .query_async::<()>(&mut self.redis.clone()),
        )
        .await;
        Ok(())
    }

    pub(crate) async fn begin_wait(&self, id: Uuid) -> Result<(), ResponseError> {
        sqlx::query("UPDATE webhook_responses SET deadline_at=clock_timestamp()+(policy->>'timeout_seconds')::integer*interval '1 second' WHERE delivery_id=$1 AND state='open' AND deadline_at IS NULL AND policy->>'mode'='sync'")
            .bind(id).execute(&self.pool).await?;
        Ok(())
    }

    /// Locks in the same delivery-then-response order as settlement. Never cancels claimed work.
    pub(crate) async fn expire_wait(&self, id: Uuid) -> Result<(), ResponseError> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT id FROM deliveries WHERE id=$1 FOR UPDATE")
            .bind(id)
            .fetch_one(&mut *tx)
            .await?;
        let cancel: Option<bool> = sqlx::query_scalar("UPDATE webhook_responses SET state='timed_out' WHERE delivery_id=$1 AND state='open' AND deadline_at<=clock_timestamp() RETURNING policy->>'timeout'='expire_pending'")
            .bind(id).fetch_optional(&mut *tx).await?;
        if cancel == Some(true) {
            sqlx::query("UPDATE deliveries SET state='cancelled' WHERE id=$1 AND state='pending' AND attempt_count=0 AND terminal_at IS NULL")
                .bind(id).execute(&mut *tx).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn wait(&self, id: Uuid) -> Result<Option<HttpResponse>, ResponseError> {
        self.begin_wait(id).await?;
        loop {
            self.expire_wait(id).await?;
            let (state, result): (String, Option<Vec<u8>>) =
                sqlx::query_as("SELECT state,result FROM webhook_responses WHERE delivery_id=$1")
                    .bind(id)
                    .fetch_one(&self.pool)
                    .await?;
            if state == "answered" {
                return serde_json::from_slice(&result.ok_or(ResponseError::Internal)?)
                    .map(Some)
                    .map_err(|_| ResponseError::Internal);
            }
            if state == "timed_out" {
                return Ok(None);
            }
            // Redis only speeds discovery. Poll PostgreSQL even if the notification is lost.
            let until = tokio::time::Instant::now() + Duration::from_millis(200);
            while tokio::time::Instant::now() < until {
                let hint = tokio::time::timeout(
                    REDIS_BUDGET,
                    redis::cmd("GET")
                        .arg(self.key(id))
                        .query_async::<Option<String>>(&mut self.redis.clone()),
                )
                .await;
                if matches!(hint, Ok(Ok(Some(_)))) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }
    }
}
