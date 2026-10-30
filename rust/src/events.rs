//! PostgreSQL is the acceptance authority; accepting work never contacts the broker.
use crate::{
    identity::{Action, authorize},
    protocol::{Accepted, EventInput, MAX_BODY_BYTES, Principal, ProtocolError},
};
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use sha2::{Digest, Sha256};
use sqlx::Row;
use uuid::Uuid;

#[derive(Clone)]
pub struct Events {
    pool: sqlx::PgPool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcceptError {
    Forbidden,
    InvalidInput,
    PayloadTooLarge,
    Conflict,
    TooManyRequests,
    Internal,
}

impl IntoResponse for AcceptError {
    fn into_response(self) -> Response {
        let (status, error) = match self {
            Self::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
            Self::InvalidInput => (StatusCode::UNPROCESSABLE_ENTITY, "invalid_input"),
            Self::PayloadTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large"),
            Self::Conflict => (StatusCode::CONFLICT, "idempotency_conflict"),
            Self::TooManyRequests => (StatusCode::TOO_MANY_REQUESTS, "workspace_quota_exceeded"),
            Self::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
        };
        (status, Json(serde_json::json!({"error": error}))).into_response()
    }
}
impl std::fmt::Display for AcceptError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for AcceptError {}
impl From<sqlx::Error> for AcceptError {
    fn from(error: sqlx::Error) -> Self {
        if error
            .as_database_error()
            .is_some_and(|e| e.constraint() == Some("workspace_quota_exceeded"))
        {
            Self::TooManyRequests
        } else {
            Self::Internal
        }
    }
}

impl Events {
    pub fn new(pool: sqlx::PgPool) -> Self {
        Self { pool }
    }

    /// Apply after Identity::migrate(), before starting acceptance or workers.
    pub async fn migrate(&self) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(727365011)")
            .execute(&mut *tx)
            .await?;
        let applied: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM identity_schema_migrations WHERE version = 2)",
        )
        .fetch_one(&mut *tx)
        .await?;
        if !applied {
            sqlx::raw_sql(include_str!("../migrations/0002_events.sql"))
                .execute(&mut *tx)
                .await?;
            sqlx::query("INSERT INTO identity_schema_migrations VALUES (2)")
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await
    }

    /// The principal must come from Identity::authenticate, never the request body.
    pub async fn accept_event(
        &self,
        principal: &Principal,
        input: EventInput,
        idempotency_key: &str,
    ) -> Result<Accepted, AcceptError> {
        input.validate().map_err(|error| match error {
            ProtocolError::BodyTooLarge => AcceptError::PayloadTooLarge,
            _ => AcceptError::InvalidInput,
        })?;
        if idempotency_key.is_empty()
            || idempotency_key.len() > 256
            || idempotency_key.chars().any(char::is_control)
        {
            return Err(AcceptError::InvalidInput);
        }
        // Fixed struct fields and ordered header pairs give a deterministic representation.
        let payload = serde_json::to_vec(&input).map_err(|_| AcceptError::Internal)?;
        if payload.len() > 2 * MAX_BODY_BYTES {
            return Err(AcceptError::PayloadTooLarge);
        }
        let hash = Sha256::digest(&payload).to_vec();
        let mut tx = self.pool.begin().await?;
        let (workspace, app): (Uuid, Uuid) =
            sqlx::query_as("SELECT workspace_id, app_id FROM event_endpoints WHERE id = $1")
                .bind(input.endpoint_id)
                .fetch_optional(&mut *tx)
                .await?
                .ok_or(AcceptError::Forbidden)?;
        authorize(principal, workspace, Action::Publish { app_id: app })
            .map_err(|_| AcceptError::Forbidden)?;
        sqlx::query(
            "INSERT INTO workspace_quotas (workspace_id) VALUES ($1) ON CONFLICT DO NOTHING",
        )
        .bind(workspace)
        .execute(&mut *tx)
        .await?;
        // Serialize workspace admission and same-key races before checking either quota or receipt.
        sqlx::query("SELECT workspace_id FROM workspace_quotas WHERE workspace_id = $1 FOR UPDATE")
            .bind(workspace)
            .fetch_one(&mut *tx)
            .await?;
        if let Some(row) = sqlx::query("SELECT e.id, e.payload_hash, e.payload, d.id AS delivery_id FROM events e JOIN deliveries d ON d.event_id = e.id AND d.generation = 1 WHERE e.workspace_id = $1 AND e.app_id = $2 AND e.idempotency_key = $3")
            .bind(workspace).bind(app).bind(idempotency_key).fetch_optional(&mut *tx).await? {
            if row.get::<Vec<u8>, _>("payload_hash") != hash || row.get::<Vec<u8>, _>("payload") != payload {
                return Err(AcceptError::Conflict);
            }
            let accepted = Accepted { event_id: row.get("id"), delivery_id: row.get("delivery_id"), status: 202 };
            tx.commit().await?;
            return Ok(accepted);
        }
        let accepted = Accepted {
            event_id: Uuid::new_v4(),
            delivery_id: Uuid::new_v4(),
            status: 202,
        };
        sqlx::query("INSERT INTO events (id, workspace_id, app_id, endpoint_id, idempotency_key, payload, payload_hash, payload_bytes) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)")
            .bind(accepted.event_id).bind(workspace).bind(app).bind(input.endpoint_id).bind(idempotency_key)
            .bind(payload).bind(hash).bind(input.body.len() as i64).execute(&mut *tx).await?;
        // The database trigger charges count/bytes, or rejects and rolls back the entire transaction.
        sqlx::query(
            "INSERT INTO deliveries (id, event_id, workspace_id, app_id) VALUES ($1,$2,$3,$4)",
        )
        .bind(accepted.delivery_id)
        .bind(accepted.event_id)
        .bind(workspace)
        .bind(app)
        .execute(&mut *tx)
        .await?;
        sqlx::query(
            "INSERT INTO event_outbox (delivery_id, workspace_id, app_id) VALUES ($1,$2,$3)",
        )
        .bind(accepted.delivery_id)
        .bind(workspace)
        .bind(app)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(accepted)
    }

    /// Retention expiry is terminal, distinct from attempt exhaustion (DLQ).
    pub async fn expire_deliveries(&self, batch_size: u32) -> Result<u32, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let selected: Vec<(Uuid, Uuid)> = sqlx::query_as("SELECT d.id, d.workspace_id FROM deliveries d JOIN events e ON e.id = d.event_id WHERE d.terminal_at IS NULL AND e.expires_at <= now() ORDER BY e.expires_at, d.id LIMIT $1 FOR UPDATE OF d SKIP LOCKED")
            .bind(i64::from(batch_size.min(1000))).fetch_all(&mut *tx).await?;
        let mut workspace_ids: Vec<Uuid> = selected
            .iter()
            .map(|(_, workspace_id)| *workspace_id)
            .collect();
        workspace_ids.sort_unstable();
        workspace_ids.dedup();
        for workspace_id in workspace_ids {
            sqlx::query(
                "SELECT workspace_id FROM workspace_quotas WHERE workspace_id = $1 FOR UPDATE",
            )
            .bind(workspace_id)
            .fetch_one(&mut *tx)
            .await?;
        }
        for (id, _) in &selected {
            sqlx::query("UPDATE deliveries SET state = 'expired' WHERE id = $1")
                .bind(id)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(selected.len() as u32)
    }
}
