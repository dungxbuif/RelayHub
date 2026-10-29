//! Fenced PostgreSQL attempts over Task 3 deliveries; Redis is only a liveness hint.
use crate::{
    identity::{Action, authorize},
    presence::{Owner, Presence, agent_scope},
    protocol::{
        Accepted, DeliveryLease, EventInput, LEASE_SECONDS, MAX_ATTEMPT_RUNTIME_SECONDS,
        MAX_ATTEMPTS, MAX_IN_FLIGHT, Principal, RETRY_DELAYS_SECONDS,
    },
};
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use sqlx::{Row, postgres::PgRow};
use uuid::Uuid;

#[derive(Clone)]
pub struct Deliveries {
    pub(crate) pool: sqlx::PgPool,
    presence: Presence,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryError {
    Forbidden,
    InvalidInput,
    Conflict,
    Gone,
    TooManyRequests,
    Unavailable,
    Internal,
}
pub type ClaimError = DeliveryError;
pub type SettleError = DeliveryError;
impl IntoResponse for DeliveryError {
    fn into_response(self) -> Response {
        let (status, error) = match self {
            Self::Forbidden => (StatusCode::FORBIDDEN, "forbidden"),
            Self::InvalidInput => (StatusCode::UNPROCESSABLE_ENTITY, "invalid_input"),
            Self::Conflict => (StatusCode::CONFLICT, "stale_or_busy_delivery"),
            Self::Gone => (StatusCode::GONE, "terminal_delivery"),
            Self::TooManyRequests => (StatusCode::TOO_MANY_REQUESTS, "delivery_limit"),
            Self::Unavailable => (StatusCode::SERVICE_UNAVAILABLE, "delivery_unavailable"),
            Self::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
        };
        (status, Json(serde_json::json!({"error":error}))).into_response()
    }
}
impl std::fmt::Display for DeliveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for DeliveryError {}
impl From<sqlx::Error> for DeliveryError {
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
#[derive(Debug, Clone, Copy)]
pub enum ProcessingResult {
    Success,
    RetryableFailure,
}
#[derive(Debug, Serialize)]
pub struct DeliveryView {
    pub delivery_id: Uuid,
    pub event_id: Uuid,
    pub generation: i32,
    pub state: String,
    pub attempts: i32,
}

fn authorize_row(principal: &Principal, row: &PgRow, action: Action) -> Result<(), DeliveryError> {
    authorize(principal, row.get("workspace_id"), action).map_err(|_| DeliveryError::Forbidden)
}
fn lease_matches(row: &PgRow, lease: &DeliveryLease, agent_id: Uuid) -> bool {
    row.get::<&str, _>("state") == "in_flight"
        && row.get::<Uuid, _>("event_id") == lease.event_id
        && row.get::<i32, _>("generation") as u32 == lease.generation
        && row.get::<i32, _>("attempt_count") == i32::from(lease.attempt)
        && row.get::<Option<Uuid>, _>("attempt_token") == Some(lease.attempt_token)
        && row.get::<Option<Uuid>, _>("agent_id") == Some(agent_id)
        && row.get::<bool, _>("lease_live")
        && row.get::<bool, _>("retained")
}
const LOCK_DELIVERY: &str = "SELECT d.*, d.terminal_at IS NOT NULL AS terminal, e.expires_at>clock_timestamp() AS retained, d.lease_expires_at>clock_timestamp() AND d.attempt_started_at+interval '300 seconds'>clock_timestamp() AS lease_live, d.next_attempt_at IS NULL OR d.next_attempt_at<=clock_timestamp() AS due FROM deliveries d JOIN events e ON e.id=d.event_id WHERE d.id=$1 FOR UPDATE OF d";

impl Deliveries {
    pub fn new(pool: sqlx::PgPool, presence: Presence) -> Self {
        Self { pool, presence }
    }
    /// Apply after Identity::migrate and Events::migrate. Does not alter acceptance accounting.
    pub async fn migrate(&self) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(727365011)")
            .execute(&mut *tx)
            .await?;
        let applied: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM identity_schema_migrations WHERE version=3)",
        )
        .fetch_one(&mut *tx)
        .await?;
        if !applied {
            sqlx::raw_sql(include_str!("../migrations/0003_delivery.sql"))
                .execute(&mut *tx)
                .await?;
            sqlx::query("INSERT INTO identity_schema_migrations VALUES(3)")
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await
    }
    /// Principal must be authenticated by Task 2. No workspace supplied by a request is trusted.
    pub async fn claim_delivery(
        &self,
        agent: &Principal,
        delivery_id: Uuid,
    ) -> Result<DeliveryLease, ClaimError> {
        self.claim_delivery_owned(agent, delivery_id, None).await
    }

    pub(crate) async fn claim_delivery_owned(
        &self,
        agent: &Principal,
        delivery_id: Uuid,
        expected_owner: Option<&Owner>,
    ) -> Result<DeliveryLease, ClaimError> {
        let (workspace, app, agent_id) = agent_scope(agent)?;
        let mut tx = self.pool.begin().await?;
        // Serialize all claims for this agent, including claims made through different gateways.
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,4))")
            .bind(format!("{workspace}:{app}:{agent_id}"))
            .execute(&mut *tx)
            .await?;
        let row = sqlx::query(LOCK_DELIVERY)
            .bind(delivery_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DeliveryError::Forbidden)?;
        authorize_row(
            agent,
            &row,
            Action::Deliver {
                app_id: row.get("app_id"),
            },
        )?;
        if let Some(expected_owner) = expected_owner
            && self.presence.owner(agent).await?.as_ref() != Some(expected_owner)
        {
            return Err(DeliveryError::Conflict);
        }
        if row.get::<bool, _>("terminal") {
            return Err(DeliveryError::Gone);
        }
        if !row.get::<bool, _>("retained") {
            sqlx::query("UPDATE deliveries SET state='expired' WHERE id=$1")
                .bind(delivery_id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            return Err(DeliveryError::Gone);
        }
        let count: i32 = row.get("attempt_count");
        if row.get::<&str, _>("state") == "in_flight" {
            if row.get::<bool, _>("lease_live") {
                return Err(DeliveryError::Conflict);
            }
            finish_attempt(&mut tx, delivery_id, count, "lease_expired", false).await?;
            tx.commit().await?;
            return Err(if count >= i32::from(MAX_ATTEMPTS) {
                DeliveryError::Gone
            } else {
                DeliveryError::Conflict
            });
        }
        if !row.get::<bool, _>("due") {
            return Err(DeliveryError::Conflict);
        }
        if self.presence.owner(agent).await?.is_none() {
            return Err(DeliveryError::Unavailable);
        }
        let in_flight:i64=sqlx::query_scalar("SELECT count(*) FROM deliveries WHERE workspace_id=$1 AND app_id=$2 AND agent_id=$3 AND state='in_flight' AND lease_expires_at>clock_timestamp() AND attempt_started_at+interval '300 seconds'>clock_timestamp()")
            .bind(workspace).bind(app).bind(agent_id).fetch_one(&mut *tx).await?;
        if in_flight >= i64::from(MAX_IN_FLIGHT) {
            return Err(DeliveryError::TooManyRequests);
        }
        let lease = DeliveryLease {
            delivery_id,
            event_id: row.get("event_id"),
            generation: row.get::<i32, _>("generation") as u32,
            attempt: (count + 1) as u8,
            attempt_token: Uuid::new_v4(),
        };
        let updated=sqlx::query("UPDATE deliveries SET state='in_flight', attempt_count=attempt_count+1, attempt_token=$2, agent_id=$3, attempt_started_at=clock_timestamp(), lease_expires_at=LEAST(clock_timestamp()+$4*interval '1 second',(SELECT expires_at FROM events WHERE id=deliveries.event_id)), next_attempt_at=NULL WHERE id=$1 AND attempt_count=$5 AND state IN ('pending','retry_wait') AND EXISTS(SELECT 1 FROM events WHERE id=deliveries.event_id AND expires_at>clock_timestamp())")
            .bind(delivery_id).bind(lease.attempt_token).bind(agent_id).bind(LEASE_SECONDS as f64).bind(count).execute(&mut *tx).await?;
        if updated.rows_affected() != 1 {
            return Err(DeliveryError::Conflict);
        }
        sqlx::query("INSERT INTO delivery_attempts(delivery_id,attempt,attempt_token,agent_id,started_at,lease_expires_at) SELECT id,attempt_count,attempt_token,agent_id,attempt_started_at,lease_expires_at FROM deliveries WHERE id=$1")
            .bind(delivery_id).execute(&mut *tx).await?;
        if let Some(expected_owner) = expected_owner
            && self.presence.owner(agent).await?.as_ref() != Some(expected_owner)
        {
            return Err(DeliveryError::Conflict);
        }
        tx.commit().await?;
        Ok(lease)
    }

    pub(crate) async fn rollback_claim(
        &self,
        agent: &Principal,
        lease: &DeliveryLease,
    ) -> Result<(), DeliveryError> {
        let (_, _, agent_id) = agent_scope(agent)?;
        let mut tx = self.pool.begin().await?;
        let deleted = sqlx::query(
            "DELETE FROM delivery_attempts WHERE delivery_id=$1 AND attempt=$2 AND attempt_token=$3 AND agent_id=$4 AND finished_at IS NULL",
        )
        .bind(lease.delivery_id)
        .bind(i32::from(lease.attempt))
        .bind(lease.attempt_token)
        .bind(agent_id)
        .execute(&mut *tx)
        .await?;
        if deleted.rows_affected() != 1 {
            return Err(DeliveryError::Conflict);
        }
        let reset = sqlx::query(
            "UPDATE deliveries SET state='pending', attempt_count=attempt_count-1, attempt_token=NULL, agent_id=NULL, attempt_started_at=NULL, lease_expires_at=NULL, next_attempt_at=NULL WHERE id=$1 AND state='in_flight' AND attempt_count=$2 AND attempt_token=$3 AND agent_id=$4",
        )
        .bind(lease.delivery_id)
        .bind(i32::from(lease.attempt))
        .bind(lease.attempt_token)
        .bind(agent_id)
        .execute(&mut *tx)
        .await?;
        if reset.rows_affected() != 1 {
            return Err(DeliveryError::Conflict);
        }
        tx.commit().await?;
        Ok(())
    }
    pub async fn settle_delivery(
        &self,
        agent: &Principal,
        lease: DeliveryLease,
        result: ProcessingResult,
    ) -> Result<(), SettleError> {
        let (_, _, agent_id) = agent_scope(agent)?;
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(LOCK_DELIVERY)
            .bind(lease.delivery_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DeliveryError::Forbidden)?;
        authorize_row(
            agent,
            &row,
            Action::Deliver {
                app_id: row.get("app_id"),
            },
        )?;
        if !lease_matches(&row, &lease, agent_id) {
            return Err(DeliveryError::Conflict);
        }
        let success = matches!(result, ProcessingResult::Success);
        finish_attempt(
            &mut tx,
            lease.delivery_id,
            i32::from(lease.attempt),
            if success {
                "success"
            } else {
                "retryable_failure"
            },
            success,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
    pub async fn heartbeat(
        &self,
        agent: &Principal,
        lease: &DeliveryLease,
    ) -> Result<(), DeliveryError> {
        let (_, _, agent_id) = agent_scope(agent)?;
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(LOCK_DELIVERY)
            .bind(lease.delivery_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DeliveryError::Forbidden)?;
        authorize_row(
            agent,
            &row,
            Action::Deliver {
                app_id: row.get("app_id"),
            },
        )?;
        if !lease_matches(&row, lease, agent_id) {
            return Err(DeliveryError::Conflict);
        }
        let updated=sqlx::query("UPDATE deliveries SET lease_expires_at=LEAST(clock_timestamp()+$2*interval '1 second',attempt_started_at+$3*interval '1 second',(SELECT expires_at FROM events WHERE id=deliveries.event_id)) WHERE id=$1 AND state='in_flight' AND lease_expires_at>clock_timestamp() AND attempt_started_at+$3*interval '1 second'>clock_timestamp() AND EXISTS(SELECT 1 FROM events WHERE id=deliveries.event_id AND expires_at>clock_timestamp())")
            .bind(lease.delivery_id).bind(LEASE_SECONDS as f64).bind(MAX_ATTEMPT_RUNTIME_SECONDS as f64).execute(&mut *tx).await?;
        if updated.rows_affected() != 1 {
            return Err(DeliveryError::Conflict);
        }
        sqlx::query("UPDATE delivery_attempts a SET lease_expires_at=d.lease_expires_at FROM deliveries d WHERE a.delivery_id=d.id AND a.attempt=d.attempt_count AND d.id=$1")
            .bind(lease.delivery_id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
    /// Narrow read handler for an authenticated route adapter; IDs only locate stored scope.
    pub async fn read_delivery(
        &self,
        principal: &Principal,
        id: Uuid,
    ) -> Result<DeliveryView, DeliveryError> {
        let row = sqlx::query("SELECT * FROM deliveries WHERE id=$1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await?
            .ok_or(DeliveryError::Forbidden)?;
        let action = if matches!(principal, Principal::Application { .. }) {
            Action::Publish {
                app_id: row.get("app_id"),
            }
        } else {
            Action::Deliver {
                app_id: row.get("app_id"),
            }
        };
        authorize_row(principal, &row, action)?;
        Ok(DeliveryView {
            delivery_id: id,
            event_id: row.get("event_id"),
            generation: row.get("generation"),
            state: row.get("state"),
            attempts: row.get("attempt_count"),
        })
    }
    /// Owner/admin-only replay. Retains event retention and immutable terminal generations.
    pub async fn replay_delivery(
        &self,
        principal: &Principal,
        id: Uuid,
    ) -> Result<Accepted, DeliveryError> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT * FROM deliveries WHERE id=$1")
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DeliveryError::Forbidden)?;
        authorize_row(principal, &row, Action::ManageApps)?;
        let actor = match principal {
            Principal::SystemAdmin { user_id } | Principal::WorkspaceOwner { user_id, .. } => {
                *user_id
            }
            _ => return Err(DeliveryError::Forbidden),
        };
        let event: Uuid = row.get("event_id");
        // Serialize generation allocation on the event, without locking a quota row before a delivery.
        let retained: bool = sqlx::query_scalar(
            "SELECT expires_at>clock_timestamp() FROM events WHERE id=$1 FOR NO KEY UPDATE",
        )
        .bind(event)
        .fetch_one(&mut *tx)
        .await?;
        if !retained {
            return Err(DeliveryError::Gone);
        }
        let active: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM deliveries WHERE event_id=$1 AND terminal_at IS NULL)",
        )
        .bind(event)
        .fetch_one(&mut *tx)
        .await?;
        if active {
            return Err(DeliveryError::Conflict);
        }
        let next: i32 =
            sqlx::query_scalar("SELECT max(generation)+1 FROM deliveries WHERE event_id=$1")
                .bind(event)
                .fetch_one(&mut *tx)
                .await?;
        let accepted = Accepted {
            event_id: event,
            delivery_id: Uuid::new_v4(),
            status: 202,
        };
        let workspace: Uuid = row.get("workspace_id");
        let app: Uuid = row.get("app_id");
        let inserted=sqlx::query("INSERT INTO deliveries(id,event_id,workspace_id,app_id,generation) SELECT $1,$2,$3,$4,$5 FROM events WHERE id=$2 AND expires_at>clock_timestamp()").bind(accepted.delivery_id).bind(event).bind(workspace).bind(app).bind(next).execute(&mut *tx).await?;
        if inserted.rows_affected() != 1 {
            return Err(DeliveryError::Gone);
        }
        sqlx::query("INSERT INTO event_outbox(delivery_id,workspace_id,app_id) VALUES($1,$2,$3)")
            .bind(accepted.delivery_id)
            .bind(workspace)
            .bind(app)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO delivery_replays(delivery_id,source_delivery_id,actor_user_id) VALUES($1,$2,$3)").bind(accepted.delivery_id).bind(id).bind(actor).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(accepted)
    }
    /// Broker redelivery also reaps abandoned attempts while every agent is offline.
    pub(crate) async fn reconcile(&self, id: Uuid) -> Result<bool, DeliveryError> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(LOCK_DELIVERY)
            .bind(id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(DeliveryError::Forbidden)?;
        let mut terminal = row.get::<bool, _>("terminal");
        if !terminal && !row.get::<bool, _>("retained") {
            sqlx::query("UPDATE deliveries SET state='expired' WHERE id=$1")
                .bind(id)
                .execute(&mut *tx)
                .await?;
            terminal = true;
        } else if !terminal
            && row.get::<&str, _>("state") == "in_flight"
            && !row.get::<bool, _>("lease_live")
        {
            let attempt: i32 = row.get("attempt_count");
            finish_attempt(&mut tx, id, attempt, "lease_expired", false).await?;
            terminal = attempt >= i32::from(MAX_ATTEMPTS);
        }
        tx.commit().await?;
        Ok(terminal)
    }
    pub(crate) async fn pending_ids(&self, agent: &Principal) -> Result<Vec<Uuid>, DeliveryError> {
        let (workspace, app, _) = agent_scope(agent)?;
        Ok(sqlx::query_scalar("SELECT id FROM deliveries WHERE workspace_id=$1 AND app_id=$2 AND terminal_at IS NULL AND (state='pending' OR state='retry_wait' AND next_attempt_at<=clock_timestamp() OR state='in_flight' AND lease_expires_at<=clock_timestamp()) ORDER BY created_at,id LIMIT 1000")
            .bind(workspace).bind(app).fetch_all(&self.pool).await?)
    }
    pub(crate) async fn input(&self, lease: &DeliveryLease) -> Result<EventInput, DeliveryError> {
        let bytes: Vec<u8> = sqlx::query_scalar("SELECT payload FROM events WHERE id=$1")
            .bind(lease.event_id)
            .fetch_one(&self.pool)
            .await?;
        serde_json::from_slice(&bytes).map_err(|_| DeliveryError::Internal)
    }
}

async fn finish_attempt(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    id: Uuid,
    attempt: i32,
    outcome: &str,
    success: bool,
) -> Result<(), DeliveryError> {
    sqlx::query("UPDATE delivery_attempts SET outcome=$3,finished_at=clock_timestamp() WHERE delivery_id=$1 AND attempt=$2 AND finished_at IS NULL")
        .bind(id).bind(attempt).bind(outcome).execute(&mut **tx).await?;
    let state = if success {
        "succeeded"
    } else if attempt >= i32::from(MAX_ATTEMPTS) {
        "dlq"
    } else {
        "retry_wait"
    };
    let delay = RETRY_DELAYS_SECONDS
        .get((attempt - 1) as usize)
        .copied()
        .unwrap_or(0);
    let updated=sqlx::query("UPDATE deliveries SET state=$2,next_attempt_at=CASE WHEN $2='retry_wait' THEN (SELECT finished_at FROM delivery_attempts WHERE delivery_id=$1 AND attempt=$3)+$4*interval '1 second' ELSE NULL END WHERE id=$1 AND state='in_flight' AND attempt_count=$3 AND ($5 OR (lease_expires_at>clock_timestamp() AND attempt_started_at+interval '300 seconds'>clock_timestamp() AND EXISTS(SELECT 1 FROM events WHERE id=deliveries.event_id AND expires_at>clock_timestamp())))")
        .bind(id).bind(state).bind(attempt).bind(delay as f64).bind(outcome=="lease_expired").execute(&mut **tx).await?;
    if updated.rows_affected() != 1 {
        return Err(DeliveryError::Conflict);
    }
    Ok(())
}
