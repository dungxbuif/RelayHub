//! Recoverable publication: delivery ID is stable across every ambiguous broker outcome.
use crate::events::Events;
use sqlx::Row;
use std::time::Duration;
use tokio::sync::watch;
use uuid::Uuid;

#[derive(Clone)]
pub struct Outbox {
    pool: sqlx::PgPool,
    jetstream: async_nats::jetstream::Context,
    namespace: String,
}

#[derive(Debug)]
pub enum DispatchError {
    Database(sqlx::Error),
    Broker,
    InvalidNamespace,
}
impl From<sqlx::Error> for DispatchError {
    fn from(error: sqlx::Error) -> Self {
        Self::Database(error)
    }
}
impl std::fmt::Display for DispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Database(_) => "outbox database error",
            Self::Broker => "outbox broker error",
            Self::InvalidNamespace => "invalid outbox namespace",
        })
    }
}
impl std::error::Error for DispatchError {}

impl Outbox {
    pub fn new(
        pool: sqlx::PgPool,
        jetstream: async_nats::jetstream::Context,
        namespace: &str,
    ) -> Result<Self, DispatchError> {
        if namespace.is_empty()
            || namespace.len() > 128
            || !namespace
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
        {
            return Err(DispatchError::InvalidNamespace);
        }
        Ok(Self {
            pool,
            jetstream,
            namespace: namespace.to_owned(),
        })
    }

    pub async fn dispatch_outbox(&self, batch_size: u32) -> Result<u32, DispatchError> {
        let mut sent = 0;
        // One transaction per publication bounds locks and preserves earlier progress on failure.
        for _ in 0..batch_size.min(1000) {
            let mut tx = self.pool.begin().await?;
            let Some(row) = sqlx::query("SELECT o.delivery_id, o.workspace_id, o.app_id, d.event_id FROM deliveries d JOIN event_outbox o ON o.delivery_id = d.id JOIN events e ON e.id = d.event_id WHERE o.sent_at IS NULL AND o.discarded_at IS NULL AND d.terminal_at IS NULL AND e.expires_at > now() ORDER BY o.created_at, o.delivery_id LIMIT 1 FOR UPDATE OF d, o SKIP LOCKED")
                .fetch_optional(&mut *tx).await? else { break; };
            let delivery: Uuid = row.get("delivery_id");
            let event: Uuid = row.get("event_id");
            let workspace: Uuid = row.get("workspace_id");
            let app: Uuid = row.get("app_id");
            let subject = format!(
                "{}.v1.workspaces.{workspace}.apps.{app}.deliveries",
                self.namespace
            );
            let payload =
                serde_json::json!({"delivery_id": delivery, "event_id": event}).to_string();
            let mut headers = async_nats::HeaderMap::new();
            headers.insert("Nats-Msg-Id", delivery.to_string());
            // Both sending and receiving PubAck must finish; ambiguous failures remain pending.
            tokio::time::timeout(Duration::from_secs(2), async {
                self.jetstream
                    .publish_with_headers(subject, headers, payload.into())
                    .await
                    .map_err(|_| DispatchError::Broker)?
                    .await
                    .map_err(|_| DispatchError::Broker)
            })
            .await
            .map_err(|_| DispatchError::Broker)??;
            sqlx::query("UPDATE event_outbox SET sent_at = now() WHERE delivery_id = $1")
                .bind(delivery)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            sent += 1;
        }
        Ok(sent)
    }

    /// Call once per replica after migrations. Broker failures cannot stall retention expiry.
    /// Receivers dropping or becoming true cancels both loops; pending publication is retryable.
    pub async fn run_workers(&self, events: Events, mut shutdown: watch::Receiver<bool>) {
        if *shutdown.borrow() {
            return;
        }
        let publisher = async {
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                if let Err(error) = self.dispatch_outbox(100).await {
                    tracing::warn!(%error, "outbox dispatch failed; will retry");
                }
            }
        };
        let reaper = async {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                if events.expire_deliveries(100).await.is_err() {
                    tracing::warn!("delivery expiry failed; will retry");
                }
            }
        };
        tokio::select! {
            _ = publisher => {},
            _ = reaper => {},
            _ = async { while shutdown.changed().await.is_ok() { if *shutdown.borrow() { break; } } } => {},
        }
    }
}
