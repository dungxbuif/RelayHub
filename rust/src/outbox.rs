//! Recoverable publication: delivery ID is stable across every ambiguous broker outcome.
use crate::{events::Events, identity::Identity};
use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::Row;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;
use uuid::Uuid;

#[derive(Clone)]
pub struct Outbox {
    pool: sqlx::PgPool,
    jetstream: async_nats::jetstream::Context,
    namespace: String,
    identity: Option<Identity>,
    http: reqwest::Client,
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
            identity: None,
            http: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .timeout(Duration::from_secs(10))
                .build()
                .map_err(|_| DispatchError::Broker)?,
        })
    }

    pub fn new_with_identity(
        pool: sqlx::PgPool,
        jetstream: async_nats::jetstream::Context,
        namespace: &str,
        identity: Identity,
    ) -> Result<Self, DispatchError> {
        let mut outbox = Self::new(pool, jetstream, namespace)?;
        outbox.identity = Some(identity);
        Ok(outbox)
    }

    pub async fn dispatch_outbox(&self, batch_size: u32) -> Result<u32, DispatchError> {
        let mut sent = 0;
        // One transaction per publication bounds locks and preserves earlier progress on failure.
        for _ in 0..batch_size.min(1000) {
            let mut tx = self.pool.begin().await?;
            let Some(row) = sqlx::query("SELECT o.delivery_id, o.workspace_id, d.target_app_id, d.event_id FROM deliveries d JOIN event_outbox o ON o.delivery_id = d.id JOIN events e ON e.id = d.event_id WHERE o.sent_at IS NULL AND o.discarded_at IS NULL AND d.terminal_at IS NULL AND e.expires_at > now() ORDER BY o.created_at, o.delivery_id LIMIT 1 FOR UPDATE OF d, o SKIP LOCKED")
                .fetch_optional(&mut *tx).await? else { break; };
            let delivery: Uuid = row.get("delivery_id");
            let event: Uuid = row.get("event_id");
            let workspace: Uuid = row.get("workspace_id");
            let app: Uuid = row.get("target_app_id");
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

    async fn dispatch_callback(&self) -> Result<bool, DispatchError> {
        let Some(identity) = &self.identity else {
            return Ok(false);
        };
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT d.id,d.event_id,d.attempt_count,d.attempt_token,e.payload,e.legacy_source_id,e.legacy_target_app_ids,to_char(e.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS created_at,e.expires_at,a.callback_url,c.encrypted_hmac_secret FROM deliveries d JOIN events e ON e.id=d.event_id JOIN identity_apps a ON a.id=d.target_app_id LEFT JOIN identity_credentials c ON c.app_id=d.target_app_id AND c.revoked_at IS NULL WHERE d.legacy_sink='callback' AND ((d.state IN ('pending','retry_wait') AND (d.next_attempt_at IS NULL OR d.next_attempt_at<=clock_timestamp())) OR (d.state='in_flight' AND d.lease_expires_at<=clock_timestamp())) AND e.expires_at>clock_timestamp() AND a.enabled AND a.callback_url IS NOT NULL AND length(btrim(a.callback_url))>0 ORDER BY d.created_at,d.id FOR UPDATE OF d SKIP LOCKED LIMIT 1")
            .fetch_optional(&mut *tx)
            .await?;
        let Some(row) = row else {
            tx.commit().await?;
            return Ok(false);
        };
        let delivery_id: Uuid = row.get("id");
        let previous_attempt = row.get::<i32, _>("attempt_count");
        let attempt = previous_attempt + 1;
        if attempt > 5 {
            sqlx::query("UPDATE deliveries SET state='dlq',terminal_at=clock_timestamp(),attempt_token=NULL,agent_id=NULL,attempt_started_at=NULL,lease_expires_at=NULL,next_attempt_at=NULL WHERE id=$1")
                .bind(delivery_id)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            return Ok(true);
        }
        let token = Uuid::new_v4();
        let worker_id = Uuid::nil();
        sqlx::query("UPDATE deliveries SET state='in_flight',attempt_count=$2,attempt_token=$3,agent_id=$4,attempt_started_at=clock_timestamp(),lease_expires_at=clock_timestamp()+interval '60 seconds',next_attempt_at=NULL WHERE id=$1")
            .bind(delivery_id)
            .bind(attempt)
            .bind(token)
            .bind(worker_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO delivery_attempts(delivery_id,attempt,attempt_token,agent_id,started_at,lease_expires_at) VALUES($1,$2,$3,$4,clock_timestamp(),clock_timestamp()+interval '60 seconds') ON CONFLICT(delivery_id,attempt) DO UPDATE SET attempt_token=EXCLUDED.attempt_token,agent_id=EXCLUDED.agent_id,started_at=EXCLUDED.started_at,lease_expires_at=EXCLUDED.lease_expires_at,finished_at=NULL,outcome=NULL")
            .bind(delivery_id)
            .bind(attempt)
            .bind(token)
            .bind(worker_id)
            .execute(&mut *tx)
            .await?;
        let event_id: Uuid = row.get("event_id");
        let event_source = row
            .get::<Option<String>, _>("legacy_source_id")
            .unwrap_or_else(|| format!("evt_{}", event_id.simple()));
        let payload: Value = serde_json::from_slice(&row.get::<Vec<u8>, _>("payload"))
            .map_err(|_| DispatchError::Broker)?;
        let event_type = payload
            .get("event_type")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let data = payload
            .get("body")
            .and_then(Value::as_str)
            .and_then(|value| STANDARD.decode(value).ok())
            .and_then(|value| serde_json::from_slice::<Value>(&value).ok())
            .unwrap_or_else(|| json!({}));
        let event = json!({
            "id": event_source,
            "type": event_type,
            "source_app_id": sqlx::query_scalar::<_, Option<String>>("SELECT legacy_source_id FROM identity_apps WHERE id=(SELECT app_id FROM events WHERE id=$1)")
                .bind(event_id)
                .fetch_one(&mut *tx)
                .await?
                .unwrap_or_else(|| row.get::<Uuid, _>("event_id").to_string()),
            "target_app_ids": row.get::<Option<Vec<String>>, _>("legacy_target_app_ids").unwrap_or_default(),
            "data": data,
            "created_at": row.get::<String, _>("created_at"),
        });
        let body = serde_json::to_vec(&event).map_err(|_| DispatchError::Broker)?;
        let callback_url: String = row.get("callback_url");
        let secret_ciphertext = row.get::<Option<String>, _>("encrypted_hmac_secret");
        let secret = secret_ciphertext
            .as_deref()
            .and_then(|value| identity.decrypt_secret(value).ok());
        let callback_url_parsed = reqwest::Url::parse(&callback_url).ok();
        let valid_url = callback_url_parsed.as_ref().is_some_and(|url| {
            matches!(url.scheme(), "http" | "https")
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
        });
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| DispatchError::Broker)?
            .as_secs()
            .to_string();
        let signature = secret.as_deref().and_then(|key| {
            let mut mac = Hmac::<Sha256>::new_from_slice(key).ok()?;
            let body_hash = Sha256::digest(&body);
            let target = callback_url_parsed.as_ref()?.path();
            let target = if let Some(query) = callback_url_parsed.as_ref()?.query() {
                format!("{target}?{query}")
            } else {
                target.to_owned()
            };
            mac.update(format!("{timestamp}\nPOST\n{target}\n{}", hex(&body_hash)).as_bytes());
            Some(hex(&mac.finalize().into_bytes()))
        });
        tx.commit().await?;
        let response = if valid_url && signature.is_some() {
            self.http
                .post(&callback_url)
                .header("content-type", "application/json")
                .header("x-relayhub-event-id", &event_source)
                .header("x-relayhub-timestamp", &timestamp)
                .header("x-relayhub-signature", signature.unwrap_or_default())
                .body(body)
                .send()
                .await
        } else {
            return self
                .finish_callback(delivery_id, attempt, token, false, false)
                .await
                .map(|_| true);
        };
        let (succeeded, retryable) = match response {
            Ok(response) if response.status().is_success() => (true, false),
            Ok(response)
                if response.status().as_u16() == 408
                    || response.status().as_u16() == 429
                    || response.status().is_server_error() =>
            {
                (false, true)
            }
            _ => (false, false),
        };
        self.finish_callback(delivery_id, attempt, token, succeeded, retryable)
            .await?;
        Ok(true)
    }

    async fn finish_callback(
        &self,
        delivery_id: Uuid,
        attempt: i32,
        token: Uuid,
        succeeded: bool,
        retryable: bool,
    ) -> Result<(), DispatchError> {
        let mut tx = self.pool.begin().await?;
        let state = if succeeded {
            "succeeded"
        } else if retryable && attempt < 5 {
            "retry_wait"
        } else {
            "dlq"
        };
        sqlx::query("UPDATE deliveries SET state=$2,terminal_at=CASE WHEN $2 IN ('succeeded','dlq') THEN clock_timestamp() ELSE NULL END,attempt_token=NULL,agent_id=NULL,attempt_started_at=NULL,lease_expires_at=NULL,next_attempt_at=CASE WHEN $2='retry_wait' THEN clock_timestamp()+LEAST(300,power(2::numeric,$3)::bigint)*interval '1 second' ELSE NULL END WHERE id=$1 AND state='in_flight' AND attempt_count=$3 AND attempt_token=$4")
            .bind(delivery_id)
            .bind(state)
            .bind(attempt)
            .bind(token)
            .execute(&mut *tx)
            .await?;
        sqlx::query("UPDATE delivery_attempts SET finished_at=clock_timestamp(),outcome=$2 WHERE delivery_id=$1 AND attempt=$3 AND attempt_token=$4 AND finished_at IS NULL")
            .bind(delivery_id)
            .bind(if succeeded { "success" } else { "retryable_failure" })
            .bind(attempt)
            .bind(token)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
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
        let callbacks = async {
            let mut tick = tokio::time::interval(Duration::from_millis(250));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tick.tick().await;
                if self.dispatch_callback().await.unwrap_or(false) {
                    continue;
                }
            }
        };
        tokio::select! {
            _ = publisher => {},
            _ = reaper => {},
            _ = callbacks => {},
            _ = async { while shutdown.changed().await.is_ok() { if *shutdown.borrow() { break; } } } => {},
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
