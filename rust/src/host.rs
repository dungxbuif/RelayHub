//! Runtime composition for the Rust v1 host and its thin dashboard adapters.
use crate::{
    dashboard::Dashboard,
    delivery::Deliveries,
    events::{AcceptError, Events},
    gateway::Gateway,
    identity::{AuthError, Identity},
    outbox::Outbox,
    presence::Presence,
    protocol::{EventInput, MAX_BODY_BYTES, Principal, RETENTION_SECONDS},
    realtime::Realtime,
    responses::Responses,
    webhooks::Webhooks,
};
use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{Next, from_fn_with_state},
    response::{IntoResponse, Response},
    routing::post,
};
use futures_util::StreamExt;
use std::{future::IntoFuture, net::SocketAddr, time::Duration};
use tokio::{sync::watch, task::JoinSet};
use uuid::Uuid;

const DRAIN: Duration = Duration::from_secs(30);

// Deliberately no Debug: connection URLs can contain credentials.
pub struct HostConfig {
    pub postgres_url: String,
    pub nats_url: String,
    pub redis_url: String,
    pub namespace: String,
    pub http_addr: SocketAddr,
}
impl HostConfig {
    /// RELAYHUB_NAMESPACE defaults to relayhub_v1; HTTP_ADDR defaults to 0.0.0.0:8080.
    /// REDIS_URL takes precedence over REDIS_ADDR (host:port, database 0).
    pub fn from_env() -> Result<Self, HostError> {
        fn required(name: &str, error: &'static str) -> Result<String, HostError> {
            std::env::var(name)
                .ok()
                .filter(|v| !v.trim().is_empty())
                .ok_or(HostError(error))
        }
        let postgres_url = required("POSTGRES_URL", "invalid POSTGRES_URL")?;
        let nats_url = required("NATS_URL", "invalid NATS_URL")?;
        let redis_url = match std::env::var("REDIS_URL") {
            Ok(url) if !url.trim().is_empty() => url,
            Err(std::env::VarError::NotPresent) => format!(
                "redis://{}/0",
                required("REDIS_ADDR", "invalid REDIS_ADDR")?
            ),
            _ => return Err(HostError("invalid REDIS_URL")),
        };
        let namespace = match std::env::var("RELAYHUB_NAMESPACE") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => "relayhub_v1".into(),
            _ => return Err(HostError("invalid RELAYHUB_NAMESPACE")),
        };
        if namespace.is_empty()
            || namespace.len() > 128
            || !namespace
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
        {
            return Err(HostError("invalid RELAYHUB_NAMESPACE"));
        }
        let http_addr = match std::env::var("HTTP_ADDR") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => "0.0.0.0:8080".into(),
            _ => return Err(HostError("invalid HTTP_ADDR")),
        }
        .parse()
        .map_err(|_| HostError("invalid HTTP_ADDR"))?;
        Ok(Self {
            postgres_url,
            nats_url,
            redis_url,
            namespace,
            http_addr,
        })
    }
}

/// Only static categories escape startup; never return datastore errors containing URLs.
#[derive(Debug)]
pub struct HostError(&'static str);
impl std::fmt::Display for HostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for HostError {}

#[derive(Clone)]
pub struct Host {
    identity: Identity,
    events: Events,
    deliveries: Deliveries,
    gateway: Gateway,
    webhooks: Webhooks,
    realtime: Realtime,
    outbox: Outbox,
    jetstream: async_nats::jetstream::Context,
    namespace: String,
    dashboard: Dashboard,
}
impl Host {
    pub async fn connect(config: &HostConfig) -> Result<Self, HostError> {
        tokio::time::timeout(Duration::from_secs(20), async {
            let pool = sqlx::postgres::PgPoolOptions::new()
                .max_connections(10)
                .acquire_timeout(Duration::from_secs(5))
                .connect(&config.postgres_url)
                .await
                .map_err(|_| HostError("postgres connection failed"))?;
            let nats = async_nats::ConnectOptions::new()
                .subscription_capacity(64)
                .connection_timeout(Duration::from_secs(5))
                .connect(&config.nats_url)
                .await
                .map_err(|_| HostError("nats connection failed"))?;
            let redis = redis::Client::open(config.redis_url.as_str())
                .map_err(|_| HostError("invalid REDIS_URL"))?
                .get_multiplexed_async_connection()
                .await
                .map_err(|_| HostError("redis connection failed"))?;
            let host = Self::new(
                pool,
                async_nats::jetstream::new(nats),
                redis,
                &config.namespace,
            )?;
            host.migrate().await?;
            Ok(host)
        })
        .await
        .map_err(|_| HostError("host startup timed out"))?
    }

    pub fn new(
        pool: sqlx::PgPool,
        jetstream: async_nats::jetstream::Context,
        redis: redis::aio::MultiplexedConnection,
        namespace: &str,
    ) -> Result<Self, HostError> {
        Self::new_with_identity(pool, jetstream, redis, namespace, None)
    }

    /// Constructor used by migration/compatibility tests to inject the same
    /// master key that the Go store uses. Production loads it from the
    /// RELAYHUB_SECRET_ENCRYPTION_KEY environment variable via `Identity::new`.
    pub fn new_with_secret_encryption_key(
        pool: sqlx::PgPool,
        jetstream: async_nats::jetstream::Context,
        redis: redis::aio::MultiplexedConnection,
        namespace: &str,
        encoded_key: &str,
    ) -> Result<Self, HostError> {
        Self::new_with_identity(pool, jetstream, redis, namespace, Some(encoded_key))
    }

    fn new_with_identity(
        pool: sqlx::PgPool,
        jetstream: async_nats::jetstream::Context,
        redis: redis::aio::MultiplexedConnection,
        namespace: &str,
        encoded_key: Option<&str>,
    ) -> Result<Self, HostError> {
        let identity = match encoded_key {
            Some(key) => Identity::with_secret_encryption_key(pool.clone(), key)
                .map_err(|_| HostError("invalid secret encryption key"))?,
            None => Identity::new(pool.clone()),
        };
        let events = Events::new(pool.clone());
        let presence =
            Presence::new(redis.clone(), namespace).map_err(|_| HostError("invalid namespace"))?;
        let deliveries = Deliveries::new(pool.clone(), presence.clone());
        let responses = Responses::new(pool.clone(), deliveries.clone(), redis.clone(), namespace)
            .map_err(|_| HostError("response initialization failed"))?;
        let id = Uuid::new_v4();
        let gateway = Gateway::new(id, deliveries.clone(), presence.clone(), jetstream.clone())
            .with_responses(responses);
        let webhooks = Webhooks::new(pool.clone(), presence.clone(), redis.clone(), namespace)
            .map_err(|_| HostError("webhook initialization failed"))?;
        let realtime = Realtime::new(id, jetstream.client().clone(), redis, namespace)
            .map_err(|_| HostError("realtime initialization failed"))?;
        let dashboard = Dashboard::new(
            pool.clone(),
            identity.clone(),
            events.clone(),
            webhooks.clone(),
            deliveries.clone(),
            presence.clone(),
            realtime.clone(),
        );
        let outbox = Outbox::new(pool, jetstream.clone(), namespace)
            .map_err(|_| HostError("outbox initialization failed"))?;
        Ok(Self {
            identity,
            events,
            deliveries,
            gateway,
            webhooks,
            realtime,
            outbox,
            jetstream,
            namespace: namespace.into(),
            dashboard,
        })
    }

    pub async fn migrate(&self) -> Result<(), HostError> {
        self.identity
            .migrate()
            .await
            .map_err(|_| HostError("identity migration failed"))?;
        self.events
            .migrate()
            .await
            .map_err(|_| HostError("events migration failed"))?;
        self.deliveries
            .migrate()
            .await
            .map_err(|_| HostError("delivery migration failed"))?;
        self.webhooks
            .migrate()
            .await
            .map_err(|_| HostError("webhook migration failed"))
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/api/v1/events", post(publish))
            .with_state((self.identity.clone(), self.events.clone()))
            .merge(self.identity.http_router())
            .merge(self.gateway.router(self.identity.clone()))
            .merge(
                self.webhooks
                    .router()
                    .route_layer(from_fn_with_state(self.identity.clone(), status_auth)),
            )
            .merge(self.realtime.router(self.identity.clone()))
            .merge(self.dashboard.router())
            .layer(axum::middleware::map_response(
                |mut response: Response| async move {
                    response
                        .headers_mut()
                        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
                    response
                },
            ))
    }

    async fn consumer(&self) -> Result<async_nats::jetstream::consumer::PullConsumer, HostError> {
        let stream = self
            .jetstream
            .get_or_create_stream(async_nats::jetstream::stream::Config {
                name: self.namespace.clone(),
                subjects: vec![format!(
                    "{}.v1.workspaces.*.apps.*.deliveries",
                    self.namespace
                )],
                retention: async_nats::jetstream::stream::RetentionPolicy::WorkQueue,
                max_age: Duration::from_secs(RETENTION_SECONDS),
                ..Default::default()
            })
            .await
            .map_err(|_| HostError("delivery stream unavailable"))?;
        stream
            .get_or_create_consumer(
                "core-host",
                async_nats::jetstream::consumer::pull::Config {
                    durable_name: Some("core-host".into()),
                    ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                    ack_wait: Duration::from_secs(30),
                    max_ack_pending: 100,
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| HostError("delivery consumer unavailable"))
    }

    async fn reconcile(&self) -> Result<(), HostError> {
        let consumer = self.consumer().await?;
        let mut messages = consumer
            .messages()
            .await
            .map_err(|_| HostError("delivery consumer unavailable"))?;
        while let Some(message) = messages.next().await {
            let message = message.map_err(|_| HostError("delivery consumer unavailable"))?;
            if self.gateway.reconcile_message(&message).await.is_err() {
                // Leave unacknowledged work recoverable after transient failures.
                tracing::warn!("delivery reconciliation failed; will retry");
            }
        }
        Err(HostError("delivery consumer closed"))
    }

    /// Runs workers for the listener's lifetime; cancellation also aborts the owned worker tasks.
    pub async fn serve(
        &self,
        listener: tokio::net::TcpListener,
        shutdown: impl std::future::Future<Output = ()> + Send,
    ) -> Result<(), HostError> {
        self.consumer().await?;
        let (stop_workers, stopped) = watch::channel(false);
        let mut workers = JoinSet::new();
        let outbox = self.outbox.clone();
        let events = self.events.clone();
        workers.spawn(async move { outbox.run_workers(events, stopped).await });
        let host = self.clone();
        workers.spawn(async move {
            loop {
                let _ = host.reconcile().await;
                tracing::warn!("delivery consumer interrupted; will reconnect");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        let (stop_http, stopped_http) = tokio::sync::oneshot::channel();
        let server = axum::serve(listener, self.router())
            .with_graceful_shutdown(async {
                let _ = stopped_http.await;
            })
            .into_future();
        tokio::pin!(server);
        let result = tokio::select! {
            _ = shutdown => Ok(()),
            _ = workers.join_next() => Err(HostError("host worker stopped")),
            _ = &mut server => Err(HostError("http server stopped")),
        };
        self.gateway
            .drain(DRAIN)
            .map_err(|_| HostError("gateway drain failed"))?;
        let _ = stop_http.send(());
        // shortcut: Gateway exposes no active-session join; keep stores/workers alive for the
        // full grace period, and add an idle notification when shutdown latency needs reducing.
        let shutdown_result = if result.is_ok() {
            tokio::time::timeout(DRAIN + Duration::from_secs(2), async {
                let (served, _) = tokio::join!(&mut server, tokio::time::sleep(DRAIN));
                served.map_err(|_| HostError("http server failed"))
            })
            .await
            .map_err(|_| HostError("http shutdown timed out"))?
        } else {
            tokio::time::sleep(DRAIN).await;
            Ok(())
        };
        let _ = stop_workers.send(true);
        // Publication/settlement cancellations recover through the existing durable state.
        let _ = tokio::time::timeout(Duration::from_secs(2), workers.shutdown()).await;
        result.and(shutdown_result)
    }
}

async fn authenticate(identity: &Identity, headers: &HeaderMap) -> Result<Principal, AuthError> {
    if headers.get_all(header::AUTHORIZATION).iter().count() != 1 {
        return Err(AuthError::Unauthorized);
    }
    let token = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .ok_or(AuthError::Unauthorized)?;
    identity.authenticate(token).await
}

async fn status_auth(
    State(identity): State<Identity>,
    mut request: Request,
    next: Next,
) -> Response {
    // Webhooks owns exactly ingress and status. Ingress authenticates its endpoint secret itself.
    if !request.uri().path().starts_with("/hooks/") {
        match authenticate(&identity, request.headers()).await {
            Ok(principal) => {
                request.extensions_mut().insert(principal);
            }
            Err(error) => return error.into_response(),
        }
    }
    next.run(request).await
}

async fn publish(
    State((identity, events)): State<(Identity, Events)>,
    request: Request,
) -> Response {
    let (parts, body) = request.into_parts();
    let body = match to_bytes(body, 2 * MAX_BODY_BYTES).await {
        Ok(body) => body,
        Err(_) => return AcceptError::PayloadTooLarge.into_response(),
    };
    let request_target = parts
        .uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or(parts.uri.path());
    let principal = match identity
        .authenticate_public_request(&parts.headers, parts.method.as_str(), request_target, &body)
        .await
    {
        Ok(principal @ Principal::Application { .. }) => principal,
        Ok(_) => return AuthError::Forbidden.into_response(),
        Err(error) => return error.into_response(),
    };
    if parts.headers.get_all("idempotency-key").iter().count() != 1 {
        return AcceptError::InvalidInput.into_response();
    }
    let Some(key) = parts
        .headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
    else {
        return AcceptError::InvalidInput.into_response();
    };
    let input: EventInput = match serde_json::from_slice(&body) {
        Ok(input) => input,
        Err(_) => return AcceptError::InvalidInput.into_response(),
    };
    match events.accept_event(&principal, input, key).await {
        Ok(receipt) => (StatusCode::ACCEPTED, Json(receipt)).into_response(),
        Err(error) => error.into_response(),
    }
}
