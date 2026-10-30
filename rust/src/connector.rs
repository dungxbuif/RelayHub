//! Fixed local HTTP forwarding over the frozen relayhub.v1 agent protocol.
use crate::{
    protocol::{
        DeliveryLease, EventInput, Frame, HEARTBEAT_SECONDS, HttpResponse, MAX_BODY_BYTES,
        MAX_IN_FLIGHT, SUBPROTOCOL, VERSION, parse_frame,
    },
    webhooks::safe_headers,
};
use futures_util::{SinkExt, StreamExt};
use reqwest::{Client, Url};
use serde::Deserialize;
use std::{
    collections::HashMap,
    io::Read,
    net::IpAddr,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    net::TcpStream,
    task::JoinSet,
    time::{Instant, timeout},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Message, client::IntoClientRequest, protocol::WebSocketConfig},
};
use uuid::Uuid;

const LOCAL_TIMEOUT: Duration = Duration::from_secs(30);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_ENVELOPE: usize = 2 * MAX_BODY_BYTES;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorConfig {
    pub server_url: String,
    pub credential_file: PathBuf,
    pub endpoint_targets: HashMap<Uuid, String>,
    pub max_in_flight: u16,
}
impl ConnectorConfig {
    /// Relative credential paths are relative to the configuration file.
    pub fn from_file(path: &Path) -> Result<Self, ConnectorError> {
        let text = read_bounded(
            std::fs::File::open(path).map_err(|_| ConnectorError::Config)?,
            65_536,
        )
        .map_err(|_| ConnectorError::Config)?;
        let mut config: Self = toml::from_str(&text).map_err(|_| ConnectorError::Config)?;
        if config.credential_file.is_relative() {
            config.credential_file = path
                .parent()
                .unwrap_or(Path::new("."))
                .join(&config.credential_file);
        }
        Ok(config)
    }
}

/// Errors deliberately contain no underlying URL, credential, HTTP data or parser input.
#[derive(Debug, Clone, Copy)]
pub enum ConnectorError {
    Config,
    Credential,
    Transport,
    InvalidDelivery,
    LocalHttp,
    Signal,
}
impl std::fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Config => "invalid connector configuration",
            Self::Credential => "invalid protected credential file",
            Self::Transport => "connector transport unavailable",
            Self::InvalidDelivery => "invalid delivery",
            Self::LocalHttp => "local HTTP request failed",
            Self::Signal => "shutdown signal unavailable",
        })
    }
}
impl std::error::Error for ConnectorError {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Credential {
    agent_id: Uuid,
    token: String,
}
fn read_bounded(file: std::fs::File, max: u64) -> Result<String, ConnectorError> {
    let mut text = String::new();
    file.take(max + 1)
        .read_to_string(&mut text)
        .map_err(|_| ConnectorError::Credential)?;
    if text.len() as u64 > max {
        return Err(ConnectorError::Credential);
    }
    Ok(text)
}
#[cfg(unix)]
fn credential(path: &Path) -> Result<Credential, ConnectorError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    // Check the opened descriptor, never a path that can change between stat and open.
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(path)
        .map_err(|_| ConnectorError::Credential)?;
    let metadata = file.metadata().map_err(|_| ConnectorError::Credential)?;
    // SAFETY: geteuid has no arguments or memory preconditions.
    if !metadata.is_file()
        || metadata.mode() & 0o077 != 0
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(ConnectorError::Credential);
    }
    let value: Credential =
        toml::from_str(&read_bounded(file, 4096)?).map_err(|_| ConnectorError::Credential)?;
    if value.agent_id.is_nil()
        || value.token.is_empty()
        || value.token.len() > 512
        || !value.token.bytes().all(|b| b.is_ascii_graphic())
    {
        return Err(ConnectorError::Credential);
    }
    Ok(value)
}
#[cfg(not(unix))]
fn credential(_: &Path) -> Result<Credential, ConnectorError> {
    // Fail closed until an equivalent owner-only ACL check is available.
    Err(ConnectorError::Credential)
}
fn url(value: &str) -> Result<Url, ConnectorError> {
    let url = Url::parse(value).map_err(|_| ConnectorError::Config)?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.host().is_none()
    {
        return Err(ConnectorError::Config);
    }
    Ok(url)
}
fn local_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_loopback() || ip.is_private(),
        IpAddr::V6(ip) => {
            // AWS IMDS also has a unique-local IPv6 address.
            ip != "fd00:ec2::254".parse::<std::net::Ipv6Addr>().unwrap()
                && (ip.is_loopback()
                    || ip.is_unique_local()
                    || ip.to_ipv4_mapped().is_some_and(|ip| local_ip(ip.into())))
        }
    }
}
fn target(value: &str) -> Result<Url, ConnectorError> {
    let mut url = url(value)?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(ConnectorError::Config);
    }
    if url.host_str() == Some("localhost") {
        // Pin localhost instead of trusting DNS or hosts-file changes.
        url.set_host(Some("127.0.0.1"))
            .map_err(|_| ConnectorError::Config)?;
    }
    let host = url
        .host_str()
        .ok_or(ConnectorError::Config)?
        .trim_matches(['[', ']']);
    let ip = host.parse::<IpAddr>().map_err(|_| ConnectorError::Config)?;
    if !local_ip(ip) || url.port_or_known_default() == Some(0) {
        return Err(ConnectorError::Config);
    }
    Ok(url)
}
fn server_url(value: &str) -> Result<Url, ConnectorError> {
    let url = url(value)?;
    if !matches!(url.scheme(), "ws" | "wss") || url.query().is_some() {
        return Err(ConnectorError::Config);
    }
    if url.scheme() == "ws"
        && !url.host_str().is_some_and(|host| {
            host.trim_matches(['[', ']'])
                .parse::<IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
        })
    {
        return Err(ConnectorError::Config);
    }
    Ok(url)
}

/// Runs until SIGTERM/Ctrl-C. Unfinished or disconnected attempts are never acknowledged.
pub async fn run_connector(config: ConnectorConfig) -> Result<(), ConnectorError> {
    if !(1..=MAX_IN_FLIGHT).contains(&config.max_in_flight) || config.endpoint_targets.is_empty() {
        return Err(ConnectorError::Config);
    }
    let server = server_url(&config.server_url)?;
    let targets: HashMap<Uuid, Url> = config
        .endpoint_targets
        .iter()
        .map(|(id, value)| {
            if id.is_nil() {
                return Err(ConnectorError::Config);
            }
            Ok((*id, target(value)?))
        })
        .collect::<Result<_, _>>()?;
    let credentials = credential(&config.credential_file)?;
    let client = Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .timeout(LOCAL_TIMEOUT)
        .build()
        .map_err(|_| ConnectorError::LocalHttp)?;
    #[cfg(unix)]
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|_| ConnectorError::Signal)?;
    let shutdown = async move {
        #[cfg(unix)]
        tokio::select! { _=sigterm.recv()=>{}, _=tokio::signal::ctrl_c()=>{} }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    };
    tokio::pin!(shutdown);
    // The server crate enables two rustls providers. Select one per connector, not globally.
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let tls = rustls::ClientConfig::builder_with_provider(std::sync::Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|_| ConnectorError::Transport)?
    .with_root_certificates(roots)
    .with_no_client_auth();
    let tls = std::sync::Arc::new(tls);
    let mut backoff = Duration::from_secs(1);
    loop {
        let mut request = server
            .as_str()
            .into_client_request()
            .map_err(|_| ConnectorError::Config)?;
        let mut authorization = format!("Bearer {}", credentials.token)
            .parse::<axum::http::HeaderValue>()
            .map_err(|_| ConnectorError::Credential)?;
        authorization.set_sensitive(true);
        request.headers_mut().insert("authorization", authorization);
        request
            .headers_mut()
            .insert("sec-websocket-protocol", SUBPROTOCOL.parse().unwrap());
        let ws_config = WebSocketConfig::default()
            .max_message_size(Some(MAX_ENVELOPE))
            .max_frame_size(Some(MAX_ENVELOPE));
        let connection = tokio::select! {
            biased;
            _=&mut shutdown=>return Ok(()),
            result=timeout(Duration::from_secs(10),tokio_tungstenite::connect_async_tls_with_config(request,Some(ws_config),false,Some(tokio_tungstenite::Connector::Rustls(tls.clone()))))=>result,
        };
        if let Ok(Ok((mut socket, response))) = connection {
            if response
                .headers()
                .get("sec-websocket-protocol")
                .and_then(|v| v.to_str().ok())
                != Some(SUBPROTOCOL)
            {
                return Err(ConnectorError::Transport);
            }
            let hello = Frame::Hello {
                version: VERSION,
                request_id: Uuid::new_v4(),
                agent_id: credentials.agent_id,
                max_in_flight: config.max_in_flight,
            };
            if send(&mut socket, &hello).await.is_ok() {
                let began = Instant::now();
                if session(
                    &mut socket,
                    &client,
                    &targets,
                    config.max_in_flight,
                    &mut shutdown,
                )
                .await
                {
                    return Ok(());
                }
                if began.elapsed() >= Duration::from_secs(30) {
                    backoff = Duration::from_secs(1);
                }
            }
        }
        tokio::select! { biased; _=&mut shutdown=>return Ok(()), _=tokio::time::sleep(backoff)=>{} }
        backoff = (backoff * 2).min(Duration::from_secs(30));
    }
}
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
async fn send(socket: &mut Socket, frame: &Frame) -> Result<(), ConnectorError> {
    let text = serde_json::to_string(frame).map_err(|_| ConnectorError::InvalidDelivery)?;
    if text.len() > MAX_ENVELOPE {
        return Err(ConnectorError::InvalidDelivery);
    }
    timeout(
        Duration::from_secs(2),
        socket.send(Message::Text(text.into())),
    )
    .await
    .map_err(|_| ConnectorError::Transport)?
    .map_err(|_| ConnectorError::Transport)
}
async fn session<F: std::future::Future<Output = ()> + Unpin>(
    socket: &mut Socket,
    client: &Client,
    targets: &HashMap<Uuid, Url>,
    max: u16,
    shutdown: &mut F,
) -> bool {
    let mut jobs = JoinSet::new();
    let mut active: HashMap<Uuid, DeliveryLease> = HashMap::new();
    let mut heartbeat = tokio::time::interval(Duration::from_secs(HEARTBEAT_SECONDS));
    heartbeat.tick().await;
    let mut draining = false;
    let mut connected = true;
    let deadline = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(deadline);
    loop {
        if draining && jobs.is_empty() {
            return true;
        }
        tokio::select! {
            biased;
            _=&mut *shutdown, if !draining=> {
                draining=true;
                deadline.as_mut().reset(Instant::now()+DRAIN_TIMEOUT);
            },
            _=&mut deadline, if draining=>return true,
            result=jobs.join_next(), if !jobs.is_empty()=> {
                let Some(Ok((request_id,result)))=result else {return draining;};
                active.remove(&request_id);
                match result {
                    Ok(frame)=>if connected {
                        let sent=if draining {
                            matches!(tokio::time::timeout_at(deadline.deadline(),send(socket,&frame)).await,Ok(Ok(())))
                        } else {send(socket,&frame).await.is_ok()};
                        if !sent {
                            if !draining {return false;}
                            connected=false;
                        }
                    },
                    Err(_)=>if !draining {return false;},
                }
            },
            _=heartbeat.tick(), if connected=> {
                // Bound the entire batch so a stalled server cannot stretch shutdown.
                let renew=async {
                    send(socket,&Frame::Heartbeat{version:VERSION,request_id:Uuid::new_v4(),lease:None}).await?;
                    for lease in active.values() {
                        send(socket,&Frame::Heartbeat{version:VERSION,request_id:Uuid::new_v4(),lease:Some(lease.clone())}).await?;
                    }
                    Ok::<_,ConnectorError>(())
                };
                let budget=if draining {(Instant::now()+Duration::from_secs(2)).min(deadline.deadline())} else {Instant::now()+Duration::from_secs(2)};
                if !matches!(tokio::time::timeout_at(budget,renew).await,Ok(Ok(()))) {
                    if !draining {return false;}
                    connected=false;
                }
            },
            message=socket.next(), if !draining=> {
                let Some(Ok(message))=message else {return false;};
                let bytes=match message {
                    Message::Text(text)=>text.as_bytes().to_vec(),
                    Message::Binary(bytes)=>bytes.to_vec(),
                    Message::Ping(_)|Message::Pong(_)=> {
                        if !matches!(timeout(Duration::from_secs(2),socket.flush()).await,Ok(Ok(()))) {return false;}
                        continue;
                    },
                    _=>return false,
                };
                let Ok(frame)=parse_frame(&bytes) else {return false;};
                match frame {
                    Frame::Delivery{request_id,lease,input,..}=> {
                        if jobs.len()>=usize::from(max) || active.contains_key(&request_id) || active.values().any(|v|v.delivery_id==lease.delivery_id) || input.path.starts_with("//") || input.method=="CONNECT" || input.headers.iter().any(|(name,_)|name.eq_ignore_ascii_case("upgrade")) {return false;}
                        let Some(target)=targets.get(&input.endpoint_id).cloned() else {return false;};
                        active.insert(request_id,lease.clone());
                        let client=client.clone();
                        jobs.spawn(async move {
                            let result=forward(client,target,request_id,lease,input).await;
                            (request_id,result)
                        });
                    },
                    Frame::Heartbeat{lease:None,..}=>{},
                    _=>return false,
                }
            }
        }
    }
}
async fn forward(
    client: Client,
    target: Url,
    request_id: Uuid,
    lease: DeliveryLease,
    input: EventInput,
) -> Result<Frame, ConnectorError> {
    let method = input
        .method
        .parse()
        .map_err(|_| ConnectorError::InvalidDelivery)?;
    let mut request = client.request(method, target);
    for (name, value) in safe_headers(input.headers, true) {
        request = request.header(name, value);
    }
    request = request
        .header("x-relayhub-event-id", lease.event_id.to_string())
        .header("x-relayhub-delivery-id", lease.delivery_id.to_string());
    let mut result = request
        .body(input.body)
        .send()
        .await
        .map_err(|_| ConnectorError::LocalHttp)?;
    if result
        .content_length()
        .is_some_and(|len| len > MAX_BODY_BYTES as u64)
    {
        return Err(ConnectorError::LocalHttp);
    }
    let mut response = HttpResponse {
        status: result.status().as_u16(),
        headers: Vec::new(),
        body: Vec::new(),
    };
    for (name, value) in result.headers() {
        response.headers.push((
            name.to_string(),
            value
                .to_str()
                .map_err(|_| ConnectorError::LocalHttp)?
                .to_owned(),
        ));
    }
    response.headers = safe_headers(response.headers, false);
    while let Some(chunk) = result
        .chunk()
        .await
        .map_err(|_| ConnectorError::LocalHttp)?
    {
        if response.body.len() + chunk.len() > MAX_BODY_BYTES {
            return Err(ConnectorError::LocalHttp);
        }
        response.body.extend_from_slice(&chunk);
    }
    response.validate().map_err(|_| ConnectorError::LocalHttp)?;
    if response.status < 200 {
        return Err(ConnectorError::LocalHttp);
    }
    Ok(Frame::Response {
        version: VERSION,
        request_id,
        event_id: lease.event_id,
        delivery_id: lease.delivery_id,
        attempt_token: lease.attempt_token,
        response,
    })
}
