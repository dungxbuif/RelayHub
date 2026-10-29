#![cfg(unix)]
#[allow(dead_code)]
mod support;

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, Response},
    routing::any,
};
use futures_util::{SinkExt, StreamExt};
use relayhub::{
    delivery::Deliveries,
    events::Events,
    gateway::Gateway,
    identity::Identity,
    presence::Presence,
    protocol::{Accepted, EventInput, Frame, Principal, parse_frame},
    responses::Responses,
};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, process::Stdio, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};
use uuid::Uuid;

type Socket = WebSocketStream<TcpStream>;
type LocalCall = (Request<Vec<u8>>, oneshot::Sender<Response<Body>>);
struct Files(PathBuf);
impl Files {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("relayhub-connector-{}", Uuid::new_v4()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn write(&self, name: &str, data: &str, mode: u32) -> PathBuf {
        use std::io::Write;
        use std::os::unix::fs::OpenOptionsExt;
        let path = self.0.join(name);
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(mode)
            .open(&path)
            .unwrap();
        file.write_all(data.as_bytes()).unwrap();
        path
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct Harness {
    services: support::TestServices,
    identity: Identity,
    events: Events,
    deliveries: Deliveries,
    gateway: Gateway,
    responses: Responses,
    app: Principal,
    agent: Principal,
    endpoint: Uuid,
    token: String,
    files: Files,
    server: TcpListener,
    local: String,
    calls: mpsc::Receiver<LocalCall>,
    local_task: tokio::task::JoinHandle<()>,
}
impl Drop for Harness {
    fn drop(&mut self) {
        self.local_task.abort();
    }
}
impl Harness {
    async fn new() -> Self {
        let services = support::TestServices::start().await;
        let identity = Identity::new(services.postgres.clone());
        identity.migrate().await.unwrap();
        let events = Events::new(services.postgres.clone());
        events.migrate().await.unwrap();
        let (user, workspace, app, endpoint, agent_id, key_id) = (
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        );
        sqlx::query("INSERT INTO identity_users(id,email) VALUES($1,$2)")
            .bind(user)
            .bind(format!("{user}@example.test"))
            .execute(&services.postgres)
            .await
            .unwrap();
        sqlx::query("INSERT INTO identity_workspaces(id,owner_id) VALUES($1,$2)")
            .bind(workspace)
            .bind(user)
            .execute(&services.postgres)
            .await
            .unwrap();
        sqlx::query("INSERT INTO identity_apps(id,workspace_id,name) VALUES($1,$2,'connector')")
            .bind(app)
            .bind(workspace)
            .execute(&services.postgres)
            .await
            .unwrap();
        sqlx::query("INSERT INTO event_endpoints(id,workspace_id,app_id) VALUES($1,$2,$3)")
            .bind(endpoint)
            .bind(workspace)
            .bind(app)
            .execute(&services.postgres)
            .await
            .unwrap();
        let token = format!("test-connector-secret-{}", Uuid::new_v4());
        sqlx::query("INSERT INTO identity_credentials(id,workspace_id,app_id,agent_id,kind,token_hash) VALUES($1,$2,$3,$4,'agent',$5)").bind(key_id).bind(workspace).bind(app).bind(agent_id).bind(Sha256::digest(token.as_bytes()).to_vec()).execute(&services.postgres).await.unwrap();
        let presence = Presence::new(services.redis.clone(), &services.namespace).unwrap();
        let deliveries = Deliveries::new(services.postgres.clone(), presence.clone());
        deliveries.migrate().await.unwrap();
        // The response API's schema belongs to Task 5; no host implementation is replaced here.
        sqlx::raw_sql(include_str!("../migrations/0004_endpoints.sql"))
            .execute(&services.postgres)
            .await
            .unwrap();
        let responses = Responses::new(
            services.postgres.clone(),
            deliveries.clone(),
            services.redis.clone(),
            &services.namespace,
        )
        .unwrap();
        let gateway = Gateway::new(
            Uuid::new_v4(),
            deliveries.clone(),
            presence,
            services.jetstream.clone(),
        );
        let (tx, calls) = mpsc::channel(16);
        let local_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let local = format!(
            "http://{}/fixed?configured=yes",
            local_listener.local_addr().unwrap()
        );
        let router = Router::new().fallback(any(move |request: Request<Body>| {
            let tx = tx.clone();
            async move {
                let (parts, body) = request.into_parts();
                let body = to_bytes(body, 2_097_153).await.unwrap().to_vec();
                let (reply, rx) = oneshot::channel();
                tx.send((Request::from_parts(parts, body), reply))
                    .await
                    .unwrap();
                rx.await.unwrap_or_else(|_| Response::new(Body::empty()))
            }
        }));
        let local_task =
            tokio::spawn(async move { axum::serve(local_listener, router).await.unwrap() });
        Self {
            services,
            identity,
            events,
            deliveries,
            gateway,
            responses,
            app: Principal::Application {
                workspace_id: workspace,
                app_id: app,
                key_id: Uuid::new_v4(),
            },
            agent: Principal::Agent {
                workspace_id: workspace,
                app_id: app,
                agent_id,
                key_id,
            },
            endpoint,
            token,
            files: Files::new(),
            server: TcpListener::bind("127.0.0.1:0").await.unwrap(),
            local,
            calls,
            local_task,
        }
    }
    fn start(&self, target: &str, max: u16) -> Child {
        let Principal::Agent { agent_id, .. } = self.agent else {
            panic!()
        };
        let cred = self.files.write(
            &format!("credential-{}.toml", Uuid::new_v4()),
            &format!("agent_id = \"{agent_id}\"\ntoken = \"{}\"\n", self.token),
            0o600,
        );
        let config=self.files.write(&format!("config-{}.toml",Uuid::new_v4()),&format!("server_url = \"ws://{}/api/v1/socket\"\ncredential_file = {:?}\nmax_in_flight = {max}\n[endpoint_targets]\n\"{}\" = {target:?}\n",self.server.local_addr().unwrap(),cred,self.endpoint),0o600);
        Command::new(env!("CARGO_BIN_EXE_relayhub"))
            .args(["connect", "--config"])
            .arg(config)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap()
    }
    // The handshake callback error type is fixed by tungstenite.
    #[allow(clippy::result_large_err)]
    async fn connect(&self) -> (Socket, Uuid) {
        let (tcp, _) = tokio::time::timeout(Duration::from_secs(5), self.server.accept())
            .await
            .expect("connector must establish authenticated socket (not unimplemented)")
            .unwrap();
        let expected = format!("Bearer {}", self.token);
        let mut socket=tokio_tungstenite::accept_hdr_async(tcp,move |request: &tokio_tungstenite::tungstenite::handshake::server::Request, mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
            assert_eq!(request.uri().path(),"/api/v1/socket");
            assert_eq!(request.headers()["authorization"],expected);
            assert_eq!(request.headers()["sec-websocket-protocol"],"relayhub.v1");
            response.headers_mut().insert("sec-websocket-protocol","relayhub.v1".parse().unwrap());
            Ok(response)
        }).await.unwrap();
        assert_eq!(
            self.identity.authenticate(&self.token).await.unwrap(),
            self.agent
        );
        let Frame::Hello {
            agent_id,
            max_in_flight,
            ..
        } = next_frame(&mut socket).await
        else {
            panic!("expected Hello")
        };
        let Principal::Agent {
            agent_id: expected, ..
        } = self.agent
        else {
            panic!()
        };
        assert_eq!(agent_id, expected);
        assert!((1..=16).contains(&max_in_flight));
        let session = self.gateway.connect(&self.agent).await.unwrap();
        (socket, session)
    }
    async fn accept(&self, body: Vec<u8>) -> Accepted {
        self.events
            .accept_event(
                &self.app,
                EventInput {
                    endpoint_id: self.endpoint,
                    event_type: "test".into(),
                    method: "POST".into(),
                    path: "/remote-path?ignored=true".into(),
                    headers: vec![
                        ("x-test".into(), "one".into()),
                        ("x-test".into(), "two".into()),
                    ],
                    body,
                },
                &Uuid::new_v4().to_string(),
            )
            .await
            .unwrap()
    }
    async fn frame(&self, session: Uuid) -> Frame {
        self.gateway
            .pending(&self.agent, session, 1)
            .await
            .unwrap()
            .pop()
            .expect("durable work claim")
    }
    async fn state(&self, id: Uuid) -> (String, i32, bool) {
        sqlx::query_as(
            "SELECT state,attempt_count,terminal_at IS NOT NULL FROM deliveries WHERE id=$1",
        )
        .bind(id)
        .fetch_one(&self.services.postgres)
        .await
        .unwrap()
    }
    async fn call(&mut self) -> LocalCall {
        tokio::time::timeout(Duration::from_secs(5), self.calls.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn settle(&self, socket: &mut Socket, original: &Frame) {
        let Frame::Delivery {
            request_id: expected,
            lease,
            ..
        } = original
        else {
            panic!()
        };
        let Frame::Response {
            request_id,
            event_id,
            delivery_id,
            attempt_token,
            response,
            ..
        } = next_frame(socket).await
        else {
            panic!("HTTP result must use frozen Response")
        };
        assert_eq!(
            (request_id, event_id, delivery_id, attempt_token),
            (
                *expected,
                lease.event_id,
                lease.delivery_id,
                lease.attempt_token
            )
        );
        self.responses
            .complete_response(&self.agent, lease.clone(), response)
            .await
            .unwrap();
        assert_eq!(
            self.state(delivery_id).await,
            ("succeeded".into(), i32::from(lease.attempt), true)
        );
    }
}
async fn next_frame(socket: &mut Socket) -> Frame {
    loop {
        let message = tokio::time::timeout(Duration::from_secs(5), socket.next())
            .await
            .unwrap()
            .expect("socket closed")
            .unwrap();
        if message.is_text() || message.is_binary() {
            let frame = parse_frame(&message.into_data()).unwrap();
            if !matches!(frame, Frame::Heartbeat { .. }) {
                return frame;
            }
        }
    }
}
async fn send(socket: &mut Socket, frame: &Frame) {
    socket
        .send(Message::Text(serde_json::to_string(frame).unwrap().into()))
        .await
        .unwrap();
}
async fn stopped_without_result(socket: &mut Socket) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(Ok(message)) = socket.next().await {
            if message.is_close() {
                return;
            }
            if message.is_text() {
                assert!(
                    matches!(
                        parse_frame(&message.into_data()).unwrap(),
                        Frame::Heartbeat { .. }
                    ),
                    "unsafe request must never succeed"
                );
            }
        }
    })
    .await
    .expect("invalid delivery should terminate session without settlement");
}
async fn terminate(child: Child) -> std::process::Output {
    assert!(
        Command::new("kill")
            .args(["-TERM", &child.id().unwrap().to_string()])
            .status()
            .await
            .unwrap()
            .success()
    );
    tokio::time::timeout(Duration::from_secs(33), child.wait_with_output())
        .await
        .unwrap()
        .unwrap()
}
fn response(status: u16, body: Vec<u8>) -> Response<Body> {
    Response::builder()
        .status(status)
        .body(Body::from(body))
        .unwrap()
}

// Detects forwarding omissions, URL joining, premature success and credential/header leakage.
#[tokio::test]
async fn local_http_receives_request() {
    let mut h = Harness::new().await;
    let child = h.start(&h.local, 1);
    let (mut socket, session) = h.connect().await;
    let accepted = h.accept(vec![0, 255, 1]).await;
    let mut frame = h.frame(session).await;
    if let Frame::Delivery { input, .. } = &mut frame {
        input.headers.extend([
            ("authorization".into(), "caller-secret".into()),
            ("cookie".into(), "caller-cookie".into()),
            ("connection".into(), "x-remove".into()),
            ("x-remove".into(), "remove".into()),
            ("host".into(), "169.254.169.254".into()),
            ("x-forwarded-host".into(), "metadata.google.internal".into()),
        ]);
    }
    send(&mut socket, &frame).await;
    let (request, reply) = h.call().await;
    assert_eq!(request.method(), "POST");
    assert_eq!(request.uri(), "/fixed?configured=yes");
    assert_eq!(request.body(), &[0, 255, 1]);
    assert_eq!(
        request
            .headers()
            .get_all("x-test")
            .iter()
            .map(|v| v.to_str().unwrap())
            .collect::<Vec<_>>(),
        ["one", "two"]
    );
    for name in [
        "authorization",
        "cookie",
        "connection",
        "x-remove",
        "x-forwarded-host",
    ] {
        assert!(!request.headers().contains_key(name));
    }
    assert_eq!(
        request.headers()["x-relayhub-event-id"],
        accepted.event_id.to_string()
    );
    assert_eq!(
        request.headers()["x-relayhub-delivery-id"],
        accepted.delivery_id.to_string()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(150), next_frame(&mut socket))
            .await
            .is_err(),
        "no settlement before local completion"
    );
    assert_eq!(
        h.state(accepted.delivery_id).await,
        ("in_flight".into(), 1, false)
    );
    let mut result = response(201, vec![255, 0, 2]);
    result
        .headers_mut()
        .append("set-cookie", "a=1".parse().unwrap());
    result
        .headers_mut()
        .append("set-cookie", "b=2".parse().unwrap());
    reply.send(result).unwrap();
    h.settle(&mut socket, &frame).await;
    let persisted: Vec<u8> =
        sqlx::query_scalar("SELECT result FROM webhook_responses WHERE delivery_id=$1")
            .bind(accepted.delivery_id)
            .fetch_one(&h.services.postgres)
            .await
            .unwrap();
    let persisted: relayhub::protocol::HttpResponse = serde_json::from_slice(&persisted).unwrap();
    assert_eq!(persisted.status, 201);
    assert_eq!(persisted.body, [255, 0, 2]);
    assert_eq!(
        persisted
            .headers
            .iter()
            .filter(|(k, _)| k == "set-cookie")
            .count(),
        2
    );
    assert!(terminate(child).await.status.success());
}

// Detects accepting unknown remote target fields or interpreting a path as a destination.
#[tokio::test]
async fn remote_target_override_rejected() {
    let mut h = Harness::new().await;
    for attack in ["target", "input_target", "network_path"] {
        let child = h.start(&h.local, 1);
        let (mut socket, session) = h.connect().await;
        let accepted = h.accept(vec![1]).await;
        let frame = h.frame(session).await;
        let mut value = serde_json::to_value(&frame).unwrap();
        match attack {
            "target" => {
                value["target"] = serde_json::json!("http://169.254.169.254/latest/meta-data")
            }
            "input_target" => value["input"]["target"] = serde_json::json!("http://127.0.0.1:1/"),
            _ => value["input"]["path"] = serde_json::json!("//169.254.169.254/latest/meta-data"),
        }
        socket
            .send(Message::Text(value.to_string().into()))
            .await
            .unwrap();
        stopped_without_result(&mut socket).await;
        assert!(h.calls.try_recv().is_err());
        assert_eq!(
            h.state(accepted.delivery_id).await,
            ("in_flight".into(), 1, false)
        );
        assert!(terminate(child).await.status.success());
    }
}

// Detects following even a same-host redirect or treating it as a second local call.
#[tokio::test]
async fn redirect_not_followed() {
    let mut h = Harness::new().await;
    let child = h.start(&h.local, 1);
    let (mut socket, session) = h.connect().await;
    let accepted = h.accept(vec![1]).await;
    let frame = h.frame(session).await;
    send(&mut socket, &frame).await;
    let (_, reply) = h.call().await;
    let mut result = response(302, vec![]);
    result
        .headers_mut()
        .insert("location", h.local.parse().unwrap());
    reply.send(result).unwrap();
    h.settle(&mut socket, &frame).await;
    assert!(h.calls.try_recv().is_err());
    let stored: Vec<u8> =
        sqlx::query_scalar("SELECT result FROM webhook_responses WHERE delivery_id=$1")
            .bind(accepted.delivery_id)
            .fetch_one(&h.services.postgres)
            .await
            .unwrap();
    assert_eq!(
        serde_json::from_slice::<relayhub::protocol::HttpResponse>(&stored)
            .unwrap()
            .status,
        302
    );
    assert!(terminate(child).await.status.success());
}

// Detects bypassing target validation before opening a socket/claiming durable work.
#[tokio::test]
async fn metadata_target_rejected() {
    let h = Harness::new().await;
    let accepted = h.accept(vec![1]).await;
    for target in [
        "http://169.254.169.254/latest/meta-data/",
        "http://169.254.170.2/",
        "http://100.100.100.200/",
        "http://metadata.google.internal/",
        "http://[fd00:ec2::254]/",
        "http://[::ffff:169.254.169.254]/",
        "http://example.com/",
        "http://8.8.8.8/",
        "http://0.0.0.0/",
        "file:///etc/passwd",
        "http://user:password@127.0.0.1/",
        "http://127.0.0.1/#fragment",
    ] {
        let output = tokio::time::timeout(
            Duration::from_secs(3),
            h.start(target, 1).wait_with_output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            output.status.code(),
            Some(2),
            "invalid target must fail validation"
        );
        assert!(!String::from_utf8_lossy(&output.stderr).contains(target));
        assert_eq!(
            h.state(accepted.delivery_id).await,
            ("pending".into(), 0, false)
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(30), h.server.accept())
                .await
                .is_err()
        );
    }
}

// Detects logging token/body/header values on successful and malformed delivery paths.
#[tokio::test]
async fn secret_not_logged() {
    let mut h = Harness::new().await;
    let child = h.start(&h.local, 1);
    let (mut socket, session) = h.connect().await;
    let a = h.accept(b"private-request-body".to_vec()).await;
    let mut frame = h.frame(session).await;
    if let Frame::Delivery { input, .. } = &mut frame {
        input
            .headers
            .push(("authorization".into(), "private-header-secret".into()));
    }
    send(&mut socket, &frame).await;
    let (_, reply) = h.call().await;
    reply
        .send(response(200, b"private-response-body".to_vec()))
        .unwrap();
    h.settle(&mut socket, &frame).await;
    socket
        .send(Message::Text(
            format!(
                "{{\"version\":1,\"token\":\"{}\",\"body\":\"private-invalid-body\"}}",
                h.token
            )
            .into(),
        ))
        .await
        .unwrap();
    stopped_without_result(&mut socket).await;
    let output = terminate(child).await;
    assert!(output.status.success());
    let logs = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    for secret in [
        &h.token,
        "private-request-body",
        "private-response-body",
        "private-header-secret",
        "private-invalid-body",
        "authorization",
    ] {
        assert!(!logs.contains(secret));
    }
    assert_eq!(h.state(a.delivery_id).await, ("succeeded".into(), 1, true));
}

// Detects losing durable identities, settling on disconnect or reusing fenced attempt tokens.
#[tokio::test]
async fn reconnect_replays_unacked() {
    let mut h = Harness::new().await;
    let child = h.start(&h.local, 1);
    let (mut socket, session) = h.connect().await;
    let accepted = h.accept(vec![9]).await;
    let frame = h.frame(session).await;
    send(&mut socket, &frame).await;
    let (_, reply) = h.call().await;
    drop(socket);
    drop(reply);
    assert_eq!(
        h.state(accepted.delivery_id).await,
        ("in_flight".into(), 1, false)
    );
    sqlx::query(
        "UPDATE deliveries SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(accepted.delivery_id)
    .execute(&h.services.postgres)
    .await
    .unwrap();
    assert!(
        h.deliveries
            .claim_delivery(&h.agent, accepted.delivery_id)
            .await
            .is_err()
    );
    sqlx::query(
        "UPDATE deliveries SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE id=$1",
    )
    .bind(accepted.delivery_id)
    .execute(&h.services.postgres)
    .await
    .unwrap();
    let (mut socket, session) = h.connect().await;
    let replay = h.frame(session).await;
    let (Frame::Delivery { lease: first, .. }, Frame::Delivery { lease: second, .. }) =
        (&frame, &replay)
    else {
        panic!()
    };
    assert_eq!(
        (
            second.event_id,
            second.delivery_id,
            second.generation,
            second.attempt
        ),
        (accepted.event_id, accepted.delivery_id, 1, 2)
    );
    assert_ne!(first.attempt_token, second.attempt_token);
    send(&mut socket, &replay).await;
    let (_, reply) = h.call().await;
    reply.send(response(200, vec![])).unwrap();
    h.settle(&mut socket, &replay).await;
    assert!(terminate(child).await.status.success());
}

// Detects immediate SIGTERM exit, post-signal work starts, unbounded drain and false success.
#[tokio::test]
async fn sigterm_drains() {
    let mut h = Harness::new().await;
    let child = h.start(&h.local, 2);
    let (mut socket, session) = h.connect().await;
    let first = h.accept(vec![1]).await;
    let frame = h.frame(session).await;
    send(&mut socket, &frame).await;
    let (_, reply) = h.call().await;
    let second = h.accept(vec![2]).await;
    let frame2 = h.frame(session).await;
    send(&mut socket, &frame2).await;
    let (_, blocked) = h.call().await;
    let started = tokio::time::Instant::now();
    assert!(
        Command::new("kill")
            .args(["-TERM", &child.id().unwrap().to_string()])
            .status()
            .await
            .unwrap()
            .success()
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    reply.send(response(204, vec![])).unwrap();
    h.settle(&mut socket, &frame).await;
    let third = h.accept(vec![3]).await;
    let frame3 = h.frame(session).await;
    send(&mut socket, &frame3).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(300), h.calls.recv())
            .await
            .is_err(),
        "no new handler after SIGTERM"
    );
    let output = tokio::time::timeout(Duration::from_secs(32), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(output.status.success());
    assert!(
        started.elapsed() >= Duration::from_secs(29),
        "active work gets the 30-second budget"
    );
    assert!(started.elapsed() < Duration::from_secs(32));
    drop(blocked);
    assert_eq!(
        h.state(first.delivery_id).await,
        ("succeeded".into(), 1, true)
    );
    for id in [second.delivery_id, third.delivery_id] {
        assert_eq!(h.state(id).await, ("in_flight".into(), 1, false));
        sqlx::query("UPDATE deliveries SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1").bind(id).execute(&h.services.postgres).await.unwrap();
        assert!(h.deliveries.claim_delivery(&h.agent, id).await.is_err());
        assert_eq!(h.state(id).await, ("retry_wait".into(), 1, false));
    }
}

// Detects insecure files, ambiguous configuration and accidental secret echo by the CLI.
#[tokio::test]
async fn configuration_and_credentials_fail_closed() {
    use std::os::unix::fs::symlink;
    let files = Files::new();
    let version = Command::new(env!("CARGO_BIN_EXE_relayhub"))
        .arg("version")
        .output()
        .await
        .unwrap();
    assert!(version.status.success());
    assert_eq!(
        String::from_utf8(version.stdout).unwrap(),
        "relayhub 0.1.0\n"
    );
    let credential =
        "agent_id = \"11111111-1111-4111-8111-111111111111\"\ntoken = \"private-config-token\"\n";
    files.write("safe.toml", credential, 0o600);
    files.write("readable.toml", credential, 0o644);
    files.write(
        "empty.toml",
        "agent_id = \"11111111-1111-4111-8111-111111111111\"\ntoken = \"\"\n",
        0o600,
    );
    files.write("large.toml", &"x".repeat(4097), 0o600);
    symlink(files.0.join("safe.toml"), files.0.join("link.toml")).unwrap();
    assert!(
        Command::new("mkfifo")
            .arg(files.0.join("pipe"))
            .status()
            .await
            .unwrap()
            .success()
    );
    for (credential, max, server, extra) in [
        ("readable.toml", 1, "ws://127.0.0.1:1/api/v1/socket", ""),
        ("link.toml", 1, "ws://127.0.0.1:1/api/v1/socket", ""),
        ("pipe", 1, "ws://127.0.0.1:1/api/v1/socket", ""),
        ("missing.toml", 1, "ws://127.0.0.1:1/api/v1/socket", ""),
        ("empty.toml", 1, "ws://127.0.0.1:1/api/v1/socket", ""),
        ("large.toml", 1, "ws://127.0.0.1:1/api/v1/socket", ""),
        ("safe.toml", 0, "ws://127.0.0.1:1/api/v1/socket", ""),
        ("safe.toml", 17, "ws://127.0.0.1:1/api/v1/socket", ""),
        ("safe.toml", 1, "ws://example.com/api/v1/socket", ""),
        (
            "safe.toml",
            1,
            "wss://user:private-url-password@example.com/api/v1/socket",
            "",
        ),
        (
            "safe.toml",
            1,
            "wss://example.com/api/v1/socket?token=private-query-token",
            "",
        ),
        (
            "safe.toml",
            1,
            "wss://example.com/api/v1/socket",
            "token = \"private-embedded-token\"\n",
        ),
    ] {
        let config=files.write(&format!("{}.toml",Uuid::new_v4()),&format!("server_url = {server:?}\ncredential_file = {credential:?}\nmax_in_flight = {max}\n{extra}[endpoint_targets]\n\"11111111-1111-4111-8111-111111111111\" = \"http://127.0.0.1:1/\"\n"),0o600);
        let output = tokio::time::timeout(
            Duration::from_secs(3),
            Command::new(env!("CARGO_BIN_EXE_relayhub"))
                .args(["connect", "--config"])
                .arg(config)
                .kill_on_drop(true)
                .output(),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(output.status.code(), Some(2));
        let logs = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        for secret in [
            "private-config-token",
            "private-url-password",
            "private-query-token",
            "private-embedded-token",
        ] {
            assert!(!logs.contains(secret));
        }
    }
}

// Detects off-by-one body limits, chunked-body bypass and malformed frame dispatch.
#[tokio::test]
async fn body_limits_and_invalid_frames_leave_work_recoverable() {
    let mut h = Harness::new().await;
    for case in [
        "exact_limit",
        "large_request",
        "large_response",
        "large_chunked_response",
        "unknown_endpoint",
        "unknown_version",
        "bad_header",
        "upgrade",
        "connect",
        "large_envelope",
    ] {
        let child = h.start(&h.local, 1);
        let (mut socket, session) = h.connect().await;
        let accepted = h
            .accept(vec![7; if case == "exact_limit" { 1_048_576 } else { 1 }])
            .await;
        let mut frame = h.frame(session).await;
        if let Frame::Delivery { input, version, .. } = &mut frame {
            match case {
                "large_request" => input.body = vec![1; 1_048_577],
                "unknown_endpoint" => input.endpoint_id = Uuid::new_v4(),
                "unknown_version" => *version = 2,
                "bad_header" => input.headers.push(("x-bad".into(), "one\r\ntwo".into())),
                "upgrade" => input.headers.push(("upgrade".into(), "websocket".into())),
                "connect" => input.method = "CONNECT".into(),
                "large_envelope" => input
                    .headers
                    .push(("x-large".into(), "a".repeat(2_097_152))),
                _ => {}
            }
        }
        if case == "large_envelope" {
            // The peer can reject the advertised length before our write completes.
            let _ = socket
                .send(Message::Text(serde_json::to_string(&frame).unwrap().into()))
                .await;
        } else {
            send(&mut socket, &frame).await;
        }
        if case == "exact_limit" {
            let (request, reply) = h.call().await;
            assert_eq!(request.body().len(), 1_048_576);
            reply.send(response(200, vec![8; 1_048_576])).unwrap();
            h.settle(&mut socket, &frame).await;
            let stored: Vec<u8> =
                sqlx::query_scalar("SELECT result FROM webhook_responses WHERE delivery_id=$1")
                    .bind(accepted.delivery_id)
                    .fetch_one(&h.services.postgres)
                    .await
                    .unwrap();
            assert_eq!(
                serde_json::from_slice::<relayhub::protocol::HttpResponse>(&stored)
                    .unwrap()
                    .body
                    .len(),
                1_048_576
            );
        } else {
            if case == "large_response" || case == "large_chunked_response" {
                let (_, reply) = h.call().await;
                let body = if case == "large_response" {
                    Body::from(vec![1; 1_048_577])
                } else {
                    Body::from_stream(futures_util::stream::iter([
                        Ok::<_, std::io::Error>(vec![1; 524_288]),
                        Ok(vec![1; 524_289]),
                    ]))
                };
                reply.send(Response::new(body)).unwrap();
            }
            stopped_without_result(&mut socket).await;
            assert!(h.calls.try_recv().is_err());
            assert_eq!(
                h.state(accepted.delivery_id).await,
                ("in_flight".into(), 1, false)
            );
        }
        assert!(terminate(child).await.status.success());
    }
}

// Detects allowing more local handlers than Hello advertises, including duplicated claims.
#[tokio::test]
async fn capacity_and_duplicate_delivery_are_bounded() {
    let mut h = Harness::new().await;
    for duplicate in [false, true] {
        let child = h.start(&h.local, 1);
        let (mut socket, session) = h.connect().await;
        let a = h.accept(vec![1]).await;
        let first = h.frame(session).await;
        send(&mut socket, &first).await;
        let (_, reply) = h.call().await;
        let second = if duplicate {
            first.clone()
        } else {
            h.accept(vec![2]).await;
            h.frame(session).await
        };
        send(&mut socket, &second).await;
        stopped_without_result(&mut socket).await;
        assert!(h.calls.try_recv().is_err());
        drop(reply);
        assert_eq!(h.state(a.delivery_id).await, ("in_flight".into(), 1, false));
        assert!(terminate(child).await.status.success());
    }
}

// Detects cancelling unrelated local work when a result cannot be sent during drain.
#[tokio::test]
async fn shutdown_keeps_draining_after_transport_loss() {
    let mut h = Harness::new().await;
    let mut child = h.start(&h.local, 2);
    let (mut socket, session) = h.connect().await;
    let first = h.accept(vec![1]).await;
    send(&mut socket, &h.frame(session).await).await;
    let (_, reply1) = h.call().await;
    let second = h.accept(vec![2]).await;
    send(&mut socket, &h.frame(session).await).await;
    let (_, reply2) = h.call().await;
    assert!(
        Command::new("kill")
            .args(["-TERM", &child.id().unwrap().to_string()])
            .status()
            .await
            .unwrap()
            .success()
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(socket);
    reply1.send(response(200, vec![5; 1_048_576])).unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        child.try_wait().unwrap().is_none(),
        "drain must finish the other local handler despite socket loss"
    );
    reply2.send(response(204, vec![])).unwrap();
    let output = tokio::time::timeout(Duration::from_secs(3), child.wait_with_output())
        .await
        .unwrap()
        .unwrap();
    assert!(output.status.success());
    for id in [first.delivery_id, second.delivery_id] {
        assert_eq!(h.state(id).await, ("in_flight".into(), 1, false));
    }
}

// Detects TLS provider ambiguity (both ring and aws-lc are enabled elsewhere in this crate).
#[tokio::test]
async fn wss_starts_tls_before_sending_credentials() {
    use tokio::io::AsyncReadExt;
    let files = Files::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    files.write(
        "credentials.toml",
        "agent_id = \"11111111-1111-4111-8111-111111111111\"\ntoken = \"private-tls-token\"\n",
        0o600,
    );
    let config=files.write("config.toml",&format!("server_url = \"wss://{}/api/v1/socket\"\ncredential_file = \"credentials.toml\"\nmax_in_flight = 1\n[endpoint_targets]\n\"11111111-1111-4111-8111-111111111111\" = \"http://127.0.0.1:1/\"\n",listener.local_addr().unwrap()),0o600);
    let child = Command::new(env!("CARGO_BIN_EXE_relayhub"))
        .args(["connect", "--config"])
        .arg(config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let (mut stream, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut prefix = [0u8; 3];
    tokio::time::timeout(Duration::from_secs(3), stream.read_exact(&mut prefix))
        .await
        .unwrap()
        .expect("WSS must start TLS without a crypto-provider panic");
    assert_eq!(prefix, [22, 3, 1]);
    let output = terminate(child).await;
    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("private-tls-token"));
}

// Detects missing total local timeout or lease renewal while the local handler runs.
#[tokio::test]
async fn local_timeout_renews_lease_and_leaves_unacked() {
    let mut h = Harness::new().await;
    let child = h.start(&h.local, 1);
    let (mut socket, session) = h.connect().await;
    let accepted = h.accept(vec![1]).await;
    let frame = h.frame(session).await;
    let Frame::Delivery {
        lease: expected, ..
    } = &frame
    else {
        panic!()
    };
    send(&mut socket, &frame).await;
    let (_, reply) = h.call().await;
    let began = tokio::time::Instant::now();
    let mut presence_heartbeat = false;
    let mut lease_heartbeat = false;
    tokio::time::timeout(Duration::from_secs(32), async {
        while let Some(Ok(message)) = socket.next().await {
            if message.is_close() {
                break;
            }
            if message.is_text() {
                let Frame::Heartbeat { lease, .. } = parse_frame(&message.into_data()).unwrap()
                else {
                    panic!("stalled HTTP must never settle")
                };
                match lease {
                    Some(lease) => {
                        assert_eq!(&lease, expected);
                        h.deliveries.heartbeat(&h.agent, &lease).await.unwrap();
                        lease_heartbeat = true;
                    }
                    None => {
                        h.gateway.heartbeat(&h.agent, session).await.unwrap();
                        presence_heartbeat = true;
                    }
                }
            }
        }
    })
    .await
    .expect("local HTTP timeout must close the attempt in 30 seconds");
    assert!(began.elapsed() >= Duration::from_secs(29));
    assert!(presence_heartbeat && lease_heartbeat);
    assert_eq!(
        h.state(accepted.delivery_id).await,
        ("in_flight".into(), 1, false)
    );
    let renewed:bool=sqlx::query_scalar("SELECT lease_expires_at>attempt_started_at+interval '70 seconds' FROM deliveries WHERE id=$1").bind(accepted.delivery_id).fetch_one(&h.services.postgres).await.unwrap();
    assert!(renewed);
    drop(reply);
    assert!(terminate(child).await.status.success());
}

// Detects forwarding the credential to an HTTP redirect destination during socket setup.
#[tokio::test]
async fn socket_redirect_never_receives_credential() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let files = Files::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let trap = TcpListener::bind("127.0.0.1:0").await.unwrap();
    files.write(
        "credentials.toml",
        "agent_id = \"11111111-1111-4111-8111-111111111111\"\ntoken = \"private-redirect-token\"\n",
        0o600,
    );
    let config=files.write("config.toml",&format!("server_url = \"ws://{}/api/v1/socket\"\ncredential_file = \"credentials.toml\"\nmax_in_flight = 1\n[endpoint_targets]\n\"11111111-1111-4111-8111-111111111111\" = \"http://127.0.0.1:1/\"\n",listener.local_addr().unwrap()),0o600);
    let child = Command::new(env!("CARGO_BIN_EXE_relayhub"))
        .args(["connect", "--config"])
        .arg(config)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let (mut stream, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        bytes.push(
            tokio::time::timeout(Duration::from_secs(3), stream.read_u8())
                .await
                .unwrap()
                .unwrap(),
        );
        assert!(bytes.len() < 4096);
    }
    assert!(
        String::from_utf8(bytes)
            .unwrap()
            .contains("Bearer private-redirect-token")
    );
    stream.write_all(format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: ws://{}/steal\r\nContent-Length: 0\r\n\r\n",trap.local_addr().unwrap()).as_bytes()).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), trap.accept())
            .await
            .is_err()
    );
    let output = terminate(child).await;
    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stderr).contains("private-redirect-token"));
}
