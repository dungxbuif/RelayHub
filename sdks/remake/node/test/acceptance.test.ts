import assert from 'node:assert/strict';
import { spawn, execFileSync, type ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, readFile, writeFile, mkdir, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createInterface } from 'node:readline';
import { setTimeout as delay } from 'node:timers/promises';
import test, { before, after, describe, type TestContext } from 'node:test';
import WebSocket, { WebSocketServer } from 'ws';
import { setImmediate as turn } from 'node:timers/promises';
import type { Delivery, AgentHandle, ProcessingResult } from '../src/client.js';

const root = fileURLToPath(new URL('../../../../', import.meta.url));
const fixtures = JSON.parse(await readFile(join(root, 'contracts/v1/fixtures.json'), 'utf8'));
let temp: string, child: ChildProcess;
let info: { url: string; endpoint: string; agent_id: string; app_token: string; agent_token: string; other_token: string; sibling_token: string };
async function sdk() { return import('../src/client.js').catch(() => assert.fail('Node SDK is not implemented')); }
async function client(agent = false, token?: string) {
  const { RelayHub } = await sdk();
  return new RelayHub({ url: info.url, token: token ?? (agent ? info.agent_token : info.app_token), ...(agent ? { agentId: info.agent_id } : {}) });
}
function input() { return { ...fixtures.event_input, endpoint_id: info.endpoint, body: Buffer.from([0, 255, 127, 128]) }; }
function deferred<T = void>() { let resolve!: (v: T) => void; const promise = new Promise<T>(r => { resolve = r; }); return { promise, resolve }; }
async function waitFor<T>(fn: () => Promise<T>, accept: (v: T) => boolean, ms = 8000): Promise<T> {
  const end = Date.now() + ms;
  do { const v = await fn(); if (accept(v)) return v; await delay(30); } while (Date.now() < end);
  throw Error('observable condition timed out');
}
async function control(action: string, id = '') {
  const r = await fetch(`${info.url}/test/${action}${id ? `?id=${id}` : ''}`, { method: 'POST' });
  assert.equal(r.status, 200); return r.json();
}
async function state(id: string) { return control('state', id); }

// Only transport composition is adapted: all authorization, acceptance, leases, settlement,
// response correlation and realtime are the actual Rust implementations with disposable stores.
const bridge = String.raw`
use axum::{Router,Json,extract::{State,Query,WebSocketUpgrade,ws::{WebSocket,Message}},http::{HeaderMap,StatusCode},response::{IntoResponse,Response},routing::{post,get}};
use relayhub::{identity::Identity,events::Events,delivery::{Deliveries,ProcessingResult},gateway::Gateway,presence::Presence,responses::Responses,realtime::Realtime,protocol::{Frame,Principal,EventInput,parse_frame}};
use serde_json::{json,Value};
use futures_util::SinkExt;
use sha2::{Digest,Sha256};
use sqlx::Row;
use std::{collections::HashMap,sync::Arc,time::Duration};
use tokio::sync::broadcast;
use uuid::Uuid;
#[path = "__SUPPORT__"] mod support;
#[derive(Clone)] struct App { pool: sqlx::PgPool, identity: Identity, events: Events, deliveries: Deliveries, gateway: Gateway, responses: Responses, agent: Principal, disconnect: broadcast::Sender<()> }
async fn auth(a:&App,h:&HeaderMap)->Result<Principal,Response> {
 let token=h.get("authorization").and_then(|v|v.to_str().ok()).and_then(|s|s.strip_prefix("Bearer ")).unwrap_or("");
 a.identity.authenticate(token).await.map_err(IntoResponse::into_response)
}
async fn publish(State(a):State<Arc<App>>,h:HeaderMap,Json(input):Json<EventInput>)->Response {
 let p=match auth(&a,&h).await {Ok(p)=>p,Err(e)=>return e};
 let key=h.get("idempotency-key").and_then(|v|v.to_str().ok()).unwrap_or("");
 match a.events.accept_event(&p,input,key).await {Ok(r)=>(StatusCode::ACCEPTED,Json(r)).into_response(),Err(e)=>e.into_response()}
}
async fn upgrade(State(a):State<Arc<App>>,h:HeaderMap,ws:WebSocketUpgrade)->Response {
 let p=match auth(&a,&h).await {Ok(p)=>p,Err(e)=>return e};
 if !matches!(p,Principal::Agent{..}) {return StatusCode::FORBIDDEN.into_response()}
 if h.get("sec-websocket-protocol").and_then(|s|s.to_str().ok())!=Some("relayhub.v1") {return StatusCode::UNPROCESSABLE_ENTITY.into_response()}
 ws.protocols(["relayhub.v1"]).max_message_size(2_097_152).on_upgrade(move|s|socket(a,p,s))
}
fn decode(m:Message)->Option<Frame> {match m {Message::Text(t)=>parse_frame(t.as_bytes()).ok(),Message::Binary(b)=>parse_frame(&b).ok(),_=>None}}
async fn socket(a:Arc<App>,p:Principal,mut s:WebSocket) {
 let Some(Ok(m))=s.recv().await else{return};
 let Some(Frame::Hello{agent_id,max_in_flight,..})=decode(m) else{return};
 if !matches!(&p,Principal::Agent{agent_id:id,..} if *id==agent_id) {return}
 let sid=a.gateway.connect(&p).await.unwrap();
 let mut outstanding=HashMap::new(); let mut scan=tokio::time::interval(Duration::from_millis(40)); let mut disconnect=a.disconnect.subscribe();
 loop {tokio::select! {
  _=disconnect.recv()=>break,
  _=scan.tick()=>{
   let frames=match a.gateway.pending(&p,sid,max_in_flight.saturating_sub(outstanding.len() as u16)).await {Ok(f)=>f,Err(_)=>break};
   for frame in frames {if let Frame::Delivery{request_id,ref lease,..}=frame {outstanding.insert(request_id,lease.clone());}
    if s.send(Message::Text(serde_json::to_string(&frame).unwrap().into())).await.is_err(){break}
   }
  },
  m=s.recv()=>{
   let Some(Ok(m))=m else {break}; if matches!(m,Message::Ping(_)|Message::Pong(_)){continue}
   let Some(f)=decode(m) else{break};
   match f {
    Frame::Ack{request_id,event_id,delivery_id,attempt_token,..}|Frame::Response{request_id,event_id,delivery_id,attempt_token,..}=>{
     let Some(lease)=outstanding.remove(&request_id) else{break};
     if (lease.event_id,lease.delivery_id,lease.attempt_token)!=(event_id,delivery_id,attempt_token){break}
     let ok=if let Frame::Response{response,..}=f {a.responses.complete_response(&p,lease,response).await.is_ok()} else {a.deliveries.settle_delivery(&p,lease,ProcessingResult::Success).await.is_ok()};
     if !ok {break}
    },
    Frame::Heartbeat{lease,..}=>{if a.gateway.heartbeat(&p,sid).await.is_err(){break} if let Some(l)=lease {if !outstanding.values().any(|v|v==&l)||a.deliveries.heartbeat(&p,&l).await.is_err(){break}}},
    _=>break
   }
  }
 }}
 let _=s.close().await; let _=a.gateway.disconnect(&p,sid).await;
}
async fn control(State(a):State<Arc<App>>,axum::extract::Path(action):axum::extract::Path<String>,Query(q):Query<HashMap<String,String>>)->Json<Value> {
 let id=q.get("id").map(|v|Uuid::parse_str(v).unwrap());
 match action.as_str() {
  "disconnect"=>{let _=a.disconnect.send(());Json(json!({}))},
  "recover"=>{let id=id.unwrap();sqlx::query("UPDATE deliveries SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1").bind(id).execute(&a.pool).await.unwrap();assert!(a.deliveries.claim_delivery(&a.agent,id).await.is_err());Json(json!({}))},
  "due"=>{sqlx::query("UPDATE deliveries SET next_attempt_at=clock_timestamp()-interval '1 second' WHERE id=$1").bind(id.unwrap()).execute(&a.pool).await.unwrap();Json(json!({}))},
  "counts"=>{let (e,d,o):(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM events),(SELECT count(*) FROM deliveries),(SELECT count(*) FROM event_outbox)").fetch_one(&a.pool).await.unwrap();Json(json!([e,d,o]))},
  "quota"=>{sqlx::query("UPDATE workspace_quotas SET outstanding_deliveries=10000 WHERE workspace_id=(SELECT workspace_id FROM event_endpoints WHERE id=$1)").bind(id.unwrap()).execute(&a.pool).await.unwrap();Json(json!({}))},
  "state"=>{let row=sqlx::query("SELECT d.state,d.attempt_count,d.generation,d.terminal_at IS NOT NULL AS terminal, e.payload,r.result,(SELECT count(*) FROM delivery_attempts a WHERE a.delivery_id=d.id AND a.outcome='lease_expired') AS expired FROM deliveries d JOIN events e ON e.id=d.event_id LEFT JOIN webhook_responses r ON r.delivery_id=d.id WHERE d.id=$1").bind(id.unwrap()).fetch_one(&a.pool).await.unwrap();let result:Option<Vec<u8>>=row.get("result");Json(json!({"state":row.get::<String,_>("state"),"attempt":row.get::<i32,_>("attempt_count"),"generation":row.get::<i32,_>("generation"),"terminal":row.get::<bool,_>("terminal"),"expired":row.get::<i64,_>("expired"),"input":serde_json::from_slice::<Value>(&row.get::<Vec<u8>,_>("payload")).unwrap(),"response":result.map(|v|serde_json::from_slice::<Value>(&v).unwrap())}))},
  _=>panic!("unknown test control")
 }
}
async fn seed(pool:&sqlx::PgPool,workspace:Uuid,app:Uuid,agent:Option<Uuid>,token:&str)->Uuid {
 let key=Uuid::new_v4();sqlx::query("INSERT INTO identity_credentials(id,workspace_id,app_id,agent_id,kind,token_hash) VALUES($1,$2,$3,$4,$5,$6)").bind(key).bind(workspace).bind(app).bind(agent).bind(if agent.is_some(){"agent"}else{"application"}).bind(Sha256::digest(token.as_bytes()).to_vec()).execute(pool).await.unwrap();key
}
#[tokio::main] async fn main(){
 let services=support::TestServices::start().await;let pool=services.postgres.clone();
 let identity=Identity::new(pool.clone());identity.migrate().await.unwrap();let events=Events::new(pool.clone());events.migrate().await.unwrap();
 let presence=Presence::new(services.redis.clone(),&services.namespace).unwrap();let deliveries=Deliveries::new(pool.clone(),presence.clone());deliveries.migrate().await.unwrap();
 sqlx::raw_sql(include_str!("__MIGRATION__")).execute(&pool).await.unwrap();
 let mut scopes=Vec::new();
 for _ in 0..2 {let user=Uuid::new_v4();let workspace=Uuid::new_v4();let app=Uuid::new_v4();
 sqlx::query("INSERT INTO identity_users(id,email) VALUES($1,$2)").bind(user).bind(format!("{user}@example.test")).execute(&pool).await.unwrap();
 sqlx::query("INSERT INTO identity_workspaces(id,owner_id) VALUES($1,$2)").bind(workspace).bind(user).execute(&pool).await.unwrap();
 sqlx::query("INSERT INTO identity_apps(id,workspace_id,name) VALUES($1,$2,'node-test')").bind(app).bind(workspace).execute(&pool).await.unwrap();scopes.push((workspace,app));}
 let (workspace,app)=scopes[0];let endpoint=Uuid::new_v4();let agent_id=Uuid::new_v4();
 sqlx::query("INSERT INTO event_endpoints(id,workspace_id,app_id) VALUES($1,$2,$3)").bind(endpoint).bind(workspace).bind(app).execute(&pool).await.unwrap();
 let app_token=Uuid::new_v4().to_string();let agent_token=Uuid::new_v4().to_string();let other_token=Uuid::new_v4().to_string();let sibling_token=Uuid::new_v4().to_string();
 seed(&pool,workspace,app,None,&app_token).await;let key=seed(&pool,workspace,app,Some(agent_id),&agent_token).await;seed(&pool,scopes[1].0,scopes[1].1,None,&other_token).await;
 let sibling=Uuid::new_v4();sqlx::query("INSERT INTO identity_apps(id,workspace_id,name) VALUES($1,$2,'sibling')").bind(sibling).bind(workspace).execute(&pool).await.unwrap();seed(&pool,workspace,sibling,None,&sibling_token).await;
 let agent=Principal::Agent{workspace_id:workspace,app_id:app,agent_id,key_id:key};
 let gateway=Gateway::new(Uuid::new_v4(),deliveries.clone(),presence,services.jetstream.clone());let responses=Responses::new(pool.clone(),deliveries.clone(),services.redis.clone(),&services.namespace).unwrap();
 let output=std::process::Command::new("docker").args(["compose","--env-file","/dev/null","-f","__COMPOSE__","-p",&services.namespace,"port","nats","4222"]).output().unwrap();
 let nats=async_nats::connect(format!("nats://{}",String::from_utf8(output.stdout).unwrap().trim())).await.unwrap();let realtime=Realtime::new(Uuid::new_v4(),nats,services.redis.clone(),&services.namespace).unwrap();
 let a=Arc::new(App{pool,identity:identity.clone(),events,deliveries,gateway:gateway.clone(),responses,agent,disconnect:broadcast::channel(16).0});
 let router=Router::new().route("/api/v1/events",post(publish)).route("/api/v1/socket",get(upgrade)).route("/test/{action}",post(control)).layer(axum::extract::DefaultBodyLimit::max(2_097_152)).with_state(a).merge(realtime.router(identity.clone())).nest("/actual",gateway.router(identity));
 let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();println!("{}",json!({"url":format!("http://{}",listener.local_addr().unwrap()),"endpoint":endpoint,"agent_id":agent_id,"app_token":app_token,"agent_token":agent_token,"other_token":other_token,"sibling_token":sibling_token}));
 axum::serve(listener,router).with_graceful_shutdown(async{tokio::signal::ctrl_c().await.unwrap()}).await.unwrap();drop(services);
}
`;

describe('Rust acceptance', () => {
before(async () => {
  temp = await mkdtemp(join(tmpdir(), 'relayhub-node-test-'));
  await mkdir(join(temp, 'src'));
  // Reuse the locked Rust dependency graph, the Task 1 service harness and migrations unchanged.
  const manifest = (await readFile(join(root, 'rust/Cargo.toml'), 'utf8')).replace('name = "relayhub"', 'name = "relayhub-node-contract-adapter"').replace('[dependencies]', `[dependencies]\nrelayhub = { path = ${JSON.stringify(join(root, 'rust'))} }`);
  await writeFile(join(temp, 'Cargo.toml'), manifest);
  await writeFile(join(temp, 'Cargo.lock'), await readFile(join(root, 'rust/Cargo.lock')));
  // include_str!/env! in the original harness need its original Cargo manifest directory.
  const support = (await readFile(join(root, 'rust/tests/support/mod.rs'), 'utf8')).replace('concat!(env!("CARGO_MANIFEST_DIR"), "/tests/compose.yaml")', JSON.stringify(join(root, 'rust/tests/compose.yaml')));
  await writeFile(join(temp, 'support.rs'), support);
  await writeFile(join(temp, 'src/main.rs'), bridge.replace('__SUPPORT__', join(temp, 'support.rs')).replace('__MIGRATION__', join(root, 'rust/migrations/0004_endpoints.sql')).replace('__COMPOSE__', join(root, 'rust/tests/compose.yaml')));
  const target = join(root, 'rust/target');
  execFileSync('cargo', ['build', '--offline', '--manifest-path', join(temp, 'Cargo.toml'), '--target-dir', target], { timeout: 180_000, stdio: ['ignore', 'ignore', 'pipe'] });
  child = spawn(join(target, 'debug/relayhub-node-contract-adapter'), [], { stdio: ['ignore', 'pipe', 'pipe'] });
  let stderr = ''; child.stderr!.on('data', b => { stderr += b.toString(); });
  const lines = createInterface({ input: child.stdout! });
  const ready = once(lines, 'line').then(([line]) => JSON.parse(line));
  info = await Promise.race([ready, once(child, 'exit').then(() => { throw Error(`Rust adapter exited: ${stderr}`); }), delay(90_000, undefined, { ref: false }).then(() => { throw Error('Rust adapter readiness timeout'); })]);
  lines.close();
  assert.deepEqual(await control('counts'), [0, 0, 0]);
}, { timeout: 280_000 });
after(async () => {
  if (child && child.exitCode === null) { const exit = once(child, 'exit'); child.kill('SIGINT'); await exit; }
  if (temp) await rm(temp, { recursive: true, force: true });
});

test('publish_is_durable_idempotent_and_authorized', async () => {
  const c = await client(); const key = crypto.randomUUID(); const before = await control('counts');
  const accepted = await c.publish(input(), key);
  assert.equal(accepted.status, 202);
  assert.deepEqual(await c.publish(input(), key), accepted);
  assert.deepEqual(await control('counts'), before.map((v: number) => v + 1));
  const row = await state(accepted.delivery_id);
  assert.equal(row.state, 'pending'); assert.equal(row.attempt, 0);
  assert.deepEqual(row.input, { ...fixtures.event_input, endpoint_id: info.endpoint });
  await assert.rejects(c.publish({ ...input(), body: Buffer.from('different') }, key), { status: 409 });
  await assert.rejects((await client(false, info.other_token)).publish(input(), crypto.randomUUID()), { status: 403 });
  await assert.rejects((await client(false, 'invalid-test-token')).publish(input(), crypto.randomUUID()), { status: 401 });
  await assert.rejects(c.publish({ ...input(), body: Buffer.alloc(1_048_577) }, crypto.randomUUID()), { status: 413 });
  await assert.rejects(c.publish(input(), ''), { status: 422 });
  assert.deepEqual(await control('counts'), before.map((v: number) => v + 1));
  // Consume this test's event so subsequent handlers see only their own work.
  const a = await (await client(true)).connectAgent(async () => undefined);
  await waitFor(() => state(accepted.delivery_id), r => r.state === 'succeeded'); await a.close();
});

test('handler_success_acks', async () => {
  const accepted = await (await client()).publish(input(), crypto.randomUUID()); const seen = deferred<Delivery>(); const release = deferred();
  // Exercise the unadapted Task 4 ACK socket, too.
  const { RelayHub } = await sdk();
  const c = new RelayHub({ url: info.url + '/actual', token: info.agent_token, agentId: info.agent_id });
  const a = await c.connectAgent(async d => { seen.resolve(d); await release.promise; });
  const d = await seen.promise;
  assert.equal(d.lease.event_id, accepted.event_id); assert.equal(d.lease.delivery_id, accepted.delivery_id);
  assert.equal(d.lease.attempt, 1); assert.equal(d.lease.generation, 1);
  assert.deepEqual([...d.input.body], [0, 255, 127, 128]);
  assert.equal((await state(accepted.delivery_id)).state, 'in_flight');
  release.resolve();
  const row = await waitFor(() => state(accepted.delivery_id), r => r.state === 'succeeded');
  assert.equal(row.terminal, true); assert.equal(row.response, null); await a.close();
});

test('typed_response_settles_once', async () => {
  const accepted = await (await client()).publish(input(), crypto.randomUUID());
  const a = await (await client(true)).connectAgent(async () => ({ ...fixtures.http_response, body: Buffer.from([0, 255, 127, 128]) }));
  const row = await waitFor(() => state(accepted.delivery_id), r => r.state === 'succeeded');
  assert.deepEqual(row.response, fixtures.http_response); assert.equal(row.attempt, 1); await a.close();
});

test('thrown_handler_nacks', async () => {
  const accepted = await (await client()).publish(input(), crypto.randomUUID()); const called = deferred();
  const a = await (await client(true)).connectAgent(async () => { called.resolve(); throw Error('private-business-error'); });
  await called.promise; await a.close();
  const row = await state(accepted.delivery_id);
  assert.equal(row.state, 'in_flight'); assert.equal(row.terminal, false); assert.equal(row.response, null);
  // No NACK exists: expire the unacked lease and let Task 4 reconcile to retry_wait.
  await control('recover', accepted.delivery_id);
  const retry = await state(accepted.delivery_id); assert.equal(retry.state, 'retry_wait'); assert.equal(retry.expired, 1);
  await control('due', accepted.delivery_id);
  const b = await (await client(true)).connectAgent(async () => undefined);
  await waitFor(() => state(accepted.delivery_id), r => r.state === 'succeeded'); await b.close();
});

test('reconnect_redelivers_same_event_id', async () => {
  const accepted = await (await client()).publish(input(), crypto.randomUUID()); const first = deferred<Delivery>(); const second = deferred<Delivery>(); const release = deferred();
  const a = await (await client(true)).connectAgent(async d => {
    if (d.lease.attempt === 1) { first.resolve(d); await release.promise; } else second.resolve(d);
  });
  const d1 = await first.promise; await control('disconnect'); await delay(100);
  await control('recover', accepted.delivery_id); assert.equal((await state(accepted.delivery_id)).state, 'retry_wait');
  await control('due', accepted.delivery_id); release.resolve();
  const d2 = await second.promise;
  assert.equal(d2.lease.event_id, d1.lease.event_id); assert.equal(d2.lease.delivery_id, d1.lease.delivery_id);
  assert.equal(d2.lease.generation, d1.lease.generation); assert.equal(d2.lease.attempt, 2);
  assert.notEqual(d2.lease.attempt_token, d1.lease.attempt_token);
  const row = await waitFor(() => state(accepted.delivery_id), r => r.state === 'succeeded'); assert.equal(row.expired, 1); await a.close();
});

test('redelivery_admits_new_attempt_while_old_handler_pending', async () => {
  const accepted = await (await client()).publish(input(), crypto.randomUUID());
  const first = deferred<Delivery>(); const second = deferred<Delivery>(); const releaseOld = deferred();
  const a = await (await client(true)).connectAgent(async d => {
    if (d.lease.attempt === 1) { first.resolve(d); await releaseOld.promise; }
    else second.resolve(d);
  });
  const d1 = await first.promise;
  await control('disconnect'); await delay(100);
  await control('recover', accepted.delivery_id); assert.equal((await state(accepted.delivery_id)).state, 'retry_wait');
  await control('due', accepted.delivery_id);
  const d2 = await Promise.race([second.promise, delay(3000).then(() => { throw Error('fresh attempt was blocked by old handler'); })]);
  assert.equal(d2.lease.event_id, d1.lease.event_id);
  assert.equal(d2.lease.delivery_id, d1.lease.delivery_id);
  assert.equal(d2.lease.generation, d1.lease.generation);
  assert.equal(d2.lease.attempt, 2);
  assert.notEqual(d2.lease.attempt_token, d1.lease.attempt_token);
  releaseOld.resolve();
  await waitFor(() => state(accepted.delivery_id), r => r.state === 'succeeded'); await a.close();
});

test('shutdown_drains', async () => {
  const c = await client(); const first = await c.publish(input(), crypto.randomUUID()); const started = deferred(); const release = deferred(); let calls = 0;
  const a = await (await client(true)).connectAgent(async () => { calls++; started.resolve(); await release.promise; });
  await started.promise;
  let closed = false; const closing = a.close().then(() => { closed = true; });
  const later = await c.publish(input(), crypto.randomUUID()); await delay(100);
  assert.equal(closed, false); assert.equal(calls, 1); release.resolve(); await closing; await a.close();
  await waitFor(() => state(first.delivery_id), r => r.state === 'succeeded');
  const row = await state(later.delivery_id); assert.notEqual(row.state, 'succeeded'); assert.equal(row.terminal, false);
  if (row.state === 'in_flight') { await control('recover', later.delivery_id); await control('due', later.delivery_id); }
  const b = await (await client(true)).connectAgent(async () => undefined); await waitFor(() => state(later.delivery_id), r => r.state === 'succeeded'); await b.close();
});

test('realtime_subscription_scoped', async () => {
  const c = await client(); const other = await client(false, info.other_token); const sibling = await client(false, info.sibling_token);
  const before = await control('counts');
  assert.deepEqual(await c.publishRealtime('orders.updated', Buffer.from('past')), { status: 202, observed_recipients: 0 });
  const messages: Buffer[] = [], foreign: Buffer[] = [], siblingMessages: Buffer[] = [];
  const subscriptions: AgentHandle[] = [];
  try {
    subscriptions.push(await c.subscribe('orders.updated', f => { messages.push(f.body); }));
    subscriptions.push(await other.subscribe('orders.updated', f => { foreign.push(f.body); }));
    subscriptions.push(await sibling.subscribe('orders.updated', f => { siblingMessages.push(f.body); }));
    await delay(100); assert.equal(messages.length, 0);
    assert.deepEqual(await c.publishRealtime('orders.updated', Buffer.from([0, 255, 127, 128])), { status: 202, observed_recipients: 1 });
    await waitFor(async () => messages.length, n => n === 1); await delay(100);
    assert.deepEqual([...messages[0]], [0, 255, 127, 128]); assert.equal(foreign.length, 0); assert.equal(siblingMessages.length, 0);
    await assert.rejects(c.subscribe('orders.*', () => {}), { status: 422 });
    await assert.rejects(c.publishRealtime('orders.updated', Buffer.alloc(1_048_577)), { status: 413 });
    assert.deepEqual(await control('counts'), before);
  } finally { await Promise.all(subscriptions.map(s => s.close())); }
  const replay: Buffer[] = []; const sub = await c.subscribe('orders.updated', f => { replay.push(f.body); });
  await delay(150); assert.equal(replay.length, 0); await sub.close();
});

test('quota_rejects_before_commit', async () => {
  const c = await client(); const before = await control('counts'); await control('quota', info.endpoint);
  await assert.rejects(c.publish(input(), crypto.randomUUID()), { status: 429 });
  assert.deepEqual(await control('counts'), before);
});

});

// Inject messages synchronously at the websocket boundary to pin the microtask
// admission race without depending on TCP packet grouping or a production host.
async function drainAgent(t: TestContext, handler: (d: Delivery) => Promise<ProcessingResult>) {
  const server = new WebSocketServer({ host: '127.0.0.1', port: 0, handleProtocols: () => 'relayhub.v1' });
  await once(server, 'listening');
  let connections = 0, transport!: WebSocket;
  server.on('connection', () => { connections++; });
  const connected = once(server, 'connection');
  const emit = WebSocket.prototype.emit;
  const capture = t.mock.method(WebSocket.prototype, 'emit', function(this: WebSocket, event: string, ...args: unknown[]) {
    if (event === 'open') transport = this;
    return emit.call(this, event, ...args);
  });
  const address = server.address(); assert.ok(address && typeof address !== 'string');
  const { RelayHub } = await sdk();
  const agent = await new RelayHub({ url: `http://127.0.0.1:${address.port}`, token: 'synthetic-test-token', agentId: fixtures.frames[0].agent_id }).connectAgent(handler);
  capture.mock.restore();
  const [peer] = await connected as [WebSocket];
  const frames: Record<string, any>[] = [];
  peer.on('message', data => frames.push(JSON.parse(data.toString())));
  t.mock.timers.enable({ apis: ['setTimeout'] });
  t.after(async () => {
    const closing = agent.close(); t.mock.timers.tick(30_000);
    transport.terminate(); peer.terminate(); await closing;
    t.mock.timers.reset();
    await new Promise<void>(resolve => server.close(() => resolve()));
  });
  return {
    agent, transport, peer, frames, connections: () => connections,
    delivery: () => transport.emit('message', Buffer.from(JSON.stringify(fixtures.frames[1]))),
    drain: (deadline_ms = 1000) => transport.emit('message', Buffer.from(JSON.stringify({ version: 1, request_id: fixtures.frames[0].request_id, type: 'drain', deadline_ms }))),
  };
}

test('drain_idle_closes_without_reconnect', async t => {
  const s = await drainAgent(t, async () => assert.fail('idle handler'));
  const closed = once(s.transport, 'close'); s.drain(); await closed;
  t.mock.timers.tick(60_000); await turn(); await turn();
  assert.equal(s.connections(), 1);
  assert.equal(s.frames.filter(f => f.type !== 'hello').length, 0, 'Drain has no ACK');
  await s.agent.close();
});

for (const response of [false, true]) test(`drain_active_settles_${response ? 'response' : 'ack'}`, async t => {
  const release = deferred(); let calls = 0;
  const s = await drainAgent(t, async () => { calls++; await release.promise; return response ? { ...fixtures.http_response, body: Buffer.from([0, 255, 127, 128]) } : undefined; });
  s.delivery(); await turn(); assert.equal(calls, 1);
  s.drain(); s.delivery(); await turn();
  assert.equal(s.transport.readyState, WebSocket.OPEN, 'active handler retains settlement window');
  const closed = once(s.transport, 'close'); release.resolve(); await closed;
  assert.equal(calls, 1);
  const settled = s.frames.filter(f => f.type === 'ack' || f.type === 'response');
  assert.equal(settled.length, 1); assert.equal(settled[0].type, response ? 'response' : 'ack');
  assert.equal(settled[0].attempt_token, fixtures.frames[1].lease.attempt_token);
  assert.equal(settled[0].request_id, fixtures.frames[1].request_id);
  if (response) assert.deepEqual(settled[0].response, fixtures.http_response);
  t.mock.timers.tick(60_000); await turn(); assert.equal(s.connections(), 1);
  await s.agent.close();
});

test('drain_queued_handler_never_starts', async t => {
  let calls = 0; const s = await drainAgent(t, async () => { calls++; });
  const closed = once(s.transport, 'close');
  s.delivery(); s.drain(); await closed;
  assert.equal(calls, 0);
  assert.equal(s.frames.filter(f => f.type === 'ack' || f.type === 'response').length, 0);
});

test('drain_deadline_closes_unfinished_handler_and_bounds_local_close', async t => {
  const release = deferred();
  const s = await drainAgent(t, async () => { await release.promise; });
  s.delivery(); await turn(); s.drain(100);
  let closed = false; const closing = s.agent.close().then(() => { closed = true; });
  t.mock.timers.tick(99); await turn();
  assert.equal(s.transport.readyState, WebSocket.OPEN); assert.equal(closed, false);
  t.mock.timers.tick(1); await closing; await turn();
  assert.equal(s.transport.readyState, WebSocket.CLOSED);
  release.resolve(); await turn();
  assert.equal(s.frames.filter(f => f.type === 'ack' || f.type === 'response').length, 0);
  t.mock.timers.tick(60_000); await turn(); assert.equal(s.connections(), 1);
});

test('drain_deadline_includes_close_handshake', async t => {
  const release = deferred(); const s = await drainAgent(t, async () => { await release.promise; });
  s.delivery(); await turn(); s.peer.pause(); s.drain(100);
  t.mock.timers.tick(90); release.resolve(); await turn(); await turn();
  assert.equal(s.transport.readyState, WebSocket.CLOSING);
  t.mock.timers.tick(10); await s.agent.close(); await turn();
  assert.equal(s.transport.readyState, WebSocket.CLOSED);
  s.peer.resume();
});
