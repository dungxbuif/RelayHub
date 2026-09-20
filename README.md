# RelayHub

RelayHub connects applications with durable events, signed HTTP callbacks,
standard WebSockets, routing rules, realtime channels and short remote function
calls. One Go image runs the API and worker. The v1 runtime uses PostgreSQL for
control/state, private NATS JetStream for delivery and Redis for shared ephemeral
sessions, ownership and rate limits. Queue v2 adds named, app-scoped HTTP batch
pull with fenced leases and explicit settlement while preserving v1 delivery.
The API also serves the complete
embedded Admin application at `/admin/`; Docusaurus documentation is built and
deployed separately under `/docs/`.

## Start a homelab stack

Install Docker with Compose v2 or newer, Git and Python 3. From a terminal:

```bash
git clone https://github.com/dungxbuif/RelayHub.git
cd RelayHub
cp .env.example .env
python3 - <<'PY'
from pathlib import Path
import secrets
p = Path('.env')
p.chmod(0o600)
text = p.read_text()
for key in ('RELAYHUB_ADMIN_TOKEN', 'RELAYHUB_SIGNING_SECRET', 'RELAYHUB_POSTGRES_PASSWORD', 'RELAYHUB_NATS_USERNAME', 'RELAYHUB_NATS_PASSWORD', 'RELAYHUB_REDIS_PASSWORD'):
    text = text.replace(key + '=\n', key + '=' + secrets.token_hex(32) + '\n')
p.write_text(text)
PY
docker compose up --build -d --wait --wait-timeout 120
docker compose ps
curl --fail http://localhost:8080/readyz
```

Open `http://localhost:8080/admin/` after the stack is healthy and sign in with
`RELAYHUB_ADMIN_TOKEN`. The browser exchanges this bootstrap credential for a
revocable cluster-wide session and never stores it. Production must terminate TLS
before RelayHub because the Admin session cookie is `Secure`.

The Admin Overview reads real cluster data: rolling request/status/event/NATS
series, live API replica and WebSocket totals, PostgreSQL delivery state, oldest
pending age and persisted delivery-latency percentiles. Events, Dead Letters and
Audit Logs provide allowlisted filters and opaque cursor pagination. Event detail
reconstructs persisted delivery timelines. Dead Letters supports audited single
or explicit batch replay (maximum 100), with confirmation and idempotent retries.
Replay advances the failed delivery generation; it does not publish a new event.
Apps and Routing Rules are managed directly in Admin, including one-time create or
rotation credentials. Realtime Studio issues five-minute app-scoped tokens on the
server and exercises realtime or durable stream sockets without exposing HMAC
credentials to browser code or exported frame logs.

Realtime v2 negotiates `relayhub.realtime.v2` and adds exact or bounded namespace
ACLs, subscribe/unsubscribe, bidirectional and batch publish,
`all`/`others`/connection/client targeting, ephemeral presence/occupancy,
Redis-backed bounded history/rewind and connection ownership, plus cross-replica
NATS routing. Admin can inspect app-scoped live connections and
disconnect the owning gateway. Omitting the subprotocol preserves v1 clients.
The official Go and TypeScript SDKs include Realtime v2 contracts and Queue v2
workers with lease heartbeat and graceful drain. Python SDK work is intentionally
out of scope.

Keep `.env` private and back it up securely. The example contains empty required
credentials; each installation generates its own. Compose publishes API 8080 only.
Set `RELAYHUB_PORT=127.0.0.1:8080` for a proxy on the same host, or restrict access
with your host firewall before exposing the default published port. PostgreSQL,
NATS, Redis and worker operations stay inside the project network. Redis
credentials are separate from host addresses; generate the password with
`openssl rand -hex 32` and never place credentials in `RELAYHUB_REDIS_ADDRS`.
Build/deploy `web/docs` separately for official integration instructions.
API replicas require no sticky sessions. Standalone Redis is the local default;
Sentinel and Cluster (database zero) are supported through `RELAYHUB_REDIS_*`
settings, with private ACL/TLS endpoints expected in production.

## Send your first signed event

This repeatable Python example creates a producer and consumer, signs the exact
request bytes, publishes an event, leases it and acknowledges it. Credentials stay
in the Python process. It prints only the generated event ID and outcome.

```bash
python3 - <<'PY'
import hashlib, hmac, json, time, uuid, urllib.request
from pathlib import Path
settings = dict(line.split('=', 1) for line in Path('.env').read_text().splitlines()
                if line and not line.startswith('#') and '=' in line)
base = 'http://localhost:' + settings.get('RELAYHUB_PORT', '8080').split(':')[-1]
def call(method, target, value=None, app=None, key=None):
    body = b'' if value is None else json.dumps(value, separators=(',', ':')).encode()
    headers = {'Content-Type': 'application/json'}
    if app:
        ts = str(int(time.time()))
        canonical = '\n'.join((ts, method, target, hashlib.sha256(body).hexdigest()))
        headers.update({'X-RelayHub-Api-Key': app['api_key'], 'X-RelayHub-Timestamp': ts,
            'X-RelayHub-Signature': hmac.new(app['hmac_secret'].encode(), canonical.encode(), hashlib.sha256).hexdigest()})
    else:
        headers['Authorization'] = 'Bearer ' + settings['RELAYHUB_ADMIN_TOKEN']
    if key: headers['Idempotency-Key'] = key
    request = urllib.request.Request(base + target, data=body if method != 'GET' else None,
                                     headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=35) as response:
        data = response.read()
        return json.loads(data) if data else None
suffix = uuid.uuid4().hex[:8]
producer = call('POST', '/api/v1/apps', {'name': 'demo-producer-' + suffix, 'delivery_mode': 'websocket'})
consumer = call('POST', '/api/v1/apps', {'name': 'demo-consumer-' + suffix, 'delivery_mode': 'websocket'})
call('POST', '/api/v1/routing/rules', {'source_app_id': producer['app_id'],
    'event_type': 'demo.created', 'target_app_id': consumer['app_id'],
    'realtime_channel': 'demo.live'})
published = call('POST', '/api/v1/events', {'type': 'demo.created',
    'data': {'message': 'hello'}}, producer, 'demo-' + suffix)
assert published['event']['target_app_ids'] == [consumer['app_id']]
print('Accepted routed event:', published['event']['id'])
PY
```

For production, store each application's API key and HMAC secret in backend secret
storage. Browsers receive short-lived WebSocket tokens only. Events are durable;
WebSocket notifications are hints. Remote functions require an online subscribed
owner and complete within the registered 1–30 second deadline.

## Verify and operate

Use `go -C backend test ./...` for the default suite and
`go -C backend test -tags=integration ./... -count=1 -timeout=180s` for the PostgreSQL/NATS/Redis
integration suite. The integration suite uses disposable testcontainers when explicit
test URLs are not supplied.

Run `go -C backend test ./...`, `go -C backend test -race ./...`, and
`go -C backend test -race -tags=integration ./... -count=1 -timeout=180s` for Go verification.
Set `RELAYHUB_TEST_POSTGRES_URL` to a reachable disposable PostgreSQL instance to make PostgreSQL
integration mandatory; otherwise the tests use Docker testcontainers. For homelab deployment, run the local checks below before building the image.

- [Deployment and every setting](web/docs/static/deploy/README.md)
- [Operations runbook: backup, restore and upgrades](docs/operations/runbook.md)
- [Security](web/docs/static/security.md) and [troubleshooting](web/docs/static/troubleshooting.md)
- [Internal deployment decisions](docs/developer/deployment-stack.md)
- [Agent index](web/docs/static/llms.txt), [full reference](web/docs/static/llms-full.txt),
  [OpenAPI](web/docs/static/openapi.json) and [integration Skill](web/docs/static/skills/relayhub-integration/SKILL.md)

After public doc edits, run `backend/scripts/build-skill.sh` and
`backend/scripts/build-llms.sh`. After Admin asset edits, run
`go -C backend generate ./web`. Container builds reject stale contracts, Skill
archives, AI indexes or embedded Admin bytes.
