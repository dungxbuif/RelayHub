# RelayHub

RelayHub connects applications with durable events, signed HTTP callbacks,
standard WebSockets, routing rules, realtime channels and short remote function
calls. One Go image runs the API and worker. The v1 runtime uses PostgreSQL for
control/state and private NATS JetStream for delivery. The public polling queue
prototype is not part of v1. The API also serves the complete
human and agent documentation at `/docs/`.

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
for key in ('RELAYHUB_ADMIN_TOKEN', 'RELAYHUB_SIGNING_SECRET', 'RELAYHUB_POSTGRES_PASSWORD', 'RELAYHUB_NATS_USERNAME', 'RELAYHUB_NATS_PASSWORD'):
    text = text.replace(key + '=\n', key + '=' + secrets.token_hex(32) + '\n')
p.write_text(text)
PY
docker compose up --build -d --wait --wait-timeout 90
docker compose ps
curl --fail http://localhost:8080/readyz
```

Keep `.env` private and back it up securely. The example contains empty required
credentials; each installation generates its own. Compose publishes API 8080 only.
Set `RELAYHUB_PORT=127.0.0.1:8080` for a proxy on the same host, or restrict access
with your host firewall before exposing the default published port. PostgreSQL,
NATS and worker operations stay inside the project network. Open
[the local docs](http://localhost:8080/docs/) for integration instructions.

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

Use `go test ./...` for the default suite and
`go test -tags=integration ./... -count=1 -timeout=180s` for the PostgreSQL/NATS
integration suite. The integration suite uses disposable testcontainers when explicit
test URLs are not supplied.

Run `go test ./...`, `go test -race ./...`, and
`go test -race -tags=integration ./... -count=1 -timeout=180s` for Go verification.
Set `RELAYHUB_TEST_POSTGRES_URL` to a reachable disposable PostgreSQL instance to make PostgreSQL
integration mandatory; otherwise the tests use Docker testcontainers. CI provides
explicit PostgreSQL/NATS services for integration and contract smoke checks and runs docs negative controls when CI is enabled.

- [Deployment and every setting](public-docs/deploy/README.md)
- [Operations runbook: backup, restore and upgrades](docs/operations/runbook.md)
- [Security](public-docs/security.md) and [troubleshooting](public-docs/troubleshooting.md)
- [Internal deployment decisions](docs/developer/deployment-stack.md)
- [Agent index](public-docs/llms.txt), [full reference](public-docs/llms-full.txt),
  [OpenAPI](public-docs/openapi.json) and [integration Skill](public-docs/skills/relayhub-integration/SKILL.md)

After public doc edits, run `go generate ./web`. The image contains a verified
snapshot; stale docs, contracts, Skill archives or embedded bytes fail build checks.
