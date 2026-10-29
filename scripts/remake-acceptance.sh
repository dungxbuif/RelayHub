#!/usr/bin/env bash
# Release rehearsal only; never takes production URLs or credentials.
set +x
set -euo pipefail
umask 077
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
export REHEARSAL_ROOT="$PWD"

fail() { printf 'FAIL: %s\n' "$1" >&2; exit 2; }
for tool in cargo rustc docker node npm go cc timeout setsid mktemp; do
    command -v "$tool" >/dev/null || fail "missing prerequisite: $tool"
done
export REHEARSAL_DOCKER
REHEARSAL_DOCKER=$(command -v docker)
timeout --kill-after=5s 15s "$REHEARSAL_DOCKER" compose version >/dev/null 2>&1 || fail 'Docker Compose v2 is required'
timeout --kill-after=5s 15s "$REHEARSAL_DOCKER" info >/dev/null 2>&1 || fail 'local Docker daemon is unavailable'
# DOCKER_CONTEXT takes precedence over DOCKER_HOST in the Docker CLI.
if [[ -z ${DOCKER_CONTEXT:-} && -n ${DOCKER_HOST:-} ]]; then
    endpoint=$DOCKER_HOST
else
    endpoint=$(timeout --kill-after=5s 15s "$REHEARSAL_DOCKER" context inspect --format '{{.Endpoints.docker.Host}}' 2>/dev/null) ||
        fail 'cannot inspect Docker context'
fi
[[ $endpoint == unix://* ]] || fail 'use a local Unix-socket Docker context for loopback fixtures'
node -e 'if (Number(process.versions.node.split(".")[0]) < 22) process.exit(1)' || fail 'Node >=22 is required'
[[ -d sdks/remake/node/node_modules && -d web/remake-admin/node_modules ]] ||
    fail 'install locked dependencies with npm ci in sdks/remake/node and web/remake-admin'

export REHEARSAL_RUN
REHEARSAL_RUN=$(mktemp -d /tmp/relayhub_rehearsal_XXXXXXXXXXXX)
project=${REHEARSAL_RUN##*/}
project=${project,,}
active=''
# shellcheck disable=SC2317 # Invoked through EXIT/INT/TERM/HUP traps.
cleanup() {
    local result=$? entry file cleanup_failed=0
    trap - EXIT INT TERM HUP
    if [[ -n $active ]]; then
        kill -TERM -- "-$active" 2>/dev/null || true
        for _ in {1..10}; do
            kill -0 -- "-$active" 2>/dev/null || break
            sleep 1
        done
        kill -KILL -- "-$active" 2>/dev/null || true
        wait "$active" 2>/dev/null || true
    fi
    # Registry contains only projects invoked by this run, including SDK adapters.
    for entry in "$REHEARSAL_RUN"/projects/*; do
        [[ -f $entry ]] || continue
        file=$(<"$entry")
        if ! timeout --kill-after=5s 45s "$REHEARSAL_DOCKER" compose --env-file /dev/null \
            -f "$file" -p "${entry##*/}" down --volumes --remove-orphans --rmi local --timeout 5 \
            >/dev/null 2>&1; then
            printf 'FAIL: cleanup of disposable project %s; retry its Compose down\n' "${entry##*/}" >&2
            result=1
            cleanup_failed=1
        fi
    done
    # Killed SDK runners cannot remove their adapter directories or raw stderr.
    rm -rf -- "$REHEARSAL_RUN/tmp"
    if ((cleanup_failed)); then
        rm -f -- "$REHEARSAL_RUN/suite.log"
        printf 'Cleanup registry retained at %s/projects (no raw suite output)\n' "$REHEARSAL_RUN" >&2
    else
        rm -rf -- "$REHEARSAL_RUN"
    fi
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP
mkdir "$REHEARSAL_RUN/bin" "$REHEARSAL_RUN/projects"
# Keep SDK/system temporary files private and covered by the outer cleanup trap.
mkdir -m 700 "$REHEARSAL_RUN/tmp"
export TMPDIR="$REHEARSAL_RUN/tmp"
# Existing suites own UUID projects but cannot trap a killed test process. Track
# their exact -f/-p pairs without changing their namespace or test implementation.
cat > "$REHEARSAL_RUN/bin/docker" <<'PROXY'
#!/usr/bin/env bash
set +x
set -euo pipefail
args=("$@")
[[ ${1:-} == compose ]] || exit 2
file='' project='' removing=0
while (($#)); do
    case $1 in
        -f) file=$2; shift ;;
        -p) project=$2; shift ;;
        down) removing=1 ;;
    esac
    shift
done
case $file in
    "$REHEARSAL_ROOT/rust/tests/compose.yaml"|"$REHEARSAL_ROOT/rust/../deploy/remake/compose.yaml"|"$REHEARSAL_ROOT/deploy/remake/compose.yaml") ;;
    *) exit 2 ;;
esac
[[ $project =~ ^relayhub_(rust_test|go_test|rehearsal)_[a-zA-Z0-9_]+$ ]] || exit 2
printf '%s\n' "$file" > "$REHEARSAL_RUN/projects/$project"
result=0
"$REHEARSAL_DOCKER" "${args[@]}" || result=$?
if ((removing && result == 0)); then
    rm -f -- "$REHEARSAL_RUN/projects/$project"
fi
exit "$result"
PROXY
chmod 700 "$REHEARSAL_RUN/bin/docker"
export PATH="$REHEARSAL_RUN/bin:$PATH"
unset RELAYHUB_RECOVERY_CHILD RELAYHUB_RECOVERY_AGENT RELAYHUB_RECOVERY_IPC
export CI=1 CARGO_TERM_COLOR=never
# Keep captured output private without changing file-mode security test fixtures.
: > "$REHEARSAL_RUN/suite.log"
umask 022

failed=0
run() {
    local label=$1 result=0
    shift
    printf 'RUN: %s\n' "$label"
    # Never echo raw test/Compose output: SDK adapter errors can contain tokens.
    # Process-group timeout and the outer trap also cover interrupted harnesses.
    setsid timeout --kill-after=5s 110s "$@" >"$REHEARSAL_RUN/suite.log" 2>&1 &
    active=$!
    wait "$active" || result=$?
    # Reap any adapter left behind by a failed runner before the next suite.
    kill -TERM -- "-$active" 2>/dev/null || true
    kill -KILL -- "-$active" 2>/dev/null || true
    active=''
    if ((result)); then
        printf 'FAIL: %s (exit %s; raw output withheld)\n' "$label" "$result" >&2
        failed=1
    else
        printf 'PASS: %s\n' "$label"
    fi
}
compose=(docker compose --env-file /dev/null -f "$PWD/deploy/remake/compose.yaml" -p "$project")
run 'rehearsal Compose config' "${compose[@]}" --profile artifact config --quiet
run 'Rust formatting' cargo fmt --manifest-path rust/Cargo.toml --check
run 'Rust locked check' cargo check --locked --manifest-path rust/Cargo.toml --all-targets
# Explicit targets make the executed acceptance surface reviewable. Recovery is
# separate so every required name still runs when an earlier suite fails.
run 'Rust unit + CLI targets' cargo test --locked --manifest-path rust/Cargo.toml --lib --bin relayhub
for suite in contracts services identity durability delivery webhooks realtime connector; do
    run "Rust $suite" cargo test --locked --manifest-path rust/Cargo.toml --test "$suite" -- --test-threads=2
done
run 'Rust documentation tests' cargo test --locked --manifest-path rust/Cargo.toml --doc
run 'recovery (all five required tests)' cargo test --locked --manifest-path rust/Cargo.toml --test recovery -- --test-threads=1
run 'CLI version' cargo run --locked --manifest-path rust/Cargo.toml --bin relayhub -- version
run 'Node typecheck' npm --prefix sdks/remake/node run check
run 'Node build' npm --prefix sdks/remake/node run build
run 'Node acceptance' npm --prefix sdks/remake/node test
run 'Go race acceptance' env GOWORK=off go -C sdks/remake/go test -race -count=1 -v ./...
run 'Go vet' env GOWORK=off go -C sdks/remake/go vet ./...
run 'dashboard typecheck' npm --prefix web/remake-admin run typecheck
run 'dashboard build' npm --prefix web/remake-admin run build
run 'dashboard browser acceptance (mock API)' npm --prefix web/remake-admin exec -- playwright test --config web/remake-admin/playwright.config.ts
run 'rehearsal artifact build' "${compose[@]}" --profile artifact build harness relayhub
run 'rehearsal artifact contracts' "${compose[@]}" --profile artifact run --rm --no-deps harness

export REHEARSAL_COMPOSE_FILE="$PWD/deploy/remake/compose.yaml"
export REHEARSAL_PROJECT="$project"
run 'live host stores, migrations, and readiness' \
    "${compose[@]}" up -d --no-build --wait --wait-timeout 90 relayhub
relayhub_addr=$(timeout --kill-after=5s 15s "${compose[@]}" port relayhub 8080) ||
    fail 'live host did not publish a loopback port'
relayhub_addr=${relayhub_addr//$'\n'/}
[[ $relayhub_addr == 127.0.0.1:* ]] || fail 'live host port is not loopback-only'
export RELAYHUB_LIVE_URL="http://$relayhub_addr"

# Seed only disposable PostgreSQL rows. Tokens and SQL stay in the owner-only
# rehearsal directory and are never printed or passed as command-line args.
live_env="$REHEARSAL_RUN/tmp/live.env"
timeout --kill-after=5s 15s node --input-type=module - "$live_env" <<'NODE'
import { createHash, randomUUID } from 'node:crypto';
import { writeFileSync } from 'node:fs';
const target = process.argv.at(-1);
const token = () => randomUUID();
const values = {
  user: randomUUID(), workspace: randomUUID(), app: randomUUID(), agent: randomUUID(),
  endpoint: randomUUID(), appCredential: randomUUID(), agentCredential: randomUUID(), appKey: token(), agentKey: token(),
};
const digest = value => createHash('sha256').update(value).digest('hex');
writeFileSync(target, Object.entries({
  LIVE_USER_ID: values.user, LIVE_WORKSPACE_ID: values.workspace, LIVE_APP_ID: values.app,
  LIVE_AGENT_ID: values.agent, LIVE_ENDPOINT_ID: values.endpoint,
  LIVE_APP_CREDENTIAL_ID: values.appCredential, LIVE_AGENT_CREDENTIAL_ID: values.agentCredential,
  LIVE_APP_TOKEN: values.appKey, LIVE_AGENT_TOKEN: values.agentKey,
  LIVE_APP_TOKEN_HASH: digest(values.appKey), LIVE_AGENT_TOKEN_HASH: digest(values.agentKey),
}).map(([key, value]) => `${key}=${value}`).join('\n') + '\n', {mode: 0o600});
NODE
set -a
# shellcheck disable=SC1090 # Generated above in the private rehearsal directory.
source "$live_env"
set +a
seed_sql="$REHEARSAL_RUN/tmp/live.sql"
(
    umask 077
    cat > "$seed_sql" <<SQL
INSERT INTO identity_users(id,email) VALUES ('$LIVE_USER_ID','release-rehearsal@example.test');
INSERT INTO identity_workspaces(id,owner_id) VALUES ('$LIVE_WORKSPACE_ID','$LIVE_USER_ID');
INSERT INTO identity_apps(id,workspace_id,name) VALUES ('$LIVE_APP_ID','$LIVE_WORKSPACE_ID','release-rehearsal');
INSERT INTO identity_credentials(id,workspace_id,app_id,kind,token_hash)
VALUES ('$LIVE_APP_CREDENTIAL_ID', '$LIVE_WORKSPACE_ID', '$LIVE_APP_ID', 'application', decode('$LIVE_APP_TOKEN_HASH','hex'));
INSERT INTO identity_credentials(id,workspace_id,app_id,kind,agent_id,token_hash)
VALUES ('$LIVE_AGENT_CREDENTIAL_ID', '$LIVE_WORKSPACE_ID', '$LIVE_APP_ID', 'agent', '$LIVE_AGENT_ID', decode('$LIVE_AGENT_TOKEN_HASH','hex'));
INSERT INTO event_endpoints(id,workspace_id,app_id) VALUES ('$LIVE_ENDPOINT_ID','$LIVE_WORKSPACE_ID','$LIVE_APP_ID');
SQL
)
run 'live host disposable credential seed' bash -c \
    'cat "$1" | docker compose --env-file /dev/null -f "$REHEARSAL_COMPOSE_FILE" -p "$REHEARSAL_PROJECT" exec -T postgres psql -v ON_ERROR_STOP=1 -U relayhub_test -d relayhub_rust_test -f -' \
    -- "$seed_sql"

export LIVE_APP_TOKEN LIVE_AGENT_TOKEN LIVE_AGENT_ID LIVE_ENDPOINT_ID
run 'live host authenticated durable publish/status and graceful drain' \
    node --input-type=module - <<'NODE'
import { randomUUID } from 'node:crypto';
import { createRequire } from 'node:module';
import { spawn } from 'node:child_process';
const require = createRequire(`${process.cwd()}/sdks/remake/node/package.json`);
const WebSocket = require('ws');
const base = process.env.RELAYHUB_LIVE_URL;
const compose = ['compose', '--env-file', '/dev/null', '-f', process.env.REHEARSAL_COMPOSE_FILE, '-p', process.env.REHEARSAL_PROJECT];
const wait = ms => new Promise(resolve => setTimeout(resolve, ms));
const fail = () => { throw new Error('live host assertion failed'); };
const command = (file, args) => new Promise((resolve, reject) => {
  const child = spawn(file, args, {stdio: ['ignore', 'pipe', 'ignore']});
  let stdout = '';
  child.stdout.on('data', chunk => { stdout += chunk; });
  child.once('error', reject);
  child.once('close', code => code === 0 ? resolve(stdout) : reject(new Error('command failed')));
});
const response = await fetch(`${base}/api/v1/events`, {
  method: 'POST',
  headers: {authorization: `Bearer ${process.env.LIVE_APP_TOKEN}`, 'idempotency-key': randomUUID(), 'content-type': 'application/json'},
  body: JSON.stringify({endpoint_id: process.env.LIVE_ENDPOINT_ID, event_type: 'release.rehearsal', method: 'POST', path: '/release', headers: [], body: Buffer.from('release rehearsal').toString('base64')}),
});
if (response.status !== 202) fail();
const receipt = await response.json();
if (typeof receipt.event_id !== 'string' || typeof receipt.delivery_id !== 'string') fail();
const status = await fetch(`${base}/api/v1/events/${receipt.event_id}`, {headers: {authorization: `Bearer ${process.env.LIVE_APP_TOKEN}`}});
if (status.status !== 200) fail();
const state = await status.json();
if (state.event_id !== receipt.event_id || state.delivery_id !== receipt.delivery_id || !['pending', 'in_flight', 'retry_wait'].includes(state.state)) fail();

const socket = new WebSocket(`${base.replace(/^http/, 'ws')}/api/v1/socket`, 'relayhub.v1', {
  headers: {authorization: `Bearer ${process.env.LIVE_AGENT_TOKEN}`}, handshakeTimeout: 10_000, perMessageDeflate: false,
});
await new Promise((resolve, reject) => {
  socket.once('open', resolve);
  socket.once('error', () => reject(new Error('socket open failed')));
});
socket.on('error', () => {});
socket.send(JSON.stringify({version: 1, request_id: randomUUID(), type: 'hello', agent_id: process.env.LIVE_AGENT_ID, max_in_flight: 1}));
await wait(100);
const stop = spawn('docker', [...compose, 'stop', '--timeout', '35', 'relayhub'], {stdio: 'ignore'});
const stopped = new Promise((resolve, reject) => {
  stop.once('error', reject);
  stop.once('close', code => code === 0 ? resolve() : reject(new Error('host stop failed')));
});
const drained = new Promise((resolve, reject) => {
  let sawDrain = false;
  const timer = setTimeout(() => reject(new Error('drain timeout')), 15_000);
  socket.on('message', bytes => {
    try {
      const frame = JSON.parse(bytes.toString());
      if (frame.type !== 'drain' || !Number.isInteger(frame.deadline_ms) || frame.deadline_ms < 1 || frame.deadline_ms > 30_000) {
        clearTimeout(timer);
        reject(new Error('invalid drain frame'));
        return;
      }
      sawDrain = true;
      clearTimeout(timer);
      resolve();
    } catch { reject(new Error('invalid drain frame')); }
  });
  socket.once('close', () => { if (!sawDrain) reject(new Error('socket closed before drain')); });
});
await drained;
await new Promise((resolve, reject) => {
  const timer = setTimeout(() => reject(new Error('socket close timeout')), 15_000);
  if (socket.readyState === WebSocket.CLOSED) { clearTimeout(timer); resolve(); return; }
  socket.once('close', () => { clearTimeout(timer); resolve(); });
});
await stopped;
const id = String(await command('docker', [...compose, 'ps', '-q', 'relayhub'])).trim();
if (!/^[0-9a-f]+$/i.test(id)) fail();
const exitCode = String(await command('docker', ['inspect', '--format', '{{.State.ExitCode}}', id])).trim();
if (exitCode !== '0') fail();
NODE

((failed == 0)) || exit 1
exit 0
