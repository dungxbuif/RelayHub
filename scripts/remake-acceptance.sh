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
run 'rehearsal artifact build' "${compose[@]}" --profile artifact build harness
run 'rehearsal artifact contracts' "${compose[@]}" --profile artifact run --rm --no-deps harness

# No override: passing library adapters or browser mocks cannot approve a host
# which does not exist. Replace this gate only with approved live host assertions.
printf '%s\n' \
    'BLOCKED: production host prerequisites are missing:' \
    '  authenticated publish/status and Response dispatch; supervised workers/reconnect;' \
    '  stop-new-claims drain while active settlements finish;' \
    '  dashboard workspace/delivery/replay/audit and scoped realtime-token adapters.' \
    'See docs/operations/remake-cutover.md. No production cutover is authorized.' >&2
((failed == 0)) || exit 1
exit 2
