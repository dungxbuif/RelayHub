#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if [ ! -f compose.yaml ]; then
  echo 'FAIL: root compose.yaml is required for acceptance' >&2
  exit 1
fi
build_dir=$(mktemp -d "${TMPDIR:-/tmp}/relayhub-e2e-build.XXXXXX")
trap 'rm -rf "$build_dir"' EXIT HUP INT TERM
# Bound the bootstrap compilation too; the client bounds stack/runtime work.
python3 - "$build_dir/e2e" <<'PYBUILD'
import subprocess, sys
try:
    subprocess.run(['go', 'build', '-o', sys.argv[1], './scripts/e2e-client.go'],
                   check=True, timeout=120, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
except (subprocess.SubprocessError, OSError):
    sys.exit('FAIL: acceptance client build failed or exceeded 120 seconds')
PYBUILD
"$build_dir/e2e" "$@"
