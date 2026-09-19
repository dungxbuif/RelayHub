#!/bin/sh
set -eu
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cd "$script_dir/.."
# Tooling dependencies only; runtime docs remain dependency-free.
# python -m pip install jsonschema==4.26.0 openapi-spec-validator==0.9.0
exec "${PYTHON:-python3}" "$script_dir/check-docs.py" "$@"
