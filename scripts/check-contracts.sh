#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
# Tooling dependencies only; runtime docs remain dependency-free.
# python -m pip install jsonschema==4.26.0 openapi-spec-validator==0.9.0
exec "${PYTHON:-python3}" scripts/check-docs.py "$@"
