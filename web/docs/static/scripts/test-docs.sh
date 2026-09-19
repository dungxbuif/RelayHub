#!/bin/sh
set -eu
exec sh "$(dirname "$0")/../../../../backend/scripts/check-contracts.sh" "$@"
