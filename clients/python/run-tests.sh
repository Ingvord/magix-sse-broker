#!/usr/bin/env bash
# Starts the packaged broker, runs the Python client tests against it, then stops it.
# Usage: clients/python/run-tests.sh [path/to/magix.jar]   (default: target/magix-0.1.jar)
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
jar="${1:-$here/../../target/magix-0.1.jar}"
port="${MAGIX_PORT:-8889}"

MICRONAUT_SERVER_PORT="$port" java -jar "$jar" >"${TMPDIR:-/tmp}/magix-python-test.log" 2>&1 &
broker=$!
trap 'kill "$broker" 2>/dev/null || true; wait "$broker" 2>/dev/null || true' EXIT

for _ in $(seq 1 50); do
  curl -sf -o /dev/null "http://localhost:$port/" && break
  sleep 0.2
done

cd "$here"
MAGIX_URL="http://localhost:$port" python3 -m pytest -q
