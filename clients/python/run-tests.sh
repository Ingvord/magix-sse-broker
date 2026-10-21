#!/usr/bin/env bash
# Starts the packaged broker, runs the Python client tests and the sseclient check against it, then stops it.
# Usage: clients/python/run-tests.sh [path/to/magix.jar]   (default: target/magix-0.1.jar)
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
jar="${1:-$here/../../target/magix-0.1.jar}"
port="${MAGIX_PORT:-8889}"

MICRONAUT_SERVER_PORT="$port" java -jar "$jar" >"${TMPDIR:-/tmp}/magix-python-test.log" 2>&1 &
broker=$!
venv=""
cleanup() {
  kill "$broker" 2>/dev/null || true
  wait "$broker" 2>/dev/null || true
  [[ -z "$venv" ]] || rm -rf "$venv"
}
trap cleanup EXIT

for _ in $(seq 1 50); do
  curl -sf -o /dev/null "http://localhost:$port/" && break
  sleep 0.2
done

cd "$here"
MAGIX_URL="http://localhost:$port" python3 -m pytest -q

# The PyPI package "sseclient" (named in the 2022 case study) clashes with "sseclient-py" on the
# module name, so it is checked from a throwaway virtualenv. Set SKIP_SSECLIENT_CHECK=1 to skip.
if [[ -z "${SKIP_SSECLIENT_CHECK:-}" ]]; then
  venv="$(mktemp -d)"
  python3 -m venv "$venv"
  "$venv/bin/pip" -q install -r requirements-sseclient.txt
  "$venv/bin/python" check_sseclient.py "http://localhost:$port"
fi
