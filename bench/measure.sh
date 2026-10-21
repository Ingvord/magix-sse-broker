#!/usr/bin/env bash
# Measures the JVM and native builds of magix and prints a Markdown report.
#
#   ./mvnw -B -Dpackaging=native-image -DskipTests clean package   # target/magix  (needs GraalVM)
#   ./mvnw -B -DskipTests package                                  # target/magix-0.1.jar, the shaded jar
#                                                                  # (run second and without "clean": the
#                                                                  #  native build leaves a thin jar behind)
#   docker build -t magix:native . && docker build -f Dockerfile.jvm -t magix:jvm .   # optional: image sizes
#   bench/measure.sh [--out FILE]
#
# Tunables (environment): RUNS=10 SUBSCRIBERS=1,10,100 MESSAGES=500 WARMUP=200 PORT=8890
# Set GRAALVM_HOME to the GraalVM that built target/magix so the report records its version.
# Uses python3 (standard library only). Every figure in the report is produced by bench/bench.py.
set -euo pipefail
cd "$(dirname "$0")/.."

out=""
if [[ "${1:-}" == "--out" ]]; then out="${2:?--out needs a file}"; fi

jar=target/magix-0.1.jar
native=target/magix
[[ -f $jar ]] || { echo "missing $jar: run ./mvnw -B -DskipTests package (after the native build, without clean)" >&2; exit 1; }
[[ -x $native ]] || { echo "missing $native: run ./mvnw -B -Dpackaging=native-image -DskipTests package" >&2; exit 1; }

runs="${RUNS:-10}"; subs="${SUBSCRIBERS:-1,10,100}"; msgs="${MESSAGES:-500}"; warm="${WARMUP:-200}"; port="${PORT:-8890}"
jvm_cmd="java -jar $jar"
loadavg="$(cut -d' ' -f1-3 /proc/loadavg)"
tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT

measure() { # label command
  echo "measuring $1 ..." >&2
  python3 bench/bench.py measure --cmd "$2" --port "$port" --runs "$runs" \
    --subscribers "$subs" --messages "$msgs" --warmup "$warm" > "$tmp/$1.json"
}
measure jvm "$jvm_cmd"
measure native "$native"

report() {
  python3 bench/bench.py report --jvm "$tmp/jvm.json" --native "$tmp/native.json" \
    --jvm-cmd "$jvm_cmd" --native-cmd "$native" --jvm-artifact "$jar" --native-artifact "$native" --loadavg "$loadavg"
}
if [[ -n $out ]]; then report | tee "$out"; else report; fi
