#!/usr/bin/env bash
# Concurrent load test for POST /api/tasks — a portable stand-in for
# `ab -n <N> -c <C>` (Apache Bench / hey / wrk not available on this host).
#
# Drives N requests at concurrency C via xargs -P. Each request reuses one body
# file; the producer auto-generates a unique idempotency key, so every request is
# a distinct task. NOTE: latency here includes per-request client (curl) spawn
# overhead, so the server is actually faster than these numbers suggest.
#
# Usage: scripts/load_test.sh [requests] [concurrency] [url]
set -euo pipefail
N=${1:-10000}
C=${2:-100}
URL=${3:-http://localhost:8081/api/tasks}

BODY=$(mktemp); LAT=$(mktemp)
trap 'rm -f "$BODY" "$LAT" "$LAT.s"' EXIT
echo '{"type":"NOTIFICATION","payload":{"to":"load"}}' > "$BODY"

start=$(date +%s.%N)
seq 1 "$N" | xargs -P "$C" -I{} \
  curl -s -o /dev/null -w "%{http_code} %{time_total}\n" \
       -X POST "$URL" -H "Content-Type: application/json" -d @"$BODY" > "$LAT"
end=$(date +%s.%N)

total=$(grep -c . "$LAT")
ok=$(awk '$1=="202"{c++} END{print c+0}' "$LAT")
wall=$(awk -v s="$start" -v e="$end" 'BEGIN{printf "%.2f", e-s}')
thr=$(awk -v n="$total" -v w="$wall" 'BEGIN{printf "%.0f", n/w}')
mean=$(awk '{s+=$2} END{printf "%.1f", (s/NR)*1000}' "$LAT")
awk '{print $2}' "$LAT" | sort -n > "$LAT.s"
pct() { awk -v n="$total" -v q="$1" 'NR==int(n*q){printf "%.1f", $1*1000; exit}' "$LAT.s"; }

echo "----- load test: POST $URL -----"
echo "requests=$total  concurrency=$C  ok(202)=$ok"
echo "wall=${wall}s  throughput=${thr} req/s"
echo "latency_ms: mean=$mean  p50=$(pct 0.50)  p95=$(pct 0.95)  p99=$(pct 0.99)  max=$(awk 'END{printf "%.1f", $1*1000}' "$LAT.s")"
