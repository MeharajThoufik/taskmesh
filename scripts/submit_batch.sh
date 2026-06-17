#!/usr/bin/env bash
# Submit N tasks to the producer's batch endpoint (pure bash + curl; no Python needed).
# Each task gets a stable idempotency key "<prefix>-<i>" so the run is repeatable/countable.
#
# Usage: scripts/submit_batch.sh [count] [type] [key-prefix] [url]
set -euo pipefail
N=${1:-1000}
TYPE=${2:-PAYMENT_CAPTURE}
PREFIX=${3:-it}
URL=${4:-http://localhost:8081/api/tasks/batch}

BATCH=$(mktemp); trap 'rm -f "$BATCH"' EXIT
{
  printf '['
  for i in $(seq 0 $((N - 1))); do
    [ "$i" -gt 0 ] && printf ','
    printf '{"type":"%s","payload":{"orderId":"%s-%d","amount":%d,"currency":"INR"},"idempotencyKey":"%s-%d"}' \
      "$TYPE" "$PREFIX" "$i" $((1000 + i)) "$PREFIX" "$i"
  done
  printf ']'
} > "$BATCH"

curl -s -X POST "$URL" -H 'Content-Type: application/json' -d @"$BATCH" \
  -o /dev/null -w "submitted $N $TYPE tasks (prefix=$PREFIX): HTTP %{http_code} in %{time_total}s\n"
