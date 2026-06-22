#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-}"

if [[ -z "$BASE_URL" ]]; then
  echo "Usage: $0 https://<app-url>" >&2
  exit 1
fi

curl -fsS "$BASE_URL/healthz"
echo
curl -fsS "$BASE_URL/readyz"
echo

JOB_ID="$(curl -fsS -X POST "$BASE_URL/jobs" \
  -H "Content-Type: application/json" \
  -d '{"handler":"echo","payload":{"message":"hello"},"priority":5,"max_retries":3,"timeout_seconds":30}' \
  | python -c 'import json,sys; print(json.load(sys.stdin)["id"])')"

echo "Created job: $JOB_ID"
curl -fsS "$BASE_URL/jobs/$JOB_ID"
echo
curl -fsS "$BASE_URL/queue/depth"
echo
