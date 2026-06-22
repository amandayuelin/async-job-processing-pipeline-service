#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-}"
RUN_RECURRING_DEMO="${RUN_RECURRING_DEMO:-0}"

if [[ -z "$BASE_URL" ]]; then
  echo "Usage: $0 https://<app-url>" >&2
  echo "Optional: RUN_RECURRING_DEMO=1 $0 https://<app-url>" >&2
  exit 1
fi

BASE_URL="${BASE_URL%/}"

section() {
  printf "\n==> %s\n" "$1"
}

pretty() {
  python3 -m json.tool
}

json_field() {
  python3 -c '
import json
import sys

data = json.load(sys.stdin)
for part in sys.argv[1].split("."):
    data = data[part]
print(data)
' "$1"
}

api_get() {
  curl -fsS "$BASE_URL$1"
}

api_post() {
  local path="$1"
  local body="$2"
  curl -fsS -X POST "$BASE_URL$path" \
    -H "Content-Type: application/json" \
    -d "$body"
}

submit_job() {
  local body="$1"
  api_post "/jobs" "$body"
}

poll_job() {
  local job_id="$1"
  local expected_statuses="$2"
  local timeout_seconds="${3:-30}"
  local started
  started="$(date +%s)"

  while true; do
    response="$(api_get "/jobs/$job_id")"
    status="$(printf "%s" "$response" | json_field status)"
    if [[ ",$expected_statuses," == *",$status,"* ]]; then
      printf "%s" "$response"
      return 0
    fi

    if (( $(date +%s) - started >= timeout_seconds )); then
      echo "Timed out waiting for job $job_id to reach one of: $expected_statuses" >&2
      echo "Last response:" >&2
      printf "%s\n" "$response" >&2
      return 1
    fi

    sleep 1
  done
}

future_timestamp() {
  python3 - <<'PY'
from datetime import datetime, timedelta, timezone

print((datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat().replace("+00:00", "Z"))
PY
}

DEMO_ID="demo-$(date +%Y%m%d%H%M%S)"

section "1. Liveness and dependency readiness"
api_get "/healthz" | pretty
api_get "/readyz" | pretty

section "2. Baseline queue depth and metrics"
api_get "/queue/depth" | pretty
api_get "/metrics" | pretty

section "3. Submit an echo job and prove idempotency"
IDEMPOTENCY_KEY="$DEMO_ID-echo"
echo_body='{"handler":"echo","payload":{"message":"hello from demo","demo_id":"'"$DEMO_ID"'"},"priority":9,"max_retries":3,"timeout_seconds":30,"idempotency_key":"'"$IDEMPOTENCY_KEY"'"}'
first_response="$(submit_job "$echo_body")"
second_response="$(submit_job "$echo_body")"
echo "First submission:"
printf "%s" "$first_response" | pretty
echo "Second submission with the same idempotency key:"
printf "%s" "$second_response" | pretty
ECHO_JOB_ID="$(printf "%s" "$first_response" | json_field id)"

section "4. Poll until the echo job succeeds"
poll_job "$ECHO_JOB_ID" "succeeded" 30 | pretty

section "5. Submit a delayed job and cancel it before it runs"
RUN_AT="$(future_timestamp)"
cancel_response="$(submit_job '{"handler":"echo","payload":{"message":"cancel me","demo_id":"'"$DEMO_ID"'"},"priority":5,"run_at":"'"$RUN_AT"'","max_retries":3,"timeout_seconds":30}')"
CANCEL_JOB_ID="$(printf "%s" "$cancel_response" | json_field id)"
echo "Delayed job:"
api_get "/jobs/$CANCEL_JOB_ID" | pretty
echo "Cancellation result:"
api_post "/jobs/$CANCEL_JOB_ID/cancel" '{}' | pretty

section "6. Force a timeout and show dead-letter behavior"
timeout_response="$(submit_job '{"handler":"sleep","payload":{"seconds":3,"demo_id":"'"$DEMO_ID"'"},"priority":5,"max_retries":0,"timeout_seconds":1}')"
TIMEOUT_JOB_ID="$(printf "%s" "$timeout_response" | json_field id)"
poll_job "$TIMEOUT_JOB_ID" "dead_lettered" 30 | pretty

section "7. Force a handler failure and show dead-letter behavior"
failure_response="$(submit_job '{"handler":"always_fail","payload":{"message":"demo failure","demo_id":"'"$DEMO_ID"'"},"priority":5,"max_retries":0,"timeout_seconds":30}')"
FAILURE_JOB_ID="$(printf "%s" "$failure_response" | json_field id)"
poll_job "$FAILURE_JOB_ID" "dead_lettered" 30 | pretty

section "8. Toggle drain mode"
api_post "/ops/drain" '{"enabled":true}' | pretty
api_get "/ops/drain" | pretty
api_post "/ops/drain" '{"enabled":false}' | pretty

if [[ "$RUN_RECURRING_DEMO" == "1" ]]; then
  section "9. Optional recurring job demo"
  recurring_response="$(submit_job '{"handler":"echo","payload":{"message":"recurring demo","demo_id":"'"$DEMO_ID"'"},"priority":5,"max_retries":1,"timeout_seconds":30,"recurring_cron":"*/5 * * * *"}')"
  RECURRING_JOB_ID="$(printf "%s" "$recurring_response" | json_field id)"
  poll_job "$RECURRING_JOB_ID" "succeeded" 30 | pretty
  echo "A future recurring job should now exist in the queue."
else
  section "9. Recurring jobs"
  echo "Skipped by default to avoid leaving ongoing scheduled demo work."
  echo "Run with RUN_RECURRING_DEMO=1 to create and show a recurring job."
fi

section "10. Final queue depth, recent jobs, and metrics"
api_get "/queue/depth" | pretty
api_get "/jobs?limit=10&offset=0" | pretty
api_get "/metrics" | pretty

section "Demo complete"
echo "Base URL: $BASE_URL"
echo "Demo ID: $DEMO_ID"
