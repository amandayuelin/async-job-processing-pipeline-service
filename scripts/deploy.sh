#!/usr/bin/env bash
set -euo pipefail

APP_NAME="${DO_APP_NAME:-async-job-pipeline}"
APP_SPEC="${APP_SPEC:-infra/app.yaml}"
DO_REGION="${DO_REGION:-nyc}"
GITHUB_BRANCH="${GITHUB_BRANCH:-main}"
API_INSTANCE_COUNT="${API_INSTANCE_COUNT:-1}"
API_INSTANCE_SIZE="${API_INSTANCE_SIZE:-basic-xxs}"
WORKER_INSTANCE_COUNT="${WORKER_INSTANCE_COUNT:-1}"
WORKER_INSTANCE_SIZE="${WORKER_INSTANCE_SIZE:-basic-xxs}"
MAX_PAGE_SIZE="${MAX_PAGE_SIZE:-100}"
MAX_PAYLOAD_BYTES="${MAX_PAYLOAD_BYTES:-65536}"
WORKER_POLL_INTERVAL_SECONDS="${WORKER_POLL_INTERVAL_SECONDS:-1}"
WORKER_BATCH_SIZE="${WORKER_BATCH_SIZE:-10}"
STALE_LOCK_SECONDS="${STALE_LOCK_SECONDS:-300}"
KAFKA_SUBMITTED_HIGH_TOPIC="${KAFKA_SUBMITTED_HIGH_TOPIC:-jobs.submitted.high}"
KAFKA_SUBMITTED_DEFAULT_TOPIC="${KAFKA_SUBMITTED_DEFAULT_TOPIC:-jobs.submitted.default}"
KAFKA_SUBMITTED_LOW_TOPIC="${KAFKA_SUBMITTED_LOW_TOPIC:-jobs.submitted.low}"
KAFKA_RETRY_TOPIC="${KAFKA_RETRY_TOPIC:-jobs.retry}"
KAFKA_DEAD_LETTER_TOPIC="${KAFKA_DEAD_LETTER_TOPIC:-jobs.dead_lettered}"

require() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require doctl
require python3

if [[ -z "${DIGITALOCEAN_ACCESS_TOKEN:-}" ]]; then
  echo "DIGITALOCEAN_ACCESS_TOKEN is required" >&2
  exit 1
fi

if [[ -z "${DATABASE_URL:-}" ]]; then
  echo "DATABASE_URL is required" >&2
  exit 1
fi

if [[ -z "${KAFKA_BOOTSTRAP_SERVERS:-}" ]]; then
  echo "KAFKA_BOOTSTRAP_SERVERS is required" >&2
  exit 1
fi

if [[ -z "${GITHUB_REPO:-}" ]]; then
  echo "GITHUB_REPO is required, for example owner/repo" >&2
  exit 1
fi

if (( ${#APP_NAME} > 32 )); then
  echo "DO_APP_NAME must be at most 32 characters for DigitalOcean App Platform. Current: $APP_NAME" >&2
  exit 1
fi

doctl auth init --access-token "$DIGITALOCEAN_ACCESS_TOKEN" >/dev/null

RENDERED_SPEC="$(mktemp)"
export APP_NAME DO_REGION GITHUB_REPO GITHUB_BRANCH DATABASE_URL KAFKA_BOOTSTRAP_SERVERS
export API_INSTANCE_COUNT API_INSTANCE_SIZE WORKER_INSTANCE_COUNT WORKER_INSTANCE_SIZE
export MAX_PAGE_SIZE MAX_PAYLOAD_BYTES WORKER_POLL_INTERVAL_SECONDS WORKER_BATCH_SIZE STALE_LOCK_SECONDS
export KAFKA_SUBMITTED_HIGH_TOPIC KAFKA_SUBMITTED_DEFAULT_TOPIC KAFKA_SUBMITTED_LOW_TOPIC KAFKA_RETRY_TOPIC KAFKA_DEAD_LETTER_TOPIC
python3 - "$APP_SPEC" "$RENDERED_SPEC" <<'PY'
import os
import sys
from pathlib import Path

source = Path(sys.argv[1]).read_text()
replacements = {
    "__APP_NAME__": os.environ["APP_NAME"],
    "__DO_REGION__": os.environ["DO_REGION"],
    "__GITHUB_REPO__": os.environ["GITHUB_REPO"],
    "__GITHUB_BRANCH__": os.environ["GITHUB_BRANCH"],
    "__DATABASE_URL__": os.environ["DATABASE_URL"],
    "__KAFKA_BOOTSTRAP_SERVERS__": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    "__API_INSTANCE_COUNT__": os.environ["API_INSTANCE_COUNT"],
    "__API_INSTANCE_SIZE__": os.environ["API_INSTANCE_SIZE"],
    "__WORKER_INSTANCE_COUNT__": os.environ["WORKER_INSTANCE_COUNT"],
    "__WORKER_INSTANCE_SIZE__": os.environ["WORKER_INSTANCE_SIZE"],
    "__MAX_PAGE_SIZE__": os.environ["MAX_PAGE_SIZE"],
    "__MAX_PAYLOAD_BYTES__": os.environ["MAX_PAYLOAD_BYTES"],
    "__WORKER_POLL_INTERVAL_SECONDS__": os.environ["WORKER_POLL_INTERVAL_SECONDS"],
    "__WORKER_BATCH_SIZE__": os.environ["WORKER_BATCH_SIZE"],
    "__STALE_LOCK_SECONDS__": os.environ["STALE_LOCK_SECONDS"],
    "__KAFKA_SUBMITTED_HIGH_TOPIC__": os.environ["KAFKA_SUBMITTED_HIGH_TOPIC"],
    "__KAFKA_SUBMITTED_DEFAULT_TOPIC__": os.environ["KAFKA_SUBMITTED_DEFAULT_TOPIC"],
    "__KAFKA_SUBMITTED_LOW_TOPIC__": os.environ["KAFKA_SUBMITTED_LOW_TOPIC"],
    "__KAFKA_RETRY_TOPIC__": os.environ["KAFKA_RETRY_TOPIC"],
    "__KAFKA_DEAD_LETTER_TOPIC__": os.environ["KAFKA_DEAD_LETTER_TOPIC"],
}
for key, value in replacements.items():
    source = source.replace(key, value)
Path(sys.argv[2]).write_text(source)
PY

APP_ID="${DO_APP_ID:-}"
if [[ -z "$APP_ID" ]]; then
  APP_ID="$(doctl apps list --format ID,Spec.Name --no-header | awk -v name="$APP_NAME" '$2 == name {print $1}')"
fi

if [[ -n "$APP_ID" ]]; then
  echo "Updating DigitalOcean App Platform app $APP_NAME ($APP_ID)"
  doctl apps update "$APP_ID" --spec "$RENDERED_SPEC" --wait
else
  echo "Creating DigitalOcean App Platform app $APP_NAME"
  doctl apps create --spec "$RENDERED_SPEC" --wait
fi

echo "Deployment requested. Run scripts/smoke.sh with the deployed app URL after the app is live."
