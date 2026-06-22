#!/usr/bin/env bash
set -euo pipefail

APP_NAME="${DO_APP_NAME:-async-job-processing-pipeline-service}"
APP_SPEC="${APP_SPEC:-infra/app.yaml}"
DO_REGION="${DO_REGION:-nyc}"
GITHUB_BRANCH="${GITHUB_BRANCH:-main}"

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

doctl auth init --access-token "$DIGITALOCEAN_ACCESS_TOKEN" >/dev/null

RENDERED_SPEC="$(mktemp)"
export APP_NAME DO_REGION GITHUB_REPO GITHUB_BRANCH DATABASE_URL KAFKA_BOOTSTRAP_SERVERS
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
}
for key, value in replacements.items():
    source = source.replace(key, value)
Path(sys.argv[2]).write_text(source)
PY

APP_ID="$(doctl apps list --format ID,Spec.Name --no-header | awk -v name="$APP_NAME" '$2 == name {print $1}')"

if [[ -n "$APP_ID" ]]; then
  echo "Updating DigitalOcean App Platform app $APP_NAME ($APP_ID)"
  doctl apps update "$APP_ID" --spec "$RENDERED_SPEC" --wait
else
  echo "Creating DigitalOcean App Platform app $APP_NAME"
  doctl apps create --spec "$RENDERED_SPEC" --wait
fi

echo "Deployment requested. Run scripts/smoke.sh with the deployed app URL after the app is live."
