#!/usr/bin/env bash
# Check the S4 recovery checkpoint using existing observations. Never dispatches a workflow.
set -euo pipefail

if [[ $# != 3 || ! $1 =~ ^[0-9]{12}$ || ! $2 =~ ^[0-9a-f]{40}$ || ! $3 =~ ^[1-9][0-9]*$ ]]; then
  echo "Usage: $0 ACADEMY_ACCOUNT EXPECTED_APP_SHA EXPECTED_CHAMPION_VERSION" >&2
  exit 2
fi

account=$1 release=$2 champion=$3
cd "$(dirname "$0")/.."
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=''

arn=$(aws sts get-caller-identity --query Arn --output text)
if [[ "$arn" != "arn:aws:sts::$account:assumed-role/voclabs/"* ]]; then
  echo "Expected the voclabs role in Academy account $account." >&2
  exit 1
fi

bash scripts/health.sh | sed -E 's/^(OK +session +).*/\1Academy voclabs verified/'
repo=$(gh repo view --json nameWithOwner --jq .nameWithOwner)
run=$(gh run list --repo "$repo" --workflow status.yaml --branch main --limit 1 --json databaseId,status,conclusion,url)
if ! id=$(jq -er '.[0] | select(.status=="completed" and .conclusion=="success") | .databaseId' <<<"$run"); then
  echo "The latest main status run must be completed and successful. Inspect it in GitHub Actions." >&2
  exit 1
fi

umask 077
artifacts=$(mktemp -d)
trap 'rm -rf -- "$artifacts"' EXIT
gh run download "$id" --repo "$repo" --name lab-status --name lab-status-app --dir "$artifacts"
platform="$artifacts/lab-status/lab-status.json"
host="$artifacts/lab-status-app/lab-status-app.json"

if ! challenger=$(jq -er --arg champion "$champion" '
  select(.infra.rds=="available" and .infra.redshift=="available" and
         .sensors.trip_files_sensor=="RUNNING" and .mlflow.registered_model=="trip-duration" and
         .mlflow.aliases.champion==$champion)
  | .mlflow.aliases.challenger | select(type=="string" and test("^[1-9][0-9]*$"))
' "$platform"); then
  echo "Platform artifact does not confirm available databases, running ingestion, and the expected registry aliases." >&2
  exit 1
fi

if ! jq -e --arg release "$release" --arg champion "$champion" --arg challenger "$challenger" '
  (.live=="blue" or .live=="green") and .git_shas[.live]==$release and
  (.git_shas.blue | strings | test("^[0-9a-f]{40}$")) and
  (.git_shas.green | strings | test("^[0-9a-f]{40}$")) and
  .git_shas.blue!=.git_shas.green and
  .serving.champion.alias=="champion" and .serving.champion.version==$champion and
  .serving.challenger.alias=="challenger" and .serving.challenger.version==$challenger and
  .serving_weights.champion==100 and .serving_weights.challenger==0
' "$host" >/dev/null; then
  echo "App-host artifact does not confirm both colours, the expected live release, and registry-aligned serving." >&2
  exit 1
fi

app=$(terraform -chdir=infra/live output -raw app_public_ip)
if ! curl -fsS -m 10 "http://$app/version" |
  jq -e --arg sha "$release" --arg tag "sha-${release:0:7}" '.git_sha==$sha and .image_tag==$tag' >/dev/null; then
  echo "The public app is serving a different release or its version endpoint is unavailable." >&2
  exit 1
fi
if ! prediction=$(curl -fsS -m 10 "http://$app/predict" -H 'Content-Type: application/json' \
  -d '{"pickup_zone_id":132,"dropoff_zone_id":230,"pickup_datetime":"2024-03-08T17:05:00"}' |
  jq -ec --arg champion "$champion" '
    select(.model.name=="trip-duration" and .model.alias=="champion" and .model.version==$champion
           and (.duration_minutes | type=="number" and .>0))
  '); then
  echo "The public quote does not confirm the expected champion and a positive duration." >&2
  exit 1
fi
printf 'S4 checkpoint verified against %s\n%s\n' "$(jq -r '.[0].url' <<<"$run")" "$prediction"
