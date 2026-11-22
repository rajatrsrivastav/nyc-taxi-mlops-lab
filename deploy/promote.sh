#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
exec 9>"$HOME/.lab-app-deploy.lock"
flock -w 600 9
version=${1:?evaluated model version required}
container=lab-serving-serving-champion-1
public=http://127.0.0.1:${HTTP_PORT:-80}

registry() { docker exec -i "$container" python - "$@" <deploy/promotion_registry.py; }

predict() {
  curl -fsS --max-time 5 -H 'Content-Type: application/json' "$public/predict" \
    -d '{"pickup_zone_id":132,"dropoff_zone_id":230,"pickup_datetime":"2024-03-08T17:05:00"}'
}
wait_for_model() {
  local alias_version=$1
  for _ in {1..30}; do
    if predict 2>/dev/null | jq -e --arg v "$alias_version" '
      (.duration_minutes | type=="number" and .>0) and
      (if $v=="" then .model.name=="static-median" and .model.alias==null
       else .model.name=="trip-duration" and .model.alias=="champion" and .model.version==$v end)
    ' >/dev/null; then return 0; fi
    sleep 1
  done
  return 1
}

snapshot=$(registry verify "$version")
previous=$(jq -r '.previous // empty' <<<"$snapshot")
printf '%s\n' "$snapshot" > promotion-result.json
if [[ -n "${GITHUB_RUN_ID:-}" ]]; then
  umask 077
  mkdir -p "$HOME/.lab-s3-promotions"
  cp promotion-result.json "$HOME/.lab-s3-promotions/$GITHUB_RUN_ID-${GITHUB_RUN_ATTEMPT:-1}.json"
fi
echo "Requested champion v$version; previous champion: ${previous:-unset}"
# Validate the public path is serving its current registry state before making a change.
wait_for_model "$previous"
recover() {
  local result=$?
  trap - EXIT INT TERM
  if (( result != 0 )); then
    echo "Promotion failed. Restoring the previous champion." >&2
    if ! registry restore "$snapshot"; then
      echo "Registry rollback failed. Review the recorded versions before taking action." >&2
      exit 1
    fi
    if [[ -n "$previous" ]]; then
      deploy/web.sh reload champion >/dev/null || exit 1
    else
      # With no alias, /reload retains the in-memory model. Restart to restore fallback.
      docker restart "$container" >/dev/null || exit 1
    fi
    wait_for_model "$previous" || { echo "Public rollback smoke failed" >&2; exit 1; }
    echo "Previous serving state restored"
  fi
  exit "$result"
}
trap recover EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
registry promote "$snapshot"
deploy/web.sh reload champion | jq -e --arg v "$version" '.alias=="champion" and .version==$v'
wait_for_model "$version"
echo "Public prediction verified champion v$version"
printf '### Model promotion\n\nPrevious champion: %s\n\nNew champion: %s\n' "${previous:-unset}" "$version" >> "${GITHUB_STEP_SUMMARY:-/dev/null}"
