#!/usr/bin/env bash
# Deploy or roll back the app on lab-app. Both operations serialize with model promotion.
set -euo pipefail
cd "$(dirname "$0")/.."
exec 9>"$HOME/.lab-app-deploy.lock"
flock -w 600 9
compose=(docker compose -f deploy/compose.web.yaml)
web=deploy/web.sh
public=http://127.0.0.1:${HTTP_PORT:-80}

public_sha() {
  curl -fsS --max-time 3 "$public/version" | jq -er '.git_sha'
}

wait_for_sha() {
  local expected=$1
  for _ in {1..20}; do
    if [[ "$(public_sha 2>/dev/null)" == "$expected" ]]; then return 0; fi
    sleep 1
  done
  echo "Public app did not report $expected" >&2
  return 1
}

other() {
  case "$1" in blue) echo green ;; green) echo blue ;; *) return 1 ;; esac
}

switch_checked() {
  local sha=$1
  "$web" switch "$idle"
  if ! wait_for_sha "$sha"; then
    echo "New route failed. Restoring $live." >&2
    "$web" switch "$live"
    wait_for_sha "$old_sha"
    return 1
  fi
}

case "${1:-}" in
  deploy)
    sha=${2:?commit SHA required}
    [[ "$sha" =~ ^[0-9a-f]{40}$ ]]
    tag="sha-${sha:0:7}"
    old_sha=$(public_sha)
    # The Compose project is shared with S0. Seed both colours before replacing its proxy.
    migration=0
    if [[ "$(docker inspect lab-app-traefik-1 --format '{{index .Config.Labels "lab.static-config"}}')" == "" ]]; then
      migration=1
      old_tag=$(docker inspect lab-app-app-1 --format '{{.Config.Image}}')
      export BLUE_TAG=${old_tag##*:} GREEN_TAG=$tag
      [[ "$BLUE_TAG" == "sha-${old_sha:0:7}" ]]
      live=blue idle=green
      "$web" init
      "${compose[@]}" pull app-green
      "${compose[@]}" up -d --no-deps --wait app-blue app-green
      "$web" switch blue
      if ! PREPARE_APP_SECRETS=0 "$web" prepare || ! "$web" smoke blue "$old_sha" || ! wait_for_sha "$old_sha"; then
        echo "Initial S3 cutover failed. Restoring the S0 proxy." >&2
        IMAGE_TAG=$BLUE_TAG docker compose -f deploy/compose.app.yaml up -d --no-deps traefik
        wait_for_sha "$old_sha"
        exit 1
      fi
    else
      "$web" init
      live=$("$web" live)
      idle=$(other "$live")
      if [[ "$old_sha" == "$sha" ]]; then
        "$web" smoke "$live" "$sha"
        exit 0
      fi
      if [[ "$live" == blue ]]; then
        BLUE_TAG=$("$web" tag blue)
        GREEN_TAG=$tag
      else
        BLUE_TAG=$tag
        GREEN_TAG=$("$web" tag green)
      fi
      export BLUE_TAG GREEN_TAG
      [[ "$("$web" tag "$live")" == "sha-${old_sha:0:7}" ]]
      "${compose[@]}" pull "app-$idle"
      "${compose[@]}" up -d --no-deps --wait "app-$idle"
    fi
    "$web" smoke "$idle" "$sha"
    switch_checked "$sha"
    # Once both colours and the new route are verified, stop the superseded S0 app.
    if [[ "$migration" == 1 ]]; then
      docker stop lab-app-app-1 >/dev/null
    fi
    "$web" status
    ;;
  rollback)
    live=$("$web" live)
    idle=$(other "$live")
    old_sha=$(public_sha)
    previous_sha=$("$web" status | jq -er --arg colour "$idle" '.git_shas[$colour] | select(test("^[0-9a-f]{40}$"))')
    started=$SECONDS
    "$web" smoke "$idle" "$previous_sha"
    switch_checked "$previous_sha"
    echo "App rollback verified in $((SECONDS - started))s"
    "$web" status
    ;;
  *) echo "Usage: $0 deploy SHA | rollback" >&2; exit 2 ;;
esac
