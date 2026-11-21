#!/usr/bin/env bash
# Register a lab host through SSM Session Manager. Get a fresh registration token
# from Settings > Actions > Runners > New self-hosted runner and paste it at the
# hidden prompt. Keep the token out of shell history and SSM command parameters.
set -euo pipefail
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 AWS_PAGER=""

role="${1:?usage: scripts/register-runner.sh <app|platform|llm>}"
case "$role" in
  app|platform|llm) ;;
  *) echo "role must be app, platform or llm" >&2; exit 1 ;;
esac

root="$(cd "$(dirname "$0")/.." && pwd)"
repo=$(gh repo view --json nameWithOwner -q .nameWithOwner)
[[ "$repo" =~ ^[a-zA-Z0-9_-]+/[a-zA-Z0-9_.-]+$ ]]
policy=$(gh api "repos/$repo/actions/permissions/fork-pr-contributor-approval" -q .approval_policy)
if [[ "$policy" != all_external_contributors ]]; then
  echo "Enable approval for all external contributors in Settings > Actions > General first." >&2
  exit 1
fi
instance_id=$(terraform -chdir="$root/infra/live" output -raw "${role}_instance_id")

IFS= read -r -d '' remote_script <<'REMOTE_SCRIPT' || true
set -euo pipefail
repo=$1
label="lab-$2"
runner_version=2.338.2

for _attempt in $(seq 1 30); do
  [[ ! -f /var/lib/lab-setup-done ]] || break
  sleep 5
done
if [[ ! -f /var/lib/lab-setup-done ]]; then
  echo "Host setup did not finish. Check /var/log/lab-setup.log." >&2
  exit 1
fi

cd /opt/actions-runner
if [[ ! -x ./config.sh ]]; then
  case "$(uname -m)" in
    aarch64) runner_arch=arm64 ;;
    x86_64) runner_arch=x64 ;;
    *) echo "Unsupported runner architecture" >&2; exit 1 ;;
  esac
  curl -fsSL -o runner.tar.gz "https://github.com/actions/runner/releases/download/v${runner_version}/actions-runner-linux-${runner_arch}-${runner_version}.tar.gz"
  tar xzf runner.tar.gz
  rm -f runner.tar.gz
  chown -R runner:runner /opt/actions-runner
fi

if [[ ! -f .runner ]]; then
  sudo -u runner -- bash -c '
    set +x
    set -euo pipefail
    if ! { exec 3<>/dev/tty; } 2>/dev/null; then
      echo "Runner registration needs a controlling terminal. Open an interactive SSM session and retry." >&2
      exit 1
    fi
    terminal_state=$(stty -g <&3)
    trap "unset token; stty \"\$terminal_state\" <&3" EXIT
    stty -echo <&3
    echo "Paste the GitHub runner registration token and press Enter." >&3
    if ! IFS= read -r token <&3; then
      echo "Token input ended before a complete line was entered." >&2
      exit 1
    fi
    if [[ -z "$token" ]]; then
      echo "The registration token was empty." >&2
      exit 1
    fi
    stty "$terminal_state" <&3
    exec 3>&-
    trap "unset token" EXIT
    ACTIONS_RUNNER_INPUT_TOKEN="$token" ./config.sh --url "https://github.com/$1" \
      --name "$2" --labels "$2" --unattended
  ' bash "$repo" "$label"
fi
if [[ ! -f .service ]]; then
  ./svc.sh install runner
fi
./svc.sh start
REMOTE_SCRIPT

# Quote the script as a single shell argument. Only executable text and public
# identifiers are passed to SSM. The registration token is read on the host.
quoted_script=${remote_script//\'/\'\\\'\'}
command="sudo bash -c '$quoted_script' bash '$repo' '$role'"
parameters=$(jq -cn --arg command "$command" '{command: [$command]}')
aws ssm start-session --target "$instance_id" \
  --document-name AWS-StartInteractiveCommand --parameters "$parameters"
