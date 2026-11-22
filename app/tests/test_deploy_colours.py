"""Exercise the host deployment sequence with fake Docker, web commands and HTTP."""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

OLD = "a" * 40
NEW = "b" * 40


@pytest.fixture
def deployment(tmp_path):
    root = Path(__file__).resolve().parents[2]
    (tmp_path / "deploy").mkdir()
    shutil.copy(root / "deploy/app-colours.sh", tmp_path / "deploy/app-colours.sh")
    fakes = {
        "docker": '''#!/bin/sh
echo "$*" >> "$TRACE"
case "$*" in
  'inspect lab-app-traefik-1'*) [ "$MODE" = migration ] || echo configured ;;
  'inspect lab-app-app-1'*) echo "ghcr.io/test/app:sha-aaaaaaa" ;;
esac
''',
        "curl": '''#!/bin/sh
case "$(cat "$ACTIVE")" in
  blue) sha="$OLD" ;;
  green) sha="$NEW" ;;
esac
printf '{"git_sha":"%s"}\n' "$sha"
''',
        "flock": "#!/bin/sh\nexit 0\n",
        "sleep": "#!/bin/sh\nexit 0\n",
        "deploy/web.sh": '''#!/bin/sh
echo "$*" >> "$TRACE"
case "$1" in
  live) cat "$ACTIVE" ;;
  tag)
    if [ "$2" = blue ]; then echo sha-aaaaaaa; else echo sha-bbbbbbb; fi ;;
  smoke) [ "$FAIL_SMOKE" != "$2" ] ;;
  switch)
    if [ "$FAIL_PUBLIC_SWITCH" != "$2" ]; then echo "$2" > "$ACTIVE"; fi ;;
  status) printf '{"git_shas":{"blue":"%s","green":"%s"}}\n' "$OLD" "$NEW" ;;
esac
''',
    }
    for name, script in fakes.items():
        path = tmp_path / name
        path.write_text(script)
        path.chmod(0o755)
    (tmp_path / "active").write_text("blue\n")
    trace = tmp_path / "trace"

    def run(action="deploy", *, mode="steady", fail_smoke="", fail_public_switch=""):
        args = ["bash", "deploy/app-colours.sh", action]
        if action == "deploy":
            args.append(NEW)
        result = subprocess.run(args, cwd=tmp_path, capture_output=True, text=True, env={
            **os.environ,
            "HOME": str(tmp_path),
            "PATH": f"{tmp_path}:{os.environ['PATH']}",
            "TRACE": str(trace),
            "ACTIVE": str(tmp_path / "active"),
            "OLD": OLD,
            "NEW": NEW,
            "MODE": mode,
            "FAIL_SMOKE": fail_smoke,
            "FAIL_PUBLIC_SWITCH": fail_public_switch,
        })
        return result, trace.read_text().splitlines(), (tmp_path / "active").read_text().strip()

    return run


def test_steady_deploy_smokes_then_switches_only_idle_colour(deployment):
    result, log, active = deployment()
    assert result.returncode == 0, result.stderr
    assert active == "green"
    assert log.index(f"smoke green {NEW}") < log.index("switch green")
    assert "compose -f deploy/compose.web.yaml up -d --no-deps --wait app-green" in log
    assert not any("--wait app-blue" in call for call in log)


def test_migration_has_both_colours_before_proxy_cutover(deployment):
    result, log, active = deployment(mode="migration")
    assert result.returncode == 0, result.stderr
    assert active == "green"
    assert log.index("compose -f deploy/compose.web.yaml up -d --no-deps --wait app-blue app-green") < log.index("prepare")
    assert log.index(f"smoke blue {OLD}") < log.index(f"smoke green {NEW}") < log.index("stop lab-app-app-1")


def test_idle_smoke_failure_keeps_previous_colour(deployment):
    result, log, active = deployment(fail_smoke="green")
    assert result.returncode != 0
    assert "switch green" not in log and active == "blue"


def test_failed_public_switch_restores_previous_colour(deployment):
    result, log, active = deployment(fail_public_switch="green")
    assert result.returncode != 0
    assert log.index("switch green") < log.index("switch blue")
    assert active == "blue"


def test_rollback_switches_without_creating_or_pulling_containers(deployment):
    result, log, active = deployment(action="rollback")
    assert result.returncode == 0, result.stderr
    assert active == "green"
    assert not any("compose" in call for call in log)


def test_failed_first_cutover_restores_s0_proxy(deployment):
    result, log, active = deployment(mode="migration", fail_smoke="blue")
    assert result.returncode != 0
    assert "compose -f deploy/compose.app.yaml up -d --no-deps traefik" in log
    assert "switch green" not in log and "stop lab-app-app-1" not in log
    assert active == "blue"
