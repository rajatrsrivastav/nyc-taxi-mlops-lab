"""Promotion evidence and restore behaviour with a fake MLflow registry."""

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "deploy"))
from promotion_registry import METRICS, current_champion, promote, restore, verify_evaluation  # noqa: E402


class Registry:
    def __init__(self, champion="7"):
        self.aliases = {} if champion is None else {"champion": champion}
        self.metrics = dict.fromkeys(METRICS, 3.0) | {"champion_mae_min": 3.5}
        self.tags = {k: str(v) for k, v in self.metrics.items()} | {
            "features_version": "1", "git_sha": "a" * 40,
            "training_window": "2024-01..2024-03", "holdout_month": "2024-04",
        }

    def get_model_version(self, name, version):
        assert (name, version) == ("trip-duration", "8")
        return SimpleNamespace(status="READY", run_id="run", tags=self.tags)

    def get_registered_model(self, _name):
        return SimpleNamespace(aliases=self.aliases)

    def get_experiment_by_name(self, _name):
        return SimpleNamespace(experiment_id="exp")

    def get_run(self, _run):
        return SimpleNamespace(info=SimpleNamespace(experiment_id="exp", status="FINISHED"),
                               data=SimpleNamespace(metrics=self.metrics))

    def set_registered_model_alias(self, _name, alias, version):
        self.aliases[alias] = version

    def delete_registered_model_alias(self, _name, alias):
        del self.aliases[alias]


@pytest.mark.parametrize("previous", [None, "7"])
def test_verified_promotion_records_and_restores_previous_version(previous):
    client = Registry(previous)
    snapshot = verify_evaluation(client, "8")
    assert snapshot["previous"] == previous and snapshot["run_id"] == "run"
    promote(client, snapshot)
    assert current_champion(client) == "8"
    restore(client, snapshot)
    assert current_champion(client) == previous


@pytest.mark.parametrize("metric,value", [("mae_min", 3.4), ("p95_latency_ms", 10.1), ("mae_min.night", float("nan"))])
def test_rejects_failed_or_invalid_evaluation(metric, value):
    client = Registry()
    client.metrics[metric] = value
    client.tags[metric] = str(value)
    with pytest.raises(ValueError):
        verify_evaluation(client, "8")
    assert current_champion(client) == "7"


def test_rejects_missing_or_mismatched_evaluation():
    client = Registry()
    client.tags["mae_min.airport"] = "99"
    with pytest.raises(ValueError, match="differ"):
        verify_evaluation(client, "8")
    del client.tags["mae_min.airport"]
    with pytest.raises(ValueError, match="lacks"):
        verify_evaluation(client, "8")


def test_refuses_to_overwrite_an_unexpected_champion():
    client = Registry()
    snapshot = verify_evaluation(client, "8")
    client.aliases["champion"] = "9"
    with pytest.raises(ValueError, match="changed"):
        promote(client, snapshot)
    with pytest.raises(ValueError, match="changed"):
        restore(client, snapshot)
    assert current_champion(client) == "9"
