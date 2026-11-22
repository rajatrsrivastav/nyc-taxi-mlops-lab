"""Registry operations invoked only by the reviewed production promotion workflow."""

import json
import math
import re
import sys

NAME = "trip-duration"
METRICS = (
    "mae_min", "champion_mae_min", "p95_latency_ms",
    "mae_min.airport", "mae_min.manhattan", "mae_min.outer", "mae_min.night",
)
VERSION_TAGS = ("training_window", "holdout_month", "features_version", "git_sha")


def verify_evaluation(client, version):
    if not re.fullmatch(r"[1-9][0-9]*", version):
        raise ValueError("Supply a numeric registered model version.")
    candidate = client.get_model_version(NAME, version)
    tags = candidate.tags
    if candidate.status != "READY" or not candidate.run_id or any(not tags.get(k) for k in (*METRICS, *VERSION_TAGS)):
        raise ValueError("The model version lacks a completed evaluation.")
    if tags["features_version"] != "1" or not re.fullmatch(r"[0-9a-f]{40}", tags["git_sha"]):
        raise ValueError("The evaluation lacks trusted training metadata.")
    experiment = client.get_experiment_by_name(NAME)
    run = client.get_run(candidate.run_id)
    if experiment is None or run.info.experiment_id != experiment.experiment_id or run.info.status != "FINISHED":
        raise ValueError("The version does not belong to a finished trip-duration training run.")
    metrics = {name: run.data.metrics.get(name) for name in METRICS}
    if any(value is None or not math.isfinite(value) or value < 0 or float(tags[name]) != value for name, value in metrics.items()):
        raise ValueError("The evaluation metrics are incomplete or differ from the version tags.")
    improvement = metrics["champion_mae_min"] - metrics["mae_min"]
    latency = metrics["p95_latency_ms"]
    if improvement < 0.25 or not 0 < latency <= 10.0:
        raise ValueError("The version failed the approved S2 MAE or latency gate.")
    return {"version": version, "run_id": candidate.run_id, "previous": current_champion(client),
            "improvement_min": improvement, "p95_latency_ms": latency}


def current_champion(client):
    version = client.get_registered_model(NAME).aliases.get("champion")
    return str(version) if version is not None else None


def promote(client, snapshot):
    if verify_evaluation(client, snapshot["version"]) != snapshot:
        raise ValueError("Registry or evaluation changed after preflight; dispatch a new promotion review.")
    client.set_registered_model_alias(NAME, "champion", snapshot["version"])


def restore(client, snapshot):
    current = current_champion(client)
    if current == snapshot["previous"]:
        return
    if current != snapshot["version"]:
        raise ValueError("Champion changed outside this promotion; refusing to overwrite it.")
    if snapshot["previous"] is None:
        client.delete_registered_model_alias(NAME, "champion")
    else:
        client.set_registered_model_alias(NAME, "champion", snapshot["previous"])


def main():
    from mlflow import MlflowClient

    command, value = sys.argv[1:]
    client = MlflowClient()
    if command == "verify":
        print(json.dumps(verify_evaluation(client, value)))
    elif command == "promote":
        promote(client, json.loads(value))
    elif command == "restore":
        restore(client, json.loads(value))
    else:
        raise ValueError("Unknown registry operation.")


if __name__ == "__main__":
    main()
