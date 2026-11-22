"""Build, register and evaluate the v2 model. Only the blocking gate permits alias publication."""

import math
import os
import re
from pathlib import Path
from uuid import uuid4

import dagster as dg
import mlflow

import tripmodel
from tripmodel.model import LABEL, REQUIRED

from ..contracts import features_contract, mlflow_contract
from ..partitions import check_month
from ..resources import ArtifactBucket, MlflowTracking, RedshiftData
from .feature_check import feature_schema, feature_value_violations


class TrainingWindow(dg.Config):
    first_month: str
    last_month: str
    holdout_month: str


class ChallengerPolicy(dg.Config):
    minimum_mae_improvement_min: float
    maximum_p95_latency_ms: float


def window_months(config: TrainingWindow) -> list[str]:
    def ordinal(month: str) -> int:
        year, number = map(int, check_month(month).split("-"))
        return year * 12 + number - 1

    first, last, holdout = map(ordinal, (config.first_month, config.last_month, config.holdout_month))
    if first > last or holdout != last + 1:
        raise dg.Failure("Use a contiguous training window and the immediately following holdout month.")
    return [f"{value // 12:04d}-{value % 12 + 1:02d}" for value in range(first, holdout + 1)]


@dg.asset(deps=["trip_features"], group_name="model", kinds={"redshift", "s3"}, pool="redshift",
          io_manager_def=dg.mem_io_manager)
def training_set(config: TrainingWindow, redshift: RedshiftData, artifacts: ArtifactBucket) -> dict:
    months = window_months(config)
    expected, actual = feature_schema(redshift)
    if actual != expected:
        raise dg.Failure("Cannot export a feature table that differs from the frozen contract.")

    available = redshift.query(
        "select f.trip_month, count(*) as rows from staging.trip_features f "
        "join staging.trip_feature_revisions r on r.trip_month = f.trip_month "
        "join staging.trip_month_quality q on q.trip_month = r.trip_month "
        "and q.run_id = r.run_id and q.status = 'approved' "
        f"where f.trip_month between '{months[0]}' and '{months[-1]}' group by f.trip_month"
    )
    if {row["trip_month"].strip() for row in available} != set(months):
        raise dg.Failure("Every training and holdout month must have materialized, quality-approved features.")
    if any(feature_value_violations(redshift, month) for month in months):
        raise dg.Failure("A selected month has features outside the frozen contract.")

    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", artifacts.bucket):
        raise dg.Failure("The artifact bucket name is invalid.")
    prefix = f"training/{uuid4().hex}"
    window = f"trip_month between '{months[0]}' and '{months[-2]}'"
    select_features = f"select {', '.join([*REQUIRED, LABEL])} from staging.trip_features"
    distances = (
        "select pickup_zone_id, dropoff_zone_id, median(trip_distance) as est_distance_miles "
        f"from staging.stg_trips where {window} and trip_distance > 0 "
        "and duration_min > 0 and duration_min <= 180 "
        "and pickup_zone_id between 1 and 263 and dropoff_zone_id between 1 and 263 "
        "group by pickup_zone_id, dropoff_zone_id"
    )
    queries = {
        "train": f"{select_features} where {window}",
        "holdout": f"{select_features} where trip_month = '{months[-1]}'",
        "distances": distances,
    }
    escaped_queries = {name: query.replace("'", "''") for name, query in queries.items()}
    redshift.execute(*(
        f"unload ('{query}') "
        f"to '{artifacts.uri(f'{prefix}/{name}/')}' iam_role default format as parquet"
        for name, query in escaped_queries.items()
    ))
    return {"prefix": prefix, "training_window": f"{months[0]}..{months[-2]}", "holdout_month": months[-1]}


@dg.asset(group_name="model", kinds={"mlflow"}, io_manager_def=dg.mem_io_manager)
def trained_model(training_set: dict, artifacts: ArtifactBucket, tracking: MlflowTracking) -> dict:
    sha = os.environ.get("GIT_SHA", "")
    if not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise dg.Failure("GIT_SHA must identify the exact training commit.")
    prefix = training_set["prefix"]
    model = tripmodel.LightGBMModel().fit(
        artifacts.read_parquet(f"{prefix}/train"), artifacts.read_parquet(f"{prefix}/distances")
    )
    tracking.client()
    with mlflow.start_run() as run:
        logged = mlflow.pyfunc.log_model(
            name=mlflow_contract()["registered_model"],
            python_model=model,
            signature=tripmodel.model_signature(),
            code_paths=[str(Path(tripmodel.__file__).parent)],
        )
        tags = {
            "training_window": training_set["training_window"],
            "holdout_month": training_set["holdout_month"],
            "features_version": str(features_contract()["version"]),
            "git_sha": sha,
        }
        version = mlflow.register_model(logged.model_uri, mlflow_contract()["registered_model"], tags=tags)
    return {"version": str(version.version), "run_id": run.info.run_id}


def champion_version(client, model_name: str) -> str | None:
    version = client.get_registered_model(model_name).aliases.get("champion")
    return str(version) if version is not None else None


@dg.asset(group_name="model", kinds={"mlflow"}, io_manager_def=dg.mem_io_manager)
def eval_report(
    training_set: dict, trained_model: dict, artifacts: ArtifactBucket, tracking: MlflowTracking,
    redshift: RedshiftData,
) -> dg.Output[dict]:
    client = tracking.client()
    contract = mlflow_contract()
    name = contract["registered_model"]
    version = trained_model["version"]
    compared_version = champion_version(client, name)
    prefix = training_set["prefix"]
    holdout = artifacts.read_parquet(f"{prefix}/holdout")
    boroughs = {int(row["location_id"]): row["borough"] for row in redshift.query(
        "select location_id, borough from ingest.taxi_zones"
    )}
    if compared_version is None:
        baseline = tripmodel.BaselineModel().fit(
            artifacts.read_parquet(f"{prefix}/train"), artifacts.read_parquet(f"{prefix}/distances")
        )

        def reference_predict(frame):
            return baseline.predict(None, frame)
    else:
        reference_predict = mlflow.pyfunc.load_model(f"models:/{name}/{compared_version}").predict

    candidate = mlflow.pyfunc.load_model(f"models:/{name}/{version}")
    metrics = tripmodel.evaluate(candidate.predict, holdout, boroughs)
    metrics["champion_mae_min"] = tripmodel.evaluate(reference_predict, holdout, boroughs)["mae_min"]
    missing = set(contract["metrics"]) - metrics.keys()
    if missing or not all(math.isfinite(metric) for metric in metrics.values()):
        raise dg.Failure(f"Evaluation is incomplete or non-finite. Missing metrics: {sorted(missing)}")
    with mlflow.start_run(run_id=trained_model["run_id"]):
        mlflow.log_metrics(metrics)
    for tag in contract["metrics"]:
        client.set_model_version_tag(name, version, tag, str(metrics[tag]))
    report = {"version": version, "compared_version": compared_version, "metrics": metrics}
    return dg.Output(report, metadata={**metrics, "version": version, "comparison": compared_version or "v1 baseline"})


@dg.asset_check(asset=eval_report, blocking=True)
def beats_champion(config: ChallengerPolicy, eval_report: dict) -> dg.AssetCheckResult:
    margin, budget = config.minimum_mae_improvement_min, config.maximum_p95_latency_ms
    if not (math.isfinite(margin) and margin >= 0 and math.isfinite(budget) and budget > 0):
        raise dg.Failure("Supply a nonnegative MAE margin and a positive p95 latency budget.")
    metrics = eval_report["metrics"]
    improvement = metrics["champion_mae_min"] - metrics["mae_min"]
    return dg.AssetCheckResult(
        passed=improvement > 0 and improvement >= margin and metrics["p95_latency_ms"] <= budget,
        metadata={"mae_improvement_min": improvement, "minimum_mae_improvement_min": margin,
                  "p95_latency_ms": metrics["p95_latency_ms"], "maximum_p95_latency_ms": budget},
    )


@dg.asset(group_name="model", kinds={"mlflow"})
def challenger(eval_report: dict, tracking: MlflowTracking) -> dg.MaterializeResult:
    client = tracking.client()
    name = mlflow_contract()["registered_model"]
    version = eval_report["version"]
    if champion_version(client, name) != eval_report["compared_version"]:
        raise dg.Failure("The champion changed during evaluation. Evaluate against the current champion.")
    client.set_registered_model_alias(name, "challenger", version)
    return dg.MaterializeResult(metadata={"version": version})


train_model = dg.define_asset_job(
    "train_model",
    selection=dg.AssetSelection.assets(training_set, trained_model, eval_report, challenger),
    # Run-scoped in-memory handoffs keep concurrent evaluations tied to their own model and holdout.
    executor_def=dg.in_process_executor,
)
