"""S2 exercises synthetic Parquet and a local MLflow registry without AWS."""

import dagster as dg
import mlflow
import pytest

from fakes import LocalArtifacts, UnloadingRedshift
from labpipes.contracts import features_contract, mlflow_contract
from labpipes.definitions import defs
from labpipes.defs.features import trip_features
from labpipes.defs.training import (
    ChallengerPolicy,
    TrainingWindow,
    beats_champion,
    challenger,
    eval_report,
    train_model,
    trained_model,
    training_set,
    window_months,
)
from labpipes.resources import MlflowTracking

MONTHS = ["2024-01", "2024-02", "2024-03", "2024-04"]


class FeatureWarehouse(UnloadingRedshift):
    extra_column: bool = False
    invalid_values: bool = False

    def query(self, sql):
        if "from information_schema.columns" in sql:
            contract = features_contract()
            rows = [
                {"column_name": col["name"], "data_type": col["type"], "character_maximum_length": None}
                for col in [*contract["keys"], *contract["inputs"], contract["label"]]
            ]
            rows[0].update(data_type="character", character_maximum_length=7)
            if self.extra_column:
                rows.append({"column_name": "dropoff_at", "data_type": "timestamp", "character_maximum_length": None})
            return rows
        if "group by f.trip_month" in sql:
            assert "q.run_id = r.run_id" in sql and "q.status = 'approved'" in sql
            return [{"trip_month": month, "rows": 4000} for month in self.months]
        if "and ((" in sql:
            assert "duration_min <= 0" in sql and "duration_min > 180" in sql and "pickup_dow > 6" in sql
            return [{"count": int(self.invalid_values)}]
        if "from ingest.taxi_zones" in sql:
            return [{"location_id": z, "borough": "Manhattan" if z not in {1, 18, 19, 20} else "Queens"}
                    for z in range(1, 21)]
        return super().query(sql)


def test_ingest_includes_blocking_feature_contract():
    job = defs.resolve_job_def("ingest_trips")
    assert dg.AssetKey("trip_features") in job.asset_layer.executable_asset_keys
    assert job.asset_layer.asset_graph.get(dg.AssetCheckKey(dg.AssetKey("trip_features"), "feature_contract")).blocking


@pytest.mark.parametrize("holdout", ["2024-03", "2024-05"])
def test_holdout_must_immediately_follow_training(holdout):
    with pytest.raises(dg.Failure):
        window_months(TrainingWindow(first_month="2024-01", last_month="2024-03", holdout_month=holdout))


@pytest.mark.parametrize("mae,latency,passed", [(5.0, 8.0, True), (5.9, 8.0, False), (5.0, 10.01, False)])
def test_gate_requires_both_improvement_and_latency(mae, latency, passed):
    result = beats_champion(
        config=ChallengerPolicy(minimum_mae_improvement_min=0.25, maximum_p95_latency_ms=10.0),
        eval_report={"metrics": {"mae_min": mae, "p95_latency_ms": latency, "champion_mae_min": 6.0}},
    )
    assert result.passed is passed


@pytest.mark.parametrize("warehouse_options", [{"extra_column": True}, {"invalid_values": True}, {"months": MONTHS[:3]}])
def test_export_rejects_leakage_invalid_values_and_missing_months(tmp_path, warehouse_options):
    redshift = FeatureWarehouse(cluster_id="lab", database="lab", db_user="labadmin", root=str(tmp_path), **warehouse_options)
    artifacts = LocalArtifacts(bucket="artifact-test", root=str(tmp_path))
    with pytest.raises(dg.Failure):
        training_set(config=TrainingWindow(first_month="2024-01", last_month="2024-03", holdout_month="2024-04"),
                     redshift=redshift, artifacts=artifacts)


def test_train_register_evaluate_and_guard_alias(tmp_path, monkeypatch):
    monkeypatch.setenv("GIT_SHA", "a" * 40)
    uri = f"sqlite:///{tmp_path}/tracking.db"
    mlflow.set_tracking_uri(uri)
    mlflow.create_experiment(mlflow_contract()["experiment"], artifact_location=str(tmp_path / "models"))
    redshift = FeatureWarehouse(cluster_id="lab", database="lab", db_user="labadmin", root=str(tmp_path))
    artifacts = LocalArtifacts(bucket="artifact-test", root=str(tmp_path))
    tracking = MlflowTracking(tracking_uri=uri)
    job = dg.Definitions(
        assets=[trip_features.to_source_asset(), training_set, trained_model, eval_report, challenger],
        asset_checks=[beats_champion],
        jobs=[train_model],
        resources={"redshift": redshift, "artifacts": artifacts, "tracking": tracking},
    ).resolve_job_def("train_model")
    config = {"ops": {
        "training_set": {"config": {"first_month": "2024-01", "last_month": "2024-03", "holdout_month": "2024-04"}},
        "eval_report_beats_champion": {"config": {
            "minimum_mae_improvement_min": 1000, "maximum_p95_latency_ms": 1000,
        }},
    }}
    blocked = job.execute_in_process(run_config=config, raise_on_error=False)
    assert not blocked.success
    assert blocked.get_asset_check_evaluations()[0].passed is False
    registry = tracking.client()
    name = mlflow_contract()["registered_model"]
    assert registry.get_registered_model(name).aliases == {}

    config["ops"]["eval_report_beats_champion"]["config"]["minimum_mae_improvement_min"] = 0
    accepted = job.execute_in_process(run_config=config)
    assert accepted.success and accepted.get_asset_check_evaluations()[0].passed
    model = registry.get_model_version_by_alias(name, "challenger")
    assert set(mlflow_contract()["version_tags"]) <= model.tags.keys()
    assert registry.get_registered_model(name).aliases == {"challenger": model.version}
    run_metrics = registry.get_run(accepted.output_for_node("trained_model")["run_id"]).data.metrics
    for metric in mlflow_contract()["metrics"]:
        assert float(model.tags[metric]) == run_metrics[metric]
