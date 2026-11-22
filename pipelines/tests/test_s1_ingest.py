"""S1 tests use local Parquet footers and a transaction-capable fake warehouse."""

import re
import sqlite3

import dagster as dg
import pytest
from fakes import FakeRedshift, fake_bucket, schema_2023, schema_2025

from labpipes.contracts import trips_schema
from labpipes.defs.marts import zone_hour_stats
from labpipes.defs.quality import RATE_METRICS, QualityPolicy, quality_outcomes, staging_quality
from labpipes.defs.raw_trips import raw_trips
from labpipes.defs.schema_check import schema_contract
from labpipes.defs.sensor import trip_files_sensor
from labpipes.defs.staging import stg_trips
from labpipes.defs.trip_files import trip_files
from labpipes.defs.zones import taxi_zones
from labpipes.partitions import trip_months
from labpipes.resources import ArtifactBucket

JAN, FEB, MAR = "2024-01", "2024-02", "2024-03"
_db: sqlite3.Connection | None = None
_batches: list[tuple[str, ...]] = []
_manifests: dict = {}
_row_overrides: dict[str, dict[int, int | None]] = {}


class MemoryArtifacts(ArtifactBucket):
    def write_json(self, key: str, obj) -> None:
        _manifests[key] = obj


class LocalWarehouse(FakeRedshift):
    def execute(self, *sqls: str) -> str:
        _batches.append(sqls)
        with _db:
            for sql in sqls:
                if sql.startswith("copy month_copy "):
                    key = sql.split("'")[1].removeprefix("s3://artifacts/")
                    (entry,) = _manifests[key]["entries"]
                    assert entry["mandatory"] and entry["meta"]["content_length"] == 1
                    assert sql.endswith("iam_role default format as parquet manifest")
                    month = re.search(r"(\d{4}-\d{2})\.parquet$", entry["url"])[1]
                    for n in range(3):
                        pickup = f"{month}-01 12:0{n}:00"
                        dropoff = f"{month}-01 12:1{n}:00"
                        values = [1, pickup, dropoff, 1, 2.0, 1, "N", 132, 230, 1, 15.0] + [0.0] * 8
                        for index, value in _row_overrides.get(month, {}).items():
                            values[index] = value
                        _db.execute("insert into month_copy values (" + ",".join("?" for _ in values) + ")", values)
                    continue
                if sql.startswith("copy ingest.taxi_zones"):
                    _db.execute("insert into ingest.taxi_zones values (132, 'Queens', 'JFK Airport', 'Airports')")
                    continue
                if sql.startswith("create schema"):
                    continue
                # SQLite runs the relevant DDL, replacement and mart JOIN. These substitutions
                # stand in for Redshift-specific syntax, not for percentile or COPY correctness.
                sql = re.sub(r" diststyle auto(?: sortkey \(\w+\))?$", "", sql)
                sql = sql.replace("datediff(second, tpep_pickup_datetime, tpep_dropoff_datetime)", "600")
                sql = sql.replace("extract(hour from tpep_pickup_datetime)", "12")
                sql = sql.replace("extract(dow from tpep_pickup_datetime)", "1")
                sql = sql.replace("percentile_cont(0.5) within group (order by s.duration_min)", "min(s.duration_min)")
                sql = sql.replace("percentile_cont(0.9) within group (order by s.duration_min)", "max(s.duration_min)")
                _db.execute(sql.replace("getdate()", "current_timestamp"))
            if any(sql.startswith("create temp table month_copy") for sql in sqls):
                _db.execute("drop table month_copy")  # A Data API batch ends its temp-table session.
        return "local"

    def query(self, sql: str) -> list[dict]:
        if "from information_schema.tables" in sql:
            sql = "select count(*) from marts.sqlite_master where type='table' and name='zone_hour_stats'"
        return [dict(row) for row in _db.execute(sql)]


@pytest.fixture
def warehouse(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    for schema in ("ingest", "staging", "marts"):
        db.execute(f"attach database ':memory:' as {schema}")
    for name, value in {"_db": db, "_batches": [], "_manifests": {}, "_row_overrides": {}}.items():
        monkeypatch.setitem(globals(), name, value)
    yield LocalWarehouse(cluster_id="lab", database="lab", db_user="labadmin")
    db.close()


def run_month(warehouse, month=JAN, columns=None, limits=None, *, etag="a", tags=None):
    bucket = fake_bucket({month: (etag, trips_schema() if columns is None else columns)})
    assets = [taxi_zones, trip_files, raw_trips, stg_trips, zone_hour_stats]
    # Exercise the S1 subgraph in isolation, including measurement-only policy overrides.
    ingest_s1 = dg.define_asset_job("ingest_s1", selection=dg.AssetSelection.assets(*assets))
    defs = dg.Definitions(
        assets=assets,
        asset_checks=[schema_contract, staging_quality],
        jobs=[ingest_s1],
        resources={"data_bucket": bucket, "redshift": warehouse, "artifacts": MemoryArtifacts(bucket="artifacts")},
    )
    config = {}
    if limits is not None:
        config = {"ops": {"stg_trips_staging_quality": {"config": {
            "warning_limits": {}, "blocking_limits": {}, **limits,
        }}}}
    with dg.DagsterInstance.ephemeral() as instance:
        instance.add_dynamic_partitions(trip_months.name, [month])
        return defs.resolve_job_def("ingest_s1").execute_in_process(
            instance=instance, partition_key=month, run_config=config, tags=tags, raise_on_error=False
        )


@pytest.mark.parametrize("columns", [schema_2025(), schema_2023(), list(reversed(trips_schema()))])
def test_schema_mismatch_blocks_copy(warehouse, columns):
    result = run_month(warehouse, columns=columns)
    assert not result.success
    assert not result.get_asset_check_evaluations()[0].passed
    assert not any(sql.startswith("copy month_copy") for batch in _batches for sql in batch)


def test_stale_sensor_request_does_not_copy_the_replacement(warehouse):
    result = run_month(warehouse, etag="replacement", tags={"trip_file_revision": "older:1:"})
    assert not result.success
    assert not _manifests
    assert not any(sql.startswith("copy month_copy") for batch in _batches for sql in batch)


def test_case_insensitive_schema_and_monthly_reload(warehouse):
    lower_names = [type(col)(col.name.lower(), col.type) for col in trips_schema()]
    for etag in ("first", "first", "replaced"):
        assert run_month(warehouse, columns=lower_names, etag=etag, limits={"blocking_limits": {"row_count": 1}}).success
        assert warehouse.scalar("select count(*) from ingest.raw_trips") == 3
        assert warehouse.scalar("select count(*) from staging.stg_trips") == 3
    copy_batches = [batch for batch in _batches if any(sql.startswith("copy month_copy") for sql in batch)]
    assert len(copy_batches) == len(_manifests) == 3
    assert all(any("delete from ingest.raw_trips where source_file = 'yellow_tripdata_2024-01.parquet'" == sql
                   for sql in batch) for batch in copy_batches)


def test_no_limits_measures_but_excludes_month(warehouse):
    result = run_month(warehouse, limits={})
    assert result.success
    check = next(check for check in result.get_asset_check_evaluations() if check.check_name == "staging_quality")
    assert check.metadata["status"].value == "measured"
    assert check.metadata["staged_rows"].value == 3
    assert warehouse.scalar("select count(*) from marts.zone_hour_stats") == 0
    assert run_month(warehouse, month=FEB, limits={"blocking_limits": {"row_count": 1}}).success
    assert warehouse.query("select first_month, last_month, trip_count from marts.zone_hour_stats") == [
        {"first_month": FEB, "last_month": FEB, "trip_count": 3}
    ]


def test_rejected_reload_repairs_existing_mart_and_stays_out_of_later_rebuild(warehouse):
    limits = {"blocking_limits": {"null_zones": QualityPolicy().blocking_limits["null_zones"]}}
    assert run_month(warehouse, limits=limits).success
    assert run_month(warehouse, month=FEB, limits=limits).success
    assert warehouse.scalar("select trip_count from marts.zone_hour_stats") == 6
    _row_overrides[JAN] = {7: None}  # Synthetic corrupt file with null pickup zones.
    failed = run_month(warehouse, limits=limits, etag="bad")
    assert not failed.success
    check = next(check for check in failed.get_asset_check_evaluations() if check.check_name == "staging_quality")
    assert not check.passed and check.severity == dg.AssetCheckSeverity.ERROR
    assert check.metadata["null_zones"].value == 3
    assert warehouse.scalar(f"select status from staging.trip_month_quality where trip_month = '{JAN}'") == "rejected"
    assert not failed.asset_materializations_for_node("zone_hour_stats")
    assert warehouse.query("select first_month, last_month, trip_count from marts.zone_hour_stats") == [
        {"first_month": FEB, "last_month": FEB, "trip_count": 3}
    ]
    assert any(
        any("values" in sql and "'loaded'" in sql for sql in batch)
        and "delete from marts.zone_hour_stats" in batch
        for batch in _batches
    )
    assert run_month(warehouse, month=MAR, limits=limits).success
    assert warehouse.query("select first_month, last_month, trip_count from marts.zone_hour_stats") == [
        {"first_month": FEB, "last_month": MAR, "trip_count": 6}
    ]


def test_schema_rejection_also_repairs_existing_mart_without_copy(warehouse):
    limits = {"blocking_limits": {"row_count": 1}}
    assert run_month(warehouse, limits=limits).success
    assert warehouse.scalar("select trip_count from marts.zone_hour_stats") == 3
    before = len(_manifests)
    failed = run_month(warehouse, columns=schema_2025(), etag="bad-schema", limits=limits)
    assert not failed.success
    assert len(_manifests) == before
    assert warehouse.scalar("select status from staging.trip_month_quality") == "rejected"
    assert warehouse.scalar("select count(*) from marts.zone_hour_stats") == 0
    assert not failed.asset_materializations_for_node("zone_hour_stats")


def test_warning_is_reported_but_does_not_reject(warehouse):
    _row_overrides[JAN] = {3: None}  # Passenger count, not required for the mart.
    result = run_month(warehouse, limits={
        "warning_limits": {"null_passengers": QualityPolicy().warning_limits["null_passengers"]},
        "blocking_limits": {"null_zones": QualityPolicy().blocking_limits["null_zones"]},
    })
    assert result.success
    check = next(check for check in result.get_asset_check_evaluations() if check.check_name == "staging_quality")
    assert not check.passed and check.severity == dg.AssetCheckSeverity.WARN
    assert check.metadata["warning_failures"].value == ["null_passengers"]
    assert warehouse.scalar("select status from staging.trip_month_quality") == "approved"
    assert warehouse.scalar("select trip_count from marts.zone_hour_stats") == 3


def test_sensor_deduplicates_and_enqueues_changed_revision():
    first = trip_files_sensor(
        dg.build_sensor_context(), data_bucket=fake_bucket({JAN: ("a", trips_schema())})
    )
    assert [r.partition_key for r in first.run_requests] == [JAN]
    assert first.dynamic_partitions_requests[0].partition_keys == [JAN]
    duplicate = trip_files_sensor(
        dg.build_sensor_context(cursor=first.cursor), data_bucket=fake_bucket({JAN: ("a", trips_schema())})
    )
    assert isinstance(duplicate, dg.SkipReason)
    changed = trip_files_sensor(
        dg.build_sensor_context(cursor=first.cursor),
        data_bucket=fake_bucket({JAN: ("b", trips_schema()), FEB: ("c", trips_schema())}),
    )
    assert [r.partition_key for r in changed.run_requests] == [JAN, FEB]
    assert changed.run_requests[0].run_key != first.run_requests[0].run_key
    assert changed.run_requests[0].tags["trip_file_revision"].startswith("b:")


def test_empty_sensor_run_config_uses_approved_limits(warehouse):
    tick = trip_files_sensor(dg.build_sensor_context(), data_bucket=fake_bucket({JAN: ("a", trips_schema())}))
    request, = tick.run_requests
    assert request.run_config == {}
    result = run_month(warehouse, tags=request.tags)
    check = next(check for check in result.get_asset_check_evaluations() if check.check_name == "staging_quality")
    assert check.metadata["warning_limits"].value == {
        "row_count": 2800000, "invalid_durations": 0.0012, "invalid_zones": 0.013,
        "duplicate_rows": 0.000001, "null_passengers": 0.15, "null_distances": 0.0,
        "negative_distances": 0.0, "negative_passengers": 0.0,
    }
    assert check.metadata["blocking_limits"].value == {
        "row_count": 2500000, "row_count_difference": 0.0, "null_timestamps": 0.0,
        "null_zones": 0.0, "invalid_durations": 0.005, "invalid_zones": 0.03,
        "duplicate_rows": 0.00001,
    }
    assert not result.success and check.metadata["blocking_failures"].value == ["row_count"]


def metrics(rows, **overrides):
    return dict.fromkeys(RATE_METRICS, 0) | {"raw_rows": rows, "staged_rows": rows} | overrides


@pytest.mark.parametrize(("rows", "passengers", "durations", "zones", "duplicates"), [
    (2964624, 140162, 2853, 31527, 0),  # January
    (3007526, 185610, 2751, 28974, 1),  # February
    (3582628, 426190, 3300, 35374, 0),  # March
    (3514289, 408576, 3352, 35759, 0),  # April
])
def test_measured_months_pass_without_warnings(rows, passengers, durations, zones, duplicates):
    measured = metrics(rows, null_passengers=passengers, invalid_durations=durations,
                       invalid_zones=zones, duplicate_rows=duplicates)
    _, warnings, blockers = quality_outcomes(QualityPolicy(), measured)
    assert not warnings and not blockers


@pytest.mark.parametrize(("rule", "rows", "changes"), [
    ("row_count", 2499999, {}),
    ("row_count_difference", 3000000, {"raw_rows": 3000001}),
    ("null_timestamps", 3000000, {"null_timestamps": 1}),
    ("null_zones", 3000000, {"null_zones": 1}),
    ("invalid_durations", 3000000, {"invalid_durations": 15001}),
    ("invalid_zones", 3000000, {"invalid_zones": 90001}),
    ("duplicate_rows", 3000000, {"duplicate_rows": 31}),
])
def test_each_approved_blocking_rule(rule, rows, changes):
    _, _, blockers = quality_outcomes(QualityPolicy(), metrics(rows, **changes))
    assert blockers == [rule]
