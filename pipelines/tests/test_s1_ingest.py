"""S1 tests use local Parquet footers and a transaction-capable fake warehouse."""

import re
import sqlite3

import dagster as dg
import pytest
from fakes import FakeRedshift, fake_bucket, schema_2023, schema_2025

from labpipes.contracts import trips_schema
from labpipes.defs.marts import zone_hour_stats
from labpipes.defs.quality import staging_quality
from labpipes.defs.raw_trips import raw_trips
from labpipes.defs.schema_check import schema_contract
from labpipes.defs.sensor import ingest_trips, trip_files_sensor
from labpipes.defs.staging import stg_trips
from labpipes.defs.trip_files import trip_files
from labpipes.defs.zones import taxi_zones
from labpipes.partitions import trip_months
from labpipes.resources import ArtifactBucket

JAN, FEB = "2024-01", "2024-02"
_db: sqlite3.Connection | None = None
_batches: list[tuple[str, ...]] = []
_manifests: dict = {}
_bad_months: set[str] = set()


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
                        if month in _bad_months:
                            values[3] = None
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
        return [dict(row) for row in _db.execute(sql)]


@pytest.fixture
def warehouse(monkeypatch):
    db = sqlite3.connect(":memory:")
    db.row_factory = sqlite3.Row
    for schema in ("ingest", "staging", "marts"):
        db.execute(f"attach database ':memory:' as {schema}")
    for name, value in {"_db": db, "_batches": [], "_manifests": {}, "_bad_months": set()}.items():
        monkeypatch.setitem(globals(), name, value)
    yield LocalWarehouse(cluster_id="lab", database="lab", db_user="labadmin")
    db.close()


def run_month(warehouse, month=JAN, columns=None, limits=None, *, etag="a", tags=None):
    bucket = fake_bucket({month: (etag, trips_schema() if columns is None else columns)})
    defs = dg.Definitions(
        assets=[taxi_zones, trip_files, raw_trips, stg_trips, zone_hour_stats],
        asset_checks=[schema_contract, staging_quality],
        jobs=[ingest_trips],
        resources={"data_bucket": bucket, "redshift": warehouse, "artifacts": MemoryArtifacts(bucket="artifacts")},
    )
    config = {"ops": {"stg_trips_staging_quality": {"config": limits}}} if limits else {}
    with dg.DagsterInstance.ephemeral() as instance:
        instance.add_dynamic_partitions(trip_months.name, [month])
        return defs.resolve_job_def("ingest_trips").execute_in_process(
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
    for etag in ("first", "replaced"):
        assert run_month(warehouse, columns=lower_names, etag=etag).success
        assert warehouse.scalar("select count(*) from ingest.raw_trips") == 3
        assert warehouse.scalar("select count(*) from staging.stg_trips") == 3
    copy_batches = [batch for batch in _batches if any(sql.startswith("copy month_copy") for sql in batch)]
    assert len(copy_batches) == len(_manifests) == 2
    assert all(any("delete from ingest.raw_trips where source_file = 'yellow_tripdata_2024-01.parquet'" == sql
                   for sql in batch) for batch in copy_batches)


def test_no_limits_measures_but_excludes_month(warehouse):
    result = run_month(warehouse)
    assert result.success
    check = next(check for check in result.get_asset_check_evaluations() if check.check_name == "staging_quality")
    assert check.metadata["status"].value == "measured"
    assert check.metadata["staged_rows"].value == 3
    assert warehouse.scalar("select count(*) from marts.zone_hour_stats") == 0


def test_rejected_reload_stays_out_of_later_full_mart_rebuild(warehouse):
    limits = {"blocking_limits": {"null_passengers": 0.0}}  # Test policy only.
    assert run_month(warehouse, limits=limits).success
    assert warehouse.scalar("select first_month from marts.zone_hour_stats") == JAN
    _bad_months.add(JAN)
    failed = run_month(warehouse, limits=limits, etag="bad")
    assert not failed.success
    check = next(check for check in failed.get_asset_check_evaluations() if check.check_name == "staging_quality")
    assert not check.passed and check.severity == dg.AssetCheckSeverity.ERROR
    assert check.metadata["null_passengers"].value == 3
    assert warehouse.scalar("select status from staging.trip_month_quality") == "rejected"
    assert not failed.asset_materializations_for_node("zone_hour_stats")
    assert run_month(warehouse, month=FEB, limits=limits).success
    assert warehouse.query("select first_month, last_month, trip_count from marts.zone_hour_stats") == [
        {"first_month": FEB, "last_month": FEB, "trip_count": 3}
    ]


def test_warning_is_reported_but_does_not_reject(warehouse):
    _bad_months.add(JAN)
    result = run_month(warehouse, limits={"warning_limits": {"null_passengers": 0.0}})
    assert result.success
    check = next(check for check in result.get_asset_check_evaluations() if check.check_name == "staging_quality")
    assert not check.passed and check.severity == dg.AssetCheckSeverity.WARN
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
