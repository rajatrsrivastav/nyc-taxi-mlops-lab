"""COPY one validated monthly object into the native Redshift landing table."""

import dagster as dg

from ..contracts import redshift_columns, trips_schema
from ..partitions import source_file, trip_months
from ..resources import ArtifactBucket, DataBucket, RedshiftData
from ..warehouse import REVISION_TAG, checked_run_id, replace_quality_status, sql_s3_uri
from .schema_check import normalized_schema, read_current_footer
from .trip_files import trip_files


@dg.asset(partitions_def=trip_months, deps=[trip_files], group_name="ingest", kinds={"redshift"}, pool="redshift")
def raw_trips(
    context: dg.AssetExecutionContext, data_bucket: DataBucket, redshift: RedshiftData, artifacts: ArtifactBucket
) -> dg.MaterializeResult:
    month = context.partition_key
    file = source_file(month)
    run_id = checked_run_id(context.run.run_id)
    expected = trips_schema()
    trip_file, footer = read_current_footer(data_bucket, month, context.run.tags.get(REVISION_TAG))
    # Also enforce the schema for raw-only materializations that omit the Dagster check.
    if normalized_schema(footer.columns) != normalized_schema(expected):
        redshift.execute(*replace_quality_status(redshift, month, run_id, "rejected"))
        raise dg.Failure("Parquet schema differs from the contract. COPY was not attempted.")

    # COPY treats an S3 URL as a prefix. The manifest restricts it to the checked object.
    manifest_key = f"s1/copy-manifests/{month}/{run_id}.json"
    manifest_uri = sql_s3_uri(artifacts.uri(manifest_key))
    artifacts.write_json(
        manifest_key,
        {"entries": [{"url": sql_s3_uri(data_bucket.trip_uri(month)), "mandatory": True,
                      "meta": {"content_length": trip_file.size}}]},
    )
    ddl = redshift_columns(expected)
    redshift.execute(
        "create schema if not exists ingest",
        f"create table if not exists ingest.raw_trips ({ddl}, source_file varchar(64) not null) "
        "diststyle auto sortkey (tpep_pickup_datetime)",
        f"create temp table month_copy ({ddl})",
        f"copy month_copy from '{manifest_uri}' iam_role default format as parquet manifest",
        f"delete from ingest.raw_trips where source_file = '{file}'",
        f"insert into ingest.raw_trips select month_copy.*, '{file}' from month_copy",
        *replace_quality_status(redshift, month, run_id, "loaded"),
    )
    rows = int(redshift.scalar(f"select count(*) from ingest.raw_trips where source_file = '{file}'"))
    if data_bucket.trip_file(month).revision != trip_file.revision or rows != footer.num_rows:
        redshift.execute(*replace_quality_status(redshift, month, run_id, "rejected"))
        raise dg.Failure("COPY did not match the checked object revision or footer row count.")
    return dg.MaterializeResult(metadata={"rows": rows, "source_file": file, "revision": trip_file.revision})
