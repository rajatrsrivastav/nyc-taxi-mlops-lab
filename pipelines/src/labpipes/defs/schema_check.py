"""Reject any positional mismatch against the frozen Parquet contract."""

import dagster as dg

from ..contracts import Column, trips_schema
from ..partitions import trip_months
from ..resources import DataBucket, Footer, RedshiftData, TripFile
from ..warehouse import REVISION_TAG, replace_quality_status
from .trip_files import trip_files


def read_current_footer(
    data_bucket: DataBucket, month: str, expected_revision: str | None
) -> tuple[TripFile, Footer]:
    trip_file = data_bucket.trip_file(month)
    if expected_revision is not None and trip_file.revision != expected_revision:
        raise dg.Failure("The object revision changed before this run. Use the new revision's run.")
    footer = data_bucket.footer(month)
    if data_bucket.trip_file(month).revision != trip_file.revision:
        raise dg.Failure("The object revision changed while reading its Parquet footer.")
    return trip_file, footer


def normalized_schema(columns: list[Column]) -> list[tuple[str, str]]:
    return [(column.name.casefold(), column.type) for column in columns]


@dg.asset_check(asset=trip_files, partitions_def=trip_months, blocking=True, pool="redshift")
def schema_contract(
    context: dg.AssetCheckExecutionContext, data_bucket: DataBucket, redshift: RedshiftData
) -> dg.AssetCheckResult:
    month = context.partition_key
    trip_file, footer = read_current_footer(data_bucket, month, context.run.tags.get(REVISION_TAG))
    expected = normalized_schema(trips_schema())
    actual = normalized_schema(footer.columns)
    matches = actual == expected
    if not matches:
        redshift.execute(*replace_quality_status(redshift, month, context.run.run_id, "rejected"))
    return dg.AssetCheckResult(
        passed=matches,
        description="Ordered Parquet columns match." if matches else "Ordered Parquet columns differ from the contract.",
        metadata={"expected": expected, "actual": actual, "revision": trip_file.revision},
    )
