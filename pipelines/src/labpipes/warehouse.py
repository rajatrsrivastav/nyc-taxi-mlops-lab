"""S1 warehouse helpers shared by the loading and publication steps."""

import re

from .partitions import check_month
from .resources import RedshiftData
from .sql import statements

QUALITY_TABLE = """create table if not exists staging.trip_month_quality (
    trip_month char(7) not null, run_id varchar(64) not null, status varchar(16) not null
) diststyle auto"""

REVISION_TAG = "trip_file_revision"


def repair_published_mart(redshift: RedshiftData, month: str) -> list[str]:
    """Append these statements after revoking eligibility, in the same pooled transaction."""
    month = check_month(month)
    mart_exists = redshift.scalar(
        "select count(*) from information_schema.tables "
        "where table_schema = 'marts' and table_name = 'zone_hour_stats'"
    )
    if mart_exists and redshift.scalar(
        "select count(*) from staging.trip_month_quality "
        f"where trip_month = '{month}' and status = 'approved'"
    ):
        # A month can contribute to any zone/hour aggregate, so rebuild from the remaining
        # eligible months. DELETE and INSERT commit alongside the eligibility update.
        return statements("zone_hour_stats")
    return []


def replace_quality_status(redshift: RedshiftData, month: str, run_id: str, status: str) -> tuple[str, ...]:
    """Replace eligibility and remove any previous publication as part of the same batch."""
    month, run_id = check_month(month), checked_run_id(run_id)
    if status not in {"loaded", "rejected"}:
        raise ValueError("Invalid load status")
    return (
        "create schema if not exists staging",
        QUALITY_TABLE,
        f"delete from staging.trip_month_quality where trip_month = '{month}'",
        f"insert into staging.trip_month_quality values ('{month}', '{run_id}', '{status}')",
        *repair_published_mart(redshift, month),
    )


def checked_run_id(run_id: str) -> str:
    if not re.fullmatch(r"[a-zA-Z0-9-]{1,64}", run_id):
        raise ValueError("Invalid Dagster run ID")
    return run_id


def sql_s3_uri(uri: str) -> str:
    """Constrain the S3 literals interpolated into Data API batch statements."""
    if not re.fullmatch(r"s3://[a-z0-9][a-z0-9.-]*[a-z0-9]/[a-zA-Z0-9_./-]+", uri):
        raise ValueError("Invalid S3 COPY URI")
    return uri
