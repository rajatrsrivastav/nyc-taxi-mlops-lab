"""S1 warehouse helpers shared by the loading and publication steps."""

import re

from .partitions import check_month

QUALITY_TABLE = """create table if not exists staging.trip_month_quality (
    trip_month char(7) not null, run_id varchar(64) not null, status varchar(16) not null
) diststyle auto"""

REVISION_TAG = "trip_file_revision"


def replace_quality_status(month: str, run_id: str, status: str) -> tuple[str, ...]:
    """Use in the same transaction as a load, or to exclude an invalid schema."""
    month, run_id = check_month(month), checked_run_id(run_id)
    if status not in {"loaded", "rejected"}:
        raise ValueError("Invalid load status")
    return (
        "create schema if not exists staging",
        QUALITY_TABLE,
        f"delete from staging.trip_month_quality where trip_month = '{month}'",
        f"insert into staging.trip_month_quality values ('{month}', '{run_id}', '{status}')",
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
