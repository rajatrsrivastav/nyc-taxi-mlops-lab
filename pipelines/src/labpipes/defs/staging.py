"""stg_trips: one month of raw_trips, typed and with a duration. Data-quality checks attach here."""

import dagster as dg

from ..partitions import source_file, trip_months
from ..resources import RedshiftData
from ..sql import statements
from ..warehouse import checked_run_id, repair_published_mart


@dg.asset(partitions_def=trip_months, deps=["raw_trips"], group_name="staging", kinds={"redshift"}, pool="redshift")
def stg_trips(context: dg.AssetExecutionContext, redshift: RedshiftData) -> dg.MaterializeResult:
    month = context.partition_key
    file = source_file(month)
    run_id = checked_run_id(context.run.run_id)
    owners = redshift.query(f"select run_id, status from staging.trip_month_quality where trip_month = '{month}'")
    if len(owners) != 1 or owners[0]["run_id"] != run_id:
        raise dg.Failure("This run no longer owns the raw month. Retry ingestion from trip_files.")
    repair = repair_published_mart(redshift, month) if owners[0]["status"] == "approved" else []
    redshift.execute(
        *statements("stg_trips", month=month, file=file),
        "update staging.trip_month_quality set status = 'staged' "
        f"where trip_month = '{month}' and run_id = '{run_id}'",
        *repair,
    )
    rows = redshift.scalar(f"select count(*) from staging.stg_trips where trip_month = '{month}'")
    return dg.MaterializeResult(metadata={"rows": dg.MetadataValue.int(int(rows))})
