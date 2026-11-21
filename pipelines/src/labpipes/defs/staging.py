"""stg_trips: one month of raw_trips, typed and with a duration. Data-quality checks attach here."""

import dagster as dg

from ..partitions import source_file, trip_months
from ..resources import RedshiftData
from ..sql import statements
from ..warehouse import checked_run_id


@dg.asset(partitions_def=trip_months, deps=["raw_trips"], group_name="staging", kinds={"redshift"}, pool="redshift")
def stg_trips(context: dg.AssetExecutionContext, redshift: RedshiftData) -> dg.MaterializeResult:
    month = context.partition_key
    file = source_file(month)
    run_id = checked_run_id(context.run.run_id)
    owner = redshift.scalar(f"select run_id from staging.trip_month_quality where trip_month = '{month}'")
    if owner != run_id:
        raise dg.Failure("This run no longer owns the raw month. Retry ingestion from trip_files.")
    redshift.execute(
        *statements("stg_trips", month=month, file=file),
        "update staging.trip_month_quality set status = 'staged' "
        f"where trip_month = '{month}' and run_id = '{run_id}'",
    )
    rows = redshift.scalar(f"select count(*) from staging.stg_trips where trip_month = '{month}'")
    return dg.MaterializeResult(metadata={"rows": dg.MetadataValue.int(int(rows))})
