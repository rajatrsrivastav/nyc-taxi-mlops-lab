"""trip_features: one month of model inputs and labels, valid trips only (contracts/features.yaml).

The feature contract check attaches here (feature_check.py): a column that isn't in the contract
stops training before it starts.
"""

import dagster as dg

from ..partitions import check_month, trip_months
from ..resources import RedshiftData
from ..sql import statements


@dg.asset(partitions_def=trip_months, deps=["stg_trips"], group_name="model", kinds={"redshift"}, pool="redshift")
def trip_features(context: dg.AssetExecutionContext, redshift: RedshiftData) -> dg.MaterializeResult:
    month = check_month(context.partition_key)
    status = redshift.scalar(
        f"select status from staging.trip_month_quality where trip_month = '{month}'"
    )
    if status != "approved":
        raise dg.Failure("Only quality-approved months can materialize features.")
    redshift.execute(
        *statements("trip_features", month=month),
        "create table if not exists staging.trip_feature_revisions (trip_month char(7), run_id varchar(36))",
        f"delete from staging.trip_feature_revisions where trip_month = '{month}'",
        "insert into staging.trip_feature_revisions select trip_month, run_id "
        f"from staging.trip_month_quality where trip_month = '{month}' and status = 'approved'",
    )
    rows = redshift.scalar(f"select count(*) from staging.trip_features where trip_month = '{month}'")
    return dg.MaterializeResult(metadata={"rows": dg.MetadataValue.int(int(rows))})
