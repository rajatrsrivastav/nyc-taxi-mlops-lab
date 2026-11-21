"""Schedule one ingestion run for each new or changed monthly object."""

import json

import dagster as dg

from ..partitions import trip_months
from ..resources import DataBucket
from ..warehouse import REVISION_TAG

ingest_trips = dg.define_asset_job(
    "ingest_trips",
    selection=dg.AssetSelection.assets("taxi_zones", "trip_files", "raw_trips", "stg_trips", "zone_hour_stats"),
)


@dg.sensor(job=ingest_trips, minimum_interval_seconds=60)
def trip_files_sensor(context: dg.SensorEvaluationContext, data_bucket: DataBucket):
    seen = json.loads(context.cursor or "{}")
    latest = {file.month: file.revision for file in data_bucket.trip_files()}
    changed = sorted(month for month, revision in latest.items() if seen.get(month) != revision)
    if not changed:
        return dg.SkipReason("No new trip file revisions.")
    return dg.SensorResult(
        dynamic_partitions_requests=[trip_months.build_add_request(changed)],
        run_requests=[
            dg.RunRequest(
                partition_key=month,
                run_key=f"{month}:{latest[month]}",
                tags={REVISION_TAG: latest[month]},
            )
            for month in changed
        ],
        cursor=json.dumps(seen | latest, sort_keys=True),
    )
