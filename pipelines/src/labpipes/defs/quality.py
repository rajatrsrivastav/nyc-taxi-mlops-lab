"""Measure staged trips and publish eligibility only under an explicit quality policy."""

import math

import dagster as dg
from pydantic import Field

from ..partitions import source_file, trip_months
from ..resources import RedshiftData
from ..sql import statements
from ..warehouse import checked_run_id
from .staging import stg_trips

RATE_METRICS = {
    "null_timestamps", "null_zones", "null_passengers", "null_distances",
    "invalid_durations", "invalid_zones", "negative_distances", "negative_passengers",
    "duplicate_rows", "row_count_difference",
}


class QualityPolicy(dg.Config):
    # Rates are proportions. row_count is the minimum number of staged rows.
    warning_limits: dict[str, float] = Field(default_factory=dict)
    blocking_limits: dict[str, float] = Field(default_factory=dict)


@dg.asset_check(asset=stg_trips, partitions_def=trip_months, blocking=True, pool="redshift")
def staging_quality(
    context: dg.AssetCheckExecutionContext, redshift: RedshiftData, config: QualityPolicy
) -> dg.AssetCheckResult:
    month = context.partition_key
    file = source_file(month)
    run_id = checked_run_id(context.run.run_id)
    warnings, blockers = config.warning_limits, config.blocking_limits
    for limits in (warnings, blockers):
        for metric, limit in limits.items():
            if not math.isfinite(limit) or limit < 0 or (
                metric != "row_count" and (metric not in RATE_METRICS or limit > 1)
            ):
                raise ValueError(f"Invalid quality limit for {metric!r}")

    owner = f"trip_month = '{month}' and run_id = '{run_id}'"
    status = redshift.scalar(f"select status from staging.trip_month_quality where {owner}")
    if status not in {"staged", "measured", "approved", "rejected"}:
        raise dg.Failure("This run does not own the staged month. Retry ingestion from trip_files.")

    profiling, duplicates = statements("staging_quality", month=month)
    metrics = dict(redshift.query(profiling)[0])
    metrics["duplicate_rows"] = int(redshift.scalar(duplicates))
    staged = int(metrics["staged_rows"])
    loaded = int(redshift.scalar(f"select count(*) from ingest.raw_trips where source_file = '{file}'"))
    metrics["raw_rows"] = loaded
    metrics["row_count_difference"] = abs(loaded - staged)
    rates = {
        metric: int(metrics[metric]) / (staged or 1)
        for metric in RATE_METRICS - {"row_count_difference"}
    }
    rates["row_count_difference"] = abs(loaded - staged) / (max(loaded, staged) or 1)

    def exceeded(limits: dict[str, float]) -> list[str]:
        return sorted(
            metric for metric, limit in limits.items()
            if (staged < limit if metric == "row_count" else rates[metric] > limit)
        )

    warning_failures = exceeded(warnings)
    blocking_failures = exceeded(blockers)
    has_policy = bool(warnings or blockers)
    status = "rejected" if blocking_failures else "approved" if has_policy else "measured"
    redshift.execute(f"update staging.trip_month_quality set status = '{status}' where {owner}")
    description = {
        "measured": "Measurements only. No approved limits are configured, so the month is excluded from the mart.",
        "approved": "Month is eligible for the mart under the configured limits.",
        "rejected": "Blocking data-quality limits exceeded. Month is excluded from the mart.",
    }[status]
    return dg.AssetCheckResult(
        passed=not (warning_failures or blocking_failures),
        severity=dg.AssetCheckSeverity.ERROR if blocking_failures else dg.AssetCheckSeverity.WARN,
        description=description,
        metadata={"counts": metrics, "rates": rates, "status": status,
                  "warning_failures": warning_failures, "blocking_failures": blocking_failures, **metrics},
    )
