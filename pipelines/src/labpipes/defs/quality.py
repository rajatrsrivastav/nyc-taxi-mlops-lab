"""Measure staged trips and publish eligibility only under an explicit quality policy."""

import math

import dagster as dg
from pydantic import Field

from ..partitions import source_file, trip_months
from ..resources import RedshiftData
from ..sql import statements
from ..warehouse import checked_run_id, repair_published_mart
from .staging import stg_trips

RATE_METRICS = {
    "null_timestamps", "null_zones", "null_passengers", "null_distances",
    "invalid_durations", "invalid_zones", "negative_distances", "negative_passengers",
    "duplicate_rows", "row_count_difference",
}

APPROVED_WARNING_LIMITS = {
    "row_count": 2800000,
    "invalid_durations": 0.0012,
    "invalid_zones": 0.013,
    "duplicate_rows": 0.000001,
    "null_passengers": 0.15,
    "null_distances": 0.0,
    "negative_distances": 0.0,
    "negative_passengers": 0.0,
}
APPROVED_BLOCKING_LIMITS = {
    "row_count": 2500000,
    "row_count_difference": 0.0,
    "null_timestamps": 0.0,
    "null_zones": 0.0,
    "invalid_durations": 0.005,
    "invalid_zones": 0.03,
    "duplicate_rows": 0.00001,
}


class QualityPolicy(dg.Config):
    # Empty run config uses the approved policy. Explicitly passing both maps as
    # empty preserves measurement mode. row_count is a minimum, rates are fractions.
    warning_limits: dict[str, float] = Field(default_factory=APPROVED_WARNING_LIMITS.copy)
    blocking_limits: dict[str, float] = Field(default_factory=APPROVED_BLOCKING_LIMITS.copy)


def quality_outcomes(policy: QualityPolicy, metrics: dict) -> tuple[dict, list[str], list[str]]:
    staged = int(metrics["staged_rows"])
    loaded = int(metrics["raw_rows"])
    rates = {
        metric: int(metrics[metric]) / (staged or 1)
        for metric in RATE_METRICS - {"row_count_difference"}
    }
    rates["row_count_difference"] = abs(loaded - staged) / (max(loaded, staged) or 1)

    def exceeded(limits: dict[str, float]) -> list[str]:
        for metric, limit in limits.items():
            if not math.isfinite(limit) or limit < 0 or (
                metric != "row_count" and (metric not in RATE_METRICS or limit > 1)
            ):
                raise ValueError(f"Invalid quality limit for {metric!r}")
        return sorted(
            metric for metric, limit in limits.items()
            if (staged < limit if metric == "row_count" else rates[metric] > limit)
        )

    return rates, exceeded(policy.warning_limits), exceeded(policy.blocking_limits)


@dg.asset_check(asset=stg_trips, partitions_def=trip_months, blocking=True, pool="redshift")
def staging_quality(
    context: dg.AssetCheckExecutionContext, redshift: RedshiftData, config: QualityPolicy
) -> dg.AssetCheckResult:
    month = context.partition_key
    file = source_file(month)
    run_id = checked_run_id(context.run.run_id)
    warnings, blockers = config.warning_limits, config.blocking_limits
    owner = f"trip_month = '{month}' and run_id = '{run_id}'"
    prior_status = redshift.scalar(f"select status from staging.trip_month_quality where {owner}")
    if prior_status not in {"staged", "measured", "approved", "rejected"}:
        raise dg.Failure("This run does not own the staged month. Retry ingestion from trip_files.")

    profiling, duplicates = statements("staging_quality", month=month)
    metrics = dict(redshift.query(profiling)[0])
    metrics["duplicate_rows"] = int(redshift.scalar(duplicates))
    staged = int(metrics["staged_rows"])
    loaded = int(redshift.scalar(f"select count(*) from ingest.raw_trips where source_file = '{file}'"))
    metrics["raw_rows"] = loaded
    metrics["row_count_difference"] = abs(loaded - staged)
    rates, warning_failures, blocking_failures = quality_outcomes(config, metrics)
    has_policy = bool(warnings or blockers)
    status = "rejected" if blocking_failures else "approved" if has_policy else "measured"
    repair = repair_published_mart(redshift, month) if prior_status == "approved" and status != "approved" else []
    redshift.execute(f"update staging.trip_month_quality set status = '{status}' where {owner}", *repair)
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
                  "warning_limits": warnings, "blocking_limits": blockers,
                  "warning_failures": warning_failures, "blocking_failures": blocking_failures, **metrics},
    )
