# 005: Staging quality gates

Stage: S1 · Date: 2026-11-21 · Author: student · Drafted by agent: yes

## Choice

Approved: keep the frozen schema-contract check blocking and use the staging limits below as the default for manual and sensor-triggered `ingest_trips` runs. The four measurement runs used the earlier deployment's empty policy. Each live month remains `measured` and excluded from the mart, pending a separate deployment and reprocessing approval.

## Rejected alternative

Zero tolerance for every anomaly would reject all four representative months on durations or zones, and February on one duplicate. A warning-only policy would allow a month with missing trip keys, unexplained row loss, or severe anomaly rates to pass. The earlier draft's empty-load-only floor and higher duration, zone, and duplicate ceilings would also admit more severe corruption. The approved limits flag low volume sooner and block larger deviations while allowing all four measured months through.

## Why

Dagster footer metadata and independent Redshift Data API queries agreed on the measurements for commit `c289cd3`. Anomaly rates divide by the staged row count, except the row count difference rate, which divides by the greater of the raw and staged counts.

| Measurement | 2024-01 | 2024-02 | 2024-03 | 2024-04 |
|---|---:|---:|---:|---:|
| Footer, raw, and staged rows (each) | 2,964,624 | 3,007,526 | 3,582,628 | 3,514,289 |
| Raw/staged count difference | 0 | 0 | 0 | 0 |
| Null timestamps, pickup zones, drop-off zones, or distances (each) | 0 | 0 | 0 | 0 |
| Null passenger counts | 140,162 (4.728%) | 185,610 (6.172%) | 426,190 (11.896%) | 408,576 (11.626%) |
| Invalid durations | 2,853 (0.0962%) | 2,751 (0.0915%) | 3,300 (0.0921%) | 3,352 (0.0954%) |
| Invalid pickup or drop-off zones | 31,527 (1.0634%) | 28,974 (0.9634%) | 35,374 (0.9874%) | 35,759 (1.0175%) |
| Negative distances or passenger counts (each) | 0 | 0 | 0 | 0 |
| Excess duplicate staged rows | 0 | 1 (0.00003325%) | 0 | 0 |

### Approved staging limits

Rate violations use a strict greater-than comparison. Row count minimums use a strict less-than comparison. Numeric rates below are the exact Dagster config fractions.

| Metric | Warn | Block | Rationale |
|---|---:|---:|---|
| `row_count` | Below `2800000` | Below `2500000` | The smallest month had 2,964,624 rows. The warning and blocking floors are about 94% and 84% of that baseline. Check this floor again if a legitimate month has lower volume. |
| `row_count_difference` | — | Above `0.0` | Every row should survive the staging transform. The four observed monthly counts match exactly. |
| `null_timestamps`, `null_zones` | — | Above `0.0` | All four months had complete timestamps and zone IDs. Missing trip times or zone keys compromise aggregation. |
| `invalid_durations` | Above `0.0012` | Above `0.005` | The observed maximum is 0.0962%. Warn at 0.12% and block at 0.5%, just over five times the measured maximum. |
| `invalid_zones` | Above `0.013` | Above `0.03` | The observed maximum is 1.0634%. Warn at 1.3% and block at 3%, about 2.8 times that maximum. |
| `duplicate_rows` | Above `0.000001` | Above `0.00001` | February had one excess row, about 0.33 per million. Warn above one per million and block above ten per million. |
| `null_passengers` | Above `0.15` | — | Valid-month rates varied from 4.728% to 11.896%. Passenger count is unused by this mart's grouping and duration calculation. |
| `null_distances`, `negative_distances`, `negative_passengers` | Above `0.0` | — | The baseline is zero. Warn on the first occurrence so a change can be investigated. |

All four measured months pass without warnings in local policy evaluation. A missing required trip key, a load below the blocking floor, a raw/staged count mismatch, or a rate above a blocking ceiling fails the monthly gate. No instructor corrupt fixture was available under the bucket's documented `events/` prefix or in the local checkout. Local tests use synthetic corrupt rows and a synthetic Parquet schema mismatch.

The Dagster job uses these defaults when run config is `{}`. The check records the effective maps in its metadata. An explicit measurement-only run can set both `warning_limits` and `blocking_limits` to empty maps under `ops.stg_trips_staging_quality.config`. A warning-only failure records an alert and allows the month into the mart.

When an approved month is reloaded or rejected, the pipeline rebuilds the existing mart from the remaining approved months in the same Redshift transaction that revokes eligibility. This also covers a blocking check that skips the normal mart asset. Later full rebuilds use the same eligibility filter.

## What would show this was wrong

Revisit limits that reject representative valid months or accept the supplied corrupt month. Record those counts, denominators, and the effect on mart coverage.
