# 005: Staging quality gates

Stage: S1 · Date: 2026-11-21 · Author: student · Drafted by agent: yes

## Choice

Approved: keep the frozen schema-contract check blocking. Pending: blocking versus warning for the staging metrics and every staging threshold. With no approved policy, the pipeline records measurements and excludes the unreviewed month from the mart.

## Rejected alternative

No staging alternative has been rejected yet. Compare blocking on excessive anomaly rates with warnings that keep an accepted month publishable. Thresholds need data from the lab.

## Why

Measure January through April 2024 in Redshift. Capture raw and staged row counts and their difference, null timestamps, zone IDs, passenger counts and distances, out-of-range durations and zones, negative distances and passenger counts, and excess copies of identical staged rows. Record each count with its denominator and monthly rate. Compare the ranges across months and examine how the instructor's corrupt file differs before agreeing which signals block.

The existing mart duration and zone filters define measurements of those ranges. They do not set an allowed monthly failure rate.

The Dagster job accepts run config on `ops.stg_trips_staging_quality.config`. `warning_limits` and `blocking_limits` map metric names to maximum rates from 0 to 1. For `row_count`, the configured number is a minimum staged count. After review, add the chosen limits as the scheduled job's default policy and rerun previously profiled partitions explicitly so they are evaluated under it. The status table prevents a rejected or unreviewed partition from being pulled into a later full mart rebuild.

## What would show this was wrong

Revisit limits that reject representative valid months or accept the supplied corrupt month. Record those counts, denominators, and the effect on mart coverage.
