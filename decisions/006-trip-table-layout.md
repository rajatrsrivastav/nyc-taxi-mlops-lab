# 006: Trip table distribution and sort keys

Stage: S1 · Date: 2026-11-21 · Author: student · Drafted by agent: yes

## Choice

Approved: start with DISTSTYLE AUTO. Keep the provided staging sort key on `pickup_at` and use the corresponding source pickup timestamp, `tpep_pickup_datetime`, for the native landing table.

## Rejected alternative

Choose a zone-based DISTKEY and a month-first compound sort key before observing query and load performance.

## Why

AUTO lets Redshift choose distribution as the tables grow. A zone key could collocate a matching join, but uneven trip counts across zones can skew work. Timestamp ordering suits time-range scans. A month-first key could improve monthly reload queries, while changing how time-range queries across months prune blocks. Single-node results provide limited evidence about redistribution on a larger cluster.

## What would show this was wrong

Use EXPLAIN and warehouse measurements to compare scanned rows or blocks, redistribution, skew, unsorted percentage, and elapsed time for monthly replacement and the mart refresh. Revisit only when those measurements justify a different layout.
