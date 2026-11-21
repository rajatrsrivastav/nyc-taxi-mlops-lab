# 004: Monthly warehouse ingestion

Stage: S1 · Date: 2026-11-21 · Author: student · Drafted by agent: yes

## Choice

Approved: use native Redshift COPY. Load each validated monthly Parquet object through a one-file manifest into a temporary table, then replace only that source file's persistent rows in a single Data API transaction.

## Rejected alternative

Query the Parquet files in place using Spectrum external tables.

## Why

Native tables support repeatable monthly replacement and the staging-to-mart workflow in the handout. COPY adds load time and warehouse storage. Spectrum avoids the second stored copy, but repeated month scans depend on the external layout and incur scan work. The one-file manifest also avoids loading other keys that match the monthly object's prefix.

## What would show this was wrong

Revisit after measuring monthly load time, warehouse storage, and the cost and latency of repeatedly scanning the same months.
