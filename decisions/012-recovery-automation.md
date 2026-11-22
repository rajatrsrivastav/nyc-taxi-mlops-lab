# 012: Recovery automation

Stage: S4 · Date: 2026-11-22 · Author: Rajat Srivastav · Drafted by agent: yes

## Choice

Approved by the student. Run a read-only checkpoint verifier on demand after normal Learner Lab startup or during recovery. Verify the Academy identity, existing health report, latest grader evidence, live release and champion prediction.

## Rejected alternative

Run a boot-time repair script that starts services or redeploys containers automatically. It would reduce operator work during a known outage, but could take action before dependencies or temporary Academy credentials are ready.

## Why

The observed Day 2 health check was green on its first pass. The repeated work was correlating API, registry and rollback evidence. The verifier automates that comparison. The operator decides when to refresh credentials or grader evidence and approves any service restart or infrastructure change. The verifier performs no model, traffic, infrastructure or workflow mutations.

## What would show this was wrong

Frequent real outages with the same proven cause, a safe idempotent recovery command and timing evidence that the manual step materially delays restoration.
