# Runbook: Restore the champion prediction service

Owner: Rajat Srivastav · Last run: 2026-11-22 · Time to restore last run: not applicable

## When to use it

Use this after Learner Lab startup, or when the app falls back to 12.25 minutes, a database is unavailable, or grader evidence is stale.

## Before you start

Use the `academy` profile in `us-east-1` and confirm STS reports account `307845861255` and role `voclabs`. Use an allowlisted network, an authenticated GitHub CLI, and the existing initialized Terraform backend. Start with `bash scripts/health.sh` and save its output and exit code.

## Steps

The initial scan began at **12:37:53 IST** and finished at **12:38:08 IST** on November 22, 2026. It took **15 seconds**, exited **0**, and reported zero failures and zero warnings. The time is for verification. No restoration was required.

| # | Check (command) | Healthy looks like | If not: fix (command) | Took | Automated? |
| --- | --- | --- | --- | --- | --- |
| 1 | `aws sts get-caller-identity` | Academy `voclabs` session verified | Select Academy credentials or refresh them through Learner Lab and recheck identity | Already healthy | Verify identity |
| 2 | `scripts/health.sh` and `aws ec2 describe-security-groups` | Laptop IP matched the configured and live TCP 80 student allowlist | Use the allowlisted network or request review of an ingress change | Already healthy | Verify the laptop allowlist |
| 3 | `scripts/health.sh` | App, platform and LLM hosts running. App and platform runners online. LLM runner is deferred to S6 | Inspect Learner Lab, SSM and runner status. Get approval before restarting | Already healthy | Check host and runner status |
| 4 | `scripts/health.sh` and grader platform artifact | RDS and Redshift available. Notebook stopped as expected | With approval, start a stopped RDS first and wait for it. Resume Redshift only if paused. Start the notebook only for exploration | Already healthy | Verify service states |
| 5 | Platform and app-host status artifacts | Dagster answered, ingestion was running, MLflow champion and challenger both pointed to v1. Green served the S3 SHA and Blue retained the rollback SHA. Weights were 100/0 | Inspect the affected service and dependencies via SSM. Request approval before restarting. Use the gated S3 workflows for any deliberate model or traffic change | Already healthy | Cross-check artifacts |
| 6 | Public `/version` and `/predict` | Green served `a941518eee4f8600289b4afbded6de03d50477c1`. The JFK-to-midtown smoke quote returned 63.43 minutes and identified `trip-duration@champion` v1 | Check serving via the private interface and inspect MLflow and app logs before proposing a repair | Already healthy | Verify release and prediction |
| 7 | `gh run list --workflow status.yaml --branch main --limit 1` | Status run 44995680065 succeeded. It was 15 minutes old at the start of triage | If evidence is stale, arrange one status dispatch, wait for completion and inspect both artifacts | Already fresh | Verify run and artifacts |

## Done when

The public app returns a positive quote from the expected champion. The health script exits zero. A recent successful status run confirms available databases, running ingestion, and both application colours.

## What we automated, and what we left as a step

Run the proposed verifier from the repo after the normal lab startup.

```bash
AWS_PROFILE=academy bash scripts/verify-restoration.sh \
  307845861255 a941518eee4f8600289b4afbded6de03d50477c1 1
```

It combines the health check, current public API and existing grader evidence. Credential refresh, status dispatch and service changes remain operator steps. The optional reboot drill was outside the approved scenario. See [decision 012](../decisions/012-recovery-automation.md).

[Observed grader run](https://github.com/rajatrsrivastav/nyc-taxi-mlops-lab/actions/runs/44995680065)
