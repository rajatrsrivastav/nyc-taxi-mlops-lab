# 001: Runner credentials

Stage: S0 · Date: 2026-11-21 · Author: student · Drafted by agent: yes

## Choice

Use the EC2 instance profile (LabInstanceProfile) for the self-hosted runner, not static session credentials in GitHub Secrets.

## Rejected alternative

Store Learner Lab session credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN) in GitHub Secrets and have the CD workflow read them as environment variables.

## Why

The CD workflow does not call the AWS API: it pulls images from GHCR (authenticated by GITHUB_TOKEN) and runs Docker Compose. The instance profile is already on the host and rotates transparently — there is nothing to maintain. Static session credentials expire every ~4 hours and would require re-running `scripts/gh-secrets.sh` after every Start Lab, adding friction for zero benefit. The only workflow that actually needs session credentials is `destroy.yaml` (S10), which runs on a GitHub-hosted runner.

## What would show this was wrong

A future CD step needs to call the AWS API from the GitHub-hosted CI runner (not the self-hosted deploy runner). In that case, CI would need session credentials in secrets. But S0's CI only builds and pushes images, so this is unlikely before S10.
