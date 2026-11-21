# 002: Image tag scheme

Stage: S0 · Date: 2026-11-21 · Author: student · Drafted by agent: yes

## Choice

Tag every image as `sha-<7-char-commit-sha>` (e.g. `sha-0a1b2c3`). This is the same scheme used by every given workflow in the repo (platform.yaml, serving.yaml, llm.yaml, mcp.yaml).

## Rejected alternative

Use `latest` as the sole tag, overwriting it on every push.

## Why

`sha-<7char>` identifies the commit prefix, and `/version` reports the full SHA. Rollback uses a retained, known-good image tag with the app Compose stack's `IMAGE_PREFIX` and `IMAGE_TAG` set. GHCR tags can be overwritten, so rebuilding the same commit can change the image behind its tag. Using only `latest` removes the stable reference to a previous build. SemVer would add a release numbering step for every main push.

## What would show this was wrong

If the repo needed human-meaningful version numbers (e.g. for a customer-facing changelog), a semver scheme on top of sha tags would be worth the process. A collision between seven-character prefixes would require revisiting the tag scheme.
