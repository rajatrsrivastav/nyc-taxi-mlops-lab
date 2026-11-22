# 011: Blue-green switch criteria

Stage: S3 · Drafted by agent · Approved by student

## Choice

After trusted CI and image publication succeed on main, CD deploys the idle colour and
checks its container health, exact Git SHA, positive prediction, and HTTP 422 for an
invalid zone. CD switches after those checks and verifies the public SHA. The old
colour stays running. An operator can dispatch rollback to switch back without a build.

## Rejected alternative

Update a single container in place or require a separate approval for each app rollout.
An in-place update couples recovery to container startup. Per-release approval delays
routine app changes when the version and API checks already provide a clear gate.
Model promotion uses its own required human review.

## Why

A warm previous colour makes rollback a routing change. The initial proxy migration
can briefly interrupt requests. Keeping two app containers consumes extra memory on
the existing host. Pin-to-colour diagnostics use a host-local entry point.

## What would show this was wrong

Both colours repeatedly pressure the host, the smoke checks miss rider-facing
regressions, or measured cutover or rollback time exceeds the lab's tolerance.
