# 010: Serving reload strategy

Stage: S3 · Drafted by agent · Approved by student

## Choice

Load each alias at startup. After a reviewed promotion, explicitly reload the champion
container. Load and warm the new version before swapping the in-memory reference. An
existing champion keeps answering during the load. Before the first champion, the app
serves the static median and identifies it in the response.

## Rejected alternative

Restart the container on every promotion. A restart is simple, but it discards the
healthy model before MLflow has supplied its replacement. Polling a loaded alias would
also remove the deliberately timed cutover from the approved workflow.

## Why

Reload keeps steady-state traffic on the old version until the replacement is usable.
The workflow checks the model returned by the public app and records the outgoing
version. Failed promotion restores that version and reloads it. If no previous
champion exists, recovery restores the unset alias and static fallback.

## What would show this was wrong

A reload exhausts app-host memory while both models are resident, or measured latency
or fallback rates rise during repeated promotions.
