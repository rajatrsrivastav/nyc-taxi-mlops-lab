# 008: Challenger gate

Stage: S2 · Drafted by agent · Approved by student

## Choice

Compare holdout MAE in minutes against the current champion, or the given v1 baseline if there
is no champion. Require a strictly positive improvement and an explicit minimum margin and
p95 single-row latency budget. Require an absolute improvement of at least 0.25 minutes and in-process single-row p95 latency of at most 10.0 ms on the platform host.

## Alternatives and trade-offs

MAE is interpretable in minutes. RMSE gives long misses more influence and may favor a model
with a worse typical error. A small margin makes it easier to publish gains that might disappear
on a later month. A large margin can hold back a practical improvement. Use comparable holdout
predictions, review the required slice metrics, and measure p95 on the platform host to verify the budget. Revisit this approach if a meaningful slice regresses despite aggregate gains.
