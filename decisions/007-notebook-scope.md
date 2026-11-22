# 007: Notebook scope

Stage: S2 · Drafted by agent · Approved by student

## Choice

Use the notebook for exploration, leakage review and a side-by-side comparison of the three
models. Put repeatable feature validation, training, evaluation, registration and the challenger
gate in Dagster. Keep exploratory runs in the notebook's separate MLflow experiment.

## Alternatives and trade-offs

Moving every experiment into Dagster makes comparisons reproducible but slows exploratory work.
Leaving registry writes in the notebook would make publication depend on a person's session and
would bypass the required gate. Review this split if the production evaluation cannot reproduce
the notebook's results on the same inputs.
