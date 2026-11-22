# 009: Training and holdout months

Stage: S2 · Drafted by agent · Approved by student

## Choice

The first candidate trains on January through March 2024 and holds out April. The pipeline
requires an explicit contiguous training window and the following month as holdout. It estimates
zone-pair distance using only trips from the chosen training window.

## Alternatives and trade-offs

February–March with April as holdout gives the same comparison period and puts more weight on
recent trips, with fewer examples for sparse zone pairs. Training on January–February and
holding out March tests an earlier cutoff. A random split across all four months would mix
past and future within the evaluation. Compare error by slice and coverage of sparse zone
pairs before choosing a shorter window.
