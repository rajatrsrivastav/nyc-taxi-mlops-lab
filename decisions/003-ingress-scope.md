# 003: Ingress scope

Stage: S0 · Date: 2026-11-21 · Author: student · Drafted by agent: yes

## Choice

Allow port 80 on the app host from a small set of specific /32 CIDRs: the student's current public IP and the grader's IP. The CIDRs live in `terraform.tfvars` (gitignored) via the `ingress_cidrs` variable.

## Rejected alternative

Open port 80 to the entire internet (`0.0.0.0/0`).

## Why

The app is a prediction endpoint with no authentication, running on a Learner Lab instance with LabRole (which has broad AWS permissions). Restricting ingress to known IPs limits who can probe the service. The cost is updating `terraform.tfvars` and re-applying when the student's IP changes (ISP, VPN), but this is a ~30-second operation. The grader's automated checks need to reach `/version` and `/predict`, so the grader's IP must be included — ruling out "student IP only."

## What would show this was wrong

If the student changes networks frequently (e.g. laptop on mobile hotspot) and the constant re-apply becomes a drag on the lab, switching to `0.0.0.0/0` would be justified: the app has no write endpoints, no stored credentials in memory, and the host has no SSH, so the residual risk is low.
