# NavCharge

Real-time usage metering, quota enforcement, and billing platform.
Domain instance: en-route air navigation charging over live ADS-B surveillance data.

## Read these first

- `docs/architecture.md` - the system specification. Architecture, data sources,
  charging methodology, persistence rationale, Kafka design, phase plan,
  measurement catalog, decision record list.
- `docs/phase1-build-spec.md` - concrete Phase 1 build order. Compose stack, DDL,
  admission contract, property tests, hour budget, definition of done.

## Current state

Phase 1, week 1. Repository skeleton only. No services built.

## Rules

**SCOPE IS LOCKED.** `docs/architecture.md` is the commitment. New ideas go in
`docs/future-work.md`, never into scope silently. If a change to the
architecture seems warranted, raise it and wait rather than implementing it.

**MEASUREMENT DISCIPLINE.** Never state a number that is not recorded in
`docs/measurements.md`. Instrument from the first commit. None of these figures
can be reconstructed later.

**DECISION RECORDS.** Every non-obvious component needs an ADR in `docs/adr/`
answering "why this and not the simpler thing." If the record cannot be
written, cut the component.

**PROPERTY TESTS ARE THE PHASE 1 DELIVERABLE,** not the charge function itself.
Every later phase assumes a charge is deterministic and never double-counted.
Do not defer them to make room for something more visible.

**MONEY IS DECIMAL.** Never a float, anywhere. Integer minor units in storage.
Half-up rounding to minor units at the final step only, never at intermediates.
The charge function is pure: no clock reads, no random, no ambient state.

**COST.** Cloud infrastructure runs ephemerally. A destroy target runs at the
end of every cloud session. Nothing is left provisioned overnight.

**STYLE.** No em-dash character anywhere in code, comments, documentation, or
commit messages. Expand all contractions.

## Do not build in Phase 1

Spark, Flink, Iceberg, ClickHouse, Azure, Kubernetes, Terraform, dbt, any model
serving or agent layer, any frontend beyond curl and a request collection.

Phase 1 is local Docker Compose only. The goal is that the rating function is
correct and the ingress is idempotent.

## Working method

Follow the hour budget in `docs/phase1-build-spec.md` section 9. Stop at the end
of each budgeted item and let me review before continuing to the next one.

Prefer small, reviewable commits over large ones. Commit messages describe what
changed and why, not just what.

## Secrets

Never commit `.env`, `credentials.json`, `*.tfstate`, or any key material.
`.env.example` documents every required variable with no values.
