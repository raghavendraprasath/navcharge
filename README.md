# NavCharge

Real-time usage metering, quota enforcement, and billing platform.

Ingests continuous usage events, enforces per-tenant quotas at low latency,
resolves each event spatially, rates it against a versioned pricing catalog,
aggregates charges in a streaming pipeline, and reconciles the streaming result
against an independent batch recompute.

The domain instance is en-route air navigation charging, using live ADS-B
surveillance data from the OpenSky Network and the published charging
methodology: the charge for a segment is the product of a distance factor, a
weight factor derived from maximum take-off weight, and a national unit rate for
the charging zone traversed.

## Stack

Java 21 and Spring Boot on the admission path, Python on the rating engine,
Kafka for the event backbone, Flink for quota state, Spark Structured Streaming
for the billing aggregate, PostgreSQL with PostGIS for spatial resolution,
MongoDB for the rate card catalog, Redis for idempotency, Iceberg on object
storage as the system of record, ClickHouse for interactive analytics, dbt for
the transformation layer, Kubernetes and Terraform for the platform, Next.js for
the dashboard.

## Status

Phase 1 in progress. See `docs/architecture.md` for the full specification and
`docs/phase1-build-spec.md` for the current build order.

## Scope of claim

This platform estimates charges independently from observed surveillance data.
It does not reproduce official invoices, which are billed on flight plan records
rather than on observed positions. Aircraft weight figures are type-level
approximations, not certificated per-airframe values. A production system would
use declared fleet data.

## Documentation

- `docs/architecture.md` - system specification
- `docs/phase1-build-spec.md` - Phase 1 build order
- `docs/adr/` - architecture decision records
- `docs/measurements.md` - recorded measurements
