# NavCharge Architecture

Technical specification for a real-time usage metering, quota enforcement, and billing platform.

| Field | Value |
|---|---|
| Version | 1.0 |
| Status | Phase 1 in progress |
| Languages | Java 21, Python 3.11, TypeScript |
| Deployment target | Kubernetes on Azure, provisioned by Terraform |

---

## 1. Overview

NavCharge ingests continuous usage events, enforces per-tenant quotas at low latency, resolves each event spatially, rates it against a versioned pricing catalog, aggregates charges in a streaming pipeline, reconciles the streaming result against an independent batch recompute, models the output through a tested transformation layer, and exposes it through a tenant-facing dashboard with a served and evaluated explanation layer.

The domain instance is en-route air navigation charging. Airlines are billed for flying through controlled airspace under a published methodology: the charge for a segment is the product of a distance factor, a weight factor derived from maximum take-off weight, and a national unit rate for the charging zone traversed. Unit rates are published annually and adjusted monthly, so closed periods are periodically restated.

### 1.1 Why this domain

Usage metering and billing is a well-defined product category. Stripe Billing, Orb, Metronome, Lago, and Amberflo are companies built on this problem, and most large enterprises run an internal equivalent as chargeback or showback.

Air navigation charging supplies properties a synthetic dataset cannot:

- **Real duplicates.** ADS-B is crowd-sourced, so identical observations genuinely arrive from multiple receivers. Deduplication operates on real duplicates rather than injected ones.
- **Real late and out-of-order events.** Receiver coverage gaps produce genuinely delayed and misordered reports, so watermarking and lateness bounds are exercised against real conditions.
- **Real tenancy and real skew.** The ICAO operator code is the tenant. Traffic distribution across operators is measured rather than simulated.
- **Real restatement pressure.** Monthly rate adjustments force recomputation of closed periods, which makes snapshot isolation and time travel functional requirements.
- **Load-bearing spatial computation.** A position stream becomes a billable segment only through point-in-polygon resolution and great-circle distance. Without spatial resolution there is no charge.
- **Money-critical correctness.** Idempotency, exactly-once reasoning, reconciliation, and auditability are requirements rather than optional hardening.

### 1.2 Scope of claim

The platform estimates charges independently from observed surveillance data. It does not reproduce official invoices, which are billed on flight plan records rather than on observed positions, and it uses type-level rather than certificated per-airframe weight figures. A production system would use declared fleet data.

---

## 2. Data Sources

| Source | Access | Use and engineering constraint |
|---|---|---|
| OpenSky Network state vectors | OAuth2 client credentials, free non-commercial | Live position, altitude, velocity, callsign, and 24-bit airframe address. Credit-budgeted at roughly 100 credits per day anonymous, 4,000 authenticated, and up to 8,000 when contributing a feed. Credit cost scales with bounding-box area. Exhaustion returns HTTP 429 with a retry-after header. Basic authentication was retired in March 2026. |
| OpenSky aircraft database | Bulk download | Airframe to type-code and operator mapping. Incomplete, which makes source quality evaluation part of the ingestion path. |
| Aircraft type weight table | Public references | Type-level maximum take-off weight. Approximate by design and flagged as such in the schema. |
| Flight information region boundaries | Open GeoJSON | Charging zone polygons. Phase 1 seeds six to eight contiguous regions rather than all member states. |
| OpenStreetMap aerodrome and airspace features | Free, open license | A second heterogeneous spatial source requiring normalization against the primary boundary dataset. |
| Published unit rates and methodology | Public circulars | Per-state en-route and terminal unit rates, service unit formulas, exemption categories, and monthly adjustments. The monthly adjustment is the mechanism that forces restatement. Exemption clauses are unstructured prose. |

### 2.1 Rate limiting as a design driver

The upstream credit budget shapes the ingestion design rather than merely constraining it:

- A token bucket sized from a configured daily budget prevents account exhaustion.
- A tight bounding box keeps per-call credit cost low.
- The poll interval is derived from the budget rather than hardcoded, so the same code runs correctly at any allowance.
- Remaining credits are exported as a Prometheus gauge.
- Exponential backoff on 429 and 5xx, honoring the retry-after header.

Every raw response is persisted as compressed JSON with a timestamp. After a week of operation this becomes a historical corpus replayable at accelerated rate, which is how throughput, load, and lag-recovery figures are obtained without exceeding the upstream allowance. The replay harness is also the backfill source for the lake, and recorded responses allow offline development against a request-interception proxy.

### 2.2 Charging methodology

```
en-route service units = (distance_km / 100) * sqrt(mtow_tonnes / 50)
en-route charge        = service_units * zone_unit_rate

terminal service units = (mtow_tonnes / 50) ** 0.70
terminal charge        = service_units * terminal_unit_rate
```

Distance is great-circle distance between zone entry and exit points, not actual track length. All arithmetic uses decimal types with half-up rounding to minor currency units at the final step only. Money never touches a floating point type.

---

## 3. System Architecture

### 3.1 Layer flow

```
OpenSky poller (credit-budgeted OAuth2 client, replay harness)
  | gRPC, deadline propagated
  v
Admission service: Spring Boot, Java 21
  - derived idempotency key, Redis dedupe window
  - quota decision against Flink keyed state
  - circuit breaker on rate card lookup, bulkhead isolation
  - transactional outbox, leader-elected publisher
  v
Kafka on Strimzi (AKS), schema registry with enforced compatibility
  |                          |
  v                          v
Flink (quota state)     Spark Structured Streaming (billing aggregate)
                          - spatial resolution: broadcast zones, bbox prefilter
                          - tumbling windows, watermarks, lateness bound
                          - late events to correction topic
                          v
Iceberg on ADLS Gen2 (Parquet)   +   ClickHouse (hot analytical)
  |                                    |
  v                                    v
Azure Data Factory: monthly close    dbt: SCD2, incremental facts,
  - full recompute from snapshot            contracts, semantic layer, lineage
  - drift vs streaming aggregate            |
  - invoice extract per tenant              v
  |                                  Spatial analytics platform
  v
FastAPI query and admin API (async, SSE, OpenAPI, versioned)
  v
Next.js dashboard: live map, invoice explorer, route cost simulator, unit-cost panel

Cross-cutting: Terraform, Kubernetes, Prometheus, Grafana, APM, error tracking,
OpenTelemetry, structured logs with correlation IDs, GitHub Actions,
feature flags, secrets management, MLflow, model serving, model router,
LangGraph and MCP agent, evaluation harness, LLM tracing
```

### 3.2 Persistence choices

| Store | Role | Reason it is this and not the simpler option |
|---|---|---|
| PostgreSQL with PostGIS | Operators, fleet, zone geometry, segments, charges, outbox | Relational integrity on money rows, row-level security for tenant isolation, and spatial indexing for point-in-polygon against zone boundaries. The fleet table is the change data capture source. |
| MongoDB | Rate card catalog | Rate cards are heterogeneous nested documents: differing exponents, divisors, optional terminal blocks, effective-date ranges, and exemption rule sets per state. The relational alternatives are a wide sparse table or an entity-attribute-value table, both of which are worse for this shape. |
| Redis | Idempotency window, hot quota counters, distributed rate limiting | Atomic set-if-not-exists with expiry makes the duplicate check and the claim a single operation, with no race between concurrent admissions of the same observation. Admission latency rules out a relational unique constraint on the hot path. |
| Iceberg on ADLS Gen2 | Cold lake, system of record | Restatement requires reading the world as of a date, so snapshot isolation and time travel are functional requirements. Schema evolution covers the addition of new meters. |
| ClickHouse | Hot analytical store | Interactive latency on the cost explorer over hundreds of millions of segment rows. Also the site for partition pruning and query profile work. |

### 3.3 Stream processing: why two engines

- **Flink, on the quota and admission path.** A quota decision must complete in milliseconds against running per-operator consumption, which requires event-at-a-time processing with keyed state and timers. Micro-batch cannot satisfy it.
- **Spark Structured Streaming, on the billing aggregate.** Throughput over latency, windowed aggregation with watermarks, distributed spatial joins, and exactly-once writes into the lake. Seconds of latency are acceptable.

One engine serving both workloads would compromise one of them. If the split cannot be justified by the measured admission percentile, Flink is dropped and Spark used alone.

### 3.4 Kafka topic and partition design

| Topic | Key | Partitions | Retention | Contents |
|---|---|---|---|---|
| `usage.positions.v1` | airframe | 12 | 7 d | Admitted, deduplicated position reports |
| `usage.corrections.v1` | airframe | 6 | 30 d | Late events past the lateness bound |
| `billing.charges.v1` | operator | 6 | 30 d | Computed charge events |
| `fleet.cdc.v1` | airframe | 3 | compact | Change capture on fleet and weight state |
| `usage.positions.dlq` | airframe | 3 | 30 d | Rejected or unparseable admissions |

The partition key is the 24-bit airframe address, not the operator code. Segment detection requires strict ordering per airframe, and Kafka guarantees ordering only within a partition. Operator-level aggregation happens downstream where ordering is irrelevant. Keying on operator would serialize hundreds of aircraft onto one partition for a large carrier.

Skew is reported as measured. With the airframe as key, hot partitions arise from geographic density inside the bounding box rather than from tenant size. The per-partition rate is recorded rather than a solved hot-tenant problem being claimed.

---

## 4. Correctness and Distributed Systems Design

Each item is implemented, measured, and documented as a decision record.

1. **Derived idempotency keys.** Computed from airframe address and observation timestamp rather than client-supplied. The upstream source snaps observations to a five-second boundary, so overlapping polls return genuinely identical observations.
2. **Transactional outbox.** Prevents a successful response followed by a failed publish, which would lose an event silently. In a billing system a lost usage event is lost revenue.
3. **At-least-once with downstream deduplication.** True exactly-once was not pursued because it couples the admission path to broker availability, and at-least-once with idempotent consumers is simpler to reason about and to test.
4. **Watermarks and a configurable lateness bound.** Late events route to a correction topic rather than being dropped, and the correction path is exercised in tests.
5. **Leader election for the outbox publisher.** Multiple admission replicas must not all publish the same outbox rows. Lease-based election or a database advisory lock.
6. **Schema registry with enforced compatibility.** Protobuf schemas registered, backward-compatible evolution enforced, and a documented breaking change that the registry rejects.
7. **Resilience patterns.** Circuit breaker on the rate card lookup, bulkhead isolation between admission and query paths, timeout and retry budgets, and gRPC deadline propagation across hops.
8. **Consumer group rebalance tuning.** Rebalance triggered mid-load, pause measured, then cooperative sticky assignment applied and the improvement recorded.
9. **Backpressure and lag-driven autoscaling.** Horizontal pod autoscaling on Kafka consumer lag rather than on processor utilization, with lag recovery measured after an injected traffic spike.
10. **Stream and batch reconciliation.** Nightly full recompute from a lake snapshot compared against the streaming aggregate, with drift reported in minor currency units and root-caused per variance.
11. **Replay and restatement.** Reprocessing a closed month from a snapshot after a rate revision, writing the restatement as a new versioned row so the original invoice and audit trail survive.

---

## 5. Spatial Design

Spatial resolution is the mechanism that converts a position stream into a billable segment.

### 5.1 Foundational

- Geography-typed multipolygon charging zones and point geometries for segment entry and exit, with a spatial index and latency measured with and without it.
- Point-in-polygon resolution on every admitted position to determine the applicable zone.
- Great-circle distance between zone entry and exit, which is the distance factor in the charge formula.
- Crossing-point interpolation between consecutive positions, because the charge is measured at the boundary rather than at either observed point.
- GeoJSON ingestion of boundary data into the seed pipeline.

### 5.2 At scale

1. **Distributed spatial join.** Evaluating every position against forty-plus polygons through per-row database lookups is the wrong plan at volume. Zone geometries are broadcast in the stream processor and a bounding-box prefilter runs before the exact intersection test.
2. **Hierarchical cell indexing.** Positions are bucketed into spatial cells so the candidate polygon set is narrowed before any geometry operation runs.
3. **Gazetteer with identifier crosswalk.** Aerodrome and waypoint names and identifier codes resolved to coordinates, with normalization, fuzzy matching, survivorship rules, and a match confidence score attached to every resolution.
4. **Open-source spatial data normalization.** OpenStreetMap aerodrome and airspace features reconciled against the primary boundary dataset.
5. **Spatial stored procedures.** Point-in-polygon and distance logic moved into database functions, with performance measured against application-side execution.
6. **Coordinate reference system decision record.** Why a geography type rather than a planar geometry type, why this spatial reference identifier, and where a projected system would genuinely be required.

---

## 6. Data Platform

### 6.1 Transformation layer

- **Slowly changing dimension type 2 on rate cards.** Rate revisions are an SCD2 problem: valid-from, valid-to, current flag. Any invoice can be reproduced as of its charge date.
- **Incremental models.** Charge facts are append-heavy with late-arriving corrections, so a merge on a surrogate key with a lookback window, chosen over insert-overwrite because corrections arrive after the window closes.
- **Snapshots on the fleet and rate card tables,** which pairs with change data capture as two mechanisms for capturing change.
- **Enforced data contracts.** Model contracts with column types and constraints on the marts, so a breaking upstream change fails the build instead of corrupting a revenue table.
- **Test taxonomy.** Generic tests on uniqueness and relationships, singular tests on charge determinism and rate card coverage gaps, equality tests between the streaming aggregate and the batch recompute, and unit tests on the charge macro with fixed inputs.
- **Exposures and lineage.** Exposures pointing at the dashboard and the invoice export, with generated documentation published as a browsable data dictionary including column-level lineage.
- **Semantic layer.** Revenue per operator, service units per zone, average charge per segment, and effective unit rate defined once rather than repeated across dashboard queries.

### 6.2 Batch close

Azure Data Factory runs the monthly close as a full recompute from an Iceberg snapshot, reconciles the result against the streaming aggregate, reports drift as a metric, and exports invoice extracts per tenant. A second general-purpose orchestrator is deliberately not introduced.

### 6.3 Database platform and security

1. **Multi-tenant isolation with row-level security.** Policies so an operator can read only its own charges and segments, with role separation across the ingest, rating, query, and admin paths and privilege administration for each.
2. **Backup, restore, and disaster recovery.** Point-in-time recovery configured, a documented restore drill with measured recovery time and recovery point objectives, and a replica failover test.
3. **Source quality and attribution scorecard.** Completeness, freshness, coverage, licensing, and attribution requirements evaluated per source on every seed load, blocking ingestion when a source fails threshold.
4. **Published client interface.** A generated client for the admission and query APIs, an OpenAPI contract, a versioning strategy with a deprecation policy, and a request collection.
5. **Design documentation.** Entity relationship diagram, data flow diagram, and written use cases in `docs/design/`.

---

## 7. Platform, Delivery, and Reliability

### 7.1 Infrastructure

- Terraform provisioning the full footprint: managed Kubernetes, lake storage, event streaming, managed relational and document databases, the batch orchestrator, a secrets vault, and the container registry. Multi-environment through workspaces, a remote state backend with locking, modules rather than a flat file, and a verified full teardown and rebuild from a single apply.
- Kubernetes: operator-managed Kafka with stateful sets, persistent volume claims, resource requests and limits, and rolling restarts. Liveness and readiness probes, secrets from the vault, and horizontal pod autoscaling driven by consumer lag through a custom metrics adapter.
- Continuous integration with five gates: linting and static type checking, unit and container-based integration tests, the evaluation harness as a regression gate, a code-health and quality gate, and a cost delta comment on infrastructure changes.
- Feature flags gating the model promotion criterion, the routing policy, and the lateness bound.
- Centralized secrets management across local development, cloud development environments, and deployed environments.

### 7.2 Observability

- **Metrics.** Consumer lag, admission latency percentiles, rating throughput, deduplication rate, outbox lag, reconciliation drift per close, and a freshness objective with alerting.
- **Traces.** Spans end to end, from admission through the stream processors to the invoice line, with application performance monitoring on the service layer.
- **Logs and errors.** Structured logging with correlation identifiers propagated from the admission request through message headers into the processors and out to the charge row, plus error tracking with release association.
- **Model layer.** Trace inspection, prompt versioning, and token cost per generated explanation.

### 7.3 Reliability practice

1. **Service level objectives.** Availability target on admission, latency objective on the ninety-ninth percentile, freshness objective on the billing aggregate, and an error budget in minutes per month with burn-rate alerting against the budget rather than raw thresholds.
2. **Runbooks.** One page each for consumer lag breach, reconciliation drift above tolerance, dead-letter accumulation, and rate-revision restatement.
3. **Chaos experiment.** A broker terminated under load, with the effect on lag, admission latency, and event loss or double-counting recorded.
4. **Blameless postmortem** on a failure encountered during the build, with timeline, impact, root cause, and remediation.

### 7.4 Cost-aware infrastructure

Cloud infrastructure runs ephemerally rather than continuously. The Terraform teardown and rebuild requirement doubles as the cost strategy: the cluster runs long enough to capture measurements, then is destroyed. A stack rebuilt many times is provably reproducible in a way one that is never torn down is not.

- Unit economics published on a dashboard panel: cost per million position reports ingested, cost per thousand invoices generated, cost per generated explanation.
- Kubernetes cost allocation by namespace and label, followed by a rightsizing pass on requests and limits.
- Cost delta reporting on every infrastructure pull request.
- Storage lifecycle tiering across the hot analytical store, the warm lake, and cold archive.
- Commitment and spot capacity modeling for the node pools.

Where infrastructure runs on free tiers, actual spend approaches zero, so consumption units are measured precisely and priced at published list rates. Cost figures are reported as modeled and labeled as such.

---

## 8. Machine Learning and Explanation Layer

### 8.1 Applied machine learning

- **Usage anomaly detection** for abuse and bill-shock prevention. Statistical baselines per operator per meter plus isolation forest for multivariate cases.
- **Charge forecasting** for the projected invoice.
- **Exemption clause extraction.** Rate card exemption conditions arrive as unstructured prose. A compact transformer is fine-tuned on the extraction task, which feeds the rating engine directly.
- **Labeling strategy.** Anomaly labels do not exist naturally, so they are constructed from injected synthetic anomalies plus manually reviewed real cases, and the construction is documented.
- **Class imbalance handled explicitly.** Precision-recall area under curve rather than accuracy, with threshold selection tied to a business cost: a false positive blocks a legitimate quota decision, a false negative sends a wrong invoice.
- **Calibration.** Reliability curve, then Platt scaling or isotonic regression, because an uncalibrated probability cannot support a cost-weighted threshold.
- **Backtesting on time-based splits,** with an explicit note on leakage. No random splits on time-series data.
- **Champion-challenger with shadow deployment,** promoted on a documented criterion behind a feature flag.
- **Registry and drift.** Versioned artifacts, input drift, prediction drift, model cards, and a documented retraining decision rule rather than a schedule.

### 8.2 Explanation and investigation

- **Self-hosted explanation serving.** Explaining large volumes of line items through a hosted frontier interface is uneconomic inside a cost-management product. A quantized open-weight model is served behind an OpenAI-compatible endpoint with continuous batching, and time to first token, tokens per second, and throughput by batch size are measured.
- **Grounded generation.** Explanations grounded strictly in tool output, with per-sentence provenance tagging, refusal when tool output does not support a conclusion, and a hard block on any unsourced figure in text attached to an invoice.
- **Reconciliation agent.** A LangGraph supervisor with routing on drift severity, and a Model Context Protocol tool server exposing read-only allowlisted tools over the analytical store, lake snapshots, the dead-letter topic, and rate cards as of a date. Human approval is required before any restatement is issued.
- **Agent memory and context compaction.** A month of investigation spans thousands of segments and exceeds a context window, so hierarchical summarization is required. Semantic recall over prior findings lets the agent recognize a recurring cause rather than re-investigating it.
- **Evaluation.** Faithfulness, context precision, and answer relevance against a golden set of invoice-change scenarios, wired as a continuous integration regression gate. Judge independence enforced by using a different model family than the system under test, with randomized response ordering to control position bias and validation against a human-labeled subset.
- **Routing and caching.** Semantic cache, self-hosted default path, frontier escalation on low confidence, secondary provider on rate limiting, and a deterministic template fallback with no model at all.
- **Structured output enforcement.** Schema-constrained generation with retry on validation failure, and a measured malformed-output rate before and after.
- **Guardrail and agent evaluation.** Refusal correctness across true and false refusal cases, trajectory correctness, tool-call accuracy, recovery from a failed tool call, steps to resolution, and cost per investigation.

### 8.3 Deliberately excluded

- Reinforcement learning from human feedback, direct preference optimization, policy optimization, and distillation. There is no preference dataset and no reward signal in this domain.
- Computer vision and vision-language models. Positions are numeric, rate cards are text, invoices are tabular.
- Diffusion, multimodal generation, and pretraining.
- A second general-purpose orchestrator alongside the batch orchestrator.

---

## 9. Phase Plan

| Phase | Focus | Deliverable |
|---|---|---|
| 1 | Admission and spatial rating | An event goes in and a charge comes out. Idempotency, transactional outbox, gRPC, spatial resolution, decimal rating, ten property tests, three integration scenarios, five decision records. |
| 2 | Change capture, streaming, spatial join | Debezium change capture, streaming aggregation with watermarks and a correction topic, Iceberg sink, distributed spatial join with prefilter, correctness proven under late and duplicate events. |
| 3 | Platform, analytics, spatial, security, cost, reliability | Terraform footprint, Kubernetes with operator-managed Kafka and lag-driven autoscaling, quota engine, schema registry, leader election, resilience patterns, runtime tuning, ClickHouse, transformation layer, batch close, gazetteer and open spatial normalization, row-level isolation, recovery drill, observability, cost track, reliability artifacts. |
| 4 | Interface, contracts, load | Dashboard with live map, invoice explorer, route cost simulator, and unit-cost panel. Published client and versioned contract. Design documentation. Load testing. |
| 5 | Machine learning, serving, agent | Anomaly detection with labeling strategy, calibration, backtesting, champion-challenger, drift and retraining. Forecasting. Self-hosted serving with measured inference figures. Routing, caching, structured output, guardrail and agent evaluation, judge validation. Fine-tuned extraction model. |

Phase 1 build order, hour budget, and definition of done are in `docs/phase1-build-spec.md`.

---

## 10. Measurement Catalog

Measurements are recorded in `docs/measurements.md` as they are obtained. None can be reconstructed after the fact, so instrumentation begins at the first commit.

| Measurement | Phase |
|---|---|
| Admission latency, 50th and 99th percentile, gRPC and REST | 1 |
| Duplicate rate as a share of total admissions over 24 hours | 1 |
| Position reports per hour, distinct airframes and operators | 1 |
| Outbox drain time after a simulated broker outage | 1 |
| Property and integration test counts | 1 |
| Rate card lookup latency before and after the compound index | 1 |
| Point-in-polygon latency with and without the spatial index | 1 |
| Sustained throughput and watermark lateness bound | 2 |
| Late events corrected rather than dropped | 2 |
| Spatial join time before and after broadcast and prefilter | 2 |
| Candidate polygon reduction from cell indexing | 2 |
| Restatement delta for a month recomputed under a revised rate | 2-3 |
| Consumer lag peak and recovery after a tenfold spike, with and without autoscaling | 3 |
| Rebalance pause before and after cooperative sticky assignment | 3 |
| Admission percentile before and after runtime tuning | 3 |
| Behavior under injected downstream failure, breaker open rate | 3 |
| Reconciliation drift at monthly close, in minor currency units | 3 |
| Chaos experiment: lost or double-counted events | 3 |
| Provisioned resource count and cold-start time to a working environment | 3 |
| Query profile before and after partitioning and clustering | 3 |
| Transformation model, test, and exposure counts, plus build time | 3 |
| Streaming versus batch equality test result | 3 |
| Contract violation caught in CI before reaching a mart | 3 |
| Gazetteer match rate and confidence distribution | 3 |
| Open spatial source reconciliation discrepancy count | 3 |
| Row-level security policy count and cross-tenant access test result | 3 |
| Recovery time and recovery point achieved in the restore drill | 3 |
| Source quality scorecard results per source | 3 |
| Modeled cost per million events ingested, per thousand invoices | 3 |
| Rightsizing savings after cost allocation | 3 |
| Storage cost before and after tiering | 3 |
| Error budget consumption over a measured window | 3 |
| Load test throughput and saturation point | 4 |
| Anomaly model precision, recall, F1, and PR-AUC on a labeled set | 5 |
| Calibration error before and after scaling | 5 |
| Backtest performance across time-based folds | 5 |
| Champion versus challenger delta over the shadow window | 5 |
| Forecast error on projected charge | 5 |
| Extraction model precision and recall against pretrained baseline | 5 |
| Time to first token, tokens per second, throughput by batch size | 5 |
| Modeled cost per explanation, self-hosted against frontier | 5 |
| Semantic cache hit rate and modeled cost avoided | 5 |
| Malformed structured output rate before and after enforcement | 5 |
| Faithfulness and context precision on the golden set | 5 |
| Judge agreement rate against the human-labeled subset | 5 |
| Guardrail refusal correctness, true and false refusal rates | 5 |
| Agent tool-call accuracy, steps to resolution, modeled cost per investigation | 5 |
| Provider fallback rate and distribution across the routing ladder | 5 |

---

## 11. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Upstream credit exhaustion | Token bucket sized from a configured budget, tight bounding box, budget-derived poll interval, recorded responses for offline development, and a local replay corpus for all load testing. |
| Incomplete upstream coverage | Coverage gaps are the source of the late-arrival work rather than a defect. Handling is documented and tested rather than hidden. |
| Approximate weight figures | Stated in the schema and README as a type-level approximation, with a note that a production system would use declared fleet data. |
| Two backend languages invite a design challenge | The split is hot-path admission against analytical query serving, which are different workloads. The measured admission percentile is the supporting evidence. If it cannot be produced, the compiled service is reduced to a thin gateway. |
| Cloud credit exhaustion | Ephemeral infrastructure, budget alerts at two thresholds, a destroy target run at the end of every session, and a spending-limit account that stops rather than bills. |
| Cost figures meaningless because spend is near zero | Consumption units measured precisely and priced at published rates, reported as modeled cost and labeled as such. |
| Component sprawl | One decision record per non-obvious choice, each answering why this and not the simpler thing. Any component whose record cannot be written is cut. |
| Unmeasured claims | Every figure in the documentation maps to a row in the measurement catalog. Instrumentation begins at the first commit. |

---

## Appendix A: Technology Inventory

| Layer | Technologies |
|---|---|
| Languages | Java 21, Python 3.11, TypeScript, SQL, Bash |
| Ingress and services | Spring Boot, gRPC, Protocol Buffers, schema registry, FastAPI, Pydantic, REST, server-sent events, OpenAPI, generated client |
| Streaming | Apache Kafka, Strimzi, Apache Flink, Spark Structured Streaming, Debezium |
| Storage | PostgreSQL, PostGIS, MongoDB, Redis, ClickHouse, Apache Iceberg, Parquet, ADLS Gen2 |
| Spatial | PostGIS geography types, spatial indexing, distributed spatial join, hierarchical cell indexing, GeoJSON, OpenStreetMap, gazetteer and crosswalk, spatial analytics platform |
| Transformation | dbt with contracts, snapshots, incremental strategies, semantic layer, exposures, generated docs |
| Orchestration | Azure Data Factory |
| Cloud and platform | Azure managed Kubernetes, lake storage, event streaming, managed relational and document databases, batch orchestrator, secrets vault, container registry, Terraform, Docker, Kubernetes, Helm |
| Machine learning | MLflow, scikit-learn, isolation forest, isotonic calibration, time-series forecasting, compact transformer with supervised fine-tuning |
| Generative AI | Self-hosted serving engine, model router with provider fallback, LangGraph, LangChain, Model Context Protocol, prompt registry, semantic cache, structured output enforcement, evaluation harness, judge harness with bias mitigation, LLM tracing |
| Observability | Prometheus, Grafana, application performance monitoring, error tracking, OpenTelemetry, structured logging with correlation identifiers |
| Cost engineering | Label-based cost allocation, cost delta gate in continuous integration, storage lifecycle tiering, commitment and spot modeling, token accounting |
| Reliability | Service level objectives, error budgets, burn-rate alerting, runbooks, chaos experiment, blameless postmortem, point-in-time recovery, restore drill |
| Security and governance | Row-level security, role and privilege administration, secrets management, source quality and attribution scorecard, prompt injection testing |
| Testing and quality | pytest, Hypothesis property-based testing, Testcontainers, JUnit, load testing, coverage reporting, code health analysis, linting, static type checking |
| Frontend | Next.js, React, TypeScript, Tailwind CSS, Recharts, spatial tiles |
| Delivery | GitHub Actions with five gates, container registry, feature flags, cloud development environments |

---

## Appendix B: Repository Layout

```
navcharge/
  docker-compose.yml, Makefile, .devcontainer/, CLAUDE.md, README.md
  docs/architecture.md       this document
  docs/phase1-build-spec.md  Phase 1 build order
  docs/adr/                  decision records
  docs/measurements.md       every captured number
  docs/future-work.md        ideas that are out of scope
  docs/slo.md, docs/runbooks/, docs/postmortems/
  docs/cost/                 unit economics and allocation
  docs/design/               ERD, data flow, use cases
  docs/data-sources/         quality and attribution scorecards
  services/admission/        Spring Boot, Java 21, gRPC
  services/rating/           charge engine, spatial segment detection
  services/poller/           credit-budgeted client, replay harness
  services/quota/            Flink keyed state job
  services/aggregate/        Spark Structured Streaming job
  services/api/              FastAPI query and admin, OpenAPI
  services/agent/            LangGraph and MCP investigator
  clients/python/            generated client
  ml/                        anomaly, forecasting, extraction, serving, evals
  web/                       Next.js dashboard
  db/postgres/migrations, db/postgres/seed, db/postgres/rls/
  db/mongo/, db/clickhouse/
  spatial/                   boundaries, gazetteer, OSM normalization
  dbt/                       models, tests, snapshots, semantic layer, exposures
  infra/terraform/           modules and environments
  infra/k8s/                 manifests and Helm charts
  ops/prometheus/, ops/grafana/, ops/cost/
  .github/workflows/
```

---

## Appendix C: Decision Records

Written as the corresponding component is built, into `docs/adr/`. If a record cannot be written, the component is cut.

1. Monorepo across three languages.
2. Document store for the rate card catalog, with the relational alternatives considered.
3. Airframe address as the partition key rather than operator code.
4. Decimal arithmetic and integer minor units for all monetary values.
5. Idempotency strategy: derived keys, bounded window, behavior on cache failure.
6. Transactional outbox instead of broker transactions, and the at-least-once argument.
7. Leader election mechanism for the outbox publisher.
8. Schema registry and the compatibility mode selected.
9. Two stream processing engines, with the latency and throughput split.
10. Table format selection driven by the restatement requirement.
11. Hot analytical store selection and the partitioning strategy.
12. Incremental materialization strategy for charge facts, and why not insert-overwrite.
13. Change data capture against warehouse snapshots as two ways to capture change.
14. Contract enforcement scope: which models carry contracts and why not all.
15. Batch orchestrator selection rather than a second general-purpose orchestrator.
16. Autoscaling on consumer lag rather than processor utilization.
17. Resilience posture: circuit breaker thresholds, bulkhead boundaries, retry budgets.
18. Geography type rather than planar geometry, and the spatial reference identifier chosen.
19. Spatial join strategy: broadcast with bounding-box prefilter rather than per-row lookup.
20. Gazetteer matching and survivorship rules, with the confidence scoring method.
21. Row-level security as the isolation mechanism rather than application-layer filtering.
22. Recovery objectives and the backup strategy that meets them.
23. Self-hosted inference serving, with the cost model that justifies it.
24. Model selection: parameter count, attention variant, context length, quantization, and why deterministic sampling is required for text attached to an invoice.
25. Routing and escalation policy across providers, and the fallback ladder.
26. Judge model independence and the bias mitigation applied.
27. Semantic cache similarity threshold and staleness policy.
28. Anomaly threshold selection on a cost-weighted basis rather than a default.
29. Supervised fine-tuning limited to the extraction task, and why preference tuning is excluded.
30. Cost allocation model, the rightsizing methodology, and why cost is reported as modeled.
31. Ephemeral infrastructure as both a cost strategy and a reproducibility guarantee.
32. Scope boundary: what this platform does not claim about the accuracy of its output.
