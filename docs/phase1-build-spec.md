# Phase 1 Build Spec: Air Navigation Charge Metering Platform

**Scope:** Weeks 1 and 2. Roughly 30 hours at 15 hours per week.
**Deliverable:** A position report goes in through the admission service and a charge comes out, computed with money-safe arithmetic, proven correct under duplicate and out-of-order input.
**Everything is local.** Docker Compose only.

---

## 0. What Phase 1 is not

Explicitly out of scope. Do not start any of these, even if a tutorial makes it look cheap.

- No Spark, no Flink, no streaming aggregation
- No Iceberg, no ClickHouse, no data lake
- No Azure, no Kubernetes, no Terraform
- No LLM layer, no agent, no MCP
- No frontend beyond `curl` and a Postman collection
- No CI beyond a single GitHub Actions job that runs the test suite

The whole point of Phase 1 is that the rating function is correct and the ingress is idempotent. Everything downstream is built on the assumption that a charge is deterministic and never double-counted. If that assumption is wrong, every later phase inherits the bug.

---

## 1. Repository layout

Monorepo. One `docker-compose.yml` at the root that brings up the entire stack.

```
navcharge/
  docker-compose.yml
  Makefile                        # up, down, seed, test, lint
  .env.example
  README.md
  docs/
    adr/
      0001-monorepo.md
      0002-mongo-for-rate-cards.md
      0003-partition-key-icao24.md
      0004-decimal-money-arithmetic.md
      0005-idempotency-strategy.md
  services/
    admission/                    # Spring Boot, Java 21
      src/main/java/io/navcharge/admission/
        api/                      # gRPC + REST controllers
        domain/                   # PositionReport, IdempotencyKey
        dedupe/                   # Redis-backed dedupe window
        outbox/                   # transactional outbox writer + publisher
        config/
      src/test/java/              # Testcontainers integration tests
      build.gradle.kts
    rating/                       # Python 3.11, the charge engine
      navcharge_rating/
        charge.py                 # the formulas, pure functions
        segments.py               # FIR entry/exit + great-circle distance
        ratecards.py              # Mongo rate card lookup, as-of date
        consumer.py               # Kafka consumer, naive synchronous path
      tests/
        test_charge_properties.py # Hypothesis
        test_segments.py
        test_ratecards.py
    poller/                       # Python, the simulated tenant SDK
      navcharge_poller/
        opensky.py                # OAuth2 client + credit budget
        publisher.py              # calls the admission service
  db/
    postgres/
      migrations/                 # Flyway or plain numbered SQL
      seed/
        fir_boundaries.geojson
        aircraft_types.csv
    mongo/
      seed_ratecards.py
  ops/
    grafana/                      # dashboards, added in Phase 3
    prometheus/
```

**Why a monorepo:** three languages, one deploy story, one compose file, and the whole thing must be clonable and runnable by a stranger in one command. Write ADR 0001 and move on.

---

## 2. Compose stack

| Service | Image | Purpose |
|---|---|---|
| postgres | `postgis/postgis:16-3.4` | Fleet, FIR geometry, flight segments, charges, outbox |
| mongo | `mongo:7` | Rate cards |
| redis | `redis:7-alpine` | Idempotency dedupe window |
| kafka | `bitnami/kafka:3.7` KRaft mode | Event backbone. No ZooKeeper. |
| kafka-ui | `provectuslabs/kafka-ui` | Inspect topics while developing |
| admission | local build | Spring Boot service |
| rating | local build | Python consumer |

Health checks and `depends_on` conditions on every service. `make up` must produce a working stack from a cold machine, and `make seed` must load FIRs, aircraft types, and rate cards.

---

## 3. Data model

### 3.1 Postgres with PostGIS

```sql
-- Operators are the tenants
CREATE TABLE operators (
  icao_operator_code TEXT PRIMARY KEY,   -- e.g. RYR, DLH, THY
  name               TEXT NOT NULL,
  billing_state      TEXT,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Type-level MTOW. Approximate on purpose, documented as such.
CREATE TABLE aircraft_types (
  type_code      TEXT PRIMARY KEY,       -- A320, B738, E190
  description    TEXT,
  mtow_tonnes    NUMERIC(8,1) NOT NULL,
  source         TEXT NOT NULL,          -- where the figure came from
  is_approximate BOOLEAN NOT NULL DEFAULT true
);

-- The fleet. This is the table Debezium will watch in Phase 2.
CREATE TABLE aircraft (
  icao24             TEXT PRIMARY KEY,   -- 24-bit ICAO address, lowercase hex
  registration       TEXT,
  type_code          TEXT REFERENCES aircraft_types(type_code),
  icao_operator_code TEXT REFERENCES operators(icao_operator_code),
  updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Charging zones and their geometry
CREATE TABLE fir_boundaries (
  fir_code      TEXT PRIMARY KEY,        -- EDUU, LFFF, EGTT
  fir_name      TEXT NOT NULL,
  charging_zone TEXT NOT NULL,           -- maps to a rate card
  geom          GEOGRAPHY(MULTIPOLYGON, 4326) NOT NULL
);
CREATE INDEX idx_fir_geom ON fir_boundaries USING GIST (geom);

-- Derived: a flight's traversal of one charging zone
CREATE TABLE flight_segments (
  segment_id     UUID PRIMARY KEY,
  flight_key     TEXT NOT NULL,          -- icao24 + callsign + first_seen day
  icao24         TEXT NOT NULL,
  fir_code       TEXT NOT NULL REFERENCES fir_boundaries(fir_code),
  entry_ts       TIMESTAMPTZ NOT NULL,
  exit_ts        TIMESTAMPTZ,
  entry_point    GEOGRAPHY(POINT, 4326) NOT NULL,
  exit_point     GEOGRAPHY(POINT, 4326),
  distance_km    NUMERIC(10,3),
  is_closed      BOOLEAN NOT NULL DEFAULT false,
  UNIQUE (flight_key, fir_code, entry_ts)
);

-- The money table. Every row is reproducible from its inputs.
CREATE TABLE charges (
  charge_id         UUID PRIMARY KEY,
  segment_id        UUID NOT NULL REFERENCES flight_segments(segment_id),
  icao_operator_code TEXT NOT NULL,
  charge_type       TEXT NOT NULL,        -- EN_ROUTE | TERMINAL
  service_units     NUMERIC(12,4) NOT NULL,
  unit_rate         NUMERIC(12,4) NOT NULL,
  currency          CHAR(3) NOT NULL DEFAULT 'EUR',
  amount_minor      BIGINT NOT NULL,      -- cents. Never a float.
  rate_card_id      TEXT NOT NULL,
  rate_card_version INT NOT NULL,
  mtow_tonnes       NUMERIC(8,1) NOT NULL,
  computed_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (segment_id, charge_type, rate_card_version)
);

-- Transactional outbox
CREATE TABLE outbox (
  id           BIGSERIAL PRIMARY KEY,
  aggregate_id TEXT NOT NULL,
  topic        TEXT NOT NULL,
  payload      JSONB NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON outbox (id) WHERE published_at IS NULL;
```

Two things to notice and to defend later. `amount_minor` is an integer count of cents, so money never touches a float. The `UNIQUE (segment_id, charge_type, rate_card_version)` constraint is what makes re-rating under a revised rate card an insert rather than an overwrite, which is what makes restatement auditable in Phase 3.

**FIR seed scope.** Load six to eight European FIRs, not all 41 states. Pick a contiguous set that your OpenSky bounding box actually covers, so most flights cross at least two zones. Full coverage is a Phase 3 concern.

### 3.2 MongoDB rate cards

One document per charging zone per effective period.

```json
{
  "_id": "DE-ENROUTE-2026",
  "schema_version": 2,
  "charging_zone": "DE",
  "state": "Germany",
  "currency": "EUR",
  "effective_from": "2026-01-01",
  "effective_to": null,
  "version": 1,
  "en_route": {
    "unit_rate": 97.79,
    "mtow_exponent": 0.5,
    "mtow_divisor": 50,
    "distance_divisor_km": 100
  },
  "terminal": {
    "unit_rate": 365.18,
    "mtow_exponent": 0.70,
    "mtow_divisor": 50
  },
  "exemptions": [
    { "kind": "MTOW_BELOW_TONNES", "value": 2.0 },
    { "kind": "FLIGHT_RULE", "value": "VFR" },
    { "kind": "MISSION_TYPE", "value": "SAR" }
  ]
}
```

This document shape is your ADR 0002. The argument is that the exponents, divisors, exemption kinds, and optional terminal block differ by state, and that a relational schema would either need a wide sparse table or a rate-attribute EAV table. Both are worse. Write the alternative down in the ADR so the choice reads as reasoned rather than defaulted.

Add a compound index on `charging_zone` plus `effective_from`, then capture `explain()` output before and after. The plan output is the evidence for the index choice, and it belongs in the decision record.

---

## 4. Kafka topic design

| Topic | Key | Partitions | Retention | Contents |
|---|---|---|---|---|
| `usage.positions.v1` | `icao24` | 12 | 7 days | Deduplicated position reports admitted by the gateway |
| `billing.charges.v1` | `icao_operator_code` | 6 | 30 days | Computed charge events |
| `usage.positions.dlq` | `icao24` | 3 | 30 days | Rejected or unparseable admissions |

**ADR 0003, the partition key.** Key on `icao24`, not on operator code. Segment detection needs strict ordering of position reports per airframe, and Kafka only guarantees ordering within a partition. Operator-level aggregation happens downstream, where ordering does not matter. Keying on operator would serialize hundreds of aircraft onto one partition for a large carrier and destroy throughput.

Note the skew honestly: with `icao24` as the key, hot partitions come from geographic density inside your bounding box rather than from tenant size. Measure the per-partition rate and write down what you saw. Do not claim you solved a hot-tenant problem you did not have.

---

## 5. Week 1: admission service first

Build the gateway before the data source. It gives the poller a target and it forces the idempotency design up front.

### 5.1 The admission contract

gRPC as the primary interface, REST as a convenience wrapper over the same handler.

```proto
service Admission {
  rpc ReportPosition (PositionReportRequest) returns (AdmissionResponse);
  rpc ReportPositionBatch (stream PositionReportRequest) returns (AdmissionSummary);
}

message PositionReportRequest {
  string idempotency_key = 1;   // sha256(icao24 + observed_at_epoch_sec)
  string icao24          = 2;
  string callsign        = 3;
  int64  observed_at     = 4;   // epoch seconds, 5s resolution from OpenSky
  double latitude        = 5;
  double longitude       = 6;
  double baro_altitude_m = 7;
  bool   on_ground       = 8;
  double velocity_ms     = 9;
}

message AdmissionResponse {
  enum Status { ACCEPTED = 0; DUPLICATE = 1; REJECTED = 2; }
  Status status  = 1;
  string reason  = 2;
}
```

The idempotency key is derived, not client-supplied. That matters: OpenSky returns state vectors snapped to a 5-second boundary, so two overlapping polls genuinely return the same observation. Deriving the key from `icao24` plus `observed_at` means the duplicates you deduplicate are real duplicates from a real source, which is the whole reason this data source was chosen.

### 5.2 Dedupe

Redis `SET key 1 NX EX 900`. A fifteen-minute window covers poll overlap and client retry without unbounded memory. `NX` returning nil means duplicate, so the check and the claim are one atomic operation and there is no race between two concurrent admissions of the same observation.

Write ADR 0005 covering: why Redis and not a Postgres unique constraint (latency on the hot path), why a bounded window and not forever (memory, and the lake is the system of record from Phase 2), and what happens on Redis failure. Fail closed or fail open is a real decision. Pick one, justify it, and say plainly what the consequence is.

### 5.3 Transactional outbox

The failure this prevents: the service returns HTTP 200 or a gRPC OK, then the Kafka publish fails, and the client never retries because it was told the write succeeded. The event is lost silently, and in a billing system a lost usage event is lost revenue.

So in one Postgres transaction, insert into `outbox` and commit. A separate publisher polls unpublished rows, produces to Kafka, and stamps `published_at`. At-least-once by construction, and the downstream dedupe handles the repeats.

Write down why you did not chase Kafka transactions and true exactly-once: it couples the admission path to broker availability, and at-least-once plus idempotent consumers is simpler to reason about and to test. The written argument belongs in ADR 0006.

### 5.4 Instrumentation, from the first commit

Micrometer with a Prometheus endpoint. Four metrics, all of which feed the measurement catalog:

- `admission_latency_seconds` histogram, so p50 and p99 exist
- `admission_total` counter tagged by status, which gives you the dedupe rate as a ratio
- `outbox_lag` gauge, the count of unpublished rows
- `outbox_publish_latency_seconds` histogram

Capture these as they occur. None of these figures can be reconstructed later.

---

## 6. Week 2: real data and the rating engine

### 6.1 OpenSky poller

Authentication is OAuth2 client credentials as of March 2026. Basic auth is gone, so ignore any tutorial older than that. Register an account, create an API client, download the credentials file.

The budget is the design constraint. Anonymous access gets roughly 100 credits per day. An authenticated account gets around 4,000, and feeding data back to the network can raise that to 8,000. Credit cost on the all-states endpoint scales with the bounding-box area in square degrees, so a tight box is cheap and the globe is not. Exhaustion returns 429 with a retry-after header.

Implementation:

- Token bucket sized from a configured daily credit budget, so the poller cannot exhaust the account
- A single bounding box covering your seeded FIRs, roughly 10 by 10 degrees
- Poll interval derived from the budget rather than hardcoded, so the same code works at 4,000 or 8,000 credits
- Read `X-Rate-Limit-Remaining` on every response and export it as a gauge
- Exponential backoff on 429 and 5xx, honoring retry-after
- Every raw response written to local disk as gzipped JSON with a timestamp, which becomes your replay corpus

The response corpus is not optional. After a week of polling it becomes a historical dataset replayable at accelerated rate, which is how throughput and lag-recovery figures are obtained in Phase 3 without exceeding the upstream credit budget.

### 6.2 Segment detection

Naive and synchronous in Phase 1. Correctness first, performance in Phase 2.

1. Consume `usage.positions.v1`, grouped by `icao24`, ordered by `observed_at`
2. For each position, point-in-polygon against `fir_boundaries` using PostGIS `ST_Intersects` on the GiST index
3. When the FIR changes between consecutive positions, close the previous segment and open a new one
4. Interpolate the crossing point between the two positions rather than using either endpoint, because charge distance is measured between entry and exit points of the zone
5. Distance is the great-circle distance between entry and exit, via `ST_Distance` on the geography type

The published methodology measures great-circle distance between zone entry and exit rather than actual track length. Follow that, and note in the README that it is a simplification of the real system, which bills on Network Manager flight plan records rather than on observed positions.

Two honest gaps to document rather than hide: coverage holes in crowdsourced ADS-B mean some segments will have missing interior positions, and a flight can leave and re-enter the same FIR. Decide how you treat each, write it down, and add a test.

### 6.3 The charge function

Pure functions, no I/O, in `charge.py`. This is the most important code in the project.

```
en-route service units = (distance_km / 100) * sqrt(mtow_tonnes / 50)
en-route charge        = service_units * unit_rate

terminal service units = (mtow_tonnes / 50) ** 0.70
terminal charge        = service_units * terminal_unit_rate
```

Rules, all non-negotiable:

- `decimal.Decimal` throughout, never float. `ROUND_HALF_UP` quantization to cents at the final step only, never at intermediates.
- The function takes an explicit `as_of` date and resolves the rate card for that date. No implicit "now."
- Same inputs must always produce the same output. No clock reads, no random, no ambient state.
- Return the rate card id and version alongside the amount, so the `charges` row records exactly which card produced it.

### 6.4 Property tests, which are the actual deliverable

Hypothesis. These are the tests that prove the assumption every later phase depends on.

| Property | Assertion |
|---|---|
| Zero distance | Distance of zero yields a charge of zero |
| Monotonic in distance | More distance never costs less |
| Monotonic in MTOW | Heavier never costs less |
| Linear in distance | Doubling distance exactly doubles en-route service units |
| Square root in MTOW | Quadrupling MTOW exactly doubles en-route service units |
| Determinism | Rating the same segment twice returns byte-identical output |
| No float drift | Summing 10,000 charges in Decimal equals the Decimal sum of the same inputs |
| Rate card boundary | A segment on `effective_to` uses the old card, on `effective_from` the new one |
| Exemption below threshold | MTOW under the exemption value yields zero |
| Non-negative | No input combination in the valid domain produces a negative charge |

The boundary test is the one that catches real bugs, because off-by-one on rate card effective dates is exactly the class of error that produces a restatement.

### 6.5 Integration tests

Testcontainers, from the Java side for admission and pytest fixtures for rating. Containers for Kafka, Postgres with PostGIS, Mongo, and Redis. Three scenarios that must pass:

1. **No double billing.** Submit the same position report five times. Exactly one `usage.positions.v1` record, exactly one charge.
2. **Out of order.** Submit positions for one aircraft in shuffled order. Segments come out identical to the sorted case.
3. **Outbox survives a broker outage.** Stop Kafka, submit reports, confirm 200 responses and rows accumulating in `outbox`, restart Kafka, confirm every event is published and none is lost.

Record the peak outbox lag during the outage and the drain time on recovery.

---

## 7. Definition of done

Phase 1 is complete when all of these are true.

- [ ] `make up` brings the full stack from cold on a clean machine
- [ ] `make seed` loads FIR geometry, aircraft types, and at least four rate cards with two versions of one of them
- [ ] The poller runs continuously for 24 hours without exhausting the credit budget
- [ ] A charge row exists for a real flight, and you can trace it back to the exact positions, FIR geometry, MTOW figure, and rate card version that produced it
- [ ] All ten property tests pass, plus the three integration scenarios
- [ ] `make test` is green in GitHub Actions
- [ ] Five ADRs written
- [ ] Prometheus endpoint exposes the four admission metrics and the OpenSky credit gauge
- [ ] README states plainly that MTOW is a type-level approximation and that this estimates charges independently rather than reproducing official invoices

---

## 8. Numbers to record before Phase 2 starts

Write these into `docs/measurements.md` as they are obtained. None of them can be reconstructed after the fact.

- p50 and p99 admission latency, gRPC and REST, under a local load run
- Duplicate rate as a percentage of total admissions over 24 hours of real polling
- Position reports admitted per hour, and distinct aircraft and operators observed
- Segments closed and charges computed in 24 hours
- Outbox drain time after the simulated broker outage
- Property test count and integration test count
- Mongo rate card lookup latency, before and after the compound index, from `explain()`
- PostGIS point-in-polygon latency per position, with and without the GiST index

That last pair is cheap to capture and it is the baseline for the query-planning work in Phase 3.

---

## 9. Hour budget

**Week 1, 15 hours.** Repo and compose 3. Postgres schema and migrations 3. FIR and aircraft type seed 2. Mongo rate cards and index 2. Spring Boot admission with gRPC, REST, Redis dedupe, outbox 5.

**Week 2, 15 hours.** OpenSky OAuth2 poller with credit budget 4. Segment detection with PostGIS 4. Charge function with Decimal 2. Property tests 3. Testcontainers integration scenarios 2.

Overruns are normal. If you are behind at the end of week 2, cut the gRPC streaming endpoint and the terminal charge calculation. Do not cut the property tests or the outbox.

---

## 10. What Phase 2 needs from Phase 1

Named here so you build toward it rather than refactoring into it.

- `aircraft` is the table Debezium will capture, so keep `updated_at` maintained on every write
- Segment detection logic must be a pure function over an ordered position list, so Spark Structured Streaming can call the same code
- The charge function must already accept `as_of`, so Iceberg time travel plugs in without a signature change
- Raw gzipped OpenSky responses on disk become the Iceberg backfill source and the replay corpus
