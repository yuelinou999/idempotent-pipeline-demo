## This repository contains the reference implementation accompanying the three-part article series on enterprise integration pipelines:
##Pillar 1 — Correctness  
##Pillar 2 — Resilience  
##Pillar 3 — Throughput
## This release focuses on Pillar 1 — Correctness: idempotent write paths, deduplication, staging, correlation, and reconciliation.

## Idempotent Pipeline

**A reference implementation of an idempotent write path for distributed enterprise integration.**

This project demonstrates the core mechanisms required to build a production-grade data integration pipeline that is resilient to duplicate delivery, out-of-order message arrival, and message replay — without relying on exactly-once delivery guarantees from the underlying message broker.

---

## The Problem This Solves

In multi-system enterprise integration, three failure modes are the default, not the exception:

- **Duplicate delivery**: System A retries a failed POST; System B already processed it.
- **Message replay**: A consumer group restarts and the broker re-delivers yesterday's batch.
- **Out-of-order arrival**: A shipment event arrives before the order event that caused it.

A naive pipeline that writes every incoming message will produce duplicate records, data corruption, or both. This project implements the architectural patterns required to handle all three failure modes correctly and — critically — **make that correctness verifiable through audit logs**.

---

## Core Concepts

### Idempotent Key vs. Correlation ID

These two concepts are often conflated. They serve entirely different purposes.

| Concept | Question it answers | Scope |
|---|---|---|
| **Idempotent Key** | "Has this pipeline already processed this version of this entity from this source system?" | Single source, single entity, single version |
| **Correlation ID** | "Which business transaction do these records from different systems belong to?" | Cross-system, single business event |

**Idempotent key format:**
```
{source_system}:::{entity_type}:::{business_key}:::{version}
```

Example:
```
oms:::order:::ORD-20260301-0042:::3
wms:::picking_ticket:::WH-PKT-88431:::1
finance:::invoice:::INV-2026-03-A0017:::1
```

These are intentionally **different** keys — they represent different entities from different systems. The Correlation ID links them to the same business transaction.

### Five-Branch Deduplication Decision

Every incoming message is evaluated against this decision table before any write occurs:

| Condition | Action |
|---|---|
| Key not seen before | ✅ `accept` — write to staging |
| Same key, newer version | ✅ `accept` — supersede previous entry |
| Same key, same version, same payload hash | ⏭️ `skip` — exact duplicate |
| Same key, same version, **different** payload hash | 🔴 `quarantine` — anomaly, route to review |
| Same key, older version | ⏭️ `skip` — stale message |

The quarantine branch is critical. "Same key + same version + different payload" means the upstream system changed the data without incrementing the version. Silently skipping this buries a data quality issue. Silently overwriting introduces uncontrolled mutations. The correct response is to surface it explicitly.

---

## Architecture

```
┌─────────────┐
│  Source:OMS │──┐
└─────────────┘  │    ┌─────────────────────────────────┐
┌─────────────┐  │    │         Ingestion Layer          │
│  Source:WMS │──┼───▶│  1. Build idempotent key         │
└─────────────┘  │    │  2. Hash payload                 │
┌─────────────┐  │    │  3. Look up existing state       │
│Source:Financ│──┤    │  4. Five-branch decision engine  │
└─────────────┘  │    │  5. Write / skip / quarantine    │
┌─────────────┐  │    │  6. Resolve Correlation ID       │
│  Source:CRM │──┘    │  7. Completeness check           │
└─────────────┘       └────────────┬────────────────────┘
                                   │
              ┌────────────────────┤
              │                    │
              ▼                    ▼
   ┌──────────────────┐  ┌─────────────────────┐
   │   Dedup Store    │  │   Staging Layer      │
   │  idempotent_log  │  │  (not yet trusted)   │
   └──────────────────┘  │  Schema validation   │
                         │  Completeness check  │
                         └────────────┬─────────┘
                                      │
                          ┌───────────┴──────────┐
                          │                      │
                          ▼                      ▼
               ┌──────────────────┐   ┌──────────────────┐
               │Production Layer  │   │  Review Queue    │
               │Clean·Deduped     │   │  (quarantined)   │
               │Validated·Latest  │   └──────────────────┘
               └──────────────────┘
```

### Three-Layer Pipeline

**Ingestion Layer** — Receives all messages. Applies the deduplication decision. Logs every outcome. Nothing reaches staging without passing through here.

**Staging Layer** — Buffer zone where data is not yet fully trusted. Completeness checks run here (PARTIAL → COMPLETE state machine). Records that pass are promoted to production. Anomalies go to the review queue.

**Production Layer** — Contains only clean, deduplicated, validated data. Populated by the materialization job, never by the ingestion path directly.

---

## Project Structure

```
idempotent-pipeline/
├── app/
│   ├── api/
│   │   └── routes/
│   │       ├── messages.py      # POST /messages/ingest, /ingest-batch
│   │       ├── queries.py       # GET /state, /staging, /correlations, /review-queue
│   │       └── jobs.py          # POST /jobs/materialize, /jobs/compact
│   │                            # GET /reports/reconciliation
│   ├── core/
│   │   ├── config.py            # Settings (database URL, etc.)
│   │   ├── database.py          # Async SQLAlchemy engine + session
│   │   └── enums.py             # Shared status enums
│   ├── models/
│   │   ├── idempotent_state.py  # Current state per entity_key
│   │   ├── ingestion_log.py     # Append-only audit trail
│   │   ├── staging.py           # Staging records
│   │   └── correlation.py       # CorrelationMap, ManualReviewQueue, ProductionProjection
│   ├── services/
│   │   ├── key_builder.py           # Deterministic idempotent key derivation
│   │   ├── payload_hasher.py        # Canonical payload fingerprinting
│   │   ├── decision_engine.py       # Five-branch dedup logic (pure function)
│   │   ├── correlation_engine.py    # Correlation ID resolution
│   │   ├── completeness_engine.py   # PARTIAL → COMPLETE state machine
│   │   ├── ingest_service.py        # Full ingestion orchestration
│   │   ├── reconciliation_service.py
│   │   ├── materialization_service.py
│   │   └── compaction_service.py
│   └── main.py                  # FastAPI application
├── tests/
│   ├── conftest.py              # In-memory SQLite fixtures
│   ├── fixtures/
│   │   └── scenarios.json       # 10 documented test scenarios
│   ├── test_decision_engine.py  # Pure unit tests (no DB)
│   ├── test_key_builder.py      # Key derivation unit tests
│   ├── test_ingest_api.py       # Full stack integration tests
│   └── test_correlation.py      # Correlation mapping tests
├── pyproject.toml
└── README.md
```

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/messages/ingest` | Ingest a single message |
| `POST` | `/messages/ingest-batch` | Ingest a batch (up to 500 messages) |
| `GET` | `/state/{idempotent_key}` | Current state for a key |
| `GET` | `/attempts/{attempt_id}` | Single ingestion attempt log entry |
| `GET` | `/correlations/{correlation_id}` | All source mappings for a transaction |
| `GET` | `/staging` | List staging records |
| `GET` | `/review-queue` | List quarantined anomalies |
| `GET` | `/reports/reconciliation` | Audit consistency report |
| `POST` | `/jobs/materialize` | Promote complete records to production |
| `POST` | `/jobs/compact` | Archive stale staging records |

---

## Quickstart

```bash
# 1. Install dependencies
pip install -r requirements-dev.txt
# 2. Start the API server
uvicorn app.main:app --reload

# 3. Open the interactive docs
http://localhost:8000/docs

# 4. Run the test suite
pytest tests/ -v
```

---

## Running the Scenarios

The `tests/fixtures/scenarios.json` file contains 10 documented test scenarios:

| # | Scenario | Expected outcome |
|---|---|---|
| 01 | First seen message | `accepted` |
| 02 | Exact duplicate | `skipped` |
| 03 | Same version, different payload | `quarantined` |
| 04 | Newer version supersedes old | `accepted` |
| 05 | Stale older version | `skipped` |
| 06 | Explicit correlation (WMS → OMS) | `accepted`, same correlation ID |
| 07 | Rule-based correlation | `accepted` |
| 08 | Out-of-order: V3 then V2 | V3 `accepted`, V2 `skipped` |
| 09 | Partial then complete | PARTIAL → COMPLETE on second message |
| 10 | Replay batch | New messages `accepted`, replays `skipped` |

---

## Reconciliation

The reconciliation report answers the auditor's question: *"How do you prove no data was lost and no data was written twice?"*

```bash
curl http://localhost:8000/reports/reconciliation?hours=24
```

The consistency invariant:
```
accepted + superseded == total unique business events processed
skipped              == total duplicate deliveries intercepted
```

---

## Design Decisions

**Why not exactly-once delivery?** This pipeline achieves exactly-once *processing*, not exactly-once *delivery*. Brokers deliver messages at least once. The dedup store ensures processing happens at most once. These are distinct guarantees.

**Why never DELETE?** Deleting staging records destroys data lineage. Every superseded or promoted record is archived (status change only), never removed. The compaction job migrates stale records to cold storage without deleting them.

**Why quarantine instead of skip or overwrite?** "Same key + same version + different payload" is a data quality anomaly. Skipping it silently buries the problem. Overwriting without a version increment introduces uncontrolled mutations. Quarantining surfaces it explicitly for human review.

**Why SQLite default with Postgres migration path?** SQLite enables zero-dependency local development and testing. The async SQLAlchemy setup is identical for Postgres — change one environment variable.

---

## Migrating to PostgreSQL

```bash
# .env
DATABASE_URL=postgresql+asyncpg://user:password@localhost/pipeline_db
```

No code changes required. The schema is Postgres-compatible.

---

## Related Writing

This project is the reference implementation for a series of articles on distributed enterprise integration:

- *Idempotent Write Paths in Distributed Enterprise Integration* — InfoQ (forthcoming)

---

## License

MIT
