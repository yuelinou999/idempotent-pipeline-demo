# Integration Pipeline Console

A small operator-facing operational console backend for an idempotent enterprise integration pipeline.

Built as a reference implementation accompanying a three-part article series on enterprise integration reliability. Demonstrates **correctness**, **delivery resilience**, **replay**, and **operational audit** in a single cohesive API.

> **Not a production platform.** This is a demo-grade implementation designed to illustrate the architectural patterns in a runnable, inspectable form.

---

## What This Is

An async FastAPI + SQLAlchemy backend that functions as a mini operations console. An operator can:

- Ingest messages through an idempotent write path and see exactly what decision was made and why
- Simulate downstream delivery with circuit breaking, retry classification, and DLQ routing
- Investigate ambiguous timeout outcomes via query-before-retry before deciding whether to retry
- Create and execute controlled replay requests from eligible queue items — with a full audit trail
- Inspect any record's complete end-to-end history in a single API call
- View a system-health overview showing what needs attention right now

---

## Architecture

Three pillars, each building on the last:

```
┌─────────────────────────────────────────────────────┐
│  Pillar 1 — Correctness                              │
│  Idempotent write path · Five-branch dedup           │
│  Staging → Production · Correlation · Audit log      │
└────────────────────────┬────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────┐
│  Pillar 2 — Delivery Resilience                      │
│  Retry classification · Circuit breaker              │
│  DLQ management · Ambiguous outcome queue            │
│  Query-before-retry · Replay with audit trail        │
└────────────────────────┬────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────┐
│  Pillar 3 — Explainability                           │
│  Entity/message inspector · Ordered event timeline   │
│  Dashboard overview · Recent activity feed           │
└─────────────────────────────────────────────────────┘
```

### Bounded Contexts

**Context 1 — Ingestion Correctness** (shared write-path core)
- Every message passes through a five-branch idempotency decision before any write
- `decision_applicator.py` applies all DB side effects with no commit; `ingest_service.py` owns the single commit
- Replay and all other flows may never bypass this context

**Context 2 — Delivery Resilience** (sibling of Context 1)
- Delivery simulation with mock-configurable downstream behavior
- Per-system circuit breaker (DB-persisted state, FK to downstream system)
- Timeout outcomes route to `AmbiguousOutcomeQueue`; query-before-retry resolves them
- Operator-initiated replay calls `simulate_delivery()` — subject to the same circuit and logging as any original delivery

**Context 3 — Explainability** (read-only query assembly)
- Inspector assembles cross-context views from all 9+ tables in a single call
- Dashboard aggregates system health across all queues and circuits

---

## Data Model

| Table | Purpose |
|---|---|
| `idempotent_state` | One row per entity (version-less key) — fast-path dedup lookup |
| `ingestion_attempt_log` | Append-only audit trail of every ingestion decision |
| `staging_record` | Accepted records buffered before promotion |
| `production_projection` | Clean, deduplicated, validated records |
| `correlation_map` | Cross-system business transaction grouping |
| `data_anomaly_queue` | Quarantined records requiring human review |
| `downstream_system` | Delivery target config + mock behavior |
| `delivery_attempt_log` | Append-only log of every delivery attempt |
| `delivery_dlq` | Terminal delivery failures awaiting operator action |
| `circuit_breaker_state` | Per-system circuit state (FK to downstream_system) |
| `ambiguous_outcome_queue` | Timeout outcomes with unknown delivery status |
| `replay_request` | Operator-initiated replay job |
| `replay_request_item` | Per-record item within a replay request |
| `replay_execution_log` | Audit record proving replay used the canonical path |

---

## API Surface

### Dashboard
| Method | Path | Description |
|---|---|---|
| `GET` | `/dashboard/overview` | System health snapshot — start here |

### Ingestion
| Method | Path | Description |
|---|---|---|
| `POST` | `/ingest` | Ingest one message through the idempotent write path |
| `POST` | `/ingest-batch` | Ingest a batch (up to 500 messages) |

### State Queries
| Method | Path | Description |
|---|---|---|
| `GET` | `/state/{entity_key}` | Current idempotent state for an entity |
| `GET` | `/attempts/{attempt_id}` | Single ingestion attempt log entry |
| `GET` | `/correlations/{correlation_id}` | Source mappings for a business transaction |
| `GET` | `/staging` | List staging records |
| `GET` | `/dlq/data-anomaly` | List data anomaly (quarantine) entries |

### Delivery
| Method | Path | Description |
|---|---|---|
| `POST` | `/delivery/simulate` | Simulate delivery to a downstream system |
| `GET` | `/dlq/delivery` | List pending delivery DLQ items |
| `POST` | `/dlq/delivery/{id}/resolve` | Mark a DLQ entry as resolved |
| `GET` | `/reports/delivery-outcomes` | Delivery health report |

### Circuit Breaker
| Method | Path | Description |
|---|---|---|
| `GET` | `/circuits` | All circuit breaker states |
| `GET` | `/circuits/{system}` | One system's circuit state |
| `POST` | `/circuits/{system}/reset` | Operator override (closed / half_open / open) |

### Ambiguous Outcomes
| Method | Path | Description |
|---|---|---|
| `GET` | `/dlq/ambiguous` | List ambiguous outcome entries |
| `GET` | `/dlq/ambiguous/{id}` | Detail for one ambiguous entry |
| `POST` | `/dlq/ambiguous/{id}/query-status` | Run query-before-retry |

### Replay
| Method | Path | Description |
|---|---|---|
| `GET` | `/replays` | List replay requests |
| `GET` | `/replays/{id}` | Full detail with items and execution log |
| `POST` | `/replays` | Create replay from eligible DLQ or ambiguous items |
| `POST` | `/replays/{id}/execute` | Execute a pending replay request |

### Inspector
| Method | Path | Description |
|---|---|---|
| `GET` | `/inspect/by-idempotent-key/{key}` | Full record history by versioned key |
| `GET` | `/inspect/by-entity-key/{key}` | Full entity history by version-less key |
| `GET` | `/inspect/by-correlation-id/{id}` | All entities in a business transaction |

### Jobs & Reports
| Method | Path | Description |
|---|---|---|
| `POST` | `/jobs/materialize` | Promote complete staging records to production |
| `POST` | `/jobs/compact` | Archive stale staging records |
| `GET` | `/reports/reconciliation` | Audit consistency report |

---

## Key Concepts

### Idempotent Key vs. Correlation ID

| | Idempotent Key | Correlation ID |
|---|---|---|
| **Answers** | "Has this version of this record been processed?" | "Which business transaction do these records belong to?" |
| **Scope** | Single source, single entity, single version | Cross-system, single business event |
| **Format** | `{source}:::{type}:::{key}:::{version}` | UUID |

### Five-Branch Deduplication

Every message is evaluated before any write:

| Condition | Decision |
|---|---|
| Key not seen before | `accept_new` |
| Same key, newer version | `accept_supersede` |
| Same key, same version, same payload hash | `skip_duplicate` |
| Same key, same version, **different** payload hash | `quarantine` |
| Same key, older version | `skip_stale` |

The `quarantine` branch surfaces data quality anomalies rather than silently overwriting or skipping them.

### Circuit Breaker States

```
CLOSED ──(N consecutive failures)──▶ OPEN
  ▲                                    │
  │                                    │ (operator reset)
  │                                    ▼
  └──────(1 success)────────────── HALF_OPEN
```

State is persisted in DB per downstream system. Half-open transition requires an explicit operator reset — no automatic time-based recovery in this implementation.

### Replay Eligibility

Replay is conservative by design:
- **DLQ items**: only `pending` entries with a valid `staging_id`
- **Ambiguous items**: only `retry_eligible` entries (query-before-retry confirmed not delivered)

Replay calls `simulate_delivery()` — the same canonical path as any original delivery. Circuit breakers, logging, and DLQ routing all apply normally. The `ReplayExecutionLog` records exactly what the delivery path returned.

---

## Quickstart

```bash
# Install
pip install -r requirements-dev.txt

# Run
uvicorn app.main:app --reload

# Interactive docs
open http://localhost:8000/docs

# Start at the dashboard
curl http://localhost:8000/dashboard/overview | python3 -m json.tool

# Test suite
pytest tests/ -v
```

---

## Demo Walkthrough

A typical operator session against the demo API:

```bash
# 1. Check system health
GET /dashboard/overview

# 2. Ingest a record
POST /ingest  {"source_system":"oms","entity_type":"order","business_key":"ORD-001","version":1,"payload":{...}}

# 3. Simulate delivery (succeeds)
POST /delivery/simulate  {"staging_id":1,"system_name":"warehouse_sync"}

# 4. Simulate delivery (fails — admissions_crm always fails in demo)
POST /delivery/simulate  {"staging_id":1,"system_name":"admissions_crm"}
# → outcome: dlq_transient_exhausted, dlq_entry_id: 1

# 5. Inspect the record's full history
GET /inspect/by-idempotent-key/oms:::order:::ORD-001:::1
# → ordered_events field shows the complete chronological story

# 6. Create and execute a replay
POST /replays  {"selection_mode":"dlq_items","source_item_ids":[1],"reason":"Post-outage recovery","requested_by":"ops@example.com"}
POST /replays/1/execute

# 7. Check the dashboard again — queue depths should reflect the activity
GET /dashboard/overview
```

---

## Downstream System Mock Behaviors

The demo seeds five downstream systems to cover all delivery paths:

| System | Behavior | Scenario |
|---|---|---|
| `warehouse_sync` | Always succeeds | Happy path |
| `admissions_crm` | Always fails (503) | Retry exhaustion → DLQ |
| `finance_ledger` | Non-retryable 400 | Immediate DLQ |
| `timeout_with_query` | Times out; supports status query | Ambiguous → retry_eligible |
| `timeout_no_query` | Times out; no status query | Ambiguous → status_unavailable |

---

## Design Decisions

**Why SQLite default?** Zero-dependency local development. Change one env var to switch to PostgreSQL — no code changes.

**Why no time-based circuit auto-recovery?** Operator-controlled recovery is explicit and auditable. The half-open state requires a deliberate reset, which forces an operator to confirm they believe the downstream has recovered.

**Why all-or-nothing replay eligibility?** A mixed batch where some items are ineligible should fail loudly. Silent partial replay would create gaps that are hard to detect.

**Why no replay bypass of the ingestion path?** Replay calls `simulate_delivery()` exactly like any other delivery call. The `ReplayExecutionLog` is the machine-readable proof that no shortcut was taken.

---

## License

MIT
