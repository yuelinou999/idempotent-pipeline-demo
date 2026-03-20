"""
dashboard_service — read-only operator overview aggregations.

Assembles the system-health snapshot for GET /dashboard/overview.
All queries are SELECTs only — no writes, flushes, or commits.

The overview answers the operator's first question when they open the console:
  "What is the current state of the pipeline, and what needs attention?"

Data sources: all existing models.  No new tables or event buses.
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.ambiguous import AmbiguousOutcomeQueue
from app.models.circuit import CircuitBreakerState
from app.models.correlation import DataAnomalyQueue
from app.models.delivery import DeliveryAttemptLog, DeliveryDLQ
from app.models.idempotent_state import IdempotentState
from app.models.replay import ReplayExecutionLog, ReplayRequest


_RECENT_HOURS = 24   # lookback window for recent activity
_RECENT_LIMIT = 20   # max recent activity entries


def _fmt(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


# ---------------------------------------------------------------------------
# Count helpers
# ---------------------------------------------------------------------------

async def _count_entities(session: AsyncSession) -> int:
    return await session.scalar(select(func.count()).select_from(IdempotentState)) or 0


async def _count_open_anomalies(session: AsyncSession) -> int:
    return await session.scalar(
        select(func.count())
        .select_from(DataAnomalyQueue)
        .where(DataAnomalyQueue.resolved == False)  # noqa: E712
    ) or 0


async def _count_pending_dlq(session: AsyncSession) -> int:
    return await session.scalar(
        select(func.count())
        .select_from(DeliveryDLQ)
        .where(DeliveryDLQ.resolution_status == "pending")
    ) or 0


async def _count_pending_ambiguous(session: AsyncSession) -> int:
    return await session.scalar(
        select(func.count())
        .select_from(AmbiguousOutcomeQueue)
        .where(AmbiguousOutcomeQueue.status == "pending")
    ) or 0


async def _circuit_health(session: AsyncSession) -> dict[str, Any]:
    rows = list(await session.scalars(
        select(CircuitBreakerState).order_by(CircuitBreakerState.system_name)
    ))
    counts = {"closed": 0, "open": 0, "half_open": 0}
    systems = []
    for r in rows:
        counts[r.state] = counts.get(r.state, 0) + 1
        systems.append({
            "system_name": r.system_name,
            "state": r.state,
            "consecutive_failure_count": r.consecutive_failure_count,
            "opened_at": _fmt(r.opened_at),
            "last_transition_reason": r.last_transition_reason,
            "links": {
                "circuit_detail": f"/circuits/{r.system_name}",
                "circuit_reset": f"/circuits/{r.system_name}/reset",
            },
        })
    return {
        "closed": counts.get("closed", 0),
        "open": counts.get("open", 0),
        "half_open": counts.get("half_open", 0),
        "systems": systems,
    }


async def _replay_summary(session: AsyncSession) -> dict[str, Any]:
    rows = await session.execute(
        select(ReplayRequest.status, func.count().label("n"))
        .group_by(ReplayRequest.status)
    )
    counts = {row.status: row.n for row in rows}
    return {
        "pending":   counts.get("pending", 0),
        "executing": counts.get("executing", 0),
        "completed": counts.get("completed", 0),
        "failed":    counts.get("failed", 0),
        "links": {"list_replays": "/replays"},
    }


# ---------------------------------------------------------------------------
# Recent activity
# ---------------------------------------------------------------------------

async def _recent_activity(session: AsyncSession) -> list[dict[str, Any]]:
    """
    Assemble a lightweight recent-activity stream from existing records.

    Sources (newest-first, limited):
      - Recent delivery failures (DeliveryAttemptLog outcome != success)
      - Recently queued ambiguous outcomes
      - Recently executed replays
      - Recently opened circuits
      - Recently queued anomalies
    """
    since = datetime.now(timezone.utc) - timedelta(hours=_RECENT_HOURS)
    events: list[dict[str, Any]] = []

    # --- Recent delivery failures ---
    fail_rows = list(await session.scalars(
        select(DeliveryAttemptLog)
        .where(DeliveryAttemptLog.outcome != "success")
        .where(DeliveryAttemptLog.attempted_at >= since)
        .order_by(DeliveryAttemptLog.attempted_at.desc())
        .limit(_RECENT_LIMIT)
    ))
    for r in fail_rows:
        events.append({
            "event_type": "delivery_failure",
            "occurred_at": _fmt(r.attempted_at),
            "summary": (
                f"Delivery {r.outcome} → {r.system_name} "
                f"(attempt {r.attempt_number}, {r.classification})"
            ),
            "context": {
                "system_name": r.system_name,
                "outcome": r.outcome,
                "classification": r.classification,
                "http_status": r.http_status,
            },
            "links": {
                "delivery_dlq": "/dlq/delivery",
                "ambiguous_queue": "/dlq/ambiguous",
            },
        })

    # --- Recently queued ambiguous outcomes ---
    amb_rows = list(await session.scalars(
        select(AmbiguousOutcomeQueue)
        .where(AmbiguousOutcomeQueue.created_at >= since)
        .order_by(AmbiguousOutcomeQueue.created_at.desc())
        .limit(_RECENT_LIMIT)
    ))
    for r in amb_rows:
        events.append({
            "event_type": "ambiguous_outcome",
            "occurred_at": _fmt(r.created_at),
            "summary": (
                f"Ambiguous outcome queued: {r.ambiguity_category} → {r.system_name} "
                f"(status: {r.status})"
            ),
            "context": {
                "system_name": r.system_name,
                "ambiguity_category": r.ambiguity_category,
                "status": r.status,
                "idempotent_key": r.idempotent_key,
            },
            "links": {
                "ambiguous_detail": f"/dlq/ambiguous/{r.id}",
                "query_status": f"/dlq/ambiguous/{r.id}/query-status",
                **(
                    {"inspector": f"/inspect/by-idempotent-key/{r.idempotent_key}"}
                    if r.idempotent_key else {}
                ),
            },
        })

    # --- Recently executed replays ---
    exec_rows = list(await session.scalars(
        select(ReplayExecutionLog)
        .where(ReplayExecutionLog.attempted_at >= since)
        .order_by(ReplayExecutionLog.attempted_at.desc())
        .limit(_RECENT_LIMIT)
    ))
    for r in exec_rows:
        events.append({
            "event_type": "replay_execution",
            "occurred_at": _fmt(r.attempted_at),
            "summary": f"Replay executed: {r.execution_result} (request #{r.replay_request_id})",
            "context": {
                "replay_request_id": r.replay_request_id,
                "execution_result": r.execution_result,
            },
            "links": {
                "replay_detail": f"/replays/{r.replay_request_id}",
            },
        })

    # --- Recently opened / changed circuits ---
    circuit_rows = list(await session.scalars(
        select(CircuitBreakerState)
        .where(CircuitBreakerState.state != "closed")
        .order_by(CircuitBreakerState.updated_at.desc())
        .limit(10)
    ))
    for r in circuit_rows:
        events.append({
            "event_type": "circuit_state_change",
            "occurred_at": _fmt(r.updated_at),
            "summary": (
                f"Circuit {r.state.upper()} → {r.system_name} "
                f"({r.consecutive_failure_count} consecutive failures)"
            ),
            "context": {
                "system_name": r.system_name,
                "state": r.state,
                "consecutive_failure_count": r.consecutive_failure_count,
                "last_transition_reason": r.last_transition_reason,
            },
            "links": {
                "circuit_detail": f"/circuits/{r.system_name}",
                "circuit_reset": f"/circuits/{r.system_name}/reset",
            },
        })

    # --- Recent anomalies ---
    anomaly_rows = list(await session.scalars(
        select(DataAnomalyQueue)
        .where(DataAnomalyQueue.queued_at >= since)
        .order_by(DataAnomalyQueue.queued_at.desc())
        .limit(10)
    ))
    for r in anomaly_rows:
        events.append({
            "event_type": "data_anomaly",
            "occurred_at": _fmt(r.queued_at),
            "summary": f"Data anomaly queued: {r.reason} (v{r.version}, {r.source_system})",
            "context": {
                "idempotent_key": r.idempotent_key,
                "reason": r.reason,
                "version": r.version,
                "resolved": r.resolved,
            },
            "links": {
                "anomaly_queue": "/dlq/data-anomaly",
                "inspector": f"/inspect/by-idempotent-key/{r.idempotent_key}",
            },
        })

    # Sort descending by occurred_at (most recent first for an activity feed)
    events.sort(key=lambda e: e["occurred_at"] or "", reverse=True)
    return events[:_RECENT_LIMIT]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def get_overview(session: AsyncSession) -> dict[str, Any]:
    """
    Assemble the full dashboard overview.  Read-only.

    Returns a structured dict suitable for operator console rendering.
    """
    now = datetime.now(timezone.utc)

    total_entities       = await _count_entities(session)
    open_anomalies       = await _count_open_anomalies(session)
    pending_dlq          = await _count_pending_dlq(session)
    pending_ambiguous    = await _count_pending_ambiguous(session)
    circuit_health       = await _circuit_health(session)
    replay_summary       = await _replay_summary(session)
    recent_activity      = await _recent_activity(session)

    attention_needed = (
        open_anomalies > 0
        or pending_dlq > 0
        or pending_ambiguous > 0
        or circuit_health["open"] > 0
        or circuit_health["half_open"] > 0
    )

    return {
        "generated_at": now.isoformat(),
        "attention_needed": attention_needed,
        "summary": {
            "total_entities": total_entities,
            "open_anomalies": open_anomalies,
            "pending_dlq_items": pending_dlq,
            "pending_ambiguous_items": pending_ambiguous,
            "open_circuits": circuit_health["open"],
            "half_open_circuits": circuit_health["half_open"],
            "pending_replays": replay_summary["pending"],
        },
        "circuit_health": circuit_health,
        "replay_summary": replay_summary,
        "recent_activity": recent_activity,
        "quick_links": {
            "delivery_dlq":       "/dlq/delivery",
            "data_anomaly_queue": "/dlq/data-anomaly",
            "ambiguous_queue":    "/dlq/ambiguous",
            "circuits":           "/circuits",
            "replays":            "/replays",
            "inspector_by_entity_key": "/inspect/by-entity-key/{entity_key}",
            "inspector_by_correlation": "/inspect/by-correlation-id/{correlation_id}",
        },
    }
