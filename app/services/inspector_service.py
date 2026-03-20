"""
inspector_service — assembles operator-facing inspection views from existing records.

Read-only.  No writes, no flushes, no commits.  Every query is a SELECT.

The inspector answers: "What is the complete story of this record or entity?"

Resolution paths:
  by_idempotent_key  — looks up IngestionAttemptLog by idempotent_key to establish
                       entity_key and correlation_id, then calls _assemble_by_entity_key.

  by_entity_key      — directly finds IdempotentState + all IngestionAttemptLog rows for
                       that entity, then calls _assemble_by_entity_key.

  by_correlation_id  — finds every StagingRecord with that correlation_id, resolves each
                       to an entity_key, and calls _assemble_by_entity_key for each,
                       returning a list of views (one per entity in the transaction).

All three paths funnel into _assemble_entity_view, which builds the full structured view
from existing models without duplicating field-level logic.
"""

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.ambiguous import AmbiguousOutcomeQueue
from app.models.correlation import DataAnomalyQueue
from app.models.delivery import DeliveryAttemptLog, DeliveryDLQ
from app.models.idempotent_state import IdempotentState
from app.models.ingestion_log import IngestionAttemptLog
from app.models.replay import ReplayExecutionLog, ReplayRequest, ReplayRequestItem
from app.models.staging import StagingRecord


# ---------------------------------------------------------------------------
# Internal assembly helpers
# ---------------------------------------------------------------------------

def _fmt(dt: datetime | None) -> str | None:
    """ISO-8601 format or None."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def _build_ordered_events(
    ingestion_history: list[dict],
    anomalies: list[dict],
    delivery_attempts: list[dict],
    delivery_dlq: list[dict],
    ambiguous_outcomes: list[dict],
    replay_activity: list[dict],
) -> list[dict]:
    """
    Assemble a single ordered event list from all inspection sections.

    Pure function — reads from already-assembled section dicts, no DB access.
    Each entry has: event_type, occurred_at, summary, ref_id, context.

    Entries with no timestamp are excluded.  Final list is sorted ascending
    by occurred_at so an operator can read the record's story in order.
    """
    events: list[dict] = []

    # --- Ingestion events ---
    for row in ingestion_history:
        if not row.get("occurred_at") and not row.get("received_at"):
            continue
        ts = row.get("received_at")
        if not ts:
            continue
        decision = row["decision"]
        label_map = {
            "accept_new":       "Accepted (new record)",
            "accept_supersede": "Accepted (supersedes earlier version)",
            "skip_duplicate":   "Skipped (exact duplicate)",
            "skip_stale":       "Skipped (stale version)",
            "quarantine":       "Quarantined (data anomaly)",
        }
        events.append({
            "event_type": "ingestion",
            "occurred_at": ts,
            "summary": label_map.get(decision, f"Ingestion: {decision}"),
            "ref_id": row["id"],
            "context": {
                "idempotent_key": row["idempotent_key"],
                "version": row["version"],
                "decision": decision,
                "source_system": row["source_system"],
            },
        })

    # --- Anomaly events ---
    for row in anomalies:
        ts = row.get("queued_at")
        if ts:
            events.append({
                "event_type": "anomaly_queued",
                "occurred_at": ts,
                "summary": f"Data anomaly queued: {row['reason']}",
                "ref_id": row["id"],
                "context": {
                    "idempotent_key": row["idempotent_key"],
                    "version": row["version"],
                    "reason": row["reason"],
                    "resolved": row["resolved"],
                },
            })
        if row.get("resolved_at"):
            events.append({
                "event_type": "anomaly_resolved",
                "occurred_at": row["resolved_at"],
                "summary": "Data anomaly resolved",
                "ref_id": row["id"],
                "context": {
                    "idempotent_key": row["idempotent_key"],
                    "resolution_note": row.get("resolution_note"),
                },
            })

    # --- Delivery attempt events ---
    for row in delivery_attempts:
        ts = row.get("attempted_at")
        if not ts:
            continue
        outcome = row["outcome"]
        outcome_labels = {
            "success": "Delivery succeeded",
            "failed":  "Delivery failed",
            "timeout": "Delivery timed out",
        }
        events.append({
            "event_type": "delivery_attempt",
            "occurred_at": ts,
            "summary": (
                f"{outcome_labels.get(outcome, f'Delivery: {outcome}')} "
                f"→ {row['system_name']} "
                f"(attempt {row['attempt_number']})"
            ),
            "ref_id": row["id"],
            "context": {
                "system_name": row["system_name"],
                "attempt_number": row["attempt_number"],
                "outcome": outcome,
                "classification": row["classification"],
                "http_status": row.get("http_status"),
            },
        })

    # --- DLQ events ---
    for row in delivery_dlq:
        ts = row.get("queued_at")
        if ts:
            events.append({
                "event_type": "dlq_queued",
                "occurred_at": ts,
                "summary": (
                    f"Delivery DLQ: {row['failure_category']} "
                    f"→ {row['system_name']}"
                ),
                "ref_id": row["id"],
                "context": {
                    "system_name": row["system_name"],
                    "failure_category": row["failure_category"],
                    "resolution_status": row["resolution_status"],
                },
            })
        if row.get("resolved_at"):
            events.append({
                "event_type": "dlq_resolved",
                "occurred_at": row["resolved_at"],
                "summary": f"DLQ entry resolved by {row.get('resolved_by', 'operator')}",
                "ref_id": row["id"],
                "context": {
                    "system_name": row["system_name"],
                    "resolution_status": row["resolution_status"],
                    "resolved_by": row.get("resolved_by"),
                },
            })

    # --- Ambiguous outcome events ---
    for row in ambiguous_outcomes:
        ts = row.get("created_at")
        if ts:
            events.append({
                "event_type": "ambiguous_queued",
                "occurred_at": ts,
                "summary": (
                    f"Ambiguous outcome queued: {row['ambiguity_category']} "
                    f"→ {row['system_name']}"
                ),
                "ref_id": row["id"],
                "context": {
                    "system_name": row["system_name"],
                    "ambiguity_category": row["ambiguity_category"],
                    "status": row["status"],
                },
            })
        # Emit a resolution event when status has moved past pending
        if row.get("status") != "pending" and row.get("updated_at") and row.get("updated_at") != row.get("created_at"):
            status = row["status"]
            status_labels = {
                "confirmed_delivered": "Query confirmed: already delivered — no retry needed",
                "retry_eligible":      "Query confirmed: not delivered — retry eligible",
                "status_unavailable":  "Query inconclusive: status unavailable — manual decision required",
            }
            events.append({
                "event_type": "ambiguous_resolved",
                "occurred_at": row["updated_at"],
                "summary": status_labels.get(status, f"Ambiguous outcome resolved: {status}"),
                "ref_id": row["id"],
                "context": {
                    "system_name": row["system_name"],
                    "new_status": status,
                    "resolution_note": row.get("resolution_note"),
                },
            })

    # --- Replay events ---
    for row in replay_activity:
        ts = row.get("item_created_at")
        if ts:
            events.append({
                "event_type": "replay_requested",
                "occurred_at": ts,
                "summary": (
                    f"Replay requested by {row.get('requested_by', 'operator')} "
                    f"(request #{row['replay_request_id']})"
                ),
                "ref_id": row["replay_request_id"],
                "context": {
                    "replay_request_id": row["replay_request_id"],
                    "system_name": row["system_name"],
                    "source_queue_type": row["selection_mode"],
                    "reason": row.get("reason"),
                },
            })
        exec_ts = row.get("executed_at")
        if exec_ts:
            result = row.get("execution_result", "unknown")
            result_labels = {
                "success":               "Replay succeeded",
                "dlq_transient_exhausted": "Replay failed again — re-queued to DLQ",
                "dlq_non_retryable":     "Replay failed (non-retryable) — re-queued to DLQ",
                "queued_ambiguous":      "Replay timed out — new ambiguous entry created",
                "circuit_open":          "Replay blocked — circuit is open",
                "error":                 "Replay execution error",
            }
            events.append({
                "event_type": "replay_executed",
                "occurred_at": exec_ts,
                "summary": result_labels.get(result, f"Replay executed: {result}"),
                "ref_id": row["replay_request_id"],
                "context": {
                    "replay_request_id": row["replay_request_id"],
                    "system_name": row["system_name"],
                    "execution_result": result,
                    "item_status": row["item_status"],
                },
            })

    # Sort ascending by occurred_at (ISO-8601 strings sort correctly as strings)
    events.sort(key=lambda e: e["occurred_at"])
    return events


async def _assemble_entity_view(
    session: AsyncSession,
    entity_key: str,
) -> dict[str, Any]:
    """
    Assemble the complete inspection view for one entity_key.

    Reads from: IdempotentState, StagingRecord, IngestionAttemptLog,
    DataAnomalyQueue, DeliveryAttemptLog, DeliveryDLQ,
    AmbiguousOutcomeQueue, ReplayRequestItem, ReplayExecutionLog, ReplayRequest.

    Returns grouped sections (for direct inspection) plus a unified `ordered_events`
    field that merges and sorts all events by timestamp ascending.
    """

    # --- Current entity state ---
    state: IdempotentState | None = await session.scalar(
        select(IdempotentState).where(IdempotentState.entity_key == entity_key)
    )

    identifiers: dict[str, Any] = {
        "entity_key": entity_key,
        "current_idempotent_key": state.idempotent_key if state else None,
        "source_system": state.source_system if state else None,
        "entity_type": state.entity_type if state else None,
        "business_key": state.business_key if state else None,
    }

    # --- Current staging context ---
    staging_summary: dict[str, Any] | None = None
    if state and state.active_staging_id:
        staging = await session.get(StagingRecord, state.active_staging_id)
        if staging:
            staging_summary = {
                "staging_id": staging.id,
                "idempotent_key": staging.idempotent_key,
                "version": staging.version,
                "completeness_status": staging.completeness_status,
                "payload_hash": staging.payload_hash,
                "correlation_id": staging.correlation_id,
                "staged_at": _fmt(staging.staged_at),
                "promoted_at": _fmt(staging.promoted_at),
            }

    # --- Ingestion history ---
    ingestion_rows = list(await session.scalars(
        select(IngestionAttemptLog)
        .where(IngestionAttemptLog.entity_key == entity_key)
        .order_by(IngestionAttemptLog.received_at)
    ))

    ingestion_history = [
        {
            "id": row.id,
            "idempotent_key": row.idempotent_key,
            "version": row.incoming_version,
            "decision": row.decision,
            "decision_reason": row.decision_reason,
            "source_system": row.source_system,
            "correlation_id": row.correlation_id,
            "received_at": _fmt(row.received_at),
        }
        for row in ingestion_rows
    ]

    # Derive the set of all idempotent_keys ever seen for this entity
    all_idempotent_keys = list({row.idempotent_key for row in ingestion_rows})
    if state and state.idempotent_key not in all_idempotent_keys:
        all_idempotent_keys.append(state.idempotent_key)

    # --- DataAnomalyQueue ---
    anomaly_rows = list(await session.scalars(
        select(DataAnomalyQueue)
        .where(DataAnomalyQueue.idempotent_key.in_(all_idempotent_keys))
        .order_by(DataAnomalyQueue.queued_at)
    )) if all_idempotent_keys else []

    anomalies = [
        {
            "id": row.id,
            "idempotent_key": row.idempotent_key,
            "version": row.version,
            "reason": row.reason,
            "reason_detail": row.reason_detail,
            "resolved": row.resolved,
            "resolution_note": row.resolution_note,
            "queued_at": _fmt(row.queued_at),
            "resolved_at": _fmt(row.resolved_at),
        }
        for row in anomaly_rows
    ]

    # --- Staging records for all idempotent keys (for delivery link) ---
    all_staging_rows = list(await session.scalars(
        select(StagingRecord)
        .where(StagingRecord.idempotent_key.in_(all_idempotent_keys))
        .order_by(StagingRecord.staged_at)
    )) if all_idempotent_keys else []

    all_staging_ids = [s.id for s in all_staging_rows]

    # --- Delivery attempts ---
    delivery_rows = list(await session.scalars(
        select(DeliveryAttemptLog)
        .where(DeliveryAttemptLog.staging_id.in_(all_staging_ids))
        .order_by(DeliveryAttemptLog.attempted_at)
    )) if all_staging_ids else []

    delivery_attempts = [
        {
            "id": row.id,
            "staging_id": row.staging_id,
            "system_name": row.system_name,
            "attempt_number": row.attempt_number,
            "http_status": row.http_status,
            "classification": row.classification,
            "outcome": row.outcome,
            "response_body_snippet": row.response_body_snippet,
            "attempted_at": _fmt(row.attempted_at),
        }
        for row in delivery_rows
    ]

    # --- DeliveryDLQ ---
    dlq_rows = list(await session.scalars(
        select(DeliveryDLQ)
        .where(DeliveryDLQ.staging_id.in_(all_staging_ids))
        .order_by(DeliveryDLQ.queued_at)
    )) if all_staging_ids else []

    delivery_dlq = [
        {
            "id": row.id,
            "staging_id": row.staging_id,
            "system_name": row.system_name,
            "failure_category": row.failure_category,
            "resolution_status": row.resolution_status,
            "resolved_by": row.resolved_by,
            "last_attempt_at": _fmt(row.last_attempt_at),
            "queued_at": _fmt(row.queued_at),
            "resolved_at": _fmt(row.resolved_at),
        }
        for row in dlq_rows
    ]

    # --- AmbiguousOutcomeQueue ---
    ambiguous_rows = list(await session.scalars(
        select(AmbiguousOutcomeQueue)
        .where(AmbiguousOutcomeQueue.staging_id.in_(all_staging_ids))
        .order_by(AmbiguousOutcomeQueue.created_at)
    )) if all_staging_ids else []

    ambiguous_outcomes = [
        {
            "id": row.id,
            "staging_id": row.staging_id,
            "system_name": row.system_name,
            "ambiguity_category": row.ambiguity_category,
            "status": row.status,
            "resolution_note": row.resolution_note,
            "idempotent_key": row.idempotent_key,
            "created_at": _fmt(row.created_at),
            "updated_at": _fmt(row.updated_at),
        }
        for row in ambiguous_rows
    ]

    # --- Replay activity ---
    # Look up ReplayRequestItems by staging_id
    replay_item_rows = list(await session.scalars(
        select(ReplayRequestItem)
        .where(ReplayRequestItem.staging_id.in_(all_staging_ids))
        .order_by(ReplayRequestItem.created_at)
    )) if all_staging_ids else []

    replay_activity = []
    for item in replay_item_rows:
        # Fetch the parent ReplayRequest for context
        request: ReplayRequest | None = await session.get(
            ReplayRequest, item.replay_request_id
        )
        # Fetch execution log if present
        exec_log: ReplayExecutionLog | None = await session.scalar(
            select(ReplayExecutionLog)
            .where(ReplayExecutionLog.replay_request_item_id == item.id)
        )
        replay_activity.append({
            "replay_request_id": item.replay_request_id,
            "replay_request_status": request.status if request else None,
            "requested_by": request.requested_by if request else None,
            "reason": request.reason if request else None,
            "selection_mode": item.source_queue_type,
            "source_queue_id": item.source_queue_id,
            "original_failure_category": item.original_failure_category,
            "system_name": item.system_name,
            "item_status": item.status,
            "result_note": item.result_note,
            "execution_result": exec_log.execution_result if exec_log else None,
            "executed_at": _fmt(exec_log.attempted_at) if exec_log else None,
            "item_created_at": _fmt(item.created_at),
        })

    # --- Summary ---
    summary = {
        "entity_key": entity_key,
        "current_version": state.current_version if state else None,
        "last_decision": state.last_decision if state else None,
        "first_seen_at": _fmt(state.first_seen_at) if state else None,
        "last_updated_at": _fmt(state.last_updated_at) if state else None,
        "has_anomalies": len(anomalies) > 0,
        "has_delivery_failures": len(delivery_dlq) > 0,
        "has_ambiguous_outcomes": len(ambiguous_outcomes) > 0,
        "has_replay_activity": len(replay_activity) > 0,
        "total_ingestion_attempts": len(ingestion_history),
        "total_delivery_attempts": len(delivery_attempts),
    }

    # --- Timeline — ordered cross-section event sequence ---
    ordered_events = _build_ordered_events(
        ingestion_history=ingestion_history,
        anomalies=anomalies,
        delivery_attempts=delivery_attempts,
        delivery_dlq=delivery_dlq,
        ambiguous_outcomes=ambiguous_outcomes,
        replay_activity=replay_activity,
    )

    return {
        "summary": summary,
        "identifiers": identifiers,
        "staging": staging_summary,
        "ingestion_history": ingestion_history,
        "anomalies": anomalies,
        "delivery_attempts": delivery_attempts,
        "delivery_dlq": delivery_dlq,
        "ambiguous_outcomes": ambiguous_outcomes,
        "replay_activity": replay_activity,
        "ordered_events": ordered_events,
    }


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

async def inspect_by_idempotent_key(
    session: AsyncSession,
    idempotent_key: str,
) -> dict[str, Any] | None:
    """
    Assemble the inspection view for the entity identified by idempotent_key.

    Resolves to entity_key via IngestionAttemptLog or IdempotentState.
    Returns None if no records exist for this key.
    """
    # Try IngestionAttemptLog first — it always stores entity_key alongside idempotent_key
    log_row = await session.scalar(
        select(IngestionAttemptLog)
        .where(IngestionAttemptLog.idempotent_key == idempotent_key)
        .limit(1)
    )
    if log_row:
        return await _assemble_entity_view(session, log_row.entity_key)

    # Fall back to IdempotentState (entity may have no ingestion log yet in edge cases)
    state = await session.scalar(
        select(IdempotentState)
        .where(IdempotentState.idempotent_key == idempotent_key)
    )
    if state:
        return await _assemble_entity_view(session, state.entity_key)

    return None


async def inspect_by_entity_key(
    session: AsyncSession,
    entity_key: str,
) -> dict[str, Any] | None:
    """
    Assemble the inspection view for the entity identified by entity_key.

    Returns None if no records exist for this entity_key.
    """
    # Verify the entity exists
    exists = await session.scalar(
        select(IdempotentState).where(IdempotentState.entity_key == entity_key)
    )
    if exists is None:
        # Check ingestion log as a fallback (entity may have been quarantined before state was set)
        log_exists = await session.scalar(
            select(IngestionAttemptLog)
            .where(IngestionAttemptLog.entity_key == entity_key)
            .limit(1)
        )
        if log_exists is None:
            return None

    return await _assemble_entity_view(session, entity_key)


async def inspect_by_correlation_id(
    session: AsyncSession,
    correlation_id: str,
) -> list[dict[str, Any]]:
    """
    Assemble inspection views for all entities sharing a correlation_id.

    A business transaction may involve multiple entities from different source
    systems — each gets its own view.  Returns a list ordered by entity_key.
    Returns an empty list if no records exist for this correlation_id.
    """
    # Collect all entity_keys associated with this correlation_id via staging records
    staging_rows = list(await session.scalars(
        select(StagingRecord)
        .where(StagingRecord.correlation_id == correlation_id)
    ))
    entity_keys_from_staging = {s.idempotent_key for s in staging_rows}

    # Also collect via IngestionAttemptLog (catches records that never made it to staging)
    log_rows = list(await session.scalars(
        select(IngestionAttemptLog)
        .where(IngestionAttemptLog.correlation_id == correlation_id)
    ))
    entity_keys_set = {row.entity_key for row in log_rows}

    # Resolve staging idempotent_keys to entity_keys via IngestionAttemptLog
    for ik in entity_keys_from_staging:
        log_row = await session.scalar(
            select(IngestionAttemptLog)
            .where(IngestionAttemptLog.idempotent_key == ik)
            .limit(1)
        )
        if log_row:
            entity_keys_set.add(log_row.entity_key)

    if not entity_keys_set:
        return []

    views = []
    for ek in sorted(entity_keys_set):
        view = await _assemble_entity_view(session, ek)
        if view:
            views.append(view)
    return views
