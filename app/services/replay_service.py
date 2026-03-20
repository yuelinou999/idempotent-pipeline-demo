"""
replay_service — operator-initiated delivery replay.

Replay re-enters the existing canonical delivery path (simulate_delivery) for
records that previously failed or produced an ambiguous outcome.  It does NOT
bypass circuit breakers, delivery attempt logging, or any other delivery-layer
behavior.  Every replay call produces the same range of outcomes as an original
delivery call — including success, DLQ, queued_ambiguous, or circuit_open.

Supported sources:
  dlq_items        — DeliveryDLQ entries with resolution_status=pending
  ambiguous_items  — AmbiguousOutcomeQueue entries with status=retry_eligible

Eligibility is validated at request creation time.  Ineligible items cause the
entire creation call to fail with a clear error.

Transaction model:
  create_replay_request commits once after building the full item list.
  execute_replay processes items sequentially; each item is its own commit
  cycle because simulate_delivery commits internally.  The request status
  is updated in a final commit after all items are processed.
"""

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import ReplayItemStatus, ReplaySelectionMode, ReplayStatus
from app.models.ambiguous import AmbiguousOutcomeQueue
from app.models.delivery import DeliveryAttemptLog, DeliveryDLQ
from app.models.replay import ReplayExecutionLog, ReplayRequest, ReplayRequestItem
from app.models.staging import StagingRecord
from app.services.delivery_service import simulate_delivery


class ReplayStateError(Exception):
    """
    Raised when a ReplayRequest exists but its current status prevents the
    requested operation.  Distinct from ValueError (not found) so the route
    layer can return 409 Conflict rather than 404 Not Found.
    """


# ---------------------------------------------------------------------------
# Eligibility helpers
# ---------------------------------------------------------------------------

async def _validate_dlq_item(
    session: AsyncSession,
    dlq_id: int,
) -> DeliveryDLQ:
    """
    Return the DeliveryDLQ row if it is eligible for replay.

    Raises ValueError for:
      - row not found
      - resolution_status != pending (already resolved or replayed)
      - staging_id is None (no staging record to replay from)
    """
    entry: DeliveryDLQ | None = await session.get(DeliveryDLQ, dlq_id)
    if entry is None:
        raise ValueError(f"DeliveryDLQ entry {dlq_id} not found.")
    if entry.resolution_status != "pending":
        raise ValueError(
            f"DeliveryDLQ entry {dlq_id} is not eligible for replay: "
            f"resolution_status is {entry.resolution_status!r}. "
            "Only 'pending' entries can be replayed."
        )
    if entry.staging_id is None:
        raise ValueError(
            f"DeliveryDLQ entry {dlq_id} has no staging_id and cannot be replayed."
        )
    return entry


async def _validate_ambiguous_item(
    session: AsyncSession,
    ambiguous_id: int,
) -> AmbiguousOutcomeQueue:
    """
    Return the AmbiguousOutcomeQueue row if it is eligible for replay.

    Raises ValueError for:
      - row not found
      - status != retry_eligible (query-before-retry has not confirmed it safe to retry)
      - staging_id is None
    """
    entry: AmbiguousOutcomeQueue | None = await session.get(
        AmbiguousOutcomeQueue, ambiguous_id
    )
    if entry is None:
        raise ValueError(f"AmbiguousOutcomeQueue entry {ambiguous_id} not found.")
    if entry.status != "retry_eligible":
        raise ValueError(
            f"AmbiguousOutcomeQueue entry {ambiguous_id} is not eligible for replay: "
            f"status is {entry.status!r}. "
            "Only 'retry_eligible' entries (confirmed not delivered by query-before-retry) "
            "can be replayed."
        )
    if entry.staging_id is None:
        raise ValueError(
            f"AmbiguousOutcomeQueue entry {ambiguous_id} has no staging_id and cannot be replayed."
        )
    return entry


# ---------------------------------------------------------------------------
# Outcome mapping helpers
# ---------------------------------------------------------------------------

def _map_outcome_to_item_status(outcome: str) -> str:
    """Map a simulate_delivery outcome string to a ReplayItemStatus value."""
    if outcome == "success":
        return ReplayItemStatus.SUCCESS.value
    if outcome in ("dlq_transient_exhausted", "dlq_non_retryable"):
        return ReplayItemStatus.DLQ.value
    if outcome == "queued_ambiguous":
        return ReplayItemStatus.QUEUED_AMBIGUOUS.value
    if outcome == "circuit_open":
        return ReplayItemStatus.CIRCUIT_OPEN.value
    return ReplayItemStatus.ERROR.value


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def create_replay_request(
    session: AsyncSession,
    selection_mode: str,
    reason: str,
    requested_by: str,
    source_item_ids: list[int],
) -> dict[str, Any]:
    """
    Create a ReplayRequest and populate its ReplayRequestItem rows.

    Validates all source items before writing anything.  If any item is
    ineligible, raises ValueError and writes nothing.

    selection_mode must be one of: dlq_items | ambiguous_items
    source_item_ids are IDs in the corresponding source table.

    Returns a summary dict with the request ID and item count.
    """
    valid_modes = {m.value for m in ReplaySelectionMode}
    if selection_mode not in valid_modes:
        raise ValueError(
            f"selection_mode must be one of {sorted(valid_modes)}, "
            f"got {selection_mode!r}"
        )
    if not source_item_ids:
        raise ValueError("source_item_ids must not be empty.")

    # --- Validate all items upfront; fail-all-or-nothing ---
    if selection_mode == ReplaySelectionMode.DLQ_ITEMS.value:
        source_rows = [
            await _validate_dlq_item(session, iid) for iid in source_item_ids
        ]
    else:  # ambiguous_items
        source_rows = [
            await _validate_ambiguous_item(session, iid) for iid in source_item_ids
        ]

    # --- Create the request ---
    request = ReplayRequest(
        selection_mode=selection_mode,
        reason=reason,
        requested_by=requested_by,
        status=ReplayStatus.PENDING.value,
    )
    session.add(request)
    await session.flush()

    # --- Create items ---
    for src in source_rows:
        if selection_mode == ReplaySelectionMode.DLQ_ITEMS.value:
            # Resolve provenance from the linked StagingRecord so the item
            # is a complete audit record at creation time, not just on execute.
            staging: StagingRecord | None = await session.get(
                StagingRecord, src.staging_id
            )
            item = ReplayRequestItem(
                replay_request_id=request.id,
                source_queue_type="delivery_dlq",
                source_queue_id=src.id,
                original_failure_category=src.failure_category,
                delivery_attempt_log_id=None,  # DLQ row does not carry attempt log FK
                staging_id=src.staging_id,
                system_name=src.system_name,
                idempotent_key=staging.idempotent_key if staging else None,
                correlation_id=staging.correlation_id if staging else None,
                status=ReplayItemStatus.PENDING.value,
            )
        else:  # ambiguous_items
            item = ReplayRequestItem(
                replay_request_id=request.id,
                source_queue_type="ambiguous_outcome_queue",
                source_queue_id=src.id,
                original_failure_category=src.ambiguity_category,
                delivery_attempt_log_id=src.delivery_attempt_log_id,
                staging_id=src.staging_id,
                system_name=src.system_name,
                idempotent_key=src.idempotent_key,
                correlation_id=src.correlation_id,
                status=ReplayItemStatus.PENDING.value,
            )
        session.add(item)

    await session.commit()

    return {
        "replay_request_id": request.id,
        "selection_mode": selection_mode,
        "status": request.status,
        "items_created": len(source_rows),
        "requested_by": requested_by,
        "reason": reason,
    }


async def execute_replay(
    session: AsyncSession,
    replay_request_id: int,
) -> dict[str, Any]:
    """
    Execute all pending items in a ReplayRequest.

    For each pending item:
      1. Calls simulate_delivery(session, staging_id, system_name) — the
         existing canonical delivery path.
      2. Writes a ReplayExecutionLog row recording the exact outcome.
      3. Updates the ReplayRequestItem status.
      4. If the source was a DLQ item and delivery succeeded, marks that
         DLQ entry as manually_replayed.

    Each item is committed individually because simulate_delivery commits
    internally.  The request status is updated in a final commit.

    Raises ValueError if the request does not exist or is not in pending status.
    """
    request: ReplayRequest | None = await session.get(ReplayRequest, replay_request_id)
    if request is None:
        raise ValueError(f"ReplayRequest {replay_request_id} not found.")
    if request.status != ReplayStatus.PENDING.value:
        raise ReplayStateError(
            f"ReplayRequest {replay_request_id} is in status {request.status!r} "
            "and cannot be executed. Only 'pending' requests can be executed."
        )

    # Mark as executing
    request.status = ReplayStatus.EXECUTING.value
    request.updated_at = datetime.now(timezone.utc)
    await session.commit()

    items = list(await session.scalars(
        select(ReplayRequestItem)
        .where(ReplayRequestItem.replay_request_id == replay_request_id)
        .where(ReplayRequestItem.status == ReplayItemStatus.PENDING.value)
        .order_by(ReplayRequestItem.id)
    ))

    all_errored = True
    had_error = False

    for item in items:
        try:
            result = await simulate_delivery(
                session=session,
                staging_id=item.staging_id,
                system_name=item.system_name,
            )
            # simulate_delivery already committed; begin a new transaction
            outcome = result["outcome"]
            item_status = _map_outcome_to_item_status(outcome)
            all_errored = False

            # Attempt to identify the delivery attempt log created during replay.
            # Query the most recent attempt log for this staging_id/system_name.
            created_attempt_log_id: int | None = None
            if outcome != "circuit_open":
                latest_log = await session.scalar(
                    select(DeliveryAttemptLog.id)
                    .where(DeliveryAttemptLog.staging_id == item.staging_id)
                    .where(DeliveryAttemptLog.system_name == item.system_name)
                    .order_by(DeliveryAttemptLog.attempted_at.desc())
                    .limit(1)
                )
                created_attempt_log_id = latest_log

            outcome_note = (
                f"Replay outcome: {outcome}. "
                f"Attempts made: {result.get('attempts_made', 0)}. "
                f"Circuit state: {result.get('circuit_state', 'unknown')}."
            )
            if result.get("dlq_entry_id"):
                outcome_note += f" New DLQ entry ID: {result['dlq_entry_id']}."
            if result.get("ambiguous_id"):
                outcome_note += f" New AmbiguousOutcomeQueue entry ID: {result['ambiguous_id']}."

        except Exception as exc:
            outcome = "error"
            item_status = ReplayItemStatus.ERROR.value
            outcome_note = f"Unexpected error during replay execution: {exc!s}"
            created_attempt_log_id = None
            had_error = True

        # Write execution log and update item — one commit per item
        log = ReplayExecutionLog(
            replay_request_id=replay_request_id,
            replay_request_item_id=item.id,
            execution_result=outcome,
            outcome_note=outcome_note,
            created_delivery_attempt_log_id=created_attempt_log_id,
            created_ingestion_attempt_log_id=None,
        )
        session.add(log)

        item.status = item_status
        item.result_note = outcome_note
        item.updated_at = datetime.now(timezone.utc)

        # If a DLQ source item was successfully replayed, mark it as replayed
        if (
            item.source_queue_type == "delivery_dlq"
            and outcome == "success"
        ):
            dlq_entry: DeliveryDLQ | None = await session.get(
                DeliveryDLQ, item.source_queue_id
            )
            if dlq_entry and dlq_entry.resolution_status == "pending":
                dlq_entry.resolution_status = "manually_replayed"
                dlq_entry.resolved_at = datetime.now(timezone.utc)
                dlq_entry.resolved_by = f"replay_request_{replay_request_id}"

        await session.commit()

    # Final request status update
    request = await session.get(ReplayRequest, replay_request_id)
    request.status = (
        ReplayStatus.FAILED.value if had_error and all_errored
        else ReplayStatus.COMPLETED.value
    )
    request.updated_at = datetime.now(timezone.utc)
    await session.commit()

    return {
        "replay_request_id": replay_request_id,
        "status": request.status,
        "items_processed": len(items),
    }


async def get_replay(
    session: AsyncSession,
    replay_request_id: int,
) -> dict[str, Any]:
    """
    Return full detail of a ReplayRequest including all items and execution logs.
    """
    request: ReplayRequest | None = await session.get(ReplayRequest, replay_request_id)
    if request is None:
        raise ValueError(f"ReplayRequest {replay_request_id} not found.")

    items = list(await session.scalars(
        select(ReplayRequestItem)
        .where(ReplayRequestItem.replay_request_id == replay_request_id)
        .order_by(ReplayRequestItem.id)
    ))

    logs = list(await session.scalars(
        select(ReplayExecutionLog)
        .where(ReplayExecutionLog.replay_request_id == replay_request_id)
        .order_by(ReplayExecutionLog.attempted_at)
    ))
    log_by_item = {lg.replay_request_item_id: lg for lg in logs}

    return {
        "id": request.id,
        "selection_mode": request.selection_mode,
        "reason": request.reason,
        "requested_by": request.requested_by,
        "status": request.status,
        "created_at": request.created_at.isoformat(),
        "updated_at": request.updated_at.isoformat(),
        "items": [
            {
                "id": item.id,
                "source_queue_type": item.source_queue_type,
                "source_queue_id": item.source_queue_id,
                "original_failure_category": item.original_failure_category,
                "staging_id": item.staging_id,
                "system_name": item.system_name,
                "idempotent_key": item.idempotent_key,
                "correlation_id": item.correlation_id,
                "status": item.status,
                "result_note": item.result_note,
                "execution_log": (
                    {
                        "execution_result": log_by_item[item.id].execution_result,
                        "outcome_note": log_by_item[item.id].outcome_note,
                        "attempted_at": log_by_item[item.id].attempted_at.isoformat(),
                        "created_delivery_attempt_log_id": (
                            log_by_item[item.id].created_delivery_attempt_log_id
                        ),
                    }
                    if item.id in log_by_item else None
                ),
            }
            for item in items
        ],
    }


async def list_replays(
    session: AsyncSession,
    status: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Return summary rows for ReplayRequest entries, newest first."""
    q = (
        select(ReplayRequest)
        .order_by(ReplayRequest.created_at.desc())
        .limit(limit)
    )
    if status:
        q = q.where(ReplayRequest.status == status)

    rows = await session.scalars(q)
    return [
        {
            "id": r.id,
            "selection_mode": r.selection_mode,
            "reason": r.reason,
            "requested_by": r.requested_by,
            "status": r.status,
            "created_at": r.created_at.isoformat(),
            "updated_at": r.updated_at.isoformat(),
        }
        for r in rows
    ]
