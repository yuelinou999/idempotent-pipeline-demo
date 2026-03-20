"""
query_before_retry_service — investigates ambiguous delivery outcomes.

When a delivery attempt times out, the pipeline creates an AmbiguousOutcomeQueue
entry and stops.  This service provides the next step: querying the downstream
system to determine what actually happened.

In the demo architecture there are no real downstream integrations.  The
query result is simulated via DownstreamSystem.mock_status_query_behavior.

Query outcomes and their effect on AmbiguousOutcomeQueue.status:

  confirmed_delivered  → status = confirmed_delivered
                         The downstream confirms it processed the request.
                         No retry is needed.  Entry is closed.

  not_found            → status = retry_eligible
                         The downstream has no record of the request.
                         The record may safely be retried by an operator.
                         Entry is closed pending the retry action.

  status_unavailable   → status = status_unavailable
                         The system does not support status queries
                         (supports_status_query = False) or the mock
                         returns status_unavailable.  Entry remains
                         unresolved; operator must decide next action.

All writes are flush-only.  The calling route handler commits.
"""

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import AmbiguousStatus, MockStatusQueryBehavior
from app.models.ambiguous import AmbiguousOutcomeQueue
from app.models.delivery import DownstreamSystem


async def run_status_query(
    session: AsyncSession,
    ambiguous_id: int,
) -> dict[str, Any]:
    """
    Run a query-before-retry check for one AmbiguousOutcomeQueue entry.

    Looks up the downstream system, simulates a status query via
    mock_status_query_behavior, updates the entry status, and returns
    a summary dict.

    Only entries in PENDING status can be queried.  Calling this on an
    already-resolved entry raises ValueError.

    Flushes but does not commit — the caller commits.
    """
    entry: AmbiguousOutcomeQueue | None = await session.get(
        AmbiguousOutcomeQueue, ambiguous_id
    )
    if entry is None:
        raise ValueError(f"AmbiguousOutcomeQueue entry {ambiguous_id} not found.")
    if entry.status != AmbiguousStatus.PENDING.value:
        raise ValueError(
            f"AmbiguousOutcomeQueue entry {ambiguous_id} is already in status "
            f"{entry.status!r} and cannot be queried again."
        )

    # Look up the downstream system to determine query capability and mock result.
    system: DownstreamSystem | None = await session.scalar(
        select(DownstreamSystem).where(
            DownstreamSystem.id == entry.downstream_system_id
        )
    )

    now = datetime.now(timezone.utc)

    if system is None or not system.supports_status_query:
        # System does not support queries — mark as status_unavailable.
        entry.status = AmbiguousStatus.STATUS_UNAVAILABLE.value
        entry.resolution_note = (
            "Downstream system does not support status queries. "
            "Manual operator decision required."
        )
        entry.updated_at = now
        await session.flush()
        return {
            "ambiguous_id": ambiguous_id,
            "system_name": entry.system_name,
            "query_result": "status_unavailable",
            "new_status": entry.status,
            "resolution_note": entry.resolution_note,
            "action_required": "manual_operator_decision",
        }

    # Simulate the status query using the mock configuration.
    mock_result = system.mock_status_query_behavior

    if mock_result == MockStatusQueryBehavior.CONFIRMED_DELIVERED.value:
        entry.status = AmbiguousStatus.CONFIRMED_DELIVERED.value
        entry.resolution_note = (
            "Downstream confirmed the request was processed. No retry needed."
        )
        action = "none_confirmed_delivered"

    elif mock_result == MockStatusQueryBehavior.NOT_FOUND.value:
        entry.status = AmbiguousStatus.RETRY_ELIGIBLE.value
        entry.resolution_note = (
            "Downstream has no record of this request. Safe to retry."
        )
        action = "retry_eligible"

    else:  # status_unavailable
        entry.status = AmbiguousStatus.STATUS_UNAVAILABLE.value
        entry.resolution_note = (
            "Downstream status query returned an inconclusive result. "
            "Manual operator decision required."
        )
        action = "manual_operator_decision"

    entry.updated_at = now
    await session.flush()

    return {
        "ambiguous_id": ambiguous_id,
        "system_name": entry.system_name,
        "query_result": mock_result,
        "new_status": entry.status,
        "resolution_note": entry.resolution_note,
        "action_required": action,
    }
