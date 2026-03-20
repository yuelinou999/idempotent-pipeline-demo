"""
Replay operator console endpoints.

Provides the operator surface for creating and executing controlled, auditable
delivery replay requests.  Every replay execution goes through the canonical
delivery path — no bypass shortcuts exist.

These routes back the "Replay" view in the operator console:
  - See all replay requests and their status
  - Inspect what was replayed, why, and what each item produced
  - Create a new replay from eligible queue-backed sources
  - Execute a pending replay request

Routes:
  GET  /replays              — list replay requests
  GET  /replays/{id}         — full detail with items and execution log
  POST /replays              — create a replay request from eligible items
  POST /replays/{id}/execute — execute a pending replay request
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas.replay import (
    CreateReplayRequest,
    ExecuteReplayResponse,
    ReplayRequestCreatedResponse,
    ReplayRequestDetail,
    ReplayRequestSummary,
)
from app.services import replay_service
from app.services.replay_service import ReplayStateError

router = APIRouter(tags=["operator console"])


@router.get(
    "/replays",
    response_model=list[ReplayRequestSummary],
    summary="List replay requests",
)
async def list_replays(
    status: str | None = Query(
        None,
        description=(
            "Filter by request status: pending | executing | completed | failed. "
            "Omit to return all."
        ),
    ),
    limit: int = Query(50, le=500),
    db: AsyncSession = Depends(get_db),
) -> list[ReplayRequestSummary]:
    """
    List all replay requests, newest first.

    Use this view to answer:
    - Which replay requests are currently pending or executing?
    - What has been replayed recently and what was the outcome?
    - Who requested each replay and why?
    """
    rows = await replay_service.list_replays(db, status=status, limit=limit)
    return [ReplayRequestSummary(**r) for r in rows]


@router.get(
    "/replays/{replay_id}",
    response_model=ReplayRequestDetail,
    summary="Full detail for one replay request",
)
async def get_replay(
    replay_id: int,
    db: AsyncSession = Depends(get_db),
) -> ReplayRequestDetail:
    """
    Return full detail for a single replay request, including all items and their
    execution logs.

    The `execution_log` field on each item is the audit proof that replay went
    through the canonical delivery path.  It records what `simulate_delivery()`
    returned for that item, including circuit state and any new DLQ or
    AmbiguousOutcomeQueue entries created.
    """
    try:
        result = await replay_service.get_replay(db, replay_request_id=replay_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return ReplayRequestDetail(**result)


@router.post(
    "/replays",
    response_model=ReplayRequestCreatedResponse,
    status_code=201,
    summary="Create a replay request from eligible queue-backed items",
)
async def create_replay(
    request: CreateReplayRequest,
    db: AsyncSession = Depends(get_db),
) -> ReplayRequestCreatedResponse:
    """
    Create a replay request from eligible items in a supported source queue.

    **Selection modes:**

    - `dlq_items` — replay from DeliveryDLQ entries.  Only `pending` entries
      with a valid `staging_id` are accepted.  Entries that have already been
      resolved or replayed are rejected.

    - `ambiguous_items` — replay from AmbiguousOutcomeQueue entries.  Only
      entries with `status = retry_eligible` are accepted (meaning query-before-retry
      confirmed the downstream did not process the request).

    **All-or-nothing validation:** if any item in `source_item_ids` is ineligible,
    the entire request is rejected with a descriptive error.  No partial replay
    requests are created.

    Creating a replay request does not execute it.  Use
    `POST /replays/{id}/execute` to run the replay.  This two-step design lets
    the operator inspect the item list before committing to execution.
    """
    try:
        result = await replay_service.create_replay_request(
            session=db,
            selection_mode=request.selection_mode,
            reason=request.reason,
            requested_by=request.requested_by,
            source_item_ids=request.source_item_ids,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return ReplayRequestCreatedResponse(**result)


@router.post(
    "/replays/{replay_id}/execute",
    response_model=ExecuteReplayResponse,
    summary="Execute a pending replay request",
)
async def execute_replay(
    replay_id: int,
    db: AsyncSession = Depends(get_db),
) -> ExecuteReplayResponse:
    """
    Execute all pending items in a replay request.

    Each item is processed by calling `simulate_delivery()` — the existing
    canonical delivery path.  Replay is therefore subject to the same behavior
    as any other delivery call:

    - The circuit breaker is consulted before each attempt.
    - DeliveryAttemptLog rows are written for every attempt made.
    - If delivery fails again, a new DLQ entry is created.
    - If a timeout occurs, a new AmbiguousOutcomeQueue entry is created.
    - If the circuit is open, the item outcome is `circuit_open` with no attempt made.

    After execution, use `GET /replays/{id}` to inspect per-item outcomes and
    execution logs.
    """
    try:
        result = await replay_service.execute_replay(
            session=db, replay_request_id=replay_id
        )
    except ReplayStateError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    return ExecuteReplayResponse(**result)
