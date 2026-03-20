"""
Ambiguous outcome queue operator console endpoints.

Provides visibility into delivery attempts whose outcome is unknown — typically
because the downstream timed out — and the query-before-retry mechanism for
resolving them.

These routes back the "Ambiguous outcome" view in the operator console:
  - See which deliveries have unknown outcomes
  - Understand why each attempt is ambiguous (timeout, unknown response)
  - Run a status query against the downstream to determine what happened
  - See whether the record needs retry or can be closed without action

Routes:
  GET  /dlq/ambiguous              — list ambiguous outcome entries
  GET  /dlq/ambiguous/{id}         — detail for one entry
  POST /dlq/ambiguous/{id}/query-status — run query-before-retry
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.ambiguous import AmbiguousOutcomeQueue
from app.schemas.ambiguous import AmbiguousOutcomeItemResponse, QueryStatusResponse
from app.services.query_before_retry_service import run_status_query

router = APIRouter(tags=["operator console"])


def _to_response(entry: AmbiguousOutcomeQueue) -> AmbiguousOutcomeItemResponse:
    return AmbiguousOutcomeItemResponse(
        id=entry.id,
        system_name=entry.system_name,
        staging_id=entry.staging_id,
        idempotent_key=entry.idempotent_key,
        correlation_id=entry.correlation_id,
        ambiguity_category=entry.ambiguity_category,
        status=entry.status,
        resolution_note=entry.resolution_note,
        delivery_attempt_log_id=entry.delivery_attempt_log_id,
        downstream_system_id=entry.downstream_system_id,
        created_at=entry.created_at.isoformat(),
        updated_at=entry.updated_at.isoformat(),
    )


@router.get(
    "/dlq/ambiguous",
    response_model=list[AmbiguousOutcomeItemResponse],
    summary="List ambiguous outcome queue entries",
)
async def list_ambiguous_queue(
    status: str = Query(
        "pending",
        description=(
            "Filter by resolution status: "
            "pending | confirmed_delivered | retry_eligible | status_unavailable"
        ),
    ),
    system_name: str | None = Query(None, description="Filter by downstream system name"),
    limit: int = Query(50, le=500),
    db: AsyncSession = Depends(get_db),
) -> list[AmbiguousOutcomeItemResponse]:
    """
    List entries in the ambiguous outcome queue for operator investigation.

    Records appear here when a delivery attempt timed out and the pipeline
    cannot determine whether the downstream processed the request.

    **Defaults to `status=pending`** — entries that have not yet been
    investigated.  Use `POST /dlq/ambiguous/{id}/query-status` to trigger
    the query-before-retry check for a specific entry.

    Use this view to answer:
    - Which deliveries have an unknown outcome right now?
    - Which systems are generating ambiguous timeouts?
    - Have any entries been confirmed as delivered without needing a retry?
    """
    q = (
        select(AmbiguousOutcomeQueue)
        .where(AmbiguousOutcomeQueue.status == status)
        .order_by(AmbiguousOutcomeQueue.created_at.desc())
        .limit(limit)
    )
    if system_name:
        q = q.where(AmbiguousOutcomeQueue.system_name == system_name)

    rows = await db.scalars(q)
    return [_to_response(r) for r in rows]


@router.get(
    "/dlq/ambiguous/{ambiguous_id}",
    response_model=AmbiguousOutcomeItemResponse,
    summary="Detail for one ambiguous outcome entry",
)
async def get_ambiguous_entry(
    ambiguous_id: int,
    db: AsyncSession = Depends(get_db),
) -> AmbiguousOutcomeItemResponse:
    """
    Return the full detail for a single ambiguous outcome entry.

    Includes the linked delivery attempt ID, downstream system ID, and any
    idempotent/correlation context from the original staging record.  Use
    this view when investigating a specific uncertain delivery before
    deciding whether to run a status query.
    """
    entry = await db.get(AmbiguousOutcomeQueue, ambiguous_id)
    if entry is None:
        raise HTTPException(
            status_code=404,
            detail=f"AmbiguousOutcomeQueue entry {ambiguous_id} not found.",
        )
    return _to_response(entry)


@router.post(
    "/dlq/ambiguous/{ambiguous_id}/query-status",
    response_model=QueryStatusResponse,
    summary="Run query-before-retry for one ambiguous outcome entry",
)
async def query_ambiguous_status(
    ambiguous_id: int,
    db: AsyncSession = Depends(get_db),
) -> QueryStatusResponse:
    """
    Run a query-before-retry check for one ambiguous outcome entry.

    Queries the downstream system (via mock configuration in the demo) to
    determine what actually happened to the timed-out delivery attempt.

    **Possible outcomes:**

    - `confirmed_delivered` — The downstream confirms it processed the request.
      No retry is needed.  The entry is closed with status `confirmed_delivered`.

    - `not_found` — The downstream has no record of the request.  The entry is
      closed with status `retry_eligible`.  The operator may trigger a retry
      (replay support is deferred to a later phase).

    - `status_unavailable` — The downstream does not support status queries, or
      the query returned an inconclusive result.  The entry status becomes
      `status_unavailable` and requires a manual operator decision.

    Only entries in `pending` status can be queried.  Calling this on an
    already-resolved entry returns HTTP 404.
    """
    try:
        result = await run_status_query(session=db, ambiguous_id=ambiguous_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    await db.commit()
    return QueryStatusResponse(**result)
