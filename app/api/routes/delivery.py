"""
Delivery Resilience operator console endpoints — Context 2.

This module provides the query surface for delivery health visibility and
operator triage of delivery failures.  It backs the following operator
console views:

  Delivery failure queue   GET  /dlq/delivery
                           POST /dlq/delivery/{id}/resolve
  Delivery health report   GET  /reports/delivery-outcomes
  Delivery attempt trigger POST /delivery/simulate

The simulate endpoint exists to drive demo scenarios that populate the DLQ and
attempt log.  In a production integration, this surface would instead receive
real delivery callbacks or be fed by a background worker.

Context boundary: these routes operate on DeliveryAttemptLog and DeliveryDLQ.
They have no access to IngestionAttemptLog or DataAnomalyQueue, which belong
to the Ingestion Correctness context (Context 1).
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.delivery import DeliveryDLQ
from app.schemas.delivery import (
    DeliveryDLQItemResponse,
    DeliveryOutcomesReport,
    ResolveDLQRequest,
    ResolveDLQResponse,
    SimulateDeliveryRequest,
    SimulateDeliveryResponse,
)
from app.services.delivery_service import (
    get_delivery_outcomes,
    resolve_dlq_entry,
    simulate_delivery,
)

router = APIRouter(tags=["operator console"])


@router.post(
    "/delivery/simulate",
    response_model=SimulateDeliveryResponse,
    summary="Trigger a delivery attempt for a staging record",
)
async def simulate_delivery_endpoint(
    request: SimulateDeliveryRequest,
    db: AsyncSession = Depends(get_db),
) -> SimulateDeliveryResponse:
    """
    Trigger a mock delivery sequence for a staging record to a named downstream system.

    The system's mock_behavior configuration determines the outcome.  Each attempt
    is recorded as a DeliveryAttemptLog row.  Records that cannot be delivered
    within the retry ceiling are routed to the delivery DLQ.

    This endpoint is used to populate the delivery queue views with realistic
    data for demo and operator console validation.

    **Possible outcomes:**
    - `success` — delivered on the first or a retry attempt
    - `dlq_transient_exhausted` — all retry attempts failed with transient errors
    - `dlq_non_retryable` — first attempt returned HTTP 400, 409, or 422
    - `queued_ambiguous` — timeout; outcome unknown; entry created in
      AmbiguousOutcomeQueue; use `POST /dlq/ambiguous/{id}/query-status` to investigate
    - `circuit_open` — circuit breaker is open; no attempts made
    """
    try:
        result = await simulate_delivery(
            session=db,
            staging_id=request.staging_id,
            system_name=request.system_name,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    return SimulateDeliveryResponse(**result)


@router.get(
    "/dlq/delivery",
    response_model=list[DeliveryDLQItemResponse],
    summary="Inspect the delivery dead-letter queue",
)
async def list_delivery_dlq(
    system_name: str | None = Query(None, description="Filter by downstream system name"),
    failure_category: str | None = Query(None, description="Filter by failure category"),
    resolution_status: str = Query(
        "pending",
        description="Resolution lifecycle filter: pending | resolved | manually_replayed",
    ),
    limit: int = Query(50, le=500),
    db: AsyncSession = Depends(get_db),
) -> list[DeliveryDLQItemResponse]:
    """
    List delivery DLQ entries for operator triage.

    Records appear here after delivery automation has reached a terminal failure:
    - `transient_exhausted` — retry ceiling was reached without a successful delivery
    - `non_retryable` — first attempt returned a non-retryable HTTP error (400 / 409 / 422)

    The `query_unavailable_escalated` category is reserved for escalations from
    AmbiguousOutcomeQueue when query-before-retry cannot resolve an outcome.

    Defaults to showing only `pending` entries — the active operator workload.
    Set `resolution_status=resolved` to review already-handled cases.

    Distinct from `GET /dlq/data-anomaly`, which surfaces data quality problems
    in the ingestion write path rather than delivery failures.
    """
    query = (
        select(DeliveryDLQ)
        .where(DeliveryDLQ.resolution_status == resolution_status)
        .order_by(DeliveryDLQ.queued_at.desc())
        .limit(limit)
    )
    if system_name:
        query = query.where(DeliveryDLQ.system_name == system_name)
    if failure_category:
        query = query.where(DeliveryDLQ.failure_category == failure_category)

    entries = await db.scalars(query)
    return [
        DeliveryDLQItemResponse(
            id=e.id,
            staging_id=e.staging_id,
            system_name=e.system_name,
            failure_category=e.failure_category,
            last_attempt_at=e.last_attempt_at.isoformat(),
            resolution_status=e.resolution_status,
            resolved_by=e.resolved_by,
            resolved_at=e.resolved_at.isoformat() if e.resolved_at else None,
            queued_at=e.queued_at.isoformat(),
        )
        for e in entries
    ]


@router.post(
    "/dlq/delivery/{dlq_id}/resolve",
    response_model=ResolveDLQResponse,
    summary="Record an operator resolution decision on a delivery failure",
)
async def resolve_delivery_dlq_entry(
    dlq_id: int,
    request: ResolveDLQRequest,
    db: AsyncSession = Depends(get_db),
) -> ResolveDLQResponse:
    """
    Mark a delivery DLQ entry as resolved and record who resolved it.

    Use this when the operator has determined that the record does not require
    replay — for example, because it was processed through a separate channel,
    because the downstream confirms it is not needed, or because it is safe to
    discard after investigation.

    This action is permanent and audited: `resolved_by` and `resolved_at` are
    written and cannot be changed.  Only entries in `pending` status can be resolved.
    """
    try:
        result = await resolve_dlq_entry(
            session=db,
            dlq_id=dlq_id,
            resolved_by=request.resolved_by,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc))

    return ResolveDLQResponse(**result)


@router.get(
    "/reports/delivery-outcomes",
    response_model=DeliveryOutcomesReport,
    summary="Delivery health overview across all downstream systems",
)
async def delivery_outcomes_report(
    hours: int = Query(24, ge=1, le=720, description="Lookback window in hours"),
    db: AsyncSession = Depends(get_db),
) -> DeliveryOutcomesReport:
    """
    Aggregated delivery health data for the operator console system overview.

    Returns two views over the selected time window:

    **delivery_attempts_by_system** — attempt volume broken down by system,
    classification (retryable / non_retryable / unknown), and outcome
    (success / failed / timeout).  Use this to identify which downstream
    systems have elevated failure rates and whether failures are retryable
    infrastructure issues or non-retryable data problems.

    **dlq_pending_by_system** — current unresolved DLQ depth per system,
    grouped by failure category.  An empty entry for a system means its
    DLQ is clear.  Non-zero pending counts indicate records awaiting
    operator action.
    """
    report = await get_delivery_outcomes(session=db, hours=hours)
    return DeliveryOutcomesReport(**report)
