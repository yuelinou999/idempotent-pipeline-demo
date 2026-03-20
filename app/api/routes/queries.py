from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.models.correlation import CorrelationMap, DataAnomalyQueue
from app.models.idempotent_state import IdempotentState
from app.models.ingestion_log import IngestionAttemptLog
from app.models.staging import StagingRecord
from app.schemas.message import (
    CorrelationResponse,
    IdempotentStateResponse,
    ReviewQueueItemResponse,
    StagingRecordResponse,
)

router = APIRouter(tags=["queries"])


@router.get("/state/{entity_key:path}", response_model=IdempotentStateResponse)
async def get_state(
    entity_key: str,
    db: AsyncSession = Depends(get_db),
) -> IdempotentStateResponse:
    """
    Retrieve the current known state for a given entity_key.

    entity_key format: {source_system}:::{entity_type}:::{business_key}

    Returns the latest version stored, its payload hash, and the last decision applied.
    """
    state = await db.scalar(
        select(IdempotentState).where(IdempotentState.entity_key == entity_key)
    )
    if not state:
        raise HTTPException(status_code=404, detail=f"No state found for entity key: {entity_key}")

    return IdempotentStateResponse(
        entity_key=state.entity_key,
        idempotent_key=state.idempotent_key,
        source_system=state.source_system,
        entity_type=state.entity_type,
        business_key=state.business_key,
        current_version=state.current_version,
        payload_hash=state.payload_hash,
        last_decision=state.last_decision,
        first_seen_at=state.first_seen_at.isoformat(),
        last_updated_at=state.last_updated_at.isoformat(),
        active_staging_id=state.active_staging_id,
    )


@router.get("/attempts/{attempt_id}", response_model=dict[str, Any])
async def get_attempt(
    attempt_id: int,
    db: AsyncSession = Depends(get_db),
) -> dict[str, Any]:
    """Retrieve a specific ingestion attempt log entry by ID."""
    attempt = await db.get(IngestionAttemptLog, attempt_id)
    if not attempt:
        raise HTTPException(status_code=404, detail=f"Attempt {attempt_id} not found")
    return {
        "id": attempt.id,
        "entity_key": attempt.entity_key,
        "idempotent_key": attempt.idempotent_key,
        "source_system": attempt.source_system,
        "entity_type": attempt.entity_type,
        "business_key": attempt.business_key,
        "incoming_version": attempt.incoming_version,
        "payload_hash": attempt.payload_hash,
        "decision": attempt.decision,
        "decision_reason": attempt.decision_reason,
        "correlation_id": attempt.correlation_id,
        "received_at": attempt.received_at.isoformat(),
    }


@router.get("/correlations/{correlation_id}", response_model=CorrelationResponse)
async def get_correlation(
    correlation_id: str,
    db: AsyncSession = Depends(get_db),
) -> CorrelationResponse:
    """
    Retrieve all source system mappings associated with a given Correlation ID.
    This shows which records from different systems belong to the same business transaction.
    """
    mappings = await db.scalars(
        select(CorrelationMap).where(CorrelationMap.correlation_id == correlation_id)
    )
    mapping_list = list(mappings)
    if not mapping_list:
        raise HTTPException(status_code=404, detail=f"Correlation ID {correlation_id!r} not found")

    return CorrelationResponse(
        correlation_id=correlation_id,
        mappings=[
            {
                "source_system": m.source_system,
                "source_key": m.source_key,
                "mapping_method": m.mapping_method,
                "mapped_at": m.mapped_at.isoformat(),
            }
            for m in mapping_list
        ],
    )


@router.get("/staging", response_model=list[StagingRecordResponse])
async def list_staging(
    status: str | None = Query(None, description="Filter by completeness status"),
    source_system: str | None = Query(None),
    limit: int = Query(50, le=500),
    db: AsyncSession = Depends(get_db),
) -> list[StagingRecordResponse]:
    """List staging records, optionally filtered by status or source system."""
    query = select(StagingRecord).order_by(StagingRecord.staged_at.desc()).limit(limit)
    if status:
        query = query.where(StagingRecord.completeness_status == status)
    if source_system:
        query = query.where(StagingRecord.source_system == source_system)

    records = await db.scalars(query)
    return [
        StagingRecordResponse(
            id=r.id,
            idempotent_key=r.idempotent_key,
            source_system=r.source_system,
            entity_type=r.entity_type,
            business_key=r.business_key,
            version=r.version,
            completeness_status=r.completeness_status,
            correlation_id=r.correlation_id,
            staged_at=r.staged_at.isoformat(),
        )
        for r in records
    ]


@router.get(
    "/dlq/data-anomaly",
    response_model=list[ReviewQueueItemResponse],
    summary="List data anomaly queue entries (quarantined records)",
    tags=["operator console"],
)
async def list_data_anomaly_queue(
    resolved: bool = Query(False, description="Include resolved items"),
    limit: int = Query(50, le=500),
    db: AsyncSession = Depends(get_db),
) -> list[ReviewQueueItemResponse]:
    """
    List entries in the data anomaly queue.

    Records appear here when the same idempotent key and version arrive with a
    different payload hash — the upstream re-emitted without incrementing the version.
    This is a data quality problem requiring investigation at the source system.

    Distinct from GET /dlq/delivery, which lists downstream delivery failures.
    """
    query = (
        select(DataAnomalyQueue)
        .order_by(DataAnomalyQueue.queued_at.desc())
        .limit(limit)
    )
    if not resolved:
        query = query.where(DataAnomalyQueue.resolved == False)  # noqa: E712

    items = await db.scalars(query)
    return [
        ReviewQueueItemResponse(
            id=i.id,
            idempotent_key=i.idempotent_key,
            source_system=i.source_system,
            entity_type=i.entity_type,
            business_key=i.business_key,
            version=i.version,
            reason=i.reason,
            reason_detail=i.reason_detail,
            resolved=i.resolved,
            queued_at=i.queued_at.isoformat(),
        )
        for i in items
    ]


@router.get(
    "/review-queue",
    response_model=list[ReviewQueueItemResponse],
    include_in_schema=False,  # hidden from docs — use /dlq/data-anomaly
)
async def list_review_queue_legacy(
    resolved: bool = Query(False),
    limit: int = Query(50, le=500),
    db: AsyncSession = Depends(get_db),
) -> list[ReviewQueueItemResponse]:
    """Backward-compatible alias for /dlq/data-anomaly. Will be removed in a future release."""
    return await list_data_anomaly_queue(resolved=resolved, limit=limit, db=db)
