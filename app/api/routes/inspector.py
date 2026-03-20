"""
Entity / message inspector operator console endpoints.

Provides end-to-end traceability for individual records and business transactions.
Each route assembles a complete inspection view from existing models without
modifying any records.

These routes back the "Inspector" view in the operator console — the view that
answers:
  - What happened to this specific record from ingestion through delivery?
  - Did it hit any anomaly queue, DLQ, or ambiguous outcome?
  - Was it ever replayed, and what did the replay produce?
  - Which entities belong to the same business transaction?

Routes:
  GET /inspect/by-idempotent-key/{idempotent_key}  — inspect by versioned record key
  GET /inspect/by-entity-key/{entity_key}           — inspect by version-less entity key
  GET /inspect/by-correlation-id/{correlation_id}   — inspect all entities in a transaction
"""

from fastapi import APIRouter, Depends, HTTPException

from app.core.database import get_db
from app.schemas.inspector import (
    CorrelationInspectionView,
    EntityInspectionView,
)
from app.services import inspector_service
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(prefix="/inspect", tags=["operator console"])


@router.get(
    "/by-idempotent-key/{idempotent_key:path}",
    response_model=EntityInspectionView,
    summary="Inspect a record by its versioned idempotent key",
)
async def inspect_by_idempotent_key(
    idempotent_key: str,
    db: AsyncSession = Depends(get_db),
) -> EntityInspectionView:
    """
    Return the complete inspection view for the entity identified by the given
    idempotent key.

    The idempotent key is versioned (`{source}:::{type}:::{key}:::{version}`).
    The inspector automatically resolves this to the entity's full history,
    including all other versions that have been ingested for the same entity.

    Use this route when you have the specific versioned key from an ingestion
    attempt log, a replay item, or an ambiguous queue entry.
    """
    result = await inspector_service.inspect_by_idempotent_key(db, idempotent_key)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"No records found for idempotent_key {idempotent_key!r}.",
        )
    return EntityInspectionView(**result)


@router.get(
    "/by-entity-key/{entity_key:path}",
    response_model=EntityInspectionView,
    summary="Inspect an entity by its version-less entity key",
)
async def inspect_by_entity_key(
    entity_key: str,
    db: AsyncSession = Depends(get_db),
) -> EntityInspectionView:
    """
    Return the complete inspection view for the entity identified by the given
    entity key.

    The entity key is version-less (`{source}:::{type}:::{key}`).  It identifies
    the entity across all versions.  Use this route when you know the entity's
    identity but are interested in its full cross-version history.
    """
    result = await inspector_service.inspect_by_entity_key(db, entity_key)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"No records found for entity_key {entity_key!r}.",
        )
    return EntityInspectionView(**result)


@router.get(
    "/by-correlation-id/{correlation_id}",
    response_model=CorrelationInspectionView,
    summary="Inspect all entities in a business transaction",
)
async def inspect_by_correlation_id(
    correlation_id: str,
    db: AsyncSession = Depends(get_db),
) -> CorrelationInspectionView:
    """
    Return inspection views for all entities that share the given correlation ID.

    A business transaction may involve records from multiple source systems.
    This route assembles one EntityInspectionView per entity in the transaction,
    making it possible to see the full cross-system picture for one business event.

    Use this route when investigating a specific business transaction — for example,
    when a downstream integration failed and you need to understand which records
    were affected across all systems involved in that transaction.

    Returns a 404 if no records are associated with this correlation_id.
    """
    results = await inspector_service.inspect_by_correlation_id(db, correlation_id)
    if not results:
        raise HTTPException(
            status_code=404,
            detail=f"No records found for correlation_id {correlation_id!r}.",
        )
    return CorrelationInspectionView(
        correlation_id=correlation_id,
        entity_count=len(results),
        entities=[EntityInspectionView(**r) for r in results],
    )
