from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas.message import (
    IngestBatchRequest,
    IngestBatchResponse,
    IngestMessageRequest,
    IngestMessageResponse,
)
from app.services.ingest_service import ingest_message

router = APIRouter(prefix="/messages", tags=["ingestion"])


@router.post("/ingest", response_model=IngestMessageResponse, status_code=200)
async def ingest_single(
    request: IngestMessageRequest,
    db: AsyncSession = Depends(get_db),
) -> IngestMessageResponse:
    """
    Ingest a single message through the idempotent write path.

    The pipeline will:
    - Derive the idempotent key
    - Apply the five-branch deduplication decision
    - Write or skip accordingly
    - Return the outcome with full reasoning

    Possible outcomes: accepted, superseded, skipped, quarantined
    """
    result = await ingest_message(
        session=db,
        source_system=request.source_system,
        entity_type=request.entity_type,
        business_key=request.business_key,
        version=request.version,
        payload=request.payload,
    )
    return IngestMessageResponse(**result)


@router.post("/ingest-batch", response_model=IngestBatchResponse, status_code=200)
async def ingest_batch(
    request: IngestBatchRequest,
    db: AsyncSession = Depends(get_db),
) -> IngestBatchResponse:
    """
    Ingest a batch of messages. Each message is processed independently
    through the same idempotent write path as the single-message endpoint.

    This endpoint is idempotent: submitting the same batch twice produces
    the same final state (duplicates are skipped, not double-written).
    """
    results = []
    for msg in request.messages:
        result = await ingest_message(
            session=db,
            source_system=msg.source_system,
            entity_type=msg.entity_type,
            business_key=msg.business_key,
            version=msg.version,
            payload=msg.payload,
        )
        results.append(IngestMessageResponse(**result))

    return IngestBatchResponse(total=len(results), results=results)
