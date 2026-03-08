"""
completeness_engine — manages the PARTIAL → COMPLETE state machine for staged records.

For the MVP, the completeness rule for an order transaction is:
  COMPLETE when a 'header' fragment AND at least one 'detail' or 'financial'
  fragment have been received under the same Correlation ID.

State transitions:
  PARTIAL    → PARTIAL    (more fragments received, still incomplete)
  PARTIAL    → COMPLETE   (all required fragments now present)
  COMPLETE   → SUPERSEDED (a newer complete version arrived)
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.staging import StagingRecord

# Entity types that count as "header" fragments
HEADER_ENTITY_TYPES = {"order", "order_header"}

# Entity types that count as "detail or financial" fragments
DETAIL_ENTITY_TYPES = {"order_line", "order_detail", "invoice", "payment", "picking_ticket"}


async def evaluate_completeness(
    session: AsyncSession,
    correlation_id: str,
) -> str:
    """
    Evaluate completeness for all active staging records under a given Correlation ID.

    Returns the new completeness status: 'partial' or 'complete'.
    Also updates the staging records in place.
    """
    if not correlation_id:
        return "partial"

    active_records = await session.scalars(
        select(StagingRecord).where(
            StagingRecord.correlation_id == correlation_id,
            StagingRecord.completeness_status.in_(["partial", "complete"]),
        )
    )
    records = list(active_records)

    if not records:
        return "partial"

    entity_types = {r.entity_type for r in records}

    has_header = bool(entity_types & HEADER_ENTITY_TYPES)
    has_detail_or_financial = bool(entity_types & DETAIL_ENTITY_TYPES)

    if has_header and has_detail_or_financial:
        # Promote all partial records under this correlation to complete
        for record in records:
            if record.completeness_status == "partial":
                record.completeness_status = "complete"
        return "complete"

    return "partial"


async def mark_superseded(
    session: AsyncSession,
    old_staging_id: int,
    new_staging_id: int,
) -> None:
    """Mark an existing staging record as superseded by a newer one."""
    from datetime import datetime, timezone

    old = await session.get(StagingRecord, old_staging_id)
    if old:
        old.completeness_status = "superseded"
        old.superseded_at = datetime.now(timezone.utc)
        old.superseded_by_id = new_staging_id
