"""
materialization_service — promotes complete staging records to production.

The production layer contains only clean, deduplicated, fully validated data.
Nothing is written here directly by the ingestion path — only by this job.

Promotion semantics
-------------------
ProductionProjection uses entity_key as its unique identity (one row per entity).
When a staging record is promoted:

  - No existing row for entity_key → INSERT new projection row.
  - Existing row, incoming version > current version → UPDATE in place (overwrite).
  - Existing row, incoming version <= current version → SKIP (already have equal or newer).

This means multiple staging records for the same entity (e.g. v1 then v2) will
ultimately leave exactly one production row showing the latest materialized version.
"""

from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.correlation import ProductionProjection
from app.models.staging import StagingRecord
from app.services.key_builder import build_entity_key


async def materialize_complete_records(session: AsyncSession) -> dict[str, int]:
    """
    Promote all COMPLETE staging records to the production projection.

    Lookup is by entity_key (version-less), so newer versions overwrite older ones
    in the single production row for that entity.

    Returns a summary of how many records were promoted vs. skipped.
    """
    complete_records = await session.scalars(
        select(StagingRecord).where(
            StagingRecord.completeness_status == "complete"
        )
    )
    records = list(complete_records)

    promoted = 0
    skipped_already_current = 0

    for record in records:
        entity_key = build_entity_key(
            record.source_system, record.entity_type, record.business_key
        )

        # Look up the current production row for this entity (by entity_key, not by version)
        existing = await session.scalar(
            select(ProductionProjection).where(
                ProductionProjection.entity_key == entity_key
            )
        )

        if existing:
            if existing.version >= record.version:
                # Production already has an equal or newer version — nothing to do.
                skipped_already_current += 1
                continue

            # Incoming version is newer — overwrite the existing projection row in place.
            existing.idempotent_key = record.idempotent_key
            existing.version = record.version
            existing.payload = record.payload
            existing.correlation_id = record.correlation_id
            existing.source_staging_id = record.id
            existing.materialized_at = datetime.now(timezone.utc)
        else:
            # First time this entity reaches production — create the row.
            projection = ProductionProjection(
                entity_key=entity_key,
                idempotent_key=record.idempotent_key,
                source_system=record.source_system,
                entity_type=record.entity_type,
                business_key=record.business_key,
                version=record.version,
                payload=record.payload,
                correlation_id=record.correlation_id,
                source_staging_id=record.id,
            )
            session.add(projection)

        # Mark the staging record as promoted regardless of insert vs. update.
        record.completeness_status = "promoted"
        record.promoted_at = datetime.now(timezone.utc)
        promoted += 1

    await session.commit()

    return {
        "promoted": promoted,
        "skipped_already_current": skipped_already_current,
    }
