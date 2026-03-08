"""
compaction_service — archives stale staging records to keep the active table lean.

Rule: never DELETE, only migrate.

Superseded and promoted records older than the retention window are moved to
an archive table (or flagged as archived). This keeps query performance high
on the active staging table while preserving full data lineage.

In the SQLite MVP, we mark records as 'archived' in place.
In a production Postgres deployment, these would be moved to a cold storage table
or exported to an object store (S3/GCS).
"""

from datetime import datetime, timedelta, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.staging import StagingRecord

DEFAULT_RETENTION_DAYS = 30


async def compact_staging(
    session: AsyncSession,
    retention_days: int = DEFAULT_RETENTION_DAYS,
) -> dict[str, int]:
    """
    Archive superseded and promoted staging records older than retention_days.

    Returns counts of archived records by previous status.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

    archivable = await session.scalars(
        select(StagingRecord).where(
            StagingRecord.completeness_status.in_(["superseded", "promoted"]),
            StagingRecord.staged_at < cutoff,
        )
    )
    records = list(archivable)

    superseded_archived = 0
    promoted_archived = 0

    for record in records:
        prev_status = record.completeness_status
        record.completeness_status = "archived"
        if prev_status == "superseded":
            superseded_archived += 1
        else:
            promoted_archived += 1

    await session.commit()

    return {
        "archived_superseded": superseded_archived,
        "archived_promoted": promoted_archived,
        "total_archived": superseded_archived + promoted_archived,
        "retention_days": retention_days,
        "cutoff_date": cutoff.isoformat(),
    }
