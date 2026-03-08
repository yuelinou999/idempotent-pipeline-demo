"""
reconciliation_service — audit and consistency reporting.

The reconciliation report answers the auditor's question:
  "How do you prove no data was lost and no data was written twice?"

Raw internal decisions stored in ingestion_attempt_log are mapped to three report families:
  accept_new, accept_supersede  → accepted
  skip_duplicate, skip_stale   → skipped
  quarantine                   → quarantined

Consistency invariant:
  accepted == unique business events written to staging
  skipped  == duplicate deliveries intercepted without side effects
"""

from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import DECISION_TO_REPORT_FAMILY
from app.models.correlation import ProductionProjection
from app.models.ingestion_log import IngestionAttemptLog
from app.models.staging import StagingRecord


async def get_reconciliation_report(
    session: AsyncSession,
    hours: int = 24,
) -> dict[str, Any]:
    """
    Generate a reconciliation report for the last N hours.

    Returns counts grouped by report family (accepted/skipped/quarantined),
    broken down by source system, plus a consistency check.
    """
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    # Raw counts by source_system + internal decision
    rows = await session.execute(
        select(
            IngestionAttemptLog.source_system,
            IngestionAttemptLog.decision,
            func.count().label("count"),
        )
        .where(IngestionAttemptLog.received_at >= since)
        .group_by(IngestionAttemptLog.source_system, IngestionAttemptLog.decision)
        .order_by(IngestionAttemptLog.source_system, IngestionAttemptLog.decision)
    )

    # Aggregate into report families
    breakdown: dict[str, dict[str, int]] = {}
    for source_system, decision, count in rows:
        family = DECISION_TO_REPORT_FAMILY.get(decision, "unknown")
        sys_counts = breakdown.setdefault(source_system, {})
        sys_counts[family] = sys_counts.get(family, 0) + count

    # Totals across all sources
    totals: dict[str, int] = {}
    for sys_counts in breakdown.values():
        for family, count in sys_counts.items():
            totals[family] = totals.get(family, 0) + count

    accepted = totals.get("accepted", 0)
    skipped = totals.get("skipped", 0)
    quarantined = totals.get("quarantined", 0)

    # Production record count for consistency check
    production_count = await session.scalar(
        select(func.count()).select_from(ProductionProjection)
    ) or 0

    # Staging completeness counts
    staging_complete = await session.scalar(
        select(func.count()).select_from(StagingRecord).where(
            StagingRecord.completeness_status == "complete"
        )
    ) or 0
    staging_partial = await session.scalar(
        select(func.count()).select_from(StagingRecord).where(
            StagingRecord.completeness_status == "partial"
        )
    ) or 0

    is_consistent = production_count <= accepted

    return {
        "report_window_hours": hours,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "totals": {
            "accepted": accepted,
            "skipped": skipped,
            "quarantined": quarantined,
            "total_attempts": accepted + skipped + quarantined,
        },
        "by_source_system": breakdown,
        "staging": {
            "complete": staging_complete,
            "partial": staging_partial,
        },
        "production": {
            "record_count": production_count,
        },
        "consistency_check": {
            "is_consistent": is_consistent,
            "expected_max_production": accepted,
            "actual_production": production_count,
            "note": (
                "consistent" if is_consistent
                else f"ALERT: production count {production_count} exceeds expected max {accepted}"
            ),
        },
    }

