from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.services.compaction_service import compact_staging
from app.services.materialization_service import materialize_complete_records
from app.services.reconciliation_service import get_reconciliation_report

router = APIRouter(tags=["jobs & reports"])


@router.get("/reports/reconciliation")
async def reconciliation_report(
    hours: int = Query(24, ge=1, le=720, description="Lookback window in hours"),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Generate a reconciliation report for the last N hours.

    The report answers: "Can we prove no data was lost and no data was written twice?"

    Consistency invariant:
      accepted + superseded == unique business events processed
      skipped == duplicate deliveries intercepted
    """
    return await get_reconciliation_report(db, hours=hours)


@router.post("/jobs/materialize")
async def run_materialization(db: AsyncSession = Depends(get_db)) -> dict:
    """
    Promote COMPLETE staging records to the production projection.

    This job is idempotent — running it multiple times produces the same result.
    """
    return await materialize_complete_records(db)


@router.post("/jobs/compact")
async def run_compaction(
    retention_days: int = Query(30, ge=1, le=365),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    Archive superseded and promoted staging records older than retention_days.

    Rule: records are never deleted, only migrated to 'archived' status.
    This keeps the active staging table lean without destroying data lineage.
    """
    return await compact_staging(db, retention_days=retention_days)
