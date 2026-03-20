"""
Dashboard operator console endpoint.

Provides the top-level pipeline health snapshot for the operator console.
This is the first screen an operator sees — a system-state overview that
surfaces what needs attention and links to the relevant drill-down surfaces.

Route:
  GET /dashboard/overview — full pipeline health snapshot
"""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas.dashboard import DashboardOverview
from app.services import dashboard_service

router = APIRouter(prefix="/dashboard", tags=["operator console"])


@router.get(
    "/overview",
    response_model=DashboardOverview,
    summary="Pipeline health overview — operator console home view",
)
async def get_overview(
    db: AsyncSession = Depends(get_db),
) -> DashboardOverview:
    """
    Return a full pipeline health snapshot suitable for the operator console home view.

    **Summary cards** show current queue depths and circuit health at a glance.
    The `attention_needed` flag is `true` when any queue has pending items or any
    circuit is not closed — useful as a top-level health indicator.

    **Circuit health** lists every downstream system that has an active circuit
    state row, with its current state and failure count.

    **Replay summary** shows replay request counts by lifecycle status.

    **Recent activity** surfaces the last 20 operator-relevant events across
    delivery failures, ambiguous outcomes, replay executions, circuit state
    changes, and data anomalies — all sorted most-recent-first.

    **Quick links** provide navigation shortcuts to each drill-down surface
    (delivery DLQ, ambiguous queue, circuits, replays, inspector).

    This endpoint is read-only and never modifies any records.
    """
    result = await dashboard_service.get_overview(db)
    return DashboardOverview(**result)
