"""
seed — idempotent startup seeding for demo / development data.

Populates DownstreamSystem rows on first boot and on every restart.
Existing rows are never overwritten, so manual changes made via the
operator console survive restarts.

Seeded systems cover all Phase 2 scenario paths:

  warehouse_sync      mock_behavior=success
                      → happy path; delivery succeeds on first attempt

  admissions_crm      mock_behavior=always_transient_fail  (max 3 retries)
                      → retries exhaust → DeliveryDLQ: transient_exhausted
                      (Scenario 11)

  finance_ledger      mock_behavior=non_retryable_400  (max 5 retries)
                      → first attempt 400 → immediate DLQ: non_retryable
                      (Scenario 12)

  timeout_with_query  mock_behavior=timeout
                      mock_status_query_behavior=not_found
                      supports_status_query=True
                      → timeout → AmbiguousOutcomeQueue
                      → query returns not_found → retry_eligible
                      (Scenarios 14, 15)

  timeout_no_query    mock_behavior=timeout
                      mock_status_query_behavior=status_unavailable
                      supports_status_query=False
                      → timeout → AmbiguousOutcomeQueue
                      → query unavailable → status_unavailable
                      (Scenario 16)
"""

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.delivery import DownstreamSystem

_DOWNSTREAM_SYSTEMS = [
    {
        "name": "warehouse_sync",
        "description": "Document and asset warehouse synchronisation service",
        "baseline_error_rate_pct": 1.0,
        "circuit_threshold_pct": 15.0,
        "max_retry_attempts": 3,
        "supports_status_query": True,
        "mock_behavior": "success",
        "mock_status_query_behavior": "not_found",
        "open_after_n_failures": 3,
    },
    {
        "name": "admissions_crm",
        "description": "Core CRM for student admissions data synchronisation",
        "baseline_error_rate_pct": 0.5,
        "circuit_threshold_pct": 15.0,
        "max_retry_attempts": 3,
        "supports_status_query": True,
        "mock_behavior": "always_transient_fail",
        "mock_status_query_behavior": "not_found",
        "open_after_n_failures": 3,
    },
    {
        "name": "finance_ledger",
        "description": "Financial ledger for tuition and fee records",
        "baseline_error_rate_pct": 2.0,
        "circuit_threshold_pct": 20.0,
        "max_retry_attempts": 5,
        "supports_status_query": False,
        "mock_behavior": "non_retryable_400",
        "mock_status_query_behavior": "status_unavailable",
        "open_after_n_failures": 3,
    },
    {
        "name": "timeout_with_query",
        "description": "Integration endpoint that times out; supports status query",
        "baseline_error_rate_pct": 2.0,
        "circuit_threshold_pct": 20.0,
        "max_retry_attempts": 1,
        "supports_status_query": True,
        "mock_behavior": "timeout",
        "mock_status_query_behavior": "not_found",
        "open_after_n_failures": 5,
    },
    {
        "name": "timeout_no_query",
        "description": "Legacy system that times out; does not support status query",
        "baseline_error_rate_pct": 3.0,
        "circuit_threshold_pct": 20.0,
        "max_retry_attempts": 1,
        "supports_status_query": False,
        "mock_behavior": "timeout",
        "mock_status_query_behavior": "status_unavailable",
        "open_after_n_failures": 5,
    },
]


async def seed_downstream_systems(session: AsyncSession) -> None:
    """
    Insert DownstreamSystem rows that do not yet exist.

    Idempotent: safe to call on every application startup.
    Existing rows are not modified.
    """
    for data in _DOWNSTREAM_SYSTEMS:
        existing = await session.scalar(
            select(DownstreamSystem).where(DownstreamSystem.name == data["name"])
        )
        if existing is None:
            session.add(DownstreamSystem(**data))

    await session.commit()
