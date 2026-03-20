"""
delivery_service — downstream delivery simulation, circuit checking, and DLQ management.

This module belongs to the Delivery Resilience context (Context 2).
DeliveryAttemptLog and DeliveryDLQ are distinct from IngestionAttemptLog and
DataAnomalyQueue, which belong to the Ingestion Correctness context (Context 1).

Execution sequence for simulate_delivery():
  1. Load DownstreamSystem config and resolve the linked StagingRecord (if any)
  2. Check circuit state — if OPEN, return circuit_open immediately (no attempts made)
  3. For each attempt (1 … max_retry_attempts):
     a. Derive a mock HTTP response
     b. Write one DeliveryAttemptLog row (flushed, not committed)
     c. Update circuit breaker state (flushed, not committed)
     d. If HTTP 200  → commit, return success
     e. If timeout   → create AmbiguousOutcomeQueue entry, commit,
                       return queued_ambiguous
     f. Run retry_classifier → RetryDecision
     g. If SEND_TO_DLQ → write DeliveryDLQ entry, commit, return
     h. If RETRY_WITH_BACKOFF → continue loop
  4. Session commit happens at the end of each terminal branch.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import (
    AmbiguityCategory,
    DeliveryClassification,
    DeliveryOutcome,
    DLQFailureCategory,
    DLQResolutionStatus,
    RetryDecision,
)
from app.models.ambiguous import AmbiguousOutcomeQueue
from app.models.delivery import DeliveryAttemptLog, DeliveryDLQ, DownstreamSystem
from app.models.staging import StagingRecord
from app.services import circuit_breaker_service as cb_svc
from app.services.retry_classifier import DownstreamError, classify_for_retry


@dataclass(frozen=True)
class _MockResponse:
    """Internal representation of a simulated downstream HTTP response."""
    http_status: int | None
    is_timeout: bool
    body_snippet: str


def _get_mock_response(mock_behavior: str, attempt_number: int) -> _MockResponse:
    """
    Return a deterministic mock response from a mock_behavior directive.

    attempt_number is included for future behaviours that vary per attempt,
    but current behaviours are stateless across attempts.
    """
    if mock_behavior == "success":
        return _MockResponse(200, False, "OK")
    if mock_behavior == "always_transient_fail":
        return _MockResponse(503, False, "Service Unavailable")
    if mock_behavior == "non_retryable_400":
        return _MockResponse(400, False, "Bad Request: payload failed downstream schema validation")
    if mock_behavior == "conflict_409":
        return _MockResponse(409, False, "Conflict: business rule violation on target record")
    if mock_behavior == "timeout":
        return _MockResponse(None, True, "")
    # Unrecognised mock_behavior defaults to success — non-destructive fallback.
    return _MockResponse(200, False, "OK (default fallback)")


def _classify_attempt(
    response: _MockResponse,
) -> tuple[DeliveryClassification, DeliveryOutcome]:
    """Derive the classification and outcome for a single attempt response."""
    if response.http_status == 200:
        return DeliveryClassification.RETRYABLE, DeliveryOutcome.SUCCESS
    if response.is_timeout:
        return DeliveryClassification.UNKNOWN, DeliveryOutcome.TIMEOUT
    if response.http_status in (400, 422, 409):
        return DeliveryClassification.NON_RETRYABLE, DeliveryOutcome.FAILED
    # 5xx and other non-200 statuses are treated as retryable failures.
    return DeliveryClassification.RETRYABLE, DeliveryOutcome.FAILED


async def simulate_delivery(
    session: AsyncSession,
    staging_id: int,
    system_name: str,
) -> dict[str, Any]:
    """
    Simulate delivery of a staging record to a named downstream system.

    Consults the circuit breaker before attempting delivery.  If the circuit
    is OPEN, returns immediately without making any attempts.  Otherwise runs
    the retry loop, updating circuit state on each attempt outcome.

    Returns a summary dict:
      system_name   — downstream system name
      outcome       — "success" | "dlq_transient_exhausted" | "dlq_non_retryable"
                      | "queued_ambiguous" | "circuit_open"
      attempts_made — total DeliveryAttemptLog rows written
      dlq_entry_id  — DeliveryDLQ.id if a DLQ entry was created, else None
      ambiguous_id  — AmbiguousOutcomeQueue.id if an ambiguous entry was created
      circuit_state — final state of the circuit after this call

    Raises ValueError if system_name does not exist in DownstreamSystem.
    """
    system: DownstreamSystem | None = await session.scalar(
        select(DownstreamSystem).where(DownstreamSystem.name == system_name)
    )
    if system is None:
        raise ValueError(
            f"Unknown downstream system: {system_name!r}. "
            "Seed data must be loaded before calling simulate_delivery."
        )

    # Resolve the StagingRecord once to populate idempotent_key and correlation_id
    # on any AmbiguousOutcomeQueue entry created during this call.
    staging: StagingRecord | None = None
    if staging_id is not None:
        staging = await session.get(StagingRecord, staging_id)

    # --- Circuit check: block immediately if OPEN ---
    allowed = await cb_svc.is_allowed(session, system_name)
    if not allowed:
        await session.commit()
        return {
            "system_name": system_name,
            "outcome": "circuit_open",
            "attempts_made": 0,
            "dlq_entry_id": None,
            "ambiguous_id": None,
            "circuit_state": "open",
        }

    last_attempt_at = datetime.now(timezone.utc)

    for attempt in range(1, system.max_retry_attempts + 1):
        last_attempt_at = datetime.now(timezone.utc)
        response = _get_mock_response(system.mock_behavior, attempt)
        classification, outcome = _classify_attempt(response)

        log_entry = DeliveryAttemptLog(
            staging_id=staging_id,
            system_name=system_name,
            attempt_number=attempt,
            http_status=response.http_status,
            classification=classification.value,
            outcome=outcome.value,
            response_body_snippet=response.body_snippet or None,
        )
        session.add(log_entry)
        await session.flush()

        # --- Terminal: success ---
        if outcome == DeliveryOutcome.SUCCESS:
            cb_state = await cb_svc.record_success(session, system_name)
            await session.commit()
            return {
                "system_name": system_name,
                "outcome": "success",
                "attempts_made": attempt,
                "dlq_entry_id": None,
                "ambiguous_id": None,
                "circuit_state": cb_state.state,
            }

        # Non-success: record failure to circuit breaker
        cb_state = await cb_svc.record_failure(
            session, system_name, system.open_after_n_failures
        )

        # --- Terminal: timeout — create AmbiguousOutcomeQueue entry ---
        if outcome == DeliveryOutcome.TIMEOUT:
            ambiguous = AmbiguousOutcomeQueue(
                delivery_attempt_log_id=log_entry.id,
                downstream_system_id=system.id,
                staging_id=staging_id,
                system_name=system_name,
                idempotent_key=staging.idempotent_key if staging else None,
                correlation_id=staging.correlation_id if staging else None,
                ambiguity_category=AmbiguityCategory.TIMEOUT.value,
                status="pending",
            )
            session.add(ambiguous)
            await session.flush()
            await session.commit()
            return {
                "system_name": system_name,
                "outcome": "queued_ambiguous",
                "attempts_made": attempt,
                "dlq_entry_id": None,
                "ambiguous_id": ambiguous.id,
                "circuit_state": cb_state.state,
            }

        # --- Classify the failure and decide next action ---
        error = DownstreamError(
            http_status=response.http_status,
            is_timeout=response.is_timeout,
            attempt_number=attempt,
            max_attempts=system.max_retry_attempts,
        )
        retry_decision = classify_for_retry(error)

        if retry_decision == RetryDecision.SEND_TO_DLQ:
            if response.http_status in (400, 422, 409):
                failure_category = DLQFailureCategory.NON_RETRYABLE
            else:
                failure_category = DLQFailureCategory.TRANSIENT_EXHAUSTED

            dlq_entry = DeliveryDLQ(
                staging_id=staging_id,
                system_name=system_name,
                failure_category=failure_category.value,
                last_attempt_at=last_attempt_at,
                resolution_status=DLQResolutionStatus.PENDING.value,
            )
            session.add(dlq_entry)
            await session.flush()
            await session.commit()
            return {
                "system_name": system_name,
                "outcome": f"dlq_{failure_category.value}",
                "attempts_made": attempt,
                "dlq_entry_id": dlq_entry.id,
                "ambiguous_id": None,
                "circuit_state": cb_state.state,
            }

        # RetryDecision.RETRY_WITH_BACKOFF — continue to next attempt.
        # In production this sleeps with exponential backoff.
        # In the demo we loop immediately.

    # Safety guard: classify_for_retry returns SEND_TO_DLQ when
    # attempt_number >= max_attempts, so this branch is unreachable in
    # normal operation.
    cb_state = await cb_svc.get_state(session, system_name)
    dlq_entry = DeliveryDLQ(
        staging_id=staging_id,
        system_name=system_name,
        failure_category=DLQFailureCategory.TRANSIENT_EXHAUSTED.value,
        last_attempt_at=last_attempt_at,
        resolution_status=DLQResolutionStatus.PENDING.value,
    )
    session.add(dlq_entry)
    await session.flush()
    await session.commit()
    return {
        "system_name": system_name,
        "outcome": f"dlq_{DLQFailureCategory.TRANSIENT_EXHAUSTED.value}",
        "attempts_made": system.max_retry_attempts,
        "dlq_entry_id": dlq_entry.id,
        "ambiguous_id": None,
        "circuit_state": cb_state.state if cb_state else "unknown",
    }


async def resolve_dlq_entry(
    session: AsyncSession,
    dlq_id: int,
    resolved_by: str,
) -> dict[str, Any]:
    """
    Record an operator resolution decision on a DeliveryDLQ entry.

    Transitions the entry from 'pending' to 'resolved'.  This path is used
    when the operator has confirmed the record does not require delivery —
    either because it was handled through a separate channel or is safe to
    discard after investigation.

    This action is permanent and audited: resolved_by and resolved_at are
    written and cannot be changed.

    Raises ValueError if the entry does not exist or is not in 'pending' status.
    """
    entry: DeliveryDLQ | None = await session.get(DeliveryDLQ, dlq_id)
    if entry is None:
        raise ValueError(f"DeliveryDLQ entry {dlq_id} not found.")
    if entry.resolution_status != DLQResolutionStatus.PENDING.value:
        raise ValueError(
            f"DeliveryDLQ entry {dlq_id} is already in status "
            f"{entry.resolution_status!r} and cannot be resolved again."
        )

    entry.resolution_status = DLQResolutionStatus.RESOLVED.value
    entry.resolved_by = resolved_by
    entry.resolved_at = datetime.now(timezone.utc)
    await session.commit()

    return {
        "id": entry.id,
        "staging_id": entry.staging_id,
        "system_name": entry.system_name,
        "failure_category": entry.failure_category,
        "resolution_status": entry.resolution_status,
        "resolved_by": entry.resolved_by,
        "resolved_at": entry.resolved_at.isoformat(),
    }


async def get_delivery_outcomes(
    session: AsyncSession,
    hours: int = 24,
) -> dict[str, Any]:
    """
    Aggregate delivery attempt data for the operator console health view.

    Returns:
      delivery_attempts_by_system — attempt counts grouped by system,
          classification, and outcome for the last N hours.
      dlq_pending_by_system — current pending DLQ depth per system,
          grouped by failure category.
    """
    since = datetime.now(timezone.utc) - timedelta(hours=hours)

    rows = await session.execute(
        select(
            DeliveryAttemptLog.system_name,
            DeliveryAttemptLog.classification,
            DeliveryAttemptLog.outcome,
            func.count().label("count"),
        )
        .where(DeliveryAttemptLog.attempted_at >= since)
        .group_by(
            DeliveryAttemptLog.system_name,
            DeliveryAttemptLog.classification,
            DeliveryAttemptLog.outcome,
        )
        .order_by(DeliveryAttemptLog.system_name)
    )

    breakdown: dict[str, list[dict]] = {}
    for system_name, classification, outcome, count in rows:
        breakdown.setdefault(system_name, []).append(
            {"classification": classification, "outcome": outcome, "count": count}
        )

    dlq_rows = await session.execute(
        select(
            DeliveryDLQ.system_name,
            DeliveryDLQ.failure_category,
            func.count().label("count"),
        )
        .where(DeliveryDLQ.resolution_status == DLQResolutionStatus.PENDING.value)
        .group_by(DeliveryDLQ.system_name, DeliveryDLQ.failure_category)
    )

    dlq_pending: dict[str, list[dict]] = {}
    for system_name, category, count in dlq_rows:
        dlq_pending.setdefault(system_name, []).append(
            {"failure_category": category, "pending_count": count}
        )

    return {
        "report_window_hours": hours,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "delivery_attempts_by_system": breakdown,
        "dlq_pending_by_system": dlq_pending,
    }
