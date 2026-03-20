"""
circuit_breaker_service — per-downstream circuit breaker state management.

Implements the three-state circuit breaker pattern for the delivery resilience
control plane.  All functions flush but do NOT commit; the calling layer
(delivery_service or a route handler) is responsible for the final commit.

State machine:
  CLOSED    → OPEN      when consecutive_failure_count reaches system.open_after_n_failures
  OPEN      → HALF_OPEN via operator reset only (force_reset)
  HALF_OPEN → CLOSED    after HALF_OPEN_REQUIRED_SUCCESSES consecutive successes
  HALF_OPEN → OPEN      on any delivery failure

Design constraints:
  - Single-process, DB-backed state (no distributed coordination)
  - Deterministic transitions — no time-based auto-recovery in this phase
  - All writes are flush-only; callers own the commit boundary
  - system_name is the public interface for all functions (operator-facing routes
    use downstream names); internally, state is fetched and stored via
    downstream_system_id FK on CircuitBreakerState
"""

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import CircuitState
from app.models.circuit import CircuitBreakerState
from app.models.delivery import DownstreamSystem

# Number of consecutive successes required to close the circuit from HALF_OPEN.
# Set to 1 for MVP simplicity: one successful probe confirms recovery.
HALF_OPEN_REQUIRED_SUCCESSES = 1


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _resolve_system(
    session: AsyncSession,
    system_name: str,
) -> DownstreamSystem | None:
    """Return the DownstreamSystem row for system_name, or None if not found."""
    return await session.scalar(
        select(DownstreamSystem).where(DownstreamSystem.name == system_name)
    )


async def _get_or_create(
    session: AsyncSession,
    system: DownstreamSystem,
) -> CircuitBreakerState:
    """
    Return the CircuitBreakerState row for the given system, creating it if absent.

    Uses downstream_system_id as the lookup key — the canonical FK association.
    The denormalised system_name field is populated from system.name at creation
    time and not updated thereafter.

    The row is flushed but not committed — the caller owns the transaction.
    """
    state = await session.scalar(
        select(CircuitBreakerState).where(
            CircuitBreakerState.downstream_system_id == system.id
        )
    )
    if state is None:
        state = CircuitBreakerState(
            downstream_system_id=system.id,
            system_name=system.name,
            state=CircuitState.CLOSED.value,
            consecutive_failure_count=0,
            consecutive_success_count=0,
        )
        session.add(state)
        await session.flush()
    return state


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Public API — all functions accept system_name; resolve to FK internally
# ---------------------------------------------------------------------------

async def is_allowed(
    session: AsyncSession,
    system_name: str,
) -> bool:
    """
    Return True if a delivery attempt should be allowed for this system.

    Returns False only when the circuit is OPEN.
    CLOSED and HALF_OPEN both allow delivery attempts.

    If no DownstreamSystem row exists for system_name, returns True (fail-open
    behaviour — an unknown system is not blocked by the circuit breaker).
    """
    system = await _resolve_system(session, system_name)
    if system is None:
        return True
    state = await _get_or_create(session, system)
    return state.state != CircuitState.OPEN.value


async def record_success(
    session: AsyncSession,
    system_name: str,
) -> CircuitBreakerState:
    """
    Record a successful delivery attempt.

    - Resets consecutive_failure_count to zero.
    - Increments consecutive_success_count.
    - If HALF_OPEN and successes reach HALF_OPEN_REQUIRED_SUCCESSES: → CLOSED.
    - If CLOSED: counters updated, state unchanged.
    """
    system = await _resolve_system(session, system_name)
    if system is None:
        raise ValueError(f"DownstreamSystem {system_name!r} not found.")
    state = await _get_or_create(session, system)
    now = _now()

    state.consecutive_failure_count = 0
    state.consecutive_success_count += 1
    state.last_success_at = now
    state.updated_at = now

    if state.state == CircuitState.HALF_OPEN.value:
        if state.consecutive_success_count >= HALF_OPEN_REQUIRED_SUCCESSES:
            state.state = CircuitState.CLOSED.value
            state.opened_at = None
            state.last_transition_reason = (
                f"recovery_confirmed_after_{state.consecutive_success_count}_success"
            )

    await session.flush()
    return state


async def record_failure(
    session: AsyncSession,
    system_name: str,
    open_after_n_failures: int,
) -> CircuitBreakerState:
    """
    Record a failed delivery attempt.

    - Increments consecutive_failure_count.
    - Resets consecutive_success_count to zero.
    - If CLOSED and threshold reached: → OPEN.
    - If HALF_OPEN: → OPEN (probe failed; recovery not confirmed).
    - If already OPEN: counts accumulate for visibility; state unchanged.
    """
    system = await _resolve_system(session, system_name)
    if system is None:
        raise ValueError(f"DownstreamSystem {system_name!r} not found.")
    state = await _get_or_create(session, system)
    now = _now()

    state.consecutive_failure_count += 1
    state.consecutive_success_count = 0
    state.last_failure_at = now
    state.updated_at = now

    if state.state == CircuitState.HALF_OPEN.value:
        state.state = CircuitState.OPEN.value
        state.opened_at = now
        state.last_transition_reason = "half_open_probe_failed"

    elif state.state == CircuitState.CLOSED.value:
        if state.consecutive_failure_count >= open_after_n_failures:
            state.state = CircuitState.OPEN.value
            state.opened_at = now
            state.last_transition_reason = (
                f"opened_after_{state.consecutive_failure_count}_consecutive_failures"
            )

    await session.flush()
    return state


async def force_reset(
    session: AsyncSession,
    system_name: str,
    target_state: str,
    reset_by: str,
) -> CircuitBreakerState:
    """
    Operator-initiated circuit state override.

    Resets all counters and transitions to the requested state.
    target_state must be one of: closed | open | half_open.

    Records the operator identity in last_transition_reason for audit.
    Flushes but does not commit — the caller commits.

    Raises ValueError if target_state is not valid or if the system does not exist.
    """
    valid = {s.value for s in CircuitState}
    if target_state not in valid:
        raise ValueError(
            f"target_state must be one of {sorted(valid)}, got {target_state!r}"
        )

    system = await _resolve_system(session, system_name)
    if system is None:
        raise ValueError(f"DownstreamSystem {system_name!r} not found.")

    state = await _get_or_create(session, system)
    now = _now()

    state.state = target_state
    state.consecutive_failure_count = 0
    state.consecutive_success_count = 0
    state.updated_at = now
    state.last_transition_reason = f"manual_reset_to_{target_state}_by_{reset_by}"

    if target_state == CircuitState.OPEN.value:
        state.opened_at = now
    elif target_state == CircuitState.CLOSED.value:
        state.opened_at = None

    await session.flush()
    return state


async def get_state(
    session: AsyncSession,
    system_name: str,
) -> CircuitBreakerState | None:
    """
    Return the CircuitBreakerState row for system_name, or None if it does not
    yet exist (no delivery attempts have been made for this system).

    Looks up the DownstreamSystem first to resolve the FK.  Returns None if
    either the system or its circuit state row does not exist.
    """
    system = await _resolve_system(session, system_name)
    if system is None:
        return None
    return await session.scalar(
        select(CircuitBreakerState).where(
            CircuitBreakerState.downstream_system_id == system.id
        )
    )


async def get_all_states(
    session: AsyncSession,
) -> list[CircuitBreakerState]:
    """
    Return all CircuitBreakerState rows ordered by system_name.
    Returns an empty list if no delivery attempts have been made yet.
    """
    rows = await session.scalars(
        select(CircuitBreakerState).order_by(CircuitBreakerState.system_name)
    )
    return list(rows)


def to_dict(state: CircuitBreakerState) -> dict[str, Any]:
    """
    Serialise a CircuitBreakerState row to a plain dict for API responses.

    Reads system_name from the denormalised column — no join required.
    """
    return {
        "system_name": state.system_name,
        "state": state.state,
        "consecutive_failure_count": state.consecutive_failure_count,
        "consecutive_success_count": state.consecutive_success_count,
        "opened_at": state.opened_at.isoformat() if state.opened_at else None,
        "last_failure_at": (
            state.last_failure_at.isoformat() if state.last_failure_at else None
        ),
        "last_success_at": (
            state.last_success_at.isoformat() if state.last_success_at else None
        ),
        "last_transition_reason": state.last_transition_reason,
        "updated_at": state.updated_at.isoformat(),
    }
