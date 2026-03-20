"""
Circuit breaker operator console endpoints.

Provides real-time visibility into the per-downstream circuit breaker state
and operator controls for manual state overrides.

These routes back the "Circuit / downstream status" view in the operator
console — the view that answers:
  - Which downstream systems are currently degraded (circuit open)?
  - When did a circuit open, and why?
  - Has a circuit recovered (half_open → closed)?

Routes:
  GET  /circuits                       — all active circuit states
  GET  /circuits/{downstream_name}     — one system's circuit state
  POST /circuits/{downstream_name}/reset — operator state override
"""

from typing import Union

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.schemas.circuit import (
    CircuitBreakerStateResponse,
    CircuitNotActiveResponse,
    CircuitResetRequest,
)
from app.services import circuit_breaker_service as cb_svc

router = APIRouter(tags=["operator console"])


@router.get(
    "/circuits",
    response_model=list[CircuitBreakerStateResponse],
    summary="Inspect circuit breaker state for all downstream systems",
)
async def list_circuits(
    db: AsyncSession = Depends(get_db),
) -> list[CircuitBreakerStateResponse]:
    """
    Return the current circuit breaker state for every downstream system
    that has had at least one delivery attempt.

    Systems with no delivery history do not appear here — their circuit is
    implicitly closed.  Use GET /circuits/{downstream_name} to inspect a
    specific system regardless of history.

    Use this view to answer:
    - Which systems currently have an open or half_open circuit?
    - Which systems have accumulated consecutive failures?
    - What caused the most recent state transition for each system?
    """
    states = await cb_svc.get_all_states(db)
    return [CircuitBreakerStateResponse(**cb_svc.to_dict(s)) for s in states]


@router.get(
    "/circuits/{downstream_name}",
    response_model=Union[CircuitBreakerStateResponse, CircuitNotActiveResponse],
    summary="Inspect circuit breaker state for one downstream system",
)
async def get_circuit(
    downstream_name: str,
    db: AsyncSession = Depends(get_db),
) -> Union[CircuitBreakerStateResponse, CircuitNotActiveResponse]:
    """
    Return the current circuit breaker state for a single downstream system.

    If no delivery attempt has ever been made to this system, returns a
    synthetic "not yet active" response indicating the circuit is implicitly
    closed.  No row is created by this endpoint.
    """
    state = await cb_svc.get_state(db, downstream_name)
    if state is None:
        return CircuitNotActiveResponse(system_name=downstream_name)
    return CircuitBreakerStateResponse(**cb_svc.to_dict(state))


@router.post(
    "/circuits/{downstream_name}/reset",
    response_model=CircuitBreakerStateResponse,
    summary="Operator override of circuit breaker state",
)
async def reset_circuit(
    downstream_name: str,
    request: CircuitResetRequest,
    db: AsyncSession = Depends(get_db),
) -> CircuitBreakerStateResponse:
    """
    Manually override the circuit breaker state for a downstream system.

    All counters are reset to zero on every reset.  The operator identity
    is recorded in last_transition_reason for audit.

    **Common operator actions:**

    Reset to `closed` — after independently confirming the downstream has
    recovered and is healthy.  Allows delivery to resume immediately.

    Reset to `half_open` — to allow exactly one recovery probe attempt
    without fully closing the circuit.  If the probe succeeds, the circuit
    closes automatically.  If it fails, the circuit re-opens.  Use this
    when you want to test recovery cautiously.

    Reset to `open` — to proactively block a system you know is degraded,
    before the failure threshold is crossed automatically.
    """
    try:
        state = await cb_svc.force_reset(
            session=db,
            system_name=downstream_name,
            target_state=request.target_state,
            reset_by=request.reset_by,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    await db.commit()
    return CircuitBreakerStateResponse(**cb_svc.to_dict(state))
