"""
Pydantic schemas for the circuit breaker operator console endpoints.
"""

from pydantic import BaseModel, ConfigDict, Field


class CircuitBreakerStateResponse(BaseModel):
    """
    Current circuit breaker state for one downstream system.

    Returned by GET /circuits and GET /circuits/{downstream_name}.
    """
    model_config = ConfigDict(from_attributes=True)

    system_name: str
    state: str = Field(
        ...,
        description="closed | open | half_open",
    )
    consecutive_failure_count: int = Field(
        ...,
        description=(
            "Number of consecutive delivery failures since the last success or reset. "
            "When this reaches the system's open_after_n_failures threshold, "
            "the circuit transitions from closed to open."
        ),
    )
    consecutive_success_count: int = Field(
        ...,
        description=(
            "Number of consecutive delivery successes since the last failure or reset. "
            "Used to close the circuit from half_open."
        ),
    )
    opened_at: str | None = Field(
        None,
        description="ISO-8601 timestamp when the circuit last transitioned to open. "
                    "Null when the circuit is closed.",
    )
    last_failure_at: str | None = Field(
        None,
        description="ISO-8601 timestamp of the most recent recorded delivery failure.",
    )
    last_success_at: str | None = Field(
        None,
        description="ISO-8601 timestamp of the most recent recorded delivery success.",
    )
    last_transition_reason: str | None = Field(
        None,
        description="Human-readable description of why the last state transition occurred.",
    )
    updated_at: str = Field(
        ...,
        description="ISO-8601 timestamp of the last write to this row.",
    )



class CircuitResetRequest(BaseModel):
    target_state: str = Field(
        ...,
        description=(
            "Target circuit state after reset: closed | half_open | open. "
            "The most common operator action is resetting to 'closed' after confirming "
            "the downstream has recovered, or to 'half_open' to allow one recovery probe "
            "without fully closing the circuit."
        ),
        examples=["closed"],
    )
    reset_by: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="Identifier of the operator performing the reset. Recorded for audit.",
        examples=["ops@example.com"],
    )


class CircuitNotActiveResponse(BaseModel):
    """
    Returned when GET /circuits/{downstream_name} is called for a system
    that has never had a delivery attempt.  No CircuitBreakerState row exists
    yet; this is a synthetic representation of the implicit initial state.
    """
    system_name: str
    state: str = "closed"
    note: str = "No delivery attempts recorded yet. Circuit is implicitly closed."
