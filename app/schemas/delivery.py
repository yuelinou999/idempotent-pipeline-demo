"""
Pydantic schemas for the Delivery Resilience context (Context 2).

These schemas back the operator console endpoints for inspecting delivery
health, reviewing delivery failures, and recording resolution decisions.
"""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Downstream system
# ---------------------------------------------------------------------------

class DownstreamSystemResponse(BaseModel):
    """Configuration and mock-behavior settings for a downstream system."""
    model_config = ConfigDict(from_attributes=True)

    id: int
    name: str
    description: str | None
    baseline_error_rate_pct: float
    circuit_threshold_pct: float
    max_retry_attempts: int
    supports_status_query: bool
    mock_behavior: str


# ---------------------------------------------------------------------------
# Delivery simulation
# ---------------------------------------------------------------------------

class SimulateDeliveryRequest(BaseModel):
    staging_id: int = Field(
        ...,
        description="ID of the StagingRecord to deliver.",
    )
    system_name: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="Name of the downstream system (must exist in the DownstreamSystem table).",
        examples=["warehouse_sync"],
    )


class SimulateDeliveryResponse(BaseModel):
    system_name: str
    outcome: str = Field(
        ...,
        description=(
            "Terminal outcome of the delivery attempt sequence. "
            "Possible values: "
            "'success' — delivered on first or retry attempt; "
            "'dlq_transient_exhausted' — retry ceiling reached without success; "
            "'dlq_non_retryable' — immediate failure (HTTP 400 / 409 / 422); "
            "'queued_ambiguous' — timeout; outcome unknown; entry created in "
            "AmbiguousOutcomeQueue for investigation; "
            "'circuit_open' — circuit is open; no attempts made."
        ),
    )
    attempts_made: int = Field(
        ...,
        description=(
            "Number of DeliveryAttemptLog rows written during this delivery sequence. "
            "Zero when the circuit was open and blocked all attempts."
        ),
    )
    dlq_entry_id: int | None = Field(
        None,
        description="DeliveryDLQ row ID if a DLQ entry was created; null otherwise.",
    )
    ambiguous_id: int | None = Field(
        None,
        description=(
            "AmbiguousOutcomeQueue row ID if the outcome was ambiguous (timeout); "
            "null for all other outcomes."
        ),
    )
    circuit_state: str | None = Field(
        None,
        description=(
            "State of the circuit breaker after this call: closed | open | half_open. "
            "Null only in unexpected error paths."
        ),
    )


# ---------------------------------------------------------------------------
# DeliveryDLQ
# ---------------------------------------------------------------------------

class DeliveryDLQItemResponse(BaseModel):
    """A single entry in the delivery dead-letter queue."""
    model_config = ConfigDict(from_attributes=True)

    id: int
    staging_id: int | None
    system_name: str
    failure_category: str = Field(
        ...,
        description=(
            "Why this record entered the DLQ: "
            "'transient_exhausted' — retry ceiling reached; "
            "'non_retryable' — HTTP 400 / 409 / 422 on first attempt; "
            "'query_unavailable_escalated' — escalated from AmbiguousOutcomeQueue "
            "when query-before-retry could not resolve the outcome."
        ),
    )
    last_attempt_at: str
    resolution_status: str = Field(
        ...,
        description="'pending' | 'resolved' | 'manually_replayed'",
    )
    resolved_by: str | None
    resolved_at: str | None
    queued_at: str


class ResolveDLQRequest(BaseModel):
    resolved_by: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="Identifier of the operator recording this resolution decision.",
        examples=["ops-team@example.com"],
    )


class ResolveDLQResponse(BaseModel):
    """Confirmation of an operator resolution decision on a DLQ entry."""
    id: int
    staging_id: int | None
    system_name: str
    failure_category: str
    resolution_status: str
    resolved_by: str
    resolved_at: str


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------

class DeliveryOutcomesReport(BaseModel):
    """
    Aggregated delivery health data for the operator console.

    delivery_attempts_by_system — attempt volume and outcome breakdown per
        downstream system over the report window.
    dlq_pending_by_system — current unresolved DLQ depth per system,
        grouped by failure category.
    """
    report_window_hours: int
    generated_at: str
    delivery_attempts_by_system: dict[str, list[dict[str, Any]]]
    dlq_pending_by_system: dict[str, list[dict[str, Any]]]
