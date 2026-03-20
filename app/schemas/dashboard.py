"""
Pydantic schemas for the dashboard overview endpoint.
"""

from typing import Any

from pydantic import BaseModel, Field


class SummaryCard(BaseModel):
    """Top-level pipeline health counts."""
    total_entities: int = Field(
        ..., description="Total distinct entities tracked in idempotent state."
    )
    open_anomalies: int = Field(
        ..., description="Unresolved DataAnomalyQueue entries."
    )
    pending_dlq_items: int = Field(
        ..., description="Pending DeliveryDLQ entries awaiting operator action."
    )
    pending_ambiguous_items: int = Field(
        ..., description="AmbiguousOutcomeQueue entries not yet resolved."
    )
    open_circuits: int = Field(
        ..., description="Downstream systems with circuit breaker in OPEN state."
    )
    half_open_circuits: int = Field(
        ..., description="Downstream systems with circuit breaker in HALF_OPEN state."
    )
    pending_replays: int = Field(
        ..., description="ReplayRequests in pending status (created but not yet executed)."
    )


class CircuitSystemSummary(BaseModel):
    """Per-system circuit breaker snapshot."""
    system_name: str
    state: str = Field(..., description="closed | open | half_open")
    consecutive_failure_count: int
    opened_at: str | None
    last_transition_reason: str | None
    links: dict[str, str] = Field(
        default_factory=dict,
        description="Operator drill-down links for this system.",
    )


class CircuitHealthSummary(BaseModel):
    """Aggregated circuit breaker health across all downstream systems."""
    closed: int
    open: int
    half_open: int
    systems: list[CircuitSystemSummary] = Field(
        default_factory=list,
        description=(
            "One entry per downstream system with an active circuit state row. "
            "Systems that have never had a delivery attempt are not listed."
        ),
    )


class ReplaySummary(BaseModel):
    """Replay request counts by status."""
    pending: int
    executing: int
    completed: int
    failed: int
    links: dict[str, str] = Field(default_factory=dict)


class ActivityEvent(BaseModel):
    """One entry in the recent activity feed."""
    event_type: str = Field(
        ...,
        description=(
            "delivery_failure | ambiguous_outcome | replay_execution | "
            "circuit_state_change | data_anomaly"
        ),
    )
    occurred_at: str = Field(
        ..., description="ISO-8601 timestamp. Feed is sorted descending (most recent first)."
    )
    summary: str = Field(
        ..., description="Concise operator-facing label describing the event."
    )
    context: dict[str, Any] = Field(
        default_factory=dict,
        description="Key fields from the source record.",
    )
    links: dict[str, str] = Field(
        default_factory=dict,
        description="Operator drill-down links relevant to this event.",
    )


class DashboardOverview(BaseModel):
    """
    Operator dashboard overview — full pipeline health snapshot.

    Designed for future UI rendering.  Each section maps to a console widget:
      summary       → health-status cards (counts + attention flag)
      circuit_health → circuit breaker status table
      replay_summary → replay queue counts
      recent_activity → activity feed (most recent events, all types)
      quick_links   → navigation shortcuts to drill-down surfaces

    All data is read-only.  This endpoint never modifies any records.
    """
    generated_at: str = Field(
        ..., description="ISO-8601 timestamp when this snapshot was assembled."
    )
    attention_needed: bool = Field(
        ...,
        description=(
            "True if any queue has pending items or any circuit is not closed. "
            "Intended as a top-level health indicator for the console header."
        ),
    )
    summary: SummaryCard
    circuit_health: CircuitHealthSummary
    replay_summary: ReplaySummary
    recent_activity: list[ActivityEvent] = Field(
        default_factory=list,
        description=(
            "Recent operator-relevant events across delivery, ambiguous outcomes, "
            "replay executions, circuit changes, and data anomalies. "
            "Sorted descending by occurred_at. Limited to the last 20 entries."
        ),
    )
    quick_links: dict[str, str] = Field(
        default_factory=dict,
        description="Navigation shortcuts to key operator surfaces.",
    )
