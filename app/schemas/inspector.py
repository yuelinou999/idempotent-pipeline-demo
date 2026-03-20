"""
Pydantic schemas for the entity / message inspector operator console endpoints.
"""

from typing import Any

from pydantic import BaseModel, Field


class IngestionEvent(BaseModel):
    id: int
    idempotent_key: str
    version: int
    decision: str = Field(
        ...,
        description=(
            "Internal decision outcome: accept_new | accept_supersede | "
            "skip_duplicate | skip_stale | quarantine"
        ),
    )
    decision_reason: str | None
    source_system: str
    correlation_id: str | None
    received_at: str


class StagingSummary(BaseModel):
    staging_id: int
    idempotent_key: str
    version: int
    completeness_status: str
    payload_hash: str
    correlation_id: str | None
    staged_at: str
    promoted_at: str | None


class AnomalyEvent(BaseModel):
    id: int
    idempotent_key: str
    version: int
    reason: str
    reason_detail: str | None
    resolved: bool
    resolution_note: str | None
    queued_at: str
    resolved_at: str | None


class DeliveryAttemptEvent(BaseModel):
    id: int
    staging_id: int
    system_name: str
    attempt_number: int
    http_status: int | None
    classification: str
    outcome: str = Field(
        ..., description="success | failed | timeout"
    )
    response_body_snippet: str | None
    attempted_at: str


class DeliveryDLQEvent(BaseModel):
    id: int
    staging_id: int
    system_name: str
    failure_category: str
    resolution_status: str
    resolved_by: str | None
    last_attempt_at: str
    queued_at: str
    resolved_at: str | None


class AmbiguousOutcomeEvent(BaseModel):
    id: int
    staging_id: int
    system_name: str
    ambiguity_category: str
    status: str = Field(
        ...,
        description=(
            "pending | confirmed_delivered | retry_eligible | status_unavailable"
        ),
    )
    resolution_note: str | None
    idempotent_key: str | None
    created_at: str
    updated_at: str


class ReplayActivityEvent(BaseModel):
    replay_request_id: int
    replay_request_status: str | None
    requested_by: str | None
    reason: str | None
    selection_mode: str
    source_queue_id: int
    original_failure_category: str | None
    system_name: str
    item_status: str
    result_note: str | None
    execution_result: str | None = Field(
        None,
        description=(
            "What simulate_delivery returned: success | dlq_transient_exhausted | "
            "dlq_non_retryable | queued_ambiguous | circuit_open | error. "
            "Null if the replay has not been executed yet."
        ),
    )
    executed_at: str | None
    item_created_at: str


class EntityIdentifiers(BaseModel):
    entity_key: str
    current_idempotent_key: str | None
    source_system: str | None
    entity_type: str | None
    business_key: str | None


class EntitySummary(BaseModel):
    entity_key: str
    current_version: int | None
    last_decision: str | None
    first_seen_at: str | None
    last_updated_at: str | None
    has_anomalies: bool
    has_delivery_failures: bool
    has_ambiguous_outcomes: bool
    has_replay_activity: bool
    total_ingestion_attempts: int
    total_delivery_attempts: int


class TimelineEvent(BaseModel):
    """
    One normalized event in the entity's ordered event stream.

    The ordered_events field merges events from all inspection sections into a single
    chronological sequence so an operator can read the record's story in order.
    """
    event_type: str = Field(
        ...,
        description=(
            "Category of event: "
            "ingestion | "
            "anomaly_queued | anomaly_resolved | "
            "delivery_attempt | "
            "dlq_queued | dlq_resolved | "
            "ambiguous_queued | ambiguous_resolved | "
            "replay_requested | replay_executed"
        ),
    )
    occurred_at: str = Field(
        ...,
        description="ISO-8601 timestamp. Timeline is sorted ascending by this field.",
    )
    summary: str = Field(
        ...,
        description="Concise operator-facing label describing what happened.",
    )
    ref_id: int = Field(
        ...,
        description="Row ID in the source table for this event.",
    )
    context: dict[str, Any] = Field(
        default_factory=dict,
        description="Key fields from the source record for quick operator reference.",
    )


class EntityInspectionView(BaseModel):
    """
    Complete inspection view for one entity.

    Assembles the end-to-end story of a single entity across:
    ingestion → staging → delivery → anomaly / DLQ / ambiguous → replay.

    Two complementary views of the same data:
      Grouped sections — direct inspection by topic (ingestion_history, anomalies, etc.)
      ordered_events   — unified ordered event sequence for chronological reading

    This view is read-only and never modifies any records.
    """
    summary: EntitySummary = Field(
        ...,
        description="High-level flags and counts — the quick-scan view.",
    )
    identifiers: EntityIdentifiers = Field(
        ...,
        description="Core identity fields for this entity.",
    )
    staging: StagingSummary | None = Field(
        None,
        description="Active staging record, if one exists.",
    )
    ingestion_history: list[IngestionEvent] = Field(
        default_factory=list,
        description="All ingestion attempts for this entity, ordered by received_at.",
    )
    anomalies: list[AnomalyEvent] = Field(
        default_factory=list,
        description="DataAnomalyQueue entries linked to this entity.",
    )
    delivery_attempts: list[DeliveryAttemptEvent] = Field(
        default_factory=list,
        description="All delivery attempt log rows for staging records of this entity.",
    )
    delivery_dlq: list[DeliveryDLQEvent] = Field(
        default_factory=list,
        description="DeliveryDLQ entries for staging records of this entity.",
    )
    ambiguous_outcomes: list[AmbiguousOutcomeEvent] = Field(
        default_factory=list,
        description="AmbiguousOutcomeQueue entries for staging records of this entity.",
    )
    replay_activity: list[ReplayActivityEvent] = Field(
        default_factory=list,
        description=(
            "ReplayRequestItem entries for staging records of this entity, "
            "with execution log context where available."
        ),
    )
    ordered_events: list[TimelineEvent] = Field(
        default_factory=list,
        description=(
            "Unified ordered event sequence assembled from all grouped sections. "
            "Sorted ascending by occurred_at. "
            "Use this field to read the entity's story chronologically. "
            "The grouped sections above are preserved for direct topic-level inspection."
        ),    )


class CorrelationInspectionView(BaseModel):
    """
    Inspection view for a correlation_id — one EntityInspectionView per entity
    in the business transaction.
    """
    correlation_id: str
    entity_count: int
    entities: list[EntityInspectionView]
