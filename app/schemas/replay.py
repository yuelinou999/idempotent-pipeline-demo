"""
Pydantic schemas for the replay operator console endpoints.
"""

from pydantic import BaseModel, Field


class CreateReplayRequest(BaseModel):
    selection_mode: str = Field(
        ...,
        description=(
            "How records were selected for replay: "
            "'dlq_items' — from pending DeliveryDLQ entries; "
            "'ambiguous_items' — from retry_eligible AmbiguousOutcomeQueue entries."
        ),
        examples=["dlq_items"],
    )
    reason: str = Field(
        ...,
        min_length=1,
        max_length=512,
        description="Why this replay is being requested. Recorded for audit.",
        examples=["Post-outage recovery after admissions_crm was unreachable"],
    )
    requested_by: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="Identifier of the operator requesting the replay.",
        examples=["ops@example.com"],
    )
    source_item_ids: list[int] = Field(
        ...,
        min_length=1,
        description=(
            "IDs in the source table identified by selection_mode. "
            "For 'dlq_items': DeliveryDLQ row IDs. "
            "For 'ambiguous_items': AmbiguousOutcomeQueue row IDs. "
            "All items must be eligible; any ineligible item causes the entire "
            "request to be rejected."
        ),
    )


class ReplayRequestSummary(BaseModel):
    """Brief replay request summary for list views."""
    id: int
    selection_mode: str
    reason: str
    requested_by: str
    status: str = Field(
        ...,
        description="pending | executing | completed | failed",
    )
    created_at: str
    updated_at: str


class ReplayRequestCreatedResponse(BaseModel):
    """Returned immediately after POST /replays."""
    replay_request_id: int
    selection_mode: str
    status: str
    items_created: int
    requested_by: str
    reason: str


class ExecutionLogSummary(BaseModel):
    execution_result: str = Field(
        ...,
        description=(
            "What simulate_delivery() returned for this item: "
            "success | dlq_transient_exhausted | dlq_non_retryable | "
            "queued_ambiguous | circuit_open | error"
        ),
    )
    outcome_note: str | None
    attempted_at: str
    created_delivery_attempt_log_id: int | None = Field(
        None,
        description=(
            "ID of the DeliveryAttemptLog row created during this replay execution. "
            "Null for circuit_open and error outcomes."
        ),
    )


class ReplayItemDetail(BaseModel):
    id: int
    source_queue_type: str = Field(
        ..., description="delivery_dlq | ambiguous_outcome_queue"
    )
    source_queue_id: int
    original_failure_category: str | None
    staging_id: int
    system_name: str
    idempotent_key: str | None
    correlation_id: str | None
    status: str = Field(
        ...,
        description=(
            "pending | success | dlq | queued_ambiguous | circuit_open | error"
        ),
    )
    result_note: str | None
    execution_log: ExecutionLogSummary | None = Field(
        None,
        description="Populated after execution. Null for pending items.",
    )


class ReplayRequestDetail(BaseModel):
    """Full detail for GET /replays/{id}."""
    id: int
    selection_mode: str
    reason: str
    requested_by: str
    status: str
    created_at: str
    updated_at: str
    items: list[ReplayItemDetail]


class ExecuteReplayResponse(BaseModel):
    """Returned by POST /replays/{id}/execute."""
    replay_request_id: int
    status: str
    items_processed: int
