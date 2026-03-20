"""
Pydantic schemas for the ambiguous outcome queue operator console endpoints.
"""

from pydantic import BaseModel, ConfigDict, Field


class AmbiguousOutcomeItemResponse(BaseModel):
    """
    One entry in the ambiguous outcome queue.

    Returned by GET /dlq/ambiguous and GET /dlq/ambiguous/{id}.
    """
    model_config = ConfigDict(from_attributes=True)

    id: int
    system_name: str
    staging_id: int | None
    idempotent_key: str | None = Field(
        None,
        description="Idempotent key of the staging record being delivered.",
    )
    correlation_id: str | None = Field(
        None,
        description="Correlation ID of the business transaction, if available.",
    )
    ambiguity_category: str = Field(
        ...,
        description="Why the outcome is unknown: timeout | unknown_response",
    )
    status: str = Field(
        ...,
        description=(
            "Resolution lifecycle: "
            "pending — not yet investigated; "
            "confirmed_delivered — downstream confirms delivery, no retry needed; "
            "retry_eligible — downstream has no record, safe to retry; "
            "status_unavailable — query failed or system does not support queries"
        ),
    )
    resolution_note: str | None = Field(
        None,
        description="Human-readable explanation set after a query-before-retry step.",
    )
    delivery_attempt_log_id: int
    downstream_system_id: int
    created_at: str
    updated_at: str



class QueryStatusResponse(BaseModel):
    """
    Result of running query-before-retry for one ambiguous outcome entry.

    Returned by POST /dlq/ambiguous/{id}/query-status.
    """
    ambiguous_id: int
    system_name: str
    query_result: str = Field(
        ...,
        description=(
            "What the downstream reported: "
            "confirmed_delivered | not_found | status_unavailable"
        ),
    )
    new_status: str = Field(
        ...,
        description=(
            "Updated AmbiguousOutcomeQueue.status after the query: "
            "confirmed_delivered | retry_eligible | status_unavailable"
        ),
    )
    resolution_note: str = Field(
        ...,
        description="Explanation of what the query found and what to do next.",
    )
    action_required: str = Field(
        ...,
        description=(
            "Operator action implied by the query result: "
            "none_confirmed_delivered — no action; delivery was confirmed; "
            "retry_eligible — record may be retried by an operator; "
            "manual_operator_decision — query inconclusive; operator must decide"
        ),
    )
