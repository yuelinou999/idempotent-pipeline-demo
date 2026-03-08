"""
Pydantic schemas for the message ingestion API.
"""

from typing import Any

from pydantic import BaseModel, Field, field_validator


class IngestMessageRequest(BaseModel):
    source_system: str = Field(..., min_length=1, max_length=64, examples=["oms"])
    entity_type: str = Field(..., min_length=1, max_length=64, examples=["order"])
    business_key: str = Field(..., min_length=1, max_length=256, examples=["ORD-20260301-0042"])
    version: int = Field(..., ge=1, examples=[3])
    payload: dict[str, Any] = Field(..., examples=[{"customer_id": "CUST-001", "total_amount": 199.99}])

    # Optional: explicit correlation reference
    correlation_id: str | None = Field(None, max_length=256)

    @field_validator("source_system", "entity_type")
    @classmethod
    def lowercase_identifier(cls, v: str) -> str:
        return v.strip().lower()


class IngestMessageResponse(BaseModel):
    idempotent_key: str
    entity_key: str

    # API outcome family: accepted | skipped | quarantined
    outcome: str

    # Raw internal decision: accept_new | accept_supersede | skip_duplicate | skip_stale | quarantine
    decision_detail: str

    reason: str
    correlation_id: str | None
    staging_id: int | None


class IngestBatchRequest(BaseModel):
    messages: list[IngestMessageRequest] = Field(..., min_length=1, max_length=500)


class IngestBatchResponse(BaseModel):
    total: int
    results: list[IngestMessageResponse]


class IdempotentStateResponse(BaseModel):
    # entity_key is the primary lookup identifier (version-less)
    entity_key: str
    # idempotent_key reflects the version currently stored
    idempotent_key: str
    source_system: str
    entity_type: str
    business_key: str
    current_version: int
    payload_hash: str
    last_decision: str
    first_seen_at: str
    last_updated_at: str
    active_staging_id: int | None

    class Config:
        from_attributes = True


class StagingRecordResponse(BaseModel):
    id: int
    idempotent_key: str
    source_system: str
    entity_type: str
    business_key: str
    version: int
    completeness_status: str
    correlation_id: str | None
    staged_at: str

    class Config:
        from_attributes = True


class ReviewQueueItemResponse(BaseModel):
    id: int
    idempotent_key: str
    source_system: str
    entity_type: str
    business_key: str
    version: int
    reason: str
    reason_detail: str | None
    resolved: bool
    queued_at: str

    class Config:
        from_attributes = True


class CorrelationResponse(BaseModel):
    correlation_id: str
    mappings: list[dict[str, Any]]

