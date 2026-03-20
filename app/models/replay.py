"""
Replay models — Context 2 (Delivery Resilience).

Three tables forming the replay audit trail:

  ReplayRequest      — an operator-initiated request to re-attempt delivery for a
                       set of records that previously failed or had an ambiguous outcome.
                       One request may cover many source items.

  ReplayRequestItem  — one row per source item (DLQ entry or AmbiguousOutcomeQueue entry)
                       included in a ReplayRequest.  Carries full provenance so an
                       operator can trace each item back to its original failure.

  ReplayExecutionLog — one row per executed item, recording what the replay call
                       produced.  Proves that replay went through the normal delivery
                       path rather than a bypass shortcut.

Replay execution calls simulate_delivery() — the existing canonical delivery path —
for each item.  This means replay is subject to the same circuit breaker, delivery
attempt logging, and outcome routing as any other delivery call.
"""

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class ReplayRequest(Base):
    """
    An operator-initiated replay request.

    status lifecycle:
      pending   → created; not yet executed
      executing → execution is in progress
      completed → all items processed (individual items may have varied outcomes)
      failed    → execution raised an unexpected error before completing
    """

    __tablename__ = "replay_request"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # How records were selected: dlq_items | ambiguous_items
    selection_mode: Mapped[str] = mapped_column(String(32), nullable=False)

    # Why this replay was requested — operator-supplied context
    reason: Mapped[str] = mapped_column(String(512), nullable=False)

    # Who requested the replay — recorded for audit
    requested_by: Mapped[str] = mapped_column(String(128), nullable=False)

    # Lifecycle: pending | executing | completed | failed
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="pending", index=True
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), index=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )

    def __repr__(self) -> str:
        return (
            f"<ReplayRequest id={self.id} mode={self.selection_mode} "
            f"status={self.status} by={self.requested_by}>"
        )


class ReplayRequestItem(Base):
    """
    One source item within a ReplayRequest.

    Provenance fields trace each item back to its origin so any operator
    reviewing a past replay can understand the full history of each record.

    source_queue_type       — "delivery_dlq" | "ambiguous_outcome_queue"
    source_queue_id         — row ID in the source table
    original_failure_category — from the source queue row (for DLQ: transient_exhausted
                              / non_retryable; for ambiguous: ambiguity_category value)
    delivery_attempt_log_id — the delivery attempt that originally produced the failure;
                              populated for ambiguous items (which carry this FK);
                              null for DLQ items in this phase
    staging_id              — the staging record being re-delivered
    system_name             — downstream system to replay delivery to
    idempotent_key          — for correlation with ingestion audit trail
    correlation_id          — business transaction context, if available

    status lifecycle:
      pending          → not yet executed
      success          → re-delivery succeeded
      dlq              → delivery failed again; new DLQ entry created
      queued_ambiguous → delivery timed out again; new AmbiguousOutcomeQueue entry created
      circuit_open     → circuit was open; no attempt made
      error            → unexpected execution error
    """

    __tablename__ = "replay_request_item"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    replay_request_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("replay_request.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Provenance
    source_queue_type: Mapped[str] = mapped_column(String(32), nullable=False)
    source_queue_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    original_failure_category: Mapped[str | None] = mapped_column(
        String(64), nullable=True
    )
    delivery_attempt_log_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True, index=True
    )

    # Delivery context
    staging_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    system_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    idempotent_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    correlation_id: Mapped[str | None] = mapped_column(String(256), nullable=True)

    # Per-item execution result
    status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending", index=True
    )
    result_note: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), index=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )

    def __repr__(self) -> str:
        return (
            f"<ReplayRequestItem id={self.id} "
            f"replay={self.replay_request_id} source={self.source_queue_type}:{self.source_queue_id} "
            f"status={self.status}>"
        )


class ReplayExecutionLog(Base):
    """
    Audit record of one item's replay execution.

    One row per executed ReplayRequestItem.  The execution_result field records
    exactly what the canonical delivery path returned, proving that replay did
    not use a bypass shortcut.

    execution_result values mirror simulate_delivery() outcomes:
      success               — delivery succeeded on replay
      dlq_transient_exhausted | dlq_non_retryable — delivery failed again; DLQ entry created
      queued_ambiguous      — timeout again; new AmbiguousOutcomeQueue entry created
      circuit_open          — circuit was open; no attempt made
      error                 — unexpected exception during execution

    created_delivery_attempt_log_id:
      The most recent DeliveryAttemptLog.id written during this replay execution.
      Nullable because circuit_open and error paths produce no attempt log.

    created_ingestion_attempt_log_id:
      Reserved for future phases where replay may re-enter the ingestion path.
      Always null in this phase.
    """

    __tablename__ = "replay_execution_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    replay_request_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("replay_request.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    replay_request_item_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("replay_request_item.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    attempted_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), index=True
    )

    # Raw outcome string from simulate_delivery()
    execution_result: Mapped[str] = mapped_column(String(64), nullable=False)
    outcome_note: Mapped[str | None] = mapped_column(Text, nullable=True)

    # FK to the delivery attempt created during this replay (null for circuit_open / error)
    created_delivery_attempt_log_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )

    # Reserved for future ingestion-layer replay
    created_ingestion_attempt_log_id: Mapped[int | None] = mapped_column(
        Integer, nullable=True
    )

    def __repr__(self) -> str:
        return (
            f"<ReplayExecutionLog id={self.id} "
            f"request={self.replay_request_id} item={self.replay_request_item_id} "
            f"result={self.execution_result}>"
        )
