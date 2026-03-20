"""
Ambiguous outcome queue model — Context 2 (Delivery Resilience).

Records delivery attempts whose outcome could not be determined — typically
because the downstream timed out without responding.  The pipeline cannot
know whether the downstream processed the request or not.

An operator or an automated query-before-retry step must investigate each
entry and resolve it to one of:
  confirmed_delivered — downstream confirms it processed the request; no retry
  retry_eligible      — downstream has no record; safe to retry
  status_unavailable  — query failed or system does not support status queries

Distinct from DeliveryDLQ (known terminal failures) and DataAnomalyQueue
(data quality anomalies in the ingestion write path).
"""

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class AmbiguousOutcomeQueue(Base):
    """
    One entry per delivery attempt whose outcome is genuinely unknown.

    Traceability fields:
      delivery_attempt_log_id  FK → delivery_attempt_log.id
                               The exact attempt that produced the ambiguous outcome.
      downstream_system_id     FK → downstream_system.id
      staging_id               Denormalized from DeliveryAttemptLog for operator
                               convenience; nullable because staging_id on the
                               attempt log is itself nullable.
      system_name              Denormalized from DownstreamSystem.name.
      idempotent_key           From the linked StagingRecord if available; null
                               when staging_id is not set or record not found.
      correlation_id           From the linked StagingRecord if available.

    Status lifecycle:
      pending              → confirmed_delivered  (query confirmed delivery)
      pending              → retry_eligible       (query confirmed not delivered)
      pending              → status_unavailable   (query failed or not supported)
    """

    __tablename__ = "ambiguous_outcome_queue"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # FK associations — canonical relational links
    delivery_attempt_log_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("delivery_attempt_log.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    downstream_system_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("downstream_system.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Denormalized convenience fields — populated at creation, not updated
    staging_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    system_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    idempotent_key: Mapped[str | None] = mapped_column(String(512), nullable=True)
    correlation_id: Mapped[str | None] = mapped_column(String(256), nullable=True)

    # Why the outcome is ambiguous: timeout | unknown_response
    ambiguity_category: Mapped[str] = mapped_column(String(32), nullable=False)

    # Resolution lifecycle: pending | confirmed_delivered | retry_eligible | status_unavailable
    status: Mapped[str] = mapped_column(
        String(32), nullable=False, default="pending", index=True
    )

    # Populated after a query-before-retry step
    resolution_note: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), index=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )

    def __repr__(self) -> str:
        return (
            f"<AmbiguousOutcomeQueue id={self.id} system={self.system_name} "
            f"status={self.status}>"
        )
