"""
ingestion_attempt_log — append-only record of every ingestion attempt.

Never updated, never deleted. This is the audit trail that answers:
  "What happened to every message that entered the pipeline?"

Stores both:
  entity_key      — version-less, for entity-level aggregation in reconciliation
  idempotent_key  — versioned, for version-specific traceability

The `decision` field stores the raw internal DecisionOutcome value
(accept_new, accept_supersede, skip_duplicate, skip_stale, quarantine).
The reconciliation service maps these to report families (accepted/skipped/quarantined).
"""

from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class IngestionAttemptLog(Base):
    __tablename__ = "ingestion_attempt_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Version-less entity identifier — used for entity-level aggregation
    entity_key: Mapped[str] = mapped_column(String(512), nullable=False, index=True)

    # Versioned idempotent key — used for version-specific traceability
    idempotent_key: Mapped[str] = mapped_column(String(512), nullable=False, index=True)

    source_system: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    business_key: Mapped[str] = mapped_column(String(256), nullable=False)
    incoming_version: Mapped[int] = mapped_column(Integer, nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    # Raw internal decision outcome stored verbatim for full audit fidelity.
    # Values: accept_new | accept_supersede | skip_duplicate | skip_stale | quarantine
    # The reconciliation service maps these to report families.
    decision: Mapped[str] = mapped_column(String(20), nullable=False, index=True)

    # Human-readable explanation — useful for debugging and audit queries
    decision_reason: Mapped[str | None] = mapped_column(String(256), nullable=True)

    # Snapshot of the raw payload
    payload_snapshot: Mapped[str | None] = mapped_column(Text, nullable=True)

    received_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), index=True)

    # Correlation ID if resolved at ingestion time
    correlation_id: Mapped[str | None] = mapped_column(String(256), nullable=True, index=True)

    def __repr__(self) -> str:
        return (
            f"<IngestionAttemptLog id={self.id} "
            f"entity={self.entity_key} v={self.incoming_version} decision={self.decision}>"
        )

