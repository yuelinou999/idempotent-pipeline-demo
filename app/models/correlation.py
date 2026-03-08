"""
correlation_map — maps source-system keys to a shared Correlation ID.

The Correlation ID answers: "Which business transaction does this record belong to?"
This is entirely separate from the idempotent key, which answers:
  "Has this specific version of this specific entity already been processed?"

The first system to report a business event generates the Correlation ID.
Subsequent systems are mapped to it either by explicit reference or matching rules.
"""

from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class CorrelationMap(Base):
    __tablename__ = "correlation_map"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # The shared business transaction identifier
    correlation_id: Mapped[str] = mapped_column(String(256), nullable=False, index=True)

    # The source system that owns this particular key
    source_system: Mapped[str] = mapped_column(String(64), nullable=False)
    source_key: Mapped[str] = mapped_column(String(256), nullable=False)

    # How the mapping was established: explicit | rule_based | self_generated
    mapping_method: Mapped[str] = mapped_column(String(32), nullable=False)

    mapped_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        # DB-level enforcement: one source key can only ever map to one correlation ID.
        # The service layer checks this before INSERT, but this constraint is the last
        # line of defence against concurrent duplicate mapping attempts.
        UniqueConstraint("source_system", "source_key", name="uq_correlation_map_source"),
    )

    def __repr__(self) -> str:
        return f"<CorrelationMap corr={self.correlation_id} src={self.source_system}:{self.source_key}>"


class ManualReviewQueue(Base):
    """
    Quarantined or unresolvable records that require human intervention.

    Records land here when:
    - same key + same version + different payload_hash (data anomaly)
    - peer-system conflict with no version precedence
    - schema validation failure
    """

    __tablename__ = "manual_review_queue"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    idempotent_key: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    source_system: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    business_key: Mapped[str] = mapped_column(String(256), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)

    payload: Mapped[str] = mapped_column(Text, nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    # Why this record was quarantined
    reason: Mapped[str] = mapped_column(String(64), nullable=False)
    reason_detail: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Resolution state
    resolved: Mapped[bool] = mapped_column(default=False, nullable=False)
    resolution_note: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    queued_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), index=True)

    def __repr__(self) -> str:
        return f"<ManualReviewQueue id={self.id} key={self.idempotent_key} reason={self.reason}>"


class ProductionProjection(Base):
    """
    Materialized production dataset — clean, deduplicated, validated.

    One row per entity (identified by entity_key). When a newer version of the same
    entity is materialized, the existing row is updated in place, not replaced.

    entity_key     — version-less identity, the unique projection key
    idempotent_key — versioned, kept only for traceability back to the source staging record
    """

    __tablename__ = "production_projection"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Version-less entity identifier — one row per entity, the lookup key.
    entity_key: Mapped[str] = mapped_column(String(512), unique=True, nullable=False, index=True)

    # Versioned idempotent key of the version currently materialized — traceability only.
    idempotent_key: Mapped[str] = mapped_column(String(512), nullable=False)

    source_system: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    business_key: Mapped[str] = mapped_column(String(256), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)

    payload: Mapped[str] = mapped_column(Text, nullable=False)
    correlation_id: Mapped[str | None] = mapped_column(String(256), nullable=True, index=True)

    # FK to the staging record this was promoted from
    source_staging_id: Mapped[int] = mapped_column(Integer, nullable=False)

    materialized_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    __table_args__ = (
        # entity_key uniqueness is already enforced by the column-level unique=True above.
        # Declared here explicitly for documentation clarity.
        UniqueConstraint("entity_key", name="uq_production_projection_entity"),
    )

    def __repr__(self) -> str:
        return (
            f"<ProductionProjection entity={self.entity_key} version={self.version}>"
        )
