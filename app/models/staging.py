"""
staging_record — accepted records buffered before promotion to production.

Data here is not fully trusted. Validation and completeness checks run here.
Records are either promoted to production_projection or marked superseded.
Nothing is ever deleted — only status transitions occur.
"""

from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class StagingRecord(Base):
    __tablename__ = "staging_record"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    idempotent_key: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    source_system: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    business_key: Mapped[str] = mapped_column(String(256), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)

    payload: Mapped[str] = mapped_column(Text, nullable=False)  # JSON string
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    # Completeness state machine: partial | complete | superseded | promoted
    completeness_status: Mapped[str] = mapped_column(String(16), nullable=False, default="partial", index=True)

    # Links to the business transaction this record belongs to
    correlation_id: Mapped[str | None] = mapped_column(String(256), nullable=True, index=True)

    staged_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    promoted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    superseded_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Points to the staging record that superseded this one (for lineage tracing)
    superseded_by_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    def __repr__(self) -> str:
        return f"<StagingRecord id={self.id} key={self.idempotent_key} status={self.completeness_status}>"
