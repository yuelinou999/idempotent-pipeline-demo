"""
idempotent_state — one row per entity_key, always reflecting the latest known state.

This is the fast-path lookup table. Before any write, the ingestion service checks here
using the entity_key (version-less). This is what makes supersede and stale detection work:
the same row is found regardless of whether v1, v2, or v3 is incoming.

entity_key  = {source_system}:::{entity_type}:::{business_key}   ← lookup key (version-less)
idempotent_key = {entity_key}:::{version}                        ← stored for traceability only
"""

from datetime import datetime

from sqlalchemy import DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class IdempotentState(Base):
    __tablename__ = "idempotent_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Version-less entity identifier — the lookup key for current-state queries.
    # One row per entity; updated in place when a newer version arrives.
    entity_key: Mapped[str] = mapped_column(String(512), unique=True, nullable=False, index=True)

    # Versioned idempotent key for the currently stored version — kept for traceability.
    idempotent_key: Mapped[str] = mapped_column(String(512), nullable=False)

    source_system: Mapped[str] = mapped_column(String(64), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(64), nullable=False)
    business_key: Mapped[str] = mapped_column(String(256), nullable=False)
    current_version: Mapped[int] = mapped_column(Integer, nullable=False)

    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    # Last internal decision applied: accept_new | accept_supersede
    last_decision: Mapped[str] = mapped_column(String(20), nullable=False)

    first_seen_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    last_updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now(), onupdate=func.now())

    # FK to the staging record currently active for this entity
    active_staging_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    def __repr__(self) -> str:
        return (
            f"<IdempotentState entity={self.entity_key} "
            f"version={self.current_version} decision={self.last_decision}>"
        )

