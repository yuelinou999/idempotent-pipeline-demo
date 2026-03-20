"""
Circuit breaker state model — Context 2 (Delivery Resilience).

One row per downstream system, associated via downstream_system_id FK.
Maintains the delivery health posture for each integration target.

Distinct from DeliveryAttemptLog (individual attempt records) and DeliveryDLQ
(terminal failures awaiting operator action).

Relational design:
  downstream_system_id  FK → downstream_system.id (unique; one state per system)
  system_name           Denormalised convenience column, populated at row creation
                        from DownstreamSystem.name.  Avoids join queries in
                        to_dict() and __repr__ without sacrificing relational
                        integrity.  The FK is the canonical association key;
                        system_name is read-only after creation.
"""

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class CircuitBreakerState(Base):
    """
    Per-downstream circuit breaker state.

    State machine:
      closed    → normal; delivery attempts allowed
      open      → degraded; delivery attempts blocked
      half_open → recovery probe; one attempt allowed

    consecutive_failure_count  — failures since last success or reset
    consecutive_success_count  — successes since last failure or reset;
                                 used to close the circuit from half_open
    opened_at                  — when the circuit last transitioned to open;
                                 null when state is closed
    last_failure_at            — timestamp of the most recent recorded failure
    last_success_at            — timestamp of the most recent recorded success
    last_transition_reason     — human-readable description of why the last
                                 state transition occurred; queryable for audit
    updated_at                 — last time this row was written
    """

    __tablename__ = "circuit_breaker_state"

    __table_args__ = (
        # One circuit state row per downstream system.
        UniqueConstraint("downstream_system_id", name="uq_circuit_downstream_system"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Relational association — the canonical key for this row.
    downstream_system_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("downstream_system.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Denormalised convenience field — populated at creation from
    # DownstreamSystem.name; not updated thereafter.  Allows to_dict()
    # and __repr__ to read the system name without a join.
    system_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    # ORM relationship — available for eager loading when needed.
    downstream_system: Mapped["DownstreamSystem"] = relationship(  # noqa: F821
        "DownstreamSystem",
        lazy="raise",   # explicit: never accidentally lazy-load in async context
    )

    # Current state: closed | open | half_open
    state: Mapped[str] = mapped_column(
        String(16), nullable=False, default="closed", index=True
    )

    consecutive_failure_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )
    consecutive_success_count: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )

    opened_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_failure_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    last_transition_reason: Mapped[str | None] = mapped_column(
        String(256), nullable=True
    )

    updated_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), onupdate=func.now(), nullable=False
    )

    def __repr__(self) -> str:
        return (
            f"<CircuitBreakerState system={self.system_name} "
            f"state={self.state} "
            f"failures={self.consecutive_failure_count}>"
        )
