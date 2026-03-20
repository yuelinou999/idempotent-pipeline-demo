"""
Delivery Resilience context models — Context 2.

Three tables covering the delivery control plane:

  DownstreamSystem   — static configuration for each downstream target, including
                       mock behaviour used in demo/test scenarios.

  DeliveryAttemptLog — append-only record of every delivery attempt.  Distinct from
                       IngestionAttemptLog (which records write-path decisions).
                       One row per attempt, including retries.

  DeliveryDLQ        — records that have exhausted the retry ceiling or failed with a
                       non-retryable error.  Distinct from DataAnomalyQueue (which
                       records data quality anomalies).  Operator-facing resolution
                       state machine: pending → resolved | manually_replayed.
"""

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.core.database import Base


class DownstreamSystem(Base):
    """
    Static configuration for a downstream integration target.

    circuit_threshold_pct  — error-rate percentage that triggers a circuit open.
                             Calibrated per-system based on its baseline error rate.
    max_retry_attempts     — ceiling after which a transient failure routes to DLQ.
    supports_status_query  — whether this system exposes a query-before-retry interface.
    mock_behavior          — demo/test directive: controls what simulate_delivery returns.
                             Values defined in MockBehavior enum.
    """

    __tablename__ = "downstream_system"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    description: Mapped[str | None] = mapped_column(String(256), nullable=True)

    baseline_error_rate_pct: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    circuit_threshold_pct: Mapped[float] = mapped_column(Float, nullable=False, default=15.0)
    max_retry_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    supports_status_query: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Demo/test directive — see MockBehavior enum for valid values.
    mock_behavior: Mapped[str] = mapped_column(String(32), nullable=False, default="success")

    # Demo/test directive for query-before-retry — see MockStatusQueryBehavior enum.
    # Consulted when mock_behavior produces a timeout outcome.
    # "not_found" is the safe default: allows retry if query is run.
    mock_status_query_behavior: Mapped[str] = mapped_column(
        String(32), nullable=False, default="not_found"
    )

    # Circuit breaker: open after this many consecutive delivery failures.
    # Kept as an explicit count rather than derived from circuit_threshold_pct
    # so test scenarios are directly predictable.
    open_after_n_failures: Mapped[int] = mapped_column(Integer, nullable=False, default=3)

    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    def __repr__(self) -> str:
        return f"<DownstreamSystem name={self.name} mock={self.mock_behavior}>"


class DeliveryAttemptLog(Base):
    """
    Append-only log of every delivery attempt to a downstream system.

    Each retry of the same staging record creates a new row with an incremented
    attempt_number.  The classification and outcome fields reflect the result of
    running retry_classifier against the mock response.

    Distinct from IngestionAttemptLog:
      IngestionAttemptLog  — records write-path decisions (accept / skip / quarantine)
      DeliveryAttemptLog   — records downstream delivery results (success / failed / timeout)
    """

    __tablename__ = "delivery_attempt_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # staging_id links this attempt to the record being delivered.
    staging_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    system_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    # attempt_number starts at 1 and increments per retry for the same staging_id.
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)

    # http_status is null for timeouts.
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # classification: retryable | non_retryable | unknown
    classification: Mapped[str] = mapped_column(String(16), nullable=False)

    # outcome: success | failed | timeout
    outcome: Mapped[str] = mapped_column(String(16), nullable=False, index=True)

    response_body_snippet: Mapped[str | None] = mapped_column(String(256), nullable=True)

    attempted_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), index=True
    )

    def __repr__(self) -> str:
        return (
            f"<DeliveryAttemptLog id={self.id} system={self.system_name} "
            f"attempt={self.attempt_number} outcome={self.outcome}>"
        )


class DeliveryDLQ(Base):
    """
    Dead-letter queue for records that could not be delivered to a downstream system.

    Records enter here via two paths:
      - retry ceiling exhausted (failure_category = transient_exhausted)
      - non-retryable HTTP error (failure_category = non_retryable)

    Escalation from ambiguous timeout outcomes and operator-triggered replay
    are deferred to later phases.

    Resolution state machine:
      pending → resolved         (operator marks as resolved without replay)
      pending → manually_replayed (reserved for a later replay phase)

    Distinct from DataAnomalyQueue:
      DataAnomalyQueue — data quality anomalies (same version, different payload)
      DeliveryDLQ      — delivery infrastructure failures after retry exhaustion
    """

    __tablename__ = "delivery_dlq"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    staging_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    system_name: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

    # failure_category: transient_exhausted | non_retryable | query_unavailable_escalated
    failure_category: Mapped[str] = mapped_column(String(32), nullable=False, index=True)

    last_attempt_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)

    # resolution_status: pending | resolved | manually_replayed
    resolution_status: Mapped[str] = mapped_column(
        String(20), nullable=False, default="pending", index=True
    )
    resolved_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    queued_at: Mapped[datetime] = mapped_column(
        DateTime, server_default=func.now(), index=True
    )

    def __repr__(self) -> str:
        return (
            f"<DeliveryDLQ id={self.id} system={self.system_name} "
            f"category={self.failure_category} status={self.resolution_status}>"
        )
