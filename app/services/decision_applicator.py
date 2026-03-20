"""
decision_applicator — executes DB side effects for a resolved write-path decision.

This module is the extracted "application layer" of the idempotent write path.
It handles all database writes that follow from a Decision but does NOT commit.
Commit responsibility belongs exclusively to the calling orchestration layer.

This separation is required so that:
  - single_record_ingest can commit after one record
  - batch_ingest (future) can wrap many applications in a single transaction
    with per-record savepoint isolation

Inputs:
  session        — active AsyncSession; must NOT be auto-committed
  context        — MessageContext with all message-derived fields
  decision       — Decision from decision_engine.make_decision()
  existing_state — current IdempotentState row, or None for first-seen records
  correlation_id — already resolved by the calling orchestrator

Outputs:
  ApplicationResult with staging_id (int | None) and correlation_id (str | None)

Side effects written (no commit):
  ACCEPT_NEW      — StagingRecord (new), IdempotentState (new), IngestionAttemptLog
  ACCEPT_SUPERSEDE— StagingRecord (new), IdempotentState (updated), IngestionAttemptLog
  SKIP_*          — IngestionAttemptLog only
  QUARANTINE      — DataAnomalyQueue, IngestionAttemptLog
"""

import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.correlation import DataAnomalyQueue
from app.models.idempotent_state import IdempotentState
from app.models.ingestion_log import IngestionAttemptLog
from app.models.staging import StagingRecord
from app.services.completeness_engine import mark_superseded
from app.services.decision_engine import Decision
from app.core.enums import DecisionOutcome


@dataclass
class MessageContext:
    """All message-derived fields needed by the applicator."""
    entity_key: str
    idempotent_key: str
    source_system: str
    entity_type: str
    business_key: str
    version: int
    payload: dict[str, Any]
    payload_hash: str
    payload_json: str  # pre-serialised JSON string of payload


@dataclass
class ApplicationResult:
    """What the applicator produced — returned to the orchestrator."""
    staging_id: int | None
    correlation_id: str | None


async def apply_decision(
    session: AsyncSession,
    context: MessageContext,
    decision: Decision,
    existing_state: IdempotentState | None,
    correlation_id: str | None,
) -> ApplicationResult:
    """
    Apply a write-path decision to the database.

    Does NOT call session.commit().  The caller controls the transaction boundary.

    For ACCEPT paths:
      1. Write a new StagingRecord
      2. Create (ACCEPT_NEW) or update (ACCEPT_SUPERSEDE) IdempotentState
      3. On ACCEPT_SUPERSEDE, mark the previous staging record as superseded
      4. Append to IngestionAttemptLog

    For SKIP paths:
      Append to IngestionAttemptLog only.

    For QUARANTINE:
      Write DataAnomalyQueue entry and append to IngestionAttemptLog.
    """
    staging_id: int | None = None

    if decision.outcome in (DecisionOutcome.ACCEPT_NEW, DecisionOutcome.ACCEPT_SUPERSEDE):
        # Write staging record
        staging = StagingRecord(
            idempotent_key=context.idempotent_key,
            source_system=context.source_system,
            entity_type=context.entity_type,
            business_key=context.business_key,
            version=context.version,
            payload=context.payload_json,
            payload_hash=context.payload_hash,
            completeness_status="partial",
            correlation_id=correlation_id,
        )
        session.add(staging)
        await session.flush()  # materialise staging.id before referencing it
        staging_id = staging.id

        if decision.outcome == DecisionOutcome.ACCEPT_NEW:
            state = IdempotentState(
                entity_key=context.entity_key,
                idempotent_key=context.idempotent_key,
                source_system=context.source_system,
                entity_type=context.entity_type,
                business_key=context.business_key,
                current_version=context.version,
                payload_hash=context.payload_hash,
                last_decision=decision.outcome.value,
                active_staging_id=staging_id,
            )
            session.add(state)

        else:  # ACCEPT_SUPERSEDE
            if existing_state and existing_state.active_staging_id:
                await mark_superseded(
                    session,
                    existing_state.active_staging_id,
                    staging_id,
                )
            existing_state.idempotent_key = context.idempotent_key
            existing_state.current_version = context.version
            existing_state.payload_hash = context.payload_hash
            existing_state.last_decision = decision.outcome.value
            existing_state.active_staging_id = staging_id

        await session.flush()

    elif decision.outcome == DecisionOutcome.QUARANTINE:
        review_entry = DataAnomalyQueue(
            idempotent_key=context.idempotent_key,
            source_system=context.source_system,
            entity_type=context.entity_type,
            business_key=context.business_key,
            version=context.version,
            payload=context.payload_json,
            payload_hash=context.payload_hash,
            reason="same_version_different_payload",
            reason_detail=decision.reason,
        )
        session.add(review_entry)

    # Always append to the audit log regardless of outcome.
    log_entry = IngestionAttemptLog(
        entity_key=context.entity_key,
        idempotent_key=context.idempotent_key,
        source_system=context.source_system,
        entity_type=context.entity_type,
        business_key=context.business_key,
        incoming_version=context.version,
        payload_hash=context.payload_hash,
        decision=decision.outcome.value,
        decision_reason=decision.reason,
        payload_snapshot=context.payload_json,
        correlation_id=correlation_id,
    )
    session.add(log_entry)

    return ApplicationResult(staging_id=staging_id, correlation_id=correlation_id)
