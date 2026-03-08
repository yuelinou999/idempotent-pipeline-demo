"""
ingest_service — orchestrates the full idempotent write path for a single message.

Processing order for each incoming message:
  1. Build entity_key (version-less) and idempotent_key (versioned)
  2. Hash payload
  3. Look up existing state using entity_key
  4. Run decision engine → internal DecisionOutcome
  5. Execute outcome:
     - ACCEPT_NEW:       resolve correlation, write staging, create state row, log attempt
     - ACCEPT_SUPERSEDE: resolve correlation, write staging, update state row, log attempt
     - SKIP_DUPLICATE:   reuse correlation_id from active staging record, log attempt only
     - SKIP_STALE:       reuse correlation_id from active staging record, log attempt only
     - QUARANTINE:       write to manual_review_queue, log attempt
  6. Run completeness check (accepted paths only)
  7. Map internal decision → API outcome family for response

Correlation propagation for skipped messages
--------------------------------------------
When a message is skipped (duplicate or stale), the entity already has an active
staging record.  We look it up via existing_state.active_staging_id and reuse its
correlation_id so that:
  - The API response returns a useful correlation_id rather than None.
  - The attempt log entry is properly correlated for reconciliation queries.
"""

import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import DECISION_TO_API, DecisionOutcome
from app.models.correlation import ManualReviewQueue
from app.models.idempotent_state import IdempotentState
from app.models.ingestion_log import IngestionAttemptLog
from app.models.staging import StagingRecord
from app.services.completeness_engine import evaluate_completeness, mark_superseded
from app.services.correlation_engine import resolve_correlation_id
from app.services.decision_engine import make_decision
from app.services.key_builder import build_entity_key, build_idempotent_key
from app.services.payload_hasher import compute_payload_hash


async def _reuse_correlation_id(
    session: AsyncSession,
    existing_state: IdempotentState | None,
) -> str | None:
    """
    Return the correlation_id already associated with the entity's active staging record.

    Lookup path:
      existing_state.active_staging_id → StagingRecord.correlation_id

    Returns None if there is no active staging record or it has no correlation_id.
    """
    if existing_state is None or existing_state.active_staging_id is None:
        return None
    active_staging = await session.get(StagingRecord, existing_state.active_staging_id)
    if active_staging is None:
        return None
    return active_staging.correlation_id


async def ingest_message(
    session: AsyncSession,
    source_system: str,
    entity_type: str,
    business_key: str,
    version: int,
    payload: dict[str, Any],
) -> dict[str, Any]:
    """
    Process a single incoming message through the full idempotent write path.

    Returns a result dict with:
      - idempotent_key:   versioned key for this specific attempt
      - entity_key:       version-less entity identifier
      - outcome:          API outcome family (accepted / skipped / quarantined)
      - decision_detail:  raw internal decision (accept_new / accept_supersede / etc.)
      - reason:           human-readable explanation
      - correlation_id:   populated for all outcomes where correlation context exists
      - staging_id:       set for accepted records only
    """

    # Step 1: derive both keys
    entity_key = build_entity_key(source_system, entity_type, business_key)
    idempotent_key = build_idempotent_key(source_system, entity_type, business_key, version)

    # Step 2: hash payload
    payload_hash = compute_payload_hash(payload)

    # Step 3: look up existing state by entity_key (version-less)
    existing_state = await session.scalar(
        select(IdempotentState).where(IdempotentState.entity_key == entity_key)
    )

    # Step 4: decide
    decision = make_decision(
        incoming_version=version,
        incoming_hash=payload_hash,
        existing_state=existing_state,
    )

    correlation_id: str | None = None
    staging_id: int | None = None

    # Step 5: execute
    if decision.outcome in (DecisionOutcome.ACCEPT_NEW, DecisionOutcome.ACCEPT_SUPERSEDE):

        # Resolve (or create) a correlation ID before writing the staging record
        correlation_id, _method = await resolve_correlation_id(
            session, source_system, business_key, payload
        )

        # Write staging record
        staging = StagingRecord(
            idempotent_key=idempotent_key,
            source_system=source_system,
            entity_type=entity_type,
            business_key=business_key,
            version=version,
            payload=json.dumps(payload),
            payload_hash=payload_hash,
            completeness_status="partial",
            correlation_id=correlation_id,
        )
        session.add(staging)
        await session.flush()  # materialise staging.id
        staging_id = staging.id

        if decision.outcome == DecisionOutcome.ACCEPT_NEW:
            state = IdempotentState(
                entity_key=entity_key,
                idempotent_key=idempotent_key,
                source_system=source_system,
                entity_type=entity_type,
                business_key=business_key,
                current_version=version,
                payload_hash=payload_hash,
                last_decision=decision.outcome.value,
                active_staging_id=staging_id,
            )
            session.add(state)

        else:  # ACCEPT_SUPERSEDE
            if existing_state and existing_state.active_staging_id:
                await mark_superseded(session, existing_state.active_staging_id, staging_id)

            existing_state.idempotent_key = idempotent_key
            existing_state.current_version = version
            existing_state.payload_hash = payload_hash
            existing_state.last_decision = decision.outcome.value
            existing_state.active_staging_id = staging_id

        await session.flush()

        # Step 6: completeness check
        await evaluate_completeness(session, correlation_id)

    elif decision.outcome in (DecisionOutcome.SKIP_DUPLICATE, DecisionOutcome.SKIP_STALE):
        # Reuse the correlation_id from the entity's currently active staging record.
        # This keeps the skip log entry correlated with the already-accepted version
        # and gives the API caller a useful correlation_id rather than None.
        correlation_id = await _reuse_correlation_id(session, existing_state)

    elif decision.outcome == DecisionOutcome.QUARANTINE:
        review_entry = ManualReviewQueue(
            idempotent_key=idempotent_key,
            source_system=source_system,
            entity_type=entity_type,
            business_key=business_key,
            version=version,
            payload=json.dumps(payload),
            payload_hash=payload_hash,
            reason="same_version_different_payload",
            reason_detail=decision.reason,
        )
        session.add(review_entry)

    # Always append to the audit log, regardless of outcome.
    # correlation_id is now populated for all outcomes where context is available.
    log_entry = IngestionAttemptLog(
        entity_key=entity_key,
        idempotent_key=idempotent_key,
        source_system=source_system,
        entity_type=entity_type,
        business_key=business_key,
        incoming_version=version,
        payload_hash=payload_hash,
        decision=decision.outcome.value,
        decision_reason=decision.reason,
        payload_snapshot=json.dumps(payload),
        correlation_id=correlation_id,
    )
    session.add(log_entry)
    await session.commit()

    # Step 7: map internal decision → API outcome family
    api_outcome = DECISION_TO_API[decision.outcome]

    return {
        "idempotent_key": idempotent_key,
        "entity_key": entity_key,
        "outcome": api_outcome.value,
        "decision_detail": decision.outcome.value,
        "reason": decision.reason,
        "correlation_id": correlation_id,
        "staging_id": staging_id,
    }
