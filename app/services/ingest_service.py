"""
ingest_service — single-record ingestion orchestrator.

Orchestrates the full idempotent write path for one message:

  1. Build entity_key (version-less) and idempotent_key (versioned)
  2. Hash payload
  3. Look up existing IdempotentState by entity_key
  4. Run decision engine → Decision
  5. Resolve or reuse correlation_id
  6. Call decision_applicator.apply_decision() — all DB writes, no commit
  7. Commit
  8. Run completeness check (accepted paths only)
  9. Map internal decision → API outcome family and return

The commit in step 7 is the only commit issued in this module.
decision_applicator never commits — that boundary is enforced by design so that
the future batch_ingest orchestrator can manage its own savepoint transaction
while reusing the same applicator.

Correlation propagation for skipped messages
--------------------------------------------
When a message is skipped (duplicate or stale), the entity already has an active
staging record.  We look up its correlation_id and propagate it so that:
  - The API response returns a useful correlation_id rather than None.
  - The IngestionAttemptLog entry is properly correlated for reconciliation queries.
"""

import json
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import DECISION_TO_API, DecisionOutcome
from app.models.idempotent_state import IdempotentState
from app.models.staging import StagingRecord
from app.services.completeness_engine import evaluate_completeness
from app.services.correlation_engine import resolve_correlation_id
from app.services.decision_applicator import ApplicationResult, MessageContext, apply_decision
from app.services.decision_engine import make_decision
from app.services.key_builder import build_entity_key, build_idempotent_key
from app.services.payload_hasher import compute_payload_hash


async def _reuse_correlation_id(
    session: AsyncSession,
    existing_state: IdempotentState | None,
) -> str | None:
    """
    Return the correlation_id from the entity's currently active staging record.

    Lookup path: existing_state.active_staging_id → StagingRecord.correlation_id

    Returns None if there is no active staging record or it carries no correlation_id.
    Used for SKIP_DUPLICATE and SKIP_STALE paths so the response is still correlated.
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
      idempotent_key  — versioned key for this specific attempt
      entity_key      — version-less entity identifier
      outcome         — API outcome family (accepted / skipped / quarantined)
      decision_detail — raw internal decision for audit / debug
      reason          — human-readable explanation from the decision engine
      correlation_id  — populated for all outcomes where correlation context exists
      staging_id      — set for accepted records only
    """
    # Step 1: derive both keys
    entity_key = build_entity_key(source_system, entity_type, business_key)
    idempotent_key = build_idempotent_key(source_system, entity_type, business_key, version)

    # Step 2: hash payload
    payload_hash = compute_payload_hash(payload)
    payload_json = json.dumps(payload)

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

    # Step 5: resolve correlation_id before applying the decision
    correlation_id: str | None = None

    if decision.outcome in (DecisionOutcome.ACCEPT_NEW, DecisionOutcome.ACCEPT_SUPERSEDE):
        correlation_id, _method = await resolve_correlation_id(
            session, source_system, business_key, payload
        )
    elif decision.outcome in (DecisionOutcome.SKIP_DUPLICATE, DecisionOutcome.SKIP_STALE):
        correlation_id = await _reuse_correlation_id(session, existing_state)

    # Step 6: apply decision — DB side effects, no commit
    context = MessageContext(
        entity_key=entity_key,
        idempotent_key=idempotent_key,
        source_system=source_system,
        entity_type=entity_type,
        business_key=business_key,
        version=version,
        payload=payload,
        payload_hash=payload_hash,
        payload_json=payload_json,
    )

    result: ApplicationResult = await apply_decision(
        session=session,
        context=context,
        decision=decision,
        existing_state=existing_state,
        correlation_id=correlation_id,
    )

    # Step 7: commit — the only commit in this module
    await session.commit()

    # Step 8: completeness check (accepted paths only, after commit)
    if decision.outcome in (DecisionOutcome.ACCEPT_NEW, DecisionOutcome.ACCEPT_SUPERSEDE):
        if result.correlation_id:
            await evaluate_completeness(session, result.correlation_id)

    # Step 9: map internal decision → API outcome family
    api_outcome = DECISION_TO_API[decision.outcome]

    return {
        "idempotent_key": idempotent_key,
        "entity_key": entity_key,
        "outcome": api_outcome.value,
        "decision_detail": decision.outcome.value,
        "reason": decision.reason,
        "correlation_id": result.correlation_id,
        "staging_id": result.staging_id,
    }
