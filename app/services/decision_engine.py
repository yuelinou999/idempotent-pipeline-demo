"""
decision_engine — the five-branch deduplication decision logic.

This is the heart of the idempotent write path. Given an incoming message
and the current known state for its idempotent key, it returns a DecisionOutcome
and an explanation.

The five branches are:

    1. Key not seen before                              → ACCEPT_NEW
    2. Same key, incoming version > current version     → ACCEPT_SUPERSEDE
    3. Same key, same version, same payload hash        → SKIP_DUPLICATE
    4. Same key, same version, different payload hash   → QUARANTINE  ← anomaly
    5. Same key, incoming version < current version     → SKIP_STALE

Branch 4 is the critical production scenario: same key + same version but the
payload changed. This means the upstream system re-emitted a record without
incrementing the version. We cannot silently skip (data quality issue would be
buried) and we cannot silently overwrite (uncontrolled mutation). We quarantine
and surface it for review.
"""

from dataclasses import dataclass

from app.core.enums import DecisionOutcome
from app.models.idempotent_state import IdempotentState


@dataclass(frozen=True)
class Decision:
    outcome: DecisionOutcome
    reason: str
    should_write_staging: bool
    should_supersede_previous: bool
    should_quarantine: bool


def make_decision(
    incoming_version: int,
    incoming_hash: str,
    existing_state: IdempotentState | None,
) -> Decision:
    """
    Core deduplication decision function.

    Args:
        incoming_version:  Version extracted from the incoming message.
        incoming_hash:     SHA-256 hash of the canonical incoming payload.
        existing_state:    Current IdempotentState row for this key, or None if first seen.

    Returns:
        A Decision dataclass describing exactly what the ingestion service should do.
    """

    # Branch 1: first time we have ever seen this key
    if existing_state is None:
        return Decision(
            outcome=DecisionOutcome.ACCEPT_NEW,
            reason="First time this key has been seen.",
            should_write_staging=True,
            should_supersede_previous=False,
            should_quarantine=False,
        )

    current_version = existing_state.current_version
    current_hash = existing_state.payload_hash

    # Branch 2: newer version — accept and supersede the existing entry
    if incoming_version > current_version:
        return Decision(
            outcome=DecisionOutcome.ACCEPT_SUPERSEDE,
            reason=f"Incoming version {incoming_version} supersedes current version {current_version}.",
            should_write_staging=True,
            should_supersede_previous=True,
            should_quarantine=False,
        )

    # Same version from here on
    if incoming_version == current_version:

        # Branch 3: exact duplicate — same version, same payload
        if incoming_hash == current_hash:
            return Decision(
                outcome=DecisionOutcome.SKIP_DUPLICATE,
                reason=f"Exact duplicate: version {incoming_version} with identical payload hash.",
                should_write_staging=False,
                should_supersede_previous=False,
                should_quarantine=False,
            )

        # Branch 4: anomaly — same version, different payload
        # The upstream system changed the payload without incrementing the version.
        # This is a data quality issue that must be surfaced, not silently handled.
        return Decision(
            outcome=DecisionOutcome.QUARANTINE,
            reason=(
                f"Anomaly: version {incoming_version} already exists with a different payload hash. "
                f"Expected {current_hash!r}, received {incoming_hash!r}. "
                "Possible causes: upstream re-emission without version increment, "
                "or transformation bug silently mutating field values."
            ),
            should_write_staging=False,
            should_supersede_previous=False,
            should_quarantine=True,
        )

    # Branch 5: stale — incoming version is older than what we already have
    # incoming_version < current_version
    return Decision(
        outcome=DecisionOutcome.SKIP_STALE,
        reason=f"Stale message: incoming version {incoming_version} is older than current version {current_version}.",
        should_write_staging=False,
        should_supersede_previous=False,
        should_quarantine=False,
    )
