"""
correlation_engine — resolves Correlation IDs for incoming messages.

The Correlation ID answers: "Which business transaction does this record belong to?"
This is entirely separate from the idempotent key.

Resolution strategy (in priority order):
  1. Existing mapping: this source_key is already mapped → return existing correlation_id.
  2. Explicit reference: the payload contains a field that directly references another
     system's key (e.g. payload["oms_order_id"] = "ORD-2026-0042").
  3. Rule-based matching: matching rules defined per source system.
  4. Self-generated: no match found → generate a new Correlation ID and anchor it.

Uniqueness guarantee
--------------------
CorrelationMap has a DB-level UniqueConstraint on (source_system, source_key).
_create_mapping handles IntegrityError from concurrent duplicate attempts by
fetching and returning the already-committed mapping instead of raising.
"""

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.correlation import CorrelationMap

# ---------------------------------------------------------------------------
# Explicit reference field mappings
# Each entry: source_system → [(payload_field, target_source_system)]
# ---------------------------------------------------------------------------
EXPLICIT_REFERENCE_FIELDS: dict[str, list[tuple[str, str]]] = {
    "wms": [("oms_order_id", "oms")],
    "finance": [("oms_order_id", "oms"), ("wms_ticket_id", "wms")],
    "crm": [("oms_order_id", "oms")],
}

# ---------------------------------------------------------------------------
# Rule-based matching fields
# Each entry: source_system → list of fields that must all match
# ---------------------------------------------------------------------------
RULE_BASED_MATCH_FIELDS: dict[str, list[str]] = {
    "oms": ["customer_id", "order_date", "total_amount"],
    "wms": ["customer_id", "order_date"],
    "finance": ["customer_id", "invoice_date", "total_amount"],
    "crm": ["customer_id", "interaction_date"],
}


async def resolve_correlation_id(
    session: AsyncSession,
    source_system: str,
    business_key: str,
    payload: dict[str, Any],
) -> tuple[str, str]:
    """
    Resolve or create a Correlation ID for an incoming message.

    Returns:
        (correlation_id, mapping_method) where mapping_method is one of:
        'existing', 'explicit', 'rule_based', 'self_generated'
    """

    # 1. Check if this source_key is already mapped
    existing = await session.scalar(
        select(CorrelationMap).where(
            CorrelationMap.source_system == source_system,
            CorrelationMap.source_key == business_key,
        )
    )
    if existing:
        return existing.correlation_id, "existing"

    # 2. Try explicit reference fields
    for field_name, ref_source in EXPLICIT_REFERENCE_FIELDS.get(source_system, []):
        ref_key = payload.get(field_name)
        if ref_key:
            anchor = await session.scalar(
                select(CorrelationMap).where(
                    CorrelationMap.source_system == ref_source,
                    CorrelationMap.source_key == str(ref_key),
                )
            )
            if anchor:
                corr_id = await _create_mapping(
                    session, anchor.correlation_id, source_system, business_key, "explicit"
                )
                return corr_id, "explicit"

    # 3. Try rule-based matching
    match_fields = RULE_BASED_MATCH_FIELDS.get(source_system, [])
    if match_fields and all(payload.get(f) is not None for f in match_fields):
        match_values = {f: payload[f] for f in match_fields}
        candidate = await _find_rule_based_match(session, source_system, match_values)
        if candidate:
            corr_id = await _create_mapping(
                session, candidate, source_system, business_key, "rule_based"
            )
            return corr_id, "rule_based"

    # 4. Self-generate — this record anchors a new transaction
    new_id = str(uuid.uuid4())
    corr_id = await _create_mapping(session, new_id, source_system, business_key, "self_generated")
    return corr_id, "self_generated"


async def _create_mapping(
    session: AsyncSession,
    correlation_id: str,
    source_system: str,
    source_key: str,
    method: str,
) -> str:
    """
    Insert a new CorrelationMap row.

    Returns the correlation_id that is now canonical for this (source_system, source_key).

    If a concurrent writer already inserted a row for the same (source_system, source_key)
    and the DB UniqueConstraint fires, we catch the IntegrityError, roll back the failed
    savepoint, and return the already-committed mapping's correlation_id instead.
    """
    mapping = CorrelationMap(
        correlation_id=correlation_id,
        source_system=source_system,
        source_key=source_key,
        mapping_method=method,
    )
    try:
        # Use a nested transaction (SAVEPOINT) so we can roll back just this
        # insert without invalidating the outer session on IntegrityError.
        async with session.begin_nested():
            session.add(mapping)
    except IntegrityError:
        # Another writer beat us to it — fetch the canonical mapping that won.
        existing = await session.scalar(
            select(CorrelationMap).where(
                CorrelationMap.source_system == source_system,
                CorrelationMap.source_key == source_key,
            )
        )
        if existing:
            return existing.correlation_id
        # Should not happen, but propagate if it does
        raise

    return correlation_id


async def _find_rule_based_match(
    session: AsyncSession,
    source_system: str,
    match_values: dict[str, Any],
) -> str | None:
    """
    TODO: Implement rule-based matching by querying staging records for
    records from the same source system with matching field values.

    For the MVP, this is a placeholder that always returns None (falls through
    to self-generated). A production implementation would query staging_record
    payloads or a dedicated match index table.
    """
    return None
