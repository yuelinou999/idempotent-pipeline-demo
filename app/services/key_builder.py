"""
key_builder — deterministic key derivation for the two-key identity model.

Two distinct keys serve two distinct purposes:

  entity_key      = {source_system}:::{entity_type}:::{business_key}
  idempotent_key  = {entity_key}:::{version}

entity_key
  Used for current-state lookup in idempotent_state.
  Answers: "What is the latest known version of this entity from this system?"
  One row per entity — updated in place when a newer version arrives.

idempotent_key
  Used for append-only attempt logging and version-specific traceability.
  Answers: "Has this exact version of this entity been processed before?"
  Stored in ingestion_attempt_log alongside entity_key.

Design rules:
  - Both keys are deterministic: same inputs always produce the same key.
  - Both keys are collision-free: different entities produce different keys.
  - If the business_key contains the delimiter, it is Base64-encoded to prevent parse errors.
  - If the resulting key exceeds MAX_KEY_LENGTH, the business_key segment is SHA-256 hashed.
"""

import base64
import hashlib

DELIMITER = ":::"
MAX_KEY_LENGTH = 512
_SAFE_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.")


def _sanitize_business_key(business_key: str) -> str:
    """
    If the business key contains the delimiter or unsafe characters, Base64-encode it.
    This eliminates all parse ambiguity.
    """
    if DELIMITER in business_key or not all(c in _SAFE_CHARS for c in business_key):
        encoded = base64.urlsafe_b64encode(business_key.encode()).decode().rstrip("=")
        return f"b64:{encoded}"
    return business_key


def _hash_segment(segment: str) -> str:
    """SHA-256 hash a key segment when it would cause the total key to exceed MAX_KEY_LENGTH."""
    return "sha256:" + hashlib.sha256(segment.encode()).hexdigest()[:32]


def build_entity_key(
    source_system: str,
    entity_type: str,
    business_key: str,
) -> str:
    """
    Build the entity key — the stable identifier for a source entity across all versions.

    Used as the primary key for idempotent_state lookups.

    Examples:
        >>> build_entity_key("oms", "order", "ORD-20260301-0042")
        'oms:::order:::ORD-20260301-0042'
    """
    safe_key = _sanitize_business_key(business_key)
    parts = [source_system, entity_type, safe_key]
    key = DELIMITER.join(parts)

    if len(key) > MAX_KEY_LENGTH:
        hashed = _hash_segment(safe_key)
        key = DELIMITER.join([source_system, entity_type, hashed])

    return key


def build_idempotent_key(
    source_system: str,
    entity_type: str,
    business_key: str,
    version: int,
) -> str:
    """
    Build the versioned idempotent key for a specific message version.

    Used in the attempt log for version-specific traceability.
    Format: {entity_key}:::{version}

    Examples:
        >>> build_idempotent_key("oms", "order", "ORD-20260301-0042", 3)
        'oms:::order:::ORD-20260301-0042:::3'
    """
    entity_key = build_entity_key(source_system, entity_type, business_key)
    key = DELIMITER.join([entity_key, str(version)])

    # entity_key already handles length overflow internally;
    # adding :::version is always safe within the 512-char budget
    return key


def parse_idempotent_key(key: str) -> dict[str, str]:
    """
    Parse a versioned idempotent key back into its components.
    Raises ValueError if the key does not have the expected 4-part structure.
    """
    parts = key.split(DELIMITER)
    if len(parts) != 4:
        raise ValueError(f"Invalid idempotent key format: {key!r}")
    return {
        "source_system": parts[0],
        "entity_type": parts[1],
        "business_key": parts[2],
        "version": parts[3],
    }


def parse_entity_key(key: str) -> dict[str, str]:
    """
    Parse an entity key back into its components.
    Raises ValueError if the key does not have the expected 3-part structure.
    """
    parts = key.split(DELIMITER)
    if len(parts) != 3:
        raise ValueError(f"Invalid entity key format: {key!r}")
    return {
        "source_system": parts[0],
        "entity_type": parts[1],
        "business_key": parts[2],
    }
