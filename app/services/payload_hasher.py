"""
payload_hasher — deterministic payload fingerprinting.

Used to detect:
  - Exact duplicates: same key + same version + same hash → skip
  - Anomalies:        same key + same version + different hash → quarantine

The hash is computed over a canonicalized (key-sorted) JSON representation
so that field ordering differences in the payload do not produce different hashes.
"""

import hashlib
import json
from typing import Any


def compute_payload_hash(payload: dict[str, Any]) -> str:
    """
    Compute a stable SHA-256 fingerprint of a payload dict.

    Keys are sorted recursively so that {"b": 1, "a": 2} and {"a": 2, "b": 1}
    produce identical hashes.

    Returns a 64-character hex string.
    """
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def compute_string_hash(raw: str) -> str:
    """Hash a pre-serialized string payload."""
    return hashlib.sha256(raw.encode()).hexdigest()
