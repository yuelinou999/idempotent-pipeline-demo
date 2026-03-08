from enum import Enum


class IngestionStatus(str, Enum):
    ACCEPTED = "accepted"
    SKIPPED = "skipped"
    SUPERSEDED = "superseded"
    QUARANTINED = "quarantined"


class StagingStatus(str, Enum):
    PARTIAL = "partial"
    COMPLETE = "complete"
    SUPERSEDED = "superseded"
    PROMOTED = "promoted"


class ReviewReason(str, Enum):
    HASH_MISMATCH = "same_version_different_payload"
    SCHEMA_VIOLATION = "schema_violation"
    CONFLICT = "peer_system_conflict"
    MANUAL = "manual_flag"


class DecisionOutcome(str, Enum):
    """
    Internal five-branch decision outcomes.
    These are stored verbatim in ingestion_attempt_log for full audit fidelity.
    They are NOT exposed directly in API responses.
    """
    ACCEPT_NEW = "accept_new"
    ACCEPT_SUPERSEDE = "accept_supersede"
    SKIP_DUPLICATE = "skip_duplicate"
    SKIP_STALE = "skip_stale"
    QUARANTINE = "quarantine"


class ApiOutcome(str, Enum):
    """
    Simplified outcome families exposed in API responses.

    Mapping from internal DecisionOutcome:
      accept_new, accept_supersede  → accepted
      skip_duplicate, skip_stale   → skipped
      quarantine                   → quarantined
    """
    ACCEPTED = "accepted"
    SKIPPED = "skipped"
    QUARANTINED = "quarantined"


# Canonical mapping: internal decision → API outcome family
DECISION_TO_API: dict[DecisionOutcome, ApiOutcome] = {
    DecisionOutcome.ACCEPT_NEW: ApiOutcome.ACCEPTED,
    DecisionOutcome.ACCEPT_SUPERSEDE: ApiOutcome.ACCEPTED,
    DecisionOutcome.SKIP_DUPLICATE: ApiOutcome.SKIPPED,
    DecisionOutcome.SKIP_STALE: ApiOutcome.SKIPPED,
    DecisionOutcome.QUARANTINE: ApiOutcome.QUARANTINED,
}

# Canonical mapping: internal decision → reconciliation report family
# Same grouping as API outcome, used by the reconciliation service.
DECISION_TO_REPORT_FAMILY: dict[str, str] = {
    DecisionOutcome.ACCEPT_NEW.value: "accepted",
    DecisionOutcome.ACCEPT_SUPERSEDE.value: "accepted",
    DecisionOutcome.SKIP_DUPLICATE.value: "skipped",
    DecisionOutcome.SKIP_STALE.value: "skipped",
    DecisionOutcome.QUARANTINE.value: "quarantined",
}

