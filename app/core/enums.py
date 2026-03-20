from enum import Enum


# ---------------------------------------------------------------------------
# Context 2: Delivery Resilience enums
# ---------------------------------------------------------------------------


class DeliveryClassification(str, Enum):
    """
    How the retry_classifier categorises a failed delivery attempt.

    RETRYABLE:     transient failure; retry with backoff is safe
    NON_RETRYABLE: permanent failure (malformed message, business conflict);
                   retry will reproduce the error
    UNKNOWN:       timeout with no response; outcome cannot be determined
                   without a query-before-retry step; recorded in
                   AmbiguousOutcomeQueue for operator investigation
    """
    RETRYABLE = "retryable"
    NON_RETRYABLE = "non_retryable"
    UNKNOWN = "unknown"


class DeliveryOutcome(str, Enum):
    """Terminal outcome of a single delivery attempt."""
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"


class RetryDecision(str, Enum):
    """
    Decision returned by retry_classifier for a failed delivery attempt.

    RETRY_WITH_BACKOFF      — transient failure, safe to retry
    SEND_TO_DLQ             — non-retryable or ceiling reached, hand off to operator
    SEND_TO_AMBIGUOUS_QUEUE — timeout; outcome unknown; record is persisted in
                              AmbiguousOutcomeQueue for operator investigation
                              and query-before-retry resolution.
    """
    RETRY_WITH_BACKOFF = "retry_with_backoff"
    SEND_TO_DLQ = "send_to_dlq"
    SEND_TO_AMBIGUOUS_QUEUE = "send_to_ambiguous_queue"


class DLQFailureCategory(str, Enum):
    """
    Why a staging record landed in the DeliveryDLQ.

    TRANSIENT_EXHAUSTED — retry ceiling reached without a successful delivery
    NON_RETRYABLE       — HTTP 400 / 422 / 409; retrying would reproduce the error
    """
    TRANSIENT_EXHAUSTED = "transient_exhausted"
    NON_RETRYABLE = "non_retryable"


class DLQResolutionStatus(str, Enum):
    """Current resolution state of a DeliveryDLQ entry."""
    PENDING = "pending"
    RESOLVED = "resolved"
    MANUALLY_REPLAYED = "manually_replayed"


class AmbiguityCategory(str, Enum):
    """
    Why a delivery attempt ended up in AmbiguousOutcomeQueue.

    TIMEOUT          — the downstream did not respond within the allowed window;
                       outcome is unknown
    UNKNOWN_RESPONSE — the downstream responded but the result could not be
                       classified as success or failure
    """
    TIMEOUT = "timeout"
    UNKNOWN_RESPONSE = "unknown_response"


class AmbiguousStatus(str, Enum):
    """
    Resolution lifecycle state of an AmbiguousOutcomeQueue entry.

    PENDING              — entry created; status query not yet run
    CONFIRMED_DELIVERED  — downstream confirmed the request was processed;
                           no retry needed; entry is closed
    RETRY_ELIGIBLE       — downstream has no record of the request;
                           safe to retry; entry is closed pending operator action
    STATUS_UNAVAILABLE   — downstream does not support status queries or the
                           query itself failed; entry remains unresolved
    """
    PENDING = "pending"
    CONFIRMED_DELIVERED = "confirmed_delivered"
    RETRY_ELIGIBLE = "retry_eligible"
    STATUS_UNAVAILABLE = "status_unavailable"


class MockStatusQueryBehavior(str, Enum):
    """
    Configurable mock response for query-before-retry status checks.
    Stored on DownstreamSystem.mock_status_query_behavior.

    CONFIRMED_DELIVERED  — downstream confirms the request was processed
    NOT_FOUND            — downstream has no record; safe to retry
    STATUS_UNAVAILABLE   — downstream cannot confirm the outcome
    """
    CONFIRMED_DELIVERED = "confirmed_delivered"
    NOT_FOUND = "not_found"
    STATUS_UNAVAILABLE = "status_unavailable"


class ReplaySelectionMode(str, Enum):
    """
    How the operator selected records for a ReplayRequest.

    DLQ_ITEMS              — replay from DeliveryDLQ entries that are
                             pending (not yet resolved or replayed)
    AMBIGUOUS_ITEMS        — replay from AmbiguousOutcomeQueue entries
                             whose query-before-retry returned retry_eligible
    """
    DLQ_ITEMS = "dlq_items"
    AMBIGUOUS_ITEMS = "ambiguous_items"


class ReplayStatus(str, Enum):
    """Lifecycle state of a ReplayRequest."""
    PENDING = "pending"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"


class ReplayItemStatus(str, Enum):
    """
    Per-item outcome after replay execution.

    PENDING           — item not yet executed
    SUCCESS           — delivery succeeded
    DLQ               — delivery failed and entry moved to DLQ again
    QUEUED_AMBIGUOUS  — delivery timed out; new AmbiguousOutcomeQueue entry created
    CIRCUIT_OPEN      — circuit was open; no attempt made
    ERROR             — unexpected error during execution
    """
    PENDING = "pending"
    SUCCESS = "success"
    DLQ = "dlq"
    QUEUED_AMBIGUOUS = "queued_ambiguous"
    CIRCUIT_OPEN = "circuit_open"
    ERROR = "error"


class CircuitState(str, Enum):
    """
    Three states of the per-downstream circuit breaker.

    CLOSED    — normal operation; delivery attempts are allowed.
    OPEN      — downstream is degraded; delivery attempts are blocked.
    HALF_OPEN — recovery probe state; one delivery attempt is allowed.
                On success → CLOSED.  On any failure → OPEN.

    Transitions:
      CLOSED    → OPEN      when consecutive_failure_count reaches open_after_n_failures
      OPEN      → HALF_OPEN via operator reset only (POST /circuits/{name}/reset)
      HALF_OPEN → CLOSED    after one successful delivery attempt
      HALF_OPEN → OPEN      after any failed delivery attempt
    """
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class MockBehavior(str, Enum):
    """
    Configurable delivery behaviour for demo / test purposes.
    Stored on DownstreamSystem.mock_behavior and interpreted by delivery_service.

    SUCCESS:               HTTP 200 on first attempt
    ALWAYS_TRANSIENT_FAIL: HTTP 503 on every attempt → exhausts retry ceiling → DLQ
    NON_RETRYABLE_400:     HTTP 400 on first attempt → immediate DLQ
    CONFLICT_409:          HTTP 409 on first attempt → immediate DLQ
    TIMEOUT:               Simulated timeout → outcome unknown → AmbiguousOutcomeQueue
    """
    SUCCESS = "success"
    ALWAYS_TRANSIENT_FAIL = "always_transient_fail"
    NON_RETRYABLE_400 = "non_retryable_400"
    CONFLICT_409 = "conflict_409"
    TIMEOUT = "timeout"


# ---------------------------------------------------------------------------
# Context 1: Ingestion Correctness enums
# ---------------------------------------------------------------------------


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
