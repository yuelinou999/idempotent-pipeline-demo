"""
retry_classifier — pure function that classifies a downstream delivery failure.

Given the outcome of a single delivery attempt, returns a RetryDecision that
tells the delivery orchestrator what to do next.

This is a pure function with no side effects and no database access.
All state is passed in via DownstreamError; the caller owns the retry loop.

Decision rules (applied in order):
  1. Timeout (no HTTP response)          → SEND_TO_AMBIGUOUS_QUEUE
     The downstream may or may not have processed the request.
     The outcome is genuinely unknown.  The delivery flow creates an
     AmbiguousOutcomeQueue entry; use POST /dlq/ambiguous/{id}/query-status
     to resolve via query-before-retry.

  2. HTTP 400 or 422 (malformed message) → SEND_TO_DLQ
     The message itself is the problem. Retrying will reproduce the error.

  3. HTTP 409 (business conflict)        → SEND_TO_DLQ
     Retrying will reproduce the conflict.

  4. attempt_number >= max_attempts      → SEND_TO_DLQ
     Automation has reached its ceiling. Human decision required.

  5. All other cases                     → RETRY_WITH_BACKOFF
     Assumed transient. Caller applies exponential backoff before next attempt.

Note on HTTP 200 with business-level errors:
  Some downstream systems return HTTP 200 with an error code in the response
  body. The caller is responsible for parsing the response body and passing
  a synthetic http_status that reflects the business outcome before calling
  classify_for_retry.
"""

from dataclasses import dataclass

from app.core.enums import RetryDecision


@dataclass(frozen=True)
class DownstreamError:
    """
    Describes the outcome of a single delivery attempt.

    http_status    — HTTP status code returned by the downstream.
                     None when is_timeout is True.
    is_timeout     — True when the request timed out with no response.
    attempt_number — 1-based index of this attempt (1 = first try).
    max_attempts   — ceiling configured for this downstream system.
    """
    http_status: int | None
    is_timeout: bool
    attempt_number: int
    max_attempts: int


def classify_for_retry(error: DownstreamError) -> RetryDecision:
    """
    Classify a delivery failure and return the appropriate retry decision.

    Pure function — no side effects, no I/O.
    """
    # Timeout: outcome unknown — persisted in AmbiguousOutcomeQueue for query-before-retry
    if error.is_timeout:
        return RetryDecision.SEND_TO_AMBIGUOUS_QUEUE

    # Non-retryable HTTP errors: retrying reproduces the problem
    if error.http_status in (400, 422):
        return RetryDecision.SEND_TO_DLQ

    if error.http_status == 409:
        return RetryDecision.SEND_TO_DLQ

    # Retry ceiling reached: hand off to operator
    if error.attempt_number >= error.max_attempts:
        return RetryDecision.SEND_TO_DLQ

    # All other cases: transient, retry with backoff
    return RetryDecision.RETRY_WITH_BACKOFF
