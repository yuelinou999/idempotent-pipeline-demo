"""
Unit tests for retry_classifier.

Pure synchronous tests — no DB, no fixtures.
Each test exercises one decision branch of classify_for_retry().
"""

from app.core.enums import RetryDecision
from app.services.retry_classifier import DownstreamError, classify_for_retry


def _error(
    http_status: int | None = None,
    is_timeout: bool = False,
    attempt_number: int = 1,
    max_attempts: int = 3,
) -> DownstreamError:
    return DownstreamError(
        http_status=http_status,
        is_timeout=is_timeout,
        attempt_number=attempt_number,
        max_attempts=max_attempts,
    )


# ---------------------------------------------------------------------------
# Timeout → SEND_TO_AMBIGUOUS_QUEUE
# ---------------------------------------------------------------------------

class TestTimeoutClassification:
    def test_timeout_sends_to_ambiguous_queue(self):
        result = classify_for_retry(_error(is_timeout=True))
        assert result == RetryDecision.SEND_TO_AMBIGUOUS_QUEUE

    def test_timeout_takes_priority_over_attempt_ceiling(self):
        # Even when attempt_number == max_attempts, timeout → ambiguous, not DLQ.
        result = classify_for_retry(_error(is_timeout=True, attempt_number=3, max_attempts=3))
        assert result == RetryDecision.SEND_TO_AMBIGUOUS_QUEUE


# ---------------------------------------------------------------------------
# Non-retryable errors → SEND_TO_DLQ
# ---------------------------------------------------------------------------

class TestNonRetryableClassification:
    def test_400_sends_to_dlq(self):
        result = classify_for_retry(_error(http_status=400))
        assert result == RetryDecision.SEND_TO_DLQ

    def test_422_sends_to_dlq(self):
        result = classify_for_retry(_error(http_status=422))
        assert result == RetryDecision.SEND_TO_DLQ

    def test_409_sends_to_dlq(self):
        # Business conflict — retrying reproduces the conflict.
        result = classify_for_retry(_error(http_status=409))
        assert result == RetryDecision.SEND_TO_DLQ

    def test_non_retryable_on_first_attempt_still_sends_to_dlq(self):
        # Attempt ceiling is irrelevant for non-retryable errors.
        result = classify_for_retry(_error(http_status=400, attempt_number=1, max_attempts=5))
        assert result == RetryDecision.SEND_TO_DLQ


# ---------------------------------------------------------------------------
# Retry ceiling → SEND_TO_DLQ
# ---------------------------------------------------------------------------

class TestRetryCeilingClassification:
    def test_attempt_at_max_sends_to_dlq(self):
        result = classify_for_retry(_error(http_status=503, attempt_number=3, max_attempts=3))
        assert result == RetryDecision.SEND_TO_DLQ

    def test_attempt_exceeds_max_sends_to_dlq(self):
        result = classify_for_retry(_error(http_status=503, attempt_number=4, max_attempts=3))
        assert result == RetryDecision.SEND_TO_DLQ

    def test_below_ceiling_is_still_retryable(self):
        result = classify_for_retry(_error(http_status=503, attempt_number=2, max_attempts=3))
        assert result == RetryDecision.RETRY_WITH_BACKOFF


# ---------------------------------------------------------------------------
# Transient failures → RETRY_WITH_BACKOFF
# ---------------------------------------------------------------------------

class TestRetryWithBackoffClassification:
    def test_503_below_ceiling_retries(self):
        result = classify_for_retry(_error(http_status=503, attempt_number=1, max_attempts=3))
        assert result == RetryDecision.RETRY_WITH_BACKOFF

    def test_500_below_ceiling_retries(self):
        result = classify_for_retry(_error(http_status=500, attempt_number=1, max_attempts=3))
        assert result == RetryDecision.RETRY_WITH_BACKOFF

    def test_429_rate_limit_retries(self):
        result = classify_for_retry(_error(http_status=429, attempt_number=1, max_attempts=3))
        assert result == RetryDecision.RETRY_WITH_BACKOFF

    def test_first_attempt_503_with_max_5_retries(self):
        result = classify_for_retry(_error(http_status=503, attempt_number=1, max_attempts=5))
        assert result == RetryDecision.RETRY_WITH_BACKOFF


# ---------------------------------------------------------------------------
# Decision priority ordering
# ---------------------------------------------------------------------------

class TestDecisionPriority:
    def test_timeout_check_runs_before_ceiling_check(self):
        """
        Timeout must be classified as SEND_TO_AMBIGUOUS_QUEUE even when the
        attempt ceiling has been reached.  The caller can only query-before-retry
        if it knows the outcome is unknown — routing a timeout directly to DLQ
        would bypass that check.
        """
        result = classify_for_retry(
            _error(is_timeout=True, attempt_number=3, max_attempts=3)
        )
        assert result == RetryDecision.SEND_TO_AMBIGUOUS_QUEUE

    def test_non_retryable_check_runs_before_ceiling_check(self):
        """
        A 400 on the first attempt must go to DLQ immediately, not be retried.
        The ceiling check must not shadow the non-retryable check.
        """
        result = classify_for_retry(
            _error(http_status=400, attempt_number=1, max_attempts=3)
        )
        assert result == RetryDecision.SEND_TO_DLQ
