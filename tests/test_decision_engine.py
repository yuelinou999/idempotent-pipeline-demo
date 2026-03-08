"""
Unit tests for the decision engine.

Pure unit tests — no database. The decision engine is a pure function that takes
an existing state (or None) and returns a Decision. Tests validate all five branches.
"""

from unittest.mock import MagicMock

from app.core.enums import DecisionOutcome
from app.services.decision_engine import make_decision


def _mock_state(version: int, payload_hash: str) -> MagicMock:
    """Create a minimal mock IdempotentState with just the fields decision engine reads."""
    state = MagicMock()
    state.current_version = version
    state.payload_hash = payload_hash
    return state


HASH_A = "a" * 64
HASH_B = "b" * 64


class TestDecisionEngine:

    def test_first_seen_returns_accept_new(self):
        """Branch 1: entity_key not in state table → ACCEPT_NEW."""
        decision = make_decision(incoming_version=1, incoming_hash=HASH_A, existing_state=None)
        assert decision.outcome == DecisionOutcome.ACCEPT_NEW
        assert decision.should_write_staging is True
        assert decision.should_supersede_previous is False
        assert decision.should_quarantine is False

    def test_newer_version_returns_accept_supersede(self):
        """Branch 2: incoming version > current version → ACCEPT_SUPERSEDE."""
        existing = _mock_state(version=1, payload_hash=HASH_A)
        decision = make_decision(incoming_version=2, incoming_hash=HASH_B, existing_state=existing)
        assert decision.outcome == DecisionOutcome.ACCEPT_SUPERSEDE
        assert decision.should_write_staging is True
        assert decision.should_supersede_previous is True
        assert decision.should_quarantine is False

    def test_exact_duplicate_returns_skip_duplicate(self):
        """Branch 3: same version + same hash → SKIP_DUPLICATE."""
        existing = _mock_state(version=1, payload_hash=HASH_A)
        decision = make_decision(incoming_version=1, incoming_hash=HASH_A, existing_state=existing)
        assert decision.outcome == DecisionOutcome.SKIP_DUPLICATE
        assert decision.should_write_staging is False
        assert decision.should_quarantine is False

    def test_same_version_different_hash_returns_quarantine(self):
        """Branch 4: same version + different hash → QUARANTINE (data anomaly)."""
        existing = _mock_state(version=1, payload_hash=HASH_A)
        decision = make_decision(incoming_version=1, incoming_hash=HASH_B, existing_state=existing)
        assert decision.outcome == DecisionOutcome.QUARANTINE
        assert decision.should_quarantine is True
        assert decision.should_write_staging is False
        assert decision.should_supersede_previous is False

    def test_older_version_returns_skip_stale(self):
        """Branch 5: incoming version < current version → SKIP_STALE."""
        existing = _mock_state(version=3, payload_hash=HASH_A)
        decision = make_decision(incoming_version=2, incoming_hash=HASH_A, existing_state=existing)
        assert decision.outcome == DecisionOutcome.SKIP_STALE
        assert decision.should_write_staging is False
        assert decision.should_quarantine is False

    def test_much_older_version_is_also_stale(self):
        existing = _mock_state(version=10, payload_hash=HASH_A)
        decision = make_decision(incoming_version=1, incoming_hash=HASH_B, existing_state=existing)
        assert decision.outcome == DecisionOutcome.SKIP_STALE

    def test_v3_arrives_before_v2_then_v2_is_stale(self):
        """
        Simulates out-of-order arrival: V3 accepted first, then V2 arrives.
        After V3 is stored, the state has current_version=3.
        V2 incoming < 3 → SKIP_STALE.
        This test validates the entity-level state lookup model is correct.
        """
        # After V3 accepted, state reflects version=3
        state_after_v3 = _mock_state(version=3, payload_hash=HASH_A)
        decision_for_v2 = make_decision(
            incoming_version=2, incoming_hash=HASH_B, existing_state=state_after_v3
        )
        assert decision_for_v2.outcome == DecisionOutcome.SKIP_STALE

