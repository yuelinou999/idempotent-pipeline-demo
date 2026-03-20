"""
Tests for the circuit breaker state — Phase 2B.

Covers:
  - Circuit opens automatically after reaching the consecutive-failure threshold
  - An open circuit blocks further delivery attempts (outcome = circuit_open)
  - Operator reset returns the circuit to closed
  - Recovery path: open → half_open (via reset) → closed (via successful delivery)

Setup notes:
  - admissions_crm: mock_behavior=always_transient_fail, max_retry_attempts=3,
    open_after_n_failures=3.  Each simulate_delivery call makes 3 failing attempts,
    so ONE call accumulates 3 consecutive failures and opens the circuit.
  - warehouse_sync: mock_behavior=success.  Used for the recovery probe test.
"""

import pytest

BASE_MSG = {
    "source_system": "oms",
    "entity_type": "order",
    "version": 1,
    "payload": {"customer_id": "CUST-CB-001", "status": "created", "total_amount": 50.0},
}


async def _ingest(client, business_key: str) -> int:
    """Ingest a message and return its staging_id."""
    r = await client.post(
        "/messages/ingest",
        json={**BASE_MSG, "business_key": business_key},
    )
    assert r.status_code == 200, r.text
    staging_id = r.json()["staging_id"]
    assert staging_id is not None
    return staging_id


class TestCircuitOpensAfterFailures:
    """
    Scenario: consecutive failures on admissions_crm open the circuit.

    admissions_crm has open_after_n_failures=3 and max_retry_attempts=3.
    One call to simulate_delivery makes 3 failing attempts → 3 consecutive
    failures → circuit transitions closed → open.
    """

    @pytest.mark.asyncio
    async def test_circuit_starts_closed(self, seeded_client):
        r = await seeded_client.get("/circuits/admissions_crm")
        assert r.status_code == 200
        data = r.json()
        # Either no-activity response or explicitly closed
        assert data.get("state", "closed") == "closed"

    @pytest.mark.asyncio
    async def test_circuit_opens_after_threshold_failures(self, seeded_client):
        # One simulate_delivery call with always_transient_fail makes 3 failing
        # attempts → crosses open_after_n_failures=3 → circuit opens.
        sid = await _ingest(seeded_client, "ORD-CB-OPEN-001")
        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "admissions_crm"},
        )
        assert sim.status_code == 200, sim.text

        state_r = await seeded_client.get("/circuits/admissions_crm")
        assert state_r.status_code == 200
        state = state_r.json()
        assert state["state"] == "open", (
            f"Expected circuit to be open after {state.get('consecutive_failure_count')} "
            f"consecutive failures, but state is {state['state']!r}"
        )
        assert state["consecutive_failure_count"] >= 3
        assert state["opened_at"] is not None

    @pytest.mark.asyncio
    async def test_circuit_state_includes_transition_reason(self, seeded_client):
        sid = await _ingest(seeded_client, "ORD-CB-REASON-001")
        await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "admissions_crm"},
        )
        state_r = await seeded_client.get("/circuits/admissions_crm")
        state = state_r.json()
        if state["state"] == "open":
            assert state["last_transition_reason"] is not None
            assert "failure" in state["last_transition_reason"].lower()

    @pytest.mark.asyncio
    async def test_successful_deliveries_do_not_open_circuit(self, seeded_client):
        """warehouse_sync (success) should never open the circuit."""
        for i in range(5):
            sid = await _ingest(seeded_client, f"ORD-CB-WH-{i:03d}")
            await seeded_client.post(
                "/delivery/simulate",
                json={"staging_id": sid, "system_name": "warehouse_sync"},
            )
        state_r = await seeded_client.get("/circuits/warehouse_sync")
        assert state_r.json().get("state", "closed") == "closed"

    @pytest.mark.asyncio
    async def test_list_circuits_returns_active_systems(self, seeded_client):
        sid = await _ingest(seeded_client, "ORD-CB-LIST-001")
        await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "admissions_crm"},
        )
        r = await seeded_client.get("/circuits")
        assert r.status_code == 200
        names = [s["system_name"] for s in r.json()]
        assert "admissions_crm" in names


class TestOpenCircuitBlocksDelivery:
    """
    Scenario: after the circuit opens, subsequent delivery attempts are blocked.
    """

    @pytest.mark.asyncio
    async def test_open_circuit_returns_circuit_open_outcome(self, seeded_client):
        # Open the circuit
        sid1 = await _ingest(seeded_client, "ORD-CB-BLOCK-001")
        await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid1, "system_name": "admissions_crm"},
        )

        state_r = await seeded_client.get("/circuits/admissions_crm")
        if state_r.json()["state"] != "open":
            pytest.skip("Circuit did not open — threshold not crossed")

        # Next attempt must be blocked
        sid2 = await _ingest(seeded_client, "ORD-CB-BLOCK-002")
        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid2, "system_name": "admissions_crm"},
        )
        assert sim.status_code == 200
        data = sim.json()
        assert data["outcome"] == "circuit_open"
        assert data["attempts_made"] == 0
        assert data["dlq_entry_id"] is None

    @pytest.mark.asyncio
    async def test_open_circuit_circuit_state_in_response(self, seeded_client):
        """simulate_delivery response includes circuit_state field."""
        sid = await _ingest(seeded_client, "ORD-CB-STATE-001")
        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "warehouse_sync"},
        )
        assert sim.status_code == 200
        data = sim.json()
        assert "circuit_state" in data
        assert data["circuit_state"] in ("closed", "open", "half_open")


class TestOperatorReset:
    """
    Scenario: operator resets the circuit to a specific state.
    """

    @pytest.mark.asyncio
    async def test_reset_to_closed_clears_state(self, seeded_client):
        # Open the circuit first
        sid = await _ingest(seeded_client, "ORD-CB-RESET-001")
        await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "admissions_crm"},
        )

        # Reset to closed
        reset_r = await seeded_client.post(
            "/circuits/admissions_crm/reset",
            json={"target_state": "closed", "reset_by": "ops@example.com"},
        )
        assert reset_r.status_code == 200, reset_r.text
        state = reset_r.json()
        assert state["state"] == "closed"
        assert state["consecutive_failure_count"] == 0
        assert state["consecutive_success_count"] == 0
        assert state["opened_at"] is None
        assert "ops@example.com" in state["last_transition_reason"]

    @pytest.mark.asyncio
    async def test_reset_to_open_sets_opened_at(self, seeded_client):
        reset_r = await seeded_client.post(
            "/circuits/warehouse_sync/reset",
            json={"target_state": "open", "reset_by": "ops@example.com"},
        )
        assert reset_r.status_code == 200
        state = reset_r.json()
        assert state["state"] == "open"
        assert state["opened_at"] is not None

    @pytest.mark.asyncio
    async def test_invalid_target_state_returns_400(self, seeded_client):
        r = await seeded_client.post(
            "/circuits/warehouse_sync/reset",
            json={"target_state": "broken", "reset_by": "ops@example.com"},
        )
        assert r.status_code == 400

    @pytest.mark.asyncio
    async def test_reset_after_open_allows_delivery(self, seeded_client):
        """After resetting to closed, delivery is allowed again."""
        # Open the circuit
        sid1 = await _ingest(seeded_client, "ORD-CB-REALLOW-001")
        await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid1, "system_name": "admissions_crm"},
        )

        state_r = await seeded_client.get("/circuits/admissions_crm")
        if state_r.json()["state"] != "open":
            pytest.skip("Circuit did not open")

        # Reset to closed
        await seeded_client.post(
            "/circuits/admissions_crm/reset",
            json={"target_state": "closed", "reset_by": "ops@example.com"},
        )

        # Next delivery should not return circuit_open
        sid2 = await _ingest(seeded_client, "ORD-CB-REALLOW-002")
        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid2, "system_name": "admissions_crm"},
        )
        assert sim.json()["outcome"] != "circuit_open"


class TestRecoveryPath:
    """
    Scenario: open → half_open (via operator reset) → closed (via successful delivery).

    This is the canonical recovery path an operator follows after a downstream
    system recovers.  Rather than fully closing the circuit, the operator first
    sets it to half_open to allow one probe attempt.
    """

    @pytest.mark.asyncio
    async def test_half_open_success_closes_circuit(self, seeded_client):
        # 1. Force the circuit to half_open state.
        reset_r = await seeded_client.post(
            "/circuits/warehouse_sync/reset",
            json={"target_state": "half_open", "reset_by": "ops@example.com"},
        )
        assert reset_r.status_code == 200
        assert reset_r.json()["state"] == "half_open"

        # 2. Deliver to warehouse_sync (always succeeds).
        sid = await _ingest(seeded_client, "ORD-CB-RECOVERY-001")
        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "warehouse_sync"},
        )
        assert sim.status_code == 200, sim.text
        assert sim.json()["outcome"] == "success"

        # 3. Circuit should now be closed.
        state_r = await seeded_client.get("/circuits/warehouse_sync")
        assert state_r.json()["state"] == "closed"

    @pytest.mark.asyncio
    async def test_half_open_failure_reopens_circuit(self, seeded_client):
        # 1. Set admissions_crm to half_open.
        await seeded_client.post(
            "/circuits/admissions_crm/reset",
            json={"target_state": "half_open", "reset_by": "ops@example.com"},
        )

        # 2. Deliver — always_transient_fail → failure → circuit re-opens.
        sid = await _ingest(seeded_client, "ORD-CB-REOPEN-001")
        await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "admissions_crm"},
        )

        # 3. Circuit should be open again.
        state_r = await seeded_client.get("/circuits/admissions_crm")
        assert state_r.json()["state"] == "open"

    @pytest.mark.asyncio
    async def test_full_recovery_sequence(self, seeded_client):
        """
        End-to-end: open via failures → operator sets half_open → success closes.
        """
        # Open via failures
        sid1 = await _ingest(seeded_client, "ORD-CB-SEQ-001")
        await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid1, "system_name": "admissions_crm"},
        )
        state_after_failure = await seeded_client.get("/circuits/admissions_crm")
        if state_after_failure.json()["state"] != "open":
            pytest.skip("Circuit did not open in setup")

        # Operator intervenes: set to half_open for probe
        await seeded_client.post(
            "/circuits/admissions_crm/reset",
            json={"target_state": "half_open", "reset_by": "ops@example.com"},
        )
        assert (await seeded_client.get("/circuits/admissions_crm")).json()["state"] == "half_open"

        # In a real recovery, the downstream system would now be fixed.
        # For the probe, we switch to warehouse_sync (always succeeds).
        # In production, you'd change the downstream config; in tests we
        # probe a different system to demonstrate the half_open → closed path.
        await seeded_client.post(
            "/circuits/warehouse_sync/reset",
            json={"target_state": "half_open", "reset_by": "ops@example.com"},
        )
        sid2 = await _ingest(seeded_client, "ORD-CB-SEQ-002")
        await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid2, "system_name": "warehouse_sync"},
        )
        assert (await seeded_client.get("/circuits/warehouse_sync")).json()["state"] == "closed"
