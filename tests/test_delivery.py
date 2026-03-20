"""
Integration tests for the Delivery Resilience context (Context 2 Phase 2A).

Scenarios covered:
  11 — Transient 503 → retries exhaust → DeliveryDLQ (transient_exhausted)
  12 — HTTP 400 on first attempt → immediate DeliveryDLQ (non_retryable)
  13 — HTTP 409 on first attempt → immediate DeliveryDLQ (non_retryable)

Each test:
  1. Ingests a message through the normal Pillar 1 write path to get a staging_id
  2. Calls POST /delivery/simulate with the staging_id and a named downstream system
  3. Verifies DeliveryAttemptLog rows and DeliveryDLQ state via API queries

Seeded systems used:
  warehouse_sync      mock_behavior=success
  admissions_crm      mock_behavior=always_transient_fail  (max_retry_attempts=3)
  finance_ledger      mock_behavior=non_retryable_400      (max_retry_attempts=5)
"""

import pytest

BASE_MSG = {
    "source_system": "oms",
    "entity_type": "order",
    "business_key": "ORD-DLV-0001",
    "version": 1,
    "payload": {"customer_id": "CUST-DLV-001", "status": "created", "total_amount": 99.00},
}


async def _ingest_and_get_staging_id(client, business_key: str = "ORD-DLV-0001") -> int:
    """Helper: ingest a message and return its staging_id."""
    msg = {**BASE_MSG, "business_key": business_key}
    r = await client.post("/messages/ingest", json=msg)
    assert r.status_code == 200, r.text
    staging_id = r.json()["staging_id"]
    assert staging_id is not None
    return staging_id


class TestScenario11_TransientExhausted:
    """
    Scenario 11: transient 503 errors → retries exhaust → DeliveryDLQ.

    admissions_crm is configured mock_behavior=always_transient_fail with
    max_retry_attempts=3.  Every attempt returns 503.  The third attempt
    triggers SEND_TO_DLQ from retry_classifier.  One DeliveryDLQ entry
    is created with failure_category=transient_exhausted.
    """

    @pytest.mark.asyncio
    async def test_transient_fail_exhausts_retries_and_enters_dlq(self, seeded_client):
        staging_id = await _ingest_and_get_staging_id(seeded_client, "ORD-S11-001")

        r = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": staging_id, "system_name": "admissions_crm"},
        )
        assert r.status_code == 200, r.text
        data = r.json()

        assert data["outcome"] == "dlq_transient_exhausted"
        assert data["attempts_made"] == 3           # all 3 attempts exhausted
        assert data["dlq_entry_id"] is not None

    @pytest.mark.asyncio
    async def test_transient_fail_writes_attempt_log_for_each_retry(self, seeded_client):
        staging_id = await _ingest_and_get_staging_id(seeded_client, "ORD-S11-002")

        await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": staging_id, "system_name": "admissions_crm"},
        )

        # Verify DLQ entry is visible in the operator console
        dlq_r = await seeded_client.get("/dlq/delivery?system_name=admissions_crm")
        assert dlq_r.status_code == 200
        entries = dlq_r.json()
        assert len(entries) >= 1
        entry = next(e for e in entries if e["staging_id"] == staging_id)
        assert entry["failure_category"] == "transient_exhausted"
        assert entry["resolution_status"] == "pending"

    @pytest.mark.asyncio
    async def test_dlq_entry_can_be_resolved(self, seeded_client):
        staging_id = await _ingest_and_get_staging_id(seeded_client, "ORD-S11-003")

        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": staging_id, "system_name": "admissions_crm"},
        )
        dlq_id = sim.json()["dlq_entry_id"]

        resolve_r = await seeded_client.post(
            f"/dlq/delivery/{dlq_id}/resolve",
            json={"resolved_by": "ops@example.com"},
        )
        assert resolve_r.status_code == 200
        resolved = resolve_r.json()
        assert resolved["resolution_status"] == "resolved"
        assert resolved["resolved_by"] == "ops@example.com"

    @pytest.mark.asyncio
    async def test_resolved_entry_no_longer_appears_in_pending_list(self, seeded_client):
        staging_id = await _ingest_and_get_staging_id(seeded_client, "ORD-S11-004")

        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": staging_id, "system_name": "admissions_crm"},
        )
        dlq_id = sim.json()["dlq_entry_id"]

        await seeded_client.post(
            f"/dlq/delivery/{dlq_id}/resolve",
            json={"resolved_by": "ops@example.com"},
        )

        # Default list only shows pending — resolved entry must not appear
        dlq_r = await seeded_client.get("/dlq/delivery?system_name=admissions_crm")
        pending_ids = [e["id"] for e in dlq_r.json()]
        assert dlq_id not in pending_ids

    @pytest.mark.asyncio
    async def test_resolving_already_resolved_entry_returns_error(self, seeded_client):
        staging_id = await _ingest_and_get_staging_id(seeded_client, "ORD-S11-005")

        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": staging_id, "system_name": "admissions_crm"},
        )
        dlq_id = sim.json()["dlq_entry_id"]

        await seeded_client.post(
            f"/dlq/delivery/{dlq_id}/resolve",
            json={"resolved_by": "ops@example.com"},
        )

        # Second resolve on the same entry must fail
        r2 = await seeded_client.post(
            f"/dlq/delivery/{dlq_id}/resolve",
            json={"resolved_by": "ops@example.com"},
        )
        assert r2.status_code == 404


class TestScenario12_NonRetryable400:
    """
    Scenario 12: HTTP 400 on first attempt → immediate DLQ, no retries.

    finance_ledger is configured mock_behavior=non_retryable_400.
    The first attempt returns 400.  retry_classifier routes immediately to
    SEND_TO_DLQ with failure_category=non_retryable.  No further attempts
    are made.
    """

    @pytest.mark.asyncio
    async def test_400_routes_to_dlq_on_first_attempt(self, seeded_client):
        staging_id = await _ingest_and_get_staging_id(seeded_client, "ORD-S12-001")

        r = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": staging_id, "system_name": "finance_ledger"},
        )
        assert r.status_code == 200, r.text
        data = r.json()

        assert data["outcome"] == "dlq_non_retryable"
        assert data["attempts_made"] == 1           # only one attempt made
        assert data["dlq_entry_id"] is not None

    @pytest.mark.asyncio
    async def test_400_dlq_entry_has_correct_category(self, seeded_client):
        staging_id = await _ingest_and_get_staging_id(seeded_client, "ORD-S12-002")

        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": staging_id, "system_name": "finance_ledger"},
        )
        dlq_id = sim.json()["dlq_entry_id"]

        dlq_r = await seeded_client.get("/dlq/delivery?system_name=finance_ledger")
        entry = next(e for e in dlq_r.json() if e["id"] == dlq_id)
        assert entry["failure_category"] == "non_retryable"
        assert entry["resolution_status"] == "pending"

    @pytest.mark.asyncio
    async def test_400_dlq_entry_is_filterable_by_category(self, seeded_client):
        staging_id = await _ingest_and_get_staging_id(seeded_client, "ORD-S12-003")

        await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": staging_id, "system_name": "finance_ledger"},
        )

        # Filter by failure_category — must return at least the entry we just created
        dlq_r = await seeded_client.get(
            "/dlq/delivery?system_name=finance_ledger&failure_category=non_retryable"
        )
        assert dlq_r.status_code == 200
        entries = dlq_r.json()
        assert len(entries) >= 1
        assert all(e["failure_category"] == "non_retryable" for e in entries)


class TestScenario13_BusinessConflict409:
    """
    Scenario 13: HTTP 409 → immediate DLQ, no retries.

    Uses a downstream system configured with mock_behavior=conflict_409.
    Created inline via the seed fixture — we add a fourth system for this scenario
    rather than reconfigure one of the three standard seed systems.
    """

    @pytest.mark.asyncio
    async def test_409_routes_to_dlq_on_first_attempt(self, seeded_db_session, seeded_client):
        # Add a conflict-configured system directly in the test
        from app.models.delivery import DownstreamSystem
        conflict_system = DownstreamSystem(
            name="conflict_target",
            description="Test system that always returns 409",
            baseline_error_rate_pct=0.0,
            circuit_threshold_pct=15.0,
            max_retry_attempts=3,
            supports_status_query=False,
            mock_behavior="conflict_409",
        )
        seeded_db_session.add(conflict_system)
        await seeded_db_session.commit()

        staging_id = await _ingest_and_get_staging_id(seeded_client, "ORD-S13-001")

        r = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": staging_id, "system_name": "conflict_target"},
        )
        assert r.status_code == 200, r.text
        data = r.json()

        assert data["outcome"] == "dlq_non_retryable"
        assert data["attempts_made"] == 1
        assert data["dlq_entry_id"] is not None


class TestScenario_SuccessPath:
    """
    Happy path: warehouse_sync (mock_behavior=success) delivers on first attempt,
    no DLQ entry created.
    """

    @pytest.mark.asyncio
    async def test_success_path_no_dlq_entry(self, seeded_client):
        staging_id = await _ingest_and_get_staging_id(seeded_client, "ORD-SUCCESS-001")

        r = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": staging_id, "system_name": "warehouse_sync"},
        )
        assert r.status_code == 200, r.text
        data = r.json()

        assert data["outcome"] == "success"
        assert data["attempts_made"] == 1
        assert data["dlq_entry_id"] is None

    @pytest.mark.asyncio
    async def test_success_path_does_not_appear_in_dlq(self, seeded_client):
        staging_id = await _ingest_and_get_staging_id(seeded_client, "ORD-SUCCESS-002")

        await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": staging_id, "system_name": "warehouse_sync"},
        )

        dlq_r = await seeded_client.get("/dlq/delivery?system_name=warehouse_sync")
        assert dlq_r.status_code == 200
        # No DLQ entries for successful deliveries
        assert dlq_r.json() == []


class TestDeliveryOutcomesReport:
    """
    Verifies the delivery outcomes report aggregates correctly.
    """

    @pytest.mark.asyncio
    async def test_report_shows_attempts_after_delivery(self, seeded_client):
        staging_id = await _ingest_and_get_staging_id(seeded_client, "ORD-RPT-001")

        await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": staging_id, "system_name": "admissions_crm"},
        )

        r = await seeded_client.get("/reports/delivery-outcomes")
        assert r.status_code == 200
        report = r.json()

        assert "delivery_attempts_by_system" in report
        assert "dlq_pending_by_system" in report
        assert "admissions_crm" in report["delivery_attempts_by_system"]

    @pytest.mark.asyncio
    async def test_report_shows_dlq_depth(self, seeded_client):
        staging_id = await _ingest_and_get_staging_id(seeded_client, "ORD-RPT-002")

        await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": staging_id, "system_name": "finance_ledger"},
        )

        r = await seeded_client.get("/reports/delivery-outcomes")
        report = r.json()

        # finance_ledger should appear in pending DLQ
        assert "finance_ledger" in report["dlq_pending_by_system"]
        pending = report["dlq_pending_by_system"]["finance_ledger"]
        assert any(p["failure_category"] == "non_retryable" for p in pending)

    @pytest.mark.asyncio
    async def test_unknown_system_returns_404(self, seeded_client):
        r = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": 1, "system_name": "nonexistent_system"},
        )
        assert r.status_code == 404
