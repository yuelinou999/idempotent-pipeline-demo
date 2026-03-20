"""
Tests for the entity / message inspector — Phase 3A.

Covers:
  Scenario A: normal accept + delivery success — happy path record
  Scenario B: record that hit DataAnomalyQueue (quarantine decision)
  Scenario C: record that hit DeliveryDLQ (transient exhausted)
  Scenario D: record that hit AmbiguousOutcomeQueue + query-before-retry
  Scenario E: record with replay activity

Each scenario ingests a record, drives the relevant pipeline path, then
inspects via each of the three routes and verifies the assembled view.
"""

import pytest

BASE = {
    "source_system": "oms",
    "entity_type": "order",
    "version": 1,
    "payload": {"customer_id": "CUST-INS-001", "status": "created", "total_amount": 10.0},
}


async def _ingest(client, business_key: str, version: int = 1, payload: dict = None) -> dict:
    body = {**BASE, "business_key": business_key, "version": version}
    if payload:
        body["payload"] = payload
    r = await client.post("/messages/ingest", json=body)
    assert r.status_code == 200, r.text
    return r.json()


class TestScenarioA_HappyPath:
    """
    Scenario A: normal accept + delivery success.

    Inspect view must contain:
    - ingestion_history with decision=accept_new
    - staging with completeness_status
    - delivery_attempts with outcome=success
    - no anomalies, no DLQ, no ambiguous, no replay
    """

    @pytest.mark.asyncio
    async def test_inspect_by_idempotent_key_happy_path(self, seeded_client):
        result = await _ingest(seeded_client, "ORD-INSA-001")
        ik = result["idempotent_key"]
        sid = result["staging_id"]

        # Deliver successfully
        await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "warehouse_sync"},
        )

        r = await seeded_client.get(f"/inspect/by-idempotent-key/{ik}")
        assert r.status_code == 200, r.text
        view = r.json()

        assert view["summary"]["total_ingestion_attempts"] == 1
        assert view["summary"]["total_delivery_attempts"] == 1
        assert view["summary"]["has_anomalies"] is False
        assert view["summary"]["has_delivery_failures"] is False
        assert view["summary"]["has_ambiguous_outcomes"] is False
        assert view["summary"]["has_replay_activity"] is False

        assert len(view["ingestion_history"]) == 1
        assert view["ingestion_history"][0]["decision"] == "accept_new"

        assert len(view["delivery_attempts"]) == 1
        assert view["delivery_attempts"][0]["outcome"] == "success"

        assert view["identifiers"]["entity_key"] is not None
        assert view["staging"] is not None

        # --- Timeline checks ---
        tl = view["ordered_events"]
        assert isinstance(tl, list)
        assert len(tl) >= 2  # at least: ingestion + delivery_attempt
        event_types = [e["event_type"] for e in tl]
        assert "ingestion" in event_types
        assert "delivery_attempt" in event_types
        # Timeline must be sorted ascending
        timestamps = [e["occurred_at"] for e in tl]
        assert timestamps == sorted(timestamps), "Timeline is not sorted ascending"
        # Every event has required fields
        for e in tl:
            assert "event_type" in e
            assert "occurred_at" in e
            assert "summary" in e
            assert "ref_id" in e
            assert "context" in e

    @pytest.mark.asyncio
    async def test_inspect_by_entity_key_happy_path(self, seeded_client):
        result = await _ingest(seeded_client, "ORD-INSA-002")
        ek = result["entity_key"]
        sid = result["staging_id"]

        await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "warehouse_sync"},
        )

        r = await seeded_client.get(f"/inspect/by-entity-key/{ek}")
        assert r.status_code == 200, r.text
        view = r.json()
        assert view["summary"]["entity_key"] == ek
        assert len(view["ingestion_history"]) >= 1

    @pytest.mark.asyncio
    async def test_unknown_key_returns_404(self, seeded_client):
        r = await seeded_client.get("/inspect/by-idempotent-key/does:::not:::exist:::99")
        assert r.status_code == 404

        r2 = await seeded_client.get("/inspect/by-entity-key/does:::not:::exist")
        assert r2.status_code == 404

        r3 = await seeded_client.get("/inspect/by-correlation-id/nonexistent-corr-id")
        assert r3.status_code == 404


class TestScenarioB_DataAnomalyQueue:
    """
    Scenario B: record with a quarantine outcome shows up in anomalies.

    Trigger quarantine by sending the same version twice with different payloads.
    The second attempt should be quarantined (same_version_different_payload).
    Inspect must show has_anomalies=True and a non-empty anomalies list.
    """

    @pytest.mark.asyncio
    async def test_anomaly_appears_in_inspection(self, seeded_client):
        # Ingest v1
        r1 = await _ingest(seeded_client, "ORD-INSB-001", version=1,
                           payload={"customer_id": "C1", "status": "created", "total_amount": 10.0})
        ik = r1["idempotent_key"]

        # Re-send same version with different payload → triggers quarantine
        await seeded_client.post("/messages/ingest", json={
            **BASE,
            "business_key": "ORD-INSB-001",
            "version": 1,
            "payload": {"customer_id": "C1", "status": "MODIFIED", "total_amount": 99.0},
        })

        r = await seeded_client.get(f"/inspect/by-idempotent-key/{ik}")
        assert r.status_code == 200, r.text
        view = r.json()

        assert view["summary"]["has_anomalies"] is True
        assert len(view["anomalies"]) >= 1
        anomaly = view["anomalies"][0]
        assert anomaly["idempotent_key"] == ik
        assert "same_version" in anomaly["reason"] or anomaly["reason"] is not None

        # --- Timeline checks ---
        tl = view["ordered_events"]
        event_types = [e["event_type"] for e in tl]
        assert "ingestion" in event_types
        assert "anomaly_queued" in event_types
        # Anomaly must appear after the ingestion that caused it
        ingest_ts = next(e["occurred_at"] for e in tl if e["event_type"] == "ingestion")
        anomaly_ts = next(e["occurred_at"] for e in tl if e["event_type"] == "anomaly_queued")
        assert anomaly_ts >= ingest_ts, "anomaly_queued must not precede ingestion"
        timestamps = [e["occurred_at"] for e in tl]
        assert timestamps == sorted(timestamps)


class TestScenarioC_DeliveryDLQ:
    """
    Scenario C: record that hit DeliveryDLQ (admissions_crm exhausts retries).

    Inspect must show has_delivery_failures=True, non-empty delivery_dlq,
    and delivery_attempts showing all the failed attempts.
    """

    @pytest.mark.asyncio
    async def test_dlq_entry_appears_in_inspection(self, seeded_client):
        result = await _ingest(seeded_client, "ORD-INSC-001")
        ik = result["idempotent_key"]
        sid = result["staging_id"]

        # Deliver to always-failing system → DLQ
        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "admissions_crm"},
        )
        assert sim.json().get("dlq_entry_id") is not None

        r = await seeded_client.get(f"/inspect/by-idempotent-key/{ik}")
        assert r.status_code == 200, r.text
        view = r.json()

        assert view["summary"]["has_delivery_failures"] is True
        assert len(view["delivery_dlq"]) >= 1
        dlq = view["delivery_dlq"][0]
        assert dlq["system_name"] == "admissions_crm"
        assert dlq["failure_category"] in ("transient_exhausted", "non_retryable")

        # delivery_attempts should show the failed attempts
        assert len(view["delivery_attempts"]) >= 1
        assert all(a["outcome"] in ("failed", "timeout") for a in view["delivery_attempts"])

    @pytest.mark.asyncio
    async def test_entity_key_and_idempotent_key_views_agree(self, seeded_client):
        result = await _ingest(seeded_client, "ORD-INSC-002")
        ik = result["idempotent_key"]
        ek = result["entity_key"]
        sid = result["staging_id"]

        await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "admissions_crm"},
        )

        by_ik = await seeded_client.get(f"/inspect/by-idempotent-key/{ik}")
        by_ek = await seeded_client.get(f"/inspect/by-entity-key/{ek}")

        assert by_ik.json()["summary"]["has_delivery_failures"] == \
               by_ek.json()["summary"]["has_delivery_failures"]
        assert len(by_ik.json()["delivery_dlq"]) == len(by_ek.json()["delivery_dlq"])


class TestScenarioD_AmbiguousOutcome:
    """
    Scenario D: record that hit AmbiguousOutcomeQueue + query-before-retry.

    Deliver to timeout_with_query → queued_ambiguous.
    Run query-status → retry_eligible.
    Inspect must show has_ambiguous_outcomes=True with the status populated.
    """

    @pytest.mark.asyncio
    async def test_ambiguous_outcome_appears_in_inspection(self, seeded_client):
        result = await _ingest(seeded_client, "ORD-INSD-001")
        ik = result["idempotent_key"]
        sid = result["staging_id"]

        # Timeout → AmbiguousOutcomeQueue
        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "timeout_with_query"},
        )
        assert sim.json()["outcome"] == "queued_ambiguous"
        ambiguous_id = sim.json()["ambiguous_id"]

        # Run query-before-retry
        await seeded_client.post(f"/dlq/ambiguous/{ambiguous_id}/query-status")

        r = await seeded_client.get(f"/inspect/by-idempotent-key/{ik}")
        assert r.status_code == 200, r.text
        view = r.json()

        assert view["summary"]["has_ambiguous_outcomes"] is True
        assert len(view["ambiguous_outcomes"]) >= 1
        amb = view["ambiguous_outcomes"][0]
        assert amb["system_name"] == "timeout_with_query"
        assert amb["ambiguity_category"] == "timeout"
        # After query-status, status should be retry_eligible (not_found path)
        assert amb["status"] == "retry_eligible"
        assert amb["resolution_note"] is not None

        # --- Timeline checks ---
        tl = view["ordered_events"]
        event_types = [e["event_type"] for e in tl]
        assert "delivery_attempt" in event_types
        assert "ambiguous_queued" in event_types
        assert "ambiguous_resolved" in event_types
        # ambiguous_queued must precede ambiguous_resolved
        queued_ts = next(e["occurred_at"] for e in tl if e["event_type"] == "ambiguous_queued")
        resolved_ts = next(e["occurred_at"] for e in tl if e["event_type"] == "ambiguous_resolved")
        assert resolved_ts >= queued_ts
        timestamps = [e["occurred_at"] for e in tl]
        assert timestamps == sorted(timestamps)
        # resolved event should describe the new status
        resolved_event = next(e for e in tl if e["event_type"] == "ambiguous_resolved")
        assert resolved_event["context"]["new_status"] == "retry_eligible"

    @pytest.mark.asyncio
    async def test_ambiguous_before_query_shows_pending(self, seeded_client):
        result = await _ingest(seeded_client, "ORD-INSD-002")
        sid = result["staging_id"]
        ik = result["idempotent_key"]

        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "timeout_with_query"},
        )
        assert sim.json()["outcome"] == "queued_ambiguous"

        # Don't run query-status — should still show pending
        r = await seeded_client.get(f"/inspect/by-idempotent-key/{ik}")
        view = r.json()
        assert view["summary"]["has_ambiguous_outcomes"] is True
        assert view["ambiguous_outcomes"][0]["status"] == "pending"


class TestScenarioE_ReplayActivity:
    """
    Scenario E: record with replay activity — replay item and execution log visible.

    Create DLQ entry, create replay request, execute it.
    Inspect must show has_replay_activity=True with execution result visible.
    """

    @pytest.mark.asyncio
    async def test_replay_activity_appears_in_inspection(self, seeded_client):
        result = await _ingest(seeded_client, "ORD-INSE-001")
        ik = result["idempotent_key"]
        sid = result["staging_id"]

        # Create DLQ entry
        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "admissions_crm"},
        )
        dlq_id = sim.json().get("dlq_entry_id")
        assert dlq_id is not None

        # Create and execute replay
        create_r = await seeded_client.post("/replays", json={
            "selection_mode": "dlq_items",
            "reason": "Inspector scenario E test",
            "requested_by": "ops@example.com",
            "source_item_ids": [dlq_id],
        })
        replay_id = create_r.json()["replay_request_id"]
        await seeded_client.post(f"/replays/{replay_id}/execute")

        r = await seeded_client.get(f"/inspect/by-idempotent-key/{ik}")
        assert r.status_code == 200, r.text
        view = r.json()

        assert view["summary"]["has_replay_activity"] is True
        assert len(view["replay_activity"]) >= 1
        replay = view["replay_activity"][0]
        assert replay["replay_request_id"] == replay_id
        # Execution should have been recorded
        assert replay["execution_result"] is not None
        assert replay["executed_at"] is not None
        assert replay["item_status"] != "pending"

        # --- Timeline checks ---
        tl = view["ordered_events"]
        event_types = [e["event_type"] for e in tl]
        assert "ingestion" in event_types
        assert "delivery_attempt" in event_types
        assert "dlq_queued" in event_types
        assert "replay_requested" in event_types
        assert "replay_executed" in event_types
        # dlq must precede replay
        dlq_ts = next(e["occurred_at"] for e in tl if e["event_type"] == "dlq_queued")
        replay_req_ts = next(e["occurred_at"] for e in tl if e["event_type"] == "replay_requested")
        assert replay_req_ts >= dlq_ts, "replay_requested must not precede dlq_queued"
        timestamps = [e["occurred_at"] for e in tl]
        assert timestamps == sorted(timestamps)

    @pytest.mark.asyncio
    async def test_replay_before_execute_shows_pending(self, seeded_client):
        result = await _ingest(seeded_client, "ORD-INSE-002")
        sid = result["staging_id"]
        ik = result["idempotent_key"]

        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "admissions_crm"},
        )
        dlq_id = sim.json().get("dlq_entry_id")
        assert dlq_id is not None

        create_r = await seeded_client.post("/replays", json={
            "selection_mode": "dlq_items",
            "reason": "Pre-execute inspection test",
            "requested_by": "ops@example.com",
            "source_item_ids": [dlq_id],
        })
        replay_id = create_r.json()["replay_request_id"]
        # Do NOT execute

        r = await seeded_client.get(f"/inspect/by-idempotent-key/{ik}")
        view = r.json()
        assert view["summary"]["has_replay_activity"] is True
        replay = view["replay_activity"][0]
        assert replay["item_status"] == "pending"
        assert replay["execution_result"] is None


class TestCorrelationInspection:
    """
    Inspect by correlation_id — covers the business-transaction view.
    """

    @pytest.mark.asyncio
    async def test_inspect_by_correlation_id(self, seeded_client):
        result = await _ingest(seeded_client, "ORD-INSE-COR-001")
        corr_id = result.get("correlation_id")
        if corr_id is None:
            pytest.skip("No correlation_id returned — correlation engine may not have fired")

        r = await seeded_client.get(f"/inspect/by-correlation-id/{corr_id}")
        assert r.status_code == 200, r.text
        view = r.json()
        assert view["correlation_id"] == corr_id
        assert view["entity_count"] >= 1
        assert len(view["entities"]) >= 1

    @pytest.mark.asyncio
    async def test_correlation_id_not_found_returns_404(self, seeded_client):
        r = await seeded_client.get("/inspect/by-correlation-id/does-not-exist-9999")
        assert r.status_code == 404
