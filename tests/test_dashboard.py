"""
Tests for the dashboard overview endpoint — Phase 3B.

Covers:
  - Overview returns coherent structure on an empty database
  - attention_needed=False when all queues are clear
  - attention_needed=True after a DLQ entry is created
  - Recent activity includes delivery failure events
  - Recent activity includes ambiguous outcome events
  - Circuit summary reflects active circuit state
  - Overview is read-only (no side effects)
"""

import pytest

BASE_MSG = {
    "source_system": "oms",
    "entity_type": "order",
    "version": 1,
    "payload": {"customer_id": "CUST-DB-001", "status": "created", "total_amount": 50.0},
}


async def _ingest(client, business_key: str) -> int:
    r = await client.post(
        "/messages/ingest",
        json={**BASE_MSG, "business_key": business_key},
    )
    assert r.status_code == 200, r.text
    return r.json()["staging_id"]


class TestDashboardStructure:
    """Overview returns the required top-level fields."""

    @pytest.mark.asyncio
    async def test_overview_returns_200(self, seeded_client):
        r = await seeded_client.get("/dashboard/overview")
        assert r.status_code == 200, r.text

    @pytest.mark.asyncio
    async def test_overview_has_required_top_level_fields(self, seeded_client):
        r = await seeded_client.get("/dashboard/overview")
        data = r.json()
        for field in [
            "generated_at", "attention_needed", "summary",
            "circuit_health", "replay_summary",
            "recent_activity", "quick_links",
        ]:
            assert field in data, f"Missing field: {field}"

    @pytest.mark.asyncio
    async def test_summary_has_required_count_fields(self, seeded_client):
        r = await seeded_client.get("/dashboard/overview")
        summary = r.json()["summary"]
        for field in [
            "total_entities", "open_anomalies", "pending_dlq_items",
            "pending_ambiguous_items", "open_circuits",
            "half_open_circuits", "pending_replays",
        ]:
            assert field in summary, f"Missing summary field: {field}"
            assert isinstance(summary[field], int)

    @pytest.mark.asyncio
    async def test_circuit_health_has_required_fields(self, seeded_client):
        r = await seeded_client.get("/dashboard/overview")
        ch = r.json()["circuit_health"]
        assert "closed" in ch
        assert "open" in ch
        assert "half_open" in ch
        assert "systems" in ch
        assert isinstance(ch["systems"], list)

    @pytest.mark.asyncio
    async def test_replay_summary_has_required_fields(self, seeded_client):
        r = await seeded_client.get("/dashboard/overview")
        rs = r.json()["replay_summary"]
        for status in ["pending", "executing", "completed", "failed"]:
            assert status in rs

    @pytest.mark.asyncio
    async def test_quick_links_present(self, seeded_client):
        r = await seeded_client.get("/dashboard/overview")
        ql = r.json()["quick_links"]
        assert "delivery_dlq" in ql
        assert "ambiguous_queue" in ql
        assert "circuits" in ql
        assert "replays" in ql
        # Inspector link templates
        assert "inspector_by_entity_key" in ql or "inspector_by_correlation" in ql


class TestDashboardCounts:
    """Summary counts reflect actual queue state."""

    @pytest.mark.asyncio
    async def test_empty_state_attention_not_needed(self, seeded_client):
        """With only seed data and no activity, all queues should be empty."""
        r = await seeded_client.get("/dashboard/overview")
        data = r.json()
        summary = data["summary"]
        # No activity seeded — all pending counts should be zero
        assert summary["open_anomalies"] == 0
        assert summary["pending_dlq_items"] == 0
        assert summary["pending_ambiguous_items"] == 0
        assert data["attention_needed"] is False

    @pytest.mark.asyncio
    async def test_attention_needed_true_after_dlq_entry(self, seeded_client):
        sid = await _ingest(seeded_client, "ORD-DB-DLQ-001")
        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "admissions_crm"},
        )
        assert sim.json().get("dlq_entry_id") is not None

        r = await seeded_client.get("/dashboard/overview")
        data = r.json()
        assert data["summary"]["pending_dlq_items"] >= 1
        assert data["attention_needed"] is True

    @pytest.mark.asyncio
    async def test_pending_ambiguous_counted(self, seeded_client):
        sid = await _ingest(seeded_client, "ORD-DB-AMB-001")
        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "timeout_with_query"},
        )
        assert sim.json()["outcome"] == "queued_ambiguous"

        r = await seeded_client.get("/dashboard/overview")
        assert r.json()["summary"]["pending_ambiguous_items"] >= 1

    @pytest.mark.asyncio
    async def test_entity_count_increments_after_ingest(self, seeded_client):
        r_before = await seeded_client.get("/dashboard/overview")
        count_before = r_before.json()["summary"]["total_entities"]

        await _ingest(seeded_client, "ORD-DB-COUNT-001")

        r_after = await seeded_client.get("/dashboard/overview")
        count_after = r_after.json()["summary"]["total_entities"]
        assert count_after == count_before + 1

    @pytest.mark.asyncio
    async def test_circuit_open_reflected_in_summary(self, seeded_client):
        # Force a circuit open
        await seeded_client.post(
            "/circuits/admissions_crm/reset",
            json={"target_state": "open", "reset_by": "test@example.com"},
        )
        r = await seeded_client.get("/dashboard/overview")
        data = r.json()
        assert data["summary"]["open_circuits"] >= 1
        assert data["attention_needed"] is True
        # Circuit should appear in circuit_health.systems
        open_systems = [s for s in data["circuit_health"]["systems"] if s["state"] == "open"]
        assert any(s["system_name"] == "admissions_crm" for s in open_systems)


class TestRecentActivity:
    """Recent activity feed includes representative events."""

    @pytest.mark.asyncio
    async def test_delivery_failure_in_recent_activity(self, seeded_client):
        sid = await _ingest(seeded_client, "ORD-DB-ACT-001")
        await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "admissions_crm"},
        )

        r = await seeded_client.get("/dashboard/overview")
        activity = r.json()["recent_activity"]
        event_types = [e["event_type"] for e in activity]
        assert "delivery_failure" in event_types

    @pytest.mark.asyncio
    async def test_ambiguous_outcome_in_recent_activity(self, seeded_client):
        sid = await _ingest(seeded_client, "ORD-DB-ACT-002")
        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "timeout_with_query"},
        )
        assert sim.json()["outcome"] == "queued_ambiguous"

        r = await seeded_client.get("/dashboard/overview")
        activity = r.json()["recent_activity"]
        event_types = [e["event_type"] for e in activity]
        assert "ambiguous_outcome" in event_types

    @pytest.mark.asyncio
    async def test_recent_activity_sorted_descending(self, seeded_client):
        """Activity feed must be ordered most-recent-first."""
        r = await seeded_client.get("/dashboard/overview")
        activity = r.json()["recent_activity"]
        if len(activity) >= 2:
            timestamps = [e["occurred_at"] for e in activity if e.get("occurred_at")]
            assert timestamps == sorted(timestamps, reverse=True), (
                "Recent activity must be sorted descending by occurred_at"
            )

    @pytest.mark.asyncio
    async def test_activity_events_have_required_fields(self, seeded_client):
        sid = await _ingest(seeded_client, "ORD-DB-ACT-003")
        await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "admissions_crm"},
        )
        r = await seeded_client.get("/dashboard/overview")
        for event in r.json()["recent_activity"]:
            assert "event_type" in event
            assert "occurred_at" in event
            assert "summary" in event
            assert "links" in event

    @pytest.mark.asyncio
    async def test_circuit_state_change_in_activity_after_reset(self, seeded_client):
        # Open a circuit — will appear as a non-closed circuit in activity
        await seeded_client.post(
            "/circuits/warehouse_sync/reset",
            json={"target_state": "open", "reset_by": "test@example.com"},
        )
        r = await seeded_client.get("/dashboard/overview")
        activity = r.json()["recent_activity"]
        event_types = [e["event_type"] for e in activity]
        assert "circuit_state_change" in event_types


class TestDashboardReadOnly:
    """Dashboard must not modify any state."""

    @pytest.mark.asyncio
    async def test_multiple_overview_calls_produce_same_counts(self, seeded_client):
        r1 = await seeded_client.get("/dashboard/overview")
        r2 = await seeded_client.get("/dashboard/overview")
        # Counts must be identical — no side effects
        assert r1.json()["summary"] == r2.json()["summary"]
        assert r1.json()["circuit_health"]["open"] == r2.json()["circuit_health"]["open"]
