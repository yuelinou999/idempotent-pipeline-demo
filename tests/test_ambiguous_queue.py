"""
Tests for the ambiguous outcome queue — Phase 2C.

Covers:
  Scenario 14: timeout delivery creates AmbiguousOutcomeQueue entry
  Scenario 15: query-before-retry returns confirmed_delivered → no retry needed
  Scenario 16: query-before-retry returns not_found → retry_eligible
  Scenario 17: query-before-retry returns status_unavailable (no query support)
               → status_unavailable, operator must decide

Setup notes:
  timeout_with_query:  mock_behavior=timeout, supports_status_query=True,
                       mock_status_query_behavior=not_found
  timeout_no_query:    mock_behavior=timeout, supports_status_query=False

  For the confirmed_delivered scenario we insert a system inline whose
  mock_status_query_behavior=confirmed_delivered.
"""

import pytest

BASE_MSG = {
    "source_system": "oms",
    "entity_type": "order",
    "version": 1,
    "payload": {"customer_id": "CUST-AQ-001", "status": "created", "total_amount": 80.0},
}


async def _ingest(client, business_key: str) -> int:
    r = await client.post(
        "/messages/ingest",
        json={**BASE_MSG, "business_key": business_key},
    )
    assert r.status_code == 200, r.text
    staging_id = r.json()["staging_id"]
    assert staging_id is not None
    return staging_id


class TestScenario14_TimeoutCreatesAmbiguousEntry:
    """
    Scenario 14: a timeout delivery outcome creates an AmbiguousOutcomeQueue entry.

    Uses timeout_with_query (mock_behavior=timeout, max_retry_attempts=1).
    simulate_delivery should return outcome=queued_ambiguous and a non-null ambiguous_id.
    The entry should be visible via GET /dlq/ambiguous.
    """

    @pytest.mark.asyncio
    async def test_timeout_creates_ambiguous_entry(self, seeded_client):
        sid = await _ingest(seeded_client, "ORD-AQ14-001")

        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "timeout_with_query"},
        )
        assert sim.status_code == 200, sim.text
        data = sim.json()

        assert data["outcome"] == "queued_ambiguous"
        assert data["ambiguous_id"] is not None
        assert data["dlq_entry_id"] is None
        assert data["attempts_made"] == 1

    @pytest.mark.asyncio
    async def test_ambiguous_entry_visible_in_list(self, seeded_client):
        sid = await _ingest(seeded_client, "ORD-AQ14-002")

        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "timeout_with_query"},
        )
        ambiguous_id = sim.json()["ambiguous_id"]

        list_r = await seeded_client.get("/dlq/ambiguous?status=pending")
        assert list_r.status_code == 200
        ids = [e["id"] for e in list_r.json()]
        assert ambiguous_id in ids

    @pytest.mark.asyncio
    async def test_ambiguous_entry_detail_has_correct_fields(self, seeded_client):
        sid = await _ingest(seeded_client, "ORD-AQ14-003")

        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "timeout_with_query"},
        )
        ambiguous_id = sim.json()["ambiguous_id"]

        detail_r = await seeded_client.get(f"/dlq/ambiguous/{ambiguous_id}")
        assert detail_r.status_code == 200
        entry = detail_r.json()

        assert entry["status"] == "pending"
        assert entry["system_name"] == "timeout_with_query"
        assert entry["ambiguity_category"] == "timeout"
        assert entry["staging_id"] == sid
        assert entry["idempotent_key"] is not None
        assert entry["delivery_attempt_log_id"] is not None

    @pytest.mark.asyncio
    async def test_ambiguous_entry_idempotent_key_matches_staging(self, seeded_client):
        """idempotent_key on the ambiguous entry matches the staging record."""
        sid = await _ingest(seeded_client, "ORD-AQ14-004")

        # Get the idempotent_key from the ingestion response
        ingest_r = await seeded_client.post(
            "/messages/ingest",
            json={**BASE_MSG, "business_key": "ORD-AQ14-004"},
        )
        # Already ingested above; this will be skip_duplicate but key is still returned
        expected_key = ingest_r.json()["idempotent_key"]

        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "timeout_with_query"},
        )
        ambiguous_id = sim.json()["ambiguous_id"]

        detail_r = await seeded_client.get(f"/dlq/ambiguous/{ambiguous_id}")
        assert detail_r.json()["idempotent_key"] == expected_key


class TestScenario15_QueryConfirmedDelivered:
    """
    Scenario 15: query-before-retry returns confirmed_delivered.

    We insert a system with mock_status_query_behavior=confirmed_delivered.
    After running query-status, the entry should be closed as confirmed_delivered
    and the response should indicate no retry is needed.
    """

    @pytest.mark.asyncio
    async def test_query_confirmed_delivered(self, seeded_db_session, seeded_client):
        # Add a system that will confirm delivery when queried
        from app.models.delivery import DownstreamSystem
        seeded_db_session.add(DownstreamSystem(
            name="timeout_confirm",
            description="Timeout system that confirms delivery on query",
            baseline_error_rate_pct=1.0,
            circuit_threshold_pct=20.0,
            max_retry_attempts=1,
            supports_status_query=True,
            mock_behavior="timeout",
            mock_status_query_behavior="confirmed_delivered",
            open_after_n_failures=5,
        ))
        await seeded_db_session.commit()

        sid = await _ingest(seeded_client, "ORD-AQ15-001")
        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "timeout_confirm"},
        )
        assert sim.json()["outcome"] == "queued_ambiguous"
        ambiguous_id = sim.json()["ambiguous_id"]

        # Run query-before-retry
        query_r = await seeded_client.post(
            f"/dlq/ambiguous/{ambiguous_id}/query-status"
        )
        assert query_r.status_code == 200, query_r.text
        result = query_r.json()

        assert result["query_result"] == "confirmed_delivered"
        assert result["new_status"] == "confirmed_delivered"
        assert result["action_required"] == "none_confirmed_delivered"

    @pytest.mark.asyncio
    async def test_confirmed_entry_not_in_pending_list(self, seeded_db_session, seeded_client):
        from app.models.delivery import DownstreamSystem
        existing = await seeded_db_session.execute(
            __import__("sqlalchemy", fromlist=["select"]).select(DownstreamSystem)
            .where(DownstreamSystem.name == "timeout_confirm")
        )
        if not existing.scalar():
            seeded_db_session.add(DownstreamSystem(
                name="timeout_confirm",
                description="Timeout system that confirms delivery on query",
                baseline_error_rate_pct=1.0,
                circuit_threshold_pct=20.0,
                max_retry_attempts=1,
                supports_status_query=True,
                mock_behavior="timeout",
                mock_status_query_behavior="confirmed_delivered",
                open_after_n_failures=5,
            ))
            await seeded_db_session.commit()

        sid = await _ingest(seeded_client, "ORD-AQ15-002")
        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "timeout_confirm"},
        )
        ambiguous_id = sim.json()["ambiguous_id"]

        await seeded_client.post(f"/dlq/ambiguous/{ambiguous_id}/query-status")

        pending_r = await seeded_client.get("/dlq/ambiguous?status=pending")
        pending_ids = [e["id"] for e in pending_r.json()]
        assert ambiguous_id not in pending_ids

        confirmed_r = await seeded_client.get(
            "/dlq/ambiguous?status=confirmed_delivered"
        )
        confirmed_ids = [e["id"] for e in confirmed_r.json()]
        assert ambiguous_id in confirmed_ids


class TestScenario16_QueryNotFound:
    """
    Scenario 16: query-before-retry returns not_found → retry_eligible.

    timeout_with_query has mock_status_query_behavior=not_found.
    After query-status, entry should be retry_eligible.
    No replay is triggered in this phase — that is deferred.
    """

    @pytest.mark.asyncio
    async def test_query_not_found_marks_retry_eligible(self, seeded_client):
        sid = await _ingest(seeded_client, "ORD-AQ16-001")

        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "timeout_with_query"},
        )
        assert sim.json()["outcome"] == "queued_ambiguous"
        ambiguous_id = sim.json()["ambiguous_id"]

        query_r = await seeded_client.post(
            f"/dlq/ambiguous/{ambiguous_id}/query-status"
        )
        assert query_r.status_code == 200, query_r.text
        result = query_r.json()

        assert result["query_result"] == "not_found"
        assert result["new_status"] == "retry_eligible"
        assert result["action_required"] == "retry_eligible"

    @pytest.mark.asyncio
    async def test_retry_eligible_entry_in_correct_status_bucket(self, seeded_client):
        sid = await _ingest(seeded_client, "ORD-AQ16-002")
        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "timeout_with_query"},
        )
        ambiguous_id = sim.json()["ambiguous_id"]

        await seeded_client.post(f"/dlq/ambiguous/{ambiguous_id}/query-status")

        # Must appear in retry_eligible list, not pending
        retry_r = await seeded_client.get("/dlq/ambiguous?status=retry_eligible")
        retry_ids = [e["id"] for e in retry_r.json()]
        assert ambiguous_id in retry_ids

        pending_r = await seeded_client.get("/dlq/ambiguous?status=pending")
        assert ambiguous_id not in [e["id"] for e in pending_r.json()]

    @pytest.mark.asyncio
    async def test_querying_resolved_entry_returns_error(self, seeded_client):
        sid = await _ingest(seeded_client, "ORD-AQ16-003")
        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "timeout_with_query"},
        )
        ambiguous_id = sim.json()["ambiguous_id"]

        # First query
        await seeded_client.post(f"/dlq/ambiguous/{ambiguous_id}/query-status")

        # Second query on already-resolved entry must fail
        r2 = await seeded_client.post(
            f"/dlq/ambiguous/{ambiguous_id}/query-status"
        )
        assert r2.status_code == 404


class TestScenario17_StatusUnavailable:
    """
    Scenario 17: system does not support status queries → status_unavailable.

    timeout_no_query has supports_status_query=False.
    query-status should return status_unavailable without calling any mock.
    """

    @pytest.mark.asyncio
    async def test_query_unavailable_when_system_has_no_query_support(self, seeded_client):
        sid = await _ingest(seeded_client, "ORD-AQ17-001")

        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "timeout_no_query"},
        )
        assert sim.json()["outcome"] == "queued_ambiguous"
        ambiguous_id = sim.json()["ambiguous_id"]

        query_r = await seeded_client.post(
            f"/dlq/ambiguous/{ambiguous_id}/query-status"
        )
        assert query_r.status_code == 200, query_r.text
        result = query_r.json()

        assert result["new_status"] == "status_unavailable"
        assert result["action_required"] == "manual_operator_decision"

    @pytest.mark.asyncio
    async def test_status_unavailable_entry_queryable_by_status(self, seeded_client):
        sid = await _ingest(seeded_client, "ORD-AQ17-002")
        sim = await seeded_client.post(
            "/delivery/simulate",
            json={"staging_id": sid, "system_name": "timeout_no_query"},
        )
        ambiguous_id = sim.json()["ambiguous_id"]

        await seeded_client.post(f"/dlq/ambiguous/{ambiguous_id}/query-status")

        unavail_r = await seeded_client.get(
            "/dlq/ambiguous?status=status_unavailable"
        )
        assert ambiguous_id in [e["id"] for e in unavail_r.json()]


class TestAmbiguousEntryNotFoundRoute:
    @pytest.mark.asyncio
    async def test_get_nonexistent_entry_returns_404(self, seeded_client):
        r = await seeded_client.get("/dlq/ambiguous/999999")
        assert r.status_code == 404

    @pytest.mark.asyncio
    async def test_query_nonexistent_entry_returns_404(self, seeded_client):
        r = await seeded_client.post("/dlq/ambiguous/999999/query-status")
        assert r.status_code == 404
