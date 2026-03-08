"""
Integration tests for the ingestion API.

Tests validate:
  - API outcome families: accepted | skipped | quarantined
  - decision_detail field exposes the raw internal decision
  - entity-level current version behavior (supersede and stale work across versions)
  - versioned attempt logging (entity_key + idempotent_key both present)
  - reconciliation report uses family counts, not raw decision labels
"""

import pytest

from app.services.key_builder import build_entity_key, build_idempotent_key

BASE_MSG = {
    "source_system": "oms",
    "entity_type": "order",
    "business_key": "ORD-TEST-0001",
    "version": 1,
    "payload": {
        "customer_id": "CUST-001",
        "order_date": "2026-03-01",
        "total_amount": 199.99,
        "status": "created",
    },
}


class TestApiOutcomeFamilies:
    """API responses must use outcome families, not internal decision labels."""

    @pytest.mark.asyncio
    async def test_first_accept_outcome_is_accepted(self, client):
        r = await client.post("/messages/ingest", json=BASE_MSG)
        assert r.status_code == 200
        data = r.json()
        assert data["outcome"] == "accepted"
        assert data["decision_detail"] == "accept_new"
        assert data["staging_id"] is not None

    @pytest.mark.asyncio
    async def test_supersede_outcome_is_accepted(self, client):
        await client.post("/messages/ingest", json=BASE_MSG)
        v2 = {**BASE_MSG, "version": 2, "payload": {**BASE_MSG["payload"], "status": "confirmed"}}
        r = await client.post("/messages/ingest", json=v2)
        assert r.json()["outcome"] == "accepted"
        assert r.json()["decision_detail"] == "accept_supersede"

    @pytest.mark.asyncio
    async def test_exact_duplicate_outcome_is_skipped(self, client):
        await client.post("/messages/ingest", json=BASE_MSG)
        r = await client.post("/messages/ingest", json=BASE_MSG)
        assert r.json()["outcome"] == "skipped"
        assert r.json()["decision_detail"] == "skip_duplicate"

    @pytest.mark.asyncio
    async def test_stale_version_outcome_is_skipped(self, client):
        v2 = {**BASE_MSG, "version": 2, "payload": {**BASE_MSG["payload"], "status": "confirmed"}}
        await client.post("/messages/ingest", json=v2)
        r = await client.post("/messages/ingest", json=BASE_MSG)  # v1 arrives late
        assert r.json()["outcome"] == "skipped"
        assert r.json()["decision_detail"] == "skip_stale"

    @pytest.mark.asyncio
    async def test_hash_mismatch_outcome_is_quarantined(self, client):
        await client.post("/messages/ingest", json=BASE_MSG)
        mutated = {**BASE_MSG, "payload": {**BASE_MSG["payload"], "total_amount": 999.99}}
        r = await client.post("/messages/ingest", json=mutated)
        assert r.json()["outcome"] == "quarantined"
        assert r.json()["decision_detail"] == "quarantine"


class TestEntityLevelStateBehavior:
    """
    Validates that the entity_key (version-less) is used for state lookup,
    which is what makes supersede and stale detection work correctly.
    """

    @pytest.mark.asyncio
    async def test_response_contains_both_keys(self, client):
        r = await client.post("/messages/ingest", json=BASE_MSG)
        data = r.json()
        expected_entity_key = build_entity_key("oms", "order", "ORD-TEST-0001")
        expected_idempotent_key = build_idempotent_key("oms", "order", "ORD-TEST-0001", 1)
        assert data["entity_key"] == expected_entity_key
        assert data["idempotent_key"] == expected_idempotent_key

    @pytest.mark.asyncio
    async def test_state_lookup_uses_entity_key(self, client):
        """GET /state/{entity_key} returns current version regardless of which version was last ingested."""
        await client.post("/messages/ingest", json=BASE_MSG)
        v2 = {**BASE_MSG, "version": 2, "payload": {**BASE_MSG["payload"], "status": "confirmed"}}
        await client.post("/messages/ingest", json=v2)

        entity_key = build_entity_key("oms", "order", "ORD-TEST-0001")
        state_r = await client.get(f"/state/{entity_key}")
        assert state_r.status_code == 200
        state = state_r.json()
        assert state["current_version"] == 2
        assert state["last_decision"] == "accept_supersede"
        assert state["entity_key"] == entity_key

    @pytest.mark.asyncio
    async def test_supersede_updates_state_in_place(self, client):
        """After v1 then v2, there is still only ONE state row, showing current_version=2."""
        await client.post("/messages/ingest", json=BASE_MSG)
        v2 = {**BASE_MSG, "version": 2, "payload": {**BASE_MSG["payload"], "status": "confirmed"}}
        await client.post("/messages/ingest", json=v2)

        entity_key = build_entity_key("oms", "order", "ORD-TEST-0001")
        state_r = await client.get(f"/state/{entity_key}")
        assert state_r.status_code == 200
        assert state_r.json()["current_version"] == 2

    @pytest.mark.asyncio
    async def test_out_of_order_v3_then_v2_v2_is_stale(self, client):
        """V3 accepted first. V2 arrives late and should be SKIP_STALE, not ACCEPT_NEW."""
        bk = "ORD-OOO-001"
        v3 = {**BASE_MSG, "business_key": bk, "version": 3,
              "payload": {**BASE_MSG["payload"], "status": "shipped"}}
        r3 = await client.post("/messages/ingest", json=v3)
        assert r3.json()["outcome"] == "accepted"
        assert r3.json()["decision_detail"] == "accept_new"

        v2 = {**BASE_MSG, "business_key": bk, "version": 2,
              "payload": {**BASE_MSG["payload"], "status": "confirmed"}}
        r2 = await client.post("/messages/ingest", json=v2)
        assert r2.json()["outcome"] == "skipped"
        assert r2.json()["decision_detail"] == "skip_stale"


class TestVersionedAttemptLogging:
    """Attempt log must store both entity_key and versioned idempotent_key."""

    @pytest.mark.asyncio
    async def test_attempt_log_has_both_keys(self, client):
        r = await client.post("/messages/ingest", json=BASE_MSG)
        attempt_r = await client.get("/attempts/1")
        assert attempt_r.status_code == 200
        attempt = attempt_r.json()
        assert "entity_key" in attempt
        assert "idempotent_key" in attempt
        assert "decision" in attempt
        # Raw decision stored in log, not API family
        assert attempt["decision"] == "accept_new"

    @pytest.mark.asyncio
    async def test_attempt_log_stores_raw_decision(self, client):
        """Even for supersede, the log stores 'accept_supersede', not 'accepted'."""
        await client.post("/messages/ingest", json=BASE_MSG)
        v2 = {**BASE_MSG, "version": 2, "payload": {**BASE_MSG["payload"], "status": "confirmed"}}
        await client.post("/messages/ingest", json=v2)

        attempt_r = await client.get("/attempts/2")
        assert attempt_r.json()["decision"] == "accept_supersede"


class TestBatchIngest:

    @pytest.mark.asyncio
    async def test_replay_batch_all_skipped(self, client):
        batch = {"messages": [{**BASE_MSG, "business_key": f"ORD-BATCH-{i:03d}"} for i in range(5)]}
        r1 = await client.post("/messages/ingest-batch", json=batch)
        assert all(m["outcome"] == "accepted" for m in r1.json()["results"])

        r2 = await client.post("/messages/ingest-batch", json=batch)
        assert all(m["outcome"] == "skipped" for m in r2.json()["results"])

    @pytest.mark.asyncio
    async def test_batch_mixed_outcomes(self, client):
        msg_new = {**BASE_MSG, "business_key": "ORD-MIX-001"}
        msg_dup_base = {**BASE_MSG, "business_key": "ORD-MIX-002"}
        await client.post("/messages/ingest", json=msg_dup_base)
        msg_quarantine_base = {**BASE_MSG, "business_key": "ORD-MIX-003"}
        await client.post("/messages/ingest", json=msg_quarantine_base)
        msg_quarantine = {**msg_quarantine_base, "payload": {**BASE_MSG["payload"], "total_amount": 1.00}}

        batch = {"messages": [msg_new, msg_dup_base, msg_quarantine]}
        r = await client.post("/messages/ingest-batch", json=batch)
        outcomes = [m["outcome"] for m in r.json()["results"]]
        assert outcomes == ["accepted", "skipped", "quarantined"]


class TestReconciliationFamilyCounts:
    """
    Reconciliation report must aggregate raw decisions into families.
    The report must never expose internal decision labels.
    """

    @pytest.mark.asyncio
    async def test_report_uses_family_keys_not_internal_labels(self, client):
        await client.post("/messages/ingest", json={**BASE_MSG, "business_key": "ORD-RC-001"})
        r = await client.get("/reports/reconciliation")
        totals = r.json()["totals"]
        # Must have these family keys
        assert "accepted" in totals
        assert "skipped" in totals
        assert "quarantined" in totals
        # Must NOT have internal decision labels
        assert "accept_new" not in totals
        assert "accept_supersede" not in totals
        assert "skip_duplicate" not in totals
        assert "skip_stale" not in totals

    @pytest.mark.asyncio
    async def test_accepted_counts_both_accept_new_and_accept_supersede(self, client):
        """accept_new + accept_supersede both count toward 'accepted' family."""
        await client.post("/messages/ingest", json={**BASE_MSG, "business_key": "ORD-RC-002"})
        v2 = {**BASE_MSG, "business_key": "ORD-RC-002", "version": 2,
              "payload": {**BASE_MSG["payload"], "status": "confirmed"}}
        await client.post("/messages/ingest", json=v2)

        r = await client.get("/reports/reconciliation")
        assert r.json()["totals"]["accepted"] == 2  # accept_new + accept_supersede

    @pytest.mark.asyncio
    async def test_skipped_counts_both_duplicate_and_stale(self, client):
        """skip_duplicate + skip_stale both count toward 'skipped' family."""
        # exact duplicate → skip_duplicate
        bk = "ORD-RC-003"
        await client.post("/messages/ingest", json={**BASE_MSG, "business_key": bk})
        await client.post("/messages/ingest", json={**BASE_MSG, "business_key": bk})
        # stale version → skip_stale
        v2 = {**BASE_MSG, "business_key": bk, "version": 2,
              "payload": {**BASE_MSG["payload"], "status": "confirmed"}}
        await client.post("/messages/ingest", json=v2)
        await client.post("/messages/ingest", json={**BASE_MSG, "business_key": bk})  # v1 again

        r = await client.get("/reports/reconciliation")
        assert r.json()["totals"]["skipped"] == 2  # 1 duplicate + 1 stale

    @pytest.mark.asyncio
    async def test_full_reconciliation_counts(self, client):
        """End-to-end: 2 accepted, 1 skipped, 1 quarantined."""
        await client.post("/messages/ingest", json={**BASE_MSG, "business_key": "ORD-F-001"})
        await client.post("/messages/ingest", json={**BASE_MSG, "business_key": "ORD-F-002"})
        await client.post("/messages/ingest", json={**BASE_MSG, "business_key": "ORD-F-001"})  # duplicate → skipped
        mutated = {**BASE_MSG, "business_key": "ORD-F-002",
                   "payload": {**BASE_MSG["payload"], "total_amount": 999.99}}
        await client.post("/messages/ingest", json=mutated)  # quarantine

        r = await client.get("/reports/reconciliation")
        totals = r.json()["totals"]
        assert totals["accepted"] == 2
        assert totals["skipped"] == 1
        assert totals["quarantined"] == 1
        assert totals["total_attempts"] == 4


class TestCorrelationIdPropagation:
    """
    Skipped messages (duplicate and stale) must return the same correlation_id
    as the already-accepted version of that entity.

    The correlation context comes from the active staging record linked via
    existing_state.active_staging_id → StagingRecord.correlation_id.
    """

    @pytest.mark.asyncio
    async def test_skip_duplicate_returns_same_correlation_id(self, client):
        """
        A duplicate replay must return the same correlation_id as the original
        accepted request — not None.
        """
        r_accept = await client.post("/messages/ingest", json=BASE_MSG)
        assert r_accept.json()["outcome"] == "accepted"
        accepted_corr_id = r_accept.json()["correlation_id"]
        assert accepted_corr_id is not None

        r_skip = await client.post("/messages/ingest", json=BASE_MSG)
        assert r_skip.json()["outcome"] == "skipped"
        assert r_skip.json()["decision_detail"] == "skip_duplicate"

        assert r_skip.json()["correlation_id"] == accepted_corr_id, (
            "skip_duplicate must propagate the existing correlation_id, not return None"
        )

    @pytest.mark.asyncio
    async def test_skip_stale_returns_same_correlation_id(self, client):
        """
        A late-arriving stale version must return the correlation_id of the
        currently active (newer) version — not None.
        """
        bk = "ORD-STALE-CORR-001"
        v2 = {**BASE_MSG, "business_key": bk, "version": 2,
              "payload": {**BASE_MSG["payload"], "status": "confirmed"}}

        r_accept = await client.post("/messages/ingest", json=v2)
        assert r_accept.json()["outcome"] == "accepted"
        accepted_corr_id = r_accept.json()["correlation_id"]
        assert accepted_corr_id is not None

        # v1 arrives late
        v1 = {**BASE_MSG, "business_key": bk, "version": 1}
        r_stale = await client.post("/messages/ingest", json=v1)
        assert r_stale.json()["outcome"] == "skipped"
        assert r_stale.json()["decision_detail"] == "skip_stale"

        assert r_stale.json()["correlation_id"] == accepted_corr_id, (
            "skip_stale must propagate the existing correlation_id, not return None"
        )

    @pytest.mark.asyncio
    async def test_skip_duplicate_correlation_id_in_attempt_log(self, client):
        """
        The ingestion attempt log entry for a skip_duplicate must also carry
        the propagated correlation_id, not None.
        """
        r_accept = await client.post("/messages/ingest", json={**BASE_MSG, "business_key": "ORD-LOG-CORR-001"})
        accepted_corr_id = r_accept.json()["correlation_id"]

        await client.post("/messages/ingest", json={**BASE_MSG, "business_key": "ORD-LOG-CORR-001"})

        # Attempt 2 is the skip — fetch it
        attempt_r = await client.get("/attempts/2")
        assert attempt_r.status_code == 200
        attempt = attempt_r.json()
        assert attempt["decision"] == "skip_duplicate"
        assert attempt["correlation_id"] == accepted_corr_id, (
            "IngestionAttemptLog.correlation_id must be populated for skip_duplicate"
        )
