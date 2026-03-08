"""Tests for Correlation ID resolution — explicit reference and self-generated."""

import pytest

from app.services.key_builder import build_entity_key


class TestCorrelation:

    @pytest.mark.asyncio
    async def test_explicit_correlation_links_wms_to_oms(self, client):
        """WMS picking ticket with explicit oms_order_id should share the OMS order's correlation ID."""
        oms_msg = {
            "source_system": "oms",
            "entity_type": "order",
            "business_key": "ORD-CORR-001",
            "version": 1,
            "payload": {"customer_id": "CUST-500", "order_date": "2026-03-01", "total_amount": 100.0},
        }
        oms_resp = await client.post("/messages/ingest", json=oms_msg)
        assert oms_resp.json()["outcome"] == "accepted"
        oms_correlation = oms_resp.json()["correlation_id"]
        assert oms_correlation is not None

        wms_msg = {
            "source_system": "wms",
            "entity_type": "picking_ticket",
            "business_key": "WH-PKT-CORR-001",
            "version": 1,
            "payload": {
                "warehouse_id": "WH-EAST",
                "oms_order_id": "ORD-CORR-001",
                "items": [{"sku": "SKU-001", "qty": 1}],
            },
        }
        wms_resp = await client.post("/messages/ingest", json=wms_msg)
        assert wms_resp.json()["outcome"] == "accepted"
        wms_correlation = wms_resp.json()["correlation_id"]

        assert wms_correlation == oms_correlation

    @pytest.mark.asyncio
    async def test_correlation_endpoint_returns_all_mapped_systems(self, client):
        """GET /correlations/{id} should return entries for all mapped source systems."""
        oms_msg = {
            "source_system": "oms",
            "entity_type": "order",
            "business_key": "ORD-CORR-002",
            "version": 1,
            "payload": {"customer_id": "CUST-501", "order_date": "2026-03-01", "total_amount": 200.0},
        }
        oms_resp = await client.post("/messages/ingest", json=oms_msg)
        corr_id = oms_resp.json()["correlation_id"]

        wms_msg = {
            "source_system": "wms",
            "entity_type": "picking_ticket",
            "business_key": "WH-PKT-CORR-002",
            "version": 1,
            "payload": {"oms_order_id": "ORD-CORR-002", "items": []},
        }
        await client.post("/messages/ingest", json=wms_msg)

        corr_resp = await client.get(f"/correlations/{corr_id}")
        assert corr_resp.status_code == 200
        mappings = corr_resp.json()["mappings"]
        source_systems = {m["source_system"] for m in mappings}
        assert "oms" in source_systems
        assert "wms" in source_systems

    @pytest.mark.asyncio
    async def test_state_lookup_uses_entity_key_not_idempotent_key(self, client):
        """
        After ingesting v1 and v2 of the same entity, GET /state/{entity_key}
        returns current_version=2 — proving entity_key is the lookup key.
        """
        bk = "ORD-CORR-003"
        msg_v1 = {"source_system": "oms", "entity_type": "order", "business_key": bk,
                  "version": 1, "payload": {"status": "created", "total_amount": 50.0}}
        msg_v2 = {"source_system": "oms", "entity_type": "order", "business_key": bk,
                  "version": 2, "payload": {"status": "confirmed", "total_amount": 50.0}}
        await client.post("/messages/ingest", json=msg_v1)
        await client.post("/messages/ingest", json=msg_v2)

        entity_key = build_entity_key("oms", "order", bk)
        state_r = await client.get(f"/state/{entity_key}")
        assert state_r.status_code == 200
        assert state_r.json()["current_version"] == 2
        assert state_r.json()["last_decision"] == "accept_supersede"

    @pytest.mark.asyncio
    async def test_unknown_correlation_id_returns_404(self, client):
        resp = await client.get("/correlations/nonexistent-id-xyz")
        assert resp.status_code == 404

