"""
Tests for CorrelationMap uniqueness constraint.

Validates:
  - (source_system, source_key) pair is unique at the DB level.
  - Attempting to map the same source key to a second correlation_id raises IntegrityError.
  - The correlation engine's _create_mapping handles IntegrityError gracefully and returns
    the already-committed correlation_id rather than propagating the error.
  - The full ingest path never creates duplicate mappings even when the same entity
    is ingested twice (idempotent re-delivery).
"""

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from app.models.correlation import CorrelationMap
from app.services.correlation_engine import _create_mapping, resolve_correlation_id


class TestCorrelationMapUniqueConstraint:

    @pytest.mark.asyncio
    async def test_first_mapping_is_inserted(self, db_session):
        corr_id = await _create_mapping(
            db_session, "CORR-001", "oms", "ORD-CON-001", "self_generated"
        )
        await db_session.commit()

        assert corr_id == "CORR-001"
        row = await db_session.scalar(
            select(CorrelationMap).where(
                CorrelationMap.source_system == "oms",
                CorrelationMap.source_key == "ORD-CON-001",
            )
        )
        assert row is not None
        assert row.correlation_id == "CORR-001"

    @pytest.mark.asyncio
    async def test_duplicate_source_key_raises_integrity_error(self, db_session):
        """
        Directly inserting two rows with the same (source_system, source_key)
        must be rejected at the DB level.
        """
        row1 = CorrelationMap(
            correlation_id="CORR-A",
            source_system="oms",
            source_key="ORD-CON-DUP",
            mapping_method="self_generated",
        )
        db_session.add(row1)
        await db_session.commit()

        row2 = CorrelationMap(
            correlation_id="CORR-B",  # different correlation_id, same source key
            source_system="oms",
            source_key="ORD-CON-DUP",
            mapping_method="self_generated",
        )
        db_session.add(row2)
        with pytest.raises(IntegrityError):
            await db_session.commit()

    @pytest.mark.asyncio
    async def test_create_mapping_handles_race_gracefully(self, db_session):
        """
        Simulates a concurrent-write race: the first _create_mapping succeeds,
        the second one (same source key, different correlation_id) hits the
        UniqueConstraint but must NOT propagate an error — it returns the
        already-committed correlation_id instead.
        """
        # First writer wins
        corr_id_first = await _create_mapping(
            db_session, "CORR-FIRST", "oms", "ORD-CON-RACE", "self_generated"
        )
        await db_session.commit()

        # Second writer races in with a different correlation_id for the same key
        corr_id_second = await _create_mapping(
            db_session, "CORR-SECOND", "oms", "ORD-CON-RACE", "self_generated"
        )
        await db_session.commit()

        # Both should resolve to the first-committed correlation_id
        assert corr_id_first == "CORR-FIRST"
        assert corr_id_second == "CORR-FIRST", (
            "_create_mapping must return the already-committed ID, not the losing one"
        )

        # Exactly one row in DB for this source key
        count = await db_session.scalar(
            select(func.count()).select_from(CorrelationMap).where(
                CorrelationMap.source_system == "oms",
                CorrelationMap.source_key == "ORD-CON-RACE",
            )
        )
        assert count == 1

    @pytest.mark.asyncio
    async def test_different_source_systems_can_have_same_source_key(self, db_session):
        """
        (oms, ORD-001) and (wms, ORD-001) are different entities — constraint must allow both.
        """
        row_oms = CorrelationMap(
            correlation_id="CORR-OMS",
            source_system="oms",
            source_key="ORD-SAME-KEY",
            mapping_method="self_generated",
        )
        row_wms = CorrelationMap(
            correlation_id="CORR-WMS",
            source_system="wms",
            source_key="ORD-SAME-KEY",
            mapping_method="self_generated",
        )
        db_session.add(row_oms)
        db_session.add(row_wms)
        # Must not raise
        await db_session.commit()

        count = await db_session.scalar(
            select(func.count()).select_from(CorrelationMap).where(
                CorrelationMap.source_key == "ORD-SAME-KEY"
            )
        )
        assert count == 2

    @pytest.mark.asyncio
    async def test_resolve_correlation_id_never_creates_duplicate_mapping(self, db_session):
        """
        Calling resolve_correlation_id twice for the same source key must not
        create a second CorrelationMap row.
        """
        payload = {"customer_id": "CUST-X", "order_date": "2026-03-01", "total_amount": 99.0}

        corr1, method1 = await resolve_correlation_id(db_session, "oms", "ORD-CON-IDEM", payload)
        await db_session.commit()

        corr2, method2 = await resolve_correlation_id(db_session, "oms", "ORD-CON-IDEM", payload)
        await db_session.commit()

        assert corr1 == corr2, "same source key must always resolve to the same correlation_id"
        assert method2 == "existing", "second call must hit the 'existing' fast path"

        count = await db_session.scalar(
            select(func.count()).select_from(CorrelationMap).where(
                CorrelationMap.source_system == "oms",
                CorrelationMap.source_key == "ORD-CON-IDEM",
            )
        )
        assert count == 1

    @pytest.mark.asyncio
    async def test_ingest_replay_does_not_duplicate_correlation_mapping(self, client):
        """
        Full-stack test: ingesting the same message twice must not create two
        CorrelationMap rows for the same source key.
        """
        msg = {
            "source_system": "oms",
            "entity_type": "order",
            "business_key": "ORD-CON-REPLAY",
            "version": 1,
            "payload": {"customer_id": "CUST-Y", "order_date": "2026-03-01", "total_amount": 50.0},
        }
        r1 = await client.post("/messages/ingest", json=msg)
        r2 = await client.post("/messages/ingest", json=msg)

        assert r1.json()["outcome"] == "accepted"
        assert r2.json()["outcome"] == "skipped"
        # Both calls must resolve to the same correlation_id
        assert r1.json()["correlation_id"] == r2.json()["correlation_id"]
