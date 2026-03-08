"""
Tests for production projection materialization.

Validates:
  - First materialization creates one projection row per entity.
  - A newer version overwrites (updates) the existing row — entity_key stays unique.
  - An older version is skipped — the existing row is not downgraded.
  - Two different entities produce two independent projection rows.
"""

import json

import pytest
from sqlalchemy import func, select

from app.models.correlation import ProductionProjection
from app.models.staging import StagingRecord
from app.services.key_builder import build_entity_key
from app.services.materialization_service import materialize_complete_records


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _staging(
    session,
    source_system: str,
    entity_type: str,
    business_key: str,
    version: int,
    status: str = "complete",
    payload: dict | None = None,
    idempotent_key: str | None = None,
    correlation_id: str | None = None,
) -> StagingRecord:
    ik = idempotent_key or f"{source_system}:::{entity_type}:::{business_key}:::{version}"
    r = StagingRecord(
        idempotent_key=ik,
        source_system=source_system,
        entity_type=entity_type,
        business_key=business_key,
        version=version,
        payload=json.dumps(payload or {"status": "created"}),
        payload_hash="a" * 64,
        completeness_status=status,
        correlation_id=correlation_id,
    )
    session.add(r)
    return r


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

class TestFirstMaterialization:

    @pytest.mark.asyncio
    async def test_complete_record_is_promoted(self, db_session):
        _staging(db_session, "oms", "order", "ORD-MAT-001", version=1)
        await db_session.flush()

        result = await materialize_complete_records(db_session)

        assert result["promoted"] == 1
        assert result["skipped_already_current"] == 0

    @pytest.mark.asyncio
    async def test_promoted_record_appears_in_production(self, db_session):
        _staging(db_session, "oms", "order", "ORD-MAT-002", version=1,
                 payload={"status": "created", "amount": 50.0})
        await db_session.flush()

        await materialize_complete_records(db_session)

        entity_key = build_entity_key("oms", "order", "ORD-MAT-002")
        proj = await db_session.scalar(
            select(ProductionProjection).where(ProductionProjection.entity_key == entity_key)
        )
        assert proj is not None
        assert proj.version == 1
        assert proj.entity_key == entity_key
        assert proj.source_system == "oms"

    @pytest.mark.asyncio
    async def test_partial_record_is_not_promoted(self, db_session):
        _staging(db_session, "oms", "order", "ORD-MAT-003", version=1, status="partial")
        await db_session.flush()

        result = await materialize_complete_records(db_session)

        assert result["promoted"] == 0
        count = await db_session.scalar(select(func.count()).select_from(ProductionProjection))
        assert count == 0

    @pytest.mark.asyncio
    async def test_staging_record_marked_promoted_after_materialization(self, db_session):
        rec = _staging(db_session, "oms", "order", "ORD-MAT-004", version=1)
        await db_session.flush()
        staging_id = rec.id

        await materialize_complete_records(db_session)

        await db_session.refresh(rec)
        assert rec.completeness_status == "promoted"
        assert rec.promoted_at is not None


class TestProjectionOverwrite:
    """
    The central correctness property: when a newer version is materialized,
    the existing production row is updated in place — entity_key remains unique.
    """

    @pytest.mark.asyncio
    async def test_newer_version_overwrites_existing_row(self, db_session):
        _staging(db_session, "oms", "order", "ORD-OW-001", version=1,
                 payload={"status": "created"})
        await db_session.flush()
        await materialize_complete_records(db_session)

        _staging(db_session, "oms", "order", "ORD-OW-001", version=2,
                 payload={"status": "confirmed"})
        await db_session.flush()
        await materialize_complete_records(db_session)

        entity_key = build_entity_key("oms", "order", "ORD-OW-001")

        # Only ONE row in production for this entity
        count = await db_session.scalar(
            select(func.count()).select_from(ProductionProjection).where(
                ProductionProjection.entity_key == entity_key
            )
        )
        assert count == 1, "entity_key must remain unique — no duplicate rows after overwrite"

        proj = await db_session.scalar(
            select(ProductionProjection).where(ProductionProjection.entity_key == entity_key)
        )
        assert proj.version == 2
        assert json.loads(proj.payload)["status"] == "confirmed"

    @pytest.mark.asyncio
    async def test_overwrite_updates_idempotent_key_for_traceability(self, db_session):
        ik_v1 = "oms:::order:::ORD-OW-002:::1"
        ik_v2 = "oms:::order:::ORD-OW-002:::2"
        _staging(db_session, "oms", "order", "ORD-OW-002", version=1, idempotent_key=ik_v1)
        await db_session.flush()
        await materialize_complete_records(db_session)

        _staging(db_session, "oms", "order", "ORD-OW-002", version=2, idempotent_key=ik_v2)
        await db_session.flush()
        await materialize_complete_records(db_session)

        entity_key = build_entity_key("oms", "order", "ORD-OW-002")
        proj = await db_session.scalar(
            select(ProductionProjection).where(ProductionProjection.entity_key == entity_key)
        )
        # idempotent_key should now point to v2
        assert proj.idempotent_key == ik_v2

    @pytest.mark.asyncio
    async def test_older_version_does_not_downgrade_production(self, db_session):
        """
        If v2 is already in production and v1 arrives late (e.g. from a replay),
        the production row must remain at v2.
        """
        _staging(db_session, "oms", "order", "ORD-OW-003", version=2,
                 payload={"status": "confirmed"})
        await db_session.flush()
        await materialize_complete_records(db_session)

        # Simulate late-arriving v1 completing (e.g. from a replay)
        _staging(db_session, "oms", "order", "ORD-OW-003", version=1,
                 payload={"status": "created"})
        await db_session.flush()
        result = await materialize_complete_records(db_session)

        assert result["skipped_already_current"] == 1

        entity_key = build_entity_key("oms", "order", "ORD-OW-003")
        proj = await db_session.scalar(
            select(ProductionProjection).where(ProductionProjection.entity_key == entity_key)
        )
        assert proj.version == 2, "production must not be downgraded by a stale version"

    @pytest.mark.asyncio
    async def test_two_entities_produce_two_independent_rows(self, db_session):
        _staging(db_session, "oms", "order", "ORD-OW-004", version=1)
        _staging(db_session, "oms", "order", "ORD-OW-005", version=1)
        await db_session.flush()

        await materialize_complete_records(db_session)

        total = await db_session.scalar(
            select(func.count()).select_from(ProductionProjection)
        )
        assert total == 2

    @pytest.mark.asyncio
    async def test_materialization_is_idempotent(self, db_session):
        """Running the job twice must not create duplicate rows or raise errors."""
        _staging(db_session, "oms", "order", "ORD-OW-006", version=1)
        await db_session.flush()

        r1 = await materialize_complete_records(db_session)
        r2 = await materialize_complete_records(db_session)

        assert r1["promoted"] == 1
        assert r2["promoted"] == 0  # already promoted on first run

        total = await db_session.scalar(
            select(func.count()).select_from(ProductionProjection)
        )
        assert total == 1
