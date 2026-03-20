"""
Shared pytest fixtures for the idempotent pipeline test suite.

Uses an in-memory SQLite database so each test run starts with a clean slate.
The downstream_systems fixture pre-seeds DownstreamSystem rows so delivery
tests can call simulate_delivery() without hitting a missing-system error.
"""

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.core.database import Base, get_db
from app.main import app

TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


@pytest_asyncio.fixture(scope="function")
async def db_engine():
    engine = create_async_engine(TEST_DATABASE_URL, echo=False)
    async with engine.begin() as conn:
        from app.models import (  # noqa: F401
            ambiguous,
            circuit,
            correlation,
            delivery,
            idempotent_state,
            ingestion_log,
            replay,
            review,
            staging,
        )
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await engine.dispose()


@pytest_asyncio.fixture(scope="function")
async def db_session(db_engine):
    session_factory = async_sessionmaker(db_engine, expire_on_commit=False)
    async with session_factory() as session:
        yield session


@pytest_asyncio.fixture(scope="function")
async def seeded_db_session(db_session: AsyncSession):
    """db_session with DownstreamSystem rows pre-loaded from seed data."""
    from app.core.seed import seed_downstream_systems
    await seed_downstream_systems(db_session)
    yield db_session


@pytest_asyncio.fixture(scope="function")
async def client(db_engine):
    """HTTP test client with the database overridden to use the test engine."""
    session_factory = async_sessionmaker(db_engine, expire_on_commit=False)

    async def override_get_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac

    app.dependency_overrides.clear()


@pytest_asyncio.fixture(scope="function")
async def seeded_client(db_engine):
    """
    HTTP test client with seed data loaded.
    Use this for delivery tests that require DownstreamSystem rows.
    """
    session_factory = async_sessionmaker(db_engine, expire_on_commit=False)

    async def override_get_db():
        async with session_factory() as session:
            yield session

    app.dependency_overrides[get_db] = override_get_db

    # Seed downstream systems before yielding the client
    async with session_factory() as session:
        from app.core.seed import seed_downstream_systems
        await seed_downstream_systems(session)

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
    ) as ac:
        yield ac

    app.dependency_overrides.clear()
