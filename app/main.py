from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import jobs, messages, queries
from app.api.routes import ambiguous as ambiguous_routes
from app.api.routes import circuit as circuit_routes
from app.api.routes import dashboard as dashboard_routes
from app.api.routes import delivery as delivery_routes
from app.api.routes import inspector as inspector_routes
from app.api.routes import replay as replay_routes
from app.core.config import settings
from app.core.database import AsyncSessionLocal, init_db
from app.core.seed import seed_downstream_systems


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    async with AsyncSessionLocal() as session:
        await seed_downstream_systems(session)
    yield


app = FastAPI(
    title="Integration Pipeline Console",
    description=(
        "Operator-facing console backend for an idempotent enterprise integration pipeline.\n\n"
        "Demonstrates correctness, delivery resilience, replay, and full operational audit.\n\n"
        "---\n\n"
        "**Pillar 1 — Correctness:** idempotent write paths, five-branch deduplication, "
        "staging/production layers, correlation mapping, audit reconciliation.\n\n"
        "**Pillar 2 — Delivery Resilience:** retry classification, DLQ management, "
        "circuit breaker per downstream system, ambiguous outcome queue with "
        "query-before-retry, and operator-initiated replay with full execution audit.\n\n"
        "**Pillar 3 — Explainability:** entity/message inspector assembling the "
        "complete end-to-end story of any record across ingestion, delivery, anomaly "
        "queues, and replay activity — with a unified ordered event timeline.\n\n"
        "**Dashboard:** pipeline health overview, circuit summary, replay summary, "
        "and recent activity feed across all subsystems.\n\n"
        "Start at `GET /dashboard/overview` for the system health snapshot, "
        "or use `GET /inspect/by-idempotent-key/{key}` to trace any specific record."
    ),
    version="0.7.0",
    lifespan=lifespan,
)

app.include_router(messages.router)
app.include_router(queries.router)
app.include_router(jobs.router)
app.include_router(delivery_routes.router)
app.include_router(circuit_routes.router)
app.include_router(ambiguous_routes.router)
app.include_router(replay_routes.router)
app.include_router(inspector_routes.router)
app.include_router(dashboard_routes.router)


@app.get("/health", tags=["meta"])
async def health() -> dict:
    return {"status": "ok", "service": settings.app_name, "version": "0.7.0"}
