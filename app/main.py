from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import jobs, messages, queries
from app.core.config import settings
from app.core.database import init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(
    title=settings.app_name,
    description=(
        "Reference implementation of an idempotent write path for distributed enterprise integration. "
        "Demonstrates deterministic key derivation, five-branch deduplication, "
        "staging/production layers, correlation mapping, and audit reconciliation."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(messages.router)
app.include_router(queries.router)
app.include_router(jobs.router)


@app.get("/health", tags=["meta"])
async def health() -> dict:
    return {"status": "ok", "service": settings.app_name}
