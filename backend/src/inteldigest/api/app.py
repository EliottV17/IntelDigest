"""ASGI factory with process-local PostgreSQL ownership."""

import asyncio
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI

from inteldigest.api.routes import digests, health
from inteldigest.config import load_api
from inteldigest.jobs.repository import Repository


def create_app() -> FastAPI:
    """Construct without configuration reads or connections until lifespan starts."""
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        config = load_api()
        pool = await asyncpg.create_pool(
            config.database_url, min_size=1, max_size=10, timeout=5, command_timeout=10,
        )
        try:
            app.state.repository = Repository(pool)
            yield
        finally:
            # asyncpg terminates its connections if close is cancelled by the deadline.
            async with asyncio.timeout(10):
                await pool.close()

    app = FastAPI(title="IntelDigest", lifespan=lifespan)
    app.include_router(health.router)
    app.include_router(digests.router)
    return app
