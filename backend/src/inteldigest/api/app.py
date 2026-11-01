"""ASGI factory with process-local PostgreSQL and Redis Publisher ownership."""

import asyncio
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI

from inteldigest.api.routes import digests, health
from inteldigest.config import load_api
from inteldigest.jobs.repository import Repository
from inteldigest.queue.publisher import Publisher


def create_app() -> FastAPI:
    """Construct without configuration reads or connections until lifespan starts."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        config = load_api()
        pool = await asyncpg.create_pool(
            config.database_url,
            min_size=1,
            max_size=10,
            timeout=5,
            command_timeout=10,
        )
        try:
            publisher = await Publisher.connect(
                config.redis_url,
                config.redis_stream,
                dial_timeout_seconds=config.redis_dial_timeout_seconds,
                publish_timeout_seconds=config.redis_publish_timeout_seconds,
            )
            try:
                app.state.repository = Repository(pool)
                app.state.publisher = publisher
                yield
            finally:
                await publisher.aclose()
        finally:
            # asyncpg terminates its connections if close is cancelled by the deadline.
            async with asyncio.timeout(10):
                await pool.close()

    app = FastAPI(title="IntelDigest", lifespan=lifespan)
    app.include_router(health.router)
    app.include_router(digests.router)
    return app
