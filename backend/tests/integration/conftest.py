"""PostgreSQL fixtures available only to explicitly opted-in integration tests."""

import asyncio
from pathlib import Path
from urllib.parse import urlsplit
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio


@pytest.fixture
def database_url(request):
    dsn = request.config.getoption("--test-database-url")
    if dsn is None:
        pytest.skip("PostgreSQL integration requires explicit --test-database-url")
    target = urlsplit(dsn)
    if (
        target.scheme not in {"postgres", "postgresql"}
        or target.hostname not in {"localhost", "127.0.0.1", "::1"}
        or target.path != "/inteldigest_test"
        or target.query or target.fragment
    ):
        pytest.fail("Use a local inteldigest_test URL without query or fragment")
    return dsn


@pytest_asyncio.fixture
async def connection(database_url):
    conn = await asyncpg.connect(database_url, timeout=5, command_timeout=5)
    transaction = conn.transaction()
    try:
        await transaction.start()
        migration = Path(__file__).resolve().parents[3] / "migrations/001_create_jobs.up.sql"
        await conn.execute(migration.read_text())
        yield conn
    finally:
        try:
            if conn.is_in_transaction():
                await transaction.rollback()
        finally:
            await conn.close(timeout=5)


@pytest_asyncio.fixture
async def claim_pool(database_url):
    """Two real connections in an owned schema, never shared application tables."""
    admin = await asyncpg.connect(database_url, timeout=5, command_timeout=5)
    schema = f"m05_{uuid4().hex}"
    created = False
    try:
        await admin.execute(f'CREATE SCHEMA "{schema}"')
        created = True
        await admin.execute("SELECT set_config('search_path', $1, false)", schema)
        migration = Path(__file__).resolve().parents[3] / "migrations/001_create_jobs.up.sql"
        await admin.execute(migration.read_text())
        pool = await asyncpg.create_pool(
            database_url, min_size=2, max_size=2, timeout=5, command_timeout=5,
            server_settings={"search_path": schema},
        )
        try:
            yield pool
        finally:
            try:
                await asyncio.wait_for(pool.close(), timeout=5)
            finally:
                pool.terminate()
    finally:
        try:
            if created:
                await admin.execute(f'DROP SCHEMA "{schema}" CASCADE')
        finally:
            await admin.close(timeout=5)
