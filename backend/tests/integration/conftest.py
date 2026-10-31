"""PostgreSQL fixtures available only to explicitly opted-in integration tests."""

from pathlib import Path
from urllib.parse import urlsplit

import asyncpg
import pytest
import pytest_asyncio


@pytest_asyncio.fixture
async def connection(request):
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
    conn = await asyncpg.connect(dsn, timeout=5, command_timeout=5)
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
