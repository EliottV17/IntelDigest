"""Parameterized job storage using asyncpg's default PostgreSQL codecs."""

import json
from collections.abc import Mapping
from typing import Any, Protocol
from uuid import UUID, uuid4

from inteldigest.jobs.models import Job


class JobDatabase(Protocol):
    """The fetchrow surface shared by asyncpg connections and pools."""

    async def fetchrow(self, query: str, *args: Any) -> Mapping[str, Any] | None: ...


class JobNotFound(LookupError):
    def __init__(self) -> None:
        super().__init__("job not found")


_COLUMNS = "id, url, status, result, error, created_at, updated_at"


def _job_from_row(row: Mapping[str, Any]) -> Job:
    values = dict(row)
    # asyncpg returns JSONB as JSON text by default, not a decoded object.
    if values["result"] is not None:
        values["result"] = json.loads(values["result"])
    return Job.model_validate(values)


class Repository:
    """Borrow a connection/pool; the caller owns its lifecycle and timeouts."""

    def __init__(self, db: JobDatabase) -> None:
        self._db = db

    async def create_job(self, url: str) -> Job:
        row = await self._db.fetchrow(
            f"INSERT INTO jobs (id, url) VALUES ($1, $2) RETURNING {_COLUMNS}",
            uuid4(), url,
        )
        if row is None:
            raise RuntimeError("job insert returned no row")
        return _job_from_row(row)

    async def get_job_by_id(self, job_id: UUID) -> Job:
        row = await self._db.fetchrow(
            f"SELECT {_COLUMNS} FROM jobs WHERE id = $1", job_id,
        )
        if row is None:
            raise JobNotFound()
        return _job_from_row(row)
