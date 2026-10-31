import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from inteldigest.jobs.models import Job
from inteldigest.jobs.repository import JobNotFound, Repository

URL = "https://example.com/article?title='; DROP TABLE jobs; --"
NOW = datetime(2026, 10, 31, 12, 30, 45, 123456, tzinfo=UTC)


def job_row(**changes):
    return {
        "id": uuid4(), "url": URL, "status": "pending",
        "result": None, "error": None, "created_at": NOW, "updated_at": NOW,
    } | changes


class FakeDatabase:
    def __init__(self, row=None, error=None):
        self.row = row
        self.error = error
        self.calls = []

    async def fetchrow(self, query, *args):
        self.calls.append((" ".join(query.split()), args))
        if self.error is not None:
            raise self.error
        return self.row


async def test_create_returns_stored_defaults_and_binds_unmodified_url():
    row = job_row()
    db = FakeDatabase(row)
    before = datetime.now(UTC)
    job = await Repository(db).create_job(URL)
    assert job == Job(**row)
    assert job.status == "pending"
    assert job.result is None and job.error is None
    assert job.created_at == job.updated_at == NOW < before
    query, args = db.calls[0]
    assert len(db.calls) == 1
    assert query.startswith("INSERT INTO jobs (id, url) VALUES ($1, $2) RETURNING ")
    assert URL not in query
    assert isinstance(args[0], UUID) and args[0].version == 4
    assert args[1:] == (URL,)
    await Repository(db).create_job(URL)
    assert db.calls[1][1][0] != args[0]


@pytest.mark.parametrize("status", ["pending", "processing", "completed", "failed"])
@pytest.mark.parametrize("raw, expected", [
    (None, None), ('{"title":"Artículo","points":[1,true,null]}',
                   {"title": "Artículo", "points": [1, True, None]}),
    ('"a JSON string"', "a JSON string"), ('[1,false]', [1, False]),
    ('42', 42), ('false', False), ('null', None),
])
async def test_lookup_decodes_jsonb_and_preserves_fields(status, raw, expected):
    row = job_row(status=status, result=raw, error="safe diagnosis")
    db = FakeDatabase(row)
    job = await Repository(db).get_job_by_id(row["id"])
    assert job == Job(**(row | {"result": expected}))
    assert db.calls == [(
        ("SELECT id, url, status, result, error, created_at, updated_at "
         "FROM jobs WHERE id = $1"), (row["id"],),
    )]
    serialized = job.model_dump(mode="json")
    assert serialized == {
        "id": str(row["id"]), "url": URL, "status": status,
        "result": expected, "error": "safe diagnosis",
        "created_at": "2026-10-31T12:30:45.123456Z",
        "updated_at": "2026-10-31T12:30:45.123456Z",
    }
    assert Job.model_validate_json(job.model_dump_json()) == job


async def test_lookup_null_fields_and_missing_job_are_distinct():
    row = job_row()
    job = await Repository(FakeDatabase(row)).get_job_by_id(row["id"])
    assert job.result is None and job.error is None
    assert job.model_dump(mode="json")["result"] is None
    assert job.model_dump(mode="json")["error"] is None
    db = FakeDatabase()
    with pytest.raises(JobNotFound, match="^job not found$"):
        await Repository(db).get_job_by_id(uuid4())
    assert len(db.calls) == 1


@pytest.mark.parametrize("operation", ["create", "lookup"])
async def test_database_failure_is_not_converted_to_missing_or_success(operation, capsys):
    error = RuntimeError("database unavailable")
    db = FakeDatabase(error=error)
    repo = Repository(db)
    with pytest.raises(RuntimeError) as caught:
        if operation == "create":
            await repo.create_job(URL)
        else:
            await repo.get_job_by_id(uuid4())
    assert caught.value is error
    assert len(db.calls) == 1
    assert capsys.readouterr() == ("", "")


@pytest.mark.parametrize("operation", ["create", "lookup"])
async def test_cancellation_reaches_database_without_retry(operation):
    started = asyncio.Event()
    canceled = asyncio.Event()

    class BlockingDatabase:
        async def fetchrow(self, query, *args):
            started.set()
            try:
                await asyncio.Future()
            finally:
                canceled.set()

    repo = Repository(BlockingDatabase())
    task = asyncio.create_task(
        repo.create_job(URL) if operation == "create" else repo.get_job_by_id(uuid4())
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert canceled.is_set()
