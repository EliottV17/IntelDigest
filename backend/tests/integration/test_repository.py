"""Opt-in PostgreSQL cases; no service or database is created or dropped."""

from uuid import uuid4

import pytest

from inteldigest.jobs.repository import JobNotFound, Repository


async def test_create_lookup_and_missing_job(connection):
    repo = Repository(connection)
    created = await repo.create_job("https://example.com/article")
    assert created.id.version == 4
    assert created.url == "https://example.com/article"
    assert created.status == "pending"
    assert created.result is None and created.error is None
    assert created.created_at == created.updated_at
    assert created.created_at.utcoffset().total_seconds() == 0
    assert await repo.get_job_by_id(created.id) == created
    with pytest.raises(JobNotFound, match="^job not found$"):
        await repo.get_job_by_id(uuid4())
    assert await repo.get_job_by_id(created.id) == created


async def test_existing_jsonb_diagnostic_and_timestamps(connection):
    repo = Repository(connection)
    created = await repo.create_job("https://example.com/existing")
    await connection.execute(
        "UPDATE jobs SET status = 'failed', result = $2::jsonb, error = $3, "
        "updated_at = created_at + interval '1 second' WHERE id = $1",
        created.id, '{"title":"Artículo","points":[1,true,null]}', "safe diagnosis",
    )
    job = await repo.get_job_by_id(created.id)
    assert job.id == created.id and job.url == created.url
    assert job.status == "failed" and job.error == "safe diagnosis"
    assert job.result == {"title": "Artículo", "points": [1, True, None]}
    assert job.created_at == created.created_at
    assert (job.updated_at - job.created_at).total_seconds() == 1
    assert job.model_dump(mode="json")["result"] == job.result
