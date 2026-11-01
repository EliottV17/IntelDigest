"""Opt-in PostgreSQL cases; no service or database is created or dropped."""

import asyncio
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


@pytest.mark.parametrize("status", ["pending", "processing", "completed", "failed"])
async def test_claim_preserves_unrelated_fields_and_rejects_nonpending(connection, status):
    repo = Repository(connection)
    created = await repo.create_job("https://example.com/claim")
    await connection.execute(
        "UPDATE jobs SET status = $2, error = 'previous', result = '{\"kept\":true}', "
        "updated_at = created_at - interval '1 second' WHERE id = $1",
        created.id, status,
    )
    before = await repo.get_job_by_id(created.id)
    assert await repo.try_mark_processing(created.id) is (status == "pending")
    after = await repo.get_job_by_id(created.id)
    if status == "pending":
        assert after.updated_at > before.updated_at
        assert after == before.model_copy(update={
            "status": "processing", "error": None, "updated_at": after.updated_at,
        })
    else:
        assert after == before
    assert await repo.try_mark_processing(created.id) is False
    assert await repo.get_job_by_id(created.id) == after


@pytest.mark.parametrize("status", ["pending", "processing", "completed", "failed"])
async def test_diagnostic_preserves_status_and_rejects_other_states(connection, status):
    repo = Repository(connection)
    created = await repo.create_job("https://example.com/diagnostic")
    await connection.execute(
        "UPDATE jobs SET status = $2, error = 'previous', result = '{\"kept\":true}', "
        "updated_at = created_at - interval '1 second' WHERE id = $1",
        created.id, status,
    )
    before = await repo.get_job_by_id(created.id)
    assert await repo.record_scrape_error(created.id, "🙂" * 1025) is (status == "processing")
    after = await repo.get_job_by_id(created.id)
    if status == "processing":
        assert after.updated_at > before.updated_at
        assert after == before.model_copy(update={"error": "🙂" * 1024, "updated_at": after.updated_at})
    else:
        assert after == before


async def test_updates_of_missing_job_leave_existing_job_unchanged(connection):
    repo = Repository(connection)
    created = await repo.create_job("https://example.com/unchanged")
    missing = uuid4()
    assert await repo.try_mark_processing(missing) is False
    assert await repo.record_scrape_error(missing, "safe reason") is False
    with pytest.raises(JobNotFound):
        await repo.get_job_by_id(missing)
    assert await repo.get_job_by_id(created.id) == created


async def test_two_connections_have_exactly_one_claim_winner(claim_pool):
    repo = Repository(claim_pool)
    created = await repo.create_job("https://example.com/concurrent")
    barrier = asyncio.Barrier(2)

    async def claim(connection):
        await barrier.wait()
        return await Repository(connection).try_mark_processing(created.id)

    async with (
        asyncio.timeout(5),
        claim_pool.acquire() as first,
        claim_pool.acquire() as second,
        asyncio.TaskGroup() as group,
    ):
        a = group.create_task(claim(first))
        b = group.create_task(claim(second))
    assert sorted([a.result(), b.result()]) == [False, True]
    after = await repo.get_job_by_id(created.id)
    assert after == created.model_copy(update={"status": "processing", "updated_at": after.updated_at})
    assert after.updated_at >= created.updated_at
    assert await repo.try_mark_processing(created.id) is False
    assert await repo.get_job_by_id(created.id) == after
