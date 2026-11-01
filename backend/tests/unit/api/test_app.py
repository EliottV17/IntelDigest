"""HTTP contracts exercised through ASGI with deterministic database doubles."""

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from inteldigest.api import app as app_module
from inteldigest.config import load_api

JOB_ID = UUID("12e45678-e89b-42d3-a456-426614174000")
TIMESTAMP = datetime(2026, 10, 31, 12, 30, 45, 123456, tzinfo=UTC)


@pytest.fixture
def dependencies(monkeypatch):
    row = {
        "id": JOB_ID, "url": "https://example.com/article", "status": "pending",
        "result": None, "error": None, "created_at": TIMESTAMP, "updated_at": TIMESTAMP,
    }
    pool = AsyncMock()
    pool.fetchrow.return_value = row
    create_pool = AsyncMock(return_value=pool)
    monkeypatch.setattr(app_module.asyncpg, "create_pool", create_pool)
    monkeypatch.setattr(app_module, "load_api", lambda: load_api({
        "DATABASE_URL": "postgresql://unused/inteldigest",
        "REDIS_URL": "redis://unused:6379/0",
        "WORKER_POOL_SIZE": "not-an-integer",
    }))
    return pool, create_pool, row


def test_factory_is_passive_and_lifespan_owns_pool(dependencies):
    pool, create_pool, _ = dependencies
    application = app_module.create_app()
    create_pool.assert_not_called()
    with TestClient(application) as client:
        create_pool.assert_awaited_once_with(
            "postgresql://unused/inteldigest", min_size=1, max_size=10,
            timeout=5, command_timeout=10,
        )
        pool.close.assert_not_awaited()
        response = client.get("/healthz")
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/json"
        assert response.json() == {"status": "ok"}
        pool.fetchrow.assert_not_awaited()
    pool.close.assert_awaited_once_with()


@pytest.mark.parametrize("method", ["GET", "HEAD"])
def test_get_job_preserves_fields_and_omits_absent_optionals(dependencies, method):
    pool, _, row = dependencies
    with TestClient(app_module.create_app()) as client:
        response = client.request(method, f"/api/v1/digests/{JOB_ID}")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    if method == "HEAD":
        assert response.content == b""
    else:
        body = response.json()
        assert body == {
            "id": str(JOB_ID), "url": row["url"], "status": "pending",
            "created_at": "2026-10-31T12:30:45.123456Z",
            "updated_at": "2026-10-31T12:30:45.123456Z",
        }
        assert datetime.fromisoformat(body["created_at"]) == TIMESTAMP
    args = pool.fetchrow.await_args.args
    assert args[1:] == (JOB_ID,)
    assert args[0].startswith("SELECT ")
    assert pool.fetchrow.await_count == 1


@pytest.mark.parametrize("status", ["pending", "processing", "completed", "failed"])
@pytest.mark.parametrize("result", [{"summary": "ñ", "optional": None}, [], False, 0, "", None])
def test_get_preserves_states_json_values_and_present_error(dependencies, status, result):
    _, _, row = dependencies
    row.update(status=status, result=None if result is None else json.dumps(result), error="")
    with TestClient(app_module.create_app()) as client:
        response = client.get(f"/api/v1/digests/{JOB_ID}")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == status
    assert body["error"] == ""
    if result is None:
        assert "result" not in body
    else:
        assert body["result"] == result


@pytest.mark.parametrize("raw_id", [str(JOB_ID).upper(), JOB_ID.hex, "{" + str(JOB_ID) + "}"])
def test_uuid_inputs_are_serialized_canonically(dependencies, raw_id):
    pool, _, _ = dependencies
    with TestClient(app_module.create_app()) as client:
        response = client.get(f"/api/v1/digests/{raw_id}")
    assert response.status_code == 200
    assert response.json()["id"] == str(JOB_ID)
    assert pool.fetchrow.await_args.args[1] == JOB_ID


@pytest.mark.parametrize("raw_id", ["not-a-uuid", "1234", "g" * 32])
def test_invalid_id_is_400_without_database_access(dependencies, raw_id):
    pool, _, _ = dependencies
    with TestClient(app_module.create_app()) as client:
        response = client.get(f"/api/v1/digests/{raw_id}")
    assert response.status_code == 400
    assert response.json() == {"error": "invalid job ID"}
    pool.fetchrow.assert_not_awaited()


def test_missing_job_is_404(dependencies):
    pool, _, _ = dependencies
    pool.fetchrow.return_value = None
    with TestClient(app_module.create_app()) as client:
        response = client.get(f"/api/v1/digests/{JOB_ID}")
    assert response.status_code == 404
    assert response.json() == {"error": "job not found"}
    assert pool.fetchrow.await_count == 1


def test_database_failure_is_safe_500_and_health_is_not_readiness(dependencies, caplog):
    pool, _, _ = dependencies
    pool.fetchrow.side_effect = RuntimeError("password=private article contents")
    with TestClient(app_module.create_app()) as client:
        response = client.get(f"/api/v1/digests/{JOB_ID}")
        health = client.head("/healthz")
    assert response.status_code == 500
    assert response.json() == {"error": "internal error"}
    assert "password" not in caplog.text
    assert "article contents" not in caplog.text
    assert health.status_code == 200
    assert health.content == b""
    assert pool.fetchrow.await_count == 1
    pool.close.assert_awaited_once_with()


def test_startup_failure_prevents_serving(dependencies):
    pool, create_pool, _ = dependencies
    create_pool.side_effect = OSError("database unavailable")
    with (
        pytest.raises(OSError, match="database unavailable"),
        TestClient(app_module.create_app()),
    ):
        pytest.fail("startup must fail before serving")
    pool.close.assert_not_awaited()


def test_lifespan_closes_pool_on_application_error(dependencies):
    pool, _, _ = dependencies
    with (
        pytest.raises(RuntimeError, match="application failure"),
        TestClient(app_module.create_app()),
    ):
        raise RuntimeError("application failure")
    pool.close.assert_awaited_once_with()


def test_post_is_not_available_in_m06(dependencies):
    pool, _, _ = dependencies
    with TestClient(app_module.create_app()) as client:
        response = client.post("/api/v1/digests", json={"url": "https://example.com"})
    assert response.status_code == 404
    pool.fetchrow.assert_not_awaited()
