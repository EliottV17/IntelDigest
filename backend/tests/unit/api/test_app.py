"""HTTP contracts exercised through ASGI with deterministic database doubles."""

import json
from datetime import UTC, datetime
from typing import Any, NamedTuple
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from inteldigest.api import app as app_module
from inteldigest.config import load_api

JOB_ID = UUID("12e45678-e89b-42d3-a456-426614174000")
TIMESTAMP = datetime(2026, 10, 31, 12, 30, 45, 123456, tzinfo=UTC)


class AppTestContext(NamedTuple):
    pool: AsyncMock
    create_pool: AsyncMock
    publisher: AsyncMock
    connect_publisher: AsyncMock
    row: dict[str, Any]


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

    publisher = AsyncMock()
    connect_publisher = AsyncMock(return_value=publisher)
    monkeypatch.setattr(app_module.Publisher, "connect", connect_publisher)

    monkeypatch.setattr(app_module, "load_api", lambda: load_api({
        "DATABASE_URL": "postgresql://unused/inteldigest",
        "REDIS_URL": "redis://unused:6379/0",
        "WORKER_POOL_SIZE": "not-an-integer",
    }))
    return AppTestContext(pool, create_pool, publisher, connect_publisher, row)


def test_factory_is_passive_and_lifespan_owns_pool_and_publisher(dependencies):
    deps = dependencies
    application = app_module.create_app()
    deps.create_pool.assert_not_called()
    deps.connect_publisher.assert_not_called()
    with TestClient(application) as client:
        deps.create_pool.assert_awaited_once_with(
            "postgresql://unused/inteldigest", min_size=1, max_size=10,
            timeout=5, command_timeout=10,
        )
        deps.connect_publisher.assert_awaited_once_with(
            "redis://unused:6379/0", "inteldigest:jobs",
            dial_timeout_seconds=5.0, publish_timeout_seconds=2.0,
        )
        deps.pool.close.assert_not_awaited()
        deps.publisher.aclose.assert_not_awaited()
        response = client.get("/healthz")
        assert response.status_code == 200
        assert response.headers["content-type"] == "application/json"
        assert response.json() == {"status": "ok"}
        deps.pool.fetchrow.assert_not_awaited()
    deps.publisher.aclose.assert_awaited_once_with()
    deps.pool.close.assert_awaited_once_with()


@pytest.mark.parametrize("method", ["GET", "HEAD"])
def test_get_job_preserves_fields_and_omits_absent_optionals(dependencies, method):
    deps = dependencies
    with TestClient(app_module.create_app()) as client:
        response = client.request(method, f"/api/v1/digests/{JOB_ID}")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/json"
    if method == "HEAD":
        assert response.content == b""
    else:
        body = response.json()
        assert body == {
            "id": str(JOB_ID), "url": deps.row["url"], "status": "pending",
            "created_at": "2026-10-31T12:30:45.123456Z",
            "updated_at": "2026-10-31T12:30:45.123456Z",
        }
        assert datetime.fromisoformat(body["created_at"]) == TIMESTAMP
    args = deps.pool.fetchrow.await_args.args
    assert args[1:] == (JOB_ID,)
    assert args[0].startswith("SELECT ")
    assert deps.pool.fetchrow.await_count == 1


@pytest.mark.parametrize("status", ["pending", "processing", "completed", "failed"])
@pytest.mark.parametrize("result", [{"summary": "ñ", "optional": None}, [], False, 0, "", None])
def test_get_preserves_states_json_values_and_present_error(dependencies, status, result):
    deps = dependencies
    deps.row.update(status=status, result=None if result is None else json.dumps(result), error="")
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
    deps = dependencies
    with TestClient(app_module.create_app()) as client:
        response = client.get(f"/api/v1/digests/{raw_id}")
    assert response.status_code == 200
    assert response.json()["id"] == str(JOB_ID)
    assert deps.pool.fetchrow.await_args.args[1] == JOB_ID


@pytest.mark.parametrize("raw_id", ["not-a-uuid", "1234", "g" * 32])
def test_invalid_id_is_400_without_database_access(dependencies, raw_id):
    deps = dependencies
    with TestClient(app_module.create_app()) as client:
        response = client.get(f"/api/v1/digests/{raw_id}")
    assert response.status_code == 400
    assert response.json() == {"error": "invalid job ID"}
    deps.pool.fetchrow.assert_not_awaited()


def test_missing_job_is_404(dependencies):
    deps = dependencies
    deps.pool.fetchrow.return_value = None
    with TestClient(app_module.create_app()) as client:
        response = client.get(f"/api/v1/digests/{JOB_ID}")
    assert response.status_code == 404
    assert response.json() == {"error": "job not found"}
    assert deps.pool.fetchrow.await_count == 1


def test_database_failure_is_safe_500_and_health_is_not_readiness(dependencies, caplog):
    deps = dependencies
    deps.pool.fetchrow.side_effect = RuntimeError("password=private article contents")
    with TestClient(app_module.create_app()) as client:
        response = client.get(f"/api/v1/digests/{JOB_ID}")
        health = client.head("/healthz")
    assert response.status_code == 500
    assert response.json() == {"error": "internal error"}
    assert "password" not in caplog.text
    assert "article contents" not in caplog.text
    assert health.status_code == 200
    assert health.content == b""
    assert deps.pool.fetchrow.await_count == 1
    deps.pool.close.assert_awaited_once_with()


def test_startup_failure_prevents_serving(dependencies):
    deps = dependencies
    deps.create_pool.side_effect = OSError("database unavailable")
    with (
        pytest.raises(OSError, match="database unavailable"),
        TestClient(app_module.create_app()),
    ):
        pytest.fail("startup must fail before serving")
    deps.pool.close.assert_not_awaited()


def test_redis_connect_failure_closes_pool_and_prevents_serving(dependencies):
    deps = dependencies
    deps.connect_publisher.side_effect = ConnectionError("redis unreachable")
    with (
        pytest.raises(ConnectionError, match="redis unreachable"),
        TestClient(app_module.create_app()),
    ):
        pytest.fail("startup must fail before serving")
    deps.create_pool.assert_awaited_once()
    deps.pool.close.assert_awaited_once()


def test_publisher_aclose_failure_still_closes_pool(dependencies):
    deps = dependencies
    deps.publisher.aclose.side_effect = RuntimeError("failed to close redis")
    with (
        pytest.raises(RuntimeError, match="failed to close redis"),
        TestClient(app_module.create_app()),
    ):
        pass
    deps.publisher.aclose.assert_awaited_once()
    deps.pool.close.assert_awaited_once()


def test_lifespan_closes_pool_on_application_error(dependencies):
    deps = dependencies
    with (
        pytest.raises(RuntimeError, match="application failure"),
        TestClient(app_module.create_app()),
    ):
        raise RuntimeError("application failure")
    deps.publisher.aclose.assert_awaited_once_with()
    deps.pool.close.assert_awaited_once_with()


def test_post_digest_valid_url(dependencies):
    deps = dependencies
    created_id = UUID("99e45678-e89b-42d3-a456-426614174999")
    deps.row["id"] = created_id
    deps.row["url"] = "https://example.com/article"

    with TestClient(app_module.create_app()) as client:
        response = client.post("/api/v1/digests", json={"url": "https://example.com/article"})

    assert response.status_code == 202
    assert response.headers["content-type"] == "application/json"
    assert response.json() == {"job_id": str(created_id), "message": "processing"}

    deps.pool.fetchrow.assert_awaited_once()
    args = deps.pool.fetchrow.await_args.args
    assert args[0].startswith("INSERT INTO jobs")
    assert args[2] == "https://example.com/article"

    deps.publisher.publish.assert_awaited_once()
    published_msg = deps.publisher.publish.await_args.args[0]
    assert published_msg.schema_version == 1
    assert published_msg.job_id == created_id
    assert published_msg.url == "https://example.com/article"


def test_post_digest_persists_before_publishing(dependencies):
    deps = dependencies
    order = []

    async def fake_fetchrow(query, *args):
        order.append("db")
        return deps.row

    async def fake_publish(message):
        order.append("publish")

    deps.pool.fetchrow.side_effect = fake_fetchrow
    deps.publisher.publish.side_effect = fake_publish

    with TestClient(app_module.create_app()) as client:
        response = client.post("/api/v1/digests", json={"url": "https://example.com/article"})

    assert response.status_code == 202
    assert order == ["db", "publish"]


def test_post_digest_ignores_extra_json_fields(dependencies):
    deps = dependencies
    with TestClient(app_module.create_app()) as client:
        response = client.post(
            "/api/v1/digests",
            json={"url": "https://example.com/article", "extra": 123, "meta": "test"},
        )
    assert response.status_code == 202
    deps.pool.fetchrow.assert_awaited_once()
    deps.publisher.publish.assert_awaited_once()


def test_post_digest_publish_failure_returns_503_and_preserves_job(dependencies):
    deps = dependencies
    created_id = UUID("77e45678-e89b-42d3-a456-426614174777")
    deps.row["id"] = created_id
    deps.row["url"] = "https://example.com/article"
    deps.publisher.publish.side_effect = RuntimeError("Redis connection lost password=secret")

    with TestClient(app_module.create_app()) as client:
        response = client.post("/api/v1/digests", json={"url": "https://example.com/article"})
        assert response.status_code == 503
        assert response.headers["content-type"] == "application/json"
        assert response.json() == {
            "error": "queue unavailable",
            "job_id": str(created_id),
        }

        get_resp = client.get(f"/api/v1/digests/{created_id}")
        assert get_resp.status_code == 200
        assert get_resp.json()["id"] == str(created_id)


def test_post_digest_create_job_failure_returns_500_and_never_publishes(dependencies, caplog):
    deps = dependencies
    deps.pool.fetchrow.side_effect = RuntimeError("database DSN postgresql://secret@host query syntax error")

    with TestClient(app_module.create_app()) as client:
        response = client.post("/api/v1/digests", json={"url": "https://example.com/article"})

    assert response.status_code == 500
    assert response.json() == {"error": "internal error"}
    deps.publisher.publish.assert_not_called()
    assert "secret" not in caplog.text
    assert "postgresql://" not in caplog.text


@pytest.mark.parametrize(
    ("payload", "content_type"),
    [
        (b"", "application/json"),
        (b"not json", "application/json"),
        (b"[]", "application/json"),
        (b'"string"', "application/json"),
        (b"123", "application/json"),
        (b"null", "application/json"),
        (b"true", "application/json"),
        (b'{"url": 123}', "application/json"),
        (b'{"url": ["a"]}', "application/json"),
        (b'{"url": true}', "application/json"),
        (b'{"url": {"nested": "value"}}', "application/json"),
    ],
)
def test_post_digest_malformed_or_non_object_json_returns_400(dependencies, payload, content_type):
    deps = dependencies
    with TestClient(app_module.create_app()) as client:
        response = client.post("/api/v1/digests", content=payload, headers={"Content-Type": content_type})
    assert response.status_code == 400
    assert response.json() == {"error": "invalid JSON body"}
    deps.pool.fetchrow.assert_not_awaited()
    deps.publisher.publish.assert_not_awaited()


@pytest.mark.parametrize(
    ("body", "expected_error"),
    [
        ({}, "url: empty"),
        ({"url": None}, "url: empty"),
        ({"url": ""}, "url: empty"),
        ({"url": "   "}, "url: empty"),
        ({"url": "\t\n"}, "url: empty"),
        ({"url": "ftp://example.com"}, 'url: scheme must be http or https, got "ftp"'),
        ({"url": "not-a-url"}, 'url: scheme must be http or https, got ""'),
        ({"url": "https://"}, "url: host is empty"),
        ({"url": "http://:8080/x"}, "url: host is empty"),
        ({"url": "http://localhost/x"}, 'url: private/reserved address "localhost"'),
        ({"url": "http://localhost./x"}, 'url: private/reserved address "localhost."'),
        ({"url": "http://LOCALHOST/x"}, 'url: private/reserved address "localhost"'),
        ({"url": "http://127.0.0.1/x"}, 'url: private/reserved address "127.0.0.1"'),
        ({"url": "http://10.0.0.1/x"}, 'url: private/reserved address "10.0.0.1"'),
        ({"url": "http://172.16.0.1/x"}, 'url: private/reserved address "172.16.0.1"'),
        ({"url": "http://192.168.1.1/x"}, 'url: private/reserved address "192.168.1.1"'),
        ({"url": "http://169.254.169.254/latest"}, 'url: private/reserved address "169.254.169.254"'),
        ({"url": "http://0.0.0.0/x"}, 'url: private/reserved address "0.0.0.0"'),
        ({"url": "http://[::1]/x"}, 'url: private/reserved address "::1"'),
        ({"url": "http://[::ffff:127.0.0.1]/x"}, 'url: private/reserved address "::ffff:127.0.0.1"'),
        ({"url": "https://example.com/foo bar"}, "url: invalid URL syntax"),
        ({"url": "http://[::1/x"}, "url: invalid URL syntax"),
        ({"url": "http://example.com:99999/x"}, "url: invalid URL syntax"),
    ],
)
def test_post_digest_invalid_url_returns_400(dependencies, body, expected_error):
    deps = dependencies
    with TestClient(app_module.create_app()) as client:
        response = client.post("/api/v1/digests", json=body)
    assert response.status_code == 400
    resp_json = response.json()
    if expected_error.startswith("url: invalid"):
        assert resp_json["error"].startswith("url: invalid")
    else:
        assert resp_json == {"error": expected_error}
    deps.pool.fetchrow.assert_not_awaited()
    deps.publisher.publish.assert_not_awaited()
