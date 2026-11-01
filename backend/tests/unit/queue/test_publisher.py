"""Unit tests for Redis Stream job publication, configuration, and timeouts."""

import asyncio
import json
import math
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from inteldigest.queue.message import SCHEMA_VERSION, Message
from inteldigest.queue.publisher import (
    Publisher,
    PublisherConnectionError,
    PublishError,
    QueueError,
)


class FakeRedisStreamClient:
    def __init__(
        self,
        *,
        ping_error: Exception | None = None,
        ping_delay: float = 0.0,
        xadd_error: Exception | None = None,
        xadd_delay: float = 0.0,
        close_error: Exception | None = None,
    ) -> None:
        self.ping_error = ping_error
        self.ping_delay = ping_delay
        self.xadd_error = xadd_error
        self.xadd_delay = xadd_delay
        self.close_error = close_error
        self.ping_calls = 0
        self.entries: list[tuple[str, dict[Any, Any]]] = []
        self.close_calls = 0
        self.started = asyncio.Event()

    async def ping(self) -> bool:
        self.ping_calls += 1
        self.started.set()
        if self.ping_delay > 0:
            await asyncio.sleep(self.ping_delay)
        if self.ping_error is not None:
            raise self.ping_error
        return True

    async def xadd(self, name: str, fields: dict[Any, Any]) -> str:
        self.started.set()
        if self.xadd_delay > 0:
            await asyncio.sleep(self.xadd_delay)
        if self.xadd_error is not None:
            raise self.xadd_error
        self.entries.append((name, dict(fields)))
        return "1700000000000-0"

    async def aclose(self) -> None:
        self.close_calls += 1
        if self.close_error is not None:
            raise self.close_error


async def test_publish_writes_v1_json_inside_data_field() -> None:
    client = FakeRedisStreamClient()
    publisher = Publisher(client, stream="jobs:test", publish_timeout_seconds=1.0)
    job_id = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
    message = Message(
        schema_version=SCHEMA_VERSION,
        job_id=job_id,
        url="https://example.com/article",
    )

    await publisher.publish(message)

    assert len(client.entries) == 1
    stream, fields = client.entries[0]
    assert stream == "jobs:test"
    assert list(fields.keys()) == ["data"]
    assert isinstance(fields["data"], str)
    assert fields["data"] == (
        '{"schema_version":1,'
        '"job_id":"6ba7b810-9dad-11d1-80b4-00c04fd430c8",'
        '"url":"https://example.com/article"}'
    )
    assert json.loads(fields["data"]) == {
        "schema_version": 1,
        "job_id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
        "url": "https://example.com/article",
    }


async def test_publish_rejects_invalid_messages_before_xadd() -> None:
    client = FakeRedisStreamClient()
    publisher = Publisher(client, stream="jobs:test", publish_timeout_seconds=1.0)

    with pytest.raises(TypeError, match="Message"):
        await publisher.publish({"schema_version": 1})  # type: ignore[arg-type]

    invalid_messages = [
        Message.model_construct(schema_version=2, job_id=uuid4(), url="https://example.com/a"),
        Message.model_construct(schema_version=1, job_id=UUID(int=0), url="https://example.com/a"),
        Message.model_construct(schema_version=1, job_id=uuid4(), url="not a URL"),
    ]
    for invalid in invalid_messages:
        with pytest.raises(ValidationError):
            await publisher.publish(invalid)

    assert client.entries == []


async def test_publish_wraps_redis_error_and_timeout_with_stream_and_cause() -> None:
    cause = ConnectionError("redis unavailable")
    failing_client = FakeRedisStreamClient(xadd_error=cause)
    publisher = Publisher(failing_client, stream="jobs:test", publish_timeout_seconds=1.0)
    message = Message(
        schema_version=SCHEMA_VERSION,
        job_id=uuid4(),
        url="https://example.com/article",
    )

    with pytest.raises(PublishError) as exc_info:
        await publisher.publish(message)
    assert exc_info.value.stream == "jobs:test"
    assert '"jobs:test"' in str(exc_info.value)
    assert exc_info.value.__cause__ is cause
    assert failing_client.entries == []

    slow_client = FakeRedisStreamClient(xadd_delay=0.2)
    timed_publisher = Publisher(slow_client, stream="jobs:timed", publish_timeout_seconds=0.02)
    with pytest.raises(PublishError) as timeout_info:
        await timed_publisher.publish(message)
    assert timeout_info.value.stream == "jobs:timed"
    assert '"jobs:timed"' in str(timeout_info.value)
    assert isinstance(timeout_info.value.__cause__, TimeoutError)
    assert slow_client.entries == []


async def test_publish_propagates_external_task_cancellation() -> None:
    client = FakeRedisStreamClient(xadd_delay=5.0)
    publisher = Publisher(client, stream="jobs:test", publish_timeout_seconds=10.0)
    message = Message(
        schema_version=SCHEMA_VERSION,
        job_id=uuid4(),
        url="https://example.com/article",
    )

    task = asyncio.create_task(publisher.publish(message))
    await client.started.wait()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert client.entries == []


@pytest.mark.parametrize(
    ("redis_url", "stream", "dial_timeout", "publish_timeout", "expected_fragment"),
    [
        ("", "jobs:test", 1.0, 1.0, "Redis URL"),
        ("http://:s3cr3t@localhost:6379/0", "jobs:test", 1.0, 1.0, "Redis URL"),
        ("unix:///tmp/redis.sock", "jobs:test", 1.0, 1.0, "Redis URL"),
        ("redis://:s3cr3t@/0", "jobs:test", 1.0, 1.0, "Redis URL"),
        ("redis://:s3cr3t@localhost:6379/-1", "jobs:test", 1.0, 1.0, "Redis URL"),
        ("redis://:s3cr3t@localhost:6379/notdb", "jobs:test", 1.0, 1.0, "Redis URL"),
        ("redis://localhost:6379/0", "", 1.0, 1.0, "stream"),
        ("redis://localhost:6379/0", "jobs:test", 0.0, 1.0, "dial timeout"),
        ("redis://localhost:6379/0", "jobs:test", -1.0, 1.0, "dial timeout"),
        ("redis://localhost:6379/0", "jobs:test", math.inf, 1.0, "dial timeout"),
        ("redis://localhost:6379/0", "jobs:test", math.nan, 1.0, "dial timeout"),
        ("redis://localhost:6379/0", "jobs:test", True, 1.0, "dial timeout"),
        ("redis://localhost:6379/0", "jobs:test", 1.0, 0.0, "publish timeout"),
        ("redis://localhost:6379/0", "jobs:test", 1.0, -0.5, "publish timeout"),
        ("redis://localhost:6379/0", "jobs:test", 1.0, math.nan, "publish timeout"),
        ("redis://localhost:6379/0", "jobs:test", 1.0, True, "publish timeout"),
    ],
)
async def test_connect_and_init_reject_invalid_configuration_without_leaking_secrets(
    monkeypatch: pytest.MonkeyPatch,
    redis_url: str,
    stream: str,
    dial_timeout: float,
    publish_timeout: float,
    expected_fragment: str,
) -> None:
    factory_called = False

    def forbidden_from_url(*args: Any, **kwargs: Any) -> Any:
        nonlocal factory_called
        factory_called = True
        raise AssertionError("from_url must not run on invalid configuration")

    monkeypatch.setattr("inteldigest.queue.publisher.aioredis.from_url", forbidden_from_url)

    with pytest.raises(ValueError, match=expected_fragment) as exc_info:
        await Publisher.connect(
            redis_url,
            stream,
            dial_timeout_seconds=dial_timeout,
            publish_timeout_seconds=publish_timeout,
        )

    assert not factory_called
    assert "s3cr3t" not in str(exc_info.value)
    assert exc_info.value.__cause__ is None
    assert exc_info.value.__context__ is None


def test_direct_publisher_init_validates_stream_and_timeout() -> None:
    client = FakeRedisStreamClient()
    with pytest.raises(ValueError, match="stream"):
        Publisher(client, stream="", publish_timeout_seconds=1.0)
    with pytest.raises(ValueError, match="publish timeout"):
        Publisher(client, stream="jobs:test", publish_timeout_seconds=0.0)
    with pytest.raises(ValueError, match="publish timeout"):
        Publisher(client, stream="jobs:test", publish_timeout_seconds=True)


async def test_connect_verifies_ping_and_closes_client_on_failure_or_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    healthy_client = FakeRedisStreamClient()

    def fake_from_url(url: str, **kwargs: Any) -> FakeRedisStreamClient:
        captured["url"] = url
        captured["kwargs"] = kwargs
        return healthy_client

    monkeypatch.setattr("inteldigest.queue.publisher.aioredis.from_url", fake_from_url)

    publisher = await Publisher.connect(
        "rediss://localhost:6380/2",
        "inteldigest:jobs",
        dial_timeout_seconds=1.5,
        publish_timeout_seconds=0.75,
    )
    assert captured == {
        "url": "rediss://localhost:6380/2",
        "kwargs": {"socket_connect_timeout": 1.5},
    }
    assert healthy_client.ping_calls == 1
    assert healthy_client.close_calls == 0

    await publisher.aclose()
    assert healthy_client.close_calls == 1

    ping_cause = OSError("connection refused")
    broken_client = FakeRedisStreamClient(ping_error=ping_cause)
    monkeypatch.setattr(
        "inteldigest.queue.publisher.aioredis.from_url",
        lambda url, **kwargs: broken_client,
    )
    with pytest.raises(PublisherConnectionError) as ping_exc:
        await Publisher.connect(
            "redis://:s3cr3t@localhost:6379/0",
            "inteldigest:jobs",
            dial_timeout_seconds=0.5,
            publish_timeout_seconds=1.0,
        )
    assert "s3cr3t" not in str(ping_exc.value)
    assert ping_exc.value.__cause__ is ping_cause
    assert broken_client.close_calls == 1

    stalled_client = FakeRedisStreamClient(ping_delay=0.2)
    monkeypatch.setattr(
        "inteldigest.queue.publisher.aioredis.from_url",
        lambda url, **kwargs: stalled_client,
    )
    with pytest.raises(PublisherConnectionError) as timeout_exc:
        await Publisher.connect(
            "redis://localhost:6379/0",
            "inteldigest:jobs",
            dial_timeout_seconds=0.02,
            publish_timeout_seconds=1.0,
        )
    assert isinstance(timeout_exc.value.__cause__, TimeoutError)
    assert stalled_client.close_calls == 1


async def test_connect_cancellation_and_close_failure_handling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    slow_ping_client = FakeRedisStreamClient(ping_delay=5.0)
    monkeypatch.setattr(
        "inteldigest.queue.publisher.aioredis.from_url",
        lambda url, **kwargs: slow_ping_client,
    )
    connect_task = asyncio.create_task(
        Publisher.connect(
            "redis://localhost:6379/0",
            "inteldigest:jobs",
            dial_timeout_seconds=10.0,
            publish_timeout_seconds=1.0,
        )
    )
    await slow_ping_client.started.wait()
    connect_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await connect_task
    assert slow_ping_client.close_calls == 1

    close_cause = RuntimeError("socket teardown failed")
    failing_close_client = FakeRedisStreamClient(close_error=close_cause)
    publisher = Publisher(failing_close_client, stream="jobs:test", publish_timeout_seconds=1.0)
    with pytest.raises(QueueError) as close_exc:
        await publisher.aclose()
    assert close_exc.value.__cause__ is close_cause
