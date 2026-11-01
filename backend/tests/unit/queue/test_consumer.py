"""Unit tests for bounded Redis Stream consumer, group management, and acknowledgements."""

import asyncio
import math
from typing import Any
from unittest.mock import patch

import pytest
from redis.exceptions import ResponseError

from inteldigest.queue.consumer import (
    Consumer,
    ConsumerConnectionError,
    ConsumerGroupMissingError,
    Delivery,
    is_group_missing,
)
from inteldigest.queue.publisher import QueueError


class FakeRedisConsumerClient:
    def __init__(
        self,
        *,
        ping_error: Exception | None = None,
        ping_delay: float = 0.0,
        xgroup_create_error: Exception | None = None,
        xgroup_create_delay: float = 0.0,
        xreadgroup_responses: list[Any] | None = None,
        xreadgroup_error: Exception | None = None,
        xreadgroup_delay: float = 0.0,
        xack_error: Exception | None = None,
        xack_delay: float = 0.0,
        aclose_error: Exception | None = None,
    ) -> None:
        self.ping_error = ping_error
        self.ping_delay = ping_delay
        self.xgroup_create_error = xgroup_create_error
        self.xgroup_create_delay = xgroup_create_delay
        self.xreadgroup_responses = list(xreadgroup_responses) if xreadgroup_responses else []
        self.xreadgroup_error = xreadgroup_error
        self.xreadgroup_delay = xreadgroup_delay
        self.xack_error = xack_error
        self.xack_delay = xack_delay
        self.aclose_error = aclose_error

        self.ping_calls = 0
        self.xgroup_create_calls: list[dict[str, Any]] = []
        self.xreadgroup_calls: list[dict[str, Any]] = []
        self.xack_calls: list[dict[str, Any]] = []
        self.aclose_calls = 0
        self.deleted_entries: list[Any] = []

    async def ping(self) -> bool:
        self.ping_calls += 1
        if self.ping_delay > 0:
            await asyncio.sleep(self.ping_delay)
        if self.ping_error is not None:
            raise self.ping_error
        return True

    async def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "0",
        mkstream: bool = False,
    ) -> bool:
        self.xgroup_create_calls.append(
            {"name": name, "groupname": groupname, "id": id, "mkstream": mkstream}
        )
        if self.xgroup_create_delay > 0:
            await asyncio.sleep(self.xgroup_create_delay)
        if self.xgroup_create_error is not None:
            raise self.xgroup_create_error
        return True

    async def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: Any,
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
    ) -> Any:
        self.xreadgroup_calls.append(
            {
                "groupname": groupname,
                "consumername": consumername,
                "streams": streams,
                "count": count,
                "block": block,
                "noack": noack,
            }
        )
        if self.xreadgroup_delay > 0:
            await asyncio.sleep(self.xreadgroup_delay)
        if self.xreadgroup_error is not None:
            raise self.xreadgroup_error
        if self.xreadgroup_responses:
            return self.xreadgroup_responses.pop(0)
        return []

    async def xack(self, name: str, groupname: str, *ids: Any) -> int:
        self.xack_calls.append({"name": name, "groupname": groupname, "ids": list(ids)})
        if self.xack_delay > 0:
            await asyncio.sleep(self.xack_delay)
        if self.xack_error is not None:
            raise self.xack_error
        return len(ids)

    async def xdel(self, name: str, *ids: Any) -> int:
        self.deleted_entries.extend(ids)
        return len(ids)

    async def delete(self, *names: Any) -> int:
        self.deleted_entries.extend(names)
        return len(names)

    async def aclose(self) -> None:
        self.aclose_calls += 1
        if self.aclose_error is not None:
            raise self.aclose_error


def _make_consumer(
    client: FakeRedisConsumerClient | None = None,
    *,
    stream: str = "jobs:test",
    group: str = "workers",
    consumer_name: str = "worker-1",
    stream_block_seconds: float = 1.25,
    operation_timeout_seconds: float = 1.0,
) -> Consumer:
    fake_client = client if client is not None else FakeRedisConsumerClient()
    return Consumer(
        fake_client,
        stream=stream,
        group=group,
        consumer_name=consumer_name,
        stream_block_seconds=stream_block_seconds,
        operation_timeout_seconds=operation_timeout_seconds,
    )


# ---------------------------------------------------------------------------
# Configuration and connect validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "invalid_url",
    [
        "",
        "   ",
        "http://localhost:6379",
        "https://localhost:6379",
        "postgres://localhost:5432/db",
        "redis://",
        "rediss://",
        "redis://:6379",
        "redis://localhost:6379/-1",
        "redis://localhost:6379/abc",
        "redis://user:pass@localhost:6379/bad-db",
        "redis://user:supersecret@localhost:6379/not-a-db",
        "redis://local host:6379/0",
        "redis://localhost:6379/0\n",
    ],
)
async def test_consumer_connect_validates_redis_url_without_credential_leak(
    invalid_url: str,
) -> None:
    with pytest.raises(ValueError) as exc_info:
        await Consumer.connect(
            redis_url=invalid_url,
            stream="jobs:test",
            group="workers",
            consumer_name="worker-1",
        )

    exc = exc_info.value
    assert str(exc) == "queue: invalid Redis URL"
    assert exc.__cause__ is None
    assert exc.__context__ is None
    assert "supersecret" not in str(exc)


@pytest.mark.parametrize(
    ("stream", "group", "consumer_name", "block", "operation", "match"),
    [
        ("", "workers", "worker-1", 1.0, 1.0, "stream is required"),
        ("jobs", "", "worker-1", 1.0, 1.0, "consumer group is required and must be nonblank"),
        ("jobs", "   ", "worker-1", 1.0, 1.0, "consumer group is required and must be nonblank"),
        ("jobs", "workers", "", 1.0, 1.0, "consumer name is required and must be nonblank"),
        ("jobs", "workers", "   ", 1.0, 1.0, "consumer name is required and must be nonblank"),
        ("jobs", "workers", "w1", 0.0, 1.0, "stream block timeout"),
        ("jobs", "workers", "w1", 0.0005, 1.0, "stream block timeout"),
        ("jobs", "workers", "w1", -1.0, 1.0, "stream block timeout"),
        ("jobs", "workers", "w1", math.nan, 1.0, "stream block timeout"),
        ("jobs", "workers", "w1", math.inf, 1.0, "stream block timeout"),
        ("jobs", "workers", "w1", True, 1.0, "stream block timeout"),  # bool rejected
        ("jobs", "workers", "w1", 1.0, 0.0, "operation timeout"),
        ("jobs", "workers", "w1", 1.0, -1.0, "operation timeout"),
        ("jobs", "workers", "w1", 1.0, math.nan, "operation timeout"),
        ("jobs", "workers", "w1", 1.0, math.inf, "operation timeout"),
        ("jobs", "workers", "w1", 1.0, True, "operation timeout"),  # bool rejected
    ],
)
def test_consumer_init_validates_parameters(
    stream: str,
    group: str,
    consumer_name: str,
    block: Any,
    operation: Any,
    match: str,
) -> None:
    client = FakeRedisConsumerClient()
    with pytest.raises(ValueError, match=match):
        Consumer(
            client,
            stream=stream,
            group=group,
            consumer_name=consumer_name,
            stream_block_seconds=block,
            operation_timeout_seconds=operation,
        )


def test_consumer_normalizes_whitespace_and_calculates_block_ms() -> None:
    client = FakeRedisConsumerClient()
    consumer = Consumer(
        client,
        stream="jobs:test",
        group="  workers  ",
        consumer_name="  worker-1  ",
        stream_block_seconds=1.25,
        operation_timeout_seconds=2.0,
    )
    assert consumer.stream == "jobs:test"
    assert consumer.group == "workers"
    assert consumer.consumer_name == "worker-1"
    assert consumer.block_ms == 1250
    assert consumer.stream_block_seconds == 1.25
    assert consumer.operation_timeout_seconds == 2.0
    assert consumer.read_timeout_seconds == 3.25


def test_consumer_block_minimum_1ms_and_integer_ms() -> None:
    client = FakeRedisConsumerClient()
    c1 = Consumer(
        client,
        stream="jobs",
        group="grp",
        consumer_name="w",
        stream_block_seconds=0.001,
        operation_timeout_seconds=1.0,
    )
    assert c1.block_ms == 1

    c2 = Consumer(
        client,
        stream="jobs",
        group="grp",
        consumer_name="w",
        stream_block_seconds=0.0015,
        operation_timeout_seconds=1.0,
    )
    assert c2.block_ms >= 1


async def test_consumer_connect_success_with_ping() -> None:
    fake_client = FakeRedisConsumerClient()
    with patch("inteldigest.queue.consumer.aioredis.from_url", return_value=fake_client) as mock_from_url:
        consumer = await Consumer.connect(
            "redis://localhost:6379/0",
            stream="jobs:test",
            group="workers",
            consumer_name="worker-1",
            dial_timeout_seconds=0.5,
            stream_block_seconds=2.0,
            operation_timeout_seconds=1.0,
        )
        assert mock_from_url.call_count == 1
        assert fake_client.ping_calls == 1
        assert consumer.stream == "jobs:test"
        assert consumer.group == "workers"
        assert consumer.consumer_name == "worker-1"
        assert consumer.block_ms == 2000
        assert consumer.read_timeout_seconds == 3.0


async def test_consumer_connect_ping_failure_closes_client_and_raises() -> None:
    fake_client = FakeRedisConsumerClient(ping_error=ConnectionError("dial failure"))
    with patch("inteldigest.queue.consumer.aioredis.from_url", return_value=fake_client):
        with pytest.raises(ConsumerConnectionError, match="failed to connect to Redis") as exc_info:
            await Consumer.connect(
                "redis://localhost:6379/0",
                stream="jobs:test",
                group="workers",
                consumer_name="worker-1",
                dial_timeout_seconds=0.5,
            )
        assert fake_client.aclose_calls == 1
        assert isinstance(exc_info.value.__cause__, ConnectionError)


async def test_consumer_connect_ping_timeout_closes_client_and_raises() -> None:
    fake_client = FakeRedisConsumerClient(ping_delay=0.2)
    with patch("inteldigest.queue.consumer.aioredis.from_url", return_value=fake_client):
        with pytest.raises(ConsumerConnectionError, match="failed to connect to Redis") as exc_info:
            await Consumer.connect(
                "redis://localhost:6379/0",
                stream="jobs:test",
                group="workers",
                consumer_name="worker-1",
                dial_timeout_seconds=0.01,
            )
        assert fake_client.aclose_calls == 1
        assert isinstance(exc_info.value.__cause__, TimeoutError)


async def test_consumer_connect_cancellation_closes_client_and_propagates() -> None:
    fake_client = FakeRedisConsumerClient(ping_delay=1.0)
    with patch("inteldigest.queue.consumer.aioredis.from_url", return_value=fake_client):
        task = asyncio.create_task(
            Consumer.connect(
                "redis://localhost:6379/0",
                stream="jobs:test",
                group="workers",
                consumer_name="worker-1",
                dial_timeout_seconds=5.0,
            )
        )
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert fake_client.aclose_calls == 1


# ---------------------------------------------------------------------------
# ensure_group
# ---------------------------------------------------------------------------


async def test_ensure_group_uses_exact_arguments_and_mkstream() -> None:
    client = FakeRedisConsumerClient()
    consumer = _make_consumer(client)

    await consumer.ensure_group()

    assert len(client.xgroup_create_calls) == 1
    call = client.xgroup_create_calls[0]
    assert call["name"] == "jobs:test"
    assert call["groupname"] == "workers"
    assert call["id"] == "0"
    assert call["mkstream"] is True


async def test_ensure_group_accepts_exact_busygroup_idempotently() -> None:
    busy_err = ResponseError("BUSYGROUP Consumer Group name already exists")
    client = FakeRedisConsumerClient(xgroup_create_error=busy_err)
    consumer = _make_consumer(client)

    # Must return cleanly (None), not raise
    result = await consumer.ensure_group()
    assert result is None


@pytest.mark.parametrize(
    "error",
    [
        ResponseError("WRONGTYPE Operation against a key holding the wrong kind of value"),
        ResponseError("NOPERM this user has no permissions"),
        ResponseError("ERR syntax error"),
        ResponseError("BUSYGROUP unexpected failure"),
        ResponseError("BUSYGROUPX Consumer Group name already exists"),
        ResponseError("prefix BUSYGROUP suffix"),  # token not at start
        ConnectionError("network split"),
        RuntimeError("driver error"),
    ],
)
async def test_ensure_group_rejects_non_busygroup_errors(error: Exception) -> None:
    client = FakeRedisConsumerClient(xgroup_create_error=error)
    consumer = _make_consumer(client)

    with pytest.raises(QueueError, match='ensure consumer group "workers" on stream "jobs:test"') as exc_info:
        await consumer.ensure_group()

    assert exc_info.value.__cause__ is error


async def test_ensure_group_times_out_with_queue_error() -> None:
    client = FakeRedisConsumerClient(xgroup_create_delay=0.2)
    consumer = _make_consumer(client, operation_timeout_seconds=0.01)

    with pytest.raises(QueueError) as exc_info:
        await consumer.ensure_group()

    assert isinstance(exc_info.value.__cause__, TimeoutError)


async def test_ensure_group_cancellation_propagates() -> None:
    client = FakeRedisConsumerClient(xgroup_create_delay=1.0)
    consumer = _make_consumer(client, operation_timeout_seconds=5.0)

    task = asyncio.create_task(consumer.ensure_group())
    await asyncio.sleep(0.01)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


# ---------------------------------------------------------------------------
# read
# ---------------------------------------------------------------------------


async def test_read_uses_exact_arguments() -> None:
    client = FakeRedisConsumerClient()
    consumer = _make_consumer(client, stream="jobs:custom", group="workers", consumer_name="worker-a")

    await consumer.read()

    assert len(client.xreadgroup_calls) == 1
    call = client.xreadgroup_calls[0]
    assert call["groupname"] == "workers"
    assert call["consumername"] == "worker-a"
    assert call["streams"] == {"jobs:custom": ">"}
    assert call["count"] == 1
    assert call["block"] == consumer.block_ms
    assert call["noack"] is False


async def test_read_returns_delivery_with_bytes_normalized_to_str() -> None:
    raw_response = [
        (
            b"jobs:test",
            [
                (
                    b"1712345678901-2",
                    {b"data": b'{"schema_version":1,"job_id":"abc","url":"https://example.com"}', b"extra": b"test"},
                )
            ],
        )
    ]
    client = FakeRedisConsumerClient(xreadgroup_responses=[raw_response])
    consumer = _make_consumer(client)

    delivery = await consumer.read()

    assert delivery is not None
    assert isinstance(delivery, Delivery)
    assert delivery.entry_id == "1712345678901-2"
    assert delivery.fields == {
        "data": '{"schema_version":1,"job_id":"abc","url":"https://example.com"}',
        "extra": "test",
    }


async def test_read_preserves_strings_and_other_types_from_test_doubles() -> None:
    raw_response = [
        [
            "jobs:test",
            [
                (
                    "1712345678901-3",
                    {"data": "not-json", "other": 7},
                )
            ],
        ]
    ]
    client = FakeRedisConsumerClient(xreadgroup_responses=[raw_response])
    consumer = _make_consumer(client)

    delivery = await consumer.read()

    assert delivery is not None
    assert delivery.entry_id == "1712345678901-3"
    assert delivery.fields["data"] == "not-json"
    assert delivery.fields["other"] == 7


async def test_read_non_utf8_bytes_does_not_crash_reader() -> None:
    # Malformed byte sequence \xff\xfe should not raise UnicodeDecodeError in Consumer.read()
    raw_response = [
        (
            b"jobs:test",
            [
                (
                    b"1712345678901-4",
                    {b"data": b"\xff\xfe\xfa\xfb"},
                )
            ],
        )
    ]
    client = FakeRedisConsumerClient(xreadgroup_responses=[raw_response])
    consumer = _make_consumer(client)

    delivery = await consumer.read()

    assert delivery is not None
    assert delivery.entry_id == "1712345678901-4"
    assert isinstance(delivery.fields["data"], str)
    assert "\ufffd" in delivery.fields["data"]


@pytest.mark.parametrize(
    "empty_response",
    [
        [],
        None,
        [("jobs:test", [])],
        [[b"jobs:test", []]],
    ],
)
async def test_read_normal_empty_responses_return_none(empty_response: Any) -> None:
    client = FakeRedisConsumerClient(xreadgroup_responses=[empty_response])
    consumer = _make_consumer(client)

    delivery = await consumer.read()
    assert delivery is None


async def test_read_nogroup_raises_consumer_group_missing_error() -> None:
    nogroup_err = ResponseError(
        "NOGROUP No such key 'jobs:test' or consumer group 'workers' in XREADGROUP with GROUP option"
    )
    client = FakeRedisConsumerClient(xreadgroup_error=nogroup_err)
    consumer = _make_consumer(client, stream="jobs:test", group="workers")

    with pytest.raises(ConsumerGroupMissingError) as exc_info:
        await consumer.read()

    err = exc_info.value
    assert err.stream == "jobs:test"
    assert err.group == "workers"
    assert err.__cause__ is nogroup_err
    assert isinstance(err, QueueError)
    assert is_group_missing(err) is True
    wrapped_twice = RuntimeError("pool failure")
    outer = RuntimeError("worker exit")
    wrapped_twice.__cause__ = err
    outer.__cause__ = wrapped_twice
    assert is_group_missing(outer) is True
    assert 'consumer group "workers" missing on stream "jobs:test"' in str(err)


@pytest.mark.parametrize(
    "malformed_response",
    [
        "corrupted",
        [123],
        [("jobs:test", [("1700000000000-0", "not-a-mapping")])],
    ],
)
async def test_read_malformed_protocol_response_raises_queue_error(
    malformed_response: Any,
) -> None:
    client = FakeRedisConsumerClient(xreadgroup_responses=[malformed_response])
    consumer = _make_consumer(client)

    with pytest.raises(QueueError, match="malformed response"):
        await consumer.read()


async def test_read_other_errors_raise_queue_error_with_cause() -> None:
    non_nogroup_prefix = ResponseError("NOGROUPX custom error")
    prefix_consumer = _make_consumer(FakeRedisConsumerClient(xreadgroup_error=non_nogroup_prefix))
    with pytest.raises(QueueError) as prefix_exc:
        await prefix_consumer.read()
    assert not isinstance(prefix_exc.value, ConsumerGroupMissingError)

    driver_err = ConnectionResetError("connection reset by peer")
    client = FakeRedisConsumerClient(xreadgroup_error=driver_err)
    consumer = _make_consumer(client)

    with pytest.raises(QueueError, match='read stream "jobs:test" with group "workers" failed') as exc_info:
        await consumer.read()

    assert exc_info.value.__cause__ is driver_err
    assert not is_group_missing(exc_info.value)


async def test_read_timeout_raises_queue_error_with_timeout_cause() -> None:
    # Operation budget expires during hanging read
    client = FakeRedisConsumerClient(xreadgroup_delay=0.2)
    # Total read timeout = 0.01 + 0.01 = 0.02s
    consumer = _make_consumer(
        client,
        stream_block_seconds=0.01,
        operation_timeout_seconds=0.01,
    )

    with pytest.raises(QueueError) as exc_info:
        await consumer.read()

    assert isinstance(exc_info.value.__cause__, TimeoutError)


async def test_read_cancellation_propagates_unmasked() -> None:
    client = FakeRedisConsumerClient(xreadgroup_delay=1.0)
    consumer = _make_consumer(client, stream_block_seconds=1.0, operation_timeout_seconds=1.0)

    task = asyncio.create_task(consumer.read())
    await asyncio.sleep(0.01)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


# ---------------------------------------------------------------------------
# ack
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "invalid_entry_id",
    [
        "",
        "   ",
        None,
        12345,
        ["1700000000000-0"],
    ],
)
async def test_ack_rejects_blank_or_non_string_entry_id_before_redis(
    invalid_entry_id: Any,
) -> None:
    client = FakeRedisConsumerClient()
    consumer = _make_consumer(client)

    with pytest.raises(ValueError, match="stream entry ID is required and must be nonblank"):
        await consumer.ack(invalid_entry_id)  # type: ignore[arg-type]

    assert len(client.xack_calls) == 0


async def test_ack_calls_xack_and_never_deletes_stream_entries() -> None:
    client = FakeRedisConsumerClient()
    consumer = _make_consumer(client, stream="jobs:test", group="workers")

    await consumer.ack("1712345678901-2")

    assert len(client.xack_calls) == 1
    call = client.xack_calls[0]
    assert call["name"] == "jobs:test"
    assert call["groupname"] == "workers"
    assert call["ids"] == ["1712345678901-2"]

    # Invariant: ACK never deletes stream entries
    assert len(client.deleted_entries) == 0


async def test_ack_wraps_errors_in_queue_error_with_cause() -> None:
    driver_err = ConnectionError("xack failed")
    client = FakeRedisConsumerClient(xack_error=driver_err)
    consumer = _make_consumer(client)

    with pytest.raises(QueueError, match='acknowledge stream entry "1712345678901-2"') as exc_info:
        await consumer.ack("1712345678901-2")

    assert exc_info.value.__cause__ is driver_err


async def test_ack_times_out_with_queue_error() -> None:
    client = FakeRedisConsumerClient(xack_delay=0.2)
    consumer = _make_consumer(client, operation_timeout_seconds=0.01)

    with pytest.raises(QueueError) as exc_info:
        await consumer.ack("1712345678901-2")

    assert isinstance(exc_info.value.__cause__, TimeoutError)


async def test_ack_cancellation_propagates_unmasked() -> None:
    client = FakeRedisConsumerClient(xack_delay=1.0)
    consumer = _make_consumer(client, operation_timeout_seconds=5.0)

    task = asyncio.create_task(consumer.ack("1712345678901-2"))
    await asyncio.sleep(0.01)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


# ---------------------------------------------------------------------------
# aclose lifecycle
# ---------------------------------------------------------------------------


async def test_aclose_calls_client_aclose_and_is_idempotent() -> None:
    client = FakeRedisConsumerClient()
    consumer = _make_consumer(client)

    await consumer.aclose()
    assert client.aclose_calls == 1

    # Calling again is a no-op
    await consumer.aclose()
    assert client.aclose_calls == 1


async def test_aclose_wraps_driver_errors_in_queue_error() -> None:
    driver_err = RuntimeError("close failed")
    client = FakeRedisConsumerClient(aclose_error=driver_err)
    consumer = _make_consumer(client)

    with pytest.raises(QueueError, match="failed to close Redis client") as exc_info:
        await consumer.aclose()

    assert exc_info.value.__cause__ is driver_err


async def test_aclose_cancellation_propagates() -> None:
    # Test double that raises CancelledError
    async def cancel_aclose() -> None:
        raise asyncio.CancelledError()

    client = FakeRedisConsumerClient()
    client.aclose = cancel_aclose  # type: ignore[method-assign]
    consumer = _make_consumer(client)

    with pytest.raises(asyncio.CancelledError):
        await consumer.aclose()
