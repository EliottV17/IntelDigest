"""Bounded Redis Stream consumer, group management, and acknowledgements."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Protocol, Self

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from inteldigest.queue.publisher import (
    QueueError,
    _validate_redis_url,
    _validate_stream,
    _validate_timeout,
)


class RedisConsumerClient(Protocol):
    """Minimal async Redis surface needed to manage consumer groups, read entries, and acknowledge."""

    def ping(self) -> Awaitable[Any]: ...
    def xgroup_create(
        self,
        name: str,
        groupname: str,
        id: str = "0",
        mkstream: bool = False,
    ) -> Awaitable[Any]: ...
    def xreadgroup(
        self,
        groupname: str,
        consumername: str,
        streams: Any,
        count: int | None = None,
        block: int | None = None,
        noack: bool = False,
    ) -> Awaitable[Any]: ...
    def xack(self, name: str, groupname: str, *ids: Any) -> Awaitable[Any]: ...
    def aclose(self) -> Awaitable[Any]: ...


class ConsumerConnectionError(QueueError):
    """Raised when initial Redis connectivity verification fails during consumer startup."""


class ConsumerGroupMissingError(QueueError):
    """Raised when XREADGROUP fails because the consumer group is missing (Redis NOGROUP)."""

    def __init__(self, stream: str, group: str) -> None:
        self.stream = stream
        self.group = group
        super().__init__(f'queue: consumer group "{group}" missing on stream "{stream}"')


_BUSYGROUP_MESSAGE = "BUSYGROUP Consumer Group name already exists"


def is_group_missing(err: BaseException) -> bool:
    """Report whether err or any chained __cause__ represents a missing consumer group."""
    current: BaseException | None = err
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        if isinstance(current, ConsumerGroupMissingError):
            return True
        seen.add(id(current))
        current = current.__cause__
    return False


@dataclass(frozen=True, slots=True)
class Delivery:
    """Redis Stream delivery identity and uninterpreted field values.

    Payload decoding and validation belong to the worker processor.
    """

    entry_id: str
    fields: Mapping[str, Any]


def _validate_name(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"queue: {name} is required and must be nonblank")
    return value.strip()


def _validate_block(seconds: float) -> float:
    if (
        isinstance(seconds, bool)
        or not isinstance(seconds, (int, float))
        or not math.isfinite(seconds)
        or seconds < 0.001
    ):
        raise ValueError(
            "queue: stream block timeout must be a finite positive number of seconds and at least 0.001"
        )
    return float(seconds)


class Consumer:
    """Owns a Redis client and provides bounded group creation, new entry reading, and ACK.

    It never claims, replays, or deletes stream entries.
    """

    def __init__(
        self,
        client: RedisConsumerClient,
        stream: str,
        group: str,
        consumer_name: str,
        *,
        stream_block_seconds: float,
        operation_timeout_seconds: float,
    ) -> None:
        self._client = client
        self._stream = _validate_stream(stream)
        self._group = _validate_name(group, "consumer group")
        self._consumer_name = _validate_name(consumer_name, "consumer name")
        self._stream_block_seconds = _validate_block(stream_block_seconds)
        self._operation_timeout_seconds = _validate_timeout(operation_timeout_seconds, "operation")
        self._block_ms = max(1, round(self._stream_block_seconds * 1000))
        self._read_timeout_seconds = self._stream_block_seconds + self._operation_timeout_seconds
        self._closed = False

    @property
    def stream(self) -> str:
        return self._stream

    @property
    def group(self) -> str:
        return self._group

    @property
    def consumer_name(self) -> str:
        return self._consumer_name

    @property
    def block_ms(self) -> int:
        return self._block_ms

    @property
    def stream_block_seconds(self) -> float:
        return self._stream_block_seconds

    @property
    def operation_timeout_seconds(self) -> float:
        return self._operation_timeout_seconds

    @property
    def read_timeout_seconds(self) -> float:
        return self._read_timeout_seconds

    @classmethod
    async def connect(
        cls,
        redis_url: str,
        stream: str,
        group: str,
        consumer_name: str,
        dial_timeout_seconds: float = 5.0,
        stream_block_seconds: float = 2.0,
        operation_timeout_seconds: float = 5.0,
    ) -> Self:
        """Validate settings, connect to Redis, verify PING, and return a Consumer."""
        _validate_redis_url(redis_url)
        validated_stream = _validate_stream(stream)
        validated_group = _validate_name(group, "consumer group")
        validated_name = _validate_name(consumer_name, "consumer name")
        dial_timeout = _validate_timeout(dial_timeout_seconds, "dial")
        block_timeout = _validate_block(stream_block_seconds)
        operation_timeout = _validate_timeout(operation_timeout_seconds, "operation")

        client = aioredis.from_url(redis_url, socket_connect_timeout=dial_timeout)
        try:
            async with asyncio.timeout(dial_timeout):
                await client.ping()
        except asyncio.CancelledError:
            with suppress(Exception):
                await client.aclose()
            raise
        except Exception as exc:
            with suppress(Exception):
                await client.aclose()
            raise ConsumerConnectionError("queue: failed to connect to Redis") from exc

        return cls(
            client,
            stream=validated_stream,
            group=validated_group,
            consumer_name=validated_name,
            stream_block_seconds=block_timeout,
            operation_timeout_seconds=operation_timeout,
        )

    async def ensure_group(self) -> None:
        """Create the consumer group at the start of the stream if absent (idempotent)."""
        try:
            async with asyncio.timeout(self._operation_timeout_seconds):
                await self._client.xgroup_create(
                    self._stream,
                    self._group,
                    id="0",
                    mkstream=True,
                )
        except asyncio.CancelledError:
            raise
        except ResponseError as exc:
            if str(exc) == _BUSYGROUP_MESSAGE:
                return
            raise QueueError(
                f'queue: ensure consumer group "{self._group}" on stream "{self._stream}" failed'
            ) from exc
        except Exception as exc:
            raise QueueError(
                f'queue: ensure consumer group "{self._group}" on stream "{self._stream}" failed'
            ) from exc

    async def read(self) -> Delivery | None:
        """Read at most one new entry from the stream for this consumer group.

        Returns None on empty block expiration. Raises ConsumerGroupMissingError
        on Redis NOGROUP response.
        """
        try:
            async with asyncio.timeout(self._read_timeout_seconds):
                response = await self._client.xreadgroup(
                    groupname=self._group,
                    consumername=self._consumer_name,
                    streams={self._stream: ">"},
                    count=1,
                    block=self._block_ms,
                    noack=False,
                )
        except asyncio.CancelledError:
            raise
        except ResponseError as exc:
            message = str(exc)
            if message == "NOGROUP" or message.startswith("NOGROUP "):
                raise ConsumerGroupMissingError(self._stream, self._group) from exc
            raise QueueError(
                f'queue: read stream "{self._stream}" with group "{self._group}" failed'
            ) from exc
        except Exception as exc:
            raise QueueError(
                f'queue: read stream "{self._stream}" with group "{self._group}" failed'
            ) from exc

        if not response:
            return None

        try:
            if isinstance(response, (str, bytes)):
                raise TypeError("unexpected scalar xreadgroup response")
            stream_entries = response[0]
            if isinstance(stream_entries, (str, bytes)) or len(stream_entries) != 2:
                raise TypeError("unexpected stream tuple in xreadgroup response")
            messages = stream_entries[1]
            if not messages:
                return None
            first_message = messages[0]
            if isinstance(first_message, (str, bytes)) or len(first_message) != 2:
                raise TypeError("unexpected message tuple in xreadgroup response")
            raw_id, raw_fields = first_message[0], first_message[1]
            if not isinstance(raw_fields, Mapping):
                raise TypeError("unexpected entry fields in xreadgroup response")
        except (IndexError, TypeError, KeyError) as exc:
            raise QueueError(
                f'queue: malformed response reading stream "{self._stream}" with group "{self._group}"'
            ) from exc

        if isinstance(raw_id, bytes):
            entry_id = raw_id.decode("utf-8", errors="replace")
        else:
            entry_id = str(raw_id)

        normalized_fields: dict[str, Any] = {}
        for k, v in raw_fields.items():
            norm_k = k.decode("utf-8", errors="replace") if isinstance(k, bytes) else str(k)
            norm_v = v.decode("utf-8", errors="replace") if isinstance(v, bytes) else v
            normalized_fields[norm_k] = norm_v

        return Delivery(entry_id=entry_id, fields=normalized_fields)

    async def ack(self, entry_id: str) -> None:
        """Acknowledge exactly the given stream entry ID without deleting it."""
        if not isinstance(entry_id, str) or not entry_id.strip():
            raise ValueError("queue: stream entry ID is required and must be nonblank")

        try:
            async with asyncio.timeout(self._operation_timeout_seconds):
                await self._client.xack(self._stream, self._group, entry_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise QueueError(
                f'queue: acknowledge stream entry "{entry_id}" on stream "{self._stream}" with group "{self._group}" failed'
            ) from exc

    async def aclose(self) -> None:
        """Release the underlying Redis client."""
        if self._closed:
            return
        self._closed = True
        try:
            await self._client.aclose()
        except asyncio.CancelledError:
            self._closed = False
            raise
        except Exception as exc:
            raise QueueError("queue: failed to close Redis client") from exc
