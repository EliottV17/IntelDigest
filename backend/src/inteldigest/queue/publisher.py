"""Bounded Redis Stream job publication using redis.asyncio."""

import asyncio
import math
from collections.abc import Awaitable
from contextlib import suppress
from typing import Any, Protocol, Self
from urllib.parse import urlsplit

import redis.asyncio as aioredis
from redis.connection import parse_url

from inteldigest.queue.message import Message


class RedisStreamClient(Protocol):
    """Minimal async Redis surface needed to verify connectivity and append stream entries."""

    def ping(self) -> Awaitable[Any]: ...
    def xadd(self, name: str, fields: dict[Any, Any]) -> Awaitable[Any]: ...
    def aclose(self) -> Awaitable[Any]: ...


class QueueError(RuntimeError):
    """Base operational error for Redis queue transport."""


class PublisherConnectionError(QueueError):
    """Raised when initial Redis connectivity verification fails."""


class PublishError(QueueError):
    """Raised when appending a job message to the configured Redis stream fails."""

    def __init__(self, stream: str) -> None:
        self.stream = stream
        super().__init__(f'queue: publish to stream "{stream}" failed')


def _validate_redis_url(redis_url: str) -> None:
    valid = (
        isinstance(redis_url, str)
        and bool(redis_url)
        and not any(ch.isspace() or ord(ch) < 0x20 or ord(ch) == 0x7F for ch in redis_url)
    )
    if valid:
        try:
            parts = urlsplit(redis_url)
            parsed_url = parse_url(redis_url) if parts.scheme in {"redis", "rediss"} else {}
            db_ok = True
            if parts.path not in ("", "/"):
                db_segment = parts.path[1:]
                db_ok = db_segment.isascii() and db_segment.isdigit()
            valid = parsed_url.get("host") is not None and db_ok
        except (TypeError, ValueError, UnicodeError):
            valid = False
    if not valid:
        raise ValueError("queue: invalid Redis URL")


def _validate_stream(stream: str) -> str:
    if not isinstance(stream, str) or not stream:
        raise ValueError("queue: Redis stream is required")
    return stream


def _validate_timeout(seconds: float, name: str) -> float:
    if (
        isinstance(seconds, bool)
        or not isinstance(seconds, (int, float))
        or not math.isfinite(seconds)
        or seconds <= 0
    ):
        raise ValueError(f"queue: {name} timeout must be a finite positive number of seconds")
    return float(seconds)


class Publisher:
    """Append validated v1 job messages to a configured Redis Stream."""

    def __init__(
        self,
        client: RedisStreamClient,
        stream: str,
        publish_timeout_seconds: float,
    ) -> None:
        self._client = client
        self._stream = _validate_stream(stream)
        self._publish_timeout_seconds = _validate_timeout(publish_timeout_seconds, "publish")

    @classmethod
    async def connect(
        cls,
        redis_url: str,
        stream: str,
        dial_timeout_seconds: float,
        publish_timeout_seconds: float,
    ) -> Self:
        """Validate settings, create an async Redis client, and confirm connectivity."""
        _validate_redis_url(redis_url)
        validated_stream = _validate_stream(stream)
        dial_timeout = _validate_timeout(dial_timeout_seconds, "dial")
        publish_timeout = _validate_timeout(publish_timeout_seconds, "publish")

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
            raise PublisherConnectionError("queue: failed to connect to Redis") from exc

        return cls(
            client,
            stream=validated_stream,
            publish_timeout_seconds=publish_timeout,
        )

    async def publish(self, message: Message) -> None:
        """Validate and append one v1 job message to the configured stream."""
        if not isinstance(message, Message):
            raise TypeError("queue: message must be a Message instance")
        validated = Message.model_validate(
            {
                "schema_version": message.schema_version,
                "job_id": message.job_id,
                "url": message.url,
            }
        )
        payload = validated.model_dump_json()

        try:
            async with asyncio.timeout(self._publish_timeout_seconds):
                await self._client.xadd(self._stream, {"data": payload})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise PublishError(self._stream) from exc

    async def aclose(self) -> None:
        """Release the underlying Redis client."""
        try:
            await self._client.aclose()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise QueueError("queue: failed to close Redis client") from exc
