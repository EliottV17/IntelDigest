"""Passive configuration loaders for the Python backend."""

from __future__ import annotations

import math
import os
import socket
import uuid
from dataclasses import dataclass
from typing import Mapping
from urllib.parse import urlsplit

from redis.connection import parse_url


@dataclass(frozen=True, slots=True)
class APIConfig:
    database_url: str
    api_port: str
    redis_url: str
    redis_stream: str
    redis_dial_timeout_seconds: float
    redis_publish_timeout_seconds: float


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    database_url: str
    redis_url: str
    redis_stream: str
    redis_dial_timeout_seconds: float
    pool_size: int
    consumer_group: str
    consumer_name: str
    stream_block_seconds: float
    queue_operation_timeout_seconds: float
    queue_read_timeout_seconds: float
    db_timeout_seconds: float
    shutdown_timeout_seconds: float
    scraper_connect_timeout_seconds: float
    scraper_tls_timeout_seconds: float
    scraper_header_timeout_seconds: float
    scraper_read_timeout_seconds: float
    scraper_request_timeout_seconds: float
    scraper_max_body_bytes: int
    scraper_max_body_read_bytes: int
    scraper_max_redirects: int


def _environment(env: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if env is None else env


def _timeout(env: Mapping[str, str], name: str, default: float, *, minimum: float = 0) -> float:
    value = env.get(name, "")
    if not value:
        return default
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        seconds = math.nan
    if not math.isfinite(seconds) or seconds < minimum or seconds <= 0:
        if minimum:
            raise ValueError(f"config: {name} must be finite and at least {minimum} seconds")
        raise ValueError(f"config: {name} must be a finite positive number of seconds")
    return seconds


def _integer(env: Mapping[str, str], name: str, default: int, minimum: int, maximum: int) -> int:
    value = env.get(name, "")
    if not value:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = minimum - 1
    if not minimum <= parsed <= maximum:
        raise ValueError(f"config: {name} must be an integer from {minimum} through {maximum}")
    return parsed


def _positive_integer(env: Mapping[str, str], name: str, default: int) -> int:
    value = env.get(name, "")
    if not value:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        parsed = 0
    if parsed <= 0:
        raise ValueError(f"config: {name} must be a positive integer")
    return parsed


def _common(env: Mapping[str, str]) -> tuple[str, str, str, float]:
    database_url = env.get("DATABASE_URL", "")
    if not database_url:
        raise ValueError("config: DATABASE_URL is required")
    redis_url = env.get("REDIS_URL", "")
    if not redis_url:
        raise ValueError("config: REDIS_URL is required")
    try:
        parsed_url = parse_url(redis_url)
        if parsed_url.get("host") is None:
            raise ValueError
        path = urlsplit(redis_url).path
        if path not in ("", "/") and int(path[1:]) < 0:
            raise ValueError
    except (TypeError, ValueError, UnicodeError):
        raise ValueError("config: invalid REDIS_URL") from None
    return database_url, redis_url, env.get("REDIS_STREAM") or "inteldigest:jobs", _timeout(
        env, "REDIS_DIAL_TIMEOUT", 5
    )


def load_api(env: Mapping[str, str] | None = None) -> APIConfig:
    """Load API/shared settings; worker-only variables are intentionally ignored."""
    values = _environment(env)
    database_url, redis_url, stream, dial = _common(values)
    return APIConfig(
        database_url=database_url,
        api_port=values.get("API_PORT") or "8080",
        redis_url=redis_url,
        redis_stream=stream,
        redis_dial_timeout_seconds=dial,
        redis_publish_timeout_seconds=_timeout(values, "REDIS_PUBLISH_TIMEOUT", 2),
    )


def load_worker(env: Mapping[str, str] | None = None) -> WorkerConfig:
    """Load shared and worker settings without requiring API-only variables."""
    values = _environment(env)
    database_url, redis_url, stream, dial = _common(values)
    group_value = values.get("WORKER_CONSUMER_GROUP")
    if group_value is None:
        group = "inteldigest:workers"
    elif not group_value.strip():
        raise ValueError("config: WORKER_CONSUMER_GROUP must be nonblank when set")
    else:
        group = group_value.strip()

    name_value = values.get("WORKER_CONSUMER_NAME")
    if name_value is None:
        try:
            hostname = socket.gethostname()
        except OSError:
            raise ValueError("config: could not determine hostname for worker consumer name") from None
        if not hostname.strip():
            raise ValueError("config: could not determine hostname for worker consumer name")
        consumer_name = f"{hostname}-{uuid.uuid4()}"
    elif not name_value.strip():
        raise ValueError("config: WORKER_CONSUMER_NAME must be nonblank when set")
    else:
        consumer_name = name_value.strip()

    pool = _integer(values, "WORKER_POOL_SIZE", 4, 1, 64)
    block = _timeout(values, "WORKER_STREAM_BLOCK", 2, minimum=0.001)
    operation = _timeout(values, "WORKER_QUEUE_OPERATION_TIMEOUT", 5)
    durations = {
        "db_timeout_seconds": _timeout(values, "WORKER_DB_TIMEOUT", 5),
        "shutdown_timeout_seconds": _timeout(values, "WORKER_SHUTDOWN_TIMEOUT", 10),
        "scraper_connect_timeout_seconds": _timeout(values, "SCRAPER_CONNECT_TIMEOUT", 5),
        "scraper_tls_timeout_seconds": _timeout(values, "SCRAPER_TLS_TIMEOUT", 5),
        "scraper_header_timeout_seconds": _timeout(values, "SCRAPER_HEADER_TIMEOUT", 10),
        "scraper_read_timeout_seconds": _timeout(values, "SCRAPER_READ_TIMEOUT", 10),
        "scraper_request_timeout_seconds": _timeout(values, "SCRAPER_REQUEST_TIMEOUT", 30),
    }
    body = _positive_integer(values, "SCRAPER_MAX_BODY_BYTES", 5_242_880)
    return WorkerConfig(
        database_url=database_url,
        redis_url=redis_url,
        redis_stream=stream,
        redis_dial_timeout_seconds=dial,
        pool_size=pool,
        consumer_group=group,
        consumer_name=consumer_name,
        stream_block_seconds=block,
        queue_operation_timeout_seconds=operation,
        queue_read_timeout_seconds=block + operation,
        **durations,
        scraper_max_body_bytes=body,
        scraper_max_body_read_bytes=body + 1,
        scraper_max_redirects=_integer(values, "SCRAPER_MAX_REDIRECTS", 5, 0, 10),
    )
