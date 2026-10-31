"""Passive configuration loaders compatible with the Go backend contract.

Durations are exposed as integer nanoseconds (``*_ns``), matching Go's
``time.Duration`` exactly and preserving sub-microsecond values.
"""

from __future__ import annotations

import os
import re
import socket
import uuid
from dataclasses import dataclass
from typing import Mapping
from urllib.parse import unquote

_MAX_INT64 = (1 << 63) - 1
_MIN_INT64 = -(1 << 63)
_DURATION_UNITS = {
    "ns": 1, "us": 1_000, "µs": 1_000, "μs": 1_000,
    "ms": 1_000_000, "s": 1_000_000_000,
    "m": 60_000_000_000, "h": 3_600_000_000_000,
}
_DURATION_PART = re.compile(r"(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(ns|us|µs|μs|ms|s|m|h)")
_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:")


@dataclass(frozen=True, slots=True)
class APIConfig:
    database_url: str
    api_port: str
    redis_url: str
    redis_stream: str
    redis_dial_timeout_ns: int
    redis_publish_timeout_ns: int


@dataclass(frozen=True, slots=True)
class WorkerConfig:
    database_url: str
    redis_url: str
    redis_stream: str
    redis_dial_timeout_ns: int
    pool_size: int
    consumer_group: str
    consumer_name: str
    stream_block_ns: int
    queue_operation_timeout_ns: int
    queue_read_timeout_ns: int
    db_timeout_ns: int
    shutdown_timeout_ns: int
    scraper_connect_timeout_ns: int
    scraper_tls_timeout_ns: int
    scraper_header_timeout_ns: int
    scraper_read_timeout_ns: int
    scraper_request_timeout_ns: int
    scraper_max_body_bytes: int
    scraper_max_body_read_bytes: int
    scraper_max_redirects: int


def _environment(env: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if env is None else env


def _duration(value: str) -> int:
    if not value:
        raise ValueError
    sign = 1
    if value[0] in "+-":
        if value[0] == "-":
            sign = -1
        value = value[1:]
    if not value:
        raise ValueError
    if value == "0":
        return 0
    total = 0
    while value:
        match = _DURATION_PART.match(value)
        if match is None:
            raise ValueError
        token = match.group(0)
        unit = match.group(1)
        number = token[: -len(unit)]
        whole, dot, fraction = number.partition(".")
        component = int(whole or "0") * _DURATION_UNITS[unit]
        if fraction:
            component += int(fraction) * _DURATION_UNITS[unit] // (10 ** len(fraction))
        total += component
        if total > _MAX_INT64:
            raise ValueError
        value = value[len(token):]
    total *= sign
    if not _MIN_INT64 <= total <= _MAX_INT64:
        raise ValueError
    return total


def _duration_setting(env: Mapping[str, str], name: str, default_ns: int, *, minimum_ns: int | None = None) -> int:
    value = env.get(name, "")
    if not value:
        return default_ns
    try:
        parsed = _duration(value)
    except (ValueError, OverflowError):
        parsed = 0
    if minimum_ns is not None:
        if parsed < minimum_ns:
            raise ValueError(f"config: {name} must be at least 1ms")
    elif parsed <= 0:
        raise ValueError(f"config: {name} must be a positive duration")
    return parsed


def _integer(env: Mapping[str, str], name: str, default: int, minimum: int, maximum: int) -> int:
    value = env.get(name, "")
    if not value:
        return default
    try:
        if not re.fullmatch(r"[+-]?[0-9]+", value):
            raise ValueError
        parsed = int(value, 10)
    except ValueError:
        parsed = minimum - 1
    if not minimum <= parsed <= maximum:
        raise ValueError(f"config: {name} must be an integer from {minimum} through {maximum}")
    return parsed


def _positive_int64(env: Mapping[str, str], name: str, default: int) -> int:
    value = env.get(name, "")
    if not value:
        return default
    try:
        if not re.fullmatch(r"[+-]?[0-9]+", value):
            raise ValueError
        parsed = int(value, 10)
        if parsed < _MIN_INT64 or parsed > _MAX_INT64:
            raise ValueError
    except ValueError:
        parsed = 0
    if parsed <= 0:
        raise ValueError(f"config: {name} must be a positive integer")
    return parsed


class _RedisDatabasePathError(ValueError):
    pass


def _validate_redis_url(raw: str) -> None:
    """Mirror the URL syntax checks relevant to Go's net/url and config."""
    try:
        if re.search(r"%(?![0-9A-Fa-f]{2})", raw):
            raise ValueError
        scheme_match = _SCHEME.match(raw)
        if scheme_match is None:
            raise ValueError
        scheme = raw[:scheme_match.end() - 1].lower()
        remainder = raw[scheme_match.end():]
        if scheme not in ("redis", "rediss") or not remainder.startswith("//"):
            raise ValueError

        # Go accepts query spaces, unlike spaces in the scheme/authority.
        before_fragment, separator, _fragment = remainder.partition("#")
        before_query, query_separator, _query = before_fragment.partition("?")
        if "\x00" in raw or any(ord(char) < 0x20 or ord(char) == 0x7f for char in raw):
            raise ValueError
        if query_separator and any(ord(char) < 0x20 or ord(char) == 0x7f for char in _query):
            raise ValueError

        authority_path = before_query[2:]
        authority = authority_path.split("/", 1)[0]
        if not authority or any(char.isspace() for char in authority):
            raise ValueError
        hostport = authority.rsplit("@", 1)[-1]
        if hostport.startswith("["):
            end = hostport.find("]")
            if end < 0:
                raise ValueError
            hostname = hostport[1:end]
            suffix = hostport[end + 1:]
            # net/url accepts bracketed literals without DNS/IP policy, but
            # rejects an empty first address component such as "[:6379]".
            if not hostname or (hostname.startswith(":") and not hostname.startswith("::")):
                raise ValueError
            if "]" in suffix or (suffix and not suffix.startswith(":")):
                raise ValueError
            port = suffix[1:] if suffix else ""
            has_port_separator = bool(suffix)
        else:
            if hostport.count(":") > 1:
                raise ValueError
            hostname, port_separator, port = hostport.partition(":")
            has_port_separator = bool(port_separator)
        if not hostname or any(char.isspace() for char in hostname):
            raise ValueError
        if has_port_separator and port and not re.fullmatch(r"[0-9]+", port):
            raise ValueError

        path = ""
        tail = authority_path[len(authority):]
        if tail.startswith("/"):
            path = unquote(tail.split("#", 1)[0])
        if path not in ("", "/"):
            if not path.startswith("/"):
                raise _RedisDatabasePathError("database")
            db = path[1:]
            if not re.fullmatch(r"[+-]?[0-9]+", db):
                raise _RedisDatabasePathError("database")
            parsed_db = int(db, 10)
            if parsed_db < 0 or parsed_db > _MAX_INT64:
                raise _RedisDatabasePathError("database")
    except _RedisDatabasePathError:
        raise
    except (ValueError, UnicodeError):
        raise ValueError("expected redis:// or rediss:// URL with a host") from None


def _common(env: Mapping[str, str]) -> tuple[str, str, str, int]:
    database_url = env.get("DATABASE_URL", "")
    if not database_url:
        raise ValueError("config: DATABASE_URL is required")
    redis_url = env.get("REDIS_URL", "")
    if not redis_url:
        raise ValueError("config: REDIS_URL is required")
    try:
        _validate_redis_url(redis_url)
    except ValueError as error:
        reason = str(error)
        if reason == "database":
            raise ValueError("config: invalid REDIS_URL: database path must be a non-negative integer") from None
        raise ValueError("config: invalid REDIS_URL: expected redis:// or rediss:// URL with a host") from None
    return database_url, redis_url, env.get("REDIS_STREAM") or "inteldigest:jobs", _duration_setting(env, "REDIS_DIAL_TIMEOUT", 5_000_000_000)


def load_api(env: Mapping[str, str] | None = None) -> APIConfig:
    """Load API/shared settings; worker-only variables are intentionally ignored."""
    values = _environment(env)
    database_url, redis_url, stream, dial = _common(values)
    port = values.get("API_PORT") or "8080"
    publish = _duration_setting(values, "REDIS_PUBLISH_TIMEOUT", 2_000_000_000)
    return APIConfig(database_url, port, redis_url, stream, dial, publish)


def load_worker(env: Mapping[str, str] | None = None) -> WorkerConfig:
    """Load shared and worker settings without requiring API-only variables."""
    values = _environment(env)
    database_url, redis_url, stream, dial = _common(values)
    pool = _integer(values, "WORKER_POOL_SIZE", 4, 1, 64)
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
    block = _duration_setting(values, "WORKER_STREAM_BLOCK", 2_000_000_000, minimum_ns=1_000_000)
    operation = _duration_setting(values, "WORKER_QUEUE_OPERATION_TIMEOUT", 5_000_000_000)
    if block > _MAX_INT64 - operation:
        raise ValueError("config: WORKER_STREAM_BLOCK plus WORKER_QUEUE_OPERATION_TIMEOUT overflows")
    read_timeout = block + operation
    duration_names = (
        ("WORKER_DB_TIMEOUT", 5_000_000_000), ("WORKER_SHUTDOWN_TIMEOUT", 10_000_000_000),
        ("SCRAPER_CONNECT_TIMEOUT", 5_000_000_000), ("SCRAPER_TLS_TIMEOUT", 5_000_000_000),
        ("SCRAPER_HEADER_TIMEOUT", 10_000_000_000), ("SCRAPER_READ_TIMEOUT", 10_000_000_000),
        ("SCRAPER_REQUEST_TIMEOUT", 30_000_000_000),
    )
    durations = [_duration_setting(values, name, default) for name, default in duration_names]
    body = _positive_int64(values, "SCRAPER_MAX_BODY_BYTES", 5_242_880)
    if body >= (1 << 63) - 1:
        raise ValueError("config: SCRAPER_MAX_BODY_BYTES must allow a safe max+1 read and int conversion")
    redirects = _integer(values, "SCRAPER_MAX_REDIRECTS", 5, 0, 10)
    return WorkerConfig(
        database_url, redis_url, stream, dial, pool, group, consumer_name, block, operation, read_timeout,
        *durations, body, body + 1, redirects,
    )
