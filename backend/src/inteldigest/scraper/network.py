"""Coupled DNS resolution, public IP pinning, safe TLS, and aiohttp network transport."""

import asyncio
import ipaddress
import math
import socket
import ssl
import time
from collections.abc import Callable, Sequence
from typing import Any, Protocol

import aiohttp
import aiohttp.connector

from inteldigest.scraper.models import ScrapeError, ScrapeErrorKind, ScrapeStage
from inteldigest.scraper.policy import (
    ERR_SECURITY_REJECTED_MESSAGE,
    SecurityRejectedError,
    is_metadata_hostname,
    is_public_address,
    valid_dial_host,
    valid_dial_port,
    valid_dns_name,
)

DEFAULT_CONNECT_TIMEOUT_SECONDS: float = 5.0
DEFAULT_TLS_TIMEOUT_SECONDS: float = 5.0

_ALLOWED_NETWORKS: frozenset[str] = frozenset({"tcp", "tcp4", "tcp6"})


class AddressResolver(Protocol):
    """Protocol for resolving hostnames to IP addresses."""

    async def resolve(
        self, host: str, port: int = 0
    ) -> Sequence[ipaddress.IPv4Address | ipaddress.IPv6Address | str | Any]: ...


class StreamDialer(Protocol):
    """Protocol for establishing transport connections."""

    async def dial(
        self,
        host: str,
        port: int,
        *,
        ssl: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter] | Any: ...


def unmap_ip_address(
    addr: ipaddress.IPv4Address | ipaddress.IPv6Address,
) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
    """Return IPv4 address if addr is an IPv4-mapped IPv6 address; otherwise addr."""
    if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped is not None:
        return addr.ipv4_mapped
    return addr


def split_dial_address(address: str) -> tuple[str, str]:
    """Parse address into (host, port) strings.

    Supports bracketed IPv6 (e.g. "[::1]:80") and unbracketed host:port forms.
    Raises ValueError on syntax or port errors.
    """
    if not address or "@" in address or "/" in address:
        raise ValueError(f"invalid dial address: {address}")

    if address.startswith("["):
        closing = address.find("]")
        if closing == -1:
            raise ValueError(f"unclosed bracket in address: {address}")
        host = address[1:closing]
        rest = address[closing + 1 :]
        if not rest.startswith(":"):
            raise ValueError(f"missing port separator after bracket: {address}")
        port = rest[1:]
        if not host or not port:
            raise ValueError(f"empty host or port in address: {address}")
        return host, port

    if "]" in address or ":" not in address:
        raise ValueError(f"missing port or stray bracket in address: {address}")

    host, _sep, port = address.rpartition(":")
    if not host or not port:
        raise ValueError(f"empty host or port in address: {address}")
    return host, port


def _reject_security(message: str = "network connection failed") -> ScrapeError:
    return ScrapeError(
        kind=ScrapeErrorKind.SECURITY_REJECTED,
        stage=ScrapeStage.FETCH,
        message=message,
        cause=SecurityRejectedError(ERR_SECURITY_REJECTED_MESSAGE),
    )


def classify_network_error(
    exc: BaseException,
    message: str = "network connection failed",
) -> ScrapeError:
    """Classify an exception into a safe ScrapeError without leaking URLs or secrets."""
    if isinstance(exc, ScrapeError):
        return exc
    if isinstance(exc, SecurityRejectedError):
        return ScrapeError(
            ScrapeErrorKind.SECURITY_REJECTED,
            ScrapeStage.FETCH,
            message,
            cause=exc,
        )
    if isinstance(exc, asyncio.CancelledError):
        return ScrapeError(
            ScrapeErrorKind.CANCELED,
            ScrapeStage.FETCH,
            message,
            cause=exc,
        )
    if isinstance(exc, (TimeoutError, asyncio.TimeoutError)):
        return ScrapeError(
            ScrapeErrorKind.TIMEOUT,
            ScrapeStage.FETCH,
            message,
            cause=exc,
        )
    return ScrapeError(
        ScrapeErrorKind.NETWORK,
        ScrapeStage.FETCH,
        message,
        cause=exc,
    )


def _parse_candidate_address(
    item: Any,
) -> ipaddress.IPv4Address | ipaddress.IPv6Address | None:
    if isinstance(item, (ipaddress.IPv4Address, ipaddress.IPv6Address)):
        return item
    if isinstance(item, str):
        try:
            return ipaddress.ip_address(item)
        except ValueError:
            return None
    if isinstance(item, dict):
        raw = item.get("host") or item.get("ip") or item.get("hostname")
        if isinstance(raw, str):
            try:
                return ipaddress.ip_address(raw)
            except ValueError:
                return None
    return None


async def default_address_resolver(
    host: str, port: int = 0
) -> list[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    """Resolve host using standard asyncio loop getaddrinfo."""
    loop = asyncio.get_running_loop()
    addr_info = await loop.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    addrs: list[ipaddress.IPv4Address | ipaddress.IPv6Address] = []
    for entry in addr_info:
        sockaddr = entry[4]
        ip_str = sockaddr[0]
        try:
            addrs.append(ipaddress.ip_address(ip_str))
        except ValueError:
            continue
    return addrs


async def default_stream_dialer(
    host: str,
    port: int,
    *,
    ssl: ssl.SSLContext | None = None,
    server_hostname: str | None = None,
) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    """Open connection using asyncio.open_connection."""
    effective_server_hostname = server_hostname if ssl is not None else None
    return await asyncio.open_connection(
        host, port, ssl=ssl, server_hostname=effective_server_hostname
    )


async def _invoke_resolver(
    resolver: AddressResolver | Callable[..., Any],
    host: str,
    port: int,
) -> Sequence[Any]:
    if hasattr(resolver, "resolve") and callable(resolver.resolve):
        try:
            return await resolver.resolve(host, port)
        except TypeError:
            return await resolver.resolve(host)
    try:
        return await resolver(host, port)
    except TypeError:
        return await resolver(host)


async def _invoke_dialer(
    dialer: StreamDialer | Callable[..., Any],
    host: str,
    port: int,
    *,
    ssl: ssl.SSLContext | None = None,
    server_hostname: str | None = None,
) -> Any:
    if hasattr(dialer, "dial") and callable(dialer.dial):
        target = dialer.dial
    else:
        target = dialer
    try:
        return await target(host, port, ssl=ssl, server_hostname=server_hostname)
    except TypeError:
        try:
            return await target(host, port)
        except TypeError:
            return await target(f"{host}:{port}")


def _is_ip_literal(host: str) -> bool:
    try:
        ipaddress.ip_address(host)
        return True
    except ValueError:
        return False


async def resolve_and_validate_address(
    host: str,
    port: int | str,
    *,
    resolver: AddressResolver | Callable[..., Any] | None = None,
    timeout_seconds: float = DEFAULT_CONNECT_TIMEOUT_SECONDS,
) -> tuple[str, int]:
    """Validate destination against policy and resolve host to a vetted public IP literal.

    Returns (dial_ip_literal, dial_port_int).
    Raises ScrapeError with appropriate classification on policy rejection,
    timeout, cancellation, or network error.
    """
    if not valid_dial_port(port):
        raise _reject_security()
    port_int = int(port)

    if not valid_dial_host(host) or is_metadata_hostname(host):
        raise _reject_security()

    try:
        ip_literal = ipaddress.ip_address(host)
    except ValueError:
        ip_literal = None

    if ip_literal is not None:
        if not is_public_address(ip_literal):
            raise _reject_security()
        unmapped = unmap_ip_address(ip_literal)
        return str(unmapped), port_int

    if not valid_dns_name(host):
        raise _reject_security()

    res = resolver or default_address_resolver
    try:
        async with asyncio.timeout(timeout_seconds):
            raw_answers = await _invoke_resolver(res, host, port_int)
    except TimeoutError as exc:
        raise ScrapeError(
            ScrapeErrorKind.TIMEOUT,
            ScrapeStage.FETCH,
            "network connection failed",
            cause=exc,
        ) from exc
    except asyncio.CancelledError as exc:
        raise ScrapeError(
            ScrapeErrorKind.CANCELED,
            ScrapeStage.FETCH,
            "network connection failed",
            cause=exc,
        ) from exc
    except ScrapeError:
        raise
    except Exception as exc:
        raise ScrapeError(
            ScrapeErrorKind.NETWORK,
            ScrapeStage.FETCH,
            "network connection failed",
            cause=exc,
        ) from exc

    if not raw_answers:
        raise _reject_security()

    approved: list[ipaddress.IPv4Address | ipaddress.IPv6Address] = []
    for item in raw_answers:
        parsed = _parse_candidate_address(item)
        if parsed is None or not is_public_address(parsed):
            raise _reject_security()
        approved.append(unmap_ip_address(parsed))

    return str(approved[0]), port_int


class SafeDialer:
    """Safe connection dialer enforcing coupled DNS resolution and public IP pinning."""

    def __init__(
        self,
        connect_timeout_seconds: float = DEFAULT_CONNECT_TIMEOUT_SECONDS,
        *,
        resolver: AddressResolver | Callable[..., Any] | None = None,
        dialer: StreamDialer | Callable[..., Any] | None = None,
    ) -> None:
        if (
            not isinstance(connect_timeout_seconds, (int, float))
            or isinstance(connect_timeout_seconds, bool)
            or not math.isfinite(connect_timeout_seconds)
            or connect_timeout_seconds <= 0
        ):
            connect_timeout_seconds = DEFAULT_CONNECT_TIMEOUT_SECONDS
        self._connect_timeout_seconds = float(connect_timeout_seconds)
        self._resolver = resolver
        self._dialer = dialer or default_stream_dialer

    async def dial(
        self,
        address: str,
        *,
        network: str = "tcp",
        ssl: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
    ) -> Any:
        """Resolve and connect to address using a single shared connect timeout budget."""
        if network not in _ALLOWED_NETWORKS:
            raise _reject_security()

        try:
            host, port = split_dial_address(address)
        except ValueError:
            raise _reject_security() from SecurityRejectedError(
                ERR_SECURITY_REJECTED_MESSAGE
            )

        start = time.monotonic()
        dial_ip, dial_port = await resolve_and_validate_address(
            host,
            port,
            resolver=self._resolver,
            timeout_seconds=self._connect_timeout_seconds,
        )

        elapsed = time.monotonic() - start
        remaining = self._connect_timeout_seconds - elapsed
        if remaining <= 0:
            raise ScrapeError(
                ScrapeErrorKind.TIMEOUT,
                ScrapeStage.FETCH,
                "network connection failed",
                cause=TimeoutError("connect timeout expired"),
            )

        if ssl is None:
            effective_hostname = None
        elif server_hostname is not None:
            effective_hostname = server_hostname
        elif not _is_ip_literal(host):
            effective_hostname = host.rstrip(".")
        else:
            effective_hostname = dial_ip

        try:
            async with asyncio.timeout(remaining):
                return await _invoke_dialer(
                    self._dialer,
                    dial_ip,
                    dial_port,
                    ssl=ssl,
                    server_hostname=effective_hostname,
                )
        except TimeoutError as exc:
            raise ScrapeError(
                ScrapeErrorKind.TIMEOUT,
                ScrapeStage.FETCH,
                "network connection failed",
                cause=exc,
            ) from exc
        except asyncio.CancelledError as exc:
            raise ScrapeError(
                ScrapeErrorKind.CANCELED,
                ScrapeStage.FETCH,
                "network connection failed",
                cause=exc,
            ) from exc
        except ScrapeError:
            raise
        except Exception as exc:
            raise ScrapeError(
                ScrapeErrorKind.NETWORK,
                ScrapeStage.FETCH,
                "network connection failed",
                cause=exc,
            ) from exc


async def dial_public_address(
    address: str,
    *,
    timeout: float = DEFAULT_CONNECT_TIMEOUT_SECONDS,
    resolver: AddressResolver | Callable[..., Any] | None = None,
    dialer: StreamDialer | Callable[..., Any] | None = None,
    network: str = "tcp",
    ssl: ssl.SSLContext | None = None,
    server_hostname: str | None = None,
) -> Any:
    """Convenience function dialing a public destination using SafeDialer."""
    return await SafeDialer(
        connect_timeout_seconds=timeout,
        resolver=resolver,
        dialer=dialer,
    ).dial(address, network=network, ssl=ssl, server_hostname=server_hostname)


def create_tls_context(
    *,
    cafile: str | None = None,
    capath: str | None = None,
    cadata: str | bytes | None = None,
) -> ssl.SSLContext:
    """Create a secure SSLContext enforcing >= TLSv1.2 and strict hostname verification."""
    ctx = ssl.create_default_context(
        purpose=ssl.Purpose.SERVER_AUTH,
        cafile=cafile,
        capath=capath,
        cadata=cadata,
    )
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.check_hostname = True
    return ctx


class SafeTCPConnector(aiohttp.TCPConnector):
    """aiohttp TCPConnector enforcing coupled DNS resolution and public IP pinning.

    Disables DNS caching, keep-alive, and HTTP proxies.
    """

    def __init__(
        self,
        *,
        connect_timeout_seconds: float = DEFAULT_CONNECT_TIMEOUT_SECONDS,
        resolver: AddressResolver | Callable[..., Any] | None = None,
        ssl_context: ssl.SSLContext | None = None,
        test_dial_target: tuple[str, int] | None = None,
        **kwargs: Any,
    ) -> None:
        if (
            not isinstance(connect_timeout_seconds, (int, float))
            or isinstance(connect_timeout_seconds, bool)
            or not math.isfinite(connect_timeout_seconds)
            or connect_timeout_seconds <= 0
        ):
            connect_timeout_seconds = DEFAULT_CONNECT_TIMEOUT_SECONDS
        self._connect_timeout_seconds = float(connect_timeout_seconds)
        self._custom_resolver = resolver
        self._test_dial_target = test_dial_target

        ssl_ctx = ssl_context or create_tls_context()
        super().__init__(
            force_close=True,
            use_dns_cache=False,
            ssl=ssl_ctx,
            **kwargs,
        )

    async def _create_connection(
        self,
        req: aiohttp.ClientRequest,
        traces: list[Any],
        timeout: Any,
    ) -> aiohttp.connector.ResponseHandler:
        if req.proxy is not None:
            raise ScrapeError(
                ScrapeErrorKind.SECURITY_REJECTED,
                ScrapeStage.FETCH,
                "network connection failed: proxies are disabled",
                cause=SecurityRejectedError("proxy connections are prohibited"),
            )
        return await super()._create_connection(req, traces, timeout)

    async def _resolve_host(
        self,
        host: str,
        port: int,
        traces: Sequence[Any] | None = None,
    ) -> list[aiohttp.connector.ResolveResult]:
        dial_ip, dial_port = await resolve_and_validate_address(
            host,
            port,
            resolver=self._custom_resolver,
            timeout_seconds=self._connect_timeout_seconds,
        )

        target_host: str
        target_port: int
        if self._test_dial_target is not None:
            target_host, target_port = self._test_dial_target
        else:
            target_host, target_port = dial_ip, dial_port

        family = socket.AF_INET6 if ":" in target_host else socket.AF_INET
        return [
            {
                "hostname": host,
                "host": target_host,
                "port": target_port,
                "family": family,
                "proto": 0,
                "flags": 0,
            }
        ]

    async def _create_direct_connection(
        self,
        req: aiohttp.ClientRequest,
        traces: list[Any],
        timeout: Any,
        *,
        client_error: type[Exception] = aiohttp.ClientConnectorError,
    ) -> Any:
        try:
            async with asyncio.timeout(self._connect_timeout_seconds):
                return await super()._create_direct_connection(
                    req, traces, timeout, client_error=client_error
                )
        except ScrapeError:
            raise
        except TimeoutError as exc:
            raise ScrapeError(
                ScrapeErrorKind.TIMEOUT,
                ScrapeStage.FETCH,
                "network connection failed",
                cause=exc,
            ) from exc
        except asyncio.CancelledError as exc:
            raise ScrapeError(
                ScrapeErrorKind.CANCELED,
                ScrapeStage.FETCH,
                "network connection failed",
                cause=exc,
            ) from exc
        except Exception as exc:
            raise ScrapeError(
                ScrapeErrorKind.NETWORK,
                ScrapeStage.FETCH,
                "network connection failed",
                cause=exc,
            ) from exc


def create_safe_connector(
    *,
    connect_timeout_seconds: float = DEFAULT_CONNECT_TIMEOUT_SECONDS,
    resolver: AddressResolver | Callable[..., Any] | None = None,
    ssl_context: ssl.SSLContext | None = None,
    test_dial_target: tuple[str, int] | None = None,
    **kwargs: Any,
) -> SafeTCPConnector:
    """Create a configured SafeTCPConnector."""
    return SafeTCPConnector(
        connect_timeout_seconds=connect_timeout_seconds,
        resolver=resolver,
        ssl_context=ssl_context,
        test_dial_target=test_dial_target,
        **kwargs,
    )


def create_safe_client_session(
    *,
    connector: aiohttp.TCPConnector | None = None,
    connect_timeout_seconds: float = DEFAULT_CONNECT_TIMEOUT_SECONDS,
    resolver: AddressResolver | Callable[..., Any] | None = None,
    ssl_context: ssl.SSLContext | None = None,
    test_dial_target: tuple[str, int] | None = None,
    **kwargs: Any,
) -> aiohttp.ClientSession:
    """Create a ClientSession with safe defaults (no env proxies, dummy cookie jar, SafeTCPConnector)."""
    if connector is None:
        connector = create_safe_connector(
            connect_timeout_seconds=connect_timeout_seconds,
            resolver=resolver,
            ssl_context=ssl_context,
            test_dial_target=test_dial_target,
        )
    return aiohttp.ClientSession(
        connector=connector,
        cookie_jar=aiohttp.DummyCookieJar(),
        trust_env=False,
        **kwargs,
    )
