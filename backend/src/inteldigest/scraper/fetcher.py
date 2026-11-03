"""Bounded HTTP page fetcher enforcing redirect safety, timeout budgets, and body limits (M12)."""

import asyncio
import codecs
import email.message
import math
import ssl
import sys
import time
import urllib.parse
import zlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Self

import aiohttp

from inteldigest.scraper.models import (
    ScrapeError,
    ScrapeErrorKind,
    ScrapeStage,
    ScrapeStats,
)
from inteldigest.scraper.network import (
    DEFAULT_CONNECT_TIMEOUT_SECONDS,
    DEFAULT_TLS_TIMEOUT_SECONDS,
    AddressResolver,
    create_safe_client_session,
)
from inteldigest.scraper.policy import (
    SecurityRejectedError,
    validate_url_policy,
)

DEFAULT_SCRAPER_CONNECT_TIMEOUT_SECONDS: float = DEFAULT_CONNECT_TIMEOUT_SECONDS
DEFAULT_SCRAPER_TLS_TIMEOUT_SECONDS: float = DEFAULT_TLS_TIMEOUT_SECONDS
DEFAULT_SCRAPER_HEADER_TIMEOUT_SECONDS: float = 10.0
DEFAULT_SCRAPER_READ_TIMEOUT_SECONDS: float = 10.0
DEFAULT_SCRAPER_REQUEST_TIMEOUT_SECONDS: float = 30.0
DEFAULT_SCRAPER_MAX_BODY_BYTES: int = 5_242_880  # 5 << 20
DEFAULT_SCRAPER_MAX_REDIRECTS: int = 5
MAX_SCRAPER_REDIRECTS: int = 10
CONTENT_SNIFF_BYTES: int = 512

_REDIRECT_STATUSES: frozenset[int] = frozenset({301, 302, 303, 307, 308})
_HTML_MARKERS: tuple[str, ...] = (
    "<!doctype html",
    "<html",
    "<head",
    "<body",
    "<article",
)
_ALLOWED_MEDIA_TYPES: frozenset[str] = frozenset({
    "text/html",
    "application/xhtml+xml",
})


@dataclass(slots=True)
class ScraperOptions:
    """Explicit network budgets and response bounds for safe scraping."""

    connect_timeout_seconds: float = DEFAULT_SCRAPER_CONNECT_TIMEOUT_SECONDS
    tls_timeout_seconds: float = DEFAULT_SCRAPER_TLS_TIMEOUT_SECONDS
    header_timeout_seconds: float = DEFAULT_SCRAPER_HEADER_TIMEOUT_SECONDS
    read_timeout_seconds: float = DEFAULT_SCRAPER_READ_TIMEOUT_SECONDS
    request_timeout_seconds: float = DEFAULT_SCRAPER_REQUEST_TIMEOUT_SECONDS
    max_body_bytes: int = DEFAULT_SCRAPER_MAX_BODY_BYTES
    max_redirects: int = DEFAULT_SCRAPER_MAX_REDIRECTS


def default_options() -> ScraperOptions:
    """Return the conservative default scraper budgets."""
    return ScraperOptions()


def validate_scraper_options(options: ScraperOptions) -> None:
    """Validate scraper configuration limits, raising ValueError on out-of-bound settings."""
    for field_name in (
        "connect_timeout_seconds",
        "tls_timeout_seconds",
        "header_timeout_seconds",
        "read_timeout_seconds",
        "request_timeout_seconds",
    ):
        val = getattr(options, field_name)
        if (
            isinstance(val, bool)
            or not isinstance(val, (int, float))
            or not math.isfinite(val)
            or val <= 0
        ):
            raise ValueError("scraper: all time limits must be positive")

    mb = options.max_body_bytes
    if (
        isinstance(mb, bool)
        or not isinstance(mb, int)
        or mb <= 0
        or mb >= sys.maxsize
    ):
        raise ValueError(
            "scraper: body limit must be positive and allow a safe max+1 read"
        )

    mr = options.max_redirects
    if (
        isinstance(mr, bool)
        or not isinstance(mr, int)
        or mr < 0
        or mr > MAX_SCRAPER_REDIRECTS
    ):
        raise ValueError(
            f"scraper: redirect limit must be between 0 and {MAX_SCRAPER_REDIRECTS}"
        )


@dataclass(frozen=True, slots=True)
class FetchedPage:
    """Result of a bounded, security-approved HTTP fetch operation."""

    requested_url: str
    final_url: str
    status_code: int
    content_type: str
    charset: str | None
    body: bytes
    fetched_at: datetime


class Fetcher:
    """Safe bounded HTTP fetcher enforcing redirect safety and decompressed size limits."""

    def __init__(
        self,
        options: ScraperOptions | None = None,
        *,
        resolver: AddressResolver | Callable[..., Any] | None = None,
        ssl_context: ssl.SSLContext | None = None,
        test_dial_target: tuple[str, int] | None = None,
        connector: aiohttp.TCPConnector | None = None,
        session: aiohttp.ClientSession | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if options is None:
            options = default_options()
        validate_scraper_options(options)
        self._options = options
        self._resolver = resolver
        self._ssl_context = ssl_context
        self._test_dial_target = test_dial_target
        self._custom_connector = connector
        self._custom_session = session
        self._clock = clock or (lambda: datetime.now(UTC))
        self._owned_session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._custom_session is not None:
            return self._custom_session
        if self._owned_session is None or self._owned_session.closed:
            self._owned_session = create_safe_client_session(
                connector=self._custom_connector,
                connect_timeout_seconds=self._options.connect_timeout_seconds,
                resolver=self._resolver,
                ssl_context=self._ssl_context,
                test_dial_target=self._test_dial_target,
                auto_decompress=False,
            )
        return self._owned_session

    async def close(self) -> None:
        """Close any owned HTTP client session and release network resources."""
        if self._owned_session is not None and not self._owned_session.closed:
            await self._owned_session.close()
            self._owned_session = None

    async def aclose(self) -> None:
        """Alias for close()."""
        await self.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def fetch(self, raw_url: str) -> FetchedPage:
        """Fetch and decompress an article HTML page, bounding memory and redirects."""
        page, _ = await self.fetch_with_stats(raw_url)
        return page

    async def fetch_with_stats(
        self,
        raw_url: str,
        *,
        stats: ScrapeStats | None = None,
    ) -> tuple[FetchedPage, ScrapeStats]:
        """Fetch page and update caller-provided ScrapeStats with decompressed bytes."""
        if stats is None:
            stats = ScrapeStats()
        stats.bytes_read = 0

        # Step 1: Initial URL policy validation before network or DNS
        validate_url_policy(raw_url)

        # Step 2: Global request timeout deadline shared across all hops
        global_start = time.monotonic()
        global_deadline = global_start + self._options.request_timeout_seconds

        session = await self._get_session()
        current_url = raw_url
        redirects = 0

        while True:
            # Check global deadline
            now = time.monotonic()
            remaining_total = global_deadline - now
            if remaining_total <= 0:
                raise ScrapeError(
                    ScrapeErrorKind.TIMEOUT,
                    ScrapeStage.FETCH,
                    "scrape request timed out",
                    cause=TimeoutError("global request timeout expired"),
                )

            hop_header_budget = min(self._options.header_timeout_seconds, remaining_total)
            hop_connect_budget = min(self._options.connect_timeout_seconds, remaining_total)

            timeout = aiohttp.ClientTimeout(
                total=remaining_total,
                connect=hop_connect_budget,
                sock_read=hop_header_budget,
            )

            headers = {
                "Accept": "text/html, application/xhtml+xml",
                "Accept-Encoding": "gzip",
            }

            try:
                response = await session.get(
                    current_url,
                    headers=headers,
                    allow_redirects=False,
                    auto_decompress=False,
                    timeout=timeout,
                )
            except ScrapeError:
                raise
            except asyncio.CancelledError as exc:
                raise ScrapeError(
                    ScrapeErrorKind.CANCELED,
                    ScrapeStage.FETCH,
                    "scrape request was canceled",
                    cause=exc,
                ) from exc
            except (TimeoutError, aiohttp.ServerTimeoutError) as exc:
                if time.monotonic() >= global_deadline:
                    raise ScrapeError(
                        ScrapeErrorKind.TIMEOUT,
                        ScrapeStage.FETCH,
                        "scrape request timed out",
                        cause=exc,
                    ) from exc
                raise ScrapeError(
                    ScrapeErrorKind.TIMEOUT,
                    ScrapeStage.FETCH,
                    "network stage timed out",
                    cause=exc,
                ) from exc
            except (
                aiohttp.ClientConnectorCertificateError,
                aiohttp.ClientSSLError,
                ssl.SSLError,
                ssl.CertificateError,
            ) as exc:
                raise ScrapeError(
                    ScrapeErrorKind.NETWORK,
                    ScrapeStage.FETCH,
                    "network request failed",
                    cause=exc,
                ) from exc
            except Exception as exc:
                raise ScrapeError(
                    ScrapeErrorKind.NETWORK,
                    ScrapeStage.FETCH,
                    "network request failed",
                    cause=exc,
                ) from exc

            status = response.status

            # Handle Redirects (301, 302, 303, 307, 308)
            if status in _REDIRECT_STATUSES:
                location = response.headers.get("Location")
                response.close()

                if not location or not location.strip():
                    raise ScrapeError(
                        ScrapeErrorKind.HTTP_STATUS,
                        ScrapeStage.FETCH,
                        http_status=status,
                    )

                if redirects >= self._options.max_redirects:
                    raise ScrapeError(
                        ScrapeErrorKind.SECURITY_REJECTED,
                        ScrapeStage.FETCH,
                        "redirect limit exceeded",
                        cause=SecurityRejectedError("redirect limit exceeded"),
                    )

                next_url = urllib.parse.urljoin(current_url, location.strip())
                # Validate redirect destination policy before request or DNS
                validate_url_policy(next_url)

                # HTTPS -> HTTP downgrade rejection
                current_scheme = urllib.parse.urlsplit(current_url).scheme.lower()
                next_scheme = urllib.parse.urlsplit(next_url).scheme.lower()
                if current_scheme == "https" and next_scheme == "http":
                    raise ScrapeError(
                        ScrapeErrorKind.SECURITY_REJECTED,
                        ScrapeStage.VALIDATION,
                        "HTTPS downgrade rejected",
                        cause=SecurityRejectedError("HTTPS downgrade rejected"),
                    )

                current_url = next_url
                redirects += 1
                continue

            # Non-redirect non-2xx status code
            if status < 200 or status >= 300:
                response.close()
                raise ScrapeError(
                    ScrapeErrorKind.HTTP_STATUS,
                    ScrapeStage.FETCH,
                    http_status=status,
                )

            # Final 2xx response: read decompressed body under bounded budget
            try:
                body = await self._read_body(response, global_deadline)
            finally:
                response.close()

            # Validate MIME type and charset
            content_type, charset = self._validate_media_type_and_charset(
                response, body
            )

            # Body successfully read and validated: record decompressed byte count
            stats.bytes_read = len(body)

            return (
                FetchedPage(
                    requested_url=raw_url,
                    final_url=current_url,
                    status_code=status,
                    content_type=content_type,
                    charset=charset,
                    body=body,
                    fetched_at=self._clock(),
                ),
                stats,
            )

    async def _read_body(
        self,
        response: aiohttp.ClientResponse,
        global_deadline: float,
    ) -> bytes:
        now = time.monotonic()
        remaining_total = global_deadline - now
        if remaining_total <= 0:
            raise ScrapeError(
                ScrapeErrorKind.TIMEOUT,
                ScrapeStage.FETCH,
                "scrape request timed out",
                cause=TimeoutError("global request timeout expired"),
            )

        body_budget = min(self._options.read_timeout_seconds, remaining_total)
        try:
            async with asyncio.timeout(body_budget):
                return await self._read_and_decompress(response)
        except ScrapeError:
            raise
        except asyncio.CancelledError as exc:
            raise ScrapeError(
                ScrapeErrorKind.CANCELED,
                ScrapeStage.FETCH,
                "response read was canceled",
                cause=exc,
            ) from exc
        except TimeoutError as exc:
            if time.monotonic() >= global_deadline:
                raise ScrapeError(
                    ScrapeErrorKind.TIMEOUT,
                    ScrapeStage.FETCH,
                    "scrape request timed out",
                    cause=exc,
                ) from exc
            raise ScrapeError(
                ScrapeErrorKind.TIMEOUT,
                ScrapeStage.FETCH,
                "response read budget expired",
                cause=exc,
            ) from exc
        except Exception as exc:
            raise ScrapeError(
                ScrapeErrorKind.NETWORK,
                ScrapeStage.FETCH,
                "response body could not be read",
                cause=exc,
            ) from exc

    async def _read_and_decompress(
        self, response: aiohttp.ClientResponse
    ) -> bytes:
        raw_encoding = response.headers.get("Content-Encoding", "").strip()
        encoding = raw_encoding.lower()

        if encoding not in ("", "identity", "gzip", "x-gzip"):
            raise ScrapeError(
                ScrapeErrorKind.UNSUPPORTED_CONTENT,
                ScrapeStage.FETCH,
                "response content encoding is unsupported",
            )

        max_limit = self._options.max_body_bytes

        if encoding in ("", "identity"):
            cl_header = response.headers.get("Content-Length")
            if cl_header is not None:
                try:
                    cl = int(cl_header.strip())
                    if cl > max_limit:
                        raise ScrapeError(
                            ScrapeErrorKind.BODY_TOO_LARGE,
                            ScrapeStage.FETCH,
                            "response exceeds configured body limit",
                        )
                except ValueError:
                    pass

            chunks: list[bytes] = []
            total = 0
            async for chunk in response.content.iter_chunked(65536):
                if not chunk:
                    continue
                total += len(chunk)
                if total > max_limit:
                    raise ScrapeError(
                        ScrapeErrorKind.BODY_TOO_LARGE,
                        ScrapeStage.FETCH,
                        "response exceeds configured body limit",
                    )
                chunks.append(chunk)
            return b"".join(chunks)

        # Gzip decompression
        decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
        decompressed_chunks: list[bytes] = []
        total_decompressed = 0

        async for chunk in response.content.iter_chunked(65536):
            if not chunk:
                continue
            unconsumed = chunk
            while unconsumed:
                allowed = (max_limit + 1) - total_decompressed
                if allowed <= 0:
                    raise ScrapeError(
                        ScrapeErrorKind.BODY_TOO_LARGE,
                        ScrapeStage.FETCH,
                        "response exceeds configured body limit",
                    )
                try:
                    out = decompressor.decompress(unconsumed, allowed)
                except zlib.error as exc:
                    raise ScrapeError(
                        ScrapeErrorKind.NETWORK,
                        ScrapeStage.FETCH,
                        "response body could not be decompressed",
                        cause=exc,
                    ) from exc

                if out:
                    total_decompressed += len(out)
                    if total_decompressed > max_limit:
                        raise ScrapeError(
                            ScrapeErrorKind.BODY_TOO_LARGE,
                            ScrapeStage.FETCH,
                            "response exceeds configured body limit",
                        )
                    decompressed_chunks.append(out)

                unconsumed = decompressor.unconsumed_tail

        try:
            flush_out = decompressor.flush()
        except zlib.error as exc:
            raise ScrapeError(
                ScrapeErrorKind.NETWORK,
                ScrapeStage.FETCH,
                "response body could not be decompressed",
                cause=exc,
            ) from exc

        if flush_out:
            total_decompressed += len(flush_out)
            if total_decompressed > max_limit:
                raise ScrapeError(
                    ScrapeErrorKind.BODY_TOO_LARGE,
                    ScrapeStage.FETCH,
                    "response exceeds configured body limit",
                )
            decompressed_chunks.append(flush_out)

        if not decompressor.eof:
            raise ScrapeError(
                ScrapeErrorKind.NETWORK,
                ScrapeStage.FETCH,
                "response body could not be decompressed: truncated gzip stream",
                cause=zlib.error("incomplete or truncated gzip stream"),
            )

        return b"".join(decompressed_chunks)

    def _validate_media_type_and_charset(
        self,
        response: aiohttp.ClientResponse,
        body: bytes,
    ) -> tuple[str, str | None]:
        raw_content_type = response.headers.get("Content-Type", "").strip()

        if not raw_content_type:
            prefix = body[:CONTENT_SNIFF_BYTES]
            if prefix.startswith(b"\xef\xbb\xbf"):
                prefix = prefix[3:]
            prefix_str = prefix.decode("utf-8", errors="replace").lstrip().lower()
            if not any(prefix_str.startswith(marker) for marker in _HTML_MARKERS):
                raise ScrapeError(
                    ScrapeErrorKind.UNSUPPORTED_CONTENT,
                    ScrapeStage.FETCH,
                    "response is not recognizable HTML",
                )
            return "text/html", None

        msg = email.message.EmailMessage()
        try:
            msg["content-type"] = raw_content_type
            ct = msg["content-type"]
        except Exception as exc:
            raise ScrapeError(
                ScrapeErrorKind.UNSUPPORTED_CONTENT,
                ScrapeStage.FETCH,
                "response MIME type is unsupported",
                cause=exc,
            ) from exc

        if ct.defects:
            raise ScrapeError(
                ScrapeErrorKind.UNSUPPORTED_CONTENT,
                ScrapeStage.FETCH,
                "response MIME type is unsupported",
            )

        media_type = f"{ct.maintype}/{ct.subtype}".lower()
        if media_type not in _ALLOWED_MEDIA_TYPES:
            raise ScrapeError(
                ScrapeErrorKind.UNSUPPORTED_CONTENT,
                ScrapeStage.FETCH,
                "response MIME type is unsupported",
            )

        charset: str | None = None
        raw_charset = ct.params.get("charset")
        if raw_charset is not None and raw_charset.strip():
            candidate = raw_charset.strip()
            try:
                codecs.lookup(candidate)
                charset = candidate
            except LookupError as exc:
                raise ScrapeError(
                    ScrapeErrorKind.UNSUPPORTED_CONTENT,
                    ScrapeStage.FETCH,
                    "HTML charset is unsupported",
                    cause=exc,
                ) from exc

        return media_type, charset


SafeFetcher = Fetcher
