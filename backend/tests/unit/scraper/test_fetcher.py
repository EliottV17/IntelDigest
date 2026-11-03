"""Unit tests for bounded HTTP fetcher and redirect safety (M12)."""

import asyncio
import gzip
import ssl
import sys
from datetime import UTC, datetime
from typing import Any

import pytest
from aiohttp import web

from inteldigest.scraper.fetcher import (
    CONTENT_SNIFF_BYTES,
    DEFAULT_SCRAPER_CONNECT_TIMEOUT_SECONDS,
    DEFAULT_SCRAPER_HEADER_TIMEOUT_SECONDS,
    DEFAULT_SCRAPER_MAX_BODY_BYTES,
    DEFAULT_SCRAPER_MAX_REDIRECTS,
    DEFAULT_SCRAPER_READ_TIMEOUT_SECONDS,
    DEFAULT_SCRAPER_REQUEST_TIMEOUT_SECONDS,
    DEFAULT_SCRAPER_TLS_TIMEOUT_SECONDS,
    MAX_SCRAPER_REDIRECTS,
    Fetcher,
    SafeFetcher,
    ScraperOptions,
    default_options,
    validate_scraper_options,
)
from inteldigest.scraper.models import (
    ScrapeError,
    ScrapeErrorKind,
    ScrapeStage,
    ScrapeStats,
)
from inteldigest.scraper.policy import SecurityRejectedError

# ---------------------------------------------------------------------------
# Test Server Helper
# ---------------------------------------------------------------------------


class LocalTestServer:
    """Manages an in-process aiohttp test server bound to 127.0.0.1."""

    def __init__(self, app: web.Application) -> None:
        self.app = app
        self.runner: web.AppRunner | None = None
        self.site: web.TCPSite | None = None
        self.port: int = 0

    async def start(self) -> LocalTestServer:
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await self.site.start()
        server = self.site._server
        assert isinstance(server, asyncio.Server)
        assert server.sockets
        self.port = int(server.sockets[0].getsockname()[1])
        return self

    async def stop(self) -> None:
        if self.runner is not None:
            await self.runner.cleanup()


def _generate_self_signed_cert(
    temp_dir: str, hostname: str = "news.example"
) -> tuple[str, str]:
    import os
    import subprocess

    key_path = os.path.join(temp_dir, "key.pem")
    cert_path = os.path.join(temp_dir, "cert.pem")
    cmd = [
        "openssl",
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        key_path,
        "-out",
        cert_path,
        "-days",
        "1",
        "-subj",
        f"/CN={hostname}",
        "-addext",
        f"subjectAltName=DNS:{hostname}",
    ]
    subprocess.check_call(cmd, stderr=subprocess.DEVNULL)
    return cert_path, key_path
def make_test_fetcher(
    server: LocalTestServer,
    options: ScraperOptions | None = None,
    clock: Any = None,
) -> Fetcher:
    async def mock_resolver(host: str, port: int) -> list[str]:
        # Return a valid public IPv4 address so policy approval passes
        return ["93.184.216.34"]

    return Fetcher(
        options=options,
        resolver=mock_resolver,
        test_dial_target=("127.0.0.1", server.port),
        clock=clock,
    )


# ---------------------------------------------------------------------------
# 1. ScraperOptions & Validation
# ---------------------------------------------------------------------------


def test_default_options_values() -> None:
    opts = default_options()
    assert opts.connect_timeout_seconds == DEFAULT_SCRAPER_CONNECT_TIMEOUT_SECONDS == 5.0
    assert opts.tls_timeout_seconds == DEFAULT_SCRAPER_TLS_TIMEOUT_SECONDS == 5.0
    assert opts.header_timeout_seconds == DEFAULT_SCRAPER_HEADER_TIMEOUT_SECONDS == 10.0
    assert opts.read_timeout_seconds == DEFAULT_SCRAPER_READ_TIMEOUT_SECONDS == 10.0
    assert opts.request_timeout_seconds == DEFAULT_SCRAPER_REQUEST_TIMEOUT_SECONDS == 30.0
    assert opts.max_body_bytes == DEFAULT_SCRAPER_MAX_BODY_BYTES == 5_242_880
    assert opts.max_redirects == DEFAULT_SCRAPER_MAX_REDIRECTS == 5
    validate_scraper_options(opts)


def test_validate_scraper_options_timeout_rejections() -> None:
    timeout_fields = [
        "connect_timeout_seconds",
        "tls_timeout_seconds",
        "header_timeout_seconds",
        "read_timeout_seconds",
        "request_timeout_seconds",
    ]
    for field in timeout_fields:
        for bad_val in (0.0, -1.0, -0.001, float("inf"), float("-inf"), float("nan"), True, False, "5"):
            opts = default_options()
            setattr(opts, field, bad_val)
            with pytest.raises(ValueError, match="scraper: all time limits must be positive"):
                validate_scraper_options(opts)


def test_validate_scraper_options_body_limit_rejections() -> None:
    for bad_val in (0, -1, -500, sys.maxsize, sys.maxsize + 1, True, False, 5.5, "5242880"):
        opts = default_options()
        opts.max_body_bytes = bad_val  # type: ignore[assignment]
        with pytest.raises(
            ValueError,
            match="scraper: body limit must be positive and allow a safe max\\+1 read",
        ):
            validate_scraper_options(opts)


def test_validate_scraper_options_redirect_limit_rejections_and_boundaries() -> None:
    for bad_val in (-1, -5, MAX_SCRAPER_REDIRECTS + 1, 100, True, False, 5.0, "5"):
        opts = default_options()
        opts.max_redirects = bad_val  # type: ignore[assignment]
        with pytest.raises(
            ValueError,
            match=f"scraper: redirect limit must be between 0 and {MAX_SCRAPER_REDIRECTS}",
        ):
            validate_scraper_options(opts)

    # 0 is valid (disables redirects)
    opts0 = default_options()
    opts0.max_redirects = 0
    validate_scraper_options(opts0)

    # 10 is valid (maximum allowed)
    opts10 = default_options()
    opts10.max_redirects = 10
    validate_scraper_options(opts10)


def test_fetcher_constructor_validates_options() -> None:
    bad_opts = default_options()
    bad_opts.max_body_bytes = -1
    with pytest.raises(ValueError):
        Fetcher(options=bad_opts)

    # SafeFetcher alias behaves identically
    assert SafeFetcher is Fetcher


# ---------------------------------------------------------------------------
# 2. Initial URL Policy Validation (Zero Dials / Zero DNS)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_validates_initial_url_before_network() -> None:
    dial_called = False

    async def mock_resolver(host: str, port: int) -> list[str]:
        nonlocal dial_called
        dial_called = True
        return ["93.184.216.34"]

    fetcher = Fetcher(resolver=mock_resolver)
    stats = ScrapeStats()

    for bad_url in [
        "http://127.0.0.1/private",
        "http://169.254.169.254/latest",
        "http://metadata.google.internal/",
        "ftp://news.example/article",
        "http://news.example:8080/article",
        "http://user:pass@news.example/article",
        "http://0x7f000001/article",
        "not-a-url",
        "",
    ]:
        with pytest.raises(ScrapeError) as exc_info:
            await fetcher.fetch_with_stats(bad_url, stats=stats)
        err = exc_info.value
        assert err.kind == ScrapeErrorKind.SECURITY_REJECTED
        assert err.stage == ScrapeStage.VALIDATION
        assert isinstance(err.cause, SecurityRejectedError)
        assert not dial_called
        assert stats.bytes_read == 0


# ---------------------------------------------------------------------------
# 3. Successful Fetch of HTML Page
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_success_html_page() -> None:
    html_content = b"<!DOCTYPE html><html><head><title>Test</title></head><body><article><p>Hello</p></article></body></html>"

    async def handler(request: web.Request) -> web.Response:
        assert request.headers.get("Accept") == "text/html, application/xhtml+xml"
        assert "gzip" in request.headers.get("Accept-Encoding", "")
        return web.Response(
            body=html_content,
            content_type="text/html",
            charset="utf-8",
        )

    app = web.Application()
    app.router.add_get("/article", handler)
    server = await LocalTestServer(app).start()

    fixed_time = datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC)
    fetcher = make_test_fetcher(server, clock=lambda: fixed_time)

    try:
        stats = ScrapeStats()
        page, returned_stats = await fetcher.fetch_with_stats(
            "http://news.example:80/article", stats=stats
        )
        assert returned_stats is stats
        assert page.requested_url == "http://news.example:80/article"
        assert page.final_url == "http://news.example:80/article"
        assert page.status_code == 200
        assert page.content_type == "text/html"
        assert page.charset == "utf-8"
        assert page.body == html_content
        assert page.fetched_at == fixed_time
        assert stats.bytes_read == len(html_content)

        # Convenience fetch() returns page directly
        direct_page = await fetcher.fetch("http://news.example:80/article")
        assert direct_page == page
    finally:
        await fetcher.close()
        await server.stop()


# ---------------------------------------------------------------------------
# 4. HTTP Status Errors (Non-2xx)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_http_status_errors() -> None:
    async def handler(request: web.Request) -> web.Response:
        status = int(request.query["status"])
        return web.Response(
            status=status,
            text=f"Error status {status}",
            content_type="text/html",
        )

    app = web.Application()
    app.router.add_get("/status", handler)
    server = await LocalTestServer(app).start()
    fetcher = make_test_fetcher(server)

    try:
        for status in [300, 400, 403, 404, 429, 500, 502, 503]:
            stats = ScrapeStats()
            with pytest.raises(ScrapeError) as exc_info:
                await fetcher.fetch_with_stats(
                    f"http://news.example:80/status?status={status}", stats=stats
                )
            err = exc_info.value
            assert err.kind == ScrapeErrorKind.HTTP_STATUS
            assert err.stage == ScrapeStage.FETCH
            assert err.http_status == status
            assert str(err) == f"scrape http_status during fetch (HTTP {status})"
            assert stats.bytes_read == 0
    finally:
        await fetcher.close()
        await server.stop()


# ---------------------------------------------------------------------------
# 5. Redirect Handling and Safety
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_redirects_resolve_relative_and_absolute_without_referer() -> None:
    captured_headers: list[dict[str, str]] = []

    async def step1(request: web.Request) -> web.Response:
        captured_headers.append(dict(request.headers))
        return web.Response(
            status=302,
            headers={"Location": "/step2"},
        )

    async def step2(request: web.Request) -> web.Response:
        captured_headers.append(dict(request.headers))
        return web.Response(
            status=301,
            headers={"Location": "http://news.example:80/final"},
        )

    async def final(request: web.Request) -> web.Response:
        captured_headers.append(dict(request.headers))
        return web.Response(
            text="<!DOCTYPE html><html><body><article><p>Final</p></article></body></html>",
            content_type="text/html",
        )

    app = web.Application()
    app.router.add_get("/step1", step1)
    app.router.add_get("/step2", step2)
    app.router.add_get("/final", final)
    server = await LocalTestServer(app).start()
    fetcher = make_test_fetcher(server)

    try:
        stats = ScrapeStats()
        page, _ = await fetcher.fetch_with_stats("http://news.example:80/step1", stats=stats)
        assert page.requested_url == "http://news.example:80/step1"
        assert page.final_url == "http://news.example:80/final"
        assert page.status_code == 200
        assert stats.bytes_read == len(page.body)

        # No hop ever forwarded Referer, Authorization, or Cookie
        assert len(captured_headers) == 3
        for h in captured_headers:
            assert "Referer" not in h
            assert "Authorization" not in h
            assert "Cookie" not in h
    finally:
        await fetcher.close()
        await server.stop()


@pytest.mark.asyncio
async def test_redirect_missing_or_empty_location() -> None:
    async def handler_no_loc(request: web.Request) -> web.Response:
        return web.Response(status=302)

    async def handler_empty_loc(request: web.Request) -> web.Response:
        return web.Response(status=301, headers={"Location": "   "})

    app = web.Application()
    app.router.add_get("/no-loc", handler_no_loc)
    app.router.add_get("/empty-loc", handler_empty_loc)
    server = await LocalTestServer(app).start()
    fetcher = make_test_fetcher(server)

    try:
        with pytest.raises(ScrapeError) as exc1:
            await fetcher.fetch("http://news.example:80/no-loc")
        assert exc1.value.kind == ScrapeErrorKind.HTTP_STATUS
        assert exc1.value.http_status == 302

        with pytest.raises(ScrapeError) as exc2:
            await fetcher.fetch("http://news.example:80/empty-loc")
        assert exc2.value.kind == ScrapeErrorKind.HTTP_STATUS
        assert exc2.value.http_status == 301
    finally:
        await fetcher.close()
        await server.stop()


@pytest.mark.asyncio
async def test_redirect_limit_zero_and_excess_reject_without_next_request() -> None:
    hop_count = 0

    async def loop_handler(request: web.Request) -> web.Response:
        nonlocal hop_count
        hop_count += 1
        return web.Response(
            status=302,
            headers={"Location": f"/loop?hop={hop_count}"},
        )

    app = web.Application()
    app.router.add_get("/loop", loop_handler)
    server = await LocalTestServer(app).start()

    try:
        # 1. max_redirects = 0: first redirect is rejected
        opts0 = default_options()
        opts0.max_redirects = 0
        fetcher0 = make_test_fetcher(server, options=opts0)
        hop_count = 0
        with pytest.raises(ScrapeError) as exc0:
            await fetcher0.fetch("http://news.example:80/loop")
        assert exc0.value.kind == ScrapeErrorKind.SECURITY_REJECTED
        assert exc0.value.stage == ScrapeStage.FETCH
        assert "redirect limit exceeded" in exc0.value.message
        assert hop_count == 1
        await fetcher0.close()

        # 2. max_redirects = 2: 2 redirects permitted, 3rd is rejected
        opts2 = default_options()
        opts2.max_redirects = 2
        fetcher2 = make_test_fetcher(server, options=opts2)
        hop_count = 0
        with pytest.raises(ScrapeError) as exc2:
            await fetcher2.fetch("http://news.example:80/loop")
        assert exc2.value.kind == ScrapeErrorKind.SECURITY_REJECTED
        assert exc2.value.stage == ScrapeStage.FETCH
        assert "redirect limit exceeded" in exc2.value.message
        assert hop_count == 3  # Initial + 2 hops
        await fetcher2.close()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_redirect_destination_policy_rejects_before_destination_request() -> None:
    async def redirect_to_private(request: web.Request) -> web.Response:
        return web.Response(
            status=302,
            headers={"Location": "http://127.0.0.1/private"},
        )

    async def redirect_to_metadata(request: web.Request) -> web.Response:
        return web.Response(
            status=302,
            headers={"Location": "http://169.254.169.254/latest"},
        )

    async def redirect_to_bad_scheme(request: web.Request) -> web.Response:
        return web.Response(
            status=302,
            headers={"Location": "ftp://news.example/file"},
        )

    app = web.Application()
    app.router.add_get("/to-private", redirect_to_private)
    app.router.add_get("/to-metadata", redirect_to_metadata)
    app.router.add_get("/to-bad-scheme", redirect_to_bad_scheme)
    server = await LocalTestServer(app).start()
    fetcher = make_test_fetcher(server)

    try:
        for path in ["/to-private", "/to-metadata", "/to-bad-scheme"]:
            stats = ScrapeStats()
            with pytest.raises(ScrapeError) as exc_info:
                await fetcher.fetch_with_stats(
                    f"http://news.example:80{path}", stats=stats
                )
            err = exc_info.value
            assert err.kind == ScrapeErrorKind.SECURITY_REJECTED
            assert err.stage == ScrapeStage.VALIDATION
            assert isinstance(err.cause, SecurityRejectedError)
            assert stats.bytes_read == 0
    finally:
        await fetcher.close()
        await server.stop()


@pytest.mark.asyncio
async def test_https_downgrade_rejected_before_http_request() -> None:
    import tempfile

    from inteldigest.scraper.network import create_tls_context

    with tempfile.TemporaryDirectory() as td:
        cert_path, key_path = _generate_self_signed_cert(td, "news.example")
        server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_ctx.load_cert_chain(cert_path, key_path)

        async def redirect_to_http(request: web.Request) -> web.Response:
            return web.Response(
                status=302,
                headers={"Location": "http://news.example:80/insecure"},
            )

        app = web.Application()
        app.router.add_get("/downgrade", redirect_to_http)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=server_ctx)
        await site.start()
        server = site._server
        assert isinstance(server, asyncio.Server)
        assert server.sockets
        server_port = int(server.sockets[0].getsockname()[1])

        client_tls = create_tls_context(cafile=cert_path)

        async def mock_resolver(host: str, port: int) -> list[str]:
            return ["93.184.216.34"]

        fetcher = Fetcher(
            resolver=mock_resolver,
            ssl_context=client_tls,
            test_dial_target=("127.0.0.1", server_port),
        )

        try:
            stats = ScrapeStats()
            with pytest.raises(ScrapeError) as exc_info:
                await fetcher.fetch_with_stats(
                    "https://news.example:443/downgrade", stats=stats
                )
            err = exc_info.value
            assert err.kind == ScrapeErrorKind.SECURITY_REJECTED
            assert err.stage == ScrapeStage.VALIDATION
            assert "HTTPS downgrade rejected" in err.message
            assert isinstance(err.cause, SecurityRejectedError)
            assert stats.bytes_read == 0
        finally:
            await fetcher.close()
            await runner.cleanup()


@pytest.mark.asyncio
async def test_redirect_hop_mixed_dns_rejects_before_second_dial() -> None:
    resolve_calls = 0

    async def switching_resolver(host: str, port: int) -> list[str]:
        nonlocal resolve_calls
        resolve_calls += 1
        if resolve_calls == 1:
            return ["93.184.216.34"]  # Valid public IP for hop 1
        # Mixed DNS on hop 2
        return ["93.184.216.34", "10.0.0.1"]

    async def step1(request: web.Request) -> web.Response:
        return web.Response(
            status=302,
            headers={"Location": "http://hop2.example:80/final"},
        )

    app = web.Application()
    app.router.add_get("/step1", step1)
    server = await LocalTestServer(app).start()

    fetcher = Fetcher(
        resolver=switching_resolver,
        test_dial_target=("127.0.0.1", server.port),
    )

    try:
        stats = ScrapeStats()
        with pytest.raises(ScrapeError) as exc_info:
            await fetcher.fetch_with_stats("http://hop1.example:80/step1", stats=stats)
        err = exc_info.value
        assert err.kind == ScrapeErrorKind.SECURITY_REJECTED
        assert err.stage == ScrapeStage.FETCH
        assert resolve_calls == 2
        assert stats.bytes_read == 0
    finally:
        await fetcher.close()
        await server.stop()


# ---------------------------------------------------------------------------
# 6. Global Request Deadline & Timeout Budgets
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_every_hop_uses_one_global_request_deadline() -> None:
    # 2 redirects where each hop sleeps 0.08s; global deadline is 0.12s
    async def slow_hop1(request: web.Request) -> web.Response:
        await asyncio.sleep(0.08)
        return web.Response(status=302, headers={"Location": "/slow-hop2"})

    async def slow_hop2(request: web.Request) -> web.Response:
        await asyncio.sleep(0.08)
        return web.Response(
            text="<!DOCTYPE html><html><body>article</body></html>",
            content_type="text/html",
        )

    app = web.Application()
    app.router.add_get("/slow-hop1", slow_hop1)
    app.router.add_get("/slow-hop2", slow_hop2)
    server = await LocalTestServer(app).start()

    opts = default_options()
    opts.request_timeout_seconds = 0.12  # Expires during second hop
    fetcher = make_test_fetcher(server, options=opts)

    try:
        stats = ScrapeStats()
        with pytest.raises(ScrapeError) as exc_info:
            await fetcher.fetch_with_stats(
                "http://news.example:80/slow-hop1", stats=stats
            )
        err = exc_info.value
        assert err.kind == ScrapeErrorKind.TIMEOUT
        assert err.stage == ScrapeStage.FETCH
        assert "scrape request timed out" in err.message
        assert stats.bytes_read == 0
    finally:
        await fetcher.close()
        await server.stop()


@pytest.mark.asyncio
async def test_header_budget_timeout() -> None:
    async def slow_headers(request: web.Request) -> web.Response:
        await asyncio.sleep(0.1)
        return web.Response(
            text="<!DOCTYPE html><html><body>article</body></html>",
            content_type="text/html",
        )

    app = web.Application()
    app.router.add_get("/slow-headers", slow_headers)
    server = await LocalTestServer(app).start()

    opts = default_options()
    opts.header_timeout_seconds = 0.03
    fetcher = make_test_fetcher(server, options=opts)

    try:
        with pytest.raises(ScrapeError) as exc_info:
            await fetcher.fetch("http://news.example:80/slow-headers")
        err = exc_info.value
        assert err.kind == ScrapeErrorKind.TIMEOUT
        assert err.stage == ScrapeStage.FETCH
    finally:
        await fetcher.close()
        await server.stop()


@pytest.mark.asyncio
async def test_body_read_budget_timeout() -> None:
    async def slow_body(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(
            status=200,
            reason="OK",
            headers={"Content-Type": "text/html"},
        )
        await response.prepare(request)
        await response.write(b"<!DOCTYPE html><html><body>")
        await asyncio.sleep(0.1)  # Trickle pause exceeds read_timeout_seconds
        await response.write(b"article</body></html>")
        await response.write_eof()
        return response

    app = web.Application()
    app.router.add_get("/slow-body", slow_body)
    server = await LocalTestServer(app).start()

    opts = default_options()
    opts.read_timeout_seconds = 0.03
    fetcher = make_test_fetcher(server, options=opts)

    try:
        with pytest.raises(ScrapeError) as exc_info:
            await fetcher.fetch("http://news.example:80/slow-body")
        err = exc_info.value
        assert err.kind == ScrapeErrorKind.TIMEOUT
        assert err.stage == ScrapeStage.FETCH
        assert "response read budget expired" in err.message
    finally:
        await fetcher.close()
        await server.stop()


@pytest.mark.asyncio
async def test_cancellation_during_fetch_is_preserved() -> None:
    started_reading = asyncio.Event()

    async def hanging_body(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(
            status=200,
            headers={"Content-Type": "text/html"},
        )
        await response.prepare(request)
        await response.write(b"<!DOCTYPE html><html><body>")
        started_reading.set()
        await asyncio.sleep(10.0)
        return response

    app = web.Application()
    app.router.add_get("/hanging", hanging_body)
    server = await LocalTestServer(app).start()
    fetcher = make_test_fetcher(server)

    try:
        task = asyncio.create_task(fetcher.fetch("http://news.example:80/hanging"))
        await started_reading.wait()
        task.cancel()

        with pytest.raises(ScrapeError) as exc_info:
            await task
        err = exc_info.value
        assert err.kind == ScrapeErrorKind.CANCELED
        assert err.stage == ScrapeStage.FETCH
        assert "canceled" in err.message
    finally:
        await fetcher.close()
        await server.stop()


# ---------------------------------------------------------------------------
# 7. Body Limits, Chunking, and Gzip Decompression
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_exact_maximum_body_reaches_caller_and_plus_one_fails() -> None:
    max_bytes = 256
    exact_body = b"<!DOCTYPE html><html><body><p>" + (b"A" * (max_bytes - 48)) + b"</p></body></html>"
    assert len(exact_body) == max_bytes
    plus_one_body = exact_body + b"X"
    assert len(plus_one_body) == max_bytes + 1

    async def exact_handler(request: web.Request) -> web.Response:
        return web.Response(body=exact_body, content_type="text/html")

    async def plus_one_handler(request: web.Request) -> web.Response:
        return web.Response(body=plus_one_body, content_type="text/html")

    app = web.Application()
    app.router.add_get("/exact", exact_handler)
    app.router.add_get("/plus-one", plus_one_handler)
    server = await LocalTestServer(app).start()

    opts = default_options()
    opts.max_body_bytes = max_bytes
    fetcher = make_test_fetcher(server, options=opts)

    try:
        # Exact max bytes succeeds
        page = await fetcher.fetch("http://news.example:80/exact")
        assert len(page.body) == max_bytes

        # Plus one fails with body_too_large
        stats = ScrapeStats()
        with pytest.raises(ScrapeError) as exc_info:
            await fetcher.fetch_with_stats("http://news.example:80/plus-one", stats=stats)
        err = exc_info.value
        assert err.kind == ScrapeErrorKind.BODY_TOO_LARGE
        assert err.stage == ScrapeStage.FETCH
        assert stats.bytes_read == 0
    finally:
        await fetcher.close()
        await server.stop()


@pytest.mark.asyncio
async def test_content_length_header_too_large_rejects_before_read() -> None:
    async def large_header_handler(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(
            status=200,
            headers={"Content-Length": "10000000", "Content-Type": "text/html"},
        )
        await response.prepare(request)
        return response

    app = web.Application()
    app.router.add_get("/too-large-cl", large_header_handler)
    server = await LocalTestServer(app).start()

    opts = default_options()
    opts.max_body_bytes = 1000
    fetcher = make_test_fetcher(server, options=opts)

    try:
        stats = ScrapeStats()
        with pytest.raises(ScrapeError) as exc_info:
            await fetcher.fetch_with_stats("http://news.example:80/too-large-cl", stats=stats)
        assert exc_info.value.kind == ScrapeErrorKind.BODY_TOO_LARGE
        assert exc_info.value.stage == ScrapeStage.FETCH
        assert stats.bytes_read == 0
    finally:
        await fetcher.close()
        await server.stop()


@pytest.mark.asyncio
async def test_chunked_body_too_large_aborts_immediately() -> None:
    async def chunked_handler(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(
            status=200,
            headers={"Content-Type": "text/html"},
        )
        await response.prepare(request)
        for _ in range(50):
            await response.write(b"X" * 100)
        await response.write_eof()
        return response

    app = web.Application()
    app.router.add_get("/chunked", chunked_handler)
    server = await LocalTestServer(app).start()

    opts = default_options()
    opts.max_body_bytes = 200
    fetcher = make_test_fetcher(server, options=opts)

    try:
        stats = ScrapeStats()
        with pytest.raises(ScrapeError) as exc_info:
            await fetcher.fetch_with_stats("http://news.example:80/chunked", stats=stats)
        assert exc_info.value.kind == ScrapeErrorKind.BODY_TOO_LARGE
        assert stats.bytes_read == 0
    finally:
        await fetcher.close()
        await server.stop()


@pytest.mark.asyncio
async def test_gzip_decoding_counts_decompressed_bytes() -> None:
    raw_html = b"<!DOCTYPE html><html><body><article><p>Gzip compressed article content.</p></article></body></html>"
    compressed = gzip.compress(raw_html)

    async def gzip_handler(request: web.Request) -> web.Response:
        return web.Response(
            body=compressed,
            headers={"Content-Encoding": "gzip", "Content-Type": "text/html"},
        )

    app = web.Application()
    app.router.add_get("/gzip", gzip_handler)
    server = await LocalTestServer(app).start()
    fetcher = make_test_fetcher(server)

    try:
        stats = ScrapeStats()
        page, _ = await fetcher.fetch_with_stats(
            "http://news.example:80/gzip", stats=stats
        )
        assert page.body == raw_html
        # stats.bytes_read counts decompressed bytes, NOT compressed wire bytes!
        assert stats.bytes_read == len(raw_html)
        assert stats.bytes_read != len(compressed)
    finally:
        await fetcher.close()
        await server.stop()


@pytest.mark.asyncio
async def test_gzip_expansion_and_corruption_are_bounded() -> None:
    # A small compressed payload that expands to more than max_body_bytes (gzip bomb)
    large_payload = b"article expansion " * 200
    compressed_large = gzip.compress(
        b"<!DOCTYPE html><html><body>" + large_payload + b"</body></html>"
    )

    async def gzip_bomb(request: web.Request) -> web.Response:
        return web.Response(
            body=compressed_large,
            headers={"Content-Encoding": "gzip", "Content-Type": "text/html"},
        )

    async def corrupt_gzip(request: web.Request) -> web.Response:
        return web.Response(
            body=b"not a valid gzip stream at all",
            headers={"Content-Encoding": "gzip", "Content-Type": "text/html"},
        )

    async def truncated_gzip(request: web.Request) -> web.Response:
        return web.Response(
            body=compressed_large[:15],
            headers={"Content-Encoding": "gzip", "Content-Type": "text/html"},
        )

    app = web.Application()
    app.router.add_get("/bomb", gzip_bomb)
    app.router.add_get("/corrupt", corrupt_gzip)
    app.router.add_get("/truncated", truncated_gzip)
    server = await LocalTestServer(app).start()

    opts = default_options()
    opts.max_body_bytes = 256  # Small limit for test
    fetcher = make_test_fetcher(server, options=opts)

    try:
        # Gzip bomb raises BODY_TOO_LARGE
        with pytest.raises(ScrapeError) as exc_bomb:
            await fetcher.fetch("http://news.example:80/bomb")
        assert exc_bomb.value.kind == ScrapeErrorKind.BODY_TOO_LARGE
        assert exc_bomb.value.stage == ScrapeStage.FETCH

        # Corrupt gzip raises NETWORK
        with pytest.raises(ScrapeError) as exc_corrupt:
            await fetcher.fetch("http://news.example:80/corrupt")
        assert exc_corrupt.value.kind == ScrapeErrorKind.NETWORK
        assert exc_corrupt.value.stage == ScrapeStage.FETCH

        # Truncated gzip raises NETWORK
        with pytest.raises(ScrapeError) as exc_trunc:
            await fetcher.fetch("http://news.example:80/truncated")
        assert exc_trunc.value.kind == ScrapeErrorKind.NETWORK
        assert exc_trunc.value.stage == ScrapeStage.FETCH
    finally:
        await fetcher.close()
        await server.stop()


@pytest.mark.asyncio
async def test_unsupported_content_encoding_rejected() -> None:
    import urllib.parse

    async def handler(request: web.Request) -> web.Response:
        enc = request.query["enc"]
        return web.Response(
            body=b"compressed bytes",
            headers={"Content-Encoding": enc, "Content-Type": "text/html"},
        )

    app = web.Application()
    app.router.add_get("/enc", handler)
    server = await LocalTestServer(app).start()
    fetcher = make_test_fetcher(server)

    try:
        for bad_enc in ["br", "deflate", "gzip, br", "zstd"]:
            stats = ScrapeStats()
            encoded_query = urllib.parse.quote(bad_enc)
            with pytest.raises(ScrapeError) as exc_info:
                await fetcher.fetch_with_stats(
                    f"http://news.example:80/enc?enc={encoded_query}", stats=stats
                )
            assert exc_info.value.kind == ScrapeErrorKind.UNSUPPORTED_CONTENT
            assert exc_info.value.stage == ScrapeStage.FETCH
            assert "unsupported" in exc_info.value.message
            assert stats.bytes_read == 0
    finally:
        await fetcher.close()
        await server.stop()


# ---------------------------------------------------------------------------
# 8. MIME Type, 512-Byte HTML Sniffing, and Charset Validation
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mime_parsing_sniffing_and_unsupported_content() -> None:
    async def missing_ct_html(request: web.Request) -> web.Response:
        # Raw aiohttp response without Content-Type header
        return web.Response(
            body=b"<!DOCTYPE html><html><body><article><p>Hello</p></article></body></html>",
            content_type="",
        )

    async def missing_ct_bom(request: web.Request) -> web.Response:
        return web.Response(
            body=b"\xef\xbb\xbf   <html><body><article><p>Hello</p></article></body></html>",
            content_type="",
        )

    async def missing_ct_beyond_sniff(request: web.Request) -> web.Response:
        # HTML tag appears after 512 bytes
        body = (b" " * (CONTENT_SNIFF_BYTES + 1)) + b"<html><body>article</body></html>"
        return web.Response(body=body, content_type="")

    async def binary_mime(request: web.Request) -> web.Response:
        return web.Response(
            body=b"<html><body>not accepted</body></html>",
            content_type="application/octet-stream",
        )

    async def malformed_mime(request: web.Request) -> web.Response:
        return web.Response(
            body=b"<html></html>",
            headers={"Content-Type": 'text/html; charset="unterminated'},
        )

    async def unknown_charset(request: web.Request) -> web.Response:
        return web.Response(
            body=b"<html><body>article</body></html>",
            headers={"Content-Type": "text/html; charset=not-a-real-charset"},
        )

    async def valid_iso_charset(request: web.Request) -> web.Response:
        return web.Response(
            body=b"<html><body>caf\xe9</body></html>",
            headers={"Content-Type": "text/html; charset=ISO-8859-1"},
        )

    async def xhtml_mime(request: web.Request) -> web.Response:
        return web.Response(
            body=b"<!DOCTYPE html><html><body>xhtml</body></html>",
            content_type="application/xhtml+xml",
        )

    app = web.Application()
    app.router.add_get("/missing-ct", missing_ct_html)
    app.router.add_get("/missing-ct-bom", missing_ct_bom)
    app.router.add_get("/beyond-sniff", missing_ct_beyond_sniff)
    app.router.add_get("/binary-mime", binary_mime)
    app.router.add_get("/malformed-mime", malformed_mime)
    app.router.add_get("/unknown-charset", unknown_charset)
    app.router.add_get("/iso-charset", valid_iso_charset)
    app.router.add_get("/xhtml-mime", xhtml_mime)
    server = await LocalTestServer(app).start()
    fetcher = make_test_fetcher(server)

    try:
        # Missing CT with HTML prefix passes sniffing
        page1 = await fetcher.fetch("http://news.example:80/missing-ct")
        assert page1.content_type == "text/html"
        assert page1.charset is None

        # Missing CT with UTF-8 BOM and leading spaces passes sniffing
        page2 = await fetcher.fetch("http://news.example:80/missing-ct-bom")
        assert page2.content_type == "text/html"

        # Beyond 512 bytes fails sniffing
        stats3 = ScrapeStats()
        with pytest.raises(ScrapeError) as exc3:
            await fetcher.fetch_with_stats("http://news.example:80/beyond-sniff", stats=stats3)
        assert exc3.value.kind == ScrapeErrorKind.UNSUPPORTED_CONTENT
        assert stats3.bytes_read == 0

        # Binary MIME fails
        stats4 = ScrapeStats()
        with pytest.raises(ScrapeError) as exc4:
            await fetcher.fetch_with_stats("http://news.example:80/binary-mime", stats=stats4)
        assert exc4.value.kind == ScrapeErrorKind.UNSUPPORTED_CONTENT
        assert stats4.bytes_read == 0

        # Malformed MIME parameter fails
        stats5 = ScrapeStats()
        with pytest.raises(ScrapeError) as exc5:
            await fetcher.fetch_with_stats("http://news.example:80/malformed-mime", stats=stats5)
        assert exc5.value.kind == ScrapeErrorKind.UNSUPPORTED_CONTENT
        assert stats5.bytes_read == 0

        # Unknown charset fails
        stats6 = ScrapeStats()
        with pytest.raises(ScrapeError) as exc6:
            await fetcher.fetch_with_stats("http://news.example:80/unknown-charset", stats=stats6)
        assert exc6.value.kind == ScrapeErrorKind.UNSUPPORTED_CONTENT
        assert stats6.bytes_read == 0

        # Valid ISO-8859-1 charset accepted and extracted
        page7 = await fetcher.fetch("http://news.example:80/iso-charset")
        assert page7.content_type == "text/html"
        assert page7.charset == "ISO-8859-1"

        # application/xhtml+xml accepted
        page8 = await fetcher.fetch("http://news.example:80/xhtml-mime")
        assert page8.content_type == "application/xhtml+xml"
    finally:
        await fetcher.close()
        await server.stop()


@pytest.mark.asyncio
async def test_sniffing_oversized_body_fails_body_too_large_first() -> None:
    # A body without Content-Type that is oversized must fail with BODY_TOO_LARGE,
    # even if it starts with non-HTML garbage!
    async def garbage_handler(request: web.Request) -> web.Response:
        return web.Response(
            body=b"not html at all " * 100,  # 1600 bytes
            content_type="",
        )

    app = web.Application()
    app.router.add_get("/garbage-oversized", garbage_handler)
    server = await LocalTestServer(app).start()

    opts = default_options()
    opts.max_body_bytes = 200  # Smaller than garbage body
    fetcher = make_test_fetcher(server, options=opts)

    try:
        stats = ScrapeStats()
        with pytest.raises(ScrapeError) as exc_info:
            await fetcher.fetch_with_stats(
                "http://news.example:80/garbage-oversized", stats=stats
            )
        # BODY_TOO_LARGE must win over UNSUPPORTED_CONTENT
        assert exc_info.value.kind == ScrapeErrorKind.BODY_TOO_LARGE
        assert stats.bytes_read == 0
    finally:
        await fetcher.close()
        await server.stop()
