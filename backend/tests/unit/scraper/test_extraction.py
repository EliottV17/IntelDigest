"""Unit tests for article extraction, charset decoding, and Scraper pipeline (M13)."""

import asyncio
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from aiohttp import web

from inteldigest.scraper.extraction import (
    ExtractedArticle,
    Scraper,
    decode_html_bytes,
    extract_readability,
)
from inteldigest.scraper.fetcher import (
    ScraperOptions,
    default_options,
)
from inteldigest.scraper.models import (
    Article,
    ScrapeError,
    ScrapeErrorKind,
    ScrapeStage,
    ScrapeStats,
)

FIXTURES_DIR = Path(__file__).resolve().parent.parent.parent / "fixtures" / "scraper"
ARTICLE_HTML_PATH = FIXTURES_DIR / "article.html"
NO_ARTICLE_HTML_PATH = FIXTURES_DIR / "no_article.html"


def read_fixture(name: str) -> bytes:
    path = FIXTURES_DIR / name
    return path.read_bytes()


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
        assert self.site._server is not None
        self.port = self.site._server.sockets[0].getsockname()[1]
        return self

    async def stop(self) -> None:
        if self.runner is not None:
            await self.runner.cleanup()


def make_test_scraper(
    server: LocalTestServer,
    options: ScraperOptions | None = None,
    extractor: Any = None,
    clock: Any = None,
) -> Scraper:
    async def mock_resolver(host: str, port: int) -> list[str]:
        return ["93.184.216.34"]

    return Scraper(
        options=options,
        resolver=mock_resolver,
        test_dial_target=("127.0.0.1", server.port),
        extractor=extractor,
        clock=clock,
    )


# ---------------------------------------------------------------------------
# 1. Charset Decoding Tests
# ---------------------------------------------------------------------------


def test_decode_html_bytes_declared_header_charset() -> None:
    # 0xe9 in ISO-8859-1 is 'é'
    raw = b"<html><body>caf\xe9</body></html>"
    decoded = decode_html_bytes(raw, declared_charset="iso-8859-1")
    assert "café" in decoded


def test_decode_html_bytes_unsupported_declared_charset() -> None:
    raw = b"<html><body>caf\xe9</body></html>"
    with pytest.raises(ScrapeError) as exc_info:
        decode_html_bytes(raw, declared_charset="nonexistent-charset-1234")
    err = exc_info.value
    assert err.kind == ScrapeErrorKind.UNSUPPORTED_CONTENT
    assert err.stage == ScrapeStage.FETCH
    assert str(err) == "scrape unsupported_content during fetch"
    assert "HTML charset is unsupported" in err.message


def test_decode_html_bytes_meta_charset() -> None:
    raw = b'<html><head><meta charset="iso-8859-1"></head><body>caf\xe9</body></html>'
    decoded = decode_html_bytes(raw)
    assert "café" in decoded


def test_decode_html_bytes_meta_http_equiv_charset() -> None:
    raw = (
        b'<html><head><meta http-equiv="Content-Type" '
        b'content="text/html; charset=windows-1252"></head><body>caf\xe9</body></html>'
    )
    decoded = decode_html_bytes(raw)
    assert "café" in decoded


def test_decode_html_bytes_meta_content_type_reversed_order() -> None:
    raw = (
        b'<html><head><meta content="text/html; charset=iso-8859-1" '
        b'http-equiv="content-type"></head><body>caf\xe9</body></html>'
    )
    decoded = decode_html_bytes(raw)
    assert "café" in decoded


def test_decode_html_bytes_unsupported_meta_charset() -> None:
    raw = b'<html><head><meta charset="bogus-charset-xyz"></head><body>test</body></html>'
    with pytest.raises(ScrapeError) as exc_info:
        decode_html_bytes(raw)
    err = exc_info.value
    assert err.kind == ScrapeErrorKind.UNSUPPORTED_CONTENT
    assert err.stage == ScrapeStage.FETCH
    assert str(err) == "scrape unsupported_content during fetch"


def test_decode_html_bytes_utf8_bom() -> None:
    raw = b"\xef\xbb\xbf<html><body>caf\xc3\xa9</body></html>"
    decoded = decode_html_bytes(raw)
    assert "café" in decoded
    assert not decoded.startswith("\ufeff")


def test_decode_html_bytes_utf16_bom() -> None:
    text = "<html><body>café</body></html>"
    raw_le = b"\xff\xfe" + text.encode("utf-16-le")
    raw_be = b"\xfe\xff" + text.encode("utf-16-be")
    assert "café" in decode_html_bytes(raw_le)
    assert "café" in decode_html_bytes(raw_be)


def test_decode_html_bytes_default_utf8_replacement() -> None:
    # 0xff is invalid in UTF-8; should be replaced without raising
    raw = b"<html><body>invalid \xff byte</body></html>"
    decoded = decode_html_bytes(raw)
    assert "invalid" in decoded
    assert "\ufffd" in decoded


def test_decode_html_bytes_expansion_beyond_wire_limit() -> None:
    # Single-byte 0xe9 expands to 2-byte UTF-8; decoding produces valid Python string
    body = b"\xe9" * 1000
    decoded = decode_html_bytes(body, declared_charset="iso-8859-1")
    assert len(decoded) == 1000
    assert decoded == "é" * 1000
    assert len(decoded.encode("utf-8")) == 2000


def test_decode_html_bytes_uppercase_meta_charset() -> None:
    raw = b'<html><head><META CHARSET="ISO-8859-1"></head><body>caf\xe9</body></html>'
    decoded = decode_html_bytes(raw)
    assert "café" in decoded


def test_decode_html_bytes_unquoted_meta_charset() -> None:
    raw = b"<html><head><meta charset=iso-8859-1></head><body>caf\xe9</body></html>"
    decoded = decode_html_bytes(raw)
    assert "café" in decoded


# ---------------------------------------------------------------------------
# 2. Readability Extraction Tests
# ---------------------------------------------------------------------------


def test_extract_readability_article_fixture() -> None:
    fixture_bytes = read_fixture("article.html")
    html_text = fixture_bytes.decode("utf-8")
    extracted = extract_readability(html_text, "http://news.example/article")

    assert isinstance(extracted, ExtractedArticle)
    assert extracted.title == "Harbor Rescue Report"
    assert extracted.language == "en"

    # Must contain key article content
    assert "twelve sailors" in extracted.text
    assert "engine stopped" in extracted.text

    # Must exclude navigation, ads, and scripts
    assert "Subscribe" not in extracted.text
    assert "Limited time offer" not in extracted.text
    assert "must not appear" not in extracted.text


def test_extract_readability_no_article_fixture() -> None:
    fixture_bytes = read_fixture("no_article.html")
    html_text = fixture_bytes.decode("utf-8")
    extracted = extract_readability(html_text, "http://news.example/no-article")
    assert extracted.text.strip() == ""


def test_extract_readability_title_and_language_fallbacks() -> None:
    html_with_h1 = """
    <html xml:lang="es">
    <body>
      <article>
        <h1>Titulo del Articulo</h1>
        <p>Este es el contenido principal del articulo periodistico.</p>
      </article>
    </body>
    </html>
    """
    extracted = extract_readability(html_with_h1)
    assert extracted.language == "es"
    assert "Titulo del Articulo" in extracted.title or "Titulo del Articulo" in extracted.text
    assert "contenido principal" in extracted.text


def test_extract_readability_empty_input() -> None:
    extracted = extract_readability("")
    assert extracted.text == ""
    assert extracted.title == ""
    assert extracted.language == ""


def test_extract_readability_wraps_unexpected_exception() -> None:
    # Malformed unparseable structure that triggers parser error
    with pytest.raises(ScrapeError) as exc_info:
        # None or invalid type causes internal error
        extract_readability(None)  # type: ignore[arg-type]
    err = exc_info.value
    assert err.kind == ScrapeErrorKind.EXTRACTION
    assert err.stage == ScrapeStage.EXTRACT
    assert str(err) == "scrape extraction during extract"
    assert "article extraction failed" in err.message


def test_extract_readability_collapses_whitespace_and_excessive_blank_lines() -> None:
    html_raw = """
    <html>
    <body>
      <article>
        <h1>Article   Title</h1>
        <p>First   paragraph   with   extra    spaces.</p>
        <br><br><br>
        <p>Second paragraph after breaks.</p>
      </article>
    </body>
    </html>
    """
    extracted = extract_readability(html_raw)
    assert "First paragraph with extra spaces." in extracted.text
    assert "Second paragraph after breaks." in extracted.text
    assert "\n\n\n" not in extracted.text


# ---------------------------------------------------------------------------
# 3. End-to-End Scraper Pipeline Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scraper_extracts_article_with_stats() -> None:
    fixture_bytes = read_fixture("article.html")

    async def handle_article(request: web.Request) -> web.Response:
        assert request.headers.get("Authorization") is None
        assert request.headers.get("Cookie") is None
        return web.Response(
            body=fixture_bytes,
            content_type="text/html",
            charset="utf-8",
        )

    app = web.Application()
    app.router.add_get("/article", handle_article)
    server = await LocalTestServer(app).start()

    fixed_time = datetime(2025, 2, 3, 4, 5, 6, tzinfo=UTC)
    options = default_options()

    try:
        scraper = make_test_scraper(
            server,
            options=options,
            clock=lambda: fixed_time,
        )
        async with scraper:
            stats = ScrapeStats()
            article, ret_stats = await scraper.scrape_with_stats(
                "http://news.example/article", stats=stats
            )

            assert isinstance(article, Article)
            assert article.requested_url == "http://news.example/article"
            assert article.final_url == "http://news.example/article"
            assert article.title == "Harbor Rescue Report"
            assert article.language == "en"
            assert "twelve sailors" in article.text
            assert "engine stopped" in article.text
            assert "Subscribe" not in article.text
            assert article.fetched_at == fixed_time
            assert article.fetched_at.tzinfo == UTC

            assert ret_stats is stats
            assert stats.bytes_read == len(fixture_bytes)
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_scraper_rejects_empty_readability_result() -> None:
    fixture_bytes = read_fixture("no_article.html")

    async def handle_no_article(request: web.Request) -> web.Response:
        return web.Response(
            body=fixture_bytes,
            content_type="text/html",
        )

    app = web.Application()
    app.router.add_get("/no-article", handle_no_article)
    server = await LocalTestServer(app).start()

    try:
        scraper = make_test_scraper(server)
        async with scraper:
            stats = ScrapeStats()
            with pytest.raises(ScrapeError) as exc_info:
                await scraper.scrape_with_stats(
                    "http://news.example/no-article", stats=stats
                )
            err = exc_info.value
            assert err.kind == ScrapeErrorKind.EXTRACTION
            assert err.stage == ScrapeStage.EXTRACT
            assert str(err) == "scrape extraction during extract"
            assert "page contains no useful article text" in err.message

            # Decompressed wire body bytes retained even on extraction failure!
            assert stats.bytes_read == len(fixture_bytes)
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_scraper_injected_extractor_cause_is_wrapped_safely() -> None:
    async def handle_article(request: web.Request) -> web.Response:
        return web.Response(
            text="<html><body>bounded input</body></html>",
            content_type="text/html",
        )

    app = web.Application()
    app.router.add_get("/extract", handle_article)
    server = await LocalTestServer(app).start()

    cause = RuntimeError("parser internal URL https://secret.example/?token=private")

    def failing_extractor(html_text: str, page_url: str = "") -> ExtractedArticle:
        raise cause

    try:
        scraper = make_test_scraper(server, extractor=failing_extractor)
        async with scraper:
            with pytest.raises(ScrapeError) as exc_info:
                await scraper.scrape("http://news.example/extract")
            err = exc_info.value
            assert err.kind == ScrapeErrorKind.EXTRACTION
            assert err.stage == ScrapeStage.EXTRACT
            assert str(err) == "scrape extraction during extract"
            assert "secret.example" not in str(err)
            assert err.cause is cause
            assert err.__cause__ is cause
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_scraper_injected_async_extractor() -> None:
    async def handle_article(request: web.Request) -> web.Response:
        return web.Response(
            text="<html><body>custom async content</body></html>",
            content_type="text/html",
        )

    app = web.Application()
    app.router.add_get("/async-article", handle_article)
    server = await LocalTestServer(app).start()

    async def custom_async_extractor(
        html_text: str, page_url: str = ""
    ) -> ExtractedArticle:
        await asyncio.sleep(0.01)
        return ExtractedArticle(
            text="custom async text",
            title="Custom Async Title",
            language="en",
        )

    try:
        scraper = make_test_scraper(server, extractor=custom_async_extractor)
        async with scraper:
            article = await scraper.scrape("http://news.example/async-article")
            assert article.text == "custom async text"
            assert article.title == "Custom Async Title"
            assert article.language == "en"
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_scraper_counts_original_charset_bytes_and_retains_after_failure() -> None:
    # 0xe9 in ISO-8859-1 -> café
    body = b"<html><body><article>caf" + b"\xe9" + b"</article></body></html>"

    async def handle_latin1(request: web.Request) -> web.Response:
        return web.Response(
            body=body,
            content_type="text/html",
            charset="iso-8859-1",
        )

    app = web.Application()
    app.router.add_get("/latin1-stats", handle_latin1)
    server = await LocalTestServer(app).start()

    cause = ValueError("custom extractor failure")

    def verifying_extractor(html_text: str, page_url: str = "") -> ExtractedArticle:
        assert "café" in html_text
        raise cause

    try:
        scraper = make_test_scraper(server, extractor=verifying_extractor)
        async with scraper:
            stats = ScrapeStats()
            with pytest.raises(ScrapeError) as exc_info:
                await scraper.scrape_with_stats(
                    "http://news.example/latin1-stats", stats=stats
                )
            err = exc_info.value
            assert err.kind == ScrapeErrorKind.EXTRACTION
            assert err.stage == ScrapeStage.EXTRACT
            assert err.cause is cause
            # Retains wire body byte count
            assert stats.bytes_read == len(body)
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_scraper_stats_zero_before_complete_body() -> None:
    scraper = Scraper()
    async with scraper:
        stats = ScrapeStats()
        with pytest.raises(ScrapeError) as exc_info:
            await scraper.scrape_with_stats("not a valid url", stats=stats)
        assert exc_info.value.kind == ScrapeErrorKind.SECURITY_REJECTED
        assert stats.bytes_read == 0


@pytest.mark.asyncio
async def test_scraper_extraction_timeout_uses_fetch_stage() -> None:
    async def handle_article(request: web.Request) -> web.Response:
        return web.Response(
            text="<html><body>timeout test</body></html>",
            content_type="text/html",
        )

    app = web.Application()
    app.router.add_get("/timeout", handle_article)
    server = await LocalTestServer(app).start()

    async def slow_extractor(html_text: str, page_url: str = "") -> ExtractedArticle:
        await asyncio.sleep(2.0)
        return ExtractedArticle(text="too late", title="Late", language="en")

    options = ScraperOptions(
        connect_timeout_seconds=5.0,
        tls_timeout_seconds=5.0,
        header_timeout_seconds=5.0,
        read_timeout_seconds=5.0,
        request_timeout_seconds=0.1,  # very short total budget
    )

    try:
        scraper = make_test_scraper(
            server,
            options=options,
            extractor=slow_extractor,
        )
        async with scraper:
            with pytest.raises(ScrapeError) as exc_info:
                await scraper.scrape("http://news.example/timeout")
            err = exc_info.value
            assert err.kind == ScrapeErrorKind.TIMEOUT
            assert err.stage == ScrapeStage.FETCH
            assert str(err) == "scrape timeout during fetch"
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_scraper_extraction_cancellation_uses_fetch_stage() -> None:
    async def handle_article(request: web.Request) -> web.Response:
        return web.Response(
            text="<html><body>cancel test</body></html>",
            content_type="text/html",
        )

    app = web.Application()
    app.router.add_get("/cancel", handle_article)
    server = await LocalTestServer(app).start()

    async def canceling_extractor(
        html_text: str, page_url: str = ""
    ) -> ExtractedArticle:
        raise asyncio.CancelledError()

    try:
        scraper = make_test_scraper(server, extractor=canceling_extractor)
        async with scraper:
            with pytest.raises(ScrapeError) as exc_info:
                await scraper.scrape("http://news.example/cancel")
            err = exc_info.value
            assert err.kind == ScrapeErrorKind.CANCELED
            assert err.stage == ScrapeStage.FETCH
            assert str(err) == "scrape canceled during fetch"
    finally:
        await server.stop()


def test_scraper_options_validation_on_init() -> None:
    invalid_options = ScraperOptions(request_timeout_seconds=-1.0)
    with pytest.raises(ValueError, match="all time limits must be positive"):
        Scraper(invalid_options)


@pytest.mark.asyncio
async def test_scraper_convenience_scrape_method() -> None:
    fixture_bytes = read_fixture("article.html")

    async def handle_article(request: web.Request) -> web.Response:
        return web.Response(
            body=fixture_bytes,
            content_type="text/html",
        )

    app = web.Application()
    app.router.add_get("/article-direct", handle_article)
    server = await LocalTestServer(app).start()

    try:
        scraper = make_test_scraper(server)
        article = await scraper.scrape("http://news.example/article-direct")
        assert article.title == "Harbor Rescue Report"
        assert "twelve sailors" in article.text
        await scraper.aclose()
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_scraper_normalizes_timezone_aware_non_utc_clock() -> None:
    offset_tz = timezone(timedelta(hours=5))
    offset_dt = datetime(2025, 6, 1, 12, 0, 0, tzinfo=offset_tz)
    fixture_bytes = read_fixture("article.html")

    async def handle_article(request: web.Request) -> web.Response:
        return web.Response(body=fixture_bytes, content_type="text/html")

    app = web.Application()
    app.router.add_get("/tz-article", handle_article)
    server = await LocalTestServer(app).start()

    try:
        scraper = make_test_scraper(server, clock=lambda: offset_dt)
        async with scraper:
            article = await scraper.scrape("http://news.example/tz-article")
            assert article.fetched_at.tzinfo == UTC
            assert article.fetched_at == offset_dt.astimezone(UTC)
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_scraper_retains_bytes_read_when_meta_charset_unsupported() -> None:
    body = b'<html><head><meta charset="invalid-unknown-codec-999"></head><body>bad</body></html>'

    async def handle_bad_charset(request: web.Request) -> web.Response:
        return web.Response(body=body, content_type="text/html")

    app = web.Application()
    app.router.add_get("/bad-charset", handle_bad_charset)
    server = await LocalTestServer(app).start()

    try:
        scraper = make_test_scraper(server)
        async with scraper:
            stats = ScrapeStats()
            with pytest.raises(ScrapeError) as exc_info:
                await scraper.scrape_with_stats("http://news.example/bad-charset", stats=stats)
            err = exc_info.value
            assert err.kind == ScrapeErrorKind.UNSUPPORTED_CONTENT
            assert err.stage == ScrapeStage.FETCH
            assert stats.bytes_read == len(body)
    finally:
        await server.stop()

