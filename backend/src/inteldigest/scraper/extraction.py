"""Article readability extraction, charset decoding, and end-to-end scraper pipeline (M13)."""

import asyncio
import codecs
import contextlib
import inspect
import re
import ssl
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Protocol, Self, runtime_checkable

import aiohttp
import readability
from lxml import html
from lxml.etree import ParserError

from inteldigest.scraper.fetcher import (
    Fetcher,
    ScraperOptions,
    default_options,
    validate_scraper_options,
)
from inteldigest.scraper.models import (
    Article,
    ScrapeError,
    ScrapeErrorKind,
    ScrapeStage,
    ScrapeStats,
)
from inteldigest.scraper.network import AddressResolver

# Regular expressions for HTML meta charset declarations
_META_CHARSET_RE = re.compile(
    rb"<meta\s+[^>]*?charset\s*=\s*[\"']?\s*([a-zA-Z0-9_\-]+)",
    re.IGNORECASE,
)
_META_HTTP_EQUIV_RE1 = re.compile(
    rb"<meta\s+[^>]*?http-equiv\s*=\s*[\"']?\s*content-type\s*[\"']?[^>]*?content\s*=\s*[\"']?[^>\"']*?charset\s*=\s*([a-zA-Z0-9_\-]+)",
    re.IGNORECASE,
)
_META_HTTP_EQUIV_RE2 = re.compile(
    rb"<meta\s+[^>]*?content\s*=\s*[\"']?[^>\"']*?charset\s*=\s*([a-zA-Z0-9_\-]+)[^>]*?http-equiv\s*=\s*[\"']?\s*content-type",
    re.IGNORECASE,
)

_BLOCK_TAGS = frozenset({
    "address",
    "article",
    "aside",
    "blockquote",
    "caption",
    "dd",
    "div",
    "dl",
    "dt",
    "figure",
    "figcaption",
    "footer",
    "form",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "hr",
    "li",
    "main",
    "nav",
    "ol",
    "p",
    "pre",
    "section",
    "table",
    "tbody",
    "td",
    "tfoot",
    "th",
    "thead",
    "tr",
    "ul",
})


def decode_html_bytes(body: bytes, declared_charset: str | None = None) -> str:
    """Decode raw wire HTML bytes into a Unicode string.

    Charset decoding occurs strictly *after* body wire limits are enforced,
    so multi-byte expansions from single-byte encodings (such as ISO-8859-1)
    reach the extractor safely without failing body size limits.
    """
    if declared_charset is not None and declared_charset.strip():
        candidate = declared_charset.strip()
        try:
            codec_info = codecs.lookup(candidate)
        except LookupError as exc:
            raise ScrapeError(
                ScrapeErrorKind.UNSUPPORTED_CONTENT,
                ScrapeStage.FETCH,
                "HTML charset is unsupported",
                cause=exc,
            ) from exc

        encoding_name = codec_info.name
        if encoding_name == "utf-8" and body.startswith(b"\xef\xbb\xbf"):
            return body[3:].decode("utf-8", errors="replace")
        return body.decode(encoding_name, errors="replace")

    # Inspect BOM
    if body.startswith(b"\xef\xbb\xbf"):
        return body[3:].decode("utf-8", errors="replace")
    if body.startswith(b"\xff\xfe"):
        return body[2:].decode("utf-16-le", errors="replace")
    if body.startswith(b"\xfe\xff"):
        return body[2:].decode("utf-16-be", errors="replace")

    # Inspect first 1024 bytes for HTML <meta> charset declarations
    prefix = body[:1024]
    match = (
        _META_CHARSET_RE.search(prefix)
        or _META_HTTP_EQUIV_RE1.search(prefix)
        or _META_HTTP_EQUIV_RE2.search(prefix)
    )
    if match:
        meta_charset = match.group(1).decode("ascii", errors="replace").strip()
        try:
            codec_info = codecs.lookup(meta_charset)
        except LookupError as exc:
            raise ScrapeError(
                ScrapeErrorKind.UNSUPPORTED_CONTENT,
                ScrapeStage.FETCH,
                "HTML charset is unsupported",
                cause=exc,
            ) from exc
        return body.decode(codec_info.name, errors="replace")

    # Default to UTF-8 with character replacement
    return body.decode("utf-8", errors="replace")


@dataclass(frozen=True, slots=True)
class ExtractedArticle:
    """Intermediate article extraction result before final Article construction."""

    text: str
    title: str
    language: str


@runtime_checkable
class ArticleExtractor(Protocol):
    """Protocol for synchronous or asynchronous article extractors."""

    def __call__(
        self, html_text: str, page_url: str = ...
    ) -> ExtractedArticle | Awaitable[ExtractedArticle]: ...


def _clean_html_to_text(elem: html.HtmlElement) -> str:
    """Convert an lxml HTML element tree into clean, paragraph-separated plain text."""
    # Remove script, style, and noscript tags
    for bad in elem.xpath(".//script | .//style | .//noscript"):
        parent = bad.getparent()
        if parent is not None:
            parent.remove(bad)

    # Convert <br> tags into line breaks
    for br in elem.xpath(".//br"):
        br.tail = "\n" + (br.tail or "")

    # Ensure block elements end with paragraph separators
    for el in elem.iter():
        if el.tag in _BLOCK_TAGS:
            el.tail = "\n\n" + (el.tail or "")

    raw = elem.text_content()
    # Normalize whitespace per line
    lines = [re.sub(r"[^\S\n]+", " ", line).strip() for line in raw.splitlines()]

    # Collapse consecutive empty lines into a single blank line
    result_lines: list[str] = []
    for line in lines:
        if line:
            result_lines.append(line)
        elif result_lines and result_lines[-1] != "":
            result_lines.append("")

    return "\n".join(result_lines).strip()


def extract_readability(
    html_text: str, page_url: str = ""
) -> ExtractedArticle:
    """Extract clean article text, title, and language using readability-lxml and lxml."""
    if not isinstance(html_text, str) or not html_text.strip():
        if not isinstance(html_text, str):
            raise ScrapeError(
                ScrapeErrorKind.EXTRACTION,
                ScrapeStage.EXTRACT,
                "article extraction failed",
                cause=TypeError(f"expected str html_text, got {type(html_text).__name__}"),
            )
        return ExtractedArticle(text="", title="", language="")

    try:
        doc = readability.Document(html_text, url=page_url or None)
        summary_html = doc.summary(html_partial=True)
        summary_elem = html.fromstring(summary_html)
        text = _clean_html_to_text(summary_elem)
    except Exception as exc:
        raise ScrapeError(
            ScrapeErrorKind.EXTRACTION,
            ScrapeStage.EXTRACT,
            "article extraction failed",
            cause=exc,
        ) from exc

    # Title extraction
    title = ""
    for candidate_fn in (doc.short_title, doc.title):
        with contextlib.suppress(AttributeError, TypeError, ValueError):
            candidate = candidate_fn()
            if candidate and candidate.strip():
                title = candidate.strip()
                break

    # Language and fallback title extraction from original document tree
    language = ""
    with contextlib.suppress(AttributeError, TypeError, ValueError, ParserError):
        tree = html.fromstring(html_text)
        if not title:
            title_tags = tree.xpath(".//title")
            if title_tags and title_tags[0].text_content().strip():
                title = title_tags[0].text_content().strip()
            else:
                h1_tags = tree.xpath(".//h1")
                if h1_tags and h1_tags[0].text_content().strip():
                    title = h1_tags[0].text_content().strip()

        for elem in [tree] + tree.xpath("//html"):
            lang = (
                elem.get("lang")
                or elem.get("{http://www.w3.org/XML/1998/namespace}lang")
                or elem.get("xml:lang")
            )
            if lang and lang.strip():
                language = lang.strip()
                break

    return ExtractedArticle(
        text=text,
        title=title,
        language=language,
    )


def _is_async_callable(fn: Any) -> bool:
    if inspect.iscoroutinefunction(fn):
        return True
    if callable(fn):
        return inspect.iscoroutinefunction(fn.__call__)
    return False


def _invoke_extractor(extractor: Any, html_text: str, page_url: str) -> Any:
    try:
        sig = inspect.signature(extractor)
        params = list(sig.parameters.values())
        if len(params) == 1:
            return extractor(html_text)
    except (ValueError, TypeError):
        pass
    return extractor(html_text, page_url)


async def _run_extractor(
    extractor: Any, html_text: str, page_url: str
) -> ExtractedArticle:
    if _is_async_callable(extractor):
        res = _invoke_extractor(extractor, html_text, page_url)
        if asyncio.iscoroutine(res):
            return await res
        return res
    res = await asyncio.to_thread(_invoke_extractor, extractor, html_text, page_url)
    if asyncio.iscoroutine(res):
        return await res
    return res


class Scraper:
    """Safe public article scraper combining bounded fetching and article extraction."""

    def __init__(
        self,
        options: ScraperOptions | None = None,
        *,
        resolver: AddressResolver | Callable[..., Any] | None = None,
        ssl_context: ssl.SSLContext | None = None,
        test_dial_target: tuple[str, int] | None = None,
        connector: aiohttp.TCPConnector | None = None,
        session: aiohttp.ClientSession | None = None,
        extractor: ArticleExtractor | Callable[..., Any] | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if options is None:
            options = default_options()
        validate_scraper_options(options)
        self._options = options
        self._extractor = extractor or extract_readability
        self._clock = clock or (lambda: datetime.now(UTC))
        self._fetcher = Fetcher(
            options=options,
            resolver=resolver,
            ssl_context=ssl_context,
            test_dial_target=test_dial_target,
            connector=connector,
            session=session,
            clock=self._clock,
        )

    async def close(self) -> None:
        """Close owned fetcher and release network resources."""
        await self._fetcher.close()

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

    async def scrape(self, raw_url: str) -> Article:
        """Fetch, decode, and extract an article from a public URL."""
        article, _ = await self.scrape_with_stats(raw_url)
        return article

    async def scrape_with_stats(
        self,
        raw_url: str,
        *,
        stats: ScrapeStats | None = None,
    ) -> tuple[Article, ScrapeStats]:
        """Fetch and extract article, updating caller-supplied stats."""
        if stats is None:
            stats = ScrapeStats()

        start_time = time.monotonic()
        total_budget = self._options.request_timeout_seconds
        global_deadline = start_time + total_budget

        page, stats = await self._fetcher.fetch_with_stats(raw_url, stats=stats)

        # Charset decoding strictly after wire/decompressed body limits are enforced
        decoded_html = decode_html_bytes(page.body, page.charset)

        # Remaining budget for extraction stage
        now = time.monotonic()
        remaining = global_deadline - now
        if remaining <= 0:
            raise ScrapeError(
                ScrapeErrorKind.TIMEOUT,
                ScrapeStage.FETCH,
                "scrape request timed out",
                cause=TimeoutError("global request timeout expired"),
            )

        try:
            async with asyncio.timeout(remaining):
                extracted = await _run_extractor(
                    self._extractor, decoded_html, page.final_url
                )
        except TimeoutError as exc:
            raise ScrapeError(
                ScrapeErrorKind.TIMEOUT,
                ScrapeStage.FETCH,
                "scrape request timed out",
                cause=exc,
            ) from exc
        except asyncio.CancelledError as exc:
            raise ScrapeError(
                ScrapeErrorKind.CANCELED,
                ScrapeStage.FETCH,
                "scrape request was canceled",
                cause=exc,
            ) from exc
        except ScrapeError:
            raise
        except Exception as exc:
            raise ScrapeError(
                ScrapeErrorKind.EXTRACTION,
                ScrapeStage.EXTRACT,
                "article extraction failed",
                cause=exc,
            ) from exc

        text = extracted.text.strip()
        if not text:
            raise ScrapeError(
                ScrapeErrorKind.EXTRACTION,
                ScrapeStage.EXTRACT,
                "page contains no useful article text",
            )

        fetched_at = page.fetched_at
        if fetched_at.tzinfo is not None:
            fetched_at = fetched_at.astimezone(UTC)
        else:
            fetched_at = fetched_at.replace(tzinfo=UTC)

        article = Article(
            text=text,
            requested_url=raw_url,
            final_url=page.final_url,
            title=extracted.title.strip(),
            language=extracted.language.strip(),
            fetched_at=fetched_at,
        )
        return article, stats
