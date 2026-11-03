"""Unit tests for scraper models and safe error formatting (M10)."""

from datetime import UTC, datetime

import pytest

from inteldigest.scraper.models import (
    Article,
    ScrapeError,
    ScrapeErrorKind,
    ScrapeStage,
    ScrapeStats,
)


def test_article_and_stats_models() -> None:
    now = datetime(2026, 11, 2, 12, 0, 0, tzinfo=UTC)
    article = Article(
        text="Paragraph one.\n\nParagraph two.",
        requested_url="https://example.com/start",
        final_url="https://example.com/final",
        title="Example Title",
        language="en",
        fetched_at=now,
    )
    assert article.text == "Paragraph one.\n\nParagraph two."
    assert article.requested_url == "https://example.com/start"
    assert article.final_url == "https://example.com/final"
    assert article.title == "Example Title"
    assert article.language == "en"
    assert article.fetched_at == now

    stats_default = ScrapeStats()
    assert stats_default.bytes_read == 0

    stats = ScrapeStats(bytes_read=4096)
    assert stats.bytes_read == 4096


def test_scrape_error_safe_string_without_http_status() -> None:
    cause = RuntimeError("raw URL https://private.example/path?token=hidden")
    err = ScrapeError(
        kind=ScrapeErrorKind.NETWORK,
        stage=ScrapeStage.FETCH,
        message="connection failed to https://private.example/path?token=hidden",
        cause=cause,
    )
    rendered = str(err)
    assert rendered == "scrape network during fetch"
    assert "private.example" not in rendered
    assert "token=hidden" not in rendered
    assert err.__cause__ is cause
    assert err.cause is cause


def test_scrape_error_safe_string_with_valid_http_status() -> None:
    err = ScrapeError(
        kind=ScrapeErrorKind.HTTP_STATUS,
        stage=ScrapeStage.FETCH,
        http_status=404,
        message="HTTP response status was not accepted",
    )
    assert str(err) == "scrape http_status during fetch (HTTP 404)"


@pytest.mark.parametrize("invalid_status", [0, 99, 600, -1])
def test_scrape_error_omits_out_of_range_http_status(invalid_status: int) -> None:
    err = ScrapeError(
        kind=ScrapeErrorKind.HTTP_STATUS,
        stage=ScrapeStage.FETCH,
        http_status=invalid_status,
        message="status out of range",
    )
    assert str(err) == "scrape http_status during fetch"


def test_scrape_error_unknown_fields_cannot_inject_unsafe_text() -> None:
    err = ScrapeError(
        kind="https://private.example/?token=raw",  # type: ignore[arg-type]
        stage="url=https://private.example",  # type: ignore[arg-type]
        message="private URL https://private.example/?token=raw",
    )
    rendered = str(err)
    assert rendered == "scrape unknown during unknown"
    assert "private.example" not in rendered
    assert "token=raw" not in rendered
