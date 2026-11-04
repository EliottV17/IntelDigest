"""Synchronous in-memory result handoff and metadata-only receiver (M14)."""

from __future__ import annotations

import logging
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Protocol, runtime_checkable
from uuid import UUID

from inteldigest.scraper.models import (
    Article,
    ScrapeError,
    ScrapeErrorKind,
    ScrapeStage,
)

_ALLOWED_KINDS: frozenset[str] = frozenset(item.value for item in ScrapeErrorKind)
_ALLOWED_STAGES: frozenset[str] = frozenset(item.value for item in ScrapeStage)


def safe_kind(kind: ScrapeErrorKind | str) -> str:
    """Return an allowlisted scrape error kind token or 'unknown'."""
    raw = kind.value if isinstance(kind, ScrapeErrorKind) else str(kind)
    return raw if raw in _ALLOWED_KINDS else "unknown"


def safe_stage(stage: ScrapeStage | str) -> str:
    """Return an allowlisted scrape stage token or 'unknown'."""
    raw = stage.value if isinstance(stage, ScrapeStage) else str(stage)
    return raw if raw in _ALLOWED_STAGES else "unknown"


def safe_status(status: int) -> int:
    """Return an HTTP status code in 100..599 or 0."""
    if isinstance(status, int) and not isinstance(status, bool) and 100 <= status <= 599:
        return status
    return 0


@dataclass(frozen=True, slots=True)
class Result:
    """Synchronous in-memory handoff for one claimed job."""

    job_id: UUID
    article: Article | None = None
    error: ScrapeError | None = None


def valid_job_id(job_id: UUID) -> bool:
    """Return True when job_id is a valid non-nil UUID."""
    return isinstance(job_id, UUID) and job_id.int != 0


def validate_result(result: Result) -> None:
    """Validate that a worker Result has a non-nil job ID and exactly one payload."""
    if not isinstance(result, Result) or not valid_job_id(result.job_id):
        raise ValueError("invalid worker result job ID")
    if (result.article is None) == (result.error is None):
        raise ValueError("worker result must contain exactly one article or scrape error")


@runtime_checkable
class Receiver(Protocol):
    """Synchronous result consumer for claimed jobs."""

    def receive(self, result: Result) -> Awaitable[None] | None: ...


class MetadataReceiver:
    """Metadata-only receiver that logs safe attributes without persisting or exposing content."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger("inteldigest.worker")

    async def receive(self, result: Result) -> None:
        """Validate result and log bounded technical metadata only."""
        validate_result(result)
        job_id_str = str(result.job_id)
        if result.article is not None:
            article = result.article
            text_bytes = len(article.text.encode("utf-8"))
            title_present = bool(article.title)
            language_present = bool(article.language)
            self._logger.info(
                "worker_result_received job_id=%s result=article text_bytes=%d "
                "title_present=%s language_present=%s",
                job_id_str,
                text_bytes,
                str(title_present).lower(),
                str(language_present).lower(),
            )
            return

        err = result.error
        assert err is not None
        self._logger.info(
            "worker_result_received job_id=%s result=scrape_error kind=%s "
            "stage=%s http_status=%d",
            job_id_str,
            safe_kind(err.kind),
            safe_stage(err.stage),
            safe_status(err.http_status),
        )
