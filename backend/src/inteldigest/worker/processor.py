"""Worker job processor: atomic claims, duplicate handling, and ACK rules (M14)."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import json
import logging
import math
import ssl
import time
from collections.abc import Awaitable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Final, Protocol, runtime_checkable
from uuid import UUID

import aiohttp

from inteldigest.jobs.models import Job
from inteldigest.queue.consumer import Delivery
from inteldigest.queue.message import Message
from inteldigest.scraper.models import (
    Article,
    ScrapeError,
    ScrapeErrorKind,
    ScrapeStage,
    ScrapeStats,
)
from inteldigest.worker.receiver import (
    Receiver,
    Result,
    safe_kind,
    safe_stage,
    safe_status,
    validate_result,
)

STATUS_PENDING: Final[str] = "pending"
STATUS_PROCESSING: Final[str] = "processing"
STATUS_COMPLETED: Final[str] = "completed"
STATUS_FAILED: Final[str] = "failed"
_TERMINAL_STATUSES: Final[frozenset[str]] = frozenset({STATUS_COMPLETED, STATUS_FAILED})


class Outcome(StrEnum):
    """Per-delivery processing outcome."""

    SKIPPED = "skipped"
    PROCESSING_DUPLICATE = "processing_duplicate"
    TERMINAL_DUPLICATE = "terminal_duplicate"
    SUCCEEDED = "succeeded"
    SCRAPE_FAILED = "scrape_failed"
    OPERATIONAL_CANCELED = "operational_canceled"
    ERROR = "error"


class ProcessError(Exception):
    """Safe static worker processing error retaining the underlying cause."""

    def __init__(self, code: str, cause: BaseException | None = None) -> None:
        self.code = code
        self.cause = cause
        super().__init__(f"worker processing failed: {code}")
        if cause is not None:
            self.__cause__ = cause

    def __str__(self) -> str:
        return f"worker processing failed: {self.code}"


@runtime_checkable
class JobRepo(Protocol):
    """Minimal persistence boundary required by a single processor."""

    async def get_job_by_id(self, job_id: UUID) -> Job: ...

    async def try_mark_processing(self, job_id: UUID) -> bool: ...

    async def record_scrape_error(self, job_id: UUID, safe_reason: str) -> bool: ...


@runtime_checkable
class Acker(Protocol):
    """Queue acknowledgement boundary required by a single processor."""

    async def ack(self, entry_id: str) -> None: ...


@runtime_checkable
class ArticleScraper(Protocol):
    """Scraper boundary required by a single processor."""

    async def scrape_with_stats(
        self,
        raw_url: str,
        *,
        stats: ScrapeStats | None = None,
    ) -> tuple[Article, ScrapeStats]: ...


@dataclass(frozen=True, slots=True)
class ProcessorOptions:
    """Per-operation timeout budgets and logger for Processor."""

    db_timeout_seconds: float = 5.0
    queue_operation_timeout_seconds: float = 5.0
    logger: logging.Logger | None = None


def decode_delivery(delivery: Delivery) -> Message:
    """Decode and validate a Redis Stream delivery's v1 JSON payload."""
    try:
        fields = delivery.fields
    except AttributeError as exc:
        raise ValueError("missing payload") from exc
    if not isinstance(fields, Mapping):
        raise TypeError("missing payload")
    raw = fields.get("data")
    if not isinstance(raw, str) or not raw:
        raise ValueError("invalid payload field")
    try:
        parsed = json.loads(raw)
    except ValueError as exc:
        raise ValueError("invalid payload object") from exc
    if not isinstance(parsed, dict):
        raise TypeError("invalid payload object")
    try:
        return Message.model_validate(parsed)
    except (ValueError, TypeError) as exc:
        raise ValueError("invalid payload") from exc


def _valid_stored_job(job: Job | None, job_id: UUID, url: str) -> bool:
    return (
        isinstance(job, Job)
        and job.id == job_id
        and job.url == url
    )


def _iter_causes(exc: BaseException | None) -> list[BaseException]:
    seen: set[int] = set()
    chain: list[BaseException] = []
    curr = exc
    while curr is not None and id(curr) not in seen:
        seen.add(id(curr))
        chain.append(curr)
        next_exc = getattr(curr, "cause", None) or curr.__cause__ or curr.__context__
        curr = next_exc if isinstance(next_exc, BaseException) else None
    return chain


def _is_certificate_error(exc: BaseException | None) -> bool:
    for item in _iter_causes(exc):
        if isinstance(
            item,
            (
                ssl.SSLCertVerificationError,
                aiohttp.ClientConnectorCertificateError,
            ),
        ):
            return True
    return False


def _is_network_error(exc: BaseException | None) -> bool:
    for item in _iter_causes(exc):
        if isinstance(item, (OSError, aiohttp.ClientError, TimeoutError)):
            return True
    return False


def safe_scrape_error(err: BaseException) -> ScrapeError:
    """Coerce an arbitrary scraper exception into a safe ScrapeError."""
    for item in _iter_causes(err):
        if isinstance(item, ScrapeError):
            return ScrapeError(
                kind=item.kind,
                stage=item.stage,
                message= str(item),
                http_status=item.http_status,
                cause=item,
            )

    kind = ScrapeErrorKind.NETWORK
    stage = ScrapeStage.FETCH
    for item in _iter_causes(err):
        if isinstance(item, TimeoutError):
            kind = ScrapeErrorKind.TIMEOUT
            break

    return ScrapeError(
        kind=kind,
        stage=stage,
        message="scrape failed",
        cause=err,
    )


def diagnostic_reason(err: ScrapeError) -> str:
    """Render the bounded safe diagnostic string persisted via record_scrape_error."""
    status = safe_status(err.http_status)
    kind_str = safe_kind(err.kind)
    if status != 0:
        return f"scraping/{kind_str}: HTTP {status}"
    stage_str = safe_stage(err.stage)
    return f"scraping/{kind_str} during {stage_str}"


def classify_scrape_error(err: ScrapeError, cause: BaseException | None = None) -> str:
    """Classify a ScrapeError into 'permanent', 'transient', or 'unknown'."""
    effective_cause = cause if cause is not None else (err.cause or err.__cause__)
    if _is_certificate_error(effective_cause):
        return "permanent"

    kind_str = safe_kind(err.kind)
    if kind_str in {
        ScrapeErrorKind.SECURITY_REJECTED.value,
        ScrapeErrorKind.BODY_TOO_LARGE.value,
        ScrapeErrorKind.UNSUPPORTED_CONTENT.value,
        ScrapeErrorKind.EXTRACTION.value,
    }:
        return "permanent"
    if kind_str in {
        ScrapeErrorKind.TIMEOUT.value,
        ScrapeErrorKind.NETWORK.value,
    }:
        return "transient"
    if kind_str == ScrapeErrorKind.HTTP_STATUS.value:
        status = safe_status(err.http_status)
        if status in {408, 429} or status >= 500:
            return "transient"
        if 400 <= status < 500:
            return "permanent"

    if _is_network_error(effective_cause):
        return "transient"
    return "unknown"


async def _await_with_parents(
    coro: Awaitable[Any],
    *,
    timeout_seconds: float,
    job_canceled: asyncio.Event | None,
    admission_closed: asyncio.Event | None = None,
) -> Any:
    """Await a coroutine bounded by timeout_seconds and parent cancellation events."""
    task = asyncio.ensure_future(coro)
    watchers: list[asyncio.Task[Any]] = []
    try:
        for ev in (job_canceled, admission_closed):
            if ev is not None:
                if ev.is_set():
                    task.cancel()
                    with contextlib.suppress(BaseException):
                        await task
                    raise asyncio.CancelledError()
                watchers.append(asyncio.create_task(ev.wait()))

        if not watchers:
            async with asyncio.timeout(timeout_seconds):
                return await task

        async with asyncio.timeout(timeout_seconds):
            done, _ = await asyncio.wait(
                [task, *watchers],
                return_when=asyncio.FIRST_COMPLETED,
            )
            if task in done:
                return task.result()
            task.cancel()
            with contextlib.suppress(BaseException):
                await task
            raise asyncio.CancelledError()
    finally:
        for w in watchers:
            if not w.done():
                w.cancel()
        if not task.done():
            task.cancel()
            with contextlib.suppress(BaseException):
                await task


async def _await_job_operation(
    coro: Awaitable[Any],
    *,
    job_canceled: asyncio.Event | None,
    timeout_seconds: float | None = None,
) -> Any:
    """Await a post-claim operation bounded only by job_canceled (ignoring admission_closed)."""
    if job_canceled is None and timeout_seconds is None:
        return await coro

    task = asyncio.ensure_future(coro)
    watcher: asyncio.Task[Any] | None = None
    try:
        if job_canceled is not None:
            if job_canceled.is_set():
                task.cancel()
                with contextlib.suppress(BaseException):
                    await task
                raise asyncio.CancelledError()
            watcher = asyncio.create_task(job_canceled.wait())

        wait_set: list[asyncio.Future[Any]] = [task]
        if watcher is not None:
            wait_set.append(watcher)

        if timeout_seconds is not None:
            async with asyncio.timeout(timeout_seconds):
                done, _ = await asyncio.wait(
                    wait_set,
                    return_when=asyncio.FIRST_COMPLETED,
                )
        else:
            done, _ = await asyncio.wait(
                wait_set,
                return_when=asyncio.FIRST_COMPLETED,
            )

        if task in done:
            return task.result()
        task.cancel()
        with contextlib.suppress(BaseException):
            await task
        raise asyncio.CancelledError()
    finally:
        if watcher is not None and not watcher.done():
            watcher.cancel()
        if not task.done():
            task.cancel()
            with contextlib.suppress(BaseException):
                await task


def _is_canceled(event: asyncio.Event | None) -> bool:
    return event is not None and event.is_set()


class Processor:
    """Processes one Redis Stream delivery under atomic claim and ACK rules."""

    def __init__(
        self,
        repo: JobRepo,
        acker: Acker,
        scraper: ArticleScraper,
        receiver: Receiver,
        options: ProcessorOptions | None = None,
    ) -> None:
        if repo is None or acker is None or scraper is None or receiver is None:
            raise ValueError("worker processor dependencies are required")
        opts = options or ProcessorOptions()
        if (
            not math.isfinite(opts.db_timeout_seconds)
            or opts.db_timeout_seconds <= 0
            or not math.isfinite(opts.queue_operation_timeout_seconds)
            or opts.queue_operation_timeout_seconds <= 0
        ):
            raise ValueError("worker operation timeouts must be positive")
        self._repo = repo
        self._acker = acker
        self._scraper = scraper
        self._receiver = receiver
        self._options = opts
        self._logger = opts.logger or logging.getLogger("inteldigest.worker")

    def _log(self, event: str, **fields: Any) -> None:
        parts = [event] + [f"{k}={v}" for k, v in fields.items()]
        self._logger.info(" ".join(parts))

    def _log_job(self, event: str, job_id: UUID, **fields: Any) -> None:
        self._log(event, job_id=str(job_id), **fields)

    def _context_outcome(
        self,
        admission_closed: asyncio.Event | None,
        job_canceled: asyncio.Event | None,
        cause: BaseException,
    ) -> tuple[Outcome, ProcessError]:
        if _is_canceled(job_canceled):
            return (
                Outcome.OPERATIONAL_CANCELED,
                ProcessError("job_context_canceled", cause),
            )
        if _is_canceled(admission_closed):
            return (
                Outcome.SKIPPED,
                ProcessError("admission_closed", cause),
            )
        return Outcome.ERROR, ProcessError("operation_timeout", cause)

    async def _ack_terminal(
        self,
        delivery: Delivery,
        job_id: UUID,
        *,
        admission_closed: asyncio.Event | None,
        job_canceled: asyncio.Event | None,
    ) -> tuple[Outcome, ProcessError | None]:
        if _is_canceled(job_canceled):
            return (
                Outcome.OPERATIONAL_CANCELED,
                ProcessError("job_context_canceled"),
            )
        if _is_canceled(admission_closed):
            return (
                Outcome.SKIPPED,
                ProcessError("admission_closed"),
            )
        try:
            await _await_with_parents(
                self._acker.ack(delivery.entry_id),
                timeout_seconds=self._options.queue_operation_timeout_seconds,
                job_canceled=job_canceled,
                admission_closed=admission_closed,
            )
        except (Exception, asyncio.CancelledError) as exc:  # noqa: BLE001
            self._log_job(
                "job_ack_failed",
                job_id,
                stream_entry_id=delivery.entry_id,
            )
            return Outcome.ERROR, ProcessError("ack_failed", exc)

        self._log_job("job_acked", job_id, stream_entry_id=delivery.entry_id)
        return Outcome.TERMINAL_DUPLICATE, None

    async def _deliver_result(
        self,
        result: Result,
        *,
        job_canceled: asyncio.Event | None,
    ) -> None:
        maybe_awaitable = self._receiver.receive(result)
        if inspect.isawaitable(maybe_awaitable):
            await _await_job_operation(
                maybe_awaitable,
                job_canceled=job_canceled,
            )

    async def process(
        self,
        delivery: Delivery,
        *,
        admission_closed: asyncio.Event | None = None,
        job_canceled: asyncio.Event | None = None,
    ) -> tuple[Outcome, ProcessError | None]:
        """Process a single delivery according to C3–C6 and C8 contracts."""
        if _is_canceled(job_canceled):
            return (
                Outcome.OPERATIONAL_CANCELED,
                ProcessError("job_context_canceled"),
            )
        if _is_canceled(admission_closed):
            return (
                Outcome.SKIPPED,
                ProcessError("admission_closed"),
            )

        try:
            message = decode_delivery(delivery)
        except (ValueError, TypeError) as exc:
            entry_id = delivery.entry_id if isinstance(delivery, Delivery) else ""
            self._log("job_skipped", reason="invalid_payload", stream_entry_id=entry_id)
            return Outcome.SKIPPED, ProcessError("invalid_payload", exc)

        job_id = message.job_id
        if _is_canceled(admission_closed):
            return Outcome.SKIPPED, ProcessError("admission_closed")
        if _is_canceled(job_canceled):
            return Outcome.OPERATIONAL_CANCELED, ProcessError("job_context_canceled")

        # 1. Pre-claim lookup
        try:
            job = await _await_with_parents(
                self._repo.get_job_by_id(job_id),
                timeout_seconds=self._options.db_timeout_seconds,
                job_canceled=job_canceled,
                admission_closed=admission_closed,
            )
        except (TimeoutError, asyncio.CancelledError) as exc:
            if not _is_canceled(job_canceled) and not _is_canceled(admission_closed) and isinstance(exc, asyncio.CancelledError):
                raise
            return self._context_outcome(admission_closed, job_canceled, exc)
        except Exception as exc:  # noqa: BLE001
            self._log_job("job_skipped", job_id, reason="database_lookup_failed")
            return Outcome.ERROR, ProcessError("database_lookup_failed", exc)

        if _is_canceled(admission_closed):
            return Outcome.SKIPPED, ProcessError("admission_closed")
        if _is_canceled(job_canceled):
            return Outcome.OPERATIONAL_CANCELED, ProcessError("job_context_canceled")

        if not _valid_stored_job(job, job_id, message.url):
            self._log_job("job_skipped", job_id, reason="job_mismatch_or_invalid")
            return Outcome.SKIPPED, ProcessError("job_mismatch_or_invalid")

        if job.status in _TERMINAL_STATUSES:
            return await self._ack_terminal(
                delivery,
                job_id,
                admission_closed=admission_closed,
                job_canceled=job_canceled,
            )
        if job.status == STATUS_PROCESSING:
            self._log_job("job_skipped", job_id, reason="already_processing")
            return Outcome.PROCESSING_DUPLICATE, None
        if job.status != STATUS_PENDING:
            self._log_job("job_skipped", job_id, reason="unknown_status")
            return Outcome.SKIPPED, ProcessError("unknown_status")

        if _is_canceled(admission_closed):
            return Outcome.SKIPPED, ProcessError("admission_closed")
        if _is_canceled(job_canceled):
            return Outcome.OPERATIONAL_CANCELED, ProcessError("job_context_canceled")

        # 2. Atomic claim
        try:
            claimed = await _await_with_parents(
                self._repo.try_mark_processing(job_id),
                timeout_seconds=self._options.db_timeout_seconds,
                job_canceled=job_canceled,
                admission_closed=admission_closed,
            )
        except (TimeoutError, asyncio.CancelledError) as exc:
            if not _is_canceled(job_canceled) and not _is_canceled(admission_closed) and isinstance(exc, asyncio.CancelledError):
                raise
            return self._context_outcome(admission_closed, job_canceled, exc)
        except Exception as exc:  # noqa: BLE001
            self._log_job("processing_claim_failed", job_id, operation="claim")
            return Outcome.ERROR, ProcessError("processing_claim_failed", exc)

        if not claimed:
            # Claim lost: requery to inspect winner/state
            try:
                fresh = await _await_with_parents(
                    self._repo.get_job_by_id(job_id),
                    timeout_seconds=self._options.db_timeout_seconds,
                    job_canceled=job_canceled,
                    admission_closed=admission_closed,
                )
            except (TimeoutError, asyncio.CancelledError) as exc:
                if not _is_canceled(job_canceled) and not _is_canceled(admission_closed) and isinstance(exc, asyncio.CancelledError):
                    raise
                return self._context_outcome(admission_closed, job_canceled, exc)
            except Exception as exc:  # noqa: BLE001
                self._log_job("claim_lost_requery_failed", job_id, operation="requery")
                return Outcome.ERROR, ProcessError("claim_lost_requery_failed", exc)

            if _is_canceled(admission_closed):
                return Outcome.SKIPPED, ProcessError("admission_closed")
            if _is_canceled(job_canceled):
                return Outcome.OPERATIONAL_CANCELED, ProcessError("job_context_canceled")

            if not _valid_stored_job(fresh, job_id, message.url):
                return Outcome.SKIPPED, ProcessError("claim_lost_requery_invalid")
            if fresh.status in _TERMINAL_STATUSES:
                return await self._ack_terminal(
                    delivery,
                    job_id,
                    admission_closed=admission_closed,
                    job_canceled=job_canceled,
                )
            if fresh.status == STATUS_PROCESSING:
                self._log_job("job_skipped", job_id, reason="claim_lost_processing")
                return Outcome.PROCESSING_DUPLICATE, None

            self._log_job("job_skipped", job_id, reason="claim_lost_unresolved")
            return Outcome.SKIPPED, ProcessError("claim_lost_unresolved")

        # 3. Claim winner: from this point on, admission_closed does NOT stop the job
        self._log_job("processing_started", job_id)
        if _is_canceled(job_canceled):
            return Outcome.OPERATIONAL_CANCELED, ProcessError("job_context_canceled")

        started = time.monotonic()
        stats = ScrapeStats()
        scrape_err: BaseException | None = None
        article: Article | None = None
        try:
            article, stats = await _await_job_operation(
                self._scraper.scrape_with_stats(job.url, stats=stats),
                job_canceled=job_canceled,
            )
        except asyncio.CancelledError as exc:
            if _is_canceled(job_canceled):
                return (
                    Outcome.OPERATIONAL_CANCELED,
                    ProcessError("job_context_canceled", exc),
                )
            scrape_err = exc
        except Exception as exc:  # noqa: BLE001
            scrape_err = exc

        duration = time.monotonic() - started
        if _is_canceled(job_canceled):
            return Outcome.OPERATIONAL_CANCELED, ProcessError("job_context_canceled")

        if scrape_err is None:
            assert article is not None
            result = Result(job_id=job_id, article=article)
            try:
                validate_result(result)
            except ValueError as exc:
                return Outcome.ERROR, ProcessError("invalid_article_result", exc)

            try:
                await self._deliver_result(result, job_canceled=job_canceled)
            except asyncio.CancelledError as exc:
                if _is_canceled(job_canceled):
                    return (
                        Outcome.OPERATIONAL_CANCELED,
                        ProcessError("job_context_canceled", exc),
                    )
                self._log_job("result_delivery_failed", job_id, operation="receiver")
                return Outcome.ERROR, ProcessError("result_delivery_failed", exc)
            except Exception as exc:  # noqa: BLE001
                self._log_job("result_delivery_failed", job_id, operation="receiver")
                return Outcome.ERROR, ProcessError("result_delivery_failed", exc)

            self._log_job(
                "scraping_completed",
                job_id,
                duration=f"{duration:.6f}s",
                bytes_read=stats.bytes_read,
            )
            return Outcome.SUCCEEDED, None

        # 4. Scrape failed: record bounded safe diagnostic and deliver typed error
        se = safe_scrape_error(scrape_err)
        classification = classify_scrape_error(se, scrape_err)
        reason = diagnostic_reason(se)

        recorded = False
        record_err: BaseException | None = None
        try:
            recorded = await _await_job_operation(
                self._repo.record_scrape_error(job_id, reason),
                job_canceled=job_canceled,
                timeout_seconds=self._options.db_timeout_seconds,
            )
        except asyncio.CancelledError as exc:
            if _is_canceled(job_canceled):
                return (
                    Outcome.OPERATIONAL_CANCELED,
                    ProcessError("job_context_canceled", exc),
                )
            record_err = exc
        except Exception as exc:  # noqa: BLE001
            record_err = exc

        if record_err is not None or not recorded:
            self._log_job(
                "scrape_error_record_failed",
                job_id,
                classification=classification,
                kind=safe_kind(se.kind),
                stage=safe_stage(se.stage),
                status=safe_status(se.http_status),
            )
            if record_err is not None:
                self._log_job(
                    "scrape_diagnostic_persistence_problem",
                    job_id,
                    operation="record_scrape_error",
                )

        if _is_canceled(job_canceled):
            return Outcome.OPERATIONAL_CANCELED, ProcessError("job_context_canceled")

        error_result = Result(job_id=job_id, error=se)
        try:
            validate_result(error_result)
        except ValueError as exc:
            return Outcome.ERROR, ProcessError("invalid_scrape_result", exc)

        try:
            await self._deliver_result(error_result, job_canceled=job_canceled)
        except asyncio.CancelledError as exc:
            if _is_canceled(job_canceled):
                return (
                    Outcome.OPERATIONAL_CANCELED,
                    ProcessError("job_context_canceled", exc),
                )
            self._log_job("result_delivery_failed", job_id, operation="receiver")
            return Outcome.ERROR, ProcessError("result_delivery_failed", exc)
        except Exception as exc:  # noqa: BLE001
            self._log_job("result_delivery_failed", job_id, operation="receiver")
            return Outcome.ERROR, ProcessError("result_delivery_failed", exc)

        self._log_job(
            "scraping_failed",
            job_id,
            classification=classification,
            kind=safe_kind(se.kind),
            stage=safe_stage(se.stage),
            status=safe_status(se.http_status),
        )
        return Outcome.SCRAPE_FAILED, None
