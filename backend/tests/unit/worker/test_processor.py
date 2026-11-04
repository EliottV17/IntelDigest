"""Unit tests for worker processor and synchronous metadata receiver (M14)."""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest

from inteldigest.jobs.models import Job
from inteldigest.jobs.repository import JobNotFound
from inteldigest.queue.consumer import Delivery
from inteldigest.scraper.models import (
    Article,
    ScrapeError,
    ScrapeErrorKind,
    ScrapeStage,
    ScrapeStats,
)
from inteldigest.worker import (
    MetadataReceiver,
    Outcome,
    ProcessError,
    Processor,
    ProcessorOptions,
    Result,
    classify_scrape_error,
    diagnostic_reason,
    validate_result,
)


def _now() -> datetime:
    return datetime.now(UTC)


def _make_job(
    job_id: UUID,
    url: str = "https://db.test/article",
    status: str = "pending",
) -> Job:
    now = _now()
    return Job(
        id=job_id,
        url=url,
        status=status,
        created_at=now,
        updated_at=now,
    )


def _payload(job_id: UUID, url: str = "https://db.test/article") -> dict[str, Any]:
    return {
        "schema_version": 1,
        "job_id": str(job_id),
        "url": url,
    }


def _delivery(
    job_id: UUID,
    entry_id: str = "171000-1",
    raw: Any = None,
    url: str = "https://db.test/article",
) -> Delivery:
    data = _payload(job_id, url) if raw is None else raw
    return Delivery(entry_id=entry_id, fields={"data": json.dumps(data)})


class FakeRepo:
    def __init__(
        self,
        job: Job | None = None,
        *,
        get_err: BaseException | None = None,
        on_get: Any = None,
        after_get_snapshot: Any = None,
        claim: bool = False,
        claim_err: BaseException | None = None,
        claim_lost_status: str = "",
        record_ok: bool = True,
        record_err: BaseException | None = None,
        delay_seconds: float = 0.0,
    ) -> None:
        self.job = job
        self.get_err = get_err
        self.on_get = on_get
        self.after_get_snapshot = after_get_snapshot
        self.claim = claim
        self.claim_err = claim_err
        self.claim_lost_status = claim_lost_status
        self.record_ok = record_ok
        self.record_err = record_err
        self.delay_seconds = delay_seconds
        self.get_calls = 0
        self.claim_calls = 0
        self.record_calls = 0
        self.record_reasons: list[str] = []

    async def get_job_by_id(self, job_id: UUID) -> Job:
        self.get_calls += 1
        if self.on_get is not None:
            res = self.on_get()
            if asyncio.iscoroutine(res):
                await res
        if self.delay_seconds > 0:
            await asyncio.sleep(self.delay_seconds)
        if self.get_err is not None:
            raise self.get_err
        if self.job is None:
            raise JobNotFound()
        snapshot = self.job.model_copy(deep=True)
        if self.after_get_snapshot is not None:
            res = self.after_get_snapshot()
            if asyncio.iscoroutine(res):
                await res
        return snapshot

    async def try_mark_processing(self, job_id: UUID) -> bool:
        self.claim_calls += 1
        if self.delay_seconds > 0:
            await asyncio.sleep(self.delay_seconds)
        if self.claim_err is not None:
            raise self.claim_err
        won = self.claim
        if won:
            self.claim = False
            if self.job is not None:
                self.job = self.job.model_copy(update={"status": "processing"})
        elif self.job is not None and self.claim_lost_status:
            self.job = self.job.model_copy(update={"status": self.claim_lost_status})
        return won

    async def record_scrape_error(self, job_id: UUID, safe_reason: str) -> bool:
        self.record_calls += 1
        self.record_reasons.append(safe_reason)
        if self.delay_seconds > 0:
            await asyncio.sleep(self.delay_seconds)
        if self.record_err is not None:
            raise self.record_err
        return self.record_ok


class FakeAcker:
    def __init__(
        self,
        *,
        err: BaseException | None = None,
        delay_seconds: float = 0.0,
    ) -> None:
        self.ids: list[str] = []
        self.err = err
        self.delay_seconds = delay_seconds

    async def ack(self, entry_id: str) -> None:
        if self.delay_seconds > 0:
            await asyncio.sleep(self.delay_seconds)
        self.ids.append(entry_id)
        if self.err is not None:
            raise self.err


class FakeScraper:
    def __init__(
        self,
        article: Article | None = None,
        stats: ScrapeStats | None = None,
        *,
        err: BaseException | None = None,
        on_scrape: Any = None,
    ) -> None:
        self.article = article or Article(
            text="Extracted body",
            requested_url="https://db.test/article",
            final_url="https://db.test/article",
            title="Article Title",
            language="en",
            fetched_at=_now(),
        )
        self.stats = stats or ScrapeStats(bytes_read=64)
        self.err = err
        self.on_scrape = on_scrape
        self.calls = 0
        self.urls: list[str] = []

    async def scrape_with_stats(
        self,
        raw_url: str,
        *,
        stats: ScrapeStats | None = None,
    ) -> tuple[Article, ScrapeStats]:
        self.calls += 1
        self.urls.append(raw_url)
        if self.on_scrape is not None:
            res = self.on_scrape()
            if asyncio.iscoroutine(res):
                await res
        if self.err is not None:
            raise self.err
        if stats is not None:
            stats.bytes_read = self.stats.bytes_read
            return self.article, stats
        return self.article, self.stats


class FakeReceiver:
    def __init__(self, *, err: BaseException | None = None) -> None:
        self.results: list[Result] = []
        self.err = err

    async def receive(self, result: Result) -> None:
        self.results.append(result)
        if self.err is not None:
            raise self.err


def _make_processor(
    repo: FakeRepo,
    acker: FakeAcker,
    scraper: FakeScraper,
    receiver: Any,
    *,
    db_timeout_seconds: float = 1.0,
    queue_operation_timeout_seconds: float = 1.0,
    logger: logging.Logger | None = None,
) -> Processor:
    return Processor(
        repo=repo,
        acker=acker,
        scraper=scraper,
        receiver=receiver,
        options=ProcessorOptions(
            db_timeout_seconds=db_timeout_seconds,
            queue_operation_timeout_seconds=queue_operation_timeout_seconds,
            logger=logger or logging.getLogger("test.worker"),
        ),
    )


@pytest.mark.asyncio
async def test_process_rejects_invalid_payload_without_effects() -> None:
    job_id = uuid4()
    repo = FakeRepo(job=_make_job(job_id), claim=True)
    acker = FakeAcker()
    scraper = FakeScraper()
    receiver = FakeReceiver()
    processor = _make_processor(repo, acker, scraper, receiver)

    cases = [
        ("missing data", Delivery(entry_id="1-0", fields={})),
        ("wrong field type", Delivery(entry_id="2-0", fields={"data": b"{}"})),
        ("empty string", Delivery(entry_id="2-1", fields={"data": ""})),
        ("malformed json", Delivery(entry_id="3-0", fields={"data": "{"})),
        ("non-object json", Delivery(entry_id="3-1", fields={"data": "[]"})),
        (
            "unknown version",
            _delivery(
                job_id,
                "4-0",
                raw={"schema_version": 99, "job_id": str(job_id), "url": "https://db.test"},
            ),
        ),
        (
            "invalid uuid",
            _delivery(
                job_id,
                "5-0",
                raw={"schema_version": 1, "job_id": "nope", "url": "https://db.test"},
            ),
        ),
        (
            "nil uuid",
            _delivery(
                job_id,
                "5-1",
                raw={
                    "schema_version": 1,
                    "job_id": "00000000-0000-0000-0000-000000000000",
                    "url": "https://db.test",
                },
            ),
        ),
        (
            "invalid url",
            _delivery(
                job_id,
                "6-0",
                raw=_payload(job_id, "file:///secret?token=private"),
            ),
        ),
    ]

    for name, delivery in cases:
        outcome, err = await processor.process(delivery)
        assert outcome is Outcome.SKIPPED, f"case {name} outcome={outcome}"
        assert isinstance(err, ProcessError), f"case {name} err={err}"
        assert err.code == "invalid_payload"
        assert "private" not in str(err)
        assert "secret" not in str(err)

    assert repo.get_calls == 0
    assert repo.claim_calls == 0
    assert scraper.calls == 0
    assert len(receiver.results) == 0
    assert len(acker.ids) == 0


@pytest.mark.asyncio
async def test_process_claim_winner_scrapes_and_delivers_without_ack() -> None:
    job_id = uuid4()
    article = Article(
        text="text",
        requested_url="https://persisted.test",
        final_url="https://final.test",
        title="title",
        language="en",
        fetched_at=_now(),
    )
    repo = FakeRepo(job=_make_job(job_id, url=article.requested_url), claim=True)
    acker = FakeAcker()
    scraper = FakeScraper(article=article, stats=ScrapeStats(bytes_read=91))
    receiver = FakeReceiver()
    processor = _make_processor(repo, acker, scraper, receiver)

    outcome, err = await processor.process(
        _delivery(job_id, "171000-2", url=article.requested_url)
    )
    assert outcome is Outcome.SUCCEEDED
    assert err is None
    assert scraper.calls == 1
    assert scraper.urls == [article.requested_url]
    assert len(receiver.results) == 1
    assert len(acker.ids) == 0

    res = receiver.results[0]
    assert res.job_id == job_id
    assert res.article == article
    assert res.error is None
    assert repo.job is not None and repo.job.status == "processing"


@pytest.mark.asyncio
async def test_process_terminal_duplicate_acks_original_stream_id() -> None:
    for terminal_status in ("completed", "failed"):
        job_id = uuid4()
        repo = FakeRepo(job=_make_job(job_id, status=terminal_status))
        acker = FakeAcker()
        scraper = FakeScraper()
        receiver = FakeReceiver()
        processor = _make_processor(repo, acker, scraper, receiver)

        outcome, err = await processor.process(_delivery(job_id, "171000-9"))
        assert outcome is Outcome.TERMINAL_DUPLICATE
        assert err is None
        assert acker.ids == ["171000-9"]
        assert repo.claim_calls == 0
        assert scraper.calls == 0
        assert len(receiver.results) == 0


@pytest.mark.asyncio
async def test_process_processing_duplicate_does_not_ack_or_scrape() -> None:
    job_id = uuid4()
    repo = FakeRepo(job=_make_job(job_id, status="processing"))
    acker = FakeAcker()
    scraper = FakeScraper()
    receiver = FakeReceiver()
    processor = _make_processor(repo, acker, scraper, receiver)

    outcome, err = await processor.process(_delivery(job_id, "4-0"))
    assert outcome is Outcome.PROCESSING_DUPLICATE
    assert err is None
    assert acker.ids == []
    assert repo.claim_calls == 0
    assert scraper.calls == 0
    assert receiver.results == []


@pytest.mark.asyncio
async def test_process_claim_loss_requeries_and_only_acks_proven_terminal() -> None:
    job_id = uuid4()
    repo = FakeRepo(job=_make_job(job_id, status="pending"), claim=False, claim_lost_status="failed")
    acker = FakeAcker()
    scraper = FakeScraper()
    receiver = FakeReceiver()
    processor = _make_processor(repo, acker, scraper, receiver)

    outcome, err = await processor.process(_delivery(job_id, "8-1"))
    assert outcome is Outcome.TERMINAL_DUPLICATE
    assert err is None
    assert repo.get_calls == 2
    assert repo.claim_calls == 1
    assert acker.ids == ["8-1"]
    assert scraper.calls == 0
    assert receiver.results == []


@pytest.mark.asyncio
async def test_process_scrape_failure_diagnoses_and_delivers_typed_error() -> None:
    job_id = uuid4()
    cause = RuntimeError("dns secret.invalid?credential=x")
    scrape_err = ScrapeError(
        ScrapeErrorKind.NETWORK,
        ScrapeStage.FETCH,
        "raw secret",
        cause=cause,
    )
    repo = FakeRepo(job=_make_job(job_id), claim=True, record_ok=True)
    acker = FakeAcker()
    scraper = FakeScraper(err=scrape_err)
    receiver = FakeReceiver()
    processor = _make_processor(repo, acker, scraper, receiver)

    outcome, err = await processor.process(_delivery(job_id, "9-2"))
    assert outcome is Outcome.SCRAPE_FAILED
    assert err is None
    assert repo.record_calls == 1
    assert repo.record_reasons == ["scraping/network during fetch"]
    assert "secret" not in repo.record_reasons[0]
    assert len(receiver.results) == 1
    assert receiver.results[0].article is None
    assert receiver.results[0].error is not None
    assert receiver.results[0].error.__cause__ is scrape_err or receiver.results[0].error.cause is cause
    assert acker.ids == []


@pytest.mark.asyncio
async def test_process_actual_job_cancellation_is_operational() -> None:
    job_id = uuid4()
    job_canceled = asyncio.Event()
    job_canceled.set()
    repo = FakeRepo(job=_make_job(job_id), claim=True)
    acker = FakeAcker()
    scraper = FakeScraper()
    receiver = FakeReceiver()
    processor = _make_processor(repo, acker, scraper, receiver)

    outcome, err = await processor.process(
        _delivery(job_id, "10-1"),
        job_canceled=job_canceled,
    )
    assert outcome is Outcome.OPERATIONAL_CANCELED
    assert isinstance(err, ProcessError)
    assert err.code == "job_context_canceled"
    assert repo.claim_calls == 0
    assert scraper.calls == 0
    assert repo.record_calls == 0
    assert receiver.results == []
    assert acker.ids == []


@pytest.mark.asyncio
async def test_process_concurrent_same_job_has_single_winner() -> None:
    job_id = uuid4()
    initial_snapshots = 0
    snapshots_released = asyncio.Event()

    async def after_get_snapshot() -> None:
        nonlocal initial_snapshots
        initial_snapshots += 1
        wait_for_peers = initial_snapshots <= 2
        if initial_snapshots == 2:
            snapshots_released.set()
        if not wait_for_peers:
            return
        await asyncio.wait_for(snapshots_released.wait(), timeout=3.0)

    repo = FakeRepo(
        job=_make_job(job_id),
        claim=True,
        after_get_snapshot=after_get_snapshot,
    )
    acker = FakeAcker()
    scraper = FakeScraper()
    receiver = FakeReceiver()
    processor = _make_processor(repo, acker, scraper, receiver)

    res1, res2 = await asyncio.gather(
        processor.process(_delivery(job_id, "12-0")),
        processor.process(_delivery(job_id, "12-1")),
    )

    outcomes = [res1[0], res2[0]]
    errs = [res1[1], res2[1]]
    assert errs == [None, None]
    assert outcomes.count(Outcome.SUCCEEDED) == 1
    assert outcomes.count(Outcome.PROCESSING_DUPLICATE) == 1
    assert scraper.calls == 1
    assert len(receiver.results) == 1
    assert receiver.results[0].job_id == job_id
    assert acker.ids == []
    assert repo.claim_calls == 2
    assert repo.get_calls == 3
    assert repo.record_calls == 0
    assert repo.job is not None and repo.job.status == "processing"


@pytest.mark.asyncio
async def test_process_admission_closing_during_preclaim_lookup_prevents_claim() -> None:
    job_id = uuid4()
    admission_closed = asyncio.Event()
    repo = FakeRepo(
        job=_make_job(job_id),
        claim=True,
        on_get=admission_closed.set,
    )
    acker = FakeAcker()
    scraper = FakeScraper()
    receiver = FakeReceiver()
    processor = _make_processor(repo, acker, scraper, receiver)

    outcome, err = await processor.process(
        _delivery(job_id, "13-1"),
        admission_closed=admission_closed,
    )
    assert outcome is Outcome.SKIPPED
    assert isinstance(err, ProcessError)
    assert err.code == "admission_closed"
    assert repo.claim_calls == 0
    assert scraper.calls == 0
    assert receiver.results == []
    assert acker.ids == []


@pytest.mark.asyncio
async def test_process_uncertain_claim_never_scrapes() -> None:
    job_id = uuid4()
    repo = FakeRepo(job=_make_job(job_id), claim_err=RuntimeError("database secret"))
    acker = FakeAcker()
    scraper = FakeScraper()
    receiver = FakeReceiver()
    processor = _make_processor(repo, acker, scraper, receiver)

    outcome, err = await processor.process(_delivery(job_id, "14-1"))
    assert outcome is Outcome.ERROR
    assert isinstance(err, ProcessError)
    assert err.code == "processing_claim_failed"
    assert "secret" not in str(err)
    assert scraper.calls == 0
    assert receiver.results == []
    assert acker.ids == []


@pytest.mark.asyncio
async def test_process_claim_loss_pending_does_not_ack_or_scrape() -> None:
    job_id = uuid4()
    repo = FakeRepo(job=_make_job(job_id, status="pending"), claim=False)
    acker = FakeAcker()
    scraper = FakeScraper()
    receiver = FakeReceiver()
    processor = _make_processor(repo, acker, scraper, receiver)

    outcome, err = await processor.process(_delivery(job_id, "15-1"))
    assert outcome is Outcome.SKIPPED
    assert isinstance(err, ProcessError)
    assert err.code == "claim_lost_unresolved"
    assert repo.get_calls == 2
    assert scraper.calls == 0
    assert receiver.results == []
    assert acker.ids == []


@pytest.mark.asyncio
async def test_process_live_job_local_timeout_wrapping_canceled_is_business_failure() -> None:
    job_id = uuid4()
    typed = ScrapeError(
        ScrapeErrorKind.TIMEOUT,
        ScrapeStage.FETCH,
        cause=asyncio.CancelledError(),
    )
    repo = FakeRepo(job=_make_job(job_id), claim=True, record_ok=True)
    acker = FakeAcker()
    scraper = FakeScraper(err=typed)
    receiver = FakeReceiver()
    processor = _make_processor(repo, acker, scraper, receiver)

    outcome, err = await processor.process(_delivery(job_id, "16-1"))
    assert outcome is Outcome.SCRAPE_FAILED
    assert err is None
    assert repo.record_calls == 1
    assert len(receiver.results) == 1
    assert receiver.results[0].error is not None
    assert acker.ids == []


@pytest.mark.asyncio
async def test_process_confirmed_winner_drains_after_admission_closes() -> None:
    job_id = uuid4()
    admission_closed = asyncio.Event()
    repo = FakeRepo(job=_make_job(job_id), claim=True)
    acker = FakeAcker()
    scraper = FakeScraper(on_scrape=admission_closed.set)
    receiver = FakeReceiver()
    processor = _make_processor(repo, acker, scraper, receiver)

    outcome, err = await processor.process(
        _delivery(job_id, "16-2"),
        admission_closed=admission_closed,
    )
    assert outcome is Outcome.SUCCEEDED
    assert err is None
    assert scraper.calls == 1
    assert len(receiver.results) == 1
    assert acker.ids == []


@pytest.mark.asyncio
async def test_process_job_cancellation_during_scrape_skips_diagnosis_and_output() -> None:
    job_id = uuid4()
    job_canceled = asyncio.Event()
    repo = FakeRepo(job=_make_job(job_id), claim=True)
    acker = FakeAcker()
    scraper = FakeScraper(
        err=ScrapeError(ScrapeErrorKind.NETWORK, ScrapeStage.FETCH),
        on_scrape=job_canceled.set,
    )
    receiver = FakeReceiver()
    processor = _make_processor(repo, acker, scraper, receiver)

    outcome, err = await processor.process(
        _delivery(job_id, "16-3"),
        job_canceled=job_canceled,
    )
    assert outcome is Outcome.OPERATIONAL_CANCELED
    assert isinstance(err, ProcessError)
    assert err.code == "job_context_canceled"
    assert repo.record_calls == 0
    assert receiver.results == []
    assert acker.ids == []


def test_scrape_classification_uses_allowlisted_codes_and_causes() -> None:
    for status, want in [(404, "permanent"), (408, "transient"), (429, "transient"), (503, "transient")]:
        err = ScrapeError(ScrapeErrorKind.HTTP_STATUS, ScrapeStage.FETCH, http_status=status)
        assert classify_scrape_error(err, None) == want

    cert_err = ssl.SSLCertVerificationError("certificate has expired")
    assert (
        classify_scrape_error(
            ScrapeError(ScrapeErrorKind.NETWORK, ScrapeStage.FETCH),
            cert_err,
        )
        == "permanent"
    )
    assert (
        classify_scrape_error(
            ScrapeError(ScrapeErrorKind.TIMEOUT, ScrapeStage.FETCH),
            TimeoutError(),
        )
        == "transient"
    )
    assert (
        classify_scrape_error(
            ScrapeError(ScrapeErrorKind.CANCELED, ScrapeStage.FETCH),
            asyncio.CancelledError(),
        )
        == "unknown"
    )
    assert (
         diagnostic_reason(
            ScrapeError(ScrapeErrorKind.HTTP_STATUS, ScrapeStage.FETCH, http_status=404)
        )
        == "scraping/http_status: HTTP 404"
    )
    assert (
        diagnostic_reason(
            ScrapeError(ScrapeErrorKind.BODY_TOO_LARGE, ScrapeStage.FETCH)
        )
        == "scraping/body_too_large during fetch"
    )


@pytest.mark.asyncio
async def test_process_receiver_failure_never_creates_synthetic_diagnosis() -> None:
    job_id = uuid4()
    repo = FakeRepo(job=_make_job(job_id), claim=True)
    acker = FakeAcker()
    scraper = FakeScraper()
    receiver = FakeReceiver(err=RuntimeError("sink secret"))
    processor = _make_processor(repo, acker, scraper, receiver)

    outcome, err = await processor.process(_delivery(job_id, "17-1"))
    assert outcome is Outcome.ERROR
    assert isinstance(err, ProcessError)
    assert err.code == "result_delivery_failed"
    assert "secret" not in str(err)
    assert repo.record_calls == 0
    assert acker.ids == []


@pytest.mark.asyncio
async def test_process_scrape_error_receiver_failure_keeps_only_real_diagnosis() -> None:
    job_id = uuid4()
    repo = FakeRepo(job=_make_job(job_id), claim=True, record_ok=True)
    acker = FakeAcker()
    scraper = FakeScraper(
        err=ScrapeError(ScrapeErrorKind.BODY_TOO_LARGE, ScrapeStage.FETCH)
    )
    receiver = FakeReceiver(err=RuntimeError("sink secret"))
    processor = _make_processor(repo, acker, scraper, receiver)

    outcome, err = await processor.process(_delivery(job_id, "18-1"))
    assert outcome is Outcome.ERROR
    assert isinstance(err, ProcessError)
    assert err.code == "result_delivery_failed"
    assert "secret" not in str(err)
    assert repo.record_calls == 1
    assert repo.record_reasons == ["scraping/body_too_large during fetch"]
    assert acker.ids == []


@pytest.mark.asyncio
async def test_process_diagnosis_persistence_failure_still_delivers_real_scrape_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    job_id = uuid4()
    logger = logging.getLogger("test.worker.diag")
    repo = FakeRepo(
        job=_make_job(job_id),
        claim=True,
        record_err=RuntimeError("database cause secret"),
    )
    acker = FakeAcker()
    scraper = FakeScraper(
        err=ScrapeError(ScrapeErrorKind.NETWORK, ScrapeStage.FETCH)
    )
    receiver = FakeReceiver()
    processor = _make_processor(repo, acker, scraper, receiver, logger=logger)

    with caplog.at_level(logging.INFO, logger="test.worker.diag"):
        outcome, err = await processor.process(_delivery(job_id, "18-2"))

    assert outcome is Outcome.SCRAPE_FAILED
    assert err is None
    assert len(receiver.results) == 1
    assert receiver.results[0].error is not None
    assert acker.ids == []
    assert "scrape_diagnostic_persistence_problem" in caplog.text
    assert "secret" not in caplog.text


def test_receiver_result_requires_valid_id_and_exactly_one_payload() -> None:
    article = Article(
        text="body",
        requested_url="https://db.test",
        final_url="https://db.test",
        title="title",
        language="en",
        fetched_at=_now(),
    )
    scrape_err = ScrapeError(ScrapeErrorKind.NETWORK, ScrapeStage.FETCH)
    nil_id = UUID(int=0)

    with pytest.raises(ValueError, match="job ID"):
        validate_result(Result(job_id=nil_id, article=article))
    with pytest.raises(ValueError, match="exactly one"):
        validate_result(Result(job_id=uuid4()))
    with pytest.raises(ValueError, match="exactly one"):
        validate_result(Result(job_id=uuid4(), article=article, error=scrape_err))

    validate_result(Result(job_id=uuid4(), article=article))
    validate_result(Result(job_id=uuid4(), error=scrape_err))


@pytest.mark.asyncio
async def test_process_rejects_stored_identity_url_and_unknown_status() -> None:
    job_id = uuid4()
    cases: list[tuple[str, Job | None, Outcome]] = [
        ("missing job", None, Outcome.ERROR),
        ("wrong ID", _make_job(uuid4(), url="https://db.test/article"), Outcome.SKIPPED),
        ("URL mismatch", _make_job(job_id, url="https://other.test/article"), Outcome.SKIPPED),
        ("unknown status", _make_job(job_id, status="future"), Outcome.SKIPPED),
    ]
    for name, job, want in cases:
        repo = FakeRepo(job=job)
        acker = FakeAcker()
        scraper = FakeScraper()
        receiver = FakeReceiver()
        processor = _make_processor(repo, acker, scraper, receiver)

        outcome, err = await processor.process(_delivery(job_id, "19-1"))
        assert outcome is want, f"{name}: outcome={outcome}"
        assert isinstance(err, ProcessError), f"{name}: err={err}"
        assert repo.claim_calls == 0
        assert scraper.calls == 0
        assert receiver.results == []
        assert acker.ids == []


@pytest.mark.asyncio
async def test_process_get_failure_has_no_side_effects() -> None:
    job_id = uuid4()
    repo = FakeRepo(get_err=RuntimeError("database password=secret"))
    acker = FakeAcker()
    scraper = FakeScraper()
    receiver = FakeReceiver()
    processor = _make_processor(repo, acker, scraper, receiver)

    outcome, err = await processor.process(_delivery(job_id, "20-1"))
    assert outcome is Outcome.ERROR
    assert isinstance(err, ProcessError)
    assert err.code == "database_lookup_failed"
    assert "secret" not in str(err)
    assert repo.claim_calls == 0
    assert scraper.calls == 0
    assert receiver.results == []
    assert acker.ids == []


@pytest.mark.asyncio
async def test_process_success_log_uses_scrape_bytes_and_excludes_content(
    caplog: pytest.LogCaptureFixture,
) -> None:
    job_id = uuid4()
    logger = logging.getLogger("test.worker.success")
    article = Article(
        text="article-text-secret",
        requested_url="https://db.test/article",
        final_url="https://db.test/article",
        title="title-secret",
        language="en",
        fetched_at=_now(),
    )
    repo = FakeRepo(job=_make_job(job_id), claim=True)
    acker = FakeAcker()
    scraper = FakeScraper(article=article, stats=ScrapeStats(bytes_read=37))
    receiver = MetadataReceiver(logger=logger)
    processor = _make_processor(repo, acker, scraper, receiver, logger=logger)

    with caplog.at_level(logging.INFO, logger="test.worker.success"):
        outcome, err = await processor.process(_delivery(job_id, "21-1"))

    assert outcome is Outcome.SUCCEEDED
    assert err is None
    assert "bytes_read=37" in caplog.text
    assert "worker_result_received" in caplog.text
    assert "article-text-secret" not in caplog.text
    assert "title-secret" not in caplog.text
    assert "https://db.test/article" not in caplog.text


def test_processor_rejects_missing_dependencies_and_timeouts() -> None:
    repo = FakeRepo()
    acker = FakeAcker()
    scraper = FakeScraper()
    receiver = FakeReceiver()
    valid = ProcessorOptions(db_timeout_seconds=1.0, queue_operation_timeout_seconds=1.0)

    with pytest.raises(ValueError, match="dependencies"):
        Processor(None, acker, scraper, receiver, valid)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="dependencies"):
        Processor(repo, None, scraper, receiver, valid)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="dependencies"):
        Processor(repo, acker, None, receiver, valid)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="dependencies"):
        Processor(repo, acker, scraper, None, valid)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="positive"):
        Processor(
            repo,
            acker,
            scraper,
            receiver,
            ProcessorOptions(db_timeout_seconds=0, queue_operation_timeout_seconds=1.0),
        )
    with pytest.raises(ValueError, match="positive"):
        Processor(
            repo,
            acker,
            scraper,
            receiver,
            ProcessorOptions(db_timeout_seconds=1.0, queue_operation_timeout_seconds=-1.0),
        )


@pytest.mark.asyncio
async def test_process_enforces_db_and_ack_timeouts() -> None:
    job_id = uuid4()
    slow_repo = FakeRepo(job=_make_job(job_id), claim=True, delay_seconds=0.2)
    acker = FakeAcker()
    scraper = FakeScraper()
    receiver = FakeReceiver()
    processor = _make_processor(
        slow_repo,
        acker,
        scraper,
        receiver,
        db_timeout_seconds=0.02,
        queue_operation_timeout_seconds=0.02,
    )

    outcome, err = await processor.process(_delivery(job_id, "22-1"))
    assert outcome is Outcome.ERROR
    assert isinstance(err, ProcessError)
    assert err.code == "operation_timeout"

    fast_terminal_repo = FakeRepo(job=_make_job(job_id, status="completed"))
    slow_acker = FakeAcker(delay_seconds=0.2)
    processor2 = _make_processor(
        fast_terminal_repo,
        slow_acker,
        scraper,
        receiver,
        db_timeout_seconds=1.0,
        queue_operation_timeout_seconds=0.02,
    )
    outcome2, err2 = await processor2.process(_delivery(job_id, "22-2"))
    assert outcome2 is Outcome.ERROR
    assert isinstance(err2, ProcessError)
    assert err2.code == "ack_failed"
