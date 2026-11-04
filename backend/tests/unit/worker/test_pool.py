"""Unit tests for bounded concurrency worker pool and graceful drain (M15)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import pytest

from inteldigest.queue.consumer import (
    ConsumerGroupMissingError,
    Delivery,
    is_group_missing,
)
from inteldigest.worker.pool import (
    JobProcessor,
    Pool,
    PoolGroupMissingError,
    PoolOptions,
    QueueReader,
    default_pool_options,
)
from inteldigest.worker.processor import Outcome, ProcessError


class LogCaptureHandler(logging.Handler):
    """Simple logging handler capturing formatted record messages in a list."""

    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(self.format(record))


def _make_logger(name: str = "test.pool") -> tuple[logging.Logger, LogCaptureHandler]:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = LogCaptureHandler()
    logger.handlers = [handler]
    logger.propagate = False
    return logger, handler


class FakeReader:
    """Deterministic fake reader returning deliveries, raising errors, or blocking."""

    def __init__(self, reads: list[Any] | None = None) -> None:
        self.reads: list[Any] = list(reads or [])
        self.calls = 0
        self.started = asyncio.Event()
        self.block_event: asyncio.Event | None = None

    async def read(self) -> Delivery | None:
        self.calls += 1
        self.started.set()
        if self.block_event is not None:
            await self.block_event.wait()
        if not self.reads:
            return None
        item = self.reads.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


class FakeProcessor:
    """Deterministic fake processor recording calls and running optional hooks."""

    def __init__(self, fn: Any = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self.fn = fn

    async def process(
        self,
        delivery: Delivery,
        *,
        admission_closed: asyncio.Event | None = None,
        job_canceled: asyncio.Event | None = None,
    ) -> tuple[Outcome, ProcessError | None]:
        call = {
            "delivery": delivery,
            "admission_closed": admission_closed,
            "job_canceled": job_canceled,
        }
        self.calls.append(call)
        if self.fn is not None:
            res = self.fn(call)
            if asyncio.iscoroutine(res):
                res = await res
            if isinstance(res, tuple):
                return res
            if isinstance(res, ProcessError):
                return (Outcome.ERROR, res)
            if isinstance(res, Exception):
                raise res
        return (Outcome.SUCCEEDED, None)


def test_pool_options_defaults_and_validation() -> None:
    defaults = default_pool_options()
    assert defaults.pool_size == 4
    assert defaults.shutdown_timeout_seconds == 10.0
    assert defaults.backoff_delay_seconds == 1.0
    assert defaults.logger is None

    # Valid custom options
    valid = PoolOptions(
        pool_size=8,
        shutdown_timeout_seconds=5.0,
        backoff_delay_seconds=0.5,
    )
    assert valid.pool_size == 8

    # pool_size bounds: 1 to 64
    with pytest.raises(ValueError, match="worker pool size must be between 1 and 64"):
        PoolOptions(pool_size=0)
    with pytest.raises(ValueError, match="worker pool size must be between 1 and 64"):
        PoolOptions(pool_size=65)
    with pytest.raises(ValueError, match="worker pool size must be between 1 and 64"):
        PoolOptions(pool_size=True)  # type: ignore[arg-type]

    # shutdown timeout and backoff: positive finite numbers
    with pytest.raises(
        ValueError, match="worker pool shutdown timeout and backoff must be positive"
    ):
        PoolOptions(shutdown_timeout_seconds=0.0)
    with pytest.raises(
        ValueError, match="worker pool shutdown timeout and backoff must be positive"
    ):
        PoolOptions(shutdown_timeout_seconds=-1.0)
    with pytest.raises(
        ValueError, match="worker pool shutdown timeout and backoff must be positive"
    ):
        PoolOptions(shutdown_timeout_seconds=float("inf"))
    with pytest.raises(
        ValueError, match="worker pool shutdown timeout and backoff must be positive"
    ):
        PoolOptions(backoff_delay_seconds=0.0)
    with pytest.raises(
        ValueError, match="worker pool shutdown timeout and backoff must be positive"
    ):
        PoolOptions(backoff_delay_seconds=-0.5)


def test_pool_constructor_dependency_validation() -> None:
    reader = FakeReader()
    processor = FakeProcessor()

    # Valid construction
    pool = Pool(reader, processor)
    assert isinstance(pool, Pool)
    assert pool.options.pool_size == 4

    # None dependencies rejected
    with pytest.raises(
        ValueError, match="worker pool dependencies are required"
    ):
        Pool(None, processor)  # type: ignore[arg-type]
    with pytest.raises(
        ValueError, match="worker pool dependencies are required"
    ):
        Pool(reader, None)  # type: ignore[arg-type]

    # Protocol check
    assert isinstance(reader, QueueReader)
    assert isinstance(processor, JobProcessor)


@pytest.mark.asyncio
async def test_pool_fixed_concurrency_and_sequential_slot_reuse() -> None:
    """Fixed concurrency: exactly pool_size readers active, slots reused sequentially."""
    workers = 3
    total_deliveries = 8
    deliveries = [
        Delivery(entry_id=f"d-{i}", fields={"idx": i})
        for i in range(total_deliveries)
    ]
    reader = FakeReader(reads=list(deliveries))

    entered = asyncio.Queue[dict[str, Any]]()
    release = asyncio.Queue[None]()

    async def _on_process(call: dict[str, Any]) -> None:
        await entered.put(call)
        await release.get()

    processor = FakeProcessor(fn=_on_process)
    options = PoolOptions(
        pool_size=workers,
        shutdown_timeout_seconds=1.0,
    )
    pool = Pool(reader, processor, options)
    stop_event = asyncio.Event()
    run_task = asyncio.create_task(pool.run(stop_event))

    # Wait until all 3 worker slots have picked up a delivery
    for _ in range(workers):
        await entered.get()

    # While all 3 workers are held in processing, reader calls must equal workers
    assert reader.calls == workers

    # Release 5 workers so remaining 5 deliveries are processed
    for _ in range(5):
        await release.put(None)

    for _ in range(5):
        await entered.get()

    # Signal stop while the 3 workers are still busy holding the last 3 deliveries
    stop_event.set()

    # Now release the final 3 workers
    for _ in range(workers):
        await release.put(None)

    await run_task

    # Exactly 8 deliveries read; no extra reads because admission was closed before release
    assert reader.calls == total_deliveries
    assert len(processor.calls) == total_deliveries


@pytest.mark.asyncio
async def test_pool_empty_read_and_per_job_error_continue_without_retry() -> None:
    """Empty reads loop immediately; per-job errors log warning without leaking raw cause."""
    secret = "DO_NOT_LOG_JOB_SECRET"
    reader = FakeReader(
        reads=[
            None,  # normal empty timeout
            Delivery(entry_id="bad", fields={}),
            Delivery(entry_id="good", fields={}),
        ]
    )

    async def _on_process(call: dict[str, Any]) -> tuple[Outcome, ProcessError | None]:
        d: Delivery = call["delivery"]
        if d.entry_id == "bad":
            return (Outcome.ERROR, ProcessError("processing_failed", RuntimeError(secret)))
        return (Outcome.SUCCEEDED, None)

    processor = FakeProcessor(fn=_on_process)
    logger, handler = _make_logger("test.pool.job_error")
    options = PoolOptions(
        pool_size=1,
        shutdown_timeout_seconds=1.0,
        logger=logger,
    )
    pool = Pool(reader, processor, options)
    stop_event = asyncio.Event()
    run_task = asyncio.create_task(pool.run(stop_event))

    # Wait until both jobs are processed
    while len(processor.calls) < 2:
        await asyncio.sleep(0.01)

    assert processor.calls[0]["delivery"].entry_id == "bad"
    assert processor.calls[1]["delivery"].entry_id == "good"

    stop_event.set()
    await run_task

    # Verify safe logging: warning logged, secret never exposed
    log_text = " ".join(handler.messages)
    assert "worker_pool_job_failed code=processing" in log_text
    assert secret not in log_text


@pytest.mark.asyncio
async def test_pool_transient_read_error_backoff_can_be_interrupted() -> None:
    """Transient read errors log safe warning, backoff is interruptible by shutdown."""
    secret = "SECRET_TRANSIENT_READ_ERROR"
    reader = FakeReader(reads=[RuntimeError(secret)])
    processor = FakeProcessor()
    logger, handler = _make_logger("test.pool.transient")
    options = PoolOptions(
        pool_size=1,
        shutdown_timeout_seconds=1.0,
        backoff_delay_seconds=3600.0,  # 1 hour backoff, must be interrupted immediately
        logger=logger,
    )
    pool = Pool(reader, processor, options)
    stop_event = asyncio.Event()
    run_task = asyncio.create_task(pool.run(stop_event))

    # Wait for reader to be invoked
    await reader.started.wait()
    await asyncio.sleep(0.02)

    # Interrupt the long backoff via stop_event
    stop_event.set()
    await asyncio.wait_for(run_task, timeout=1.0)

    # Ensure backoff did not hot-loop
    assert reader.calls == 1

    # Safe log verification
    log_text = " ".join(handler.messages)
    assert "worker_pool_read_failed code=transient" in log_text
    assert secret not in log_text


@pytest.mark.asyncio
async def test_pool_wrapped_group_missing_stops_and_preserves_cause() -> None:
    """Fatal missing consumer group stops pool immediately, raises PoolGroupMissingError."""
    cause = ConsumerGroupMissingError("mystream", "mygroup")
    wrapped = RuntimeError("outer private detail")
    wrapped.__cause__ = cause

    reader = FakeReader(reads=[wrapped])
    processor = FakeProcessor()
    logger, handler = _make_logger("test.pool.nogroup")
    options = PoolOptions(
        pool_size=2,
        shutdown_timeout_seconds=1.0,
        logger=logger,
    )
    pool = Pool(reader, processor, options)

    with pytest.raises(PoolGroupMissingError) as exc_info:
        await pool.run()

    err = exc_info.value
    # is_group_missing returns True on PoolGroupMissingError
    assert is_group_missing(err) is True
    # str(err) does not leak outer private details
    assert str(err) == "worker pool stopped: consumer group is missing"
    assert "outer private detail" not in str(err)

    # __cause__ preserves the original error
    assert err.__cause__ is wrapped

    # Safe logs check
    log_text = " ".join(handler.messages)
    assert "outer private detail" not in log_text

    # No deliveries processed
    assert len(processor.calls) == 0


@pytest.mark.asyncio
async def test_pool_late_delivery_after_cancellation_is_not_processed() -> None:
    """A delivery returned after admission closes is dropped and never passed to processor."""
    reader_entered = asyncio.Event()
    reader_release = asyncio.Event()

    class LateDeliveryReader:
        def __init__(self) -> None:
            self.calls = 0

        async def read(self) -> Delivery | None:
            self.calls += 1
            reader_entered.set()
            await reader_release.wait()
            # Deliberately returns delivery after admission is canceled
            return Delivery(entry_id="late", fields={})

    reader = LateDeliveryReader()
    processor = FakeProcessor()
    options = PoolOptions(pool_size=1, shutdown_timeout_seconds=1.0)
    pool = Pool(reader, processor, options)

    stop_event = asyncio.Event()
    run_task = asyncio.create_task(pool.run(stop_event))

    await reader_entered.wait()
    # Close admission while reader is blocked
    stop_event.set()
    await asyncio.sleep(0.01)

    # Release reader to return late delivery
    reader_release.set()
    await run_task

    # Delivery must NOT be processed
    assert len(processor.calls) == 0


@pytest.mark.asyncio
async def test_pool_drain_timer_runs_while_long_reader_and_confirmed_job_remain() -> None:
    """Drain timer sets job_canceled on timeout even while a blocked reader is still active."""
    reader_entered = asyncio.Event()
    reader_release = asyncio.Event()

    class LongReader:
        def __init__(self) -> None:
            self.calls = 0

        async def read(self) -> Delivery | None:
            self.calls += 1
            if self.calls == 1:
                return Delivery(entry_id="confirmed", fields={})
            reader_entered.set()
            try:
                await reader_release.wait()
            except asyncio.CancelledError:
                # Deliberately ignore cancellation to simulate blocked reader
                await reader_release.wait()
            return None

    job_entered = asyncio.Event()
    job_release = asyncio.Event()

    async def _on_process(call: dict[str, Any]) -> None:
        job_entered.set()
        await job_release.wait()

    reader = LongReader()
    processor = FakeProcessor(fn=_on_process)
    options = PoolOptions(
        pool_size=2,
        shutdown_timeout_seconds=0.03,  # 30ms drain timer
    )
    pool = Pool(reader, processor, options)
    stop_event = asyncio.Event()
    run_task = asyncio.create_task(pool.run(stop_event))

    # Wait until confirmed job is in processor and worker 2 is in long read
    await job_entered.wait()
    await reader_entered.wait()

    # Close admission
    stop_event.set()

    # Verify that after drain timeout (30ms), job_canceled is set while reader is still blocked
    call = processor.calls[0]
    job_canceled_event: asyncio.Event = call["job_canceled"]
    await asyncio.wait_for(job_canceled_event.wait(), timeout=1.0)
    assert job_canceled_event.is_set()

    # Ensure run() has NOT returned yet because reader and processor are still running
    assert not run_task.done()

    # Release both and join cleanly
    reader_release.set()
    job_release.set()
    await run_task
    assert run_task.done()


@pytest.mark.asyncio
async def test_pool_pre_stopped_and_single_run_lifecycle() -> None:
    """Pre-stopped event exits cleanly with 0 reads; second run raises RuntimeError."""
    reader = FakeReader()
    processor = FakeProcessor()
    options = PoolOptions(pool_size=1)
    pool = Pool(reader, processor, options)

    # 1. Stop event pre-set
    stop_event = asyncio.Event()
    stop_event.set()
    await pool.run(stop_event)
    assert reader.calls == 0

    # 2. Second run call rejected
    with pytest.raises(RuntimeError, match="worker pool can only be run once"):
        await pool.run()


@pytest.mark.asyncio
async def test_pool_stop_method_triggers_drain_and_exits() -> None:
    """Calling pool.stop() externally triggers admission closure and graceful drain."""
    reader = FakeReader(
        reads=[
            Delivery(entry_id="one", fields={}),
            Delivery(entry_id="two", fields={}),
        ]
    )
    processor = FakeProcessor()
    options = PoolOptions(pool_size=1, shutdown_timeout_seconds=1.0)
    pool = Pool(reader, processor, options)

    run_task = asyncio.create_task(pool.run())

    # Wait until first job is processed
    while len(processor.calls) < 1:
        await asyncio.sleep(0.01)

    # Call pool.stop()
    pool.stop()
    await run_task

    assert run_task.done()


@pytest.mark.asyncio
async def test_pool_run_task_cancellation_drains_cleanly() -> None:
    """Canceling the task awaiting pool.run() initiates graceful drain and joins."""
    job_entered = asyncio.Event()
    job_release = asyncio.Event()

    async def _on_process(call: dict[str, Any]) -> None:
        job_entered.set()
        await job_release.wait()

    reader = FakeReader(reads=[Delivery(entry_id="job-1", fields={})])
    processor = FakeProcessor(fn=_on_process)
    options = PoolOptions(pool_size=1, shutdown_timeout_seconds=1.0)
    pool = Pool(reader, processor, options)

    run_task = asyncio.create_task(pool.run())

    await job_entered.wait()
    # Cancel run_task from outside
    run_task.cancel()
    await asyncio.sleep(0.01)

    # Job is still running; run_task must not be done prematurely
    assert not run_task.done()
    # Admission is closed
    call = processor.calls[0]
    adm_event: asyncio.Event = call["admission_closed"]
    assert adm_event.is_set()

    # Release job; run_task completes cleanly
    job_release.set()
    await run_task
    assert run_task.done()


@pytest.mark.asyncio
async def test_pool_exception_group_missing_stops_and_preserves_cause() -> None:
    """ExceptionGroup containing ConsumerGroupMissingError is recognized as fatal."""
    cause = ConsumerGroupMissingError("stream-1", "group-1")
    group_exc = ExceptionGroup("batch errors", [cause])

    reader = FakeReader(reads=[group_exc])
    processor = FakeProcessor()
    options = PoolOptions(pool_size=1)
    pool = Pool(reader, processor, options)

    with pytest.raises(PoolGroupMissingError) as exc_info:
        await pool.run()

    err = exc_info.value
    assert is_group_missing(err) is True
    assert str(err) == "worker pool stopped: consumer group is missing"
    assert err.__cause__ is group_exc


@pytest.mark.asyncio
async def test_pool_unexpected_processor_exception_logs_warning_and_continues() -> None:
    """An unexpected unhandled exception in processor is caught, logged safely, and does not kill pool."""
    secret = "SECRET_UNEXPECTED_EXCEPTION_DETAIL"
    reader = FakeReader(
        reads=[
            Delivery(entry_id="crash", fields={}),
            Delivery(entry_id="ok", fields={}),
        ]
    )

    async def _on_process(call: dict[str, Any]) -> None:
        d: Delivery = call["delivery"]
        if d.entry_id == "crash":
            raise RuntimeError(secret)

    processor = FakeProcessor(fn=_on_process)
    logger, handler = _make_logger("test.pool.crash")
    options = PoolOptions(pool_size=1, logger=logger)
    pool = Pool(reader, processor, options)
    stop_event = asyncio.Event()
    run_task = asyncio.create_task(pool.run(stop_event))

    while len(processor.calls) < 2:
        await asyncio.sleep(0.01)

    stop_event.set()
    await run_task

    log_text = " ".join(handler.messages)
    assert "worker_pool_job_failed code=processing" in log_text
    assert secret not in log_text


@pytest.mark.asyncio
async def test_pool_idle_stop_returns_immediately_without_waiting_drain_timeout() -> None:
    """An idle pool stops immediately without waiting for shutdown_timeout_seconds."""
    reader = FakeReader()
    processor = FakeProcessor()
    options = PoolOptions(pool_size=2, shutdown_timeout_seconds=10.0)
    pool = Pool(reader, processor, options)

    stop_event = asyncio.Event()
    run_task = asyncio.create_task(pool.run(stop_event))
    await asyncio.sleep(0.01)

    stop_event.set()
    # Must return almost immediately, well before 10s
    await asyncio.wait_for(run_task, timeout=0.2)
    assert run_task.done()


@pytest.mark.asyncio
async def test_pool_pre_canceled_task_exits_cleanly() -> None:
    """A task canceled before pool.run() starts reading exits cleanly with 0 reads."""
    reader = FakeReader()
    processor = FakeProcessor()
    pool = Pool(reader, processor)

    async def _run() -> None:
        task = asyncio.current_task()
        if task is not None:
            task.cancel()
        await pool.run()

    task = asyncio.create_task(_run())
    await task
    assert reader.calls == 0

