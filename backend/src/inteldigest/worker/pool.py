"""Bounded concurrency worker pool and graceful drain (M15)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import math
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from inteldigest.queue.consumer import (
    ConsumerGroupMissingError,
    Delivery,
    is_group_missing,
)
from inteldigest.worker.processor import Outcome, ProcessError


@runtime_checkable
class QueueReader(Protocol):
    """Queue reader supplying one delivery at a time."""

    async def read(self) -> Delivery | None: ...


@runtime_checkable
class JobProcessor(Protocol):
    """Job processor processing deliveries with admission and cancellation events."""

    async def process(
        self,
        delivery: Delivery,
        *,
        admission_closed: asyncio.Event | None = None,
        job_canceled: asyncio.Event | None = None,
    ) -> tuple[Outcome, ProcessError | None]: ...


@dataclass(frozen=True, slots=True)
class PoolOptions:
    """Configuration options for worker pool concurrency, backoff, and drain."""

    pool_size: int = 4
    shutdown_timeout_seconds: float = 10.0
    backoff_delay_seconds: float = 1.0
    logger: logging.Logger | None = None

    def __post_init__(self) -> None:
        if (
            isinstance(self.pool_size, bool)
            or not isinstance(self.pool_size, int)
            or not (1 <= self.pool_size <= 64)
        ):
            raise ValueError("worker pool size must be between 1 and 64")
        if (
            isinstance(self.shutdown_timeout_seconds, bool)
            or not isinstance(self.shutdown_timeout_seconds, (int, float))
            or not math.isfinite(self.shutdown_timeout_seconds)
            or self.shutdown_timeout_seconds <= 0
            or isinstance(self.backoff_delay_seconds, bool)
            or not isinstance(self.backoff_delay_seconds, (int, float))
            or not math.isfinite(self.backoff_delay_seconds)
            or self.backoff_delay_seconds <= 0
        ):
            raise ValueError("worker pool shutdown timeout and backoff must be positive")


def default_pool_options() -> PoolOptions:
    """Return default pool options with 4 workers and 10s shutdown timeout."""
    return PoolOptions(
        pool_size=4,
        shutdown_timeout_seconds=10.0,
        backoff_delay_seconds=1.0,
        logger=None,
    )


class PoolGroupMissingError(ConsumerGroupMissingError):
    """Raised when worker pool stops due to a fatal missing Redis consumer group."""

    def __init__(
        self,
        message: str = "worker pool stopped: consumer group is missing",
        *,
        cause: BaseException | None = None,
    ) -> None:
        self.stream = getattr(cause, "stream", "")
        self.group = getattr(cause, "group", "")
        super(ConsumerGroupMissingError, self).__init__(message)
        self.cause = cause
        if cause is not None:
            self.__cause__ = cause

    def __str__(self) -> str:
        return "worker pool stopped: consumer group is missing"


def _is_group_missing(err: BaseException | None) -> bool:
    """Recursively determine if any chained cause or exception represents a missing consumer group."""
    if err is None:
        return False
    if is_group_missing(err):
        return True
    seen: set[int] = set()
    stack: list[BaseException] = [err]
    while stack:
        curr = stack.pop()
        if id(curr) in seen:
            continue
        seen.add(id(curr))
        if isinstance(curr, ConsumerGroupMissingError):
            return True
        if isinstance(curr, BaseExceptionGroup):
            stack.extend(curr.exceptions)
        next_exc = getattr(curr, "cause", None) or curr.__cause__ or curr.__context__
        if isinstance(next_exc, BaseException):
            stack.append(next_exc)
    return False


class Pool:
    """Worker pool bounding concurrency without an accumulating in-memory queue."""

    def __init__(
        self,
        reader: QueueReader,
        processor: JobProcessor,
        options: PoolOptions | None = None,
    ) -> None:
        if reader is None or processor is None:
            raise ValueError("worker pool dependencies are required")
        opts = options or default_pool_options()
        if not isinstance(opts, PoolOptions):
            raise TypeError("worker pool options must be an instance of PoolOptions")
        self._reader = reader
        self._processor = processor
        self._options = opts
        self._logger = opts.logger or logging.getLogger("inteldigest.worker.pool")
        self._ran = False
        self._stopped = False
        self._admission_closed: asyncio.Event | None = None
        self._stopping: asyncio.Event | None = None

    @property
    def options(self) -> PoolOptions:
        """Return the configuration options of this worker pool."""
        return self._options

    def stop(self) -> None:
        """Signal the pool to close admission and initiate graceful drain."""
        self._stopped = True
        if self._admission_closed is not None and not self._admission_closed.is_set():
            self._admission_closed.set()
        if self._stopping is not None and not self._stopping.is_set():
            self._stopping.set()

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        """Run exactly pool_size sequential workers until stopped, drained, or canceled."""
        if self._ran:
            raise RuntimeError("worker pool can only be run once")
        self._ran = True

        cur_task = asyncio.current_task()
        is_cancelling = (
            cur_task is not None
            and hasattr(cur_task, "cancelling")
            and cur_task.cancelling() > 0
        )
        if (
            self._stopped
            or (stop_event is not None and stop_event.is_set())
            or is_cancelling
        ):
            if is_cancelling and cur_task is not None:
                while cur_task.cancelling() > 0:
                    cur_task.uncancel()
            return

        admission_closed = asyncio.Event()
        self._admission_closed = admission_closed
        stopping = asyncio.Event()
        self._stopping = stopping
        job_canceled = asyncio.Event()
        workers_done = asyncio.Event()
        fatal_error: BaseException | None = None

        def _is_admission_closed() -> bool:
            if admission_closed.is_set():
                return True
            if stop_event is not None and stop_event.is_set():
                _stop(None)
                return True
            return False

        def _stop(err: BaseException | None = None) -> None:
            nonlocal fatal_error
            if err is not None and fatal_error is None:
                fatal_error = err
            if not admission_closed.is_set():
                admission_closed.set()
            if not stopping.is_set():
                stopping.set()

        async def _drain_timer() -> None:
            await stopping.wait()
            try:
                async with asyncio.timeout(self._options.shutdown_timeout_seconds):
                    await workers_done.wait()
            except TimeoutError:
                job_canceled.set()

        async def _stop_watcher() -> None:
            if stop_event is None:
                return
            stop_task = asyncio.create_task(stop_event.wait())
            stopping_task = asyncio.create_task(stopping.wait())
            try:
                done, _ = await asyncio.wait(
                    [stop_task, stopping_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if stop_task in done:
                    _stop(None)
            finally:
                stop_task.cancel()
                stopping_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await stop_task
                with contextlib.suppress(asyncio.CancelledError):
                    await stopping_task

        async def _worker_loop() -> None:
            while not _is_admission_closed():
                delivery: Delivery | None = None
                try:
                    read_task = asyncio.create_task(self._reader.read())
                    admission_task = asyncio.create_task(admission_closed.wait())
                    try:
                        done, _ = await asyncio.wait(
                            [read_task, admission_task],
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        if admission_task in done or _is_admission_closed():
                            read_task.cancel()
                            with contextlib.suppress(asyncio.CancelledError, Exception):
                                await read_task
                            return
                        delivery = read_task.result()
                    finally:
                        if not admission_task.done():
                            admission_task.cancel()
                            with contextlib.suppress(asyncio.CancelledError):
                                await admission_task
                except Exception as exc:  # noqa: BLE001
                    if _is_group_missing(exc):
                        _stop(exc)
                        return
                    if _is_admission_closed():
                        return
                    self._logger.warning("worker_pool_read_failed code=transient")
                    try:
                        async with asyncio.timeout(self._options.backoff_delay_seconds):
                            await admission_closed.wait()
                        return
                    except TimeoutError:
                        continue

                if _is_admission_closed():
                    return
                if delivery is None:
                    continue

                try:
                    _, process_err = await self._processor.process(
                        delivery,
                        admission_closed=admission_closed,
                        job_canceled=job_canceled,
                    )
                    if process_err is not None:
                        self._logger.warning("worker_pool_job_failed code=processing")
                except Exception:  # noqa: BLE001
                    self._logger.warning("worker_pool_job_failed code=processing")

        drain_task = asyncio.create_task(_drain_timer())
        watcher_task = asyncio.create_task(_stop_watcher())
        worker_tasks = [
            asyncio.create_task(_worker_loop())
            for _ in range(self._options.pool_size)
        ]

        try:
            all_workers = asyncio.gather(*worker_tasks)
            while not all_workers.done():
                try:
                    await asyncio.shield(all_workers)
                except asyncio.CancelledError:
                    _stop(None)
                    cur = asyncio.current_task()
                    if cur is not None and hasattr(cur, "uncancel"):
                        while cur.cancelling() > 0:
                            cur.uncancel()
        finally:
            workers_done.set()
            drain_task.cancel()
            watcher_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await drain_task
            with contextlib.suppress(asyncio.CancelledError):
                await watcher_task

        if fatal_error is not None:
            raise PoolGroupMissingError(cause=fatal_error)
