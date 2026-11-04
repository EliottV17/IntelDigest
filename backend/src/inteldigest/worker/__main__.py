"""Standalone Python worker entrypoint, lifecycle orchestration, and signals (M16)."""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import signal
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from inteldigest.config import WorkerConfig, load_worker
from inteldigest.jobs.repository import Repository
from inteldigest.queue.consumer import Consumer
from inteldigest.scraper.extraction import Scraper
from inteldigest.scraper.fetcher import ScraperOptions
from inteldigest.worker.pool import Pool, PoolOptions
from inteldigest.worker.processor import Processor, ProcessorOptions
from inteldigest.worker.receiver import MetadataReceiver


class WorkerStartupError(Exception):
    """Coded worker startup/lifecycle error that preserves cause while masking secrets."""

    def __init__(self, code: str, cause: BaseException | None = None) -> None:
        self.code = code
        self.cause = cause
        super().__init__(f"worker: {code}")
        if cause is not None:
            self.__cause__ = cause

    def __str__(self) -> str:
        return f"worker: {self.code}"

    def __repr__(self) -> str:
        return f"WorkerStartupError({self.code!r}, cause={self.cause!r})"


def exit_code(err: BaseException | None) -> int:
    """Return process exit code: 0 on success, 1 on any error."""
    if err is None:
        return 0
    return 1


def exit_code_label(err: BaseException | None) -> str:
    """Return error code label: err.code for WorkerStartupError, else 'worker_failed'."""
    if isinstance(err, WorkerStartupError):
        return err.code
    if isinstance(err, BaseExceptionGroup):
        for exc in err.exceptions:
            if isinstance(exc, WorkerStartupError):
                return exc.code
    return "worker_failed"


@dataclass
class WorkerDependencies:
    """Injectable dependency factories for worker lifecycle orchestration."""

    load_config: Callable[[], WorkerConfig] | None = None
    register_signals: Callable[[asyncio.Event], Callable[[], None]] | None = None
    new_db: Callable[..., Any] | None = None
    new_consumer: Callable[..., Any] | None = None
    new_scraper: Callable[..., Any] | None = None
    new_processor: Callable[..., Any] | None = None
    new_pool: Callable[..., Any] | None = None
    logger: logging.Logger | None = None


def default_register_signals(stop_event: asyncio.Event) -> Callable[[], None]:
    """Register SIGINT/SIGTERM handlers on the running loop and return unregister hook."""
    loop = asyncio.get_running_loop()
    registered_sigs: list[signal.Signals] = []

    def on_signal() -> None:
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, on_signal)
            registered_sigs.append(sig)
        except (NotImplementedError, RuntimeError):
            pass

    def unregister() -> None:
        for sig in registered_sigs:
            with contextlib.suppress(Exception):
                loop.remove_signal_handler(sig)

    return unregister


async def default_new_db(database_url: str) -> Any:
    """Construct PostgreSQL asyncpg connection pool."""
    import asyncpg

    return await asyncpg.create_pool(database_url)


async def default_new_consumer(cfg: WorkerConfig) -> Consumer:
    """Construct connected and verified Redis stream consumer."""
    return await Consumer.connect(
        redis_url=cfg.redis_url,
        stream=cfg.redis_stream,
        group=cfg.consumer_group,
        consumer_name=cfg.consumer_name,
        dial_timeout_seconds=cfg.redis_dial_timeout_seconds,
        stream_block_seconds=cfg.stream_block_seconds,
        operation_timeout_seconds=cfg.queue_operation_timeout_seconds,
    )


def default_new_scraper(options: ScraperOptions) -> Scraper:
    """Construct article scraper with configured limits."""
    return Scraper(options=options)


def default_new_processor(
    repo: Any,
    acker: Any,
    scraper: Any,
    receiver: Any,
    options: ProcessorOptions,
) -> Processor:
    """Construct job delivery processor."""
    return Processor(
        repo=repo,
        acker=acker,
        scraper=scraper,
        receiver=receiver,
        options=options,
    )


def default_new_pool(
    reader: Any,
    processor: Any,
    options: PoolOptions,
) -> Pool:
    """Construct bounded concurrency worker pool."""
    return Pool(
        reader=reader,
        processor=processor,
        options=options,
    )


def production_dependencies() -> WorkerDependencies:
    """Build default production worker dependencies."""
    return WorkerDependencies(
        load_config=load_worker,
        register_signals=default_register_signals,
        new_db=default_new_db,
        new_consumer=default_new_consumer,
        new_scraper=default_new_scraper,
        new_processor=default_new_processor,
        new_pool=default_new_pool,
        logger=None,
    )


async def _close_handle(handle: Any) -> None:
    if handle is None:
        return
    for method_name in ("aclose", "close"):
        close_fn = getattr(handle, method_name, None)
        if callable(close_fn):
            res = close_fn()
            if inspect.isawaitable(res):
                await res
            break


async def _ping_db(db: Any) -> None:
    if hasattr(db, "ping") and callable(db.ping):
        res = db.ping()
        if inspect.isawaitable(res):
            await res
    elif hasattr(db, "execute") and callable(db.execute):
        res = db.execute("SELECT 1")
        if inspect.isawaitable(res):
            await res
    elif hasattr(db, "fetchrow") and callable(db.fetchrow):
        res = db.fetchrow("SELECT 1")
        if inspect.isawaitable(res):
            await res
    elif hasattr(db, "fetchval") and callable(db.fetchval):
        res = db.fetchval("SELECT 1")
        if inspect.isawaitable(res):
            await res
    else:
        raise AttributeError("database handle does not support ping or SELECT 1")


def _unpack_result(res: Any) -> tuple[Any, Exception | None]:
    if isinstance(res, tuple) and len(res) == 2 and isinstance(res[1], BaseException):
        return res[0], res[1]
    return res, None


async def run_with_dependencies(
    deps: WorkerDependencies | None = None,
    *,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Run worker lifecycle with dependency injection, strict ordering, and clean shutdown."""
    if deps is None:
        deps = production_dependencies()

    if (
        deps.load_config is None
        or not callable(deps.load_config)
        or deps.register_signals is None
        or not callable(deps.register_signals)
        or deps.new_db is None
        or not callable(deps.new_db)
        or deps.new_consumer is None
        or not callable(deps.new_consumer)
        or deps.new_scraper is None
        or not callable(deps.new_scraper)
        or deps.new_processor is None
        or not callable(deps.new_processor)
        or deps.new_pool is None
        or not callable(deps.new_pool)
    ):
        raise WorkerStartupError("invalid_dependencies")

    if stop_event is None:
        stop_event = asyncio.Event()

    # Step 1: Register signals before loading configuration or opening dependencies
    try:
        unregister = deps.register_signals(stop_event)
    except Exception as exc:
        raise WorkerStartupError("signal_context_unavailable", exc) from exc
    if not callable(unregister):
        raise WorkerStartupError("signal_context_unavailable")

    unregistered = False

    def stop_signals() -> None:
        nonlocal unregistered
        if not unregistered:
            unregistered = True
            with contextlib.suppress(Exception):
                unregister()

    database: Any = None
    consumer: Any = None
    article_scraper: Any = None
    run_err: WorkerStartupError | None = None
    consumer_close_err: WorkerStartupError | None = None

    try:
        # Step 2: Load worker configuration
        try:
            cfg = deps.load_config()
        except Exception as exc:
            raise WorkerStartupError("configuration_failed", exc) from exc
        if cfg is None:
            raise WorkerStartupError("configuration_unavailable")
        if stop_event.is_set():
            raise WorkerStartupError("startup_canceled")

        # Step 3: Open PostgreSQL (bounded by db_timeout_seconds) and verify ping
        try:
            async with asyncio.timeout(cfg.db_timeout_seconds):
                res = deps.new_db(cfg.database_url)
                if inspect.isawaitable(res):
                    res = await res
        except Exception as exc:
            raise WorkerStartupError("database_open_failed", exc) from exc
        db_handle, open_err = _unpack_result(res)
        database = db_handle
        if open_err is not None:
            raise WorkerStartupError("database_open_failed", open_err) from open_err
        if database is None:
            raise WorkerStartupError("database_unavailable")
        if stop_event.is_set():
            raise WorkerStartupError("startup_canceled")

        try:
            async with asyncio.timeout(cfg.db_timeout_seconds):
                await _ping_db(database)
        except Exception as exc:
            raise WorkerStartupError("database_ping_failed", exc) from exc
        if stop_event.is_set():
            raise WorkerStartupError("startup_canceled")

        # Step 4: Open Redis consumer and ensure consumer group
        try:
            res = deps.new_consumer(cfg)
            if inspect.isawaitable(res):
                res = await res
        except Exception as exc:
            raise WorkerStartupError("consumer_open_failed", exc) from exc
        consumer_handle, cons_err = _unpack_result(res)
        consumer = consumer_handle
        if cons_err is not None:
            raise WorkerStartupError("consumer_open_failed", cons_err) from cons_err
        if consumer is None:
            raise WorkerStartupError("consumer_unavailable")
        if stop_event.is_set():
            raise WorkerStartupError("startup_canceled")

        try:
            async with asyncio.timeout(cfg.queue_operation_timeout_seconds):
                res = consumer.ensure_group()
                if inspect.isawaitable(res):
                    await res
        except Exception as exc:
            raise WorkerStartupError("consumer_group_failed", exc) from exc
        if stop_event.is_set():
            raise WorkerStartupError("startup_canceled")

        # Step 5: Initialize Scraper, Repository, Processor (with MetadataReceiver), and Pool
        scraper_opts = ScraperOptions(
            connect_timeout_seconds=cfg.scraper_connect_timeout_seconds,
            tls_timeout_seconds=cfg.scraper_tls_timeout_seconds,
            header_timeout_seconds=cfg.scraper_header_timeout_seconds,
            read_timeout_seconds=cfg.scraper_read_timeout_seconds,
            request_timeout_seconds=cfg.scraper_request_timeout_seconds,
            max_body_bytes=cfg.scraper_max_body_bytes,
            max_redirects=cfg.scraper_max_redirects,
        )
        try:
            res = deps.new_scraper(scraper_opts)
            if inspect.isawaitable(res):
                res = await res
        except Exception as exc:
            raise WorkerStartupError("scraper_init_failed", exc) from exc
        scr_handle, scr_err = _unpack_result(res)
        article_scraper = scr_handle
        if scr_err is not None:
            raise WorkerStartupError("scraper_init_failed", scr_err) from scr_err
        if article_scraper is None:
            raise WorkerStartupError("scraper_unavailable")
        if stop_event.is_set():
            raise WorkerStartupError("startup_canceled")

        base_logger = deps.logger or logging.getLogger("inteldigest.worker")
        job_logger = logging.LoggerAdapter(
            base_logger,
            {"consumer_group": cfg.consumer_group, "consumer_name": cfg.consumer_name},
        )
        repo = Repository(database)
        receiver = MetadataReceiver(logger=job_logger)
        processor_opts = ProcessorOptions(
            db_timeout_seconds=cfg.db_timeout_seconds,
            queue_operation_timeout_seconds=cfg.queue_operation_timeout_seconds,
            logger=job_logger,
        )
        try:
            res = deps.new_processor(repo, consumer, article_scraper, receiver, processor_opts)
            if inspect.isawaitable(res):
                res = await res
        except Exception as exc:
            raise WorkerStartupError("processor_init_failed", exc) from exc
        proc_handle, proc_err = _unpack_result(res)
        processor = proc_handle
        if proc_err is not None:
            raise WorkerStartupError("processor_init_failed", proc_err) from proc_err
        if processor is None:
            raise WorkerStartupError("processor_unavailable")
        if stop_event.is_set():
            raise WorkerStartupError("startup_canceled")

        pool_opts = PoolOptions(
            pool_size=cfg.pool_size,
            shutdown_timeout_seconds=cfg.shutdown_timeout_seconds,
            backoff_delay_seconds=1.0,
            logger=job_logger,
        )
        try:
            res = deps.new_pool(consumer, processor, pool_opts)
            if inspect.isawaitable(res):
                res = await res
        except Exception as exc:
            raise WorkerStartupError("pool_init_failed", exc) from exc
        pool_handle, pool_err = _unpack_result(res)
        pool = pool_handle
        if pool_err is not None:
            raise WorkerStartupError("pool_init_failed", pool_err) from pool_err
        if pool is None:
            raise WorkerStartupError("pool_unavailable")

        job_logger.info(
            "worker_started consumer_group=%s consumer_name=%s pool_size=%d",
            cfg.consumer_group,
            cfg.consumer_name,
            cfg.pool_size,
            extra={
                "consumer_group": cfg.consumer_group,
                "consumer_name": cfg.consumer_name,
                "pool_size": cfg.pool_size,
            },
        )

        # Step 6: Run pool and wait for all workers to join before cleanup
        try:
            await pool.run(stop_event)
        except Exception as exc:  # noqa: BLE001
            run_err = WorkerStartupError("worker_run_failed", exc)

    finally:
        # Step 7: Ordered resource closure: scraper -> consumer -> database
        if article_scraper is not None:
            with contextlib.suppress(Exception):
                await _close_handle(article_scraper)
        try:
            if consumer is not None:
                await _close_handle(consumer)
        except Exception as exc:  # noqa: BLE001
            consumer_close_err = WorkerStartupError("consumer_close_failed", exc)
        finally:
            if database is not None:
                with contextlib.suppress(Exception):
                    await _close_handle(database)
        stop_signals()

    if run_err is not None:
        if consumer_close_err is not None:
            run_err.close_error = consumer_close_err  # type: ignore[attr-defined]
            run_err.__context__ = consumer_close_err
        raise run_err
    if consumer_close_err is not None:
        raise consumer_close_err


def run(
    deps: WorkerDependencies | None = None,
    *,
    stop_event: asyncio.Event | None = None,
) -> int:
    """Synchronous runner for standalone worker process."""
    err: BaseException | None = None
    try:
        try:
            asyncio.get_running_loop()
            in_loop = True
        except RuntimeError:
            in_loop = False

        if in_loop:
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(
                    asyncio.run,
                    run_with_dependencies(deps=deps, stop_event=stop_event),
                )
                future.result()
        else:
            asyncio.run(run_with_dependencies(deps=deps, stop_event=stop_event))
    except BaseException as exc:  # noqa: BLE001
        err = exc
        logger = deps.logger if deps and deps.logger else logging.getLogger("inteldigest.worker")
        logger.error(
            "worker_exit error_code=%s",
            exit_code_label(err),
            extra={"error_code": exit_code_label(err)},
        )
    return exit_code(err)


def main() -> int:
    """CLI entrypoint for python -m inteldigest.worker."""
    return run()


if __name__ == "__main__":
    import sys

    sys.exit(main())
