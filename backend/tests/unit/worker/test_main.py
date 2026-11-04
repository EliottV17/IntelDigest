"""Unit tests for standalone Python worker lifecycle, signals, and safe exit codes (M16)."""

from __future__ import annotations

import asyncio
import logging
from typing import Any
from unittest.mock import patch
from uuid import uuid4

import pytest

from inteldigest.config import WorkerConfig
from inteldigest.queue.consumer import (
    ConsumerGroupMissingError,
    Delivery,
    is_group_missing,
)
from inteldigest.scraper.fetcher import ScraperOptions
from inteldigest.scraper.models import (
    Article,
    ScrapeError,
    ScrapeErrorKind,
    ScrapeStage,
)
from inteldigest.worker import (
    MetadataReceiver,
    PoolGroupMissingError,
    PoolOptions,
    ProcessorOptions,
    Result,
    WorkerDependencies,
    WorkerStartupError,
    exit_code,
    exit_code_label,
    main,
    production_dependencies,
    run,
    run_with_dependencies,
)


class LogCaptureHandler(logging.Handler):
    """Logging handler capturing emitted messages."""

    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)
        self.messages.append(self.format(record))


def _make_logger(name: str = "test.worker.main") -> tuple[logging.Logger, LogCaptureHandler]:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    handler = LogCaptureHandler()
    logger.handlers = [handler]
    logger.propagate = False
    return logger, handler


def _sample_config() -> WorkerConfig:
    return WorkerConfig(
        database_url="postgres://user:secret-pass@localhost:5432/inteldigest",
        redis_url="redis://:secret-token@localhost:6379/0",
        redis_stream="inteldigest:jobs",
        redis_dial_timeout_seconds=5.0,
        pool_size=4,
        consumer_group="inteldigest:workers",
        consumer_name="worker-host-1",
        stream_block_seconds=2.0,
        queue_operation_timeout_seconds=5.0,
        queue_read_timeout_seconds=7.0,
        db_timeout_seconds=5.0,
        shutdown_timeout_seconds=10.0,
        scraper_connect_timeout_seconds=5.0,
        scraper_tls_timeout_seconds=5.0,
        scraper_header_timeout_seconds=10.0,
        scraper_read_timeout_seconds=10.0,
        scraper_request_timeout_seconds=30.0,
        scraper_max_body_bytes=5_242_880,
        scraper_max_body_read_bytes=5_242_881,
        scraper_max_redirects=5,
    )


class FakeDB:
    def __init__(self, events: list[str], *, ping_err: Exception | None = None) -> None:
        self.events = events
        self.ping_err = ping_err
        self.closed = False
        self.ping_calls = 0

    async def ping(self) -> None:
        self.events.append("db-ping")
        self.ping_calls += 1
        if self.ping_err:
            raise self.ping_err

    async def close(self) -> None:
        self.events.append("db-close")
        self.closed = True

    async def fetchrow(self, query: str, *args: Any) -> Any:
        return None


class FakeConsumer:
    def __init__(
        self,
        events: list[str],
        *,
        group_err: Exception | None = None,
        close_err: Exception | None = None,
    ) -> None:
        self.events = events
        self.group_err = group_err
        self.close_err = close_err
        self.closed = False
        self.group_calls = 0

    async def ensure_group(self) -> None:
        self.events.append("consumer-ensure-group")
        self.group_calls += 1
        if self.group_err:
            raise self.group_err

    async def aclose(self) -> None:
        self.events.append("consumer-close")
        self.closed = True
        if self.close_err:
            raise self.close_err

    async def read(self) -> Delivery | None:
        return None

    async def ack(self, entry_id: str) -> None:
        pass


class FakeScraper:
    def __init__(self, events: list[str], *, close_err: Exception | None = None) -> None:
        self.events = events
        self.close_err = close_err
        self.closed = False

    async def close(self) -> None:
        self.events.append("scraper-close")
        self.closed = True
        if self.close_err:
            raise self.close_err


class FakeProcessor:
    def __init__(self, events: list[str]) -> None:
        self.events = events


class FakePool:
    def __init__(self, events: list[str], *, run_err: Exception | None = None) -> None:
        self.events = events
        self.run_err = run_err
        self.running = False
        self.ran = False

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        self.events.append("pool-run")
        self.running = True
        self.ran = True
        try:
            if self.run_err:
                raise self.run_err
        finally:
            self.running = False


def _build_test_harness(
    events: list[str],
    *,
    cfg: WorkerConfig | None = None,
    config_err: Exception | None = None,
    new_db_err: Exception | None = None,
    new_db_val: Any = "default",
    ping_err: Exception | None = None,
    new_consumer_err: Exception | None = None,
    new_consumer_val: Any = "default",
    group_err: Exception | None = None,
    consumer_close_err: Exception | None = None,
    new_scraper_err: Exception | None = None,
    new_scraper_val: Any = "default",
    scraper_close_err: Exception | None = None,
    new_processor_err: Exception | None = None,
    new_processor_val: Any = "default",
    new_pool_err: Exception | None = None,
    new_pool_val: Any = "default",
    pool_run_err: Exception | None = None,
    logger: logging.Logger | None = None,
) -> tuple[WorkerDependencies, dict[str, Any]]:
    config_obj = cfg if cfg is not None else _sample_config()
    db = FakeDB(events, ping_err=ping_err) if new_db_val == "default" else new_db_val
    consumer = (
        FakeConsumer(events, group_err=group_err, close_err=consumer_close_err)
        if new_consumer_val == "default"
        else new_consumer_val
    )
    scraper = (
        FakeScraper(events, close_err=scraper_close_err)
        if new_scraper_val == "default"
        else new_scraper_val
    )
    processor = FakeProcessor(events) if new_processor_val == "default" else new_processor_val
    pool = FakePool(events, run_err=pool_run_err) if new_pool_val == "default" else new_pool_val

    recorded_args: dict[str, Any] = {
        "db": db,
        "consumer": consumer,
        "scraper": scraper,
        "processor": processor,
        "pool": pool,
    }

    def register_signals(stop_event: asyncio.Event) -> Any:
        events.append("signal-register")

        def unregister() -> None:
            events.append("signal-unregister")

        return unregister

    def load_config() -> WorkerConfig:
        events.append("load-config")
        if config_err:
            raise config_err
        return config_obj

    def new_db(dsn: str) -> Any:
        events.append("db-new")
        recorded_args["db_dsn"] = dsn
        if new_db_err:
            if db is not None:
                return db, new_db_err
            raise new_db_err
        return db

    def new_consumer(conf: WorkerConfig) -> Any:
        events.append("consumer-new")
        recorded_args["consumer_conf"] = conf
        if new_consumer_err:
            if consumer is not None:
                return consumer, new_consumer_err
            raise new_consumer_err
        return consumer

    def new_scraper(opts: ScraperOptions) -> Any:
        events.append("scraper-new")
        recorded_args["scraper_opts"] = opts
        if new_scraper_err:
            raise new_scraper_err
        return scraper

    def new_processor(repo: Any, acker: Any, scr: Any, rec: Any, opts: ProcessorOptions) -> Any:
        events.append("processor-new")
        recorded_args["processor_opts"] = opts
        recorded_args["processor_repo"] = repo
        recorded_args["processor_acker"] = acker
        recorded_args["processor_scraper"] = scr
        recorded_args["processor_receiver"] = rec
        if new_processor_err:
            raise new_processor_err
        return processor

    def new_pool(reader: Any, proc: Any, opts: PoolOptions) -> Any:
        events.append("pool-new")
        recorded_args["pool_reader"] = reader
        recorded_args["pool_processor"] = proc
        recorded_args["pool_opts"] = opts
        if new_pool_err:
            raise new_pool_err
        return pool

    deps = WorkerDependencies(
        load_config=load_config,
        register_signals=register_signals,
        new_db=new_db,
        new_consumer=new_consumer,
        new_scraper=new_scraper,
        new_processor=new_processor,
        new_pool=new_pool,
        logger=logger,
    )
    return deps, recorded_args


# ===========================================================================
# 1. WorkerStartupError contract, secrecy, exit code labeling
# ===========================================================================


def test_worker_startup_error_preserves_cause_and_masks_secrets() -> None:
    sensitive_cause = ValueError("connect failed: postgres://app:s3cr3t@db.internal:5432/inteldigest")
    err = WorkerStartupError("database_open_failed", sensitive_cause)

    assert str(err) == "worker: database_open_failed"
    assert err.code == "database_open_failed"
    assert err.cause is sensitive_cause
    assert err.__cause__ is sensitive_cause
    assert "postgres" not in str(err)
    assert "s3cr3t" not in str(err)
    assert repr(err).startswith("WorkerStartupError('database_open_failed'")

    assert exit_code(None) == 0
    assert exit_code(err) == 1
    assert exit_code(RuntimeError("arbitrary")) == 1

    assert exit_code_label(err) == "database_open_failed"
    assert exit_code_label(WorkerStartupError("consumer_group_failed")) == "consumer_group_failed"
    assert exit_code_label(RuntimeError("something else")) == "worker_failed"
    assert exit_code_label(None) == "worker_failed"


def test_worker_startup_error_chains_with_is_group_missing() -> None:
    group_err = ConsumerGroupMissingError("inteldigest:jobs", "inteldigest:workers")
    startup_err = WorkerStartupError("consumer_group_failed", group_err)

    assert is_group_missing(startup_err) is True

    pool_missing = PoolGroupMissingError(cause=group_err)
    run_err = WorkerStartupError("worker_run_failed", pool_missing)
    assert is_group_missing(run_err) is True


# ===========================================================================
# 2. Lifecycle ordering and clean teardown
# ===========================================================================


@pytest.mark.asyncio
async def test_lifecycle_exact_ordering_and_signal_registration() -> None:
    events: list[str] = []
    logger, handler = _make_logger("test.lifecycle")
    deps, rec = _build_test_harness(events, logger=logger)
    stop_event = asyncio.Event()

    await run_with_dependencies(deps, stop_event=stop_event)

    expected = [
        "signal-register",
        "load-config",
        "db-new",
        "db-ping",
        "consumer-new",
        "consumer-ensure-group",
        "scraper-new",
        "processor-new",
        "pool-new",
        "pool-run",
        "scraper-close",
        "consumer-close",
        "db-close",
        "signal-unregister",
    ]
    assert events == expected
    assert rec["db"].closed is True
    assert rec["consumer"].closed is True
    assert rec["scraper"].closed is True
    assert rec["pool"].ran is True

    # Validate worker_started log record
    started_msgs = [m for m in handler.messages if "worker_started" in m]
    assert len(started_msgs) == 1
    msg = started_msgs[0]
    assert "consumer_group=inteldigest:workers" in msg
    assert "consumer_name=worker-host-1" in msg
    assert "pool_size=4" in msg
    # Never leak sensitive config in log messages
    for forbidden in ("secret-pass", "secret-token", "postgres://", "redis://"):
        assert forbidden not in msg


@pytest.mark.asyncio
async def test_config_mapping_and_options_propagation() -> None:
    events: list[str] = []
    deps, rec = _build_test_harness(events)

    await run_with_dependencies(deps)

    # ScraperOptions mapping
    scraper_opts: ScraperOptions = rec["scraper_opts"]
    assert scraper_opts.connect_timeout_seconds == 5.0
    assert scraper_opts.tls_timeout_seconds == 5.0
    assert scraper_opts.header_timeout_seconds == 10.0
    assert scraper_opts.read_timeout_seconds == 10.0
    assert scraper_opts.request_timeout_seconds == 30.0
    assert scraper_opts.max_body_bytes == 5_242_880
    assert scraper_opts.max_redirects == 5

    # ProcessorOptions mapping
    processor_opts: ProcessorOptions = rec["processor_opts"]
    assert processor_opts.db_timeout_seconds == 5.0
    assert processor_opts.queue_operation_timeout_seconds == 5.0

    # PoolOptions mapping
    pool_opts: PoolOptions = rec["pool_opts"]
    assert pool_opts.pool_size == 4
    assert pool_opts.shutdown_timeout_seconds == 10.0
    assert pool_opts.backoff_delay_seconds == 1.0


# ===========================================================================
# 3. Early cancellation between lifecycle steps
# ===========================================================================


@pytest.mark.asyncio
async def test_stop_event_before_db_skips_factories_and_unregisters() -> None:
    events: list[str] = []
    deps, _ = _build_test_harness(events)
    stop_event = asyncio.Event()

    def load_and_cancel() -> WorkerConfig:
        events.append("load-config")
        stop_event.set()
        return _sample_config()

    deps.load_config = load_and_cancel

    with pytest.raises(WorkerStartupError) as exc_info:
        await run_with_dependencies(deps, stop_event=stop_event)

    assert exc_info.value.code == "startup_canceled"
    assert "db-new" not in events
    assert "consumer-new" not in events
    assert events == ["signal-register", "load-config", "signal-unregister"]


@pytest.mark.asyncio
async def test_stop_event_after_db_open_closes_db_and_skips_ping() -> None:
    events: list[str] = []
    deps, rec = _build_test_harness(events)
    stop_event = asyncio.Event()

    original_new_db = deps.new_db

    def new_db_and_cancel(dsn: str) -> Any:
        res = original_new_db(dsn)
        stop_event.set()
        return res

    deps.new_db = new_db_and_cancel

    with pytest.raises(WorkerStartupError) as exc_info:
        await run_with_dependencies(deps, stop_event=stop_event)

    assert exc_info.value.code == "startup_canceled"
    assert "db-ping" not in events
    assert "consumer-new" not in events
    assert rec["db"].closed is True
    assert events == ["signal-register", "load-config", "db-new", "db-close", "signal-unregister"]


@pytest.mark.asyncio
async def test_stop_event_after_ping_closes_db_and_skips_consumer() -> None:
    events: list[str] = []
    deps, rec = _build_test_harness(events)
    stop_event = asyncio.Event()

    original_ping = rec["db"].ping

    async def ping_and_cancel() -> None:
        await original_ping()
        stop_event.set()

    rec["db"].ping = ping_and_cancel

    with pytest.raises(WorkerStartupError) as exc_info:
        await run_with_dependencies(deps, stop_event=stop_event)

    assert exc_info.value.code == "startup_canceled"
    assert "consumer-new" not in events
    assert rec["db"].closed is True
    assert events == [
        "signal-register",
        "load-config",
        "db-new",
        "db-ping",
        "db-close",
        "signal-unregister",
    ]


@pytest.mark.asyncio
async def test_stop_event_after_consumer_open_closes_consumer_then_db() -> None:
    events: list[str] = []
    deps, rec = _build_test_harness(events)
    stop_event = asyncio.Event()

    original_new_consumer = deps.new_consumer

    def new_consumer_and_cancel(conf: WorkerConfig) -> Any:
        res = original_new_consumer(conf)
        stop_event.set()
        return res

    deps.new_consumer = new_consumer_and_cancel

    with pytest.raises(WorkerStartupError) as exc_info:
        await run_with_dependencies(deps, stop_event=stop_event)

    assert exc_info.value.code == "startup_canceled"
    assert "consumer-ensure-group" not in events
    assert "scraper-new" not in events
    assert rec["consumer"].closed is True
    assert rec["db"].closed is True
    assert events == [
        "signal-register",
        "load-config",
        "db-new",
        "db-ping",
        "consumer-new",
        "consumer-close",
        "db-close",
        "signal-unregister",
    ]


@pytest.mark.asyncio
async def test_stop_event_after_ensure_group_closes_consumer_then_db() -> None:
    events: list[str] = []
    deps, rec = _build_test_harness(events)
    stop_event = asyncio.Event()

    original_ensure = rec["consumer"].ensure_group

    async def ensure_and_cancel() -> None:
        await original_ensure()
        stop_event.set()

    rec["consumer"].ensure_group = ensure_and_cancel

    with pytest.raises(WorkerStartupError) as exc_info:
        await run_with_dependencies(deps, stop_event=stop_event)

    assert exc_info.value.code == "startup_canceled"
    assert "scraper-new" not in events
    assert rec["consumer"].closed is True
    assert rec["db"].closed is True
    assert events == [
        "signal-register",
        "load-config",
        "db-new",
        "db-ping",
        "consumer-new",
        "consumer-ensure-group",
        "consumer-close",
        "db-close",
        "signal-unregister",
    ]


# ===========================================================================
# 4. Step failure cleanup and no-later-factories
# ===========================================================================


@pytest.mark.parametrize(
    ("harness_kw", "expected_code", "forbidden_events"),
    [
        (
            {"config_err": ValueError("bad config")},
            "configuration_failed",
            ["db-new", "consumer-new", "scraper-new"],
        ),
        (
            {"cfg": None, "new_db_val": None},
            "configuration_unavailable",
            ["db-new", "consumer-new"],
        ),
        (
            {"new_db_err": ConnectionRefusedError("no pg")},
            "database_open_failed",
            ["db-ping", "consumer-new", "scraper-new"],
        ),
        (
            {"new_db_val": None},
            "database_unavailable",
            ["db-ping", "consumer-new"],
        ),
        (
            {"ping_err": TimeoutError("pg timeout")},
            "database_ping_failed",
            ["consumer-new", "scraper-new"],
        ),
        (
            {"new_consumer_err": ConnectionRefusedError("no redis")},
            "consumer_open_failed",
            ["consumer-ensure-group", "scraper-new"],
        ),
        (
            {"new_consumer_val": None},
            "consumer_unavailable",
            ["consumer-ensure-group", "scraper-new"],
        ),
        (
            {"group_err": RuntimeError("redis group error")},
            "consumer_group_failed",
            ["scraper-new", "processor-new"],
        ),
        (
            {"new_scraper_err": RuntimeError("scraper init error")},
            "scraper_init_failed",
            ["processor-new", "pool-new"],
        ),
        (
            {"new_scraper_val": None},
            "scraper_unavailable",
            ["processor-new", "pool-new"],
        ),
        (
            {"new_processor_err": RuntimeError("processor init error")},
            "processor_init_failed",
            ["pool-new", "pool-run"],
        ),
        (
            {"new_processor_val": None},
            "processor_unavailable",
            ["pool-new", "pool-run"],
        ),
        (
            {"new_pool_err": RuntimeError("pool init error")},
            "pool_init_failed",
            ["pool-run"],
        ),
        (
            {"new_pool_val": None},
            "pool_unavailable",
            ["pool-run"],
        ),
        (
            {"pool_run_err": RuntimeError("pool runtime crash")},
            "worker_run_failed",
            [],
        ),
    ],
)
@pytest.mark.asyncio
async def test_startup_step_failures_cleanup_and_skip_later(
    harness_kw: dict[str, Any],
    expected_code: str,
    forbidden_events: list[str],
) -> None:
    events: list[str] = []
    # If cfg is explicitly None, pass a factory returning None
    if "cfg" in harness_kw and harness_kw["cfg"] is None:
        deps, _ = _build_test_harness(events, new_db_val=harness_kw.get("new_db_val"))
        deps.load_config = lambda: None  # type: ignore[return-value]
    else:
        deps, _ = _build_test_harness(events, **harness_kw)

    with pytest.raises(WorkerStartupError) as exc_info:
        await run_with_dependencies(deps)

    assert exc_info.value.code == expected_code
    for forbidden in forbidden_events:
        assert forbidden not in events, f"{forbidden} unexpectedly found in {events}"
    assert "signal-unregister" in events


# ===========================================================================
# 5. Handle returned with error is still closed
# ===========================================================================


@pytest.mark.asyncio
async def test_db_handle_returned_with_error_is_still_closed() -> None:
    events: list[str] = []
    db = FakeDB(events)
    deps, _ = _build_test_harness(
        events,
        new_db_val=db,
        new_db_err=RuntimeError("db factory returned handle with error"),
    )

    with pytest.raises(WorkerStartupError) as exc_info:
        await run_with_dependencies(deps)

    assert exc_info.value.code == "database_open_failed"
    assert db.closed is True
    assert "db-close" in events


@pytest.mark.asyncio
async def test_consumer_handle_returned_with_error_is_still_closed() -> None:
    events: list[str] = []
    consumer = FakeConsumer(events)
    deps, _ = _build_test_harness(
        events,
        new_consumer_val=consumer,
        new_consumer_err=RuntimeError("consumer factory returned handle with error"),
    )

    with pytest.raises(WorkerStartupError) as exc_info:
        await run_with_dependencies(deps)

    assert exc_info.value.code == "consumer_open_failed"
    assert consumer.closed is True
    assert "consumer-close" in events
    assert "db-close" in events


# ===========================================================================
# 6. Consumer close error never skips database close
# ===========================================================================


@pytest.mark.asyncio
async def test_consumer_close_error_never_skips_database_close() -> None:
    events: list[str] = []
    deps, rec = _build_test_harness(
        events,
        consumer_close_err=RuntimeError("redis socket close error"),
    )

    with pytest.raises(WorkerStartupError) as exc_info:
        await run_with_dependencies(deps)

    assert exc_info.value.code == "consumer_close_failed"
    assert rec["consumer"].closed is True
    assert rec["db"].closed is True
    assert "db-close" in events
    # Consumer close happened before DB close
    consumer_close_idx = events.index("consumer-close")
    db_close_idx = events.index("db-close")
    assert consumer_close_idx < db_close_idx


@pytest.mark.asyncio
async def test_pool_run_error_and_consumer_close_error_both_preserved() -> None:
    events: list[str] = []
    run_err = RuntimeError("pool run crashed")
    close_err = RuntimeError("redis close crashed")
    deps, rec = _build_test_harness(
        events,
        pool_run_err=run_err,
        consumer_close_err=close_err,
    )

    with pytest.raises(Exception) as exc_info:
        await run_with_dependencies(deps)

    err = exc_info.value
    # Database MUST be closed regardless
    assert rec["db"].closed is True
    assert "db-close" in events

    # Both errors are inspectable
    if isinstance(err, WorkerStartupError):
        assert err.code == "worker_run_failed"
        assert err.cause is run_err
        # close error is attached via attribute or context
        close_sub_err = getattr(err, "close_error", err.__context__)
        assert isinstance(close_sub_err, WorkerStartupError)
        assert close_sub_err.code == "consumer_close_failed"
        assert close_sub_err.cause is close_err
    elif isinstance(err, BaseExceptionGroup):
        codes = [e.code for e in err.exceptions if isinstance(e, WorkerStartupError)]
        assert "worker_run_failed" in codes
        assert "consumer_close_failed" in codes


# ===========================================================================
# 7. Resources not closed until pool.run returns
# ===========================================================================


@pytest.mark.asyncio
async def test_resources_not_closed_until_pool_run_returns() -> None:
    events: list[str] = []
    deps, rec = _build_test_harness(events)

    run_started = asyncio.Event()
    release_run = asyncio.Event()

    async def controlled_run(stop_event: asyncio.Event | None = None) -> None:
        events.append("pool-run")
        run_started.set()
        await release_run.wait()

    rec["pool"].run = controlled_run

    task = asyncio.create_task(run_with_dependencies(deps))
    await run_started.wait()

    # While pool.run is executing, resources MUST NOT be closed
    assert rec["db"].closed is False
    assert rec["consumer"].closed is False
    assert "db-close" not in events
    assert "consumer-close" not in events

    release_run.set()
    await task

    # Once pool.run finishes, resources are closed
    assert rec["db"].closed is True
    assert rec["consumer"].closed is True
    assert "db-close" in events
    assert "consumer-close" in events


# ===========================================================================
# 8. Dependency validation
# ===========================================================================


@pytest.mark.parametrize(
    "missing_attr",
    [
        "load_config",
        "register_signals",
        "new_db",
        "new_consumer",
        "new_scraper",
        "new_processor",
        "new_pool",
    ],
)
@pytest.mark.asyncio
async def test_invalid_dependencies_rejected(missing_attr: str) -> None:
    events: list[str] = []
    deps, _ = _build_test_harness(events)
    setattr(deps, missing_attr, None)

    with pytest.raises(WorkerStartupError) as exc_info:
        await run_with_dependencies(deps)

    assert exc_info.value.code == "invalid_dependencies"


# ===========================================================================
# 9. Sync run() and main() entrypoint
# ===========================================================================


def test_sync_run_returns_exit_code_zero_on_success() -> None:
    events: list[str] = []
    deps, _ = _build_test_harness(events)

    ret = run(deps)
    assert ret == 0
    assert "pool-run" in events


def test_sync_run_returns_exit_code_one_and_logs_error() -> None:
    events: list[str] = []
    logger, handler = _make_logger("test.sync_run")
    deps, _ = _build_test_harness(
        events,
        config_err=ValueError("invalid config in sync test"),
        logger=logger,
    )

    ret = run(deps)
    assert ret == 1
    assert any("worker_exit" in msg for msg in handler.messages)


def test_main_calls_run() -> None:
    with patch("inteldigest.worker.__main__.run", return_value=0) as mock_run:
        assert main() == 0
        mock_run.assert_called_once()


def test_production_dependencies_factory() -> None:
    deps = production_dependencies()
    assert callable(deps.load_config)
    assert callable(deps.register_signals)
    assert callable(deps.new_db)
    assert callable(deps.new_consumer)
    assert callable(deps.new_scraper)
    assert callable(deps.new_processor)
    assert callable(deps.new_pool)


# ===========================================================================
# 10. MetadataReceiver safe logging
# ===========================================================================


@pytest.mark.asyncio
async def test_metadata_receiver_safe_logging() -> None:
    logger, handler = _make_logger("test.receiver")
    receiver = MetadataReceiver(logger=logger)
    job_id = uuid4()

    from datetime import UTC, datetime

    article = Article(
        title="Test Article Title",
        text="Sample article content with private details.",
        requested_url="https://example.com/article",
        final_url="https://example.com/article",
        language="en",
        fetched_at=datetime.now(UTC),
    )
    result_success = Result(job_id=job_id, article=article)
    await receiver.receive(result_success)

    assert len(handler.messages) == 1
    msg = handler.messages[0]
    assert "result=article" in msg
    assert f"job_id={job_id}" in msg
    assert "title_present=true" in msg
    assert "language_present=true" in msg
    # Verify article text is NOT logged
    assert "Sample article content" not in msg

    scrape_err = ScrapeError(
        ScrapeErrorKind.SECURITY_REJECTED,
        ScrapeStage.VALIDATION,
        "blocked internal IP address",
        http_status=0,
    )
    result_err = Result(job_id=job_id, error=scrape_err)
    await receiver.receive(result_err)

    assert len(handler.messages) == 2
    err_msg = handler.messages[1]
    assert "result=scrape_error" in err_msg
    assert "kind=security_rejected" in err_msg
    assert "stage=validation" in err_msg
    assert "blocked internal IP" not in err_msg
