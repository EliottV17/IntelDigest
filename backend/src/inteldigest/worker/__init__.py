"""Worker job processor, concurrency pool, and standalone process lifecycle (M16)."""

from typing import Any

from inteldigest.worker.pool import (
    JobProcessor,
    Pool,
    PoolGroupMissingError,
    PoolOptions,
    QueueReader,
    default_pool_options,
)
from inteldigest.worker.processor import (
    STATUS_COMPLETED,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_PROCESSING,
    Acker,
    ArticleScraper,
    JobRepo,
    Outcome,
    ProcessError,
    Processor,
    ProcessorOptions,
    classify_scrape_error,
    decode_delivery,
    diagnostic_reason,
    safe_scrape_error,
)
from inteldigest.worker.receiver import (
    MetadataReceiver,
    Receiver,
    Result,
    safe_kind,
    safe_stage,
    safe_status,
    valid_job_id,
    validate_result,
)

_LIFECYCLE_EXPORTS = frozenset({
    "WorkerDependencies",
    "WorkerStartupError",
    "exit_code",
    "exit_code_label",
    "main",
    "production_dependencies",
    "run",
    "run_with_dependencies",
})


def __getattr__(name: str) -> Any:
    if name in _LIFECYCLE_EXPORTS:
        from inteldigest.worker import __main__ as worker_main

        return getattr(worker_main, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "STATUS_COMPLETED",
    "STATUS_FAILED",
    "STATUS_PENDING",
    "STATUS_PROCESSING",
    "Acker",
    "ArticleScraper",
    "JobProcessor",
    "JobRepo",
    "MetadataReceiver",
    "Outcome",
    "Pool",
    "PoolGroupMissingError",
    "PoolOptions",
    "ProcessError",
    "Processor",
    "ProcessorOptions",
    "QueueReader",
    "Receiver",
    "Result",
    "WorkerDependencies",
    "WorkerStartupError",
    "classify_scrape_error",
    "decode_delivery",
    "default_pool_options",
    "diagnostic_reason",
    "exit_code",
    "exit_code_label",
    "main",
    "production_dependencies",
    "run",
    "run_with_dependencies",
    "safe_kind",
    "safe_scrape_error",
    "safe_stage",
    "safe_status",
    "valid_job_id",
    "validate_result",
]
