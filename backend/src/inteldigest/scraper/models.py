"""Domain models and classified error types for the safe scraper."""

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class ScrapeErrorKind(StrEnum):
    """Controlled classification of scraper failures."""

    SECURITY_REJECTED = "security_rejected"
    TIMEOUT = "timeout"
    NETWORK = "network"
    CANCELED = "canceled"
    BODY_TOO_LARGE = "body_too_large"
    HTTP_STATUS = "http_status"
    UNSUPPORTED_CONTENT = "unsupported_content"
    EXTRACTION = "extraction"


class ScrapeStage(StrEnum):
    """Controlled pipeline stage where a scraper failure occurred."""

    VALIDATION = "validation"
    FETCH = "fetch"
    EXTRACT = "extract"


_ALLOWED_KINDS: frozenset[str] = frozenset(item.value for item in ScrapeErrorKind)
_ALLOWED_STAGES: frozenset[str] = frozenset(item.value for item in ScrapeStage)


@dataclass(frozen=True, slots=True)
class Article:
    """Extracted article text and metadata for downstream stages."""

    text: str
    requested_url: str
    final_url: str
    title: str
    language: str
    fetched_at: datetime


@dataclass(slots=True)
class ScrapeStats:
    """Per-call scrape measurements (decompressed final-response body bytes)."""

    bytes_read: int = 0


class ScrapeError(Exception):
    """Classified scraper error whose string form never exposes raw causes or URLs.

    The underlying exception remains accessible via ``__cause__`` and ``cause``
    for programmatic inspection while ``str(err)`` emits only allowlisted
    classification tokens and an HTTP status code when in 100..599.
    """

    def __init__(
        self,
        kind: ScrapeErrorKind | str,
        stage: ScrapeStage | str,
        message: str = "",
        *,
        http_status: int = 0,
        cause: BaseException | None = None,
    ) -> None:
        self.kind: ScrapeErrorKind | str = kind
        self.stage: ScrapeStage | str = stage
        self.http_status: int = http_status
        self.message: str = message
        self.cause: BaseException | None = cause
        super().__init__(self._format_safe())
        if cause is not None:
            self.__cause__ = cause

    def _format_safe(self) -> str:
        raw_kind = self.kind.value if isinstance(self.kind, ScrapeErrorKind) else str(self.kind)
        kind_str = raw_kind if raw_kind in _ALLOWED_KINDS else "unknown"

        raw_stage = (
            self.stage.value if isinstance(self.stage, ScrapeStage) else str(self.stage)
        )
        stage_str = raw_stage if raw_stage in _ALLOWED_STAGES else "unknown"

        if (
            isinstance(self.http_status, int)
            and not isinstance(self.http_status, bool)
            and 100 <= self.http_status <= 599
        ):
            return f"scrape {kind_str} during {stage_str} (HTTP {self.http_status})"
        return f"scrape {kind_str} during {stage_str}"

    def __str__(self) -> str:
        return self._format_safe()
