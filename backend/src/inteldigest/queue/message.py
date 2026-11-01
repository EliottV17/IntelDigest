"""Versioned Redis Stream job message contract."""

from typing import Final
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import BaseModel, ConfigDict, StrictInt, StrictStr, field_validator

SCHEMA_VERSION: Final[int] = 1


class Message(BaseModel):
    """Stable payload serialized inside a Redis Stream entry's `data` field."""

    model_config = ConfigDict(frozen=True)

    schema_version: StrictInt
    job_id: UUID
    url: StrictStr

    @field_validator("schema_version")
    @classmethod
    def _validate_schema_version(cls, value: int) -> int:
        if value != SCHEMA_VERSION:
            raise ValueError(f"unsupported schema_version {value}")
        return value

    @field_validator("job_id")
    @classmethod
    def _validate_job_id(cls, value: UUID) -> UUID:
        if value.int == 0:
            raise ValueError("job_id is required")
        return value

    @field_validator("url")
    @classmethod
    def _validate_url(cls, value: str) -> str:
        if not value or any(ch.isspace() or ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value):
            raise ValueError("url must be an absolute HTTP(S) URL")
        try:
            parsed = urlsplit(value)
            _ = parsed.port
        except ValueError:
            raise ValueError("url must be an absolute HTTP(S) URL") from None
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise ValueError("url must be an absolute HTTP(S) URL")
        return value
