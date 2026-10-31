"""Persisted job data; HTTP response policy belongs to the API layer."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, JsonValue


class Job(BaseModel):
    id: UUID
    url: str
    status: str
    result: JsonValue | None = None
    error: str | None = None
    created_at: datetime
    updated_at: datetime
