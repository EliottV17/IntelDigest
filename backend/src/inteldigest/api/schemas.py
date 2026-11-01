"""HTTP response fields, independent of the repository's null-storage policy."""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, JsonValue


class JobResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    url: str
    status: str
    result: JsonValue | None = None
    error: str | None = None
    created_at: datetime
    updated_at: datetime
