"""The lookup surface consumed by the HTTP routes."""

from typing import Protocol
from uuid import UUID

from fastapi import Request

from inteldigest.jobs.models import Job


class JobLookup(Protocol):
    async def get_job_by_id(self, job_id: UUID) -> Job: ...


def get_repository(request: Request) -> JobLookup:
    return request.app.state.repository
