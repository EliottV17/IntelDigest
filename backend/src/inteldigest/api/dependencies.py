"""The repository and queue surfaces consumed by the HTTP routes."""

from typing import Protocol
from uuid import UUID

from fastapi import Request

from inteldigest.jobs.models import Job
from inteldigest.queue.message import Message


class JobRepository(Protocol):
    async def create_job(self, url: str) -> Job: ...
    async def get_job_by_id(self, job_id: UUID) -> Job: ...


class JobLookup(Protocol):
    """Retained for backward compatibility with existing route imports."""

    async def get_job_by_id(self, job_id: UUID) -> Job: ...


class JobPublisher(Protocol):
    async def publish(self, message: Message) -> None: ...


def get_repository(request: Request) -> JobRepository:
    return request.app.state.repository


def get_publisher(request: Request) -> JobPublisher:
    return request.app.state.publisher
