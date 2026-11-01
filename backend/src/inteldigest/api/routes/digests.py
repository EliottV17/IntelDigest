"""Read and submit persisted jobs with queue publication."""

import json
import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse

from inteldigest.api.dependencies import (
    JobPublisher,
    JobRepository,
    get_publisher,
    get_repository,
)
from inteldigest.api.schemas import CreateJobResponse, JobResponse
from inteldigest.jobs.repository import JobNotFound
from inteldigest.jobs.validation import validate_url
from inteldigest.queue.message import SCHEMA_VERSION, Message

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post(
    "/api/v1/digests",
    status_code=202,
    response_model=CreateJobResponse,
    responses={
        202: {"description": "Job accepted for processing", "model": CreateJobResponse},
        400: {"description": "Invalid JSON body or URL"},
        500: {"description": "Internal error"},
        503: {"description": "Queue unavailable"},
    },
)
async def create_digest(
    request: Request,
    repository: Annotated[JobRepository, Depends(get_repository)],
    publisher: Annotated[JobPublisher, Depends(get_publisher)],
) -> JSONResponse:
    raw_body = await request.body()
    if not raw_body:
        return JSONResponse({"error": "invalid JSON body"}, status_code=400)
    try:
        data = json.loads(raw_body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return JSONResponse({"error": "invalid JSON body"}, status_code=400)

    if not isinstance(data, dict):
        return JSONResponse({"error": "invalid JSON body"}, status_code=400)

    if "url" not in data or data["url"] is None:
        raw_url = ""
    elif isinstance(data["url"], str):
        raw_url = data["url"]
    else:
        return JSONResponse({"error": "invalid JSON body"}, status_code=400)

    try:
        validate_url(raw_url)
    except ValueError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)

    try:
        job = await repository.create_job(raw_url)
    except Exception:  # noqa: BLE001 -- HTTP boundary must return a safe generic error.
        logger.error("creating job failed")
        return JSONResponse({"error": "internal error"}, status_code=500)

    message = Message(schema_version=SCHEMA_VERSION, job_id=job.id, url=job.url)
    try:
        await publisher.publish(message)
    except Exception:  # noqa: BLE001 -- HTTP boundary must return a safe generic error.
        logger.error("publishing job failed", extra={"job_id": str(job.id)})
        return JSONResponse(
            {"error": "queue unavailable", "job_id": str(job.id)},
            status_code=503,
        )

    return JSONResponse(
        {"job_id": str(job.id), "message": "processing"},
        status_code=202,
    )


@router.api_route(
    "/api/v1/digests/{job_id}", methods=["GET", "HEAD"],
    response_model=JobResponse, response_model_exclude_none=True,
    responses={
        400: {"description": "Invalid job ID"},
        404: {"description": "Job not found"},
        500: {"description": "Internal error"},
    },
)
async def get_digest(
    job_id: str, repository: Annotated[JobRepository, Depends(get_repository)],
) -> JobResponse | JSONResponse:
    try:
        parsed_id = UUID(job_id)
    except ValueError:
        return JSONResponse({"error": "invalid job ID"}, status_code=400)
    try:
        job = await repository.get_job_by_id(parsed_id)
        return JobResponse.model_validate(job)
    except JobNotFound:
        return JSONResponse({"error": "job not found"}, status_code=404)
    except Exception:  # noqa: BLE001 -- HTTP boundary must return a safe generic error.
        # Driver exceptions may contain DSNs, SQL values or article data.
        logger.error("getting job failed")
        return JSONResponse({"error": "internal error"}, status_code=500)
