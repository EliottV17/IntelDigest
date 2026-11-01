"""Read persisted jobs without scraping or queue operations."""

import logging
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse

from inteldigest.api.dependencies import JobLookup, get_repository
from inteldigest.api.schemas import JobResponse
from inteldigest.jobs.repository import JobNotFound

router = APIRouter()
logger = logging.getLogger(__name__)


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
    job_id: str, repository: Annotated[JobLookup, Depends(get_repository)],
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
