from __future__ import annotations

from typing import Annotated, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from sqlmodel import col, select
from sqlmodel.ext.asyncio.session import AsyncSession

from scheduler.database import get_session
from scheduler.models import Job, JobStatus, JobTrigger
from scheduler.scheduler import Scheduler

router = APIRouter()


def get_scheduler(request: Request) -> Scheduler:
    scheduler = getattr(request.app.state, "scheduler", None)
    if scheduler is None:
        raise HTTPException(status_code=503, detail="Scheduler not initialized")
    return scheduler


# ---------------------------------------------------------------------------
# Request / response schemas
# ---------------------------------------------------------------------------

class WebhookPayload(BaseModel):
    account_id: str
    source_id: str


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.post("/webhooks/data-available", status_code=201)
async def data_available(
    payload: WebhookPayload,
    scheduler: Annotated[Scheduler, Depends(get_scheduler)],
) -> dict:
    config = scheduler.config

    source = next((s for s in config.sources if s.id == payload.source_id), None)
    if source is None:
        raise HTTPException(status_code=404, detail="Source not found or disabled")
    if source.account_id != payload.account_id:
        raise HTTPException(status_code=400, detail="Source does not belong to account")

    job = await scheduler.trigger_immediate(payload.source_id)
    if job is None:
        # trigger_immediate returns None only when concurrency cap is hit or source running
        raise HTTPException(
            status_code=409,
            detail="Job already running for this source or account concurrency cap reached",
        )

    return {"job_id": job.id, "status": job.status, "trigger": JobTrigger.webhook}


@router.get("/jobs", response_model=list[Job])
async def list_jobs(
    account_id: Optional[str] = Query(default=None),
    source_id: Optional[str] = Query(default=None),
    status: Optional[str] = Query(default=None),
    session: AsyncSession = Depends(get_session),
) -> list[Job]:
    query = select(Job).order_by(col(Job.created_at).desc()).limit(100)

    if account_id:
        query = query.where(Job.account_id == account_id)
    if source_id:
        query = query.where(Job.source_id == source_id)
    if status:
        if status not in JobStatus.__members__:
            raise HTTPException(status_code=400, detail=f"Invalid status: {status}")
        query = query.where(Job.status == status)

    result = await session.exec(query)
    return list(result.all())


@router.get("/jobs/{job_id}", response_model=Job)
async def get_job(job_id: str, session: AsyncSession = Depends(get_session)) -> Job:
    job = await session.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job
