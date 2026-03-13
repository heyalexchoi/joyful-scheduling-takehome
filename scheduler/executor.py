from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime

from sqlmodel.ext.asyncio.session import AsyncSession

from scheduler.models import Job, JobStatus

logger = logging.getLogger(__name__)

SUCCESS_RATE = 0.8


async def execute_job(job: Job, timeout_seconds: int, session: AsyncSession) -> None:
    """Simulate a data pull: random sleep, then 80% success / 20% failure."""
    job.status = JobStatus.running
    job.started_at = datetime.utcnow()
    session.add(job)
    await session.commit()

    logger.info("job_started", extra={"job_id": job.id, "source_id": job.source_id})

    try:
        duration = random.uniform(1, 5)
        await asyncio.wait_for(asyncio.sleep(duration), timeout=timeout_seconds)

        if random.random() < SUCCESS_RATE:
            job.status = JobStatus.succeeded
            logger.info("job_succeeded", extra={"job_id": job.id, "source_id": job.source_id})
        else:
            job.status = JobStatus.failed
            job.error = "Simulated failure"
            logger.warning("job_failed", extra={"job_id": job.id, "source_id": job.source_id, "error": job.error})

    except asyncio.TimeoutError:
        job.status = JobStatus.failed
        job.error = f"Timed out after {timeout_seconds}s"
        logger.warning("job_timeout", extra={"job_id": job.id, "source_id": job.source_id})

    job.completed_at = datetime.utcnow()
    session.add(job)
    await session.commit()
