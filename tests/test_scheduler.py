from __future__ import annotations

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest
from sqlmodel import select

from scheduler.models import Job, JobStatus, JobTrigger
from scheduler.scheduler import Scheduler


@pytest.fixture
def scheduler(sample_config, session_factory):
    return Scheduler(sample_config, session_factory)


async def test_scheduled_job_created_when_due(scheduler, session):
    """First tick should schedule the source (no prior run)."""
    with patch("scheduler.scheduler.execute_job", new_callable=AsyncMock):
        await scheduler._tick()

    result = await session.exec(select(Job).where(Job.source_id == "src_test"))
    jobs = result.all()
    assert len(jobs) == 1
    assert jobs[0].trigger == JobTrigger.scheduled


async def test_no_duplicate_when_running(scheduler, session):
    """Skip scheduling if a running job exists for the source."""
    running = Job(
        source_id="src_test",
        account_id="acc_test",
        bot_type_id="bot_ehr",
        status=JobStatus.running,
        trigger=JobTrigger.scheduled,
    )
    session.add(running)
    await session.commit()

    with patch("scheduler.scheduler.execute_job", new_callable=AsyncMock):
        await scheduler._tick()

    result = await session.exec(select(Job))
    assert len(result.all()) == 1  # only the running one, no new job


async def test_concurrency_cap_blocks_scheduling(scheduler, session, sample_config):
    """Account with max_concurrent_jobs=2: block when cap is reached."""
    # Fill cap with running jobs (different source IDs to avoid source-level guard)
    for i in range(2):
        session.add(Job(
            source_id=f"src_other_{i}",
            account_id="acc_test",
            bot_type_id="bot_ehr",
            status=JobStatus.running,
            trigger=JobTrigger.scheduled,
        ))
    await session.commit()

    with patch("scheduler.scheduler.execute_job", new_callable=AsyncMock):
        await scheduler._tick()

    result = await session.exec(select(Job).where(Job.source_id == "src_test"))
    assert result.first() is None  # src_test was not scheduled


async def test_not_scheduled_before_interval_elapsed(scheduler, session):
    """Source with a recent successful job should not be re-scheduled."""
    recent = Job(
        source_id="src_test",
        account_id="acc_test",
        bot_type_id="bot_ehr",
        status=JobStatus.succeeded,
        trigger=JobTrigger.scheduled,
        created_at=datetime.utcnow() - timedelta(minutes=5),  # 5m ago, interval=60m
    )
    session.add(recent)
    await session.commit()

    with patch("scheduler.scheduler.execute_job", new_callable=AsyncMock):
        await scheduler._tick()

    result = await session.exec(select(Job).where(Job.status == JobStatus.pending))
    assert result.first() is None


async def test_scheduled_after_interval_elapsed(scheduler, session):
    """Source whose last job was > interval ago should be scheduled."""
    old = Job(
        source_id="src_test",
        account_id="acc_test",
        bot_type_id="bot_ehr",
        status=JobStatus.succeeded,
        trigger=JobTrigger.scheduled,
        created_at=datetime.utcnow() - timedelta(minutes=90),  # 90m ago, interval=60m
    )
    session.add(old)
    await session.commit()

    with patch("scheduler.scheduler.execute_job", new_callable=AsyncMock):
        await scheduler._tick()

    result = await session.exec(select(Job).where(Job.source_id == "src_test").order_by(Job.created_at.desc()))
    jobs = result.all()
    assert len(jobs) == 2
    assert jobs[0].status == JobStatus.pending


async def test_webhook_trigger_bypasses_interval(scheduler, session):
    """Webhook trigger should create a job even if interval hasn't elapsed."""
    recent = Job(
        source_id="src_test",
        account_id="acc_test",
        bot_type_id="bot_ehr",
        status=JobStatus.succeeded,
        trigger=JobTrigger.scheduled,
        created_at=datetime.utcnow() - timedelta(minutes=5),
    )
    session.add(recent)
    await session.commit()

    with patch("scheduler.scheduler.execute_job", new_callable=AsyncMock):
        job = await scheduler.trigger_immediate("src_test")

    assert job is not None
    assert job.trigger == JobTrigger.webhook
