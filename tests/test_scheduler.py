from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import AsyncMock, patch

import pytest
from sqlmodel import col, select

from scheduler.models import Job, JobStatus, JobTrigger, utcnow
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
        created_at=utcnow() - timedelta(minutes=5),  # 5m ago, interval=60m
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
        created_at=utcnow() - timedelta(minutes=90),  # 90m ago, interval=60m
    )
    session.add(old)
    await session.commit()

    with patch("scheduler.scheduler.execute_job", new_callable=AsyncMock):
        await scheduler._tick()

    result = await session.exec(select(Job).where(Job.source_id == "src_test").order_by(col(Job.created_at).desc()))
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
        created_at=utcnow() - timedelta(minutes=5),
    )
    session.add(recent)
    await session.commit()

    with patch("scheduler.scheduler.execute_job", new_callable=AsyncMock):
        job = await scheduler.trigger_immediate("src_test")

    assert job is not None
    assert job.trigger == JobTrigger.webhook


async def test_retry_failed_job_after_backoff(scheduler, session):
    """Failed job with attempt=1 past backoff should be reset to pending."""
    failed = Job(
        source_id="src_test",
        account_id="acc_test",
        bot_type_id="bot_ehr",
        status=JobStatus.failed,
        trigger=JobTrigger.scheduled,
        attempt=1,
        completed_at=utcnow() - timedelta(minutes=5),  # past 2^1*30s=60s backoff
    )
    session.add(failed)
    await session.commit()
    await session.refresh(failed)

    with patch("scheduler.scheduler.execute_job", new_callable=AsyncMock):
        await scheduler._tick()

    await session.refresh(failed)
    assert failed.status == JobStatus.pending
    assert failed.attempt == 2
    assert failed.error is None


async def test_retry_skipped_before_backoff(scheduler, session):
    """Failed job with completed_at too recent should not be retried."""
    failed = Job(
        source_id="src_test",
        account_id="acc_test",
        bot_type_id="bot_ehr",
        status=JobStatus.failed,
        trigger=JobTrigger.scheduled,
        attempt=1,
        completed_at=utcnow() - timedelta(seconds=10),  # within 60s backoff
    )
    session.add(failed)
    await session.commit()

    with patch("scheduler.scheduler.execute_job", new_callable=AsyncMock):
        await scheduler._tick()

    await session.refresh(failed)
    assert failed.status == JobStatus.failed


async def test_retry_skipped_at_max_attempts(scheduler, session):
    """Failed job at max retry_attempts should not be retried."""
    failed = Job(
        source_id="src_test",
        account_id="acc_test",
        bot_type_id="bot_ehr",
        status=JobStatus.failed,
        trigger=JobTrigger.scheduled,
        attempt=3,  # retry_attempts=3 in sample_config
        completed_at=utcnow() - timedelta(hours=1),
    )
    session.add(failed)
    await session.commit()

    with patch("scheduler.scheduler.execute_job", new_callable=AsyncMock):
        await scheduler._tick()

    await session.refresh(failed)
    assert failed.status == JobStatus.failed


async def test_retry_respects_concurrency_cap(scheduler, session):
    """Failed job past backoff should not retry if account is at concurrency cap."""
    # Fill cap
    for i in range(2):
        session.add(Job(
            source_id=f"src_other_{i}",
            account_id="acc_test",
            bot_type_id="bot_ehr",
            status=JobStatus.running,
            trigger=JobTrigger.scheduled,
        ))
    failed = Job(
        source_id="src_test",
        account_id="acc_test",
        bot_type_id="bot_ehr",
        status=JobStatus.failed,
        trigger=JobTrigger.scheduled,
        attempt=1,
        completed_at=utcnow() - timedelta(minutes=5),
    )
    session.add(failed)
    await session.commit()

    with patch("scheduler.scheduler.execute_job", new_callable=AsyncMock):
        await scheduler._tick()

    await session.refresh(failed)
    assert failed.status == JobStatus.failed
