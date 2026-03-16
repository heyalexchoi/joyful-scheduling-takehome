from __future__ import annotations

from unittest.mock import patch

from scheduler.executor import execute_job
from scheduler.models import Job, JobStatus, JobTrigger


async def test_execute_job_success(session):
    job = Job(source_id="src_test", account_id="acc_test", bot_type_id="bot_ehr",
              status=JobStatus.pending, trigger=JobTrigger.scheduled)
    session.add(job)
    await session.commit()

    with patch("scheduler.executor.random.uniform", return_value=0.01), \
         patch("scheduler.executor.random.random", return_value=0.1):  # 0.1 < 0.8 = success
        await execute_job(job, timeout_seconds=30, session=session)

    assert job.status == JobStatus.succeeded
    assert job.started_at is not None
    assert job.completed_at is not None
    assert job.error is None


async def test_execute_job_failure(session):
    job = Job(source_id="src_test", account_id="acc_test", bot_type_id="bot_ehr",
              status=JobStatus.pending, trigger=JobTrigger.scheduled)
    session.add(job)
    await session.commit()

    with patch("scheduler.executor.random.uniform", return_value=0.01), \
         patch("scheduler.executor.random.random", return_value=0.9):  # 0.9 >= 0.8 = failure
        await execute_job(job, timeout_seconds=30, session=session)

    assert job.status == JobStatus.failed
    assert job.error == "Simulated failure"
    assert job.completed_at is not None


async def test_execute_job_timeout(session):
    job = Job(source_id="src_test", account_id="acc_test", bot_type_id="bot_ehr",
              status=JobStatus.pending, trigger=JobTrigger.scheduled)
    session.add(job)
    await session.commit()

    with patch("scheduler.executor.random.uniform", return_value=10):  # 10s sleep
        await execute_job(job, timeout_seconds=0, session=session)

    assert job.status == JobStatus.failed
    assert job.error is not None
    assert "Timed out" in job.error
