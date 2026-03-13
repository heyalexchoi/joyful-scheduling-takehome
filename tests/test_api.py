from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from sqlmodel import select

from scheduler.models import Job, JobStatus, JobTrigger


async def test_webhook_creates_job(client):
    c, sched = client
    with patch.object(sched, "trigger_immediate", new_callable=AsyncMock) as mock_trigger:
        mock_trigger.return_value = Job(
            id="job-123",
            source_id="src_test",
            account_id="acc_test",
            bot_type_id="bot_ehr",
            status=JobStatus.pending,
            trigger=JobTrigger.webhook,
        )
        resp = await c.post("/webhooks/data-available", json={
            "account_id": "acc_test",
            "source_id": "src_test",
        })

    assert resp.status_code == 201
    assert resp.json()["job_id"] == "job-123"
    mock_trigger.assert_awaited_once_with("src_test")


async def test_webhook_unknown_source(client):
    c, sched = client
    resp = await c.post("/webhooks/data-available", json={
        "account_id": "acc_test",
        "source_id": "src_nonexistent",
    })
    assert resp.status_code == 404


async def test_webhook_wrong_account(client):
    c, sched = client
    resp = await c.post("/webhooks/data-available", json={
        "account_id": "acc_wrong",
        "source_id": "src_test",
    })
    assert resp.status_code == 400


async def test_webhook_conflict_when_running(client):
    c, sched = client
    with patch.object(sched, "trigger_immediate", new_callable=AsyncMock) as mock_trigger:
        mock_trigger.return_value = None  # None = blocked by guard
        resp = await c.post("/webhooks/data-available", json={
            "account_id": "acc_test",
            "source_id": "src_test",
        })
    assert resp.status_code == 409


async def test_list_jobs_empty(client):
    c, _ = client
    resp = await c.get("/jobs")
    assert resp.status_code == 200
    assert resp.json() == []


async def test_list_jobs_with_filter(client, session_factory):
    from scheduler.models import Job, JobStatus, JobTrigger

    factory = session_factory
    async with factory() as s:
        s.add(Job(source_id="src_test", account_id="acc_test", bot_type_id="bot_ehr",
                  status=JobStatus.succeeded, trigger=JobTrigger.scheduled))
        s.add(Job(source_id="src_other", account_id="acc_other", bot_type_id="bot_ehr",
                  status=JobStatus.failed, trigger=JobTrigger.scheduled))
        await s.commit()

    c, _ = client
    resp = await c.get("/jobs?account_id=acc_test")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["source_id"] == "src_test"


async def test_get_job_not_found(client):
    c, _ = client
    resp = await c.get("/jobs/nonexistent-id")
    assert resp.status_code == 404


async def test_get_job(client, session_factory):
    factory = session_factory
    async with factory() as s:
        job = Job(source_id="src_test", account_id="acc_test", bot_type_id="bot_ehr",
                  status=JobStatus.running, trigger=JobTrigger.webhook)
        s.add(job)
        await s.commit()
        await s.refresh(job)
        job_id = job.id

    c, _ = client
    resp = await c.get(f"/jobs/{job_id}")
    assert resp.status_code == 200
    assert resp.json()["id"] == job_id
    assert resp.json()["status"] == "running"
