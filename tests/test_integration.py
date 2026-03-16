from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

import scheduler.api as api_module
import scheduler.database as db_module
from scheduler.config import load_config
from scheduler.scheduler import Scheduler


@pytest_asyncio.fixture
async def int_engine(tmp_path):
    url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
def int_session_factory(int_engine):
    return async_sessionmaker(int_engine, class_=AsyncSession, expire_on_commit=False)


@pytest_asyncio.fixture
async def int_client(int_session_factory):
    config = load_config("data")
    db_module.async_session_factory = int_session_factory
    sched = Scheduler(config, int_session_factory)
    api_module.set_scheduler(sched)
    app = FastAPI()
    app.include_router(api_module.router)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c, sched


async def test_webhook_triggers_job_to_completion(int_client):
    """POST webhook → job runs through full lifecycle → GET /jobs/{id} reflects it."""
    c, sched = int_client

    with patch("scheduler.executor.random.uniform", return_value=0.01):
        resp = await c.post("/webhooks/data-available", json={
            "account_id": "acc_001",
            "source_id": "src_001",
        })
        assert resp.status_code == 201
        job_id = resp.json()["job_id"]
        await asyncio.gather(*sched._in_flight, return_exceptions=True)

    resp = await c.get(f"/jobs/{job_id}")
    assert resp.status_code == 200
    job = resp.json()
    assert job["status"] in ("succeeded", "failed")
    assert job["started_at"] is not None
    assert job["completed_at"] is not None
    assert job["trigger"] == "webhook"


async def test_scheduled_jobs_created_on_first_tick(int_client):
    """First tick schedules all enabled sources within their account caps; all run to completion."""
    c, sched = int_client

    with patch("scheduler.executor.random.uniform", return_value=0.01):
        await sched._tick()
        await asyncio.gather(*sched._in_flight, return_exceptions=True)

    resp = await c.get("/jobs")
    jobs = resp.json()
    assert len(jobs) > 0
    for job in jobs:
        assert job["status"] in ("succeeded", "failed")
        assert job["started_at"] is not None
        assert job["completed_at"] is not None


async def test_interval_guard_prevents_reschedule(int_client):
    """
    Real-time test: start the scheduler loop, let it tick twice (5s apart).
    All source intervals are 60+ minutes so the second tick should create no new jobs.
    """
    c, sched = int_client

    with patch("scheduler.executor.random.uniform", return_value=0.01):
        sched.start()

        # First tick fires immediately; wait for it and its jobs to finish
        await asyncio.sleep(0.5)
        await asyncio.gather(*sched._in_flight, return_exceptions=True)

        first_count = len((await c.get("/jobs")).json())
        assert first_count > 0

        # Wait for the second tick to fire (TICK_INTERVAL_SECONDS = 5)
        await asyncio.sleep(5.5)
        await asyncio.gather(*sched._in_flight, return_exceptions=True)
        await sched.stop()

    second_count = len((await c.get("/jobs")).json())
    assert second_count == first_count  # interval guard blocked all re-scheduling


async def test_graceful_shutdown_completes_in_flight_jobs(int_client):
    """stop() waits for in-flight tasks; all jobs reach terminal state."""
    c, sched = int_client

    # Jobs take 0.5s so they're definitely still in-flight when stop() is called
    with patch("scheduler.executor.random.uniform", return_value=0.5):
        await sched._tick()
        assert len(sched._in_flight) > 0
        await sched.stop()

    assert len(sched._in_flight) == 0
    jobs = (await c.get("/jobs")).json()
    assert len(jobs) > 0
    assert all(j["status"] in ("succeeded", "failed") for j in jobs)
