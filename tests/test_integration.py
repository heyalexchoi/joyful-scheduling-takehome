from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest
import pytest_asyncio
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession

import scheduler.database as db_module
from scheduler.api import router
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


@pytest.fixture
def int_session_factory(int_engine):
    return async_sessionmaker(int_engine, class_=AsyncSession, expire_on_commit=False)


@pytest_asyncio.fixture
async def int_client(int_session_factory):
    config = load_config("data")
    db_module.async_session_factory = int_session_factory
    sched = Scheduler(config, int_session_factory)
    app = FastAPI()
    app.state.scheduler = sched
    app.include_router(router)
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
    Start the real scheduler loop with tick interval reduced to 0 so both ticks
    fire back-to-back without sleeping. All source intervals are 60+ minutes so
    the second tick should create no new jobs.
    """
    c, sched = int_client

    with patch("scheduler.scheduler.TICK_INTERVAL_SECONDS", 0), \
         patch("scheduler.executor.random.uniform", return_value=0.01):
        sched.start()

        # Let the first tick run and its jobs complete
        await asyncio.sleep(0.1)
        await asyncio.gather(*sched._in_flight, return_exceptions=True)
        first_count = len((await c.get("/jobs")).json())
        assert first_count > 0

        # Let the second tick run — interval guard should block all re-scheduling
        await asyncio.sleep(0.1)
        await asyncio.gather(*sched._in_flight, return_exceptions=True)
        await sched.stop()

    second_count = len((await c.get("/jobs")).json())
    assert second_count == first_count


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
