from __future__ import annotations

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from sqlmodel import SQLModel
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from scheduler.config import Config
from scheduler.models import Account, AccountSettings, BotType, DefaultSchedule, IngestionSource


# ---------------------------------------------------------------------------
# In-memory DB fixtures
# ---------------------------------------------------------------------------

TEST_DATABASE_URL = "sqlite+aiosqlite:///:memory:"


@pytest_asyncio.fixture
async def engine():
    e = create_async_engine(TEST_DATABASE_URL, connect_args={"check_same_thread": False})
    async with e.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)
    yield e
    async with e.begin() as conn:
        await conn.run_sync(SQLModel.metadata.drop_all)
    await e.dispose()


@pytest_asyncio.fixture
def session_factory(engine):
    return async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


@pytest_asyncio.fixture
async def session(session_factory):
    async with session_factory() as s:
        yield s


# ---------------------------------------------------------------------------
# Sample config fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_config() -> Config:
    account = Account(
        id="acc_test",
        name="Test Account",
        tier="standard",
        settings=AccountSettings(max_concurrent_jobs=2, priority="normal"),
        created_at="2024-01-01T00:00:00Z",
    )
    bot_type = BotType(
        id="bot_ehr",
        name="EHR",
        description="Test bot",
        default_schedule=DefaultSchedule(
            interval_minutes=60,
            retry_attempts=3,
            timeout_seconds=300,
        ),
        supported_sources=["epic"],
    )
    source = IngestionSource(
        id="src_test",
        account_id="acc_test",
        bot_type_id="bot_ehr",
        name="Test Source",
        source_system="epic",
        enabled=True,
        schedule_override=None,
        credentials_ref="vault://test",
        created_at="2024-01-01T00:00:00Z",
    )
    return Config(
        sources=[source],
        accounts={"acc_test": account},
        bot_types={"bot_ehr": bot_type},
    )


# ---------------------------------------------------------------------------
# Test app / client fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture
async def test_app(session_factory, sample_config):
    """FastAPI app wired to in-memory DB, scheduler NOT started (controlled in tests)."""
    import scheduler.database as db_module
    import scheduler.api as api_module
    from scheduler.scheduler import Scheduler
    from fastapi import FastAPI

    # Patch db module so get_session (used by API routes) hits the test DB
    db_module.async_session_factory = session_factory

    sched = Scheduler(sample_config, session_factory)
    api_module.set_scheduler(sched)

    app = FastAPI()
    app.include_router(api_module.router)

    return app, sched


@pytest_asyncio.fixture
async def client(test_app):
    app, sched = test_app
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c, sched
