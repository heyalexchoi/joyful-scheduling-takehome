from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from scheduler.api import router
from scheduler.config import load_config
from scheduler.database import init_db, async_session_factory
from scheduler.scheduler import Scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await init_db()

    config = load_config("data")
    logger.info(
        "config_loaded",
        extra={
            "sources": len(config.sources),
            "accounts": len(config.accounts),
            "bot_types": len(config.bot_types),
        },
    )

    scheduler = Scheduler(config, async_session_factory)
    app.state.scheduler = scheduler
    scheduler.start()

    yield

    await scheduler.stop()


app = FastAPI(title="Joyful Job Scheduler", lifespan=lifespan)
app.include_router(router)
