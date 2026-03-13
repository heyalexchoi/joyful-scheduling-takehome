from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import datetime, timedelta

from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from scheduler.config import Config
from scheduler.executor import execute_job
from scheduler.models import IngestionSource, Job, JobStatus, JobTrigger

logger = logging.getLogger(__name__)

TICK_INTERVAL_SECONDS = 5


class Scheduler:
    def __init__(self, config: Config, session_factory: Callable) -> None:
        self.config = config
        self._session_factory = session_factory
        self._task: asyncio.Task | None = None
        self._in_flight: set[asyncio.Task] = set()

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="scheduler-loop")
        logger.info("scheduler_started")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        if self._in_flight:
            logger.info("scheduler_draining", extra={"in_flight": len(self._in_flight)})
            await asyncio.gather(*self._in_flight, return_exceptions=True)

        logger.info("scheduler_stopped")

    async def _loop(self) -> None:
        while True:
            try:
                await self._tick()
            except Exception:
                logger.exception("scheduler_tick_error")
            await asyncio.sleep(TICK_INTERVAL_SECONDS)

    async def _tick(self) -> None:
        async with self._session_factory() as session:
            for source in self.config.sources:
                try:
                    await self._maybe_schedule(source, session, trigger=JobTrigger.scheduled)
                except Exception:
                    logger.exception("schedule_error", extra={"source_id": source.id})

    async def _maybe_schedule(
        self,
        source: IngestionSource,
        session: AsyncSession,
        trigger: JobTrigger,
    ) -> Job | None:
        account = self.config.accounts[source.account_id]
        bot_type = self.config.bot_types[source.bot_type_id]

        active_statuses = [JobStatus.pending, JobStatus.running]

        # Don't start if a job is already pending/running for this source
        active_for_source = await session.exec(
            select(Job).where(
                Job.source_id == source.id,
                Job.status.in_(active_statuses),  # type: ignore[union-attr]
            )
        )
        if active_for_source.first():
            return None

        # Enforce account-level concurrency cap
        active_for_account = await session.exec(
            select(Job).where(
                Job.account_id == source.account_id,
                Job.status.in_(active_statuses),  # type: ignore[union-attr]
            )
        )
        if len(active_for_account.all()) >= account.settings.max_concurrent_jobs:
            logger.debug(
                "concurrency_cap_reached",
                extra={"account_id": source.account_id, "source_id": source.id},
            )
            return None

        if trigger == JobTrigger.scheduled:
            interval = self.config.get_effective_interval(source)
            last_job = await session.exec(
                select(Job)
                .where(Job.source_id == source.id)
                .order_by(Job.created_at.desc())
                .limit(1)
            )
            last = last_job.first()
            if last and (datetime.utcnow() - last.created_at) < timedelta(minutes=interval):
                return None

        job = Job(
            source_id=source.id,
            account_id=source.account_id,
            bot_type_id=source.bot_type_id,
            trigger=trigger,
        )
        session.add(job)
        await session.commit()
        await session.refresh(job)

        logger.info(
            "job_created",
            extra={"job_id": job.id, "source_id": source.id, "trigger": trigger},
        )

        task = asyncio.create_task(
            self._run_job(job.id, bot_type.default_schedule.timeout_seconds),
            name=f"job-{job.id}",
        )
        self._in_flight.add(task)
        task.add_done_callback(self._in_flight.discard)

        return job

    async def _run_job(self, job_id: str, timeout_seconds: int) -> None:
        async with self._session_factory() as session:
            job = await session.get(Job, job_id)
            if job is None:
                return
            await execute_job(job, timeout_seconds, session)

    async def trigger_immediate(self, source_id: str) -> Job | None:
        """Trigger an immediate job for a source, bypassing the schedule check."""
        source = next((s for s in self.config.sources if s.id == source_id), None)
        if source is None:
            return None

        async with self._session_factory() as session:
            return await self._maybe_schedule(source, session, trigger=JobTrigger.webhook)
