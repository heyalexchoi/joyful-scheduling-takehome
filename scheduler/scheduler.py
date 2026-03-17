from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import timedelta

from sqlmodel import col, func, select
from sqlmodel.ext.asyncio.session import AsyncSession

from scheduler.config import Config
from scheduler.executor import execute_job
from scheduler.models import IngestionSource, Job, JobStatus, JobTrigger, utcnow

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
                    await session.rollback()
            await self._retry_failed(session)

    async def _source_has_active_job(self, session: AsyncSession, source_id: str) -> bool:
        active_statuses = [JobStatus.pending, JobStatus.running]
        result = await session.exec(
            select(Job).where(
                Job.source_id == source_id,
                col(Job.status).in_(active_statuses),
            )
        )
        return result.first() is not None

    async def _account_at_cap(self, session: AsyncSession, account_id: str) -> bool:
        account = self.config.accounts[account_id]
        active_statuses = [JobStatus.pending, JobStatus.running]
        result = await session.exec(
            select(func.count()).select_from(Job).where(
                Job.account_id == account_id,
                col(Job.status).in_(active_statuses),
            )
        )
        count = result.one()
        return count >= account.settings.max_concurrent_jobs

    async def _maybe_schedule(
        self,
        source: IngestionSource,
        session: AsyncSession,
        trigger: JobTrigger,
    ) -> Job | None:
        bot_type = self.config.bot_types[source.bot_type_id]

        if await self._source_has_active_job(session, source.id):
            return None

        if await self._account_at_cap(session, source.account_id):
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
                .order_by(col(Job.created_at).desc())
                .limit(1)
            )
            last = last_job.first()
            if last and (utcnow() - last.created_at) < timedelta(minutes=interval):
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

        self._spawn_job(job.id, bot_type.default_schedule.timeout_seconds)
        return job

    async def _retry_failed(self, session: AsyncSession) -> None:
        result = await session.exec(
            select(Job).where(
                col(Job.status) == JobStatus.failed,
                col(Job.completed_at).is_not(None),
            )
        )
        for job in result.all():
            bot_type = self.config.bot_types.get(job.bot_type_id)
            if bot_type is None:
                continue
            if job.attempt >= bot_type.default_schedule.retry_attempts:
                continue
            backoff = timedelta(seconds=2 ** job.attempt * 30)
            if utcnow() - job.completed_at < backoff:
                continue
            if await self._source_has_active_job(session, job.source_id):
                continue
            if await self._account_at_cap(session, job.account_id):
                continue

            job.status = JobStatus.pending
            job.attempt += 1
            job.error = None
            job.started_at = None
            job.completed_at = None
            session.add(job)
            await session.commit()

            logger.info(
                "job_retry_scheduled",
                extra={"job_id": job.id, "source_id": job.source_id, "attempt": job.attempt},
            )
            self._spawn_job(job.id, bot_type.default_schedule.timeout_seconds)

    def _spawn_job(self, job_id: str, timeout_seconds: int) -> None:
        task = asyncio.create_task(
            self._run_job(job_id, timeout_seconds),
            name=f"job-{job_id}",
        )
        self._in_flight.add(task)
        task.add_done_callback(self._in_flight.discard)

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
