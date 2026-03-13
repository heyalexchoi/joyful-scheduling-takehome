from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

from sqlmodel import Field, SQLModel


# ---------------------------------------------------------------------------
# Config models (loaded from JSON, not persisted to DB)
# ---------------------------------------------------------------------------

class DefaultSchedule(SQLModel):
    interval_minutes: int
    retry_attempts: int
    timeout_seconds: int


class BotType(SQLModel):
    id: str
    name: str
    description: str
    default_schedule: DefaultSchedule
    supported_sources: list[str]


class ScheduleOverride(SQLModel):
    interval_minutes: int


class AccountSettings(SQLModel):
    max_concurrent_jobs: int
    priority: str


class Account(SQLModel):
    id: str
    name: str
    tier: str
    settings: AccountSettings
    created_at: datetime


class IngestionSource(SQLModel):
    id: str
    account_id: str
    bot_type_id: str
    name: str
    source_system: str
    enabled: bool
    schedule_override: Optional[ScheduleOverride] = None
    credentials_ref: str
    created_at: datetime


# ---------------------------------------------------------------------------
# Job model (persisted to DB)
# ---------------------------------------------------------------------------

class JobStatus(str, Enum):
    pending = "pending"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"


class JobTrigger(str, Enum):
    scheduled = "scheduled"
    webhook = "webhook"


class Job(SQLModel, table=True):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), primary_key=True)
    source_id: str = Field(index=True)
    account_id: str = Field(index=True)
    bot_type_id: str
    status: str = Field(default=JobStatus.pending, index=True)
    trigger: str = Field(default=JobTrigger.scheduled)
    attempt: int = Field(default=1)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = Field(default=None)
    completed_at: Optional[datetime] = Field(default=None)
    error: Optional[str] = Field(default=None)
