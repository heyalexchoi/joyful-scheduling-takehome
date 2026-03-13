# Implementation Plan: Data Ingestion Job Scheduler

## Data Model Understanding

**Ingestion sources** are the primary entity — each source is one schedulable unit of work.
We join to `bot_types` for default interval/retry/timeout, and to `accounts` for `max_concurrent_jobs`.

Effective model per source:
- **interval**: `source.schedule_override.interval_minutes` ?? `bot_type.default_schedule.interval_minutes`
- **retry_attempts / timeout_seconds**: from `bot_type.default_schedule`
- **concurrency cap**: from `account.settings.max_concurrent_jobs`
- **enabled**: from `source.enabled` (skip disabled sources entirely)

Priority/tier are implicitly encoded in `max_concurrent_jobs` (enterprise=5, standard=2, starter=1).

Concurrency restrictions overrule scheduling — if a source is "due" but its account is at the concurrency cap, the job is deferred to the next tick (not dropped).

## Tech Stack

- **Python 3.10+**
- **FastAPI** + **uvicorn** — async HTTP server
- **SQLModel** + **aiosqlite** — unified Pydantic + SQLAlchemy models, async SQLite for job persistence
- **pytest** + **httpx** — testing

## Project Structure

```
scheduler/
├── __init__.py
├── main.py              # Entry point: FastAPI app + lifespan (starts scheduler)
├── models.py            # All domain models: config (Account, BotType, Source) + Job (SQLModel table)
├── database.py          # Async engine + session setup only
├── config.py            # Load JSON files from data/, build lookup dicts
├── scheduler.py         # Core scheduling loop (asyncio background task)
├── executor.py          # Job execution (simulate with sleep + random outcome)
├── api.py               # FastAPI router: webhook + job endpoints
data/
├── accounts.json        # (provided)
├── ingestion_sources.json
├── bot_types.json
tests/
├── conftest.py          # Fixtures: test DB, test client, sample config
├── test_scheduler.py    # Scheduling logic, concurrency guards
├── test_api.py          # Webhook trigger, job listing/filtering
├── test_executor.py     # Job execution, retry logic
pyproject.toml
```

## Implementation Steps

### Step 1: Project setup
- Create `pyproject.toml` with dependencies
- Create package structure with `__init__.py` files

### Step 2: Models + config loading (`models.py` + `config.py`)
- SQLModel models: `Account`, `BotType` (with `DefaultSchedule`), `IngestionSource` (with optional `ScheduleOverride`), `Job` (table=True)
- `Job` is the only DB-backed model; the rest are plain SQLModel (Pydantic) for config
- `load_config(data_dir)` -> returns a `Config` object with:
  - `sources: list[IngestionSource]` (only enabled ones)
  - `accounts: dict[str, Account]` (keyed by id)
  - `bot_types: dict[str, BotType]` (keyed by id)
- Helper: `get_effective_interval(source, bot_type) -> int` (minutes)

### Step 3: Database (`database.py`)
- Async engine with aiosqlite
- `init_db()` — create tables (reads metadata from SQLModel models in `models.py`)
- `get_session()` — async session factory
- Job table fields (defined in `models.py` as `Job(SQLModel, table=True)`):
  - `id: str` (UUID)
  - `source_id: str`
  - `account_id: str`
  - `bot_type_id: str`
  - `status: str` (pending, running, succeeded, failed)
  - `trigger: str` ("scheduled" or "webhook")
  - `attempt: int` (1-based, for retries)
  - `created_at: datetime`
  - `started_at: datetime | None`
  - `completed_at: datetime | None`
  - `error: str | None`
- Index on `source_id`, `account_id`, `status` for efficient queries

### Step 4: Executor (`executor.py`)
- `execute_job(job_id, session)`:
  - Set status to "running", set `started_at`
  - `await asyncio.sleep(random.uniform(1, 5))` — simulate work
  - 80% chance success, 20% failure
  - On success: set status "succeeded", set `completed_at`
  - On failure: set status "failed", set `completed_at`, set `error` message
- Timeout handling: wrap execution in `asyncio.wait_for` using bot_type's `timeout_seconds`

### Step 5: Scheduler (`scheduler.py`)
- `Scheduler` class:
  - Holds reference to `Config` and DB session factory
  - `start()` — launches the tick loop as an asyncio task
  - `stop()` — cancels the task, waits for in-flight jobs to finish (graceful shutdown)
  - `_tick()` — one scheduling pass:
    ```
    for each enabled source:
      interval = get_effective_interval(source, bot_type)
      last_run = query last job created_at for this source
      if last_run is None or (now - last_run >= interval):
        running_for_account = count running jobs for account
        if running_for_account >= account.max_concurrent_jobs:
          continue  # defer to next tick
        if has_running_job_for_source(source_id):
          continue  # don't duplicate
        create_and_execute_job(source, trigger="scheduled")
    ```
  - `trigger_immediate(source_id)` — called by webhook, creates job bypassing schedule check (still respects no-duplicate-running and concurrency cap)
  - Tick interval: 5 seconds (configurable)

### Step 6: API (`api.py`)
- `POST /webhooks/data-available`
  - Body: `{ "account_id": str, "source_id": str }`
  - Validate source exists, is enabled, belongs to account
  - Call `scheduler.trigger_immediate(source_id)`
  - Return 201 with job_id, or 409 if already running, or 429 if account at concurrency cap
- `GET /jobs`
  - Query params: `account_id`, `source_id`, `status` (all optional)
  - Return list of jobs, newest first, limit 100
- `GET /jobs/{job_id}`
  - Return single job or 404

### Step 7: Main entry point (`main.py`)
- FastAPI app with `lifespan` context manager:
  - On startup: `init_db()`, `load_config()`, create `Scheduler`, `scheduler.start()`
  - On shutdown: `scheduler.stop()`
- Include API router
- Run with: `uvicorn scheduler.main:app`

### Step 8: Retry logic (bonus)
- When a job fails, check `attempt < bot_type.retry_attempts`
- If retryable, schedule a retry after `2^attempt * 30` seconds
- Track via `attempt` field on the new retry job
- The scheduler tick checks for pending retry jobs whose retry_after time has passed

### Step 9: Tests
- **test_config.py**: Config loading, effective interval calculation
- **test_scheduler.py**:
  - Source gets scheduled when interval elapsed
  - Source NOT scheduled when already running
  - Source NOT scheduled when account at concurrency cap
  - Deferred job runs on subsequent tick when slot opens
- **test_api.py**:
  - Webhook creates and runs a job
  - Webhook returns 409 when source already running
  - GET /jobs returns jobs, filtering works
  - GET /jobs/{id} returns 404 for missing job
- **test_executor.py**: Job transitions through statuses correctly

### Step 10: README
- How to run (`pip install -e .` then `uvicorn scheduler.main:app`)
- Design decisions and trade-offs
- What would improve with more time

## Key Design Decisions

1. **SQLModel + SQLite** — SQLModel unifies Pydantic and SQLAlchemy so we define `Job` once and use it for both DB operations and API responses. SQLite gives persistence for free with minimal complexity.
2. **Tick-based scanning** — Simple, predictable. With 10 sources and 5s ticks, scanning all sources is trivial. A priority queue (next-run heap) would be better at scale but is overengineering here.
3. **Concurrency overrules schedule** — A source that's "due" but blocked by concurrency limits just waits. Next tick it'll run if a slot opened. No separate queue needed.
4. **Webhook respects guards** — Immediate trigger still checks no-duplicate-running and concurrency cap. Returns informative status codes (409/429) so callers know why.
5. **asyncio throughout** — No threads. The scheduler loop, job execution, and HTTP server all share one event loop.

## What We Skip / Future Work

- Rate limiting per data source (design: token bucket per source_system)
- Metrics/observability (Prometheus counters for jobs_started, jobs_completed, jobs_failed)
- Persistent scheduler state across restarts (currently reschedules from scratch on startup, which is fine)
- Priority-based ordering when multiple sources compete for limited slots
