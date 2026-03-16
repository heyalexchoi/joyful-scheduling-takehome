# Solution: Data Ingestion Job Scheduler

## How to Run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
uvicorn scheduler.main:app --reload
```

The scheduler starts automatically on app startup and begins polling every 5 seconds.

### API Endpoints

```
POST /webhooks/data-available   Trigger an immediate job for a source
GET  /jobs                      List jobs (filter: ?account_id=&source_id=&status=)
GET  /jobs/{job_id}             Get a specific job
```

Example webhook call:
```bash
curl -X POST http://127.0.0.1:8000/webhooks/data-available \
  -H "Content-Type: application/json" \
  -d '{"account_id": "acc_001", "source_id": "src_001"}'
```

### Running Tests

```bash
pytest tests/ -v
```

---

## Design Decisions

### Data Model

`IngestionSource` is the primary unit of work. Each source is scheduled independently, joined to:
- `BotType` for default interval, retry attempts, and timeout
- `Account` for the `max_concurrent_jobs` concurrency cap

Schedule resolution: `source.schedule_override.interval_minutes` takes priority over `bot_type.default_schedule.interval_minutes`.

Tier and priority are implicitly encoded in `max_concurrent_jobs` (enterprise=5, standard=2, starter=1) — no separate priority queue is needed.

### Scheduler

A tick-based loop (5s interval) scans all enabled sources each cycle. For each source, it checks three guards in order:

1. **No active job** (pending or running) for this source — guards against duplicates even in the brief window between job creation and execution start
2. **Account concurrency cap** not exceeded (also checked against pending+running)
3. **Interval elapsed** since last job was created (scheduled trigger only; webhooks bypass this)

If all guards pass, a `Job` record is created in SQLite and an `asyncio.Task` is spawned to execute it. The scheduler tracks in-flight tasks for graceful shutdown. Each source is wrapped in a try/except with an explicit session rollback so a failure on one source doesn't corrupt the session for the rest.

This is simpler and more predictable than a priority queue / next-run-time heap. With ~10 sources and 5s ticks, scanning is trivially cheap.

### Retry with Exponential Backoff

On failure, a job is eligible for retry if `attempt < bot_type.retry_attempts`. Each tick, `_retry_failed()` scans failed jobs and re-enqueues any past their backoff window (`2^attempt * 30s` from `completed_at`). The same concurrency guards apply — a retry won't fire if the source is already active or the account is at its cap.

The job record is reused: `status` resets to `pending`, `attempt` increments, and `started_at`/`completed_at`/`error` are cleared. `completed_at` doubles as the timestamp of the last attempt for backoff calculation.

### Concurrency Model

Single `asyncio` event loop shared between FastAPI and the scheduler loop. No threads. Job execution (`asyncio.sleep` + DB writes) is non-blocking, so many jobs can run concurrently without blocking the scheduler or HTTP server.

### Persistence

SQLModel + SQLite (`aiosqlite`). SQLModel unifies Pydantic and SQLAlchemy so `Job` is defined once and serves as both the DB table and the API response schema. SQLite gives persistence across restarts with zero operational overhead.

### Dependency Injection

The `Scheduler` receives its `session_factory` as a constructor argument rather than importing it from the module. This makes the dependency explicit and keeps tests clean — no monkey-patching, just pass a factory backed by an in-memory SQLite engine.

### Webhook Behavior

`POST /webhooks/data-available` triggers a job immediately, bypassing the schedule interval check. It still respects the no-duplicate-running and concurrency cap guards, returning:
- `201` with `job_id` on success
- `404` if the source doesn't exist or is disabled
- `400` if the source doesn't belong to the given account
- `409` if already running or account is at its concurrency cap

### Graceful Shutdown

On shutdown, the scheduler loop is cancelled and the app waits for all in-flight job tasks to complete before exiting.

---

## Trade-offs

| Decision | Alternative | Why chosen |
|---|---|---|
| Tick-based scan | Priority queue (min-heap by next_run_time) | Simpler, sufficient at this scale |
| SQLite | In-memory dict | Persistence for free, cleaner queries |
| Single event loop | Thread pool | No overhead, jobs are I/O-bound simulations |
| Config loaded once | Re-read each tick | Sources don't change at runtime; reload would need a signal/API |
| Reuse job record on retry | Create new job per attempt | `attempt` field tracks progress on one record; simpler queries |

---

## What I'd Improve Given More Time

- **Rate limiting per source system**: Token bucket per `source_system` to avoid hammering the same external endpoint from multiple accounts (e.g., two accounts both using Epic).
- **Config hot-reload**: Watch the JSON files or add a `PATCH /sources/{id}` API to enable/disable sources at runtime.
- **Metrics**: Prometheus counters for `jobs_started`, `jobs_succeeded`, `jobs_failed`, and a gauge for `jobs_running`.
- **Structured logging**: Replace `logging.info(msg, extra={...})` with `structlog` for proper JSON output in production.
- **Postgres**: SQLite is fine for a single instance, but a real deployment would want Postgres to support horizontal scaling of the API layer and avoid SQLite's single-writer bottleneck under concurrent job execution.
