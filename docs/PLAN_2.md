# Plan 2: Retry Logic + Test Coverage Gaps

## 1. Retry Logic

### Approach

Reuse the existing `Job` record rather than creating a new one. On retry, reset `status` to `pending` and increment `attempt`.

### Changes to `scheduler.py`

Add a `_retry_failed()` method called at the end of `_tick()`, after the existing source scheduling loop.

Logic:
1. Query jobs where `status == failed` AND `attempt < bot_type.retry_attempts`
2. For each, compute backoff: `2^attempt * 30s` from `completed_at`
3. If enough time has elapsed AND the source isn't already active (pending/running from a separate trigger) AND account concurrency cap not exceeded — reset `status = pending`, increment `attempt`, clear `error`, spawn a `_run_job` task
4. The existing per-source guard in `_maybe_schedule` doesn't apply here since we're not creating a new job. We need the guards inline in `_retry_failed` or extract them.

### Guard reuse

Extract account concurrency check and source-active check into small helpers, so both `_maybe_schedule` and `_retry_failed` can use them without duplication.

### Config lookup

To get `retry_attempts` for a failed job, look up `bot_type_id` on the job → `config.bot_types[job.bot_type_id].default_schedule.retry_attempts`.

### Flow

```
_tick()
  → for each source: _maybe_schedule(source, session, scheduled)   # existing
  → _retry_failed(session)                                         # new
```

```
_retry_failed(session):
  failed_jobs = query Job where status=failed
  for job in failed_jobs:
    bot_type = config.bot_types[job.bot_type_id]
    if job.attempt >= bot_type.default_schedule.retry_attempts:
      continue
    backoff = timedelta(seconds=2**job.attempt * 30)
    if datetime.utcnow() - job.completed_at < backoff:
      continue
    if source_has_active_job(session, job.source_id):   # another job pending/running
      continue
    if account_at_cap(session, job.account_id):
      continue
    job.status = JobStatus.pending
    job.attempt += 1
    job.error = None
    job.started_at = None
    job.completed_at = None
    session.add(job)
    await session.commit()
    spawn _run_job(job.id, timeout)
```

## 2. Test Coverage Gaps

### 2a. GET /jobs filters — `test_api.py`

- `test_list_jobs_filter_by_source_id`: Insert 2 jobs with different source_ids, filter by one, assert only that one returned.
- `test_list_jobs_filter_by_status`: Insert jobs with different statuses, filter by `status=failed`, assert correct subset.

### 2b. Executor — `tests/test_executor.py` (new file)

- `test_execute_job_success`: Patch `random.random` to return 0.1 (< 0.8 = success), patch `random.uniform` to return 0.1 (fast), call `execute_job`, assert status=succeeded, started_at and completed_at set.
- `test_execute_job_failure`: Patch `random.random` to return 0.9 (>= 0.8 = failure), assert status=failed, error="Simulated failure".
- `test_execute_job_timeout`: Pass `timeout_seconds=0`, assert status=failed, error contains "Timed out".

### 2c. Retry — `test_scheduler.py`

- `test_retry_failed_job_after_backoff`: Insert a failed job with `attempt=1`, `completed_at` far enough in the past. Run `_tick()`. Assert job is now pending with `attempt=2`.
- `test_retry_skipped_before_backoff`: Insert a failed job with `completed_at` just now. Run `_tick()`. Assert job still failed.
- `test_retry_skipped_at_max_attempts`: Insert a failed job with `attempt=3` (== retry_attempts). Run `_tick()`. Assert job still failed.
- `test_retry_respects_concurrency_cap`: Fill account to cap with running jobs, insert a failed job past backoff. Run `_tick()`. Assert job still failed.

## 3. Type Fixes

Install `pyright` (Pylance's CLI) and run against `scheduler/` to get real diagnostics. Fix what it reports — no guessing.

```bash
pip install pyright && pyright scheduler/
```

## 4. File changes summary

| File | Change |
|---|---|
| `scheduler/scheduler.py` | Add `_retry_failed()`, extract `_source_has_active_job()` and `_account_at_cap()` helpers |
| `tests/test_scheduler.py` | Add 4 retry tests |
| `tests/test_api.py` | Add 2 filter tests |
| `tests/test_executor.py` | New file, 3 tests |
