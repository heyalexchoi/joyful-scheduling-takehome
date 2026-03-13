# Take-Home Challenge: Data Ingestion Job Scheduler

## Overview

At Joyful, we operate a fleet of automated bots that pull data from various healthcare data sources—Electronic Health Records (EHRs), clearing houses, and payer portals. Each of our customer accounts may have multiple ingestion sources configured, each with its own bot type and scheduling requirements.

Your task is to design and implement a **job scheduling system** that orchestrates these data ingestion jobs with the following capabilities:

1. **Dynamic Scheduling**: Different ingestion sources require different polling frequencies based on the bot type, account tier, and source configuration
2. **Job Execution Tracking**: Track job runs, their status (pending, running, succeeded, failed), and maintain execution history
3. **Webhook Notifications**: Expose an API for external systems to notify when new data is available, triggering immediate ingestion

## Time Expectation

This challenge is designed to be completed in approximately **4 hours**. We value quality over quantity—a well-designed partial solution is preferred over a rushed complete one.

## Constraints

- **No workflow orchestration frameworks** (Airflow, Dagster, Prefect, Temporal, Celery Beat, etc.)
- Build the scheduling logic from primitives (threads, async, time-based checks, etc.)
- You may use any other libraries you find appropriate
- Python 3.10+ is preferred, but not required
- Include a `requirements.txt` or `pyproject.toml` for dependencies

## Requirements

### Core Scheduler (Required)

Build a scheduler that:

1. **Loads configuration** from the provided example data (accounts, ingestion sources, bot types)
2. **Schedules jobs** based on each source's configured cadence
3. **Executes jobs** (simulate the actual data pull with a sleep + random success/failure)
4. **Tracks state** of all jobs (pending, running, succeeded, failed) with timestamps
5. **Handles concurrent execution** appropriately (e.g., don't start a new job for a source if one is already running)

### Webhook API (Required)

Implement a simple HTTP API with:

1. `POST /webhooks/data-available` - External systems call this to signal new data is ready
   - Should accept a payload identifying the account and source
   - Should trigger an immediate job run (bypassing the normal schedule)
   
2. `GET /jobs` - List recent jobs with their status
   - Support filtering by account_id, source_id, status
   
3. `GET /jobs/{job_id}` - Get details of a specific job run

### Bonus (If Time Permits)

- Rate limiting: Ensure we don't overwhelm any single data source
- Retry logic with exponential backoff for failed jobs
- Graceful shutdown handling
- Metrics/observability hooks
- Persistent state (survive restarts)

## Provided Data

See the `data/` directory for example configuration:

- `accounts.json` - Customer accounts with their tier and settings
- `ingestion_sources.json` - The configured data sources per account
- `bot_types.json` - Bot type definitions with default scheduling parameters

## Deliverables

1. **Working code** that implements the requirements
2. **README** explaining:
   - How to run your solution
   - Key design decisions and trade-offs
   - What you would improve given more time
3. **Tests** for critical paths (doesn't need to be exhaustive)

## Evaluation Criteria

We're looking for:

- **System Design**: Clean architecture, separation of concerns, extensibility
- **Code Quality**: Readable, well-organized, appropriate abstractions
- **Trade-off Awareness**: Understanding of what you chose and why
- **Production Thinking**: Error handling, logging, edge cases
- **Communication**: Clear documentation of your approach

## Questions?

If anything is unclear, make a reasonable assumption and document it. We're interested in seeing how you approach ambiguity.

---

Good luck! We're excited to see your approach.

