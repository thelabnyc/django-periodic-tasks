# django-periodic-tasks

Periodic/cron task scheduling for django-tasks. Backend-agnostic replacement for celery-beat.

## Project Structure

- `django_periodic_tasks/` - Main package
  - `models.py` - ScheduledTask model
  - `registry.py` - ScheduleRegistry, ScheduleEntry, @scheduled_task decorator
  - `cron.py` - Cron expression utilities (croniter wrapper)
  - `sync.py` - Code-to-DB sync
  - `scheduler.py` - PeriodicTaskScheduler (daemon thread)
  - `task_resolver.py` - Resolve task_path -> Task object
  - `admin.py` - Django admin (read-only for code-defined tasks)
  - `management/commands/` - run_scheduler, scheduler_db_worker
- `sandbox/` - Development/test Django project
- `tests/` - Test suite

## Development

Tests run inside Docker (PostgreSQL required for SELECT FOR UPDATE SKIP LOCKED):

```bash
mise run test         # Run test suite
mise run mypy         # Type checking
mise run coverage     # Tests with coverage
mise run tox          # Full tox matrix
```

Or directly with docker compose:

```bash
docker compose run --rm test uv run python sandbox/manage.py test --noinput -v 2 tests
```

## Key Conventions

- Python 3.13+ required
- All code must pass mypy strict mode
- Tests use django-tasks DummyBackend with ENQUEUE_ON_COMMIT=False
- ScheduledTask.save() auto-computes next_run_at when it's None and task is enabled
- Code-defined schedules use source=CODE, admin-defined use source=DATABASE
- Scheduler uses SELECT FOR UPDATE SKIP LOCKED for safe multi-worker deployment
