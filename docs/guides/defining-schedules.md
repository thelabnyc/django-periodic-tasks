# Defining Schedules

django-periodic-tasks supports two ways to define scheduled tasks: in code (version-controlled, deployed with your app) and in the database (managed at runtime through the Django admin).

## Code-Defined Schedules

Use the `@scheduled_task` decorator to register a django-tasks `Task` for periodic execution:

```python
from django_tasks import task
from django_periodic_tasks.registry import scheduled_task


@scheduled_task(cron="0 8 * * 1-5")  # Weekdays at 8:00 AM UTC
@task()
def morning_report() -> None:
    ...
```

### Decorator Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `cron` | `str` | *(required)* | 5-field cron expression |
| `name` | `str` | task's `module_path` | Unique schedule name |
| `timezone` | `str` | `"UTC"` | IANA timezone for cron matching |
| `args` | `list` | `[]` | Positional arguments for `task.enqueue()` |
| `kwargs` | `dict` | `{}` | Keyword arguments for `task.enqueue()` |
| `queue_name` | `str` | `"default"` | Queue name for `task.using()` |
| `priority` | `int` | `0` | Priority for `task.using()` |
| `backend` | `str` | `"default"` | Backend name for `task.using()` |

### Task Arguments

Pass arguments that will be forwarded to `task.enqueue()` on each execution:

```python
@scheduled_task(
    cron="0 0 * * 0",
    args=[42],
    kwargs={"full": True},
)
@task()
def weekly_sync(tenant_id: int, full: bool = False) -> None:
    ...
```

### Task Options

Control which queue, priority, and backend the task uses:

```python
@scheduled_task(
    cron="*/10 * * * *",
    queue_name="high-priority",
    priority=10,
    backend="default",
)
@task()
def health_check() -> None:
    ...
```

## Cron Expression Syntax

django-periodic-tasks uses standard 5-field cron expressions:

```
┌───────────── minute (0–59)
│ ┌───────────── hour (0–23)
│ │ ┌───────────── day of month (1–31)
│ │ │ ┌───────────── month (1–12)
│ │ │ │ ┌───────────── day of week (0–6, Sun=0)
│ │ │ │ │
* * * * *
```

**Examples:**

| Expression | Meaning |
|------------|---------|
| `* * * * *` | Every minute |
| `*/15 * * * *` | Every 15 minutes |
| `0 * * * *` | Every hour |
| `0 8 * * *` | Daily at 8:00 AM |
| `0 8 * * 1-5` | Weekdays at 8:00 AM |
| `0 0 1 * *` | First of every month at midnight |
| `30 2 * * 0` | Sundays at 2:30 AM |

### Using fluentcron

If you prefer a type-safe, self-documenting API over raw cron strings, use the [fluentcron](https://gitlab.com/thelabnyc/fluentcron/) library:

```sh
pip install fluentcron
```

fluentcron provides a fluent builder and shortcut functions that produce standard cron expression strings:

```python
from django_tasks import task
from django_periodic_tasks import scheduled_task
from fluentcron import CronSchedule, daily_at, every_n_minutes

# Shortcut function — returns a cron string directly
@scheduled_task(cron=every_n_minutes(15))  # "*/15 * * * *"
@task()
def health_check() -> None:
    ...


# Shortcut function
@scheduled_task(cron=daily_at(8))  # "0 8 * * *"
@task()
def morning_report() -> None:
    ...


# Fluent builder — call str() or .to_str() for the cron string
@scheduled_task(
    cron=str(CronSchedule().weekly().on_monday().at(9, 30)),  # "30 9 * * 1"
)
@task()
def weekly_digest() -> None:
    ...
```

fluentcron also ships with `CommonSchedules` presets for frequently used expressions:

```python
from fluentcron import CommonSchedules

@scheduled_task(cron=CommonSchedules.EVERY_HOUR)  # "0 * * * *"
@task()
def sync_data() -> None:
    ...
```

### Timezone Support

By default, cron expressions are evaluated in UTC. Specify a timezone to match against local time:

```python
@scheduled_task(
    cron="0 9 * * *",       # 9:00 AM
    timezone="US/Eastern",   # in Eastern time
)
@task()
def east_coast_morning() -> None:
    ...
```

The `timezone` parameter accepts any IANA timezone name (e.g. `"US/Pacific"`, `"Europe/London"`, `"Asia/Tokyo"`).

## Database-Defined Schedules

For schedules that need to be managed at runtime without a code deployment, create them through the Django admin:

1. Navigate to **Django Periodic Tasks > Scheduled tasks** in the admin.
2. Click **Add scheduled task**.
3. Fill in the task path (e.g. `myapp.tasks.morning_report`), cron expression, and any arguments.
4. The schedule takes effect on the next scheduler tick.

Database-defined schedules have `source = Database` and can be freely edited or disabled through the admin.

## Schedule Sync

When the scheduler starts, it syncs code-defined schedules to the database:

- **New** code schedules are created as `ScheduledTask` rows with `source = Code`.
- **Changed** code schedules (cron expression, task path, options) are updated in place.
- **Removed** code schedules (present in DB but no longer in the registry) are disabled.
- **Database-defined** schedules are never modified by the sync process.

Code-defined schedules appear as read-only in the Django admin.
