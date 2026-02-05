# Getting Started

This guide walks you through installing django-periodic-tasks, defining your first scheduled task, and running the scheduler.

## Prerequisites

- Python 3.13+
- Django 5.2+
- [django-tasks](https://github.com/RealOrangeOne/django-tasks) installed and configured with at least one backend

## Installation

Install the package:

```sh
pip install django-periodic-tasks
```

Add both `django_tasks` and `django_periodic_tasks` to your `INSTALLED_APPS`:

```python
INSTALLED_APPS = [
    # ...
    "django_tasks",
    "django_tasks.backends.database",  # if using the database backend
    "django_periodic_tasks",
    # ...
]
```

Run migrations to create the `ScheduledTask` table:

```sh
python manage.py migrate
```

## Define Your First Scheduled Task

Create a task using django-tasks' `@task()` decorator, then register it for periodic execution with `@scheduled_task()`. Place your tasks in a `tasks.py` module inside any installed app — django-periodic-tasks automatically discovers these modules at startup (similar to how Django discovers `admin.py` files):

```python
# myapp/tasks.py
from django_tasks import task
from django_periodic_tasks.registry import scheduled_task


@scheduled_task(cron="*/5 * * * *")  # Every 5 minutes
@task()
def send_digest() -> None:
    """Send the email digest."""
    ...
```

The `@scheduled_task` decorator must be applied **above** `@task()`. The `cron` parameter accepts any standard 5-field cron expression.

## Run the Scheduler

Enable the scheduler by adding this to your settings:

```python
# settings.py
PERIODIC_TASKS_AUTOSTART = True
```

The scheduler starts automatically as a daemon thread when Django starts. It checks for due tasks every 15 seconds (configurable via `PERIODIC_TASKS_SCHEDULER_INTERVAL`).

Run your task worker as usual (e.g. `python manage.py db_worker` for the database backend). The scheduler thread enqueues tasks; the worker executes them.

## Verify It Works

1. Open the Django admin at `/admin/django_periodic_tasks/scheduledtask/`.
2. You should see your task listed with `source = Code`.
3. The `next_run_at` field shows when the task will next be enqueued.
4. After the scheduled time passes, `last_run_at` and `total_run_count` will update.
