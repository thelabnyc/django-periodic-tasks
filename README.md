# django-periodic-tasks

Periodic/cron task scheduling for [django-tasks](https://github.com/RealOrangeOne/django-tasks). Backend-agnostic replacement for celery-beat + django-celery-beat.

## Installation

```bash
pip install django-periodic-tasks
```

Add to `INSTALLED_APPS`:

```python
INSTALLED_APPS = [
    ...
    "django_periodic_tasks",
]
```

## Usage

### Define scheduled tasks

```python
from django_tasks import task
from django_periodic_tasks import scheduled_task

@scheduled_task(cron="0 5 * * *", name="daily-report")
@task()
def daily_report() -> None:
    ...
```

### Run the scheduler

Enable the autostart setting so the scheduler runs as a daemon thread inside your Django process:

```python
# settings.py
PERIODIC_TASKS_AUTOSTART = True
```

Or run it as a standalone process:

```bash
python manage.py run_scheduler
```
