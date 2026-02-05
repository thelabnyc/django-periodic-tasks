# django-periodic-tasks

Periodic/cron task scheduling for [django-tasks](https://github.com/RealOrangeOne/django-tasks). A backend-agnostic replacement for celery-beat.

## Features

- **Cron-based scheduling** — Standard 5-field cron expressions with timezone support.
- **Code-defined schedules** — Declare schedules in Python with the `@scheduled_task` decorator; they sync to the database automatically.
- **Database-defined schedules** — Create and manage schedules through the Django admin for runtime flexibility.
- **Exactly-once execution** — Optional `@exactly_once` decorator guarantees a task runs at most once per scheduled invocation, even with non-transactional backends.
- **Multi-worker safe** — Uses `SELECT FOR UPDATE SKIP LOCKED` so multiple scheduler processes never double-enqueue the same task.
- **Backend-agnostic** — Works with any django-tasks backend (database, RQ, etc.).

## Installation

```sh
pip install django-periodic-tasks
```

Add to your `INSTALLED_APPS` and run migrations:

```python
INSTALLED_APPS = [
    # ...
    "django_tasks",
    "django_periodic_tasks",
    # ...
]
```

```sh
python manage.py migrate
```

## Next Steps

{nav}

<style type="text/css">
.autodoc { display: none; }
</style>

::: sandbox.settings_docgen.setup
