# Models

## ScheduledTask

::: django_periodic_tasks.models.ScheduledTask
    :docstring:

## ScheduledTask.Source

Where a scheduled task definition comes from.

- `CODE` — Managed by the codebase and synced automatically on scheduler startup.
- `DATABASE` — Managed by operators through the Django admin.

## TaskExecution

::: django_periodic_tasks.models.TaskExecution
    :docstring:

## TaskExecution.Status

The lifecycle status of an execution permit.

- `PENDING` — Created by the scheduler, awaiting worker pickup.
- `COMPLETED` — The `@exactly_once` decorator ran the task successfully.
