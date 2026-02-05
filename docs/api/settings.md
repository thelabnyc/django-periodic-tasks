# Settings

django-periodic-tasks is configured through Django settings.

## PERIODIC_TASKS_AUTOSTART

| | |
|---|---|
| **Type** | `bool` |
| **Default** | `False` |

When `True`, the scheduler starts automatically as a daemon thread during `AppConfig.ready()`. Useful for development and simple single-process deployments.

```python
PERIODIC_TASKS_AUTOSTART = True
```

## PERIODIC_TASKS_SCHEDULER_INTERVAL

| | |
|---|---|
| **Type** | `int` |
| **Default** | `15` |

Seconds between scheduler ticks. On each tick the scheduler queries the database for due tasks and enqueues them.

```python
PERIODIC_TASKS_SCHEDULER_INTERVAL = 30  # check every 30 seconds
```

Lower values mean tasks are enqueued closer to their scheduled time, but increase database load. The default of 15 seconds is a good balance for most applications.
