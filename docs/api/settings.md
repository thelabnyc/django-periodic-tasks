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

## PERIODIC_TASKS_REDISPATCH_AFTER

| | |
|---|---|
| **Type** | `int` |
| **Default** | `300` |

Seconds since the latest dispatch attempt before stale cleanup re-dispatches a still-`PENDING` `@exactly_once` execution. This also controls how old a never-dispatched `TaskExecution` row must be before cleanup treats it as stale.

```python
PERIODIC_TASKS_REDISPATCH_AFTER = 300  # re-dispatch after 5 minutes without completion
```

Use a value larger than your expected queue latency. Lower values recover dropped broker messages sooner, but can create more duplicate queue messages for slow-but-valid work.

## PERIODIC_TASKS_MAX_DISPATCH_ATTEMPTS

| | |
|---|---|
| **Type** | `int` |
| **Default** | `3` |

Maximum number of successful enqueue attempts for one `@exactly_once` execution, including the initial enqueue. Once a `PENDING` execution reaches this count, stale cleanup stops re-dispatching it.

```python
PERIODIC_TASKS_MAX_DISPATCH_ATTEMPTS = 3
```

Exhausted `PENDING` rows are kept for operator inspection. Stale cleanup will not re-dispatch them again, though a worker delivery already in flight may still mark them completed.
