# Running the Scheduler

django-periodic-tasks runs as a daemon thread inside your Django process. Enable it with a single setting and the scheduler starts automatically alongside your application.

## Autostart (Recommended)

Add the following to your Django settings:

```python
# settings.py
PERIODIC_TASKS_AUTOSTART = True
```

When Django starts, `AppConfig.ready()` launches the scheduler as a daemon thread. It syncs code-defined schedules to the database, then enters a tick-sleep loop that checks for due tasks every `PERIODIC_TASKS_SCHEDULER_INTERVAL` seconds (default: 15).

The daemon thread exits automatically when the main process shuts down — no signal handling or cleanup is needed on your part.

### Tuning the Interval

```python
PERIODIC_TASKS_SCHEDULER_INTERVAL = 30  # check every 30 seconds
```

Lower values mean tasks are enqueued closer to their scheduled time but increase database load. The default of 15 seconds is a good balance for most applications.

## Standalone Scheduler

For cases where you want the scheduler in its own dedicated process (e.g. separating scheduling from request serving), use the `run_scheduler` management command:

```sh
python manage.py run_scheduler
```

This runs the scheduler loop in the main thread (blocking). You'll need to run your task worker separately.

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--interval` | `15` | Seconds between scheduler ticks |
| `-v 0` | — | Suppress all output |
| `-v 2` | — | Enable debug logging |

## Multi-Worker Deployment

The scheduler is safe to run across multiple processes. Each scheduler tick uses `SELECT FOR UPDATE SKIP LOCKED` to claim due tasks, so even if multiple scheduler instances run simultaneously, each task is enqueued exactly once.

A typical production setup with two web/worker processes:

```
┌─────────────────────────┐  ┌─────────────────────────┐
│   gunicorn / daphne     │  │   gunicorn / daphne     │
│   (autostart=True)      │  │   (autostart=True)      │
│   scheduler + app       │  │   scheduler + app       │
└─────────────────────────┘  └─────────────────────────┘
         │                            │
         └──────────┬─────────────────┘
                    │
           ┌────────▼────────┐
           │   PostgreSQL    │
           │  (shared DB)    │
           └─────────────────┘
```

Both instances run the scheduler, but `SKIP LOCKED` ensures no duplicate enqueues. Scale horizontally by adding more processes.

## Graceful Shutdown

The `run_scheduler` command handles `SIGINT` and `SIGTERM` for graceful shutdown:

- The current scheduler tick completes.
- The stop event is set, and the scheduler thread exits.
- The process terminates cleanly.

When using autostart, the scheduler thread is a daemon thread and exits automatically when the main process terminates. In container environments (Docker, Kubernetes), this means `docker stop` and Kubernetes pod termination work correctly out of the box.
