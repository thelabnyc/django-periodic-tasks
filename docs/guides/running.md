# Running the Scheduler

django-periodic-tasks provides several ways to run the scheduler depending on your backend and deployment needs.

## Combined Worker (Recommended)

If you're using the django-tasks **database backend**, the `scheduler_db_worker` command runs both the task worker and the periodic scheduler in a single process:

```sh
python manage.py scheduler_db_worker
```

The scheduler runs as a daemon thread alongside the database worker. This is the simplest and recommended setup for most deployments.

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--scheduler-interval` | `15` | Seconds between scheduler ticks |

All standard `db_worker` flags are also available (e.g. `--queue-name`, `--backend`).

## Standalone Scheduler

For non-database backends (e.g. RQ) or when you want the scheduler in its own process, use `run_scheduler`:

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

## Autostart Mode

For development or simple single-process deployments, enable the autostart setting to start the scheduler automatically when Django starts:

```python
# settings.py
PERIODIC_TASKS_AUTOSTART = True
PERIODIC_TASKS_SCHEDULER_INTERVAL = 15  # optional, default 15
```

The scheduler starts as a daemon thread during `AppConfig.ready()`. This is convenient for development but offers less control than the management commands.

!!! warning
    Autostart mode is not recommended for production deployments. Use the management commands instead for proper signal handling and process management.

## Multi-Worker Deployment

The scheduler is safe to run across multiple processes. Each scheduler tick uses `SELECT FOR UPDATE SKIP LOCKED` to claim due tasks, so even if multiple scheduler instances run simultaneously, each task is enqueued exactly once.

A typical production setup:

```
┌─────────────────────────┐  ┌─────────────────────────┐
│  scheduler_db_worker    │  │  scheduler_db_worker    │
│  (scheduler + worker)   │  │  (scheduler + worker)   │
└─────────────────────────┘  └─────────────────────────┘
         │                            │
         └──────────┬─────────────────┘
                    │
           ┌────────▼────────┐
           │   PostgreSQL    │
           │  (shared DB)    │
           └─────────────────┘
```

Both instances run the scheduler, but `SKIP LOCKED` ensures no duplicate enqueues. Scale horizontally by adding more worker processes.

## Graceful Shutdown

Both management commands handle `SIGINT` and `SIGTERM` for graceful shutdown:

- The current scheduler tick completes.
- The stop event is set, and the scheduler thread exits.
- The process terminates cleanly.

In container environments (Docker, Kubernetes), this means `docker stop` and Kubernetes pod termination work correctly out of the box.
