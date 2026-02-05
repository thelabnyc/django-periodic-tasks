import logging
import threading

from django.db import transaction
from django.utils import timezone

from django_periodic_tasks.cron import compute_next_run_at
from django_periodic_tasks.models import ScheduledTask
from django_periodic_tasks.sync import sync_code_schedules
from django_periodic_tasks.task_resolver import resolve_task

logger = logging.getLogger(__name__)


class PeriodicTaskScheduler(threading.Thread):
    """Daemon thread that periodically enqueues due scheduled tasks.

    On each tick the scheduler:

    1. Queries :class:`~django_periodic_tasks.models.ScheduledTask` for rows
       whose ``next_run_at ≤ now`` and ``enabled = True``.
    2. Locks those rows with ``SELECT FOR UPDATE SKIP LOCKED`` so multiple
       scheduler instances can run safely in parallel.
    3. Resolves each task path to a django-tasks ``Task`` object and calls
       ``task.using(...).enqueue(...)``.
    4. Updates ``last_run_at``, ``next_run_at``, and ``total_run_count``.

    Args:
        interval: Seconds between scheduler ticks (default ``15``).
    """

    daemon = True

    def __init__(self, interval: int = 15) -> None:
        super().__init__(name="periodic-task-scheduler")
        self.interval = interval
        self._stop_event = threading.Event()

    def run(self) -> None:
        """Start the scheduler loop.

        Syncs code-defined schedules to the database, then enters the
        tick-sleep loop until :meth:`stop` is called.
        """
        logger.info("Periodic task scheduler starting (interval=%ds)", self.interval)
        sync_code_schedules()
        while not self._stop_event.is_set():
            self.tick()
            self._stop_event.wait(self.interval)
        logger.info("Periodic task scheduler stopped")

    def tick(self) -> None:
        """Single scheduler tick: find and enqueue due tasks.

        All due tasks are locked with ``SELECT FOR UPDATE SKIP LOCKED`` for the
        duration of the tick so that concurrent scheduler instances never
        enqueue the same task twice.
        """
        now = timezone.now()

        with transaction.atomic():
            due_tasks = list(
                ScheduledTask.objects.filter(
                    enabled=True,
                    next_run_at__lte=now,
                )
                .select_for_update(skip_locked=True)
            )

            for st in due_tasks:
                try:
                    self._process_task(st)
                except Exception:
                    logger.exception("Failed to enqueue scheduled task id=%s", st.id)

    def _process_task(self, st: ScheduledTask) -> None:
        task_obj = resolve_task(st.task_path)
        configured = task_obj.using(
            queue_name=st.queue_name,
            priority=st.priority,
            backend=st.backend,
        )
        configured.enqueue(*st.args, **st.kwargs)

        st.last_run_at = timezone.now()
        st.next_run_at = compute_next_run_at(st.cron_expression, st.timezone)
        st.total_run_count += 1
        st.save(update_fields=["last_run_at", "next_run_at", "total_run_count"])

        logger.info("Enqueued scheduled task: %s (run #%d)", st.name, st.total_run_count)

    def stop(self) -> None:
        """Signal the scheduler to stop after the current tick completes."""
        self._stop_event.set()
