from datetime import timedelta
import logging
import threading

from django.db import transaction
from django.utils import timezone

from django_periodic_tasks.cron import compute_next_run_at
from django_periodic_tasks.models import ScheduledTask, TaskExecution
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
        if interval <= 0:
            raise ValueError(f"interval must be positive, got {interval}")
        super().__init__(name="periodic-task-scheduler")
        self.interval = interval
        self._stop_event = threading.Event()

    def run(self) -> None:
        """Start the scheduler loop.

        Syncs code-defined schedules to the database, then enters the
        tick-sleep loop until :meth:`stop` is called.
        """
        logger.info("Periodic task scheduler starting (interval=%ds)", self.interval)
        try:
            sync_code_schedules()
        except Exception:
            logger.exception("Failed to sync code schedules on startup")
        while not self._stop_event.is_set():
            try:
                self.tick()
            except Exception:
                logger.exception("Scheduler tick failed")
            self._stop_event.wait(self.interval)
        logger.info("Periodic task scheduler stopped")

    def tick(self) -> None:
        """Single scheduler tick: find and enqueue due tasks.

        All due tasks are locked with ``SELECT FOR UPDATE SKIP LOCKED`` for the
        duration of the tick so that concurrent scheduler instances never
        enqueue the same task twice.
        """
        try:
            self._cleanup_stale_executions()
        except Exception:
            logger.exception("Stale execution cleanup failed")

        try:
            self._delete_old_executions()
        except Exception:
            logger.exception("Old execution cleanup failed")

        now = timezone.now()

        with transaction.atomic():
            due_tasks = list(
                ScheduledTask.objects.filter(
                    enabled=True,
                    next_run_at__lte=now,
                ).select_for_update(skip_locked=True)
            )

            for st in due_tasks:
                try:
                    with transaction.atomic():
                        self._process_task(st)
                except Exception:
                    logger.exception("Failed to enqueue scheduled task id=%s", st.id)
                    try:
                        st.next_run_at = compute_next_run_at(st.cron_expression, st.timezone)
                        st.save(update_fields=["next_run_at"])
                    except Exception:
                        logger.exception("Failed to advance next_run_at for task id=%s", st.id)

    def _process_task(self, st: ScheduledTask) -> None:
        task_obj = resolve_task(st.task_path)
        configured = task_obj.using(
            queue_name=st.queue_name,
            priority=st.priority,
            backend=st.backend,
        )

        is_exactly_once = getattr(task_obj.func, "_exactly_once", False)

        if is_exactly_once:
            execution = TaskExecution.objects.create(scheduled_task=st)
            enqueue_kwargs = {**st.kwargs, "_periodic_tasks_execution_id": str(execution.id)}

            def _deferred_enqueue() -> None:
                configured.enqueue(*st.args, **enqueue_kwargs)

            transaction.on_commit(_deferred_enqueue)
        else:
            configured.enqueue(*st.args, **st.kwargs)

        st.last_run_at = timezone.now()
        st.next_run_at = compute_next_run_at(st.cron_expression, st.timezone)
        st.total_run_count += 1
        st.save(update_fields=["last_run_at", "next_run_at", "total_run_count"])

        logger.info("Enqueued scheduled task: %s (run #%d)", st.name, st.total_run_count)

    def _cleanup_stale_executions(self) -> None:
        """Re-enqueue stale PENDING TaskExecutions that were never delivered.

        A TaskExecution can become stale if the scheduler committed the row but
        the ``on_commit`` callback that enqueues the task never fired (e.g. the
        process crashed, the connection was reset, etc.).

        Direct enqueue (not ``on_commit``) is safe here because the
        TaskExecution row was committed in a prior tick's transaction and is
        already visible to workers.
        """
        threshold = timezone.now() - timedelta(seconds=max(60, 2 * self.interval))

        with transaction.atomic():
            stale = list(
                TaskExecution.objects.filter(
                    status=TaskExecution.Status.PENDING,
                    created_at__lt=threshold,
                    scheduled_task__enabled=True,
                )
                .select_related("scheduled_task")
                .select_for_update(skip_locked=True)
            )

        for execution in stale:
            try:
                st = execution.scheduled_task
                task_obj = resolve_task(st.task_path)
                configured = task_obj.using(
                    queue_name=st.queue_name,
                    priority=st.priority,
                    backend=st.backend,
                )
                enqueue_kwargs = {
                    **st.kwargs,
                    "_periodic_tasks_execution_id": str(execution.id),
                }
                configured.enqueue(*st.args, **enqueue_kwargs)
                logger.info(
                    "Re-enqueued stale execution %s for task %s",
                    execution.id,
                    st.name,
                )
            except Exception:
                logger.exception(
                    "Failed to re-enqueue stale execution %s",
                    execution.id,
                )

    def _delete_old_executions(self) -> None:
        """Bulk-delete non-PENDING TaskExecution rows older than 24 hours.

        COMPLETED rows have no ongoing purpose once workers have finished
        processing them.  PENDING rows are preserved because they may still be
        awaiting delivery or re-enqueue by stale cleanup.
        """
        threshold = timezone.now() - timedelta(hours=24)
        deleted, _ = (
            TaskExecution.objects.filter(
                created_at__lt=threshold,
            )
            .exclude(
                status=TaskExecution.Status.PENDING,
            )
            .delete()
        )
        if deleted:
            logger.info("Deleted %d old task execution(s)", deleted)

    def stop(self) -> None:
        """Signal the scheduler to stop after the current tick completes."""
        self._stop_event.set()
