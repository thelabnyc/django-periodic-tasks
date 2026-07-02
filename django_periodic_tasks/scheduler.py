from datetime import timedelta
import logging
import threading

from django.conf import settings
from django.db import transaction
from django.db.models import F, Q
from django.utils import timezone

from django_periodic_tasks.cron import compute_next_run_at
from django_periodic_tasks.enqueue import dispatch_execution
from django_periodic_tasks.models import ScheduledTask, TaskExecution
from django_periodic_tasks.sync import sync_code_schedules
from django_periodic_tasks.task_resolver import resolve_task

logger = logging.getLogger(__name__)

_STALE_EXECUTION_BATCH_SIZE = 100
_DEFAULT_REDISPATCH_AFTER = 300  # seconds since the last dispatch attempt before re-dispatch
_DEFAULT_MAX_DISPATCH_ATTEMPTS = 3  # total dispatch attempts per execution (initial + retries)


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
        st.enqueue_now()

        st.last_run_at = timezone.now()
        st.next_run_at = compute_next_run_at(st.cron_expression, st.timezone)
        st.total_run_count = F("total_run_count") + 1
        st.save(update_fields=["last_run_at", "next_run_at", "total_run_count"])

        logger.info("Enqueued scheduled task: %s", st.name)

    def _max_dispatch_attempts(self) -> int:
        max_attempts: int = getattr(settings, "PERIODIC_TASKS_MAX_DISPATCH_ATTEMPTS", _DEFAULT_MAX_DISPATCH_ATTEMPTS)
        if max_attempts <= 0:
            raise ValueError(f"PERIODIC_TASKS_MAX_DISPATCH_ATTEMPTS must be positive, got {max_attempts}")
        return max_attempts

    def _cleanup_stale_executions(self) -> None:
        """Re-dispatch PENDING executions whose redelivery lease has expired.

        ``dispatched_at`` is a lease ("last dispatch attempt"), not a permanent "sent"
        marker. A PENDING row is re-dispatched when it was never dispatched
        (``dispatched_at IS NULL`` and older than the lease) or its last attempt did not
        complete within ``PERIODIC_TASKS_REDISPATCH_AFTER`` — capped at
        ``PERIODIC_TASKS_MAX_DISPATCH_ATTEMPTS`` total. This avoids both a per-tick
        re-enqueue storm and permanent silent loss; ``@exactly_once`` suppresses a
        re-dispatch that merely raced a slow worker.

        Direct enqueue (not ``on_commit``) is safe: the row was committed in a prior
        tick and is already visible to workers.
        """
        redispatch_after: int = getattr(settings, "PERIODIC_TASKS_REDISPATCH_AFTER", _DEFAULT_REDISPATCH_AFTER)
        if redispatch_after <= 0:
            raise ValueError(f"PERIODIC_TASKS_REDISPATCH_AFTER must be positive, got {redispatch_after}")
        max_attempts = self._max_dispatch_attempts()

        threshold = timezone.now() - timedelta(seconds=redispatch_after)
        # Lease expired with attempts left: never dispatched but old enough (a real loss,
        # not a row whose on_commit is about to fire), or dispatched but not completed in time.
        lease_expired = Q(dispatch_count__lt=max_attempts) & (Q(dispatched_at__isnull=True, created_at__lt=threshold) | Q(dispatched_at__lt=threshold))

        # Read candidate IDs without locking, then lock + dispatch one row per
        # transaction. Each lock — and the broker call it guards — is scoped to a
        # single row, so the transaction never stays open across the whole loop.
        # Concurrent schedulers then share the backlog via SKIP LOCKED instead of
        # the first one grabbing every stale row in a single batch. Re-checking
        # ``lease_expired`` after the lock keeps re-dispatch bounded: a row another
        # scheduler just dispatched has a fresh lease (or +1 count) and is skipped.
        stale_ids = list(
            TaskExecution.objects.filter(
                lease_expired,
                status=TaskExecution.Status.PENDING,
                scheduled_task__enabled=True,
            )
            .order_by("created_at", "id")
            .values_list("id", flat=True)[:_STALE_EXECUTION_BATCH_SIZE]
        )

        for execution_id in stale_ids:
            try:
                with transaction.atomic():
                    execution = (
                        TaskExecution.objects.filter(
                            lease_expired,
                            id=execution_id,
                            status=TaskExecution.Status.PENDING,
                            scheduled_task__enabled=True,
                        )
                        .select_related("scheduled_task")
                        # of=("self",): lock only the execution row, NOT the joined
                        # ScheduledTask. Siblings share one parent; locking it would
                        # make a concurrent scheduler SKIP LOCKED past every sibling.
                        .select_for_update(skip_locked=True, of=("self",))
                        .first()
                    )
                    if execution is None:
                        # Locked by another scheduler, or lease no longer expired
                        # (dispatched/completed) since the unlocked read. Not ours.
                        continue
                    attempt = execution.dispatch_count + 1
                    st = execution.scheduled_task
                    configured = resolve_task(st.task_path).using(
                        queue_name=st.queue_name,
                        priority=st.priority,
                        backend=st.backend,
                    )
                    dispatch_execution(configured, execution)
                logger.info(
                    "Re-dispatched stale execution %s (attempt %d/%d) for task %s",
                    execution_id,
                    attempt,
                    max_attempts,
                    st.name,
                )
                if attempt >= max_attempts:
                    logger.warning(
                        "Queued final dispatch attempt for execution %s (%d/%d); no further re-dispatches will be attempted (task %s)",
                        execution_id,
                        attempt,
                        max_attempts,
                        st.name,
                    )
            except Exception:
                logger.exception(
                    "Failed to re-dispatch stale execution %s",
                    execution_id,
                )

    def _delete_old_executions(self) -> None:
        """Bulk-delete non-PENDING TaskExecution rows older than 24 hours.

        Terminal (non-PENDING) rows have no ongoing purpose once workers have
        finished processing them. PENDING rows are preserved: rows with attempts
        remaining stay eligible for re-dispatch, while exhausted rows remain as
        evidence for operator inspection.
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
