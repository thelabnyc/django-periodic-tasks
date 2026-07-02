from datetime import datetime, timedelta, timezone
from unittest.mock import patch
import threading
import time

from django.db import connection
from django.test import TestCase, TransactionTestCase, override_settings
from django.utils import timezone as django_tz

from django_periodic_tasks.compat import DUMMY_BACKEND_PATH, default_task_backend
from django_periodic_tasks.models import ScheduledTask, TaskExecution
from django_periodic_tasks.scheduler import PeriodicTaskScheduler

DUMMY_BACKEND_SETTINGS = {
    "default": {
        "BACKEND": DUMMY_BACKEND_PATH,
        "ENQUEUE_ON_COMMIT": False,
        "QUEUES": ["default", "special"],
    }
}


@override_settings(TASKS=DUMMY_BACKEND_SETTINGS)
class TestSchedulerTick(TestCase):
    def setUp(self) -> None:
        default_task_backend.clear()

    def _create_due_task(self, name: str = "due-task", **kwargs: object) -> ScheduledTask:
        defaults = {
            "name": name,
            "task_path": "sandbox.testapp.tasks.example_task",
            "cron_expression": "* * * * *",
            "enabled": True,
            "next_run_at": datetime.now(tz=timezone.utc) - timedelta(minutes=1),
        }
        defaults.update(kwargs)  # type: ignore[arg-type]
        return ScheduledTask.objects.create(**defaults)

    def _create_future_task(self, name: str = "future-task") -> ScheduledTask:
        return ScheduledTask.objects.create(
            name=name,
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="* * * * *",
            enabled=True,
            next_run_at=datetime.now(tz=timezone.utc) + timedelta(hours=1),
        )

    def test_tick_enqueues_due_task(self) -> None:
        self._create_due_task()
        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler.tick()

        self.assertEqual(len(default_task_backend.results), 1)

    def test_tick_skips_future_task(self) -> None:
        self._create_future_task()
        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler.tick()

        self.assertEqual(len(default_task_backend.results), 0)

    def test_tick_skips_disabled_task(self) -> None:
        ScheduledTask.objects.create(
            name="disabled",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="* * * * *",
            enabled=False,
            next_run_at=datetime.now(tz=timezone.utc) - timedelta(minutes=1),
        )
        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler.tick()

        self.assertEqual(len(default_task_backend.results), 0)

    def test_tick_updates_last_run_at(self) -> None:
        st = self._create_due_task()
        self.assertIsNone(st.last_run_at)

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler.tick()

        st.refresh_from_db()
        self.assertIsNotNone(st.last_run_at)

    def test_tick_updates_next_run_at(self) -> None:
        old_next = datetime.now(tz=timezone.utc) - timedelta(minutes=1)
        st = self._create_due_task(next_run_at=old_next)

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler.tick()

        st.refresh_from_db()
        self.assertIsNotNone(st.next_run_at)
        self.assertGreater(st.next_run_at, old_next)

    def test_tick_increments_run_count(self) -> None:
        st = self._create_due_task()
        self.assertEqual(st.total_run_count, 0)

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler.tick()

        st.refresh_from_db()
        self.assertEqual(st.total_run_count, 1)

    def test_tick_multiple_due_tasks(self) -> None:
        self._create_due_task(name="task-1")
        self._create_due_task(name="task-2")
        self._create_due_task(name="task-3")

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler.tick()

        self.assertEqual(len(default_task_backend.results), 3)

    def test_tick_with_task_args(self) -> None:
        self._create_due_task(
            name="args-task",
            task_path="sandbox.testapp.tasks.example_task_with_args",
            args=["hello"],
            kwargs={"count": 5},
        )

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler.tick()

        self.assertEqual(len(default_task_backend.results), 1)

    def test_tick_handles_bad_task_path(self) -> None:
        self._create_due_task(
            name="bad-task",
            task_path="nonexistent.module.task",
        )

        scheduler = PeriodicTaskScheduler(interval=60)
        # Should not raise; should log error and continue
        scheduler.tick()

        self.assertEqual(len(default_task_backend.results), 0)

    def test_tick_continues_after_single_failure(self) -> None:
        self._create_due_task(name="bad-task", task_path="nonexistent.module.task")
        self._create_due_task(name="good-task")

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler.tick()

        # The good task should still be enqueued
        self.assertEqual(len(default_task_backend.results), 1)

    def test_failed_task_advances_next_run_at(self) -> None:
        """Bug 3: A failed task should advance next_run_at to the next cron time."""
        old_next = datetime.now(tz=timezone.utc) - timedelta(minutes=1)
        st = self._create_due_task(
            name="fail-advance",
            task_path="nonexistent.module.task",
            next_run_at=old_next,
        )

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler.tick()

        st.refresh_from_db()
        # next_run_at should have been advanced despite the failure
        self.assertIsNotNone(st.next_run_at)
        self.assertGreater(st.next_run_at, old_next)

    def test_savepoint_isolates_task_failures(self) -> None:
        """Bug 2: A failing task should not roll back the entire tick transaction."""
        # Create two due tasks: one bad, one good
        self._create_due_task(name="bad-task", task_path="nonexistent.module.task")
        good_st = self._create_due_task(name="good-task")

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler.tick()

        # Good task should be processed normally
        good_st.refresh_from_db()
        self.assertEqual(good_st.total_run_count, 1)
        self.assertEqual(len(default_task_backend.results), 1)

    def test_tick_uses_queue_name(self) -> None:
        self._create_due_task(name="queued-task", queue_name="special")

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler.tick()

        self.assertEqual(len(default_task_backend.results), 1)
        result = default_task_backend.results[0]
        self.assertEqual(result.task.queue_name, "special")

    def test_tick_uses_priority(self) -> None:
        self._create_due_task(name="priority-task", priority=10)

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler.tick()

        self.assertEqual(len(default_task_backend.results), 1)
        result = default_task_backend.results[0]
        self.assertEqual(result.task.priority, 10)


@override_settings(TASKS=DUMMY_BACKEND_SETTINGS)
class TestSchedulerThread(TestCase):
    def test_scheduler_is_daemon(self) -> None:
        scheduler = PeriodicTaskScheduler(interval=60)
        self.assertTrue(scheduler.daemon)

    def test_stop(self) -> None:
        scheduler = PeriodicTaskScheduler(interval=60)
        self.assertFalse(scheduler._stop_event.is_set())
        scheduler.stop()
        self.assertTrue(scheduler._stop_event.is_set())

    def test_zero_interval_raises(self) -> None:
        """Bug 12: interval <= 0 should raise ValueError."""
        with self.assertRaises(ValueError):
            PeriodicTaskScheduler(interval=0)

    def test_negative_interval_raises(self) -> None:
        """Bug 12: interval <= 0 should raise ValueError."""
        with self.assertRaises(ValueError):
            PeriodicTaskScheduler(interval=-5)

    @patch("django_periodic_tasks.scheduler.sync_code_schedules", side_effect=RuntimeError("sync boom"))
    def test_run_survives_sync_error(self, mock_sync: object) -> None:
        """Bug 1: run() should not crash if sync_code_schedules raises."""
        scheduler = PeriodicTaskScheduler(interval=1)
        scheduler._stop_event.set()
        # Should not raise
        scheduler.run()

    @patch.object(PeriodicTaskScheduler, "tick", side_effect=RuntimeError("tick boom"))
    def test_run_survives_tick_error(self, mock_tick: object) -> None:
        """Bug 1: run() should not crash if tick() raises."""
        scheduler = PeriodicTaskScheduler(interval=1)
        # After one tick error, stop
        original_wait = scheduler._stop_event.wait

        call_count = 0

        def stop_after_one(timeout: float | None = None) -> bool:
            nonlocal call_count
            call_count += 1
            if call_count >= 1:
                scheduler._stop_event.set()
            return original_wait(timeout)

        scheduler._stop_event.wait = stop_after_one  # type: ignore[assignment]
        # Should not raise
        scheduler.run()


@override_settings(TASKS=DUMMY_BACKEND_SETTINGS)
class TestSchedulerConcurrency(TransactionTestCase):
    def setUp(self) -> None:
        default_task_backend.clear()

    def test_concurrent_ticks_no_duplicate_enqueue(self) -> None:
        """Two schedulers ticking concurrently must not enqueue the same task twice."""
        st = ScheduledTask.objects.create(
            name="concurrent-test",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="* * * * *",
            enabled=True,
            next_run_at=datetime.now(tz=timezone.utc) - timedelta(minutes=1),
        )

        entered = threading.Event()
        original_process = PeriodicTaskScheduler._process_task

        def slow_process(self_sched: PeriodicTaskScheduler, task: ScheduledTask) -> None:
            entered.set()
            time.sleep(0.5)
            original_process(self_sched, task)

        scheduler_a = PeriodicTaskScheduler(interval=60)
        scheduler_b = PeriodicTaskScheduler(interval=60)

        def tick_in_thread() -> None:
            try:
                scheduler_a.tick()
            finally:
                connection.close()

        with patch.object(PeriodicTaskScheduler, "_process_task", slow_process):
            thread_a = threading.Thread(target=tick_in_thread)
            thread_a.start()

            # Wait for thread A to be inside _process_task (lock is held)
            entered.wait()
            # Thread B should skip the locked row
            scheduler_b.tick()

            thread_a.join()

        st.refresh_from_db()
        self.assertEqual(st.total_run_count, 1)


@override_settings(TASKS=DUMMY_BACKEND_SETTINGS)
class TestSchedulerExactlyOnce(TestCase):
    """Tests for exactly_once task processing in the scheduler."""

    def setUp(self) -> None:
        default_task_backend.clear()

    def _create_due_task(self, **kwargs: object) -> ScheduledTask:
        defaults = {
            "name": "exactly-once-test",
            "task_path": "sandbox.testapp.tasks.exactly_once_task",
            "cron_expression": "* * * * *",
            "enabled": True,
            "next_run_at": datetime.now(tz=timezone.utc) - timedelta(minutes=1),
        }
        defaults.update(kwargs)  # type: ignore[arg-type]
        return ScheduledTask.objects.create(**defaults)

    def test_exactly_once_creates_execution_and_defers_enqueue(self) -> None:
        """For @exactly_once tasks, _process_task should create TaskExecution and defer enqueue."""
        st = self._create_due_task()

        scheduler = PeriodicTaskScheduler(interval=60)

        with self.captureOnCommitCallbacks(execute=True):
            scheduler.tick()

        # TaskExecution should exist
        self.assertEqual(TaskExecution.objects.count(), 1)
        execution = TaskExecution.objects.first()
        self.assertIsNotNone(execution)
        assert execution is not None
        self.assertEqual(execution.scheduled_task_id, st.pk)
        self.assertEqual(execution.status, TaskExecution.Status.PENDING)

        # Task should have been enqueued (via on_commit callback)
        self.assertEqual(len(default_task_backend.results), 1)
        result = default_task_backend.results[0]
        # The enqueued task should have _periodic_tasks_execution_id in kwargs
        self.assertIn("_periodic_tasks_execution_id", result.kwargs)
        self.assertEqual(result.kwargs["_periodic_tasks_execution_id"], str(execution.id))

    def test_exactly_once_updates_tracking(self) -> None:
        """Tracking fields (last_run_at, next_run_at, total_run_count) should still be updated."""
        st = self._create_due_task()

        scheduler = PeriodicTaskScheduler(interval=60)
        with self.captureOnCommitCallbacks(execute=True):
            scheduler.tick()

        st.refresh_from_db()
        self.assertIsNotNone(st.last_run_at)
        self.assertEqual(st.total_run_count, 1)
        self.assertGreater(st.next_run_at, datetime.now(tz=timezone.utc) - timedelta(minutes=1))

    def test_non_exactly_once_task_enqueues_immediately(self) -> None:
        """Regular tasks (without @exactly_once) should still enqueue immediately."""
        ScheduledTask.objects.create(
            name="regular-task",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="* * * * *",
            enabled=True,
            next_run_at=datetime.now(tz=timezone.utc) - timedelta(minutes=1),
        )

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler.tick()

        # Should be enqueued immediately (no on_commit needed)
        self.assertEqual(len(default_task_backend.results), 1)
        # No TaskExecution created
        self.assertEqual(TaskExecution.objects.count(), 0)


@override_settings(TASKS=DUMMY_BACKEND_SETTINGS)
class TestStaleCleanupConcurrency(TransactionTestCase):
    """Verify per-row SELECT FOR UPDATE SKIP LOCKED keeps re-enqueue exactly-once.

    The lock is now scoped to a single row (not held across the whole loop), but
    while one scheduler holds a row, a concurrent scheduler must skip it rather
    than re-enqueue it a second time.
    """

    def setUp(self) -> None:
        default_task_backend.clear()

    def _create_stale_execution(self, st: ScheduledTask) -> TaskExecution:
        execution = TaskExecution.objects.create(scheduled_task=st)
        stale_time = django_tz.now() - timedelta(minutes=10)
        TaskExecution.objects.filter(id=execution.id).update(created_at=stale_time)
        execution.refresh_from_db()
        return execution

    def test_concurrent_stale_cleanup_no_duplicate_enqueue(self) -> None:
        """Two schedulers running stale cleanup concurrently must not re-enqueue the same execution twice."""
        st = ScheduledTask.objects.create(
            name="concurrent-stale-test",
            task_path="sandbox.testapp.tasks.exactly_once_task",
            cron_expression="* * * * *",
            enabled=True,
            next_run_at=datetime.now(tz=timezone.utc) + timedelta(hours=1),
        )
        self._create_stale_execution(st)

        # Track how many times resolve_task is called across threads.
        # While thread A holds the row lock (inside its per-row transaction),
        # thread B's SKIP LOCKED re-query returns nothing, so B never resolves
        # or enqueues. If the row were unlocked early, both would resolve.
        resolve_calls: list[str] = []
        entered = threading.Event()

        from django_periodic_tasks.task_resolver import resolve_task as original_resolve

        def slow_resolve(task_path: str) -> object:
            resolve_calls.append(threading.current_thread().name)
            if len(resolve_calls) == 1:
                entered.set()
                time.sleep(0.5)
            return original_resolve(task_path)

        scheduler_a = PeriodicTaskScheduler(interval=60)
        scheduler_b = PeriodicTaskScheduler(interval=60)

        def cleanup_in_thread() -> None:
            try:
                scheduler_a._cleanup_stale_executions()
            finally:
                connection.close()

        with patch("django_periodic_tasks.scheduler.resolve_task", slow_resolve):
            thread_a = threading.Thread(target=cleanup_in_thread)
            thread_a.start()

            # Wait for thread A to be inside the enqueue loop (past the lock)
            entered.wait()
            # Thread B should skip the locked row (if lock is held correctly)
            scheduler_b._cleanup_stale_executions()

            thread_a.join()

        # With the bug: both threads call resolve_task -> 2
        # With the fix: only thread A calls resolve_task -> 1
        self.assertEqual(len(resolve_calls), 1)

    def test_held_row_does_not_block_sibling_executions(self) -> None:
        """While one scheduler holds a single stale row, a second scheduler must
        still re-enqueue the *other* stale executions of the same task.

        Regression guard for ``select_for_update(of=("self",))``: siblings share
        one parent ScheduledTask, so locking the parent row too would make the
        second scheduler's SKIP LOCKED skip every sibling (0 drained) instead of
        the N-1 it doesn't hold.
        """
        st = ScheduledTask.objects.create(
            name="head-of-line-test",
            task_path="sandbox.testapp.tasks.exactly_once_task",
            cron_expression="* * * * *",
            enabled=True,
            next_run_at=datetime.now(tz=timezone.utc) + timedelta(hours=1),
        )
        total = 4
        for _ in range(total):
            self._create_stale_execution(st)

        from django_periodic_tasks.task_resolver import resolve_task as original_resolve

        entered = threading.Event()
        release = threading.Event()
        parked: list[bool] = []

        def gated_resolve(task_path: str) -> object:
            # Park only the first call (scheduler X), holding that one row's lock.
            if not parked:
                parked.append(True)
                entered.set()
                release.wait(timeout=10)
            return original_resolve(task_path)

        def run_x() -> None:
            try:
                PeriodicTaskScheduler(interval=60)._cleanup_stale_executions()
            finally:
                connection.close()

        with patch("django_periodic_tasks.scheduler.resolve_task", gated_resolve):
            thread_x = threading.Thread(target=run_x)
            thread_x.start()
            self.assertTrue(entered.wait(timeout=10))  # X now holds one row's lock

            # Scheduler Y runs while X is parked; it must drain the other rows.
            PeriodicTaskScheduler(interval=60)._cleanup_stale_executions()
            y_dispatched = TaskExecution.objects.filter(dispatched_at__isnull=False).count()

            release.set()
            thread_x.join()

        self.assertEqual(y_dispatched, total - 1)  # every sibling except the held one
        # Once X finishes, all rows are dispatched exactly once.
        self.assertEqual(TaskExecution.objects.filter(dispatched_at__isnull=True).count(), 0)


@override_settings(TASKS=DUMMY_BACKEND_SETTINGS)
class TestSchedulerStaleCleanup(TestCase):
    """Tests for stale PENDING TaskExecution cleanup."""

    def setUp(self) -> None:
        default_task_backend.clear()

    def _create_due_task(self, **kwargs: object) -> ScheduledTask:
        defaults = {
            "name": "stale-cleanup-test",
            "task_path": "sandbox.testapp.tasks.exactly_once_task",
            "cron_expression": "* * * * *",
            "enabled": True,
            "next_run_at": datetime.now(tz=timezone.utc) + timedelta(hours=1),
        }
        defaults.update(kwargs)  # type: ignore[arg-type]
        return ScheduledTask.objects.create(**defaults)

    def _create_stale_execution(self, st: ScheduledTask) -> TaskExecution:
        """Create a never-dispatched TaskExecution, backdated so its lease has expired."""
        execution = TaskExecution.objects.create(scheduled_task=st)
        # Backdate using .update() to bypass auto_now_add
        stale_time = django_tz.now() - timedelta(minutes=10)
        TaskExecution.objects.filter(id=execution.id).update(created_at=stale_time)
        execution.refresh_from_db()
        return execution

    def test_cleanup_reenqueues_stale_pending(self) -> None:
        """A stale undispatched PENDING execution gets re-enqueued once with correct kwargs."""
        st = self._create_due_task()
        execution = self._create_stale_execution(st)

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler._cleanup_stale_executions()

        self.assertEqual(len(default_task_backend.results), 1)
        result = default_task_backend.results[0]
        self.assertIn("_periodic_tasks_execution_id", result.kwargs)
        self.assertEqual(result.kwargs["_periodic_tasks_execution_id"], str(execution.id))

        execution.refresh_from_db()
        self.assertIsNotNone(execution.dispatched_at)

        scheduler._cleanup_stale_executions()
        self.assertEqual(len(default_task_backend.results), 1)

    def test_cleanup_skips_recently_dispatched(self) -> None:
        """A PENDING execution dispatched within the lease window is not re-dispatched."""
        st = self._create_due_task()
        execution = TaskExecution.objects.create(scheduled_task=st)
        # Old row, but dispatched just now -> lease still valid.
        TaskExecution.objects.filter(id=execution.id).update(
            created_at=django_tz.now() - timedelta(minutes=10),
            dispatched_at=django_tz.now(),
            dispatch_count=1,
        )

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler._cleanup_stale_executions()

        self.assertEqual(len(default_task_backend.results), 0)

    def test_cleanup_redispatches_expired_lease(self) -> None:
        """A PENDING execution whose dispatch lease has expired IS re-dispatched (bug: dropped by broker)."""
        st = self._create_due_task()
        execution = TaskExecution.objects.create(scheduled_task=st)
        stale_time = django_tz.now() - timedelta(minutes=10)
        TaskExecution.objects.filter(id=execution.id).update(
            created_at=stale_time,
            dispatched_at=stale_time,
            dispatch_count=1,
        )

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler._cleanup_stale_executions()

        self.assertEqual(len(default_task_backend.results), 1)
        execution.refresh_from_db()
        self.assertEqual(execution.dispatch_count, 2)
        self.assertGreater(execution.dispatched_at, stale_time)

    def test_cleanup_stops_after_max_attempts(self) -> None:
        """A row that has exhausted PERIODIC_TASKS_MAX_DISPATCH_ATTEMPTS is not re-dispatched."""
        st = self._create_due_task()
        execution = TaskExecution.objects.create(scheduled_task=st)
        stale_time = django_tz.now() - timedelta(minutes=10)
        # Default max attempts is 3; a lease-expired row already at 3 is exhausted.
        TaskExecution.objects.filter(id=execution.id).update(
            created_at=stale_time,
            dispatched_at=stale_time,
            dispatch_count=3,
        )

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler._cleanup_stale_executions()

        self.assertEqual(len(default_task_backend.results), 0)

    def test_cleanup_warns_on_final_attempt(self) -> None:
        """The final re-dispatch logs that no further attempts will be made; earlier ones do not."""
        st = self._create_due_task()
        stale_time = django_tz.now() - timedelta(minutes=10)
        scheduler = PeriodicTaskScheduler(interval=60)
        logger_name = "django_periodic_tasks.scheduler"

        # A never-dispatched row (attempt 1 of 3) re-dispatches without warning.
        early = self._create_stale_execution(st)
        with self.assertNoLogs(logger_name, level="WARNING"):
            scheduler._cleanup_stale_executions()
        early.refresh_from_db()
        self.assertEqual(early.dispatch_count, 1)

        # A row already at max-1 attempts: this re-dispatch is the last, so it warns.
        final = TaskExecution.objects.create(scheduled_task=st)
        TaskExecution.objects.filter(id=final.id).update(
            created_at=stale_time,
            dispatched_at=stale_time,
            dispatch_count=2,
        )
        with self.assertLogs(logger_name, level="WARNING") as cm:
            scheduler._cleanup_stale_executions()
        self.assertTrue(any("final dispatch attempt" in line for line in cm.output))
        final.refresh_from_db()
        self.assertEqual(final.dispatch_count, 3)

    def test_cleanup_skips_recent_pending(self) -> None:
        """A fresh PENDING execution should not be re-enqueued."""
        st = self._create_due_task()
        TaskExecution.objects.create(scheduled_task=st)

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler._cleanup_stale_executions()

        self.assertEqual(len(default_task_backend.results), 0)

    def test_cleanup_skips_completed(self) -> None:
        """A COMPLETED execution should not be re-enqueued."""
        st = self._create_due_task()
        execution = TaskExecution.objects.create(
            scheduled_task=st,
            status=TaskExecution.Status.COMPLETED,
            completed_at=django_tz.now(),
        )
        # Backdate to make it old enough
        stale_time = django_tz.now() - timedelta(minutes=10)
        TaskExecution.objects.filter(id=execution.id).update(created_at=stale_time)

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler._cleanup_stale_executions()

        self.assertEqual(len(default_task_backend.results), 0)

    def test_cleanup_skips_disabled_task(self) -> None:
        """A PENDING execution for a disabled ScheduledTask should not be re-enqueued."""
        st = self._create_due_task(enabled=False)
        self._create_stale_execution(st)

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler._cleanup_stale_executions()

        self.assertEqual(len(default_task_backend.results), 0)

    def test_cleanup_handles_bad_task_path(self) -> None:
        """An unresolvable task path should not crash cleanup."""
        st = self._create_due_task(
            name="bad-path-cleanup",
            task_path="nonexistent.module.task",
        )
        self._create_stale_execution(st)

        scheduler = PeriodicTaskScheduler(interval=60)
        # Should not raise
        scheduler._cleanup_stale_executions()

        self.assertEqual(len(default_task_backend.results), 0)

    def test_cleanup_uses_task_options(self) -> None:
        """Re-enqueue should respect queue_name/priority/backend from ScheduledTask."""
        st = self._create_due_task(
            name="options-cleanup",
            queue_name="special",
            priority=10,
        )
        self._create_stale_execution(st)

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler._cleanup_stale_executions()

        self.assertEqual(len(default_task_backend.results), 1)
        result = default_task_backend.results[0]
        self.assertEqual(result.task.queue_name, "special")
        self.assertEqual(result.task.priority, 10)

    @override_settings(PERIODIC_TASKS_REDISPATCH_AFTER=300)
    def test_cleanup_respects_redispatch_after(self) -> None:
        """The lease is PERIODIC_TASKS_REDISPATCH_AFTER, independent of the tick interval."""
        st = self._create_due_task(name="redispatch-after")
        execution = TaskExecution.objects.create(scheduled_task=st)

        # 299s old, lease window 300s -> not yet eligible.
        TaskExecution.objects.filter(id=execution.id).update(created_at=django_tz.now() - timedelta(seconds=299))
        scheduler = PeriodicTaskScheduler(interval=15)
        scheduler._cleanup_stale_executions()
        self.assertEqual(len(default_task_backend.results), 0)

        # Older than the window -> eligible.
        default_task_backend.clear()
        TaskExecution.objects.filter(id=execution.id).update(created_at=django_tz.now() - timedelta(seconds=301))
        scheduler._cleanup_stale_executions()
        self.assertEqual(len(default_task_backend.results), 1)

    def test_cleanup_and_delete_failures_do_not_block_tick(self) -> None:
        """Exceptions in _cleanup_stale_executions or _delete_old_executions don't block tick."""
        ScheduledTask.objects.create(
            name="tick-after-failures",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="* * * * *",
            enabled=True,
            next_run_at=datetime.now(tz=timezone.utc) - timedelta(minutes=1),
        )

        scheduler = PeriodicTaskScheduler(interval=60)
        with (
            patch.object(
                PeriodicTaskScheduler,
                "_cleanup_stale_executions",
                side_effect=RuntimeError("cleanup boom"),
            ),
            patch.object(
                PeriodicTaskScheduler,
                "_delete_old_executions",
                side_effect=RuntimeError("delete boom"),
            ),
        ):
            scheduler.tick()

        self.assertEqual(len(default_task_backend.results), 1)

    def test_cleanup_includes_task_kwargs(self) -> None:
        """Re-enqueue should merge ScheduledTask.kwargs with the execution_id."""
        st = self._create_due_task(
            name="kwargs-cleanup",
            kwargs={"foo": "bar", "count": 42},
        )
        execution = self._create_stale_execution(st)

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler._cleanup_stale_executions()

        self.assertEqual(len(default_task_backend.results), 1)
        result = default_task_backend.results[0]
        self.assertEqual(result.kwargs["foo"], "bar")
        self.assertEqual(result.kwargs["count"], 42)
        self.assertEqual(result.kwargs["_periodic_tasks_execution_id"], str(execution.id))


@override_settings(TASKS=DUMMY_BACKEND_SETTINGS)
class TestSchedulerDeleteOldExecutions(TestCase):
    """Tests for bulk deletion of old non-PENDING TaskExecution rows."""

    def setUp(self) -> None:
        default_task_backend.clear()

    def _create_task(self) -> ScheduledTask:
        return ScheduledTask.objects.create(
            name="delete-old-test",
            task_path="sandbox.testapp.tasks.exactly_once_task",
            cron_expression="* * * * *",
            enabled=True,
            next_run_at=datetime.now(tz=timezone.utc) + timedelta(hours=1),
        )

    def _create_old_execution(self, st: ScheduledTask, status: str, hours_ago: float = 25) -> TaskExecution:
        execution = TaskExecution.objects.create(scheduled_task=st, status=status)
        old_time = django_tz.now() - timedelta(hours=hours_ago)
        TaskExecution.objects.filter(id=execution.id).update(created_at=old_time)
        execution.refresh_from_db()
        return execution

    def test_delete_old_completed_executions(self) -> None:
        """COMPLETED execution older than 24h is deleted."""
        st = self._create_task()
        self._create_old_execution(st, TaskExecution.Status.COMPLETED)

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler._delete_old_executions()

        self.assertEqual(TaskExecution.objects.count(), 0)

    def test_delete_preserves_pending(self) -> None:
        """PENDING execution older than 24h with attempts remaining is NOT deleted."""
        st = self._create_task()
        self._create_old_execution(st, TaskExecution.Status.PENDING)

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler._delete_old_executions()

        self.assertEqual(TaskExecution.objects.count(), 1)

    def test_delete_preserves_exhausted_pending(self) -> None:
        """PENDING execution with exhausted attempts is NOT deleted."""
        st = self._create_task()
        execution = self._create_old_execution(st, TaskExecution.Status.PENDING)
        old_time = django_tz.now() - timedelta(hours=25)
        TaskExecution.objects.filter(id=execution.id).update(
            dispatch_count=3,  # default max
            dispatched_at=old_time,
        )

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler._delete_old_executions()

        self.assertEqual(TaskExecution.objects.count(), 1)

    def test_delete_preserves_recent(self) -> None:
        """COMPLETED execution younger than 24h is NOT deleted."""
        st = self._create_task()
        self._create_old_execution(st, TaskExecution.Status.COMPLETED, hours_ago=23)

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler._delete_old_executions()

        self.assertEqual(TaskExecution.objects.count(), 1)
