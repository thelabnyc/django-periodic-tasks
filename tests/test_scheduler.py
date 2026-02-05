import threading
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from django.db import connection
from django.test import TestCase, TransactionTestCase, override_settings
from django_tasks import default_task_backend

from django_periodic_tasks.models import ScheduledTask, TaskExecution
from django_periodic_tasks.scheduler import PeriodicTaskScheduler


DUMMY_BACKEND_SETTINGS = {
    "default": {
        "BACKEND": "django_tasks.backends.dummy.DummyBackend",
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

    @patch.object(PeriodicTaskScheduler, "tick")
    def test_run_calls_tick(self, mock_tick: object) -> None:
        scheduler = PeriodicTaskScheduler(interval=1)
        # Stop immediately after first tick
        scheduler._stop_event.set()
        scheduler.run()

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

        with self.captureOnCommitCallbacks(execute=True) as callbacks:
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
        # The enqueued task should have _execution_id in kwargs
        self.assertIn("_execution_id", result.kwargs)
        self.assertEqual(result.kwargs["_execution_id"], str(execution.id))

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
