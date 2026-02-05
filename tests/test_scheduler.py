from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from django.test import TestCase, override_settings
from django_tasks import default_task_backend

from django_periodic_tasks.models import ScheduledTask
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

    @patch.object(PeriodicTaskScheduler, "tick")
    def test_run_calls_tick(self, mock_tick: object) -> None:
        scheduler = PeriodicTaskScheduler(interval=1)
        # Stop immediately after first tick
        scheduler._stop_event.set()
        scheduler.run()
