from datetime import datetime, timedelta, timezone

from django.test import TestCase, override_settings

from django_periodic_tasks.compat import DUMMY_BACKEND_PATH, default_task_backend
from django_periodic_tasks.enqueue import enqueue_scheduled_task
from django_periodic_tasks.models import ScheduledTask, TaskExecution

DUMMY_BACKEND_SETTINGS = {
    "default": {
        "BACKEND": DUMMY_BACKEND_PATH,
        "ENQUEUE_ON_COMMIT": False,
        "QUEUES": ["default", "special"],
    }
}


@override_settings(TASKS=DUMMY_BACKEND_SETTINGS)
class TestEnqueueScheduledTask(TestCase):
    def setUp(self) -> None:
        default_task_backend.clear()

    def _create_task(self, **kwargs: object) -> ScheduledTask:
        defaults = {
            "name": "enqueue-test",
            "task_path": "sandbox.testapp.tasks.example_task",
            "cron_expression": "* * * * *",
            "enabled": True,
            "next_run_at": datetime.now(tz=timezone.utc) + timedelta(hours=1),
        }
        defaults.update(kwargs)
        return ScheduledTask.objects.create(**defaults)

    def test_regular_task_enqueues_immediately(self) -> None:
        st = self._create_task()
        enqueue_scheduled_task(st)

        self.assertEqual(len(default_task_backend.results), 1)
        self.assertEqual(TaskExecution.objects.count(), 0)

    def test_exactly_once_creates_execution_and_defers(self) -> None:
        st = self._create_task(
            name="eo-test",
            task_path="sandbox.testapp.tasks.exactly_once_task",
        )

        with self.captureOnCommitCallbacks(execute=True):
            enqueue_scheduled_task(st)

        self.assertEqual(TaskExecution.objects.count(), 1)
        execution = TaskExecution.objects.first()
        assert execution is not None
        self.assertEqual(execution.scheduled_task_id, st.pk)
        self.assertEqual(execution.status, TaskExecution.Status.PENDING)

        self.assertEqual(len(default_task_backend.results), 1)
        result = default_task_backend.results[0]
        self.assertIn("_periodic_tasks_execution_id", result.kwargs)
        self.assertEqual(result.kwargs["_periodic_tasks_execution_id"], str(execution.id))

    def test_unresolvable_task_raises(self) -> None:
        st = self._create_task(
            name="bad-path",
            task_path="nonexistent.module.task",
        )
        with self.assertRaises(ImportError):
            enqueue_scheduled_task(st)

    def test_passes_queue_name_priority_backend(self) -> None:
        st = self._create_task(
            name="options-test",
            queue_name="special",
            priority=10,
        )
        enqueue_scheduled_task(st)

        self.assertEqual(len(default_task_backend.results), 1)
        result = default_task_backend.results[0]
        self.assertEqual(result.task.queue_name, "special")
        self.assertEqual(result.task.priority, 10)

    def test_passes_args_and_kwargs(self) -> None:
        st = self._create_task(
            name="args-test",
            task_path="sandbox.testapp.tasks.example_task_with_args",
            args=["hello"],
            kwargs={"count": 5},
        )
        enqueue_scheduled_task(st)

        self.assertEqual(len(default_task_backend.results), 1)
        result = default_task_backend.results[0]
        self.assertEqual(result.args, ["hello"])
        self.assertEqual(result.kwargs, {"count": 5})

    def test_exactly_once_merges_kwargs(self) -> None:
        st = self._create_task(
            name="eo-kwargs",
            task_path="sandbox.testapp.tasks.exactly_once_task",
            kwargs={"foo": "bar"},
        )

        with self.captureOnCommitCallbacks(execute=True):
            enqueue_scheduled_task(st)

        self.assertEqual(len(default_task_backend.results), 1)
        result = default_task_backend.results[0]
        self.assertEqual(result.kwargs["foo"], "bar")
        self.assertIn("_periodic_tasks_execution_id", result.kwargs)

    def test_enqueue_now_delegates(self) -> None:
        """ScheduledTask.enqueue_now() delegates to enqueue_scheduled_task."""
        st = self._create_task(name="method-test")
        st.enqueue_now()

        self.assertEqual(len(default_task_backend.results), 1)

    def test_enqueue_now_exactly_once(self) -> None:
        """ScheduledTask.enqueue_now() respects @exactly_once."""
        st = self._create_task(
            name="method-eo",
            task_path="sandbox.testapp.tasks.exactly_once_task",
        )

        with self.captureOnCommitCallbacks(execute=True):
            st.enqueue_now()

        self.assertEqual(TaskExecution.objects.count(), 1)
        self.assertEqual(len(default_task_backend.results), 1)
