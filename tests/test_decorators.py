from django.test import TestCase
from django.utils import timezone

from django_periodic_tasks.decorators import exactly_once
from django_periodic_tasks.models import ScheduledTask, TaskExecution


class TestExactlyOnceDecorator(TestCase):
    def setUp(self) -> None:
        self.scheduled_task = ScheduledTask.objects.create(
            name="deco-test",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="* * * * *",
        )

    def test_runs_normally_without_execution_id(self) -> None:
        """Without _execution_id, the decorator passes through to the inner function."""
        call_log: list[str] = []

        @exactly_once
        def my_func(x: int) -> int:
            call_log.append("called")
            return x + 1

        result = my_func(5)
        self.assertEqual(result, 6)
        self.assertEqual(call_log, ["called"])

    def test_runs_with_valid_pending_execution(self) -> None:
        """With a valid PENDING execution, the function runs and marks COMPLETED."""
        execution = TaskExecution.objects.create(
            scheduled_task=self.scheduled_task,
        )
        call_log: list[str] = []

        @exactly_once
        def my_func() -> str:
            call_log.append("called")
            return "done"

        result = my_func(_execution_id=str(execution.id))
        self.assertEqual(result, "done")
        self.assertEqual(call_log, ["called"])

        execution.refresh_from_db()
        self.assertEqual(execution.status, TaskExecution.Status.COMPLETED)
        self.assertIsNotNone(execution.completed_at)

    def test_skips_already_completed_execution(self) -> None:
        """If execution is already COMPLETED, the function should be skipped."""
        execution = TaskExecution.objects.create(
            scheduled_task=self.scheduled_task,
            status=TaskExecution.Status.COMPLETED,
            completed_at=timezone.now(),
        )
        call_log: list[str] = []

        @exactly_once
        def my_func() -> str:
            call_log.append("called")
            return "done"

        result = my_func(_execution_id=str(execution.id))
        self.assertIsNone(result)
        self.assertEqual(call_log, [])

    def test_skips_nonexistent_execution(self) -> None:
        """If the execution ID doesn't exist, the function should be skipped."""
        import uuid

        call_log: list[str] = []

        @exactly_once
        def my_func() -> str:
            call_log.append("called")
            return "done"

        result = my_func(_execution_id=str(uuid.uuid4()))
        self.assertIsNone(result)
        self.assertEqual(call_log, [])

    def test_exactly_once_marker_attribute(self) -> None:
        """The decorator should set _exactly_once = True on the wrapper."""

        @exactly_once
        def my_func() -> None:
            pass

        self.assertTrue(getattr(my_func, "_exactly_once", False))

    def test_preserves_function_metadata(self) -> None:
        """The decorator should use functools.wraps to preserve __qualname__ etc."""

        @exactly_once
        def my_func() -> None:
            """My docstring."""
            pass

        self.assertEqual(my_func.__name__, "my_func")
        self.assertEqual(my_func.__doc__, "My docstring.")

    def test_passes_through_args_and_kwargs(self) -> None:
        """The decorator should forward all args/kwargs (except _execution_id) to the inner function."""
        execution = TaskExecution.objects.create(
            scheduled_task=self.scheduled_task,
        )

        @exactly_once
        def my_func(a: int, b: int, c: str = "default") -> str:
            return f"{a}+{b}={c}"

        result = my_func(1, 2, c="three", _execution_id=str(execution.id))
        self.assertEqual(result, "1+2=three")
