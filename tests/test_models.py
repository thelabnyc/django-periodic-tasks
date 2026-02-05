from datetime import datetime, timezone

from django.db import IntegrityError
from django.test import TestCase

from django_periodic_tasks.models import ScheduledTask


class TestScheduledTaskCreation(TestCase):
    def test_create_basic(self) -> None:
        task = ScheduledTask.objects.create(
            name="test-task",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="* * * * *",
        )
        self.assertEqual(task.name, "test-task")
        self.assertEqual(task.task_path, "sandbox.testapp.tasks.example_task")
        self.assertEqual(task.cron_expression, "* * * * *")
        self.assertTrue(task.enabled)
        self.assertEqual(task.source, ScheduledTask.Source.DATABASE)

    def test_default_values(self) -> None:
        task = ScheduledTask.objects.create(
            name="test-defaults",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
        )
        self.assertEqual(task.timezone, "UTC")
        self.assertEqual(task.args, [])
        self.assertEqual(task.kwargs, {})
        self.assertEqual(task.queue_name, "default")
        self.assertEqual(task.priority, 0)
        self.assertEqual(task.backend, "default")
        self.assertEqual(task.total_run_count, 0)
        self.assertIsNone(task.last_run_at)

    def test_unique_name_constraint(self) -> None:
        ScheduledTask.objects.create(
            name="unique-name",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="* * * * *",
        )
        with self.assertRaises(IntegrityError):
            ScheduledTask.objects.create(
                name="unique-name",
                task_path="sandbox.testapp.tasks.example_task",
                cron_expression="0 5 * * *",
            )

    def test_json_field_args(self) -> None:
        task = ScheduledTask.objects.create(
            name="with-args",
            task_path="sandbox.testapp.tasks.example_task_with_args",
            cron_expression="* * * * *",
            args=["hello", 42],
            kwargs={"key": "value"},
        )
        task.refresh_from_db()
        self.assertEqual(task.args, ["hello", 42])
        self.assertEqual(task.kwargs, {"key": "value"})

    def test_code_source(self) -> None:
        task = ScheduledTask.objects.create(
            name="code-task",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
            source=ScheduledTask.Source.CODE,
        )
        self.assertEqual(task.source, ScheduledTask.Source.CODE)

    def test_str_representation(self) -> None:
        task = ScheduledTask.objects.create(
            name="my-task",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
        )
        self.assertEqual(str(task), "my-task (0 5 * * *)")

    def test_next_run_at_computed_on_save(self) -> None:
        task = ScheduledTask(
            name="auto-next-run",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
            enabled=True,
        )
        self.assertIsNone(task.next_run_at)
        task.save()
        self.assertIsNotNone(task.next_run_at)

    def test_next_run_at_not_computed_when_disabled(self) -> None:
        task = ScheduledTask.objects.create(
            name="disabled-task",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
            enabled=False,
        )
        self.assertIsNone(task.next_run_at)

    def test_next_run_at_cleared_when_disabled(self) -> None:
        task = ScheduledTask.objects.create(
            name="to-disable",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
            enabled=True,
        )
        self.assertIsNotNone(task.next_run_at)
        task.enabled = False
        task.save()
        self.assertIsNone(task.next_run_at)

    def test_timestamps(self) -> None:
        task = ScheduledTask.objects.create(
            name="timestamps",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="* * * * *",
        )
        self.assertIsNotNone(task.created_at)
        self.assertIsNotNone(task.updated_at)

    def test_last_run_tracking(self) -> None:
        task = ScheduledTask.objects.create(
            name="tracking",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="* * * * *",
        )
        now = datetime.now(tz=timezone.utc)
        task.last_run_at = now
        task.total_run_count = 5
        task.save()
        task.refresh_from_db()
        self.assertEqual(task.total_run_count, 5)
        self.assertIsNotNone(task.last_run_at)
