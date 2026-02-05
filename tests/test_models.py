from datetime import datetime, timezone

from django.core.exceptions import ValidationError
from django.db import IntegrityError
from django.test import TestCase

from django_periodic_tasks.models import ScheduledTask, TaskExecution


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

    def test_save_with_update_fields_includes_updated_at(self) -> None:
        """Bug 7: save(update_fields=[...]) must include updated_at so auto_now fires."""
        task = ScheduledTask.objects.create(
            name="update-fields-test",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="* * * * *",
        )
        old_updated_at = task.updated_at
        # Simulate passage of time
        import time

        time.sleep(0.01)
        task.total_run_count = 99
        task.save(update_fields=["total_run_count"])
        task.refresh_from_db()
        self.assertEqual(task.total_run_count, 99)
        self.assertGreater(task.updated_at, old_updated_at)

    def test_save_with_update_fields_includes_next_run_at_when_modified(self) -> None:
        """Bug 13: save(update_fields=[...]) must include next_run_at when save() modifies it."""
        task = ScheduledTask.objects.create(
            name="next-run-update-fields",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="* * * * *",
            enabled=True,
        )
        self.assertIsNotNone(task.next_run_at)

        # Disable the task - save() sets next_run_at=None
        task.enabled = False
        task.save(update_fields=["enabled"])
        task.refresh_from_db()
        self.assertIsNone(task.next_run_at)

    def test_save_with_update_fields_computes_next_run_at_when_none(self) -> None:
        """Bug 13: When next_run_at is None and task is enabled, save should compute it."""
        task = ScheduledTask.objects.create(
            name="compute-next-run",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="* * * * *",
            enabled=False,
        )
        self.assertIsNone(task.next_run_at)

        # Re-enable - save() should compute next_run_at
        task.enabled = True
        task.save(update_fields=["enabled"])
        task.refresh_from_db()
        self.assertIsNotNone(task.next_run_at)


class TestScheduledTaskClean(TestCase):
    def test_clean_valid_task(self) -> None:
        task = ScheduledTask(
            name="valid-task",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="* * * * *",
            timezone="UTC",
        )
        # Should not raise
        task.clean()

    def test_clean_invalid_cron_expression(self) -> None:
        task = ScheduledTask(
            name="bad-cron",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="not valid",
            timezone="UTC",
        )
        with self.assertRaises(ValidationError) as cm:
            task.clean()
        self.assertIn("cron_expression", cm.exception.message_dict)

    def test_clean_invalid_timezone(self) -> None:
        task = ScheduledTask(
            name="bad-tz",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="* * * * *",
            timezone="Not/A/Timezone",
        )
        with self.assertRaises(ValidationError) as cm:
            task.clean()
        self.assertIn("timezone", cm.exception.message_dict)

    def test_clean_invalid_task_path_no_dot(self) -> None:
        task = ScheduledTask(
            name="bad-path",
            task_path="nodot",
            cron_expression="* * * * *",
            timezone="UTC",
        )
        with self.assertRaises(ValidationError) as cm:
            task.clean()
        self.assertIn("task_path", cm.exception.message_dict)

    def test_clean_multiple_errors(self) -> None:
        task = ScheduledTask(
            name="all-bad",
            task_path="nodot",
            cron_expression="not valid",
            timezone="Not/A/Timezone",
        )
        with self.assertRaises(ValidationError) as cm:
            task.clean()
        errors = cm.exception.message_dict
        self.assertIn("cron_expression", errors)
        self.assertIn("timezone", errors)
        self.assertIn("task_path", errors)


class TestTaskExecution(TestCase):
    def setUp(self) -> None:
        self.scheduled_task = ScheduledTask.objects.create(
            name="exec-test",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="* * * * *",
        )

    def test_create_execution(self) -> None:
        execution = TaskExecution.objects.create(
            scheduled_task=self.scheduled_task,
        )
        self.assertEqual(execution.status, TaskExecution.Status.PENDING)
        self.assertIsNotNone(execution.id)
        self.assertIsNotNone(execution.created_at)
        self.assertIsNone(execution.completed_at)

    def test_uuid_primary_key(self) -> None:
        import uuid

        execution = TaskExecution.objects.create(
            scheduled_task=self.scheduled_task,
        )
        self.assertIsInstance(execution.id, uuid.UUID)

    def test_status_choices(self) -> None:
        self.assertEqual(TaskExecution.Status.PENDING, "pending")
        self.assertEqual(TaskExecution.Status.COMPLETED, "completed")
        self.assertEqual(TaskExecution.Status.SKIPPED, "skipped")

    def test_cascade_delete(self) -> None:
        TaskExecution.objects.create(scheduled_task=self.scheduled_task)
        self.assertEqual(TaskExecution.objects.count(), 1)
        self.scheduled_task.delete()
        self.assertEqual(TaskExecution.objects.count(), 0)

    def test_str_representation(self) -> None:
        execution = TaskExecution.objects.create(
            scheduled_task=self.scheduled_task,
        )
        s = str(execution)
        self.assertIn("exec-test", s)
        self.assertIn("pending", s)
