from django.test import TestCase
from django_tasks import task

from django_periodic_tasks.models import ScheduledTask
from django_periodic_tasks.registry import ScheduleRegistry
from django_periodic_tasks.sync import sync_code_schedules


@task()
def task_a() -> None:
    pass


@task()
def task_b() -> None:
    pass


@task()
def task_c() -> None:
    pass


class TestSyncCodeSchedules(TestCase):
    def setUp(self) -> None:
        self.registry = ScheduleRegistry()

    def test_creates_new_records(self) -> None:
        self.registry.register(task_a, cron="0 5 * * *", name="task-a")
        self.registry.register(task_b, cron="*/30 * * * *", name="task-b")

        sync_code_schedules(self.registry)

        self.assertEqual(ScheduledTask.objects.count(), 2)
        st = ScheduledTask.objects.get(name="task-a")
        self.assertEqual(st.task_path, task_a.module_path)
        self.assertEqual(st.cron_expression, "0 5 * * *")
        self.assertEqual(st.source, ScheduledTask.Source.CODE)
        self.assertTrue(st.enabled)
        self.assertIsNotNone(st.next_run_at)

    def test_updates_changed_cron(self) -> None:
        self.registry.register(task_a, cron="0 5 * * *", name="task-a")
        sync_code_schedules(self.registry)

        # Now re-register with different cron
        registry2 = ScheduleRegistry()
        registry2.register(task_a, cron="0 6 * * *", name="task-a")
        sync_code_schedules(registry2)

        st = ScheduledTask.objects.get(name="task-a")
        self.assertEqual(st.cron_expression, "0 6 * * *")

    def test_updates_changed_task_path(self) -> None:
        self.registry.register(task_a, cron="0 5 * * *", name="my-task")
        sync_code_schedules(self.registry)

        registry2 = ScheduleRegistry()
        registry2.register(task_b, cron="0 5 * * *", name="my-task")
        sync_code_schedules(registry2)

        st = ScheduledTask.objects.get(name="my-task")
        self.assertEqual(st.task_path, task_b.module_path)

    def test_disables_removed_code_entries(self) -> None:
        self.registry.register(task_a, cron="0 5 * * *", name="task-a")
        self.registry.register(task_b, cron="*/30 * * * *", name="task-b")
        sync_code_schedules(self.registry)

        # Sync again with only task-a
        registry2 = ScheduleRegistry()
        registry2.register(task_a, cron="0 5 * * *", name="task-a")
        sync_code_schedules(registry2)

        st_a = ScheduledTask.objects.get(name="task-a")
        st_b = ScheduledTask.objects.get(name="task-b")
        self.assertTrue(st_a.enabled)
        self.assertFalse(st_b.enabled)

    def test_ignores_database_source_records(self) -> None:
        # Create a DB-source record
        ScheduledTask.objects.create(
            name="db-task",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 0 * * *",
            source=ScheduledTask.Source.DATABASE,
        )

        self.registry.register(task_a, cron="0 5 * * *", name="task-a")
        sync_code_schedules(self.registry)

        # DB task should still exist and be unchanged
        db_task = ScheduledTask.objects.get(name="db-task")
        self.assertEqual(db_task.source, ScheduledTask.Source.DATABASE)
        self.assertTrue(db_task.enabled)
        self.assertEqual(ScheduledTask.objects.count(), 2)

    def test_idempotent(self) -> None:
        self.registry.register(task_a, cron="0 5 * * *", name="task-a")

        sync_code_schedules(self.registry)
        sync_code_schedules(self.registry)
        sync_code_schedules(self.registry)

        self.assertEqual(ScheduledTask.objects.filter(name="task-a").count(), 1)

    def test_re_enables_previously_disabled_code_entry(self) -> None:
        self.registry.register(task_a, cron="0 5 * * *", name="task-a")
        sync_code_schedules(self.registry)

        # Remove and sync to disable
        empty_registry = ScheduleRegistry()
        sync_code_schedules(empty_registry)
        st = ScheduledTask.objects.get(name="task-a")
        self.assertFalse(st.enabled)

        # Add back and sync to re-enable
        sync_code_schedules(self.registry)
        st.refresh_from_db()
        self.assertTrue(st.enabled)

    def test_syncs_options(self) -> None:
        self.registry.register(
            task_a,
            cron="0 5 * * *",
            name="task-a",
            timezone="America/New_York",
            args=["hello"],
            kwargs={"key": "value"},
            queue_name="special",
            priority=10,
            backend="database",
        )
        sync_code_schedules(self.registry)

        st = ScheduledTask.objects.get(name="task-a")
        self.assertEqual(st.timezone, "America/New_York")
        self.assertEqual(st.args, ["hello"])
        self.assertEqual(st.kwargs, {"key": "value"})
        self.assertEqual(st.queue_name, "special")
        self.assertEqual(st.priority, 10)
        self.assertEqual(st.backend, "database")
