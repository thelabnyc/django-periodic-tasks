from datetime import datetime, timedelta, timezone

from django.test import TestCase, override_settings
from django.utils import timezone as django_tz
from django_tasks import default_task_backend, task

from django_periodic_tasks.decorators import exactly_once
from django_periodic_tasks.models import ScheduledTask, TaskExecution
from django_periodic_tasks.registry import ScheduleRegistry
from django_periodic_tasks.scheduler import PeriodicTaskScheduler
from django_periodic_tasks.sync import sync_code_schedules

DUMMY_BACKEND_SETTINGS = {
    "default": {
        "BACKEND": "django_tasks.backends.dummy.DummyBackend",
        "ENQUEUE_ON_COMMIT": False,
    }
}


@task()
def integration_task() -> str:
    return "done"


@task()
def parameterized_task(name: str, count: int = 1) -> str:
    return f"{name}: {count}"


@task()
@exactly_once
def exactly_once_integration_task() -> str:
    return "exactly-once-done"


@override_settings(TASKS=DUMMY_BACKEND_SETTINGS)
class TestFullFlow(TestCase):
    """Full integration test: register -> sync -> tick -> enqueue."""

    def setUp(self) -> None:
        default_task_backend.clear()

    def test_code_defined_full_flow(self) -> None:
        # 1. Register task in registry
        registry = ScheduleRegistry()
        registry.register(integration_task, cron="* * * * *", name="integration-test")

        # 2. Sync to DB
        sync_code_schedules(registry)

        # 3. Verify DB record created
        st = ScheduledTask.objects.get(name="integration-test")
        self.assertEqual(st.source, ScheduledTask.Source.CODE)
        self.assertTrue(st.enabled)
        self.assertIsNotNone(st.next_run_at)

        # 4. Make it due by backdating next_run_at
        st.next_run_at = datetime.now(tz=timezone.utc) - timedelta(minutes=1)
        st.save(update_fields=["next_run_at"])

        # 5. Run scheduler tick
        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler.tick()

        # 6. Verify task was enqueued
        self.assertEqual(len(default_task_backend.results), 1)

        # 7. Verify tracking updated
        st.refresh_from_db()
        self.assertIsNotNone(st.last_run_at)
        self.assertEqual(st.total_run_count, 1)
        # next_run_at should be in the future
        self.assertGreater(st.next_run_at, datetime.now(tz=timezone.utc))

    def test_db_defined_full_flow(self) -> None:
        # 1. Create DB-defined task directly
        st = ScheduledTask.objects.create(
            name="db-integration-test",
            task_path="tests.test_integration.integration_task",
            cron_expression="*/5 * * * *",
            source=ScheduledTask.Source.DATABASE,
        )

        # 2. Make it due
        st.next_run_at = datetime.now(tz=timezone.utc) - timedelta(minutes=1)
        st.save(update_fields=["next_run_at"])

        # 3. Run scheduler tick
        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler.tick()

        # 4. Verify task was enqueued
        self.assertEqual(len(default_task_backend.results), 1)

    def test_with_args_and_kwargs(self) -> None:
        registry = ScheduleRegistry()
        registry.register(
            parameterized_task,
            cron="* * * * *",
            name="param-task",
            args=["hello"],
            kwargs={"count": 42},
        )
        sync_code_schedules(registry)

        st = ScheduledTask.objects.get(name="param-task")
        st.next_run_at = datetime.now(tz=timezone.utc) - timedelta(minutes=1)
        st.save(update_fields=["next_run_at"])

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler.tick()

        self.assertEqual(len(default_task_backend.results), 1)
        result = default_task_backend.results[0]
        self.assertEqual(result.args, ["hello"])
        self.assertEqual(result.kwargs, {"count": 42})

    def test_disabled_task_not_enqueued(self) -> None:
        registry = ScheduleRegistry()
        registry.register(integration_task, cron="* * * * *", name="disabled-test")
        sync_code_schedules(registry)

        st = ScheduledTask.objects.get(name="disabled-test")
        st.enabled = False
        st.save()

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler.tick()

        self.assertEqual(len(default_task_backend.results), 0)

    def test_sync_then_remove_then_sync(self) -> None:
        """Code task removed from registry should be disabled."""
        registry = ScheduleRegistry()
        registry.register(integration_task, cron="* * * * *", name="removable")
        sync_code_schedules(registry)

        st = ScheduledTask.objects.get(name="removable")
        self.assertTrue(st.enabled)

        # Sync with empty registry
        empty_registry = ScheduleRegistry()
        sync_code_schedules(empty_registry)

        st.refresh_from_db()
        self.assertFalse(st.enabled)

        # Make it due (even though disabled)
        st.next_run_at = datetime.now(tz=timezone.utc) - timedelta(minutes=1)
        st.save(update_fields=["next_run_at"])

        scheduler = PeriodicTaskScheduler(interval=60)
        scheduler.tick()

        # Should not be enqueued because disabled
        self.assertEqual(len(default_task_backend.results), 0)


@override_settings(TASKS=DUMMY_BACKEND_SETTINGS)
class TestExactlyOnceFullFlow(TestCase):
    """End-to-end integration tests for the exactly_once flow."""

    def setUp(self) -> None:
        default_task_backend.clear()

    def test_exactly_once_register_sync_tick_enqueue(self) -> None:
        """Full flow: register -> sync -> tick -> TaskExecution + deferred enqueue."""
        # 1. Register an exactly_once task
        registry = ScheduleRegistry()
        registry.register(
            exactly_once_integration_task,
            cron="* * * * *",
            name="eo-integration",
        )

        # 2. Sync to DB
        sync_code_schedules(registry)

        # 3. Verify DB record
        st = ScheduledTask.objects.get(name="eo-integration")
        self.assertTrue(st.enabled)
        self.assertIsNotNone(st.next_run_at)

        # 4. Make it due
        st.next_run_at = datetime.now(tz=timezone.utc) - timedelta(minutes=1)
        st.save(update_fields=["next_run_at"])

        # 5. Run scheduler tick (with on_commit capture)
        scheduler = PeriodicTaskScheduler(interval=60)
        with self.captureOnCommitCallbacks(execute=True):
            scheduler.tick()

        # 6. Verify TaskExecution was created
        self.assertEqual(TaskExecution.objects.count(), 1)
        execution = TaskExecution.objects.first()
        assert execution is not None
        self.assertEqual(execution.scheduled_task_id, st.pk)
        self.assertEqual(execution.status, TaskExecution.Status.PENDING)

        # 7. Verify task was enqueued with _periodic_tasks_execution_id
        self.assertEqual(len(default_task_backend.results), 1)
        result = default_task_backend.results[0]
        self.assertIn("_periodic_tasks_execution_id", result.kwargs)
        self.assertEqual(result.kwargs["_periodic_tasks_execution_id"], str(execution.id))

        # 8. Verify tracking updated
        st.refresh_from_db()
        self.assertIsNotNone(st.last_run_at)
        self.assertEqual(st.total_run_count, 1)

    def test_exactly_once_decorator_skips_duplicate(self) -> None:
        """Simulate what happens when a worker picks up a task: decorator runs,
        then a duplicate pick-up is skipped."""
        # Create a ScheduledTask and TaskExecution
        st = ScheduledTask.objects.create(
            name="eo-dup-test",
            task_path="tests.test_integration.exactly_once_integration_task",
            cron_expression="* * * * *",
        )
        execution = TaskExecution.objects.create(scheduled_task=st)

        # First call: should run and mark COMPLETED
        result = exactly_once_integration_task.func(_periodic_tasks_execution_id=str(execution.id))
        self.assertEqual(result, "exactly-once-done")

        execution.refresh_from_db()
        self.assertEqual(execution.status, TaskExecution.Status.COMPLETED)
        self.assertIsNotNone(execution.completed_at)

        # Second call with same execution_id: should skip
        result2 = exactly_once_integration_task.func(_periodic_tasks_execution_id=str(execution.id))
        self.assertIsNone(result2)

    def test_stale_execution_redelivery(self) -> None:
        """Stale PENDING execution from a failed on_commit gets re-enqueued on next tick."""
        # 1. Register and sync an exactly_once task
        registry = ScheduleRegistry()
        registry.register(
            exactly_once_integration_task,
            cron="* * * * *",
            name="eo-stale-redeliver",
        )
        sync_code_schedules(registry)

        # 2. Make it due
        st = ScheduledTask.objects.get(name="eo-stale-redeliver")
        st.next_run_at = datetime.now(tz=timezone.utc) - timedelta(minutes=1)
        st.save(update_fields=["next_run_at"])

        # 3. Tick with on_commit NOT executing — simulates on_commit failure
        scheduler = PeriodicTaskScheduler(interval=60)
        with self.captureOnCommitCallbacks(execute=False):
            scheduler.tick()

        # 4. Verify PENDING TaskExecution exists, no task in queue
        self.assertEqual(TaskExecution.objects.count(), 1)
        execution = TaskExecution.objects.first()
        assert execution is not None
        self.assertEqual(execution.status, TaskExecution.Status.PENDING)
        self.assertEqual(len(default_task_backend.results), 0)

        # 5. Backdate execution's created_at to make it stale
        stale_time = django_tz.now() - timedelta(minutes=10)
        TaskExecution.objects.filter(id=execution.id).update(created_at=stale_time)

        # 6. Tick again — cleanup should re-enqueue
        scheduler.tick()

        # 7. Verify task enqueued with correct _periodic_tasks_execution_id
        self.assertEqual(len(default_task_backend.results), 1)
        result = default_task_backend.results[0]
        self.assertIn("_periodic_tasks_execution_id", result.kwargs)
        self.assertEqual(
            result.kwargs["_periodic_tasks_execution_id"], str(execution.id)
        )

    def test_exactly_once_manual_invocation_passthrough(self) -> None:
        """Manual invocation without _periodic_tasks_execution_id should run normally."""
        result = exactly_once_integration_task.func()
        self.assertEqual(result, "exactly-once-done")
