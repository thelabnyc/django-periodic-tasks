from django.test import SimpleTestCase
from django_tasks import task

from django_periodic_tasks.registry import ScheduleEntry, ScheduleRegistry, scheduled_task


@task()
def dummy_task() -> None:
    pass


@task()
def another_task() -> None:
    pass


@task()
def options_task() -> None:
    pass


class TestScheduleRegistry(SimpleTestCase):
    def setUp(self) -> None:
        self.registry = ScheduleRegistry()

    def test_register_task(self) -> None:
        self.registry.register(dummy_task, cron="0 5 * * *", name="test-task")
        entries = self.registry.get_entries()
        self.assertIn("test-task", entries)
        self.assertEqual(entries["test-task"].cron_expression, "0 5 * * *")
        self.assertEqual(entries["test-task"].task, dummy_task)

    def test_register_with_options(self) -> None:
        self.registry.register(
            dummy_task,
            cron="*/30 * * * *",
            name="with-options",
            timezone="America/New_York",
            args=["hello"],
            kwargs={"key": "value"},
            queue_name="special",
            priority=10,
            backend="database",
        )
        entry = self.registry.get_entries()["with-options"]
        self.assertEqual(entry.timezone, "America/New_York")
        self.assertEqual(entry.args, ["hello"])
        self.assertEqual(entry.kwargs, {"key": "value"})
        self.assertEqual(entry.queue_name, "special")
        self.assertEqual(entry.priority, 10)
        self.assertEqual(entry.backend, "database")

    def test_register_duplicate_name_raises(self) -> None:
        self.registry.register(dummy_task, cron="* * * * *", name="dup")
        with self.assertRaises(ValueError):
            self.registry.register(another_task, cron="0 0 * * *", name="dup")

    def test_get_entries_returns_copy(self) -> None:
        self.registry.register(dummy_task, cron="* * * * *", name="test")
        entries1 = self.registry.get_entries()
        entries2 = self.registry.get_entries()
        self.assertEqual(entries1, entries2)
        self.assertIsNot(entries1, entries2)

    def test_default_values(self) -> None:
        self.registry.register(dummy_task, cron="* * * * *", name="defaults")
        entry = self.registry.get_entries()["defaults"]
        self.assertEqual(entry.timezone, "UTC")
        self.assertEqual(entry.args, [])
        self.assertEqual(entry.kwargs, {})
        self.assertEqual(entry.queue_name, "default")
        self.assertEqual(entry.priority, 0)
        self.assertEqual(entry.backend, "default")

    def test_invalid_cron_raises(self) -> None:
        with self.assertRaises(ValueError):
            self.registry.register(dummy_task, cron="invalid", name="bad-cron")


class TestScheduledTaskDecorator(SimpleTestCase):
    def setUp(self) -> None:
        self.registry = ScheduleRegistry()

    def test_decorator_registers_task(self) -> None:
        scheduled_task(cron="0 5 * * *", name="decorated-task", registry=self.registry)(dummy_task)
        entries = self.registry.get_entries()
        self.assertIn("decorated-task", entries)
        self.assertEqual(entries["decorated-task"].task, dummy_task)

    def test_decorator_passes_through(self) -> None:
        result = scheduled_task(cron="* * * * *", name="passthrough", registry=self.registry)(dummy_task)
        # The decorator should return the original task object
        self.assertIs(result, dummy_task)
        self.assertTrue(hasattr(result, "enqueue"))

    def test_decorator_auto_name(self) -> None:
        scheduled_task(cron="* * * * *", registry=self.registry)(another_task)
        entries = self.registry.get_entries()
        self.assertIn(another_task.module_path, entries)

    def test_decorator_with_options(self) -> None:
        scheduled_task(
            cron="*/10 * * * *",
            name="options-task",
            timezone="Europe/London",
            queue_name="high",
            registry=self.registry,
        )(options_task)

        entry = self.registry.get_entries()["options-task"]
        self.assertEqual(entry.timezone, "Europe/London")
        self.assertEqual(entry.queue_name, "high")


class TestScheduleEntry(SimpleTestCase):
    def test_frozen(self) -> None:
        entry = ScheduleEntry(
            task=dummy_task,
            cron_expression="* * * * *",
            name="test",
        )
        with self.assertRaises(AttributeError):
            entry.name = "changed"  # type: ignore[misc]
