from django.test import SimpleTestCase
from django_tasks.base import Task

from django_periodic_tasks.task_resolver import resolve_task


class TestResolveTask(SimpleTestCase):
    def test_resolve_valid_task(self) -> None:
        task = resolve_task("sandbox.testapp.tasks.example_task")
        self.assertIsInstance(task, Task)

    def test_resolve_task_with_args(self) -> None:
        task = resolve_task("sandbox.testapp.tasks.example_task_with_args")
        self.assertIsInstance(task, Task)

    def test_resolve_missing_module(self) -> None:
        with self.assertRaises(ImportError):
            resolve_task("nonexistent.module.task")

    def test_resolve_missing_attribute(self) -> None:
        with self.assertRaises(AttributeError):
            resolve_task("sandbox.testapp.tasks.nonexistent_task")

    def test_resolve_non_task_object(self) -> None:
        with self.assertRaises(TypeError):
            # os.path is a module, not a Task
            resolve_task("os.path")

    def test_resolved_task_is_enqueueable(self) -> None:
        task = resolve_task("sandbox.testapp.tasks.example_task")
        self.assertTrue(hasattr(task, "enqueue"))
        self.assertTrue(hasattr(task, "using"))
