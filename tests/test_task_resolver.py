from django.test import SimpleTestCase

from django_periodic_tasks.compat import TASK_CLASSES
from django_periodic_tasks.task_resolver import get_all_task_choices, resolve_task


class TestResolveTask(SimpleTestCase):
    def test_resolve_valid_task(self) -> None:
        task = resolve_task("sandbox.testapp.tasks.example_task")
        self.assertIsInstance(task, TASK_CLASSES)

    def test_resolve_task_with_args(self) -> None:
        task = resolve_task("sandbox.testapp.tasks.example_task_with_args")
        self.assertIsInstance(task, TASK_CLASSES)

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


class TestGetAllTaskChoices(SimpleTestCase):
    def test_discovers_sandbox_tasks(self) -> None:
        # Ensure the sandbox tasks module is imported so tasks are in sys.modules
        import sandbox.testapp.tasks  # noqa: F401

        choices = get_all_task_choices()
        paths = [path for path, _label in choices]
        self.assertIn("sandbox.testapp.tasks.example_task", paths)
        self.assertIn("sandbox.testapp.tasks.example_task_with_args", paths)
        self.assertIn("sandbox.testapp.tasks.exactly_once_task", paths)

    def test_returns_sorted_tuples(self) -> None:
        import sandbox.testapp.tasks  # noqa: F401

        choices = get_all_task_choices()
        # Each choice is a (path, path) tuple
        for path, label in choices:
            self.assertEqual(path, label)
        # Sorted order
        paths = [path for path, _label in choices]
        self.assertEqual(paths, sorted(paths))

    def test_no_duplicates(self) -> None:
        import sandbox.testapp.tasks  # noqa: F401

        choices = get_all_task_choices()
        paths = [path for path, _label in choices]
        self.assertEqual(len(paths), len(set(paths)))
