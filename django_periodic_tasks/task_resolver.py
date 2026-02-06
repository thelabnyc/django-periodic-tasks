from importlib import import_module

from django_periodic_tasks.compat import TASK_CLASSES
from django_periodic_tasks.registry import TaskLike


def resolve_task(task_path: str) -> TaskLike:
    """Import and return a django-tasks Task object from its dotted module path.

    The task_path should be a dotted path like "myapp.tasks.my_task".
    """
    module_path, _, attr_name = task_path.rpartition(".")
    if not module_path:
        raise ImportError(f"Invalid task path: {task_path}")

    module = import_module(module_path)
    obj = getattr(module, attr_name)

    if not isinstance(obj, TASK_CLASSES):
        raise TypeError(f"{task_path} is not a django-tasks Task instance (got {type(obj).__name__})")

    return obj  # type: ignore[return-value]
