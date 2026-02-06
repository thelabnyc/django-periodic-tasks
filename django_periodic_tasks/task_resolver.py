from importlib import import_module

from django_tasks.base import Task


def resolve_task(task_path: str) -> Task[..., object]:
    """Import and return a django-tasks Task object from its dotted module path.

    The task_path should be a dotted path like "myapp.tasks.my_task".
    """
    module_path, _, attr_name = task_path.rpartition(".")
    if not module_path:
        raise ImportError(f"Invalid task path: {task_path}")

    module = import_module(module_path)
    obj = getattr(module, attr_name)

    if not isinstance(obj, Task):
        raise TypeError(f"{task_path} is not a django-tasks Task instance (got {type(obj).__name__})")

    return obj
