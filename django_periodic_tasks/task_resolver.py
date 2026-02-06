from importlib import import_module
import sys

from django_periodic_tasks.compat import TASK_CLASSES
from django_periodic_tasks.registry import TaskLike


def get_all_task_choices() -> list[tuple[str, str]]:
    """Scan sys.modules for Task instances and return as Django choices.

    Returns a sorted list of (task_path, task_path) tuples suitable for use
    as the ``choices`` argument on a model field.
    """
    seen: set[str] = set()
    for module in list(sys.modules.values()):
        try:
            attrs = vars(module)
        except Exception:
            continue
        for attr_name, obj in attrs.items():
            if attr_name.startswith("_"):
                continue
            if not isinstance(obj, TASK_CLASSES):
                continue
            module_name = getattr(module, "__name__", None)
            if module_name is None:
                continue
            path = f"{module_name}.{attr_name}"
            seen.add(path)
    return sorted((path, path) for path in seen)


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
