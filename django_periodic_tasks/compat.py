"""Compatibility shim for django-tasks (third-party) and django.tasks (Django 6.0+).

Prefers the third-party ``django-tasks`` package when available (better generic
type safety), falling back to Django's native ``django.tasks`` module.

For static analysis (mypy), ``django-tasks`` is always available as a dev
dependency, so the TYPE_CHECKING block provides authoritative types.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # mypy always sees the third-party django-tasks (dev dependency)
    from django_tasks import default_task_backend as default_task_backend
    from django_tasks import task as task

# ---------------------------------------------------------------------------
# Detect which Task implementations are available
# ---------------------------------------------------------------------------

_ThirdPartyTask: type | None = None
_NativeTask: type | None = None

try:
    from django_tasks.base import Task as _TP  # 0.9.0+

    _ThirdPartyTask = _TP
except ImportError:
    try:
        from django_tasks.task import Task as _TP  # type:ignore # 0.7.0-0.8.x

        _ThirdPartyTask = _TP
    except ImportError:
        pass

try:
    from django.tasks.base import Task as _NT

    _NativeTask = _NT
except ImportError:
    pass

if _ThirdPartyTask is None and _NativeTask is None:
    raise ImportError(
        "django-periodic-tasks requires either 'django-tasks' (pip install django-tasks) or Django 6.0+ (which ships django.tasks natively). Neither was found."
    )

# ---------------------------------------------------------------------------
# TASK_CLASSES — tuple of concrete Task types for isinstance() checks
# ---------------------------------------------------------------------------

TASK_CLASSES: tuple[type, ...] = tuple(cls for cls in (_ThirdPartyTask, _NativeTask) if cls is not None)

# ---------------------------------------------------------------------------
# task decorator & default_task_backend — prefer third-party for generic type
# safety, fall back to native django.tasks.
# ---------------------------------------------------------------------------

if not TYPE_CHECKING:
    if _ThirdPartyTask is not None:
        from django_tasks import default_task_backend, task

        DUMMY_BACKEND_PATH: str = "django_tasks.backends.dummy.DummyBackend"
    else:
        from django.tasks import (  # type: ignore[no-redef,import-untyped,unused-ignore]
            default_task_backend,
            task,
        )

        DUMMY_BACKEND_PATH: str = "django.tasks.backends.dummy.DummyBackend"  # type: ignore[no-redef]
else:
    DUMMY_BACKEND_PATH: str = "django_tasks.backends.dummy.DummyBackend"

__all__ = [
    "DUMMY_BACKEND_PATH",
    "TASK_CLASSES",
    "default_task_backend",
    "task",
]
