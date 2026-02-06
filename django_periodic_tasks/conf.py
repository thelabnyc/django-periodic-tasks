from __future__ import annotations

from typing import Protocol

from django.conf import settings
from django.utils.module_loading import import_string

_DEFAULT_SCHEDULER_CLASS = "django_periodic_tasks.scheduler.PeriodicTaskScheduler"


class SchedulerProtocol(Protocol):
    """Protocol for periodic task scheduler implementations.

    Custom schedulers must accept ``interval`` as a constructor parameter and
    implement ``start()``, ``run()``, and ``stop()`` methods.
    """

    interval: int

    def __init__(self, interval: int) -> None: ...
    def start(self) -> None: ...
    def run(self) -> None: ...
    def stop(self) -> None: ...


def get_scheduler_class() -> type[SchedulerProtocol]:
    """Resolve the scheduler class from settings.

    Returns the class at ``PERIODIC_TASKS_SCHEDULER_CLASS``, defaulting to
    :class:`~django_periodic_tasks.scheduler.PeriodicTaskScheduler`.
    """
    path: str = getattr(
        settings,
        "PERIODIC_TASKS_SCHEDULER_CLASS",
        _DEFAULT_SCHEDULER_CLASS,
    )
    scheduler_class: type[SchedulerProtocol] = import_string(path)
    return scheduler_class
