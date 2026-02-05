from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from django_periodic_tasks.cron import validate_cron_expression


@dataclass(frozen=True)
class ScheduleEntry:
    """An immutable record describing a single scheduled task.

    Each entry holds the task object, its cron schedule, and the options that
    will be forwarded to ``task.using()`` / ``task.enqueue()`` at execution time.

    Attributes:
        task: The django-tasks ``Task`` object to enqueue.
        cron_expression: A standard 5-field cron expression (e.g. ``"*/15 * * * *"``).
        name: Unique name for this schedule (used as the DB primary key).
        timezone: IANA timezone name used for cron matching (default ``"UTC"``).
        args: Positional arguments passed to ``task.enqueue()``.
        kwargs: Keyword arguments passed to ``task.enqueue()``.
        queue_name: Task queue name passed to ``task.using()``.
        priority: Task priority passed to ``task.using()``.
        backend: Task backend name passed to ``task.using()``.
    """

    task: Any
    cron_expression: str
    name: str
    timezone: str = "UTC"
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    queue_name: str = "default"
    priority: int = 0
    backend: str = "default"


class ScheduleRegistry:
    """Singleton registry for code-defined schedules."""

    def __init__(self) -> None:
        self._entries: dict[str, ScheduleEntry] = {}

    def register(
        self,
        task: Any,
        *,
        cron: str,
        name: str,
        timezone: str = "UTC",
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        queue_name: str = "default",
        priority: int = 0,
        backend: str = "default",
    ) -> None:
        """Register a task with the given cron schedule.

        Args:
            task: A django-tasks ``Task`` object.
            cron: A 5-field cron expression (e.g. ``"0 */6 * * *"``).
            name: Unique name for this schedule.
            timezone: IANA timezone for cron matching (default ``"UTC"``).
            args: Positional arguments for ``task.enqueue()``.
            kwargs: Keyword arguments for ``task.enqueue()``.
            queue_name: Queue name for ``task.using()``.
            priority: Priority for ``task.using()``.
            backend: Backend name for ``task.using()``.

        Raises:
            ValueError: If the cron expression is invalid or the name is already registered.
        """
        if not validate_cron_expression(cron):
            raise ValueError(f"Invalid cron expression: {cron}")
        if name in self._entries:
            raise ValueError(f"Schedule with name '{name}' is already registered")
        self._entries[name] = ScheduleEntry(
            task=task,
            cron_expression=cron,
            name=name,
            timezone=timezone,
            args=args or [],
            kwargs=kwargs or {},
            queue_name=queue_name,
            priority=priority,
            backend=backend,
        )

    def get_entries(self) -> dict[str, ScheduleEntry]:
        """Return a copy of all registered schedule entries, keyed by name."""
        return dict(self._entries)


schedule_registry = ScheduleRegistry()


def scheduled_task(
    *,
    cron: str,
    name: str | None = None,
    registry: ScheduleRegistry | None = None,
    **kwargs: Any,
) -> Any:
    """Decorator that registers a django-tasks ``Task`` with the schedule registry.

    Apply this decorator **after** ``@task()`` to register the task for periodic
    execution::

        @scheduled_task(cron="*/5 * * * *")
        @task()
        def send_digest(user_id: int) -> None:
            ...

    Args:
        cron: A 5-field cron expression (e.g. ``"0 8 * * 1-5"``).
        name: Unique schedule name. Defaults to the task's ``module_path``.
        registry: An alternate ``ScheduleRegistry`` instance (defaults to the
            global ``schedule_registry``).
        **kwargs: Extra options forwarded to
            :meth:`ScheduleRegistry.register` (``timezone``, ``args``,
            ``kwargs``, ``queue_name``, ``priority``, ``backend``).
    """
    target_registry = registry or schedule_registry

    def decorator(task_obj: Any) -> Any:
        actual_name = name or task_obj.module_path
        target_registry.register(task_obj, cron=cron, name=actual_name, **kwargs)
        return task_obj

    return decorator
