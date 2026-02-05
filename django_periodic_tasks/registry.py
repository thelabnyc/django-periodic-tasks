from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from django_periodic_tasks.cron import validate_cron_expression


@dataclass(frozen=True)
class ScheduleEntry:
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
        return dict(self._entries)


schedule_registry = ScheduleRegistry()


def scheduled_task(
    *,
    cron: str,
    name: str | None = None,
    registry: ScheduleRegistry | None = None,
    **kwargs: Any,
) -> Any:
    """Decorator that registers a Task with the schedule registry."""
    target_registry = registry or schedule_registry

    def decorator(task_obj: Any) -> Any:
        actual_name = name or task_obj.module_path
        target_registry.register(task_obj, cron=cron, name=actual_name, **kwargs)
        return task_obj

    return decorator
