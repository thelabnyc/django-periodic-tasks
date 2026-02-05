from typing import Any


class ScheduleRegistry:
    """Singleton registry for code-defined schedules."""

    def __init__(self) -> None:
        self._entries: dict[str, Any] = {}

    def register(self, task: Any, *, cron: str, name: str, **kwargs: Any) -> None:
        raise NotImplementedError

    def get_entries(self) -> dict[str, Any]:
        return dict(self._entries)


schedule_registry = ScheduleRegistry()


def scheduled_task(*, cron: str, name: str | None = None, **kwargs: Any) -> Any:
    """Decorator that registers a Task with the schedule registry."""

    def decorator(task_obj: Any) -> Any:
        return task_obj

    return decorator
