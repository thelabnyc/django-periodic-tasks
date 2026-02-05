from __future__ import annotations

import functools
import logging
from typing import Any, Callable

logger = logging.getLogger(__name__)


def exactly_once(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator ensuring a scheduled task runs at most once per invocation.

    When the scheduler creates a ``TaskExecution`` row and passes its ID via
    the ``_periodic_tasks_execution_id`` keyword argument, this decorator will:

    1. Pop ``_periodic_tasks_execution_id`` from kwargs.
    2. Lock the ``TaskExecution`` row with ``SELECT FOR UPDATE``.
    3. Run the wrapped function only if the row's status is ``PENDING``.
    4. Mark the row ``COMPLETED`` on success.

    If ``_periodic_tasks_execution_id`` is absent (e.g. manual invocation), the wrapped
    function runs normally without any execution-permit logic.
    """

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        execution_id: str | None = kwargs.pop("_periodic_tasks_execution_id", None)

        if execution_id is None:
            return func(*args, **kwargs)

        from django.db import transaction
        from django.utils import timezone

        from django_periodic_tasks.models import TaskExecution

        with transaction.atomic():
            execution = (
                TaskExecution.objects.select_for_update()
                .filter(id=execution_id, status=TaskExecution.Status.PENDING)
                .first()
            )

            if execution is None:
                logger.warning(
                    "TaskExecution %s not found or not PENDING, skipping",
                    execution_id,
                )
                return None

            result = func(*args, **kwargs)

            execution.status = TaskExecution.Status.COMPLETED
            execution.completed_at = timezone.now()
            execution.save(update_fields=["status", "completed_at"])

        return result

    wrapper._exactly_once = True  # type: ignore[attr-defined]
    return wrapper
