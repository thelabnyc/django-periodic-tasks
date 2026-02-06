from __future__ import annotations

from collections.abc import Callable
import functools
import logging

from django.db import transaction
from django.utils import timezone

logger = logging.getLogger(__name__)

_exactly_once_funcs: set[Callable[..., object]] = set()


def is_exactly_once(func: object) -> bool:
    """Check whether a function was decorated with ``@exactly_once``."""
    return func in _exactly_once_funcs


def exactly_once[R](func: Callable[..., R]) -> Callable[..., R | None]:
    """Decorator ensuring a scheduled task runs at most once per invocation.

    When :meth:`~django_periodic_tasks.models.ScheduledTask.enqueue_now` (used
    by both the scheduler and the Django admin "Run now" action) creates a
    ``TaskExecution`` row and passes its ID via the
    ``_periodic_tasks_execution_id`` keyword argument, this decorator will:

    1. Pop ``_periodic_tasks_execution_id`` from kwargs.
    2. Lock the ``TaskExecution`` row with ``SELECT FOR UPDATE``.
    3. Run the wrapped function only if the row's status is ``PENDING``.
    4. Mark the row ``COMPLETED`` on success.

    If ``_periodic_tasks_execution_id`` is absent (e.g. manual invocation), the
    wrapped function runs normally without any execution-permit logic.

    .. warning::

        This decorator is designed exclusively for tasks managed by
        :class:`~django_periodic_tasks.models.ScheduledTask`.  The
        deduplication guarantee depends on
        :func:`~django_periodic_tasks.enqueue.enqueue_scheduled_task` creating a
        ``TaskExecution`` row and injecting ``_periodic_tasks_execution_id``
        into the task kwargs before enqueue.

        Calling a ``@exactly_once``-decorated task directly via
        ``task.enqueue()`` (bypassing ``ScheduledTask``) will run the function
        normally but **without** any deduplication protection.
    """

    @functools.wraps(func)
    def wrapper(*args: object, **kwargs: object) -> R | None:
        raw_execution_id = kwargs.pop("_periodic_tasks_execution_id", None)

        if raw_execution_id is None:
            return func(*args, **kwargs)

        execution_id = str(raw_execution_id)

        from django_periodic_tasks.models import (
            TaskExecution,  # Avoid AppRegistryNotReady
        )

        with transaction.atomic():
            execution = TaskExecution.objects.select_for_update().filter(id=execution_id, status=TaskExecution.Status.PENDING).first()

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

    _exactly_once_funcs.add(wrapper)
    return wrapper
