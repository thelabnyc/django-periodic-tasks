from __future__ import annotations

from typing import TYPE_CHECKING

from django.db import transaction

from django_periodic_tasks.decorators import is_exactly_once
from django_periodic_tasks.task_resolver import resolve_task

if TYPE_CHECKING:
    from django_periodic_tasks.models import ScheduledTask


def enqueue_scheduled_task(st: ScheduledTask) -> None:
    """Enqueue a ScheduledTask, respecting ``@exactly_once`` semantics.

    For regular tasks the enqueue happens immediately.  For tasks decorated with
    ``@exactly_once``, a :class:`~django_periodic_tasks.models.TaskExecution`
    row is created inside an atomic block and the actual enqueue is deferred to
    ``transaction.on_commit`` so that workers never see an execution-id that
    hasn't been committed yet.
    """
    from django_periodic_tasks.models import TaskExecution

    task_obj = resolve_task(st.task_path)
    configured = task_obj.using(
        queue_name=st.queue_name,
        priority=st.priority,
        backend=st.backend,
    )

    if is_exactly_once(task_obj.func):
        with transaction.atomic():
            execution = TaskExecution.objects.create(scheduled_task=st)
            enqueue_kwargs = {
                **st.kwargs,
                "_periodic_tasks_execution_id": str(execution.id),
            }

            def _deferred_enqueue() -> None:
                configured.enqueue(*st.args, **enqueue_kwargs)

            transaction.on_commit(_deferred_enqueue)
    else:
        configured.enqueue(*st.args, **st.kwargs)
