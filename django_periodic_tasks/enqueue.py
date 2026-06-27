from __future__ import annotations

from typing import TYPE_CHECKING

from django.db import transaction
from django.utils import timezone

from django_periodic_tasks.decorators import is_exactly_once
from django_periodic_tasks.task_resolver import resolve_task

if TYPE_CHECKING:
    from django_periodic_tasks.models import ScheduledTask, TaskExecution
    from django_periodic_tasks.registry import TaskLike


def dispatch_execution(configured: TaskLike, execution: TaskExecution) -> None:
    """Enqueue a configured task for a concrete execution, then mark it dispatched.

    Takes the whole ``execution`` (not a loose ``st``/``execution_id`` pair) so a caller
    can't pass a row id that doesn't belong to the schedule. ``configured`` stays a
    separate arg because callers must build it eagerly — before the row exists — so a bad
    queue/backend raises early.

    Enqueue first, stamp ``dispatched_at`` second, deliberately: if the enqueue raises,
    the row stays ``dispatched_at IS NULL`` so stale cleanup can recover it — a possible
    duplicate is better than a lost execution. The stamp records a *successful enqueue*,
    not a successful run. A duplicate from a lost stamp is suppressed by ``@exactly_once``
    once a run completes; a body that keeps raising stays PENDING, so a duplicate can
    re-run it.
    """
    from django_periodic_tasks.models import TaskExecution

    st = execution.scheduled_task
    configured.enqueue(
        *st.args,
        **{
            **st.kwargs,
            "_periodic_tasks_execution_id": str(execution.id),
        },
    )
    TaskExecution.objects.filter(id=execution.id).update(dispatched_at=timezone.now())


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
    # Built eagerly so a misconfigured queue/backend raises before a row exists.
    configured = task_obj.using(
        queue_name=st.queue_name,
        priority=st.priority,
        backend=st.backend,
    )

    if not is_exactly_once(task_obj.func):
        configured.enqueue(*st.args, **st.kwargs)
        return

    with transaction.atomic():
        execution = TaskExecution.objects.create(scheduled_task=st)
        transaction.on_commit(lambda: dispatch_execution(configured, execution))
