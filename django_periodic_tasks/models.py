from collections.abc import Iterable
from zoneinfo import ZoneInfo
import uuid

from django.core.exceptions import ValidationError
from django.db import models
from django.db.models.base import ModelBase

from django_periodic_tasks.cron import compute_next_run_at, validate_cron_expression
from django_periodic_tasks.task_resolver import get_all_task_choices


class ScheduledTask(models.Model):
    """A persistent record of a periodic task and its cron schedule.

    Each row represents one scheduled task. The scheduler queries this table on
    every tick to find tasks whose ``next_run_at`` has passed, then enqueues
    them via django-tasks.

    Tasks can originate from two sources (see :class:`Source`):

    * **Code-defined** — registered with :func:`~django_periodic_tasks.registry.scheduled_task`
      and synced to the database on scheduler startup.
    * **Database-defined** — created manually through the Django admin.
    """

    class Source(models.TextChoices):
        """Where a scheduled task definition comes from.

        ``CODE`` schedules are managed by the codebase and synced automatically.
        ``DATABASE`` schedules are managed by operators through the Django admin.
        """

        CODE = "code", "Code"
        DATABASE = "database", "Database"

    # Identity
    name = models.CharField(max_length=200, unique=True)
    task_path = models.CharField(max_length=200, choices=get_all_task_choices)

    # Schedule
    cron_expression = models.CharField(max_length=200)
    timezone = models.CharField(max_length=63, default="UTC")

    # Arguments (passed to task.enqueue())
    args = models.JSONField(default=list, blank=True)
    kwargs = models.JSONField(default=dict, blank=True)

    # Source & Status
    source = models.CharField(max_length=20, choices=Source.choices, default=Source.DATABASE)
    enabled = models.BooleanField(default=True)

    # Execution tracking
    last_run_at = models.DateTimeField(null=True, blank=True)
    next_run_at = models.DateTimeField(null=True, blank=True)
    total_run_count = models.PositiveIntegerField(default=0)

    # Task options (passed to task.using())
    queue_name = models.CharField(max_length=32, default="default", blank=True)
    priority = models.IntegerField(default=0)
    backend = models.CharField(max_length=32, default="default", blank=True)

    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        indexes = [
            models.Index(
                fields=["next_run_at"],
                condition=models.Q(enabled=True),
                name="periodic_due_tasks_idx",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.name} ({self.cron_expression})"

    def clean(self) -> None:
        errors: dict[str, str] = {}
        if not validate_cron_expression(self.cron_expression):
            errors["cron_expression"] = f"Invalid cron expression: {self.cron_expression}"
        try:
            ZoneInfo(self.timezone)
        except (KeyError, ValueError):
            errors["timezone"] = f"Invalid timezone: {self.timezone}"
        if "." not in self.task_path:
            errors["task_path"] = "task_path must be a dotted module path (e.g. 'myapp.tasks.my_task')"
        if errors:
            raise ValidationError(errors)

    def save(
        self,
        *,
        force_insert: bool | tuple[ModelBase, ...] = False,
        force_update: bool = False,
        using: str | None = None,
        update_fields: Iterable[str] | None = None,
    ) -> None:
        original_next_run_at = self.next_run_at

        if not self.enabled:
            self.next_run_at = None
        elif self.next_run_at is None:
            self.next_run_at = compute_next_run_at(self.cron_expression, self.timezone)

        effective_update_fields: list[str] | None = None
        if update_fields is not None:
            fields = set(update_fields)
            # Always include updated_at so auto_now fires
            fields.add("updated_at")
            # Include next_run_at if save() modified it
            if self.next_run_at != original_next_run_at:
                fields.add("next_run_at")
            effective_update_fields = list(fields)

        super().save(
            force_insert=force_insert,
            force_update=force_update,
            using=using,
            update_fields=effective_update_fields,
        )

    def enqueue_now(self) -> None:
        """Enqueue this task immediately, respecting ``@exactly_once`` semantics.

        Delegates to :func:`~django_periodic_tasks.enqueue.enqueue_scheduled_task`.
        """
        from django_periodic_tasks.enqueue import enqueue_scheduled_task

        enqueue_scheduled_task(self)


class TaskExecution(models.Model):
    """An execution permit for a single scheduled task invocation.

    Used by the ``@exactly_once`` decorator to ensure a task runs at most once
    per scheduled invocation, even with non-transactional backends (e.g. Redis/RQ).
    """

    class Status(models.TextChoices):
        PENDING = "pending", "Pending"
        COMPLETED = "completed", "Completed"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    scheduled_task = models.ForeignKey(
        ScheduledTask,
        on_delete=models.CASCADE,
        related_name="executions",
    )
    status = models.CharField(
        max_length=20,
        choices=Status.choices,
        default=Status.PENDING,
    )
    created_at = models.DateTimeField(auto_now_add=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(
                fields=["status"],
                condition=models.Q(status="pending"),
                name="periodic_pending_exec_idx",
            ),
        ]

    def __str__(self) -> str:
        return f"{self.scheduled_task.name} [{self.status}]"
