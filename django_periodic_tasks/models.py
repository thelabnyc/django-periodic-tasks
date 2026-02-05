from django.db import models

from django_periodic_tasks.cron import compute_next_run_at


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
    task_path = models.CharField(max_length=200)

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
    next_run_at = models.DateTimeField(null=True, blank=True, db_index=True)
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

    def save(self, *args: object, **kwargs: object) -> None:
        if not self.enabled:
            self.next_run_at = None
        elif self.next_run_at is None:
            self.next_run_at = compute_next_run_at(self.cron_expression, self.timezone)
        super().save(*args, **kwargs)  # type: ignore[arg-type]
