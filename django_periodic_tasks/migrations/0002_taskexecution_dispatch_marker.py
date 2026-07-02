import logging

from django.db import migrations, models
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.migrations.state import StateApps
from django.utils import timezone

logger = logging.getLogger(__name__)


def backfill_dispatched_at(apps: StateApps, schema_editor: BaseDatabaseSchemaEditor) -> None:
    # Treat every pre-existing row as dispatched *at deploy time* (dispatched_at = now,
    # NOT created_at) so the first post-deploy cleanup won't re-enqueue the whole PENDING
    # backlog at once. Backfilling to now() gives each row a fresh redelivery lease; setting
    # dispatch_count=1 matches that interpretation (one dispatch attempt already exists).
    TaskExecution = apps.get_model("django_periodic_tasks", "TaskExecution")
    n = TaskExecution.objects.filter(dispatched_at__isnull=True).update(
        dispatched_at=timezone.now(),
        dispatch_count=1,
    )
    logger.info("Backfilled dispatched_at on %d task execution(s)", n)


class Migration(migrations.Migration):
    dependencies = [
        ("django_periodic_tasks", "0001_initial"),
    ]

    operations = [
        migrations.AddField(
            model_name="taskexecution",
            name="dispatched_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="taskexecution",
            name="dispatch_count",
            field=models.PositiveIntegerField(default=0),
        ),
        # no-op reverse: the dispatched_at column is dropped on reverse anyway.
        migrations.RunPython(backfill_dispatched_at, migrations.RunPython.noop),
        migrations.RemoveIndex(
            model_name="taskexecution",
            name="periodic_pending_exec_idx",
        ),
        migrations.AddIndex(
            model_name="taskexecution",
            index=models.Index(
                condition=models.Q(status="pending"),
                fields=["created_at"],
                name="periodic_redispatch_idx",
            ),
        ),
    ]
