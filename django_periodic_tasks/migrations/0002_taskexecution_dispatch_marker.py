import logging

from django.db import migrations, models
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.migrations.state import StateApps
from django.db.models import F

logger = logging.getLogger(__name__)


def backfill_dispatched_at(apps: StateApps, schema_editor: BaseDatabaseSchemaEditor) -> None:
    # Treat every pre-existing row as already dispatched (dispatched_at = created_at) so the
    # first post-deploy cleanup tick won't re-enqueue the whole PENDING backlog at once. Rows
    # genuinely lost before this migration become unrecoverable by cleanup — that's the
    # accepted one-time trade-off for avoiding a re-enqueue storm.
    TaskExecution = apps.get_model("django_periodic_tasks", "TaskExecution")
    n = TaskExecution.objects.filter(dispatched_at__isnull=True).update(dispatched_at=F("created_at"))
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
        # no-op reverse: the dispatched_at column is dropped on reverse anyway.
        migrations.RunPython(backfill_dispatched_at, migrations.RunPython.noop),
        migrations.RemoveIndex(
            model_name="taskexecution",
            name="periodic_pending_exec_idx",
        ),
        migrations.AddIndex(
            model_name="taskexecution",
            index=models.Index(
                condition=models.Q(dispatched_at__isnull=True, status="pending"),
                fields=["created_at"],
                name="periodic_lost_exec_idx",
            ),
        ),
    ]
