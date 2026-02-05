import logging

from django_periodic_tasks.models import ScheduledTask
from django_periodic_tasks.registry import ScheduleRegistry, schedule_registry

logger = logging.getLogger(__name__)


def sync_code_schedules(registry: ScheduleRegistry | None = None) -> None:
    """Sync in-memory registry to ScheduledTask DB records.

    - Creates new ScheduledTask for new code entries
    - Updates cron_expression/task_path/options if changed
    - Disables stale code entries (in registry previously, now removed)
    - Never touches source=DATABASE records
    """
    target_registry = registry or schedule_registry
    entries = target_registry.get_entries()

    # Create or update code entries
    seen_names: set[str] = set()
    for name, entry in entries.items():
        seen_names.add(name)
        defaults = {
            "task_path": entry.task.module_path,
            "cron_expression": entry.cron_expression,
            "timezone": entry.timezone,
            "args": entry.args,
            "kwargs": entry.kwargs,
            "queue_name": entry.queue_name,
            "priority": entry.priority,
            "backend": entry.backend,
            "source": ScheduledTask.Source.CODE,
            "enabled": True,
        }
        obj, created = ScheduledTask.objects.update_or_create(
            name=name,
            defaults=defaults,
        )
        if created:
            logger.info("Created code schedule: %s", name)
        else:
            logger.debug("Updated code schedule: %s", name)

    # Disable code-defined entries that are no longer in the registry
    stale = ScheduledTask.objects.filter(
        source=ScheduledTask.Source.CODE,
        enabled=True,
    ).exclude(name__in=seen_names)

    stale_count = stale.update(enabled=False, next_run_at=None)
    if stale_count:
        logger.info("Disabled %d stale code schedule(s)", stale_count)
