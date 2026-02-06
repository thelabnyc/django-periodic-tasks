from __future__ import annotations

import logging

from django.apps import AppConfig
from django.conf import settings
from django.utils.module_loading import autodiscover_modules

from django_periodic_tasks.conf import SchedulerProtocol, get_scheduler_class

logger = logging.getLogger(__name__)

_scheduler: SchedulerProtocol | None = None


class DjangoPeriodicTasksConfig(AppConfig):
    name = "django_periodic_tasks"
    verbose_name = "Django Periodic Tasks"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self) -> None:
        autodiscover_modules("tasks")

        global _scheduler  # noqa: PLW0603

        if not getattr(settings, "PERIODIC_TASKS_AUTOSTART", False):
            return

        if _scheduler is not None:
            return

        scheduler_class = get_scheduler_class()
        interval: int = getattr(settings, "PERIODIC_TASKS_SCHEDULER_INTERVAL", 15)
        _scheduler = scheduler_class(interval=interval)
        _scheduler.start()
        logger.info("Auto-started periodic task scheduler (interval=%ds)", interval)
