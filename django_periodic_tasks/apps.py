from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from django.apps import AppConfig

if TYPE_CHECKING:
    from django_periodic_tasks.scheduler import PeriodicTaskScheduler

logger = logging.getLogger(__name__)

_scheduler: PeriodicTaskScheduler | None = None


class DjangoPeriodicTasksConfig(AppConfig):
    name = "django_periodic_tasks"
    verbose_name = "Django Periodic Tasks"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self) -> None:
        from django.utils.module_loading import autodiscover_modules

        autodiscover_modules("tasks")

        global _scheduler  # noqa: PLW0603

        from django.conf import settings

        if not getattr(settings, "PERIODIC_TASKS_AUTOSTART", False):
            return

        if _scheduler is not None:
            return

        from django_periodic_tasks.scheduler import PeriodicTaskScheduler

        interval: int = getattr(settings, "PERIODIC_TASKS_SCHEDULER_INTERVAL", 15)
        _scheduler = PeriodicTaskScheduler(interval=interval)
        _scheduler.start()
        logger.info("Auto-started periodic task scheduler (interval=%ds)", interval)
