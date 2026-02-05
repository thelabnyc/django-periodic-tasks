import logging
from argparse import ArgumentParser
from typing import Any

from django_tasks.backends.database.management.commands.db_worker import Command as DbWorkerCommand

from django_periodic_tasks.scheduler import PeriodicTaskScheduler

logger = logging.getLogger(__name__)


class Command(DbWorkerCommand):
    """Run the django-tasks database worker with the periodic scheduler.

    This is the recommended way to run django-periodic-tasks with the
    ``DatabaseBackend``. It starts the scheduler as a daemon thread alongside
    the standard ``db_worker`` command, so a single process handles both
    task execution and periodic scheduling.
    """

    help = "Run a database background worker with periodic task scheduling"

    def add_arguments(self, parser: ArgumentParser) -> None:
        super().add_arguments(parser)
        from django.conf import settings

        default_interval: int = getattr(settings, "PERIODIC_TASKS_SCHEDULER_INTERVAL", 15)
        parser.add_argument(
            "--scheduler-interval",
            type=int,
            default=default_interval,
            help="Interval in seconds between scheduler ticks (default: %(default)s)",
        )

    def handle(  # type: ignore[override]
        self,
        *,
        scheduler_interval: int,
        **options: Any,
    ) -> None:
        scheduler = PeriodicTaskScheduler(interval=scheduler_interval)
        scheduler.start()
        logger.info("Scheduler daemon thread started (interval=%ds)", scheduler_interval)

        try:
            super().handle(**options)
        finally:
            scheduler.stop()
            scheduler.join(timeout=30)
