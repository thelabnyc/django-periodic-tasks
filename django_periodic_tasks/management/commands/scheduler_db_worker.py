import logging
from argparse import ArgumentParser
from typing import Any

from django_tasks.backends.database.management.commands.db_worker import Command as DbWorkerCommand

from django_periodic_tasks.scheduler import PeriodicTaskScheduler

logger = logging.getLogger(__name__)


class Command(DbWorkerCommand):
    help = "Run a database background worker with periodic task scheduling"

    def add_arguments(self, parser: ArgumentParser) -> None:
        super().add_arguments(parser)
        parser.add_argument(
            "--scheduler-interval",
            type=int,
            default=15,
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
