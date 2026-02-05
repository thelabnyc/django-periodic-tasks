import logging
import signal
from argparse import ArgumentParser
from types import FrameType

from django.core.management.base import BaseCommand

from django_periodic_tasks.scheduler import PeriodicTaskScheduler

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """Run the periodic task scheduler as a standalone process.

    This command starts the scheduler loop in the main thread (blocking).
    It syncs code-defined schedules to the database, then repeatedly checks for
    due tasks and enqueues them. Use this when running the scheduler separately
    from the task worker, or when using a non-database task backend.
    """

    help = "Run the periodic task scheduler (without a task worker)"

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--interval",
            type=int,
            default=15,
            help="Interval in seconds between scheduler ticks (default: %(default)s)",
        )

    def handle(self, *, interval: int, verbosity: int, **options: object) -> None:
        self._configure_logging(verbosity)

        scheduler = PeriodicTaskScheduler(interval=interval)
        logger.info("Starting periodic task scheduler (interval=%ds)", interval)

        def shutdown(signum: int, frame: FrameType | None) -> None:
            logger.info("Received %s, stopping scheduler...", signal.strsignal(signum))
            scheduler.stop()

        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)

        # Run in the main thread (blocking)
        scheduler.run()

    def _configure_logging(self, verbosity: int) -> None:
        pkg_logger = logging.getLogger("django_periodic_tasks")
        if verbosity == 0:
            pkg_logger.setLevel(logging.CRITICAL)
        elif verbosity == 1:
            pkg_logger.setLevel(logging.INFO)
        else:
            pkg_logger.setLevel(logging.DEBUG)

        if not pkg_logger.hasHandlers():
            pkg_logger.addHandler(logging.StreamHandler(self.stdout))
