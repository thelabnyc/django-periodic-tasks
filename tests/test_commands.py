from io import StringIO
from unittest.mock import MagicMock, patch

from django.core.management import call_command, get_commands
from django.test import TestCase, override_settings

from django_periodic_tasks.compat import DUMMY_BACKEND_PATH
from django_periodic_tasks.scheduler import PeriodicTaskScheduler

DUMMY_BACKEND_SETTINGS = {
    "default": {
        "BACKEND": DUMMY_BACKEND_PATH,
        "ENQUEUE_ON_COMMIT": False,
    }
}


@override_settings(TASKS=DUMMY_BACKEND_SETTINGS)
class TestRunSchedulerCommand(TestCase):
    def test_command_registered(self) -> None:
        commands = get_commands()
        self.assertIn("run_scheduler", commands)

    @override_settings(PERIODIC_TASKS_SCHEDULER_INTERVAL=42)
    @patch.object(PeriodicTaskScheduler, "run")
    @patch.object(PeriodicTaskScheduler, "__init__", return_value=None)
    def test_command_passes_correct_interval(self, mock_init: MagicMock, mock_run: object) -> None:
        """Command should pass --interval (or settings default) to the scheduler."""
        # With explicit --interval flag
        call_command("run_scheduler", "--interval", "30", stdout=StringIO())
        mock_init.assert_called_once_with(interval=30)

        mock_init.reset_mock()

        # Without --interval, should use PERIODIC_TASKS_SCHEDULER_INTERVAL setting
        call_command("run_scheduler", stdout=StringIO())
        mock_init.assert_called_once_with(interval=42)

    @override_settings(
        PERIODIC_TASKS_SCHEDULER_CLASS="django_periodic_tasks.scheduler.PeriodicTaskScheduler",
    )
    @patch.object(PeriodicTaskScheduler, "run")
    @patch.object(PeriodicTaskScheduler, "__init__", return_value=None)
    def test_command_respects_custom_scheduler_class(self, mock_init: MagicMock, mock_run: object) -> None:
        """Command should use the class from PERIODIC_TASKS_SCHEDULER_CLASS."""
        call_command("run_scheduler", stdout=StringIO())
        mock_init.assert_called_once_with(interval=15)
        mock_run.assert_called_once()  # type: ignore[union-attr]

    @override_settings(
        PERIODIC_TASKS_SCHEDULER_CLASS="nonexistent.module.Scheduler",
    )
    def test_command_invalid_scheduler_class_raises(self) -> None:
        """An invalid PERIODIC_TASKS_SCHEDULER_CLASS path raises ImportError."""
        with self.assertRaises(ImportError):
            call_command("run_scheduler", stdout=StringIO())
