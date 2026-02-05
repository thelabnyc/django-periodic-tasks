from io import StringIO
from unittest.mock import MagicMock, patch

from django.core.management import call_command, get_commands
from django.test import TestCase, override_settings

from django_periodic_tasks.scheduler import PeriodicTaskScheduler

DUMMY_BACKEND_SETTINGS = {
    "default": {
        "BACKEND": "django_tasks.backends.dummy.DummyBackend",
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
