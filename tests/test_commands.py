from io import StringIO
from unittest.mock import patch

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

    @patch.object(PeriodicTaskScheduler, "run")
    def test_command_runs_scheduler(self, mock_run: object) -> None:
        out = StringIO()
        call_command("run_scheduler", "--interval", "30", stdout=out)

    @patch.object(PeriodicTaskScheduler, "run")
    def test_default_interval(self, mock_run: object) -> None:
        out = StringIO()
        call_command("run_scheduler", stdout=out)


@override_settings(TASKS=DUMMY_BACKEND_SETTINGS)
class TestSchedulerDbWorkerCommand(TestCase):
    def test_command_registered(self) -> None:
        commands = get_commands()
        self.assertIn("scheduler_db_worker", commands)
