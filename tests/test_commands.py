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

    @patch.object(PeriodicTaskScheduler, "run")
    def test_command_runs_scheduler(self, mock_run: object) -> None:
        out = StringIO()
        call_command("run_scheduler", "--interval", "30", stdout=out)

    @patch.object(PeriodicTaskScheduler, "run")
    def test_default_interval(self, mock_run: object) -> None:
        out = StringIO()
        call_command("run_scheduler", stdout=out)

    @override_settings(PERIODIC_TASKS_SCHEDULER_INTERVAL=42)
    @patch.object(PeriodicTaskScheduler, "run")
    @patch.object(PeriodicTaskScheduler, "__init__", return_value=None)
    def test_default_interval_from_settings(self, mock_init: MagicMock, mock_run: object) -> None:
        """Bug 11: Default --interval should come from PERIODIC_TASKS_SCHEDULER_INTERVAL."""
        out = StringIO()
        call_command("run_scheduler", stdout=out)
        mock_init.assert_called_once_with(interval=42)


@override_settings(TASKS=DUMMY_BACKEND_SETTINGS)
class TestSchedulerDbWorkerCommand(TestCase):
    def test_command_registered(self) -> None:
        commands = get_commands()
        self.assertIn("scheduler_db_worker", commands)

    @override_settings(
        TASKS={
            "default": {
                "BACKEND": "django_tasks.backends.database.DatabaseBackend",
            }
        }
    )
    @patch.object(PeriodicTaskScheduler, "start")
    @patch.object(PeriodicTaskScheduler, "stop")
    @patch.object(PeriodicTaskScheduler, "join")
    @patch(
        "django_tasks.backends.database.management.commands.db_worker.Command.handle"
    )
    def test_stop_joins_scheduler_thread(
        self,
        mock_db_handle: object,
        mock_join: MagicMock,
        mock_stop: MagicMock,
        mock_start: object,
    ) -> None:
        """Bug 8: scheduler_db_worker should join() after stop()."""
        out = StringIO()
        call_command("scheduler_db_worker", stdout=out)
        mock_stop.assert_called_once()
        mock_join.assert_called_once_with(timeout=30)

    @override_settings(PERIODIC_TASKS_SCHEDULER_INTERVAL=42)
    def test_default_scheduler_interval_from_settings(self) -> None:
        """Bug 11: Default --scheduler-interval should come from settings."""
        import argparse

        from django_periodic_tasks.management.commands.scheduler_db_worker import (
            Command,
        )

        cmd = Command()
        parser = argparse.ArgumentParser()
        cmd.add_arguments(parser)
        # Check that --scheduler-interval default is from settings
        for action in parser._actions:
            if "--scheduler-interval" in getattr(action, "option_strings", []):
                self.assertEqual(action.default, 42)
                return
        self.fail("--scheduler-interval argument not found")
