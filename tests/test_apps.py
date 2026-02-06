from unittest.mock import patch

from django.apps import apps
from django.test import TestCase, override_settings

import django_periodic_tasks.apps as apps_module


def _get_app() -> apps_module.DjangoPeriodicTasksConfig:
    config = apps.get_app_config("django_periodic_tasks")
    assert isinstance(config, apps_module.DjangoPeriodicTasksConfig)
    return config


class TestAppConfigReady(TestCase):
    def setUp(self) -> None:
        # Reset module-level guard before each test
        apps_module._scheduler = None

    def tearDown(self) -> None:
        apps_module._scheduler = None

    @patch("django_periodic_tasks.scheduler.PeriodicTaskScheduler.start")
    def test_autostart_creates_scheduler_with_correct_interval(self, mock_start: object) -> None:
        """When AUTOSTART=True, ready() creates a scheduler with the configured interval."""
        # Default interval (15s)
        with self.settings(PERIODIC_TASKS_AUTOSTART=True):
            _get_app().ready()
            self.assertIsNotNone(apps_module._scheduler)
            assert apps_module._scheduler is not None
            self.assertEqual(apps_module._scheduler.interval, 15)
            mock_start.assert_called_once()  # type: ignore[union-attr]

        apps_module._scheduler = None
        mock_start.reset_mock()  # type: ignore[union-attr]

        # Custom interval
        with self.settings(PERIODIC_TASKS_AUTOSTART=True, PERIODIC_TASKS_SCHEDULER_INTERVAL=30):
            _get_app().ready()
            assert apps_module._scheduler is not None
            self.assertEqual(apps_module._scheduler.interval, 30)
            mock_start.assert_called_once()  # type: ignore[union-attr]

    @patch("django_periodic_tasks.scheduler.PeriodicTaskScheduler.start")
    def test_does_not_start_when_autostart_disabled(self, mock_start: object) -> None:
        """Scheduler should not start when AUTOSTART is False or absent."""
        with self.settings(PERIODIC_TASKS_AUTOSTART=False):
            _get_app().ready()
            self.assertIsNone(apps_module._scheduler)

        # Also when the setting is absent entirely
        _get_app().ready()
        self.assertIsNone(apps_module._scheduler)

        mock_start.assert_not_called()  # type: ignore[union-attr]

    @patch("django_periodic_tasks.scheduler.PeriodicTaskScheduler.start")
    @override_settings(PERIODIC_TASKS_AUTOSTART=True)
    def test_double_ready_does_not_start_twice(self, mock_start: object) -> None:
        app = _get_app()
        app.ready()
        app.ready()

        mock_start.assert_called_once()  # type: ignore[union-attr]

    @patch("django_periodic_tasks.scheduler.PeriodicTaskScheduler.start")
    @override_settings(
        PERIODIC_TASKS_AUTOSTART=True,
        PERIODIC_TASKS_SCHEDULER_CLASS="django_periodic_tasks.scheduler.PeriodicTaskScheduler",
    )
    def test_custom_scheduler_class_setting(self, mock_start: object) -> None:
        """When PERIODIC_TASKS_SCHEDULER_CLASS is set, ready() uses that class."""
        _get_app().ready()
        self.assertIsNotNone(apps_module._scheduler)
        assert apps_module._scheduler is not None
        from django_periodic_tasks.scheduler import PeriodicTaskScheduler

        self.assertIsInstance(apps_module._scheduler, PeriodicTaskScheduler)
        mock_start.assert_called_once()  # type: ignore[union-attr]

    @override_settings(
        PERIODIC_TASKS_AUTOSTART=True,
        PERIODIC_TASKS_SCHEDULER_CLASS="nonexistent.module.Scheduler",
    )
    def test_invalid_scheduler_class_raises(self) -> None:
        """An invalid PERIODIC_TASKS_SCHEDULER_CLASS path raises ImportError."""
        with self.assertRaises(ImportError):
            _get_app().ready()
