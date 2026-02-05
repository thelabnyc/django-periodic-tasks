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
    @override_settings(PERIODIC_TASKS_AUTOSTART=True)
    def test_starts_scheduler_when_autostart_true(self, mock_start: object) -> None:
        _get_app().ready()

        self.assertIsNotNone(apps_module._scheduler)
        mock_start.assert_called_once()  # type: ignore[union-attr]

    @patch("django_periodic_tasks.scheduler.PeriodicTaskScheduler.start")
    @override_settings(PERIODIC_TASKS_AUTOSTART=False)
    def test_does_not_start_when_autostart_false(self, mock_start: object) -> None:
        _get_app().ready()

        self.assertIsNone(apps_module._scheduler)
        mock_start.assert_not_called()  # type: ignore[union-attr]

    @patch("django_periodic_tasks.scheduler.PeriodicTaskScheduler.start")
    def test_does_not_start_when_autostart_absent(self, mock_start: object) -> None:
        _get_app().ready()

        self.assertIsNone(apps_module._scheduler)
        mock_start.assert_not_called()  # type: ignore[union-attr]

    @patch("django_periodic_tasks.scheduler.PeriodicTaskScheduler.start")
    @override_settings(PERIODIC_TASKS_AUTOSTART=True, PERIODIC_TASKS_SCHEDULER_INTERVAL=30)
    def test_passes_interval_setting(self, mock_start: object) -> None:
        _get_app().ready()

        self.assertIsNotNone(apps_module._scheduler)
        assert apps_module._scheduler is not None
        self.assertEqual(apps_module._scheduler.interval, 30)

    @patch("django_periodic_tasks.scheduler.PeriodicTaskScheduler.start")
    @override_settings(PERIODIC_TASKS_AUTOSTART=True)
    def test_uses_default_interval(self, mock_start: object) -> None:
        _get_app().ready()

        assert apps_module._scheduler is not None
        self.assertEqual(apps_module._scheduler.interval, 15)

    @patch("django_periodic_tasks.scheduler.PeriodicTaskScheduler.start")
    @override_settings(PERIODIC_TASKS_AUTOSTART=True)
    def test_double_ready_does_not_start_twice(self, mock_start: object) -> None:
        app = _get_app()
        app.ready()
        app.ready()

        mock_start.assert_called_once()  # type: ignore[union-attr]

    @patch("django_periodic_tasks.apps.autodiscover_modules")
    def test_autodiscovers_tasks_modules(self, mock_autodiscover: object) -> None:
        _get_app().ready()

        mock_autodiscover.assert_called_once_with("tasks")  # type: ignore[union-attr]
