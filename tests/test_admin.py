from typing import Any

from django.contrib import messages
from django.contrib.admin.sites import AdminSite
from django.contrib.auth.models import User
from django.contrib.messages.storage.fallback import FallbackStorage
from django.contrib.sessions.backends.db import SessionStore
from django.forms import Select
from django.http import HttpRequest
from django.test import RequestFactory, TestCase, override_settings

from django_periodic_tasks.admin import ScheduledTaskAdmin
from django_periodic_tasks.compat import DUMMY_BACKEND_PATH, default_task_backend
from django_periodic_tasks.models import ScheduledTask, TaskExecution

DUMMY_BACKEND_SETTINGS = {
    "default": {
        "BACKEND": DUMMY_BACKEND_PATH,
        "ENQUEUE_ON_COMMIT": False,
    }
}


class TestScheduledTaskAdmin(TestCase):
    def setUp(self) -> None:
        self.site = AdminSite()
        self.admin = ScheduledTaskAdmin(ScheduledTask, self.site)
        self.factory = RequestFactory()
        self.superuser = User.objects.create_superuser(username="admin", password="admin", email="admin@test.com")

    def test_list_display(self) -> None:
        self.assertIn("name", self.admin.list_display)
        self.assertIn("task_path", self.admin.list_display)
        self.assertIn("cron_expression", self.admin.list_display)
        self.assertIn("source", self.admin.list_display)
        self.assertIn("enabled", self.admin.list_display)
        self.assertIn("last_run_at", self.admin.list_display)
        self.assertIn("next_run_at", self.admin.list_display)
        self.assertIn("total_run_count", self.admin.list_display)

    def test_list_filter(self) -> None:
        self.assertIn("source", self.admin.list_filter)
        self.assertIn("enabled", self.admin.list_filter)

    def test_code_defined_fields_readonly(self) -> None:
        code_task = ScheduledTask.objects.create(
            name="code-task",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
            source=ScheduledTask.Source.CODE,
        )
        request = self.factory.get("/admin/")
        readonly = self.admin.get_readonly_fields(request, code_task)
        # All model fields should be read-only for code-defined tasks
        self.assertIn("name", readonly)
        self.assertIn("task_path", readonly)
        self.assertIn("cron_expression", readonly)
        self.assertIn("source", readonly)

    def test_db_defined_fields_editable(self) -> None:
        db_task = ScheduledTask.objects.create(
            name="db-task",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
            source=ScheduledTask.Source.DATABASE,
        )
        request = self.factory.get("/admin/")
        readonly = self.admin.get_readonly_fields(request, db_task)
        # Only tracking fields should be read-only for DB tasks
        self.assertNotIn("name", readonly)
        self.assertNotIn("task_path", readonly)
        self.assertNotIn("cron_expression", readonly)
        self.assertIn("source", readonly)
        self.assertIn("last_run_at", readonly)
        self.assertIn("next_run_at", readonly)
        self.assertIn("total_run_count", readonly)

    def test_new_task_fields_editable(self) -> None:
        request = self.factory.get("/admin/")
        readonly = self.admin.get_readonly_fields(request, None)
        # New tasks should have minimal read-only fields
        self.assertNotIn("name", readonly)
        self.assertNotIn("task_path", readonly)

    def test_cannot_delete_code_defined(self) -> None:
        code_task = ScheduledTask.objects.create(
            name="code-task",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
            source=ScheduledTask.Source.CODE,
        )
        request = self.factory.get("/admin/")
        request.user = self.superuser
        self.assertFalse(self.admin.has_delete_permission(request, code_task))

    def test_can_delete_db_defined(self) -> None:
        db_task = ScheduledTask.objects.create(
            name="db-task",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
            source=ScheduledTask.Source.DATABASE,
        )
        request = self.factory.get("/admin/")
        request.user = self.superuser
        self.assertTrue(self.admin.has_delete_permission(request, db_task))

    def test_search_fields(self) -> None:
        self.assertIn("name", self.admin.search_fields)
        self.assertIn("task_path", self.admin.search_fields)

    def test_task_path_renders_as_select(self) -> None:
        request = self.factory.get("/admin/")
        request.user = self.superuser
        form_class = self.admin.get_form(request, obj=None)
        form = form_class()
        widget = form.fields["task_path"].widget
        self.assertIsInstance(widget, Select)

    def test_actions_registered(self) -> None:
        request = self.factory.get("/admin/")
        request.user = self.superuser
        actions = self.admin.get_actions(request)
        self.assertIn("enable_selected", actions)
        self.assertIn("disable_selected", actions)
        self.assertIn("run_selected_now", actions)


class _AdminActionTestBase(TestCase):
    """Shared setUp for admin action tests."""

    def setUp(self) -> None:
        self.site = AdminSite()
        self.admin = ScheduledTaskAdmin(ScheduledTask, self.site)
        self.factory = RequestFactory()
        self.superuser = User.objects.create_superuser(username="admin", password="admin", email="admin@test.com")

    def _make_request(self) -> HttpRequest:
        request = self.factory.post("/admin/")
        request.user = self.superuser
        request.session = SessionStore()
        request._messages = FallbackStorage(request)  # type: ignore[attr-defined]
        return request

    def _get_messages(self, request: HttpRequest) -> list[Any]:
        return list(messages.get_messages(request))


class TestEnableAction(_AdminActionTestBase):
    def test_enable_disabled_tasks(self) -> None:
        t1 = ScheduledTask.objects.create(
            name="t1",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
            enabled=False,
        )
        t2 = ScheduledTask.objects.create(
            name="t2",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 6 * * *",
            enabled=False,
        )
        request = self._make_request()
        qs = ScheduledTask.objects.filter(pk__in=[t1.pk, t2.pk])
        self.admin.enable_selected(request, qs)

        t1.refresh_from_db()
        t2.refresh_from_db()
        self.assertTrue(t1.enabled)
        self.assertTrue(t2.enabled)
        self.assertIsNotNone(t1.next_run_at)
        self.assertIsNotNone(t2.next_run_at)

        msgs = self._get_messages(request)
        self.assertEqual(len(msgs), 1)
        self.assertIn("2", str(msgs[0]))

    def test_already_enabled_reports_zero(self) -> None:
        ScheduledTask.objects.create(
            name="t1",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
            enabled=True,
        )
        request = self._make_request()
        qs = ScheduledTask.objects.all()
        self.admin.enable_selected(request, qs)

        msgs = self._get_messages(request)
        self.assertEqual(len(msgs), 1)
        self.assertIn("0", str(msgs[0]))

    def test_mixed_enabled_disabled(self) -> None:
        ScheduledTask.objects.create(
            name="already-on",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
            enabled=True,
        )
        ScheduledTask.objects.create(
            name="was-off",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 6 * * *",
            enabled=False,
        )
        request = self._make_request()
        qs = ScheduledTask.objects.all()
        self.admin.enable_selected(request, qs)

        msgs = self._get_messages(request)
        self.assertIn("1", str(msgs[0]))


class TestDisableAction(_AdminActionTestBase):
    def test_disable_enabled_tasks(self) -> None:
        t1 = ScheduledTask.objects.create(
            name="t1",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
            enabled=True,
        )
        t2 = ScheduledTask.objects.create(
            name="t2",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 6 * * *",
            enabled=True,
        )
        request = self._make_request()
        qs = ScheduledTask.objects.filter(pk__in=[t1.pk, t2.pk])
        self.admin.disable_selected(request, qs)

        t1.refresh_from_db()
        t2.refresh_from_db()
        self.assertFalse(t1.enabled)
        self.assertFalse(t2.enabled)
        self.assertIsNone(t1.next_run_at)
        self.assertIsNone(t2.next_run_at)

        msgs = self._get_messages(request)
        self.assertEqual(len(msgs), 1)
        self.assertIn("2", str(msgs[0]))

    def test_already_disabled_reports_zero(self) -> None:
        ScheduledTask.objects.create(
            name="t1",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
            enabled=False,
        )
        request = self._make_request()
        qs = ScheduledTask.objects.all()
        self.admin.disable_selected(request, qs)

        msgs = self._get_messages(request)
        self.assertEqual(len(msgs), 1)
        self.assertIn("0", str(msgs[0]))

    def test_mixed_enabled_disabled(self) -> None:
        ScheduledTask.objects.create(
            name="was-on",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
            enabled=True,
        )
        ScheduledTask.objects.create(
            name="already-off",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 6 * * *",
            enabled=False,
        )
        request = self._make_request()
        qs = ScheduledTask.objects.all()
        self.admin.disable_selected(request, qs)

        msgs = self._get_messages(request)
        self.assertIn("1", str(msgs[0]))


@override_settings(TASKS=DUMMY_BACKEND_SETTINGS)
class TestRunNowAction(_AdminActionTestBase):
    def setUp(self) -> None:
        super().setUp()
        default_task_backend.clear()

    def test_enqueues_task(self) -> None:
        st = ScheduledTask.objects.create(
            name="run-me",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
            enabled=True,
        )
        request = self._make_request()
        qs = ScheduledTask.objects.filter(pk=st.pk)
        self.admin.run_selected_now(request, qs)

        self.assertEqual(len(default_task_backend.results), 1)
        msgs = self._get_messages(request)
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs[0].level, messages.SUCCESS)
        self.assertIn("1", str(msgs[0]))

    def test_enqueues_disabled_task(self) -> None:
        st = ScheduledTask.objects.create(
            name="disabled-run",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
            enabled=False,
        )
        request = self._make_request()
        qs = ScheduledTask.objects.filter(pk=st.pk)
        self.admin.run_selected_now(request, qs)

        self.assertEqual(len(default_task_backend.results), 1)

    def test_skips_unresolvable_task(self) -> None:
        st = ScheduledTask.objects.create(
            name="bad-task",
            task_path="nonexistent.module.fake_task",
            cron_expression="0 5 * * *",
            enabled=True,
        )
        request = self._make_request()
        qs = ScheduledTask.objects.filter(pk=st.pk)
        self.admin.run_selected_now(request, qs)

        self.assertEqual(len(default_task_backend.results), 0)
        msgs = self._get_messages(request)
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs[0].level, messages.WARNING)
        self.assertIn("bad-task", str(msgs[0]))

    def test_mixed_valid_and_invalid(self) -> None:
        ScheduledTask.objects.create(
            name="good-task",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
            enabled=True,
        )
        ScheduledTask.objects.create(
            name="bad-task",
            task_path="nonexistent.module.fake_task",
            cron_expression="0 6 * * *",
            enabled=True,
        )
        request = self._make_request()
        qs = ScheduledTask.objects.all()
        self.admin.run_selected_now(request, qs)

        self.assertEqual(len(default_task_backend.results), 1)
        msgs = self._get_messages(request)
        self.assertEqual(len(msgs), 2)
        levels = {m.level for m in msgs}
        self.assertIn(messages.SUCCESS, levels)
        self.assertIn(messages.WARNING, levels)

    def test_does_not_update_tracking_fields(self) -> None:
        st = ScheduledTask.objects.create(
            name="track-test",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
            enabled=True,
        )
        original_last_run_at = st.last_run_at
        original_next_run_at = st.next_run_at
        original_total_run_count = st.total_run_count

        request = self._make_request()
        qs = ScheduledTask.objects.filter(pk=st.pk)
        self.admin.run_selected_now(request, qs)

        st.refresh_from_db()
        self.assertEqual(st.last_run_at, original_last_run_at)
        self.assertEqual(st.next_run_at, original_next_run_at)
        self.assertEqual(st.total_run_count, original_total_run_count)

    def test_multiple_tasks_enqueued(self) -> None:
        ScheduledTask.objects.create(
            name="t1",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 5 * * *",
            enabled=True,
        )
        ScheduledTask.objects.create(
            name="t2",
            task_path="sandbox.testapp.tasks.example_task",
            cron_expression="0 6 * * *",
            enabled=True,
        )
        request = self._make_request()
        qs = ScheduledTask.objects.all()
        self.admin.run_selected_now(request, qs)

        self.assertEqual(len(default_task_backend.results), 2)
        msgs = self._get_messages(request)
        self.assertEqual(len(msgs), 1)
        self.assertIn("2", str(msgs[0]))

    def test_exactly_once_task_creates_execution(self) -> None:
        st = ScheduledTask.objects.create(
            name="eo-admin-test",
            task_path="sandbox.testapp.tasks.exactly_once_task",
            cron_expression="0 5 * * *",
            enabled=True,
        )
        request = self._make_request()
        qs = ScheduledTask.objects.filter(pk=st.pk)

        with self.captureOnCommitCallbacks(execute=True):
            self.admin.run_selected_now(request, qs)

        self.assertEqual(TaskExecution.objects.count(), 1)
        execution = TaskExecution.objects.first()
        assert execution is not None
        self.assertEqual(execution.scheduled_task_id, st.pk)

        self.assertEqual(len(default_task_backend.results), 1)
        result = default_task_backend.results[0]
        self.assertIn("_periodic_tasks_execution_id", result.kwargs)
        self.assertEqual(result.kwargs["_periodic_tasks_execution_id"], str(execution.id))

        msgs = self._get_messages(request)
        self.assertEqual(len(msgs), 1)
        self.assertEqual(msgs[0].level, messages.SUCCESS)
