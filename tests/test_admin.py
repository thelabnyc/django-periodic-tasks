from django.contrib.admin.sites import AdminSite
from django.contrib.auth.models import User
from django.test import RequestFactory, TestCase

from django_periodic_tasks.admin import ScheduledTaskAdmin
from django_periodic_tasks.models import ScheduledTask


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
