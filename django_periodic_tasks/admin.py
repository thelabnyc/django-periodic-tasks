from typing import Any

from django.contrib import admin
from django.http import HttpRequest

from django_periodic_tasks.models import ScheduledTask

# Fields that are always read-only (computed/tracking)
TRACKING_READONLY = ("source", "last_run_at", "next_run_at", "total_run_count", "created_at", "updated_at")

# All editable model fields
ALL_FIELDS = (
    "name",
    "task_path",
    "cron_expression",
    "timezone",
    "args",
    "kwargs",
    "source",
    "enabled",
    "queue_name",
    "priority",
    "backend",
    "last_run_at",
    "next_run_at",
    "total_run_count",
    "created_at",
    "updated_at",
)


@admin.register(ScheduledTask)
class ScheduledTaskAdmin(admin.ModelAdmin[ScheduledTask]):
    list_display = (
        "name",
        "task_path",
        "cron_expression",
        "source",
        "enabled",
        "last_run_at",
        "next_run_at",
        "total_run_count",
    )
    list_filter = ("source", "enabled")
    search_fields = ("name", "task_path")
    ordering = ("name",)

    def get_readonly_fields(
        self,
        request: HttpRequest,
        obj: ScheduledTask | None = None,
    ) -> tuple[str, ...]:
        if obj is None:
            # New task being created
            return TRACKING_READONLY
        if obj.source == ScheduledTask.Source.CODE:
            # Code-defined tasks: everything is read-only
            return ALL_FIELDS
        # DB-defined tasks: only tracking fields are read-only
        return TRACKING_READONLY

    def has_delete_permission(
        self,
        request: HttpRequest,
        obj: Any = None,
    ) -> bool:
        if obj is not None and obj.source == ScheduledTask.Source.CODE:
            return False
        return super().has_delete_permission(request, obj)
