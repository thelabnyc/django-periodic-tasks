from __future__ import annotations

import logging

from django.contrib import admin, messages
from django.db.models import QuerySet
from django.http import HttpRequest

from django_periodic_tasks.models import ScheduledTask, TaskExecution

logger = logging.getLogger(__name__)

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
    actions = ["enable_selected", "disable_selected", "run_selected_now"]

    @admin.action(description="Enable selected scheduled tasks")
    def enable_selected(self, request: HttpRequest, queryset: QuerySet[ScheduledTask]) -> None:
        count = 0
        for task in queryset:
            if not task.enabled:
                task.enabled = True
                task.save(update_fields=["enabled"])
                count += 1
        messages.success(request, f"Enabled {count} scheduled task(s).")

    @admin.action(description="Disable selected scheduled tasks")
    def disable_selected(self, request: HttpRequest, queryset: QuerySet[ScheduledTask]) -> None:
        count = 0
        for task in queryset:
            if task.enabled:
                task.enabled = False
                task.save(update_fields=["enabled"])
                count += 1
        messages.success(request, f"Disabled {count} scheduled task(s).")

    @admin.action(description="Run selected tasks now (ad-hoc enqueue)")
    def run_selected_now(self, request: HttpRequest, queryset: QuerySet[ScheduledTask]) -> None:
        enqueued = 0
        failed: list[str] = []
        for st in queryset:
            try:
                st.enqueue_now()
            except Exception:
                logger.exception("Failed to enqueue task: %s (path=%s)", st.name, st.task_path)
                failed.append(st.name)
                continue
            enqueued += 1
        if enqueued:
            messages.success(request, f"Enqueued {enqueued} task(s).")
        if failed:
            messages.warning(request, f"Failed to enqueue task(s): {', '.join(failed)}")

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
        obj: ScheduledTask | None = None,
    ) -> bool:
        if obj is not None and obj.source == ScheduledTask.Source.CODE:
            return False
        return super().has_delete_permission(request, obj)


@admin.register(TaskExecution)
class TaskExecutionAdmin(admin.ModelAdmin[TaskExecution]):
    list_display = ("id", "scheduled_task", "status", "created_at", "completed_at")
    list_filter = ("status",)
    search_fields = ("scheduled_task__name",)
    ordering = ("-created_at",)

    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj: TaskExecution | None = None) -> bool:
        return False

    def has_delete_permission(self, request: HttpRequest, obj: TaskExecution | None = None) -> bool:
        return False
