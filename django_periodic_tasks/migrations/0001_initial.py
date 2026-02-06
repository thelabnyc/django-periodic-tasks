import uuid

from django.db import migrations, models

import django_periodic_tasks.task_resolver


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="ScheduledTask",
            fields=[
                ("id", models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("name", models.CharField(max_length=200, unique=True)),
                ("task_path", models.CharField(choices=django_periodic_tasks.task_resolver.get_all_task_choices, max_length=200)),
                ("cron_expression", models.CharField(max_length=200)),
                ("timezone", models.CharField(default="UTC", max_length=63)),
                ("args", models.JSONField(blank=True, default=list)),
                ("kwargs", models.JSONField(blank=True, default=dict)),
                ("source", models.CharField(choices=[("code", "Code"), ("database", "Database")], default="database", max_length=20)),
                ("enabled", models.BooleanField(default=True)),
                ("last_run_at", models.DateTimeField(blank=True, null=True)),
                ("next_run_at", models.DateTimeField(blank=True, null=True)),
                ("total_run_count", models.PositiveIntegerField(default=0)),
                ("queue_name", models.CharField(blank=True, default="default", max_length=32)),
                ("priority", models.IntegerField(default=0)),
                ("backend", models.CharField(blank=True, default="default", max_length=32)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("updated_at", models.DateTimeField(auto_now=True)),
            ],
            options={
                "indexes": [models.Index(condition=models.Q(("enabled", True)), fields=["next_run_at"], name="periodic_due_tasks_idx")],
            },
        ),
        migrations.CreateModel(
            name="TaskExecution",
            fields=[
                ("id", models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ("status", models.CharField(choices=[("pending", "Pending"), ("completed", "Completed")], default="pending", max_length=20)),
                ("created_at", models.DateTimeField(auto_now_add=True)),
                ("completed_at", models.DateTimeField(blank=True, null=True)),
                (
                    "scheduled_task",
                    models.ForeignKey(on_delete=models.deletion.CASCADE, related_name="executions", to="django_periodic_tasks.scheduledtask"),
                ),
            ],
            options={
                "indexes": [models.Index(condition=models.Q(("status", "pending")), fields=["status"], name="periodic_pending_exec_idx")],
            },
        ),
    ]
