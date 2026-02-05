from django.apps import apps
from django.conf import settings
import django

INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django_tasks",
    "django_periodic_tasks",
]
SECRET_KEY = "docs"
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
DATABASES = {"default": {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}}
TASKS = {"default": {"BACKEND": "django_tasks.backends.dummy.DummyBackend"}}
USE_TZ = True

if not apps.ready and not settings.configured:
    django.setup()

import django_stubs_ext  # noqa: E402

django_stubs_ext.monkeypatch()

setup = None
