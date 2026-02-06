from pathlib import Path

import django_stubs_ext

django_stubs_ext.monkeypatch()

BASE_DIR = Path(__file__).resolve().parent

SECRET_KEY = "insecure-test-secret-key"

DEBUG = True

ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "django_periodic_tasks",
    "sandbox.testapp",
]

# Conditionally add django_tasks apps when the third-party package is installed.
# Django 6.0+ has native django.tasks which doesn't need INSTALLED_APPS entries.
try:
    import django_tasks  # noqa: F401

    INSTALLED_APPS.insert(-2, "django_tasks")
    INSTALLED_APPS.insert(-2, "django_tasks.backends.database")
except ImportError:
    pass

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
]

ROOT_URLCONF = "sandbox.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "postgres",
        "USER": "postgres",
        "PASSWORD": "",
        "HOST": "postgres",
        "PORT": 5432,
    }
}

# Detect the correct DummyBackend path
try:
    import django_tasks.backends.dummy  # noqa: F401

    _dummy_backend = "django_tasks.backends.dummy.DummyBackend"
except ImportError:
    _dummy_backend = "django.tasks.backends.dummy.DummyBackend"

TASKS = {
    "default": {
        "BACKEND": _dummy_backend,
        "ENQUEUE_ON_COMMIT": False,
    }
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

USE_TZ = True
TIME_ZONE = "UTC"

STATIC_URL = "static/"
