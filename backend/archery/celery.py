import os

from django.core.exceptions import ImproperlyConfigured

try:
    from celery import Celery
except ImportError:
    Celery = None


if Celery is not None:
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "archery.settings")
    import django

    django.setup()

    app = Celery("archery")
    app.config_from_object("django.conf:settings", namespace="CELERY")
    app.autodiscover_tasks(related_name="celery_tasks")

    from common.task_queue import celery_runtime_settings

    runtime_settings = celery_runtime_settings()
    missing_settings = [
        setting_name
        for setting_name, value in (
            ("CELERY_BROKER_URL", runtime_settings["broker_url"]),
            ("CELERY_RESULT_BACKEND", runtime_settings["result_backend"]),
        )
        if not value
    ]
    if missing_settings:
        raise ImproperlyConfigured(
            f"{', '.join(missing_settings)} required to initialize Celery."
        )

    overrides = {}
    if runtime_settings["broker_url"]:
        overrides["broker_url"] = runtime_settings["broker_url"]
    if runtime_settings["result_backend"]:
        overrides["result_backend"] = runtime_settings["result_backend"]
    if runtime_settings["task_default_queue"]:
        overrides["task_default_queue"] = runtime_settings["task_default_queue"]
    if runtime_settings["task_soft_time_limit"]:
        overrides["task_soft_time_limit"] = runtime_settings["task_soft_time_limit"]
    if runtime_settings["task_time_limit"]:
        overrides["task_time_limit"] = runtime_settings["task_time_limit"]
    if overrides:
        app.conf.update(**overrides)
else:
    app = None
