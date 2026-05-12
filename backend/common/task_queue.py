import datetime
import hashlib
import hmac
import importlib
import json
import logging
import traceback
import uuid
from decimal import Decimal
from dataclasses import dataclass
from typing import Any

from django.conf import settings
from django.apps import apps
from django.db import transaction
from django.db.models import Model
from django.utils import timezone

from common.config import SysConfig
from sql.models import Config, TaskSchedule

logger = logging.getLogger("default")

TASK_BACKEND_CELERY = "celery"


@dataclass
class TaskResult:
    task_name: str
    callable_path: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    success: bool
    result: Any = None
    error: str = ""
    traceback_text: str = ""
    started: datetime.datetime | None = None
    stopped: datetime.datetime | None = None
    backend: str = ""
    backend_job_id: str = ""
    schedule_name: str = ""


def async_task(func, *args, hook=None, task_name=None, timeout=None, **kwargs):
    payload = _encode_task_payload(
        func=func,
        args=args,
        kwargs=kwargs,
        hook=hook,
        task_name=task_name,
        schedule_name="",
    )
    return get_task_backend().enqueue_payload(
        payload=payload,
        task_name=task_name or _callable_path(func),
        timeout=timeout,
    )


def schedule(
    func,
    *args,
    hook=None,
    name=None,
    schedule_type="O",
    next_run=None,
    repeats=1,
    timeout=None,
    **kwargs,
):
    if not name:
        raise ValueError("Scheduled tasks require a name.")
    if schedule_type != "O":
        raise ValueError("Only one-off schedules are supported.")
    if repeats != 1:
        raise ValueError("Only single-run schedules are supported.")
    if next_run is None:
        raise ValueError("Scheduled tasks require next_run.")

    payload = _encode_task_payload(
        func=func,
        args=args,
        kwargs=kwargs,
        hook=hook,
        task_name=name,
        schedule_name=name,
    )
    return get_task_backend().schedule_payload(
        name=name,
        payload=payload,
        run_at=next_run,
        task_name=name,
        callable_path=_callable_path(func),
        timeout=timeout,
    )


def delete_schedule(name):
    get_task_backend().cancel_scheduled(name)


def task_info(name):
    return (
        TaskSchedule.objects.filter(name=name, status=TaskSchedule.STATUS_SCHEDULED)
        .order_by("-id")
        .first()
    )


def execute_payload(payload):
    try:
        task_payload = _decode_task_payload(payload)
    except Exception:
        logger.exception("Rejected task payload before execution.")
        raise
    callable_path = task_payload["callable_path"]
    callback_path = task_payload["callback_path"]
    task_name = task_payload["task_name"]
    schedule_name = task_payload["schedule_name"]
    started = _now()
    backend = current_task_backend()
    backend_job_id = task_payload.get("backend_job_id", "")

    if schedule_name:
        _mark_schedule_running(schedule_name, backend_job_id=backend_job_id)

    try:
        target = _import_from_path(callable_path)
        result = target(*task_payload["args"], **task_payload["kwargs"])
    except Exception as exc:
        task_result = TaskResult(
            task_name=task_name,
            callable_path=callable_path,
            args=tuple(task_payload["args"]),
            kwargs=task_payload["kwargs"],
            success=False,
            result=str(exc),
            error=str(exc),
            traceback_text=traceback.format_exc(),
            started=started,
            stopped=_now(),
            backend=backend,
            backend_job_id=backend_job_id,
            schedule_name=schedule_name,
        )
        if schedule_name:
            _mark_schedule_failed(schedule_name, str(exc))
        _run_callback_safely(callback_path, task_result, callable_path, "failure")
        raise

    task_result = TaskResult(
        task_name=task_name,
        callable_path=callable_path,
        args=tuple(task_payload["args"]),
        kwargs=task_payload["kwargs"],
        success=True,
        result=result,
        started=started,
        stopped=_now(),
        backend=backend,
        backend_job_id=backend_job_id,
        schedule_name=schedule_name,
    )
    if schedule_name:
        _mark_schedule_completed(schedule_name)
    _run_callback_safely(callback_path, task_result, callable_path, "success")
    return result


def task_backend_info(full=False):
    backend = get_task_backend()
    return {
        "active": backend.backend_id,
        "config": backend.health_snapshot(full=full),
        "scheduled": {
            "pending": TaskSchedule.objects.filter(
                status=TaskSchedule.STATUS_SCHEDULED
            ).count(),
            "running": TaskSchedule.objects.filter(
                status=TaskSchedule.STATUS_RUNNING
            ).count(),
        },
    }


def current_task_backend():
    return TASK_BACKEND_CELERY


def celery_runtime_settings():
    values = {
        "broker_url": getattr(settings, "CELERY_BROKER_URL", ""),
        "result_backend": getattr(settings, "CELERY_RESULT_BACKEND", ""),
        "task_default_queue": getattr(settings, "CELERY_TASK_DEFAULT_QUEUE", "default"),
        "task_soft_time_limit": getattr(settings, "CELERY_TASK_SOFT_TIME_LIMIT", None),
        "task_time_limit": getattr(settings, "CELERY_TASK_TIME_LIMIT", None),
    }

    try:
        config_map = dict(
            Config.objects.filter(
                item__in=(
                    "celery_broker_url",
                    "celery_result_backend",
                    "celery_task_default_queue",
                    "celery_task_soft_time_limit",
                    "celery_task_time_limit",
                )
            ).values_list("item", "value")
        )
    except Exception:
        config_map = {}

    if config_map.get("celery_broker_url"):
        values["broker_url"] = config_map["celery_broker_url"]
    if config_map.get("celery_result_backend"):
        values["result_backend"] = config_map["celery_result_backend"]
    if config_map.get("celery_task_default_queue"):
        values["task_default_queue"] = config_map["celery_task_default_queue"]
    if config_map.get("celery_task_soft_time_limit") not in (None, ""):
        values["task_soft_time_limit"] = int(config_map["celery_task_soft_time_limit"])
    if config_map.get("celery_task_time_limit") not in (None, ""):
        values["task_time_limit"] = int(config_map["celery_task_time_limit"])
    return values


class BaseTaskBackend:
    backend_id = ""

    def enqueue_payload(self, payload, task_name, timeout=None):
        raise NotImplementedError

    def schedule_payload(
        self, name, payload, run_at, task_name, callable_path, timeout
    ):
        raise NotImplementedError

    def cancel_scheduled(self, name):
        raise NotImplementedError

    def health_snapshot(self, full=False):
        raise NotImplementedError

    @staticmethod
    def _normalized_timeout(timeout):
        if timeout in (None, "", -1):
            return None
        return int(timeout)


class CeleryTaskBackend(BaseTaskBackend):
    backend_id = TASK_BACKEND_CELERY

    def enqueue_payload(self, payload, task_name, timeout=None):
        task = _celery_execute_task()
        _refresh_celery_runtime_config()
        apply_kwargs = {}
        timeout_value = self._normalized_timeout(timeout)
        if timeout_value is not None:
            apply_kwargs["time_limit"] = timeout_value
        result = task.apply_async(
            args=[payload],
            queue=celery_runtime_settings()["task_default_queue"],
            **apply_kwargs,
        )
        return result.id

    def schedule_payload(
        self, name, payload, run_at, task_name, callable_path, timeout
    ):
        run_at_value = _normalize_run_at(run_at)
        timeout_value = self._normalized_timeout(timeout)
        with transaction.atomic():
            TaskSchedule.objects.update_or_create(
                name=name,
                defaults={
                    "backend": self.backend_id,
                    "task_name": task_name,
                    "callable_path": callable_path,
                    "payload": payload,
                    "run_at": run_at_value,
                    "status": TaskSchedule.STATUS_SCHEDULED,
                    "backend_job_id": "",
                    "last_error": "",
                    "completed_at": None,
                    "cancelled_at": None,
                },
            )
        task = _celery_execute_task()
        _refresh_celery_runtime_config()
        apply_kwargs = {
            "eta": run_at_value,
            "queue": celery_runtime_settings()["task_default_queue"],
        }
        if timeout_value is not None:
            apply_kwargs["time_limit"] = timeout_value
        try:
            result = task.apply_async(args=[payload], **apply_kwargs)
        except Exception as exc:
            _mark_schedule_failed(name, str(exc))
            raise
        TaskSchedule.objects.filter(name=name).update(backend_job_id=result.id)
        return task_info(name)

    def cancel_scheduled(self, name):
        schedule_entry = TaskSchedule.objects.filter(name=name).order_by("-id").first()
        if not schedule_entry:
            return
        backend_job_id = schedule_entry.backend_job_id
        if backend_job_id:
            app = _celery_app()
            app.control.revoke(backend_job_id)
        TaskSchedule.objects.filter(id=schedule_entry.id).update(
            status=TaskSchedule.STATUS_CANCELLED,
            cancelled_at=_now(),
        )

    def health_snapshot(self, full=False):
        info = {"label": "Celery"}
        try:
            import celery

            config = celery_runtime_settings()
            app = _celery_app()
            _refresh_celery_runtime_config()
            inspect = app.control.inspect(timeout=1)
            ping = inspect.ping() if inspect else None
            stats = inspect.stats() if inspect and full else None
            info.update(
                {
                    "version": celery.__version__,
                    "broker_url": _mask_connection_string(config["broker_url"]),
                    "result_backend": _mask_connection_string(config["result_backend"]),
                    "task_default_queue": config["task_default_queue"],
                    "task_soft_time_limit": config["task_soft_time_limit"],
                    "task_time_limit": config["task_time_limit"],
                    "workers": ping if ping else "No reachable Celery workers.",
                    "stats": stats if full else None,
                }
            )
        except Exception as exc:
            info["error"] = f"Failed to get Celery info: {exc}"
        return info


def get_task_backend():
    return CeleryTaskBackend()


def _encode_task_payload(func, args, kwargs, hook, task_name, schedule_name):
    payload = {
        "callable_path": _callable_path(func),
        "args": args,
        "kwargs": kwargs,
        "callback_path": _callable_path(hook) if hook else "",
        "task_name": task_name or _callable_path(func),
        "schedule_name": schedule_name,
        "backend_job_id": "",
    }
    serialized_payload = _serialize_task_value(payload)
    envelope = {
        "payload": serialized_payload,
        "signature": _sign_task_payload(serialized_payload),
    }
    return _payload_json(envelope)


def _decode_task_payload(payload):
    envelope = json.loads(payload)
    serialized_payload = envelope["payload"]
    provided_signature = envelope["signature"]
    expected_signature = _sign_task_payload(serialized_payload)
    if not hmac.compare_digest(provided_signature, expected_signature):
        raise ValueError("Task payload signature verification failed.")
    return _deserialize_task_value(serialized_payload)


def _payload_json(value):
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _task_signing_secret():
    return settings.SECRET_KEY.encode("utf-8")


def _sign_task_payload(serialized_payload):
    return hmac.new(
        _task_signing_secret(),
        _payload_json(serialized_payload).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def _serialize_task_value(value):
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, tuple):
        return {
            "__task_type__": "tuple",
            "items": [_serialize_task_value(item) for item in value],
        }
    if isinstance(value, list):
        return [_serialize_task_value(item) for item in value]
    if isinstance(value, dict):
        if all(isinstance(key, str) for key in value) and "__task_type__" not in value:
            return {
                key: _serialize_task_value(item_value)
                for key, item_value in value.items()
            }
        return {
            "__task_type__": "dict",
            "items": [
                [
                    _serialize_task_value(item_key),
                    _serialize_task_value(item_value),
                ]
                for item_key, item_value in value.items()
            ],
        }
    if isinstance(value, datetime.datetime):
        return {"__task_type__": "datetime", "value": value.isoformat()}
    if isinstance(value, datetime.date):
        return {"__task_type__": "date", "value": value.isoformat()}
    if isinstance(value, datetime.time):
        return {"__task_type__": "time", "value": value.isoformat()}
    if isinstance(value, Decimal):
        return {"__task_type__": "decimal", "value": str(value)}
    if isinstance(value, uuid.UUID):
        return {"__task_type__": "uuid", "value": str(value)}
    if isinstance(value, Model):
        if value.pk is None:
            raise TypeError(
                f"Cannot serialize unsaved model instance {value.__class__.__name__}."
            )
        return {
            "__task_type__": "model",
            "label": value._meta.label,
            "pk": _serialize_task_value(value.pk),
        }
    raise TypeError(f"Unsupported task payload type: {type(value)!r}")


def _deserialize_task_value(value):
    if isinstance(value, list):
        return [_deserialize_task_value(item) for item in value]
    if not isinstance(value, dict):
        return value

    task_type = value.get("__task_type__")
    if not task_type:
        return {
            item_key: _deserialize_task_value(item_value)
            for item_key, item_value in value.items()
        }
    if task_type == "tuple":
        return tuple(_deserialize_task_value(item) for item in value["items"])
    if task_type == "dict":
        return {
            _deserialize_task_value(item_key): _deserialize_task_value(item_value)
            for item_key, item_value in value["items"]
        }
    if task_type == "datetime":
        return datetime.datetime.fromisoformat(value["value"])
    if task_type == "date":
        return datetime.date.fromisoformat(value["value"])
    if task_type == "time":
        return datetime.time.fromisoformat(value["value"])
    if task_type == "decimal":
        return Decimal(value["value"])
    if task_type == "uuid":
        return uuid.UUID(value["value"])
    if task_type == "model":
        model = apps.get_model(value["label"])
        if model is None:
            raise LookupError(f"Unknown model reference: {value['label']}")
        return model._default_manager.get(pk=_deserialize_task_value(value["pk"]))
    raise ValueError(f"Unsupported task payload marker: {task_type}")


def _callable_path(value):
    if not value:
        return ""
    if isinstance(value, str):
        return value
    return f"{value.__module__}.{value.__qualname__}"


def _import_from_path(path):
    module_path, attr_name = path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, attr_name)


def _run_callback(callback_path, task_result):
    if not callback_path:
        return
    callback = _import_from_path(callback_path)
    callback(task_result)


def _run_callback_safely(callback_path, task_result, callable_path, phase):
    try:
        _run_callback(callback_path, task_result)
    except Exception:
        logger.exception(
            "Task callback failed for %s during %s handling.",
            callable_path,
            phase,
        )


def _normalize_run_at(run_at):
    if timezone.is_aware(run_at):
        return (
            timezone.localtime(run_at)
            if settings.USE_TZ
            else timezone.make_naive(run_at)
        )
    return run_at


def _now():
    now = timezone.now()
    if settings.USE_TZ:
        return now
    if timezone.is_aware(now):
        return timezone.make_naive(now)
    return now


def _mask_connection_string(value):
    if not value:
        return ""
    if "://" not in value:
        return value
    scheme, rest = value.split("://", 1)
    if "@" not in rest:
        return f"{scheme}://{rest}"
    _, host = rest.rsplit("@", 1)
    return f"{scheme}://***@{host}"


def _mark_schedule_running(name, backend_job_id=""):
    updates = {
        "status": TaskSchedule.STATUS_RUNNING,
        "last_error": "",
    }
    if backend_job_id:
        updates["backend_job_id"] = backend_job_id
    TaskSchedule.objects.filter(name=name).update(**updates)


def _mark_schedule_completed(name):
    TaskSchedule.objects.filter(name=name).update(
        status=TaskSchedule.STATUS_COMPLETED,
        completed_at=_now(),
    )


def _mark_schedule_failed(name, error):
    TaskSchedule.objects.filter(name=name).update(
        status=TaskSchedule.STATUS_FAILED,
        completed_at=_now(),
        last_error=error[:2000],
    )


def _celery_app():
    from archery.celery import app as celery_app

    if celery_app is None:
        raise RuntimeError(
            "Celery support is unavailable because the celery package is not installed."
        )
    return celery_app


def _refresh_celery_runtime_config():
    app = _celery_app()
    config = celery_runtime_settings()
    overrides = {}
    if config["broker_url"]:
        overrides["broker_url"] = config["broker_url"]
    if config["result_backend"]:
        overrides["result_backend"] = config["result_backend"]
    if config["task_default_queue"]:
        overrides["task_default_queue"] = config["task_default_queue"]
    if config["task_soft_time_limit"]:
        overrides["task_soft_time_limit"] = config["task_soft_time_limit"]
    if config["task_time_limit"]:
        overrides["task_time_limit"] = config["task_time_limit"]
    if overrides:
        app.conf.update(**overrides)
    return app


def _celery_execute_task():
    _celery_app()
    from common.celery_tasks import execute_payload_task

    return execute_payload_task
