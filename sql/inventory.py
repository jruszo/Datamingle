import datetime
import logging

from django.db import transaction
from django.db import close_old_connections
from django.utils import timezone

from common.config import SysConfig
from common.task_queue import delete_schedule, schedule, task_info
from sql.engines import get_engine
from sql.models import Config, Instance, TaskSchedule

logger = logging.getLogger("default")

INVENTORY_REFRESH_INTERVAL_DEFAULT = "24h"
INVENTORY_REFRESH_INTERVAL_CHOICES = ("1h", "6h", "12h", "24h")
INVENTORY_REFRESH_INTERVAL_DELTAS = {
    "1h": datetime.timedelta(hours=1),
    "6h": datetime.timedelta(hours=6),
    "12h": datetime.timedelta(hours=12),
    "24h": datetime.timedelta(hours=24),
}
INVENTORY_REFRESH_SCHEDULE_NAME = "inventory-refresh-global"
INVENTORY_REFRESH_SCHEDULE_LOCK_NAME = "inventory_refresh_schedule_lock"


def get_inventory_refresh_interval():
    interval = SysConfig().get(
        "inventory_refresh_interval", INVENTORY_REFRESH_INTERVAL_DEFAULT
    )
    if interval not in INVENTORY_REFRESH_INTERVAL_CHOICES:
        return INVENTORY_REFRESH_INTERVAL_DEFAULT
    return interval


def calculate_next_inventory_refresh_run(from_time=None):
    current = from_time or timezone.now()
    return current + INVENTORY_REFRESH_INTERVAL_DELTAS[get_inventory_refresh_interval()]


def get_inventory_refresh_schedule():
    return task_info(INVENTORY_REFRESH_SCHEDULE_NAME)


def schedule_inventory_refresh(run_at=None):
    next_run = run_at or calculate_next_inventory_refresh_run()
    with transaction.atomic():
        Config.objects.update_or_create(
            item=INVENTORY_REFRESH_SCHEDULE_LOCK_NAME,
            defaults={
                "value": "1",
                "description": "Internal lock for the inventory refresh scheduler.",
            },
        )
        Config.objects.select_for_update().get(
            item=INVENTORY_REFRESH_SCHEDULE_LOCK_NAME
        )
        delete_schedule(INVENTORY_REFRESH_SCHEDULE_NAME)
        schedule(
            "sql.inventory.refresh_inventory_snapshots",
            hook="sql.inventory.inventory_refresh_task_callback",
            name=INVENTORY_REFRESH_SCHEDULE_NAME,
            schedule_type="O",
            next_run=next_run,
            repeats=1,
            timeout=None,
        )
    return get_inventory_refresh_schedule()


def ensure_inventory_refresh_schedule(force=False):
    existing_schedule = get_inventory_refresh_schedule()
    if (
        existing_schedule
        and existing_schedule.backend == TaskSchedule.BACKEND_CELERY
        and not force
    ):
        return existing_schedule
    return schedule_inventory_refresh()


def _format_inventory_version(value):
    if isinstance(value, (list, tuple)):
        parts = []
        for part in value:
            if part is None:
                continue
            normalized = str(part).strip()
            if normalized:
                parts.append(normalized)
        return ".".join(parts)
    if value in (None, ""):
        return ""
    return str(value).strip()


def _normalize_inventory_details(details):
    payload = details or {}
    return {
        "hostname": str(payload.get("hostname") or "").strip(),
        "version": _format_inventory_version(payload.get("version")),
    }


def collect_inventory_snapshot(instance):
    engine = get_engine(instance=instance)
    test_result = engine.test_connection()
    if getattr(test_result, "error", ""):
        raise RuntimeError(test_result.error)
    return _normalize_inventory_details(engine.get_inventory_details())


def refresh_instance_inventory_snapshot(instance, now=None):
    attempt_time = now or timezone.now()
    update_fields = ["inventory_last_attempt_at", "inventory_status"]
    instance.inventory_last_attempt_at = attempt_time

    try:
        details = collect_inventory_snapshot(instance)
    except Exception:
        logger.exception(
            "Failed to refresh inventory snapshot for instance_id=%s", instance.id
        )
        instance.inventory_status = (
            Instance.INVENTORY_STATUS_STALE
            if instance.inventory_last_success_at
            else Instance.INVENTORY_STATUS_FAILED
        )
        instance.save(update_fields=update_fields)
        return {"success": False, "status": instance.inventory_status}

    instance.inventory_status = Instance.INVENTORY_STATUS_OK
    instance.inventory_last_success_at = attempt_time
    instance.inventory_detected_hostname = details["hostname"]
    instance.inventory_detected_version = details["version"]
    update_fields.extend(
        [
            "inventory_last_success_at",
            "inventory_detected_hostname",
            "inventory_detected_version",
        ]
    )
    instance.save(update_fields=update_fields)
    return {
        "success": True,
        "status": instance.inventory_status,
        "details": details,
    }


def refresh_inventory_snapshots():
    close_old_connections()
    summary = {"total": 0, "ok": 0, "stale": 0, "failed": 0}
    status_key_map = {
        Instance.INVENTORY_STATUS_OK: "ok",
        Instance.INVENTORY_STATUS_STALE: "stale",
        Instance.INVENTORY_STATUS_FAILED: "failed",
    }
    try:
        for instance in Instance.objects.order_by("id").iterator():
            summary["total"] += 1
            result = refresh_instance_inventory_snapshot(instance=instance)
            summary_key = status_key_map.get(result.get("status"), "failed")
            summary[summary_key] += 1
    finally:
        close_old_connections()
    return summary


def inventory_refresh_task_callback(task_result):
    try:
        ensure_inventory_refresh_schedule(force=True)
    except Exception:
        logger.exception("Failed to re-arm the inventory refresh schedule.")
