from datetime import datetime, timezone as datetime_timezone

from django.conf import settings
from django.utils import timezone


def agent_utc_now():
    now = datetime.now(datetime_timezone.utc)
    if settings.USE_TZ:
        return now
    return now.replace(tzinfo=None)


def agent_datetime_to_utc_iso(value):
    if value is None:
        return None
    if timezone.is_naive(value):
        value = value.replace(tzinfo=datetime_timezone.utc)
    else:
        value = value.astimezone(datetime_timezone.utc)
    return value.isoformat().replace("+00:00", "Z")
