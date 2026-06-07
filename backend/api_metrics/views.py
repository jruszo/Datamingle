import logging
import json
import re
import uuid
from io import BytesIO
from datetime import datetime, timezone
from urllib.parse import quote

import requests
from PIL import Image, ImageOps, UnidentifiedImageError
from django.conf import settings
from django.core.files.base import ContentFile
from django.db import transaction
from django.http import FileResponse, QueryDict
from django.shortcuts import get_object_or_404
from django.utils import timezone as django_timezone
from rest_framework import permissions, status, views
from rest_framework.exceptions import PermissionDenied, ValidationError
from rest_framework.parsers import FormParser, MultiPartParser
from rest_framework.response import Response
from rest_framework.throttling import ScopedRateThrottle

from api_core.response import success_response
from common.utils.openai import get_openai_config

logger = logging.getLogger(__name__)

LABEL_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
PROM_DURATION_RE = re.compile(
    r"^\s*(?P<value>\d+(?:\.\d+)?)(?P<unit>ms|s|m|h|d|w|y)?\s*$"
)

DEFAULT_TIMEOUT_SECONDS = 20
DEFAULT_MAX_QUERY_LENGTH = 8192
DEFAULT_MAX_RANGE_SECONDS = 30 * 24 * 60 * 60
DEFAULT_MIN_STEP_SECONDS = 15
DEFAULT_MAX_RANGE_POINTS = 11000
DEFAULT_MAX_MATCHERS = 32
DEFAULT_METRIC_NAME_LIMIT = 300
DEFAULT_MAX_METRIC_NAME_LIMIT = 1000
AI_METRIC_CONTEXT_LIMIT = 300
AI_LABEL_CONTEXT_LIMIT = 200
AI_METADATA_CONTEXT_LIMIT = 100
DASHBOARD_HISTORY_LIMIT = 50
DASHBOARD_ICON_MAX_BYTES = 2 * 1024 * 1024
DASHBOARD_ICON_SIZE = 256
DASHBOARD_ICON_MAX_PIXELS = 16_000_000
DASHBOARD_ICON_FORMATS = {"JPEG", "PNG", "WEBP", "GIF"}

PROMETHEUS_READ_BASE_PATH = "/prometheus/api/v1"
TENANT_PARAM_NAMES = {
    "org_id",
    "organization_id",
    "tenant",
    "tenant_id",
    "x-scope-orgid",
    "x_scope_orgid",
}


def _setting_int(name, default):
    try:
        value = int(getattr(settings, name, default) or default)
    except (TypeError, ValueError):
        return default
    return max(value, 1)


def _cortex_url(path):
    base_url = getattr(settings, "DATAMINGLE_CORTEX_URL", "http://cortex:9009")
    return f"{base_url.rstrip('/')}{PROMETHEUS_READ_BASE_PATH}{path}"


def _cortex_json(path, organization_id, params=None):
    response = requests.get(
        _cortex_url(path),
        params=params,
        headers={
            "Accept": "application/json",
            "X-Scope-OrgID": organization_id,
        },
        timeout=_setting_int(
            "DATAMINGLE_METRICS_PROXY_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS
        ),
    )
    response.raise_for_status()
    payload = response.json()
    if payload.get("status") != "success":
        raise ValueError(payload.get("error") or "Metrics backend request failed.")
    return payload.get("data")


def _promql_ai_context(organization_id, search_text=""):
    names = _cortex_json("/label/__name__/values", organization_id)
    labels = _cortex_json("/labels", organization_id)
    metadata = _cortex_json("/metadata", organization_id)
    if not isinstance(names, list) or not isinstance(labels, list):
        raise ValueError("Metrics backend returned invalid discovery data.")
    if not isinstance(metadata, dict):
        metadata = {}

    search_tokens = {
        token
        for token in re.findall(r"[a-zA-Z_:][a-zA-Z0-9_:]*", search_text.lower())
        if len(token) >= 2
    }

    def metric_rank(name):
        lowered = name.lower()
        matches = [token for token in search_tokens if token in lowered]
        return (
            -len(matches),
            -max((len(token) for token in matches), default=0),
            lowered,
        )

    metric_names = sorted(
        {str(name)[:255] for name in names if str(name).strip()},
        key=metric_rank,
    )[:AI_METRIC_CONTEXT_LIMIT]
    label_names = sorted(
        {
            str(label)[:128]
            for label in labels
            if str(label).strip() and str(label) != "__name__"
        },
        key=str.lower,
    )[:AI_LABEL_CONTEXT_LIMIT]
    metric_help = []
    for metric in metric_names:
        entries = metadata.get(metric) or []
        if not isinstance(entries, list):
            continue
        for entry in entries[:2]:
            if not isinstance(entry, dict):
                continue
            metric_help.append(
                {
                    "metric": metric,
                    "type": str(entry.get("type") or "")[:40],
                    "unit": str(entry.get("unit") or "")[:40],
                    "help": str(entry.get("help") or "")[:500],
                }
            )
            if len(metric_help) >= AI_METADATA_CONTEXT_LIMIT:
                break
        if len(metric_help) >= AI_METADATA_CONTEXT_LIMIT:
            break

    return {
        "metric_names": metric_names,
        "label_names": label_names,
        "metric_help": metric_help,
    }


def _request_param_lists(request):
    if request.method == "GET":
        return list(request.query_params.lists())

    data = request.data
    if isinstance(data, QueryDict):
        return list(data.lists())
    if isinstance(data, dict):
        return [
            (key, value if isinstance(value, list) else [value])
            for key, value in data.items()
        ]
    return []


def _request_param_pairs(request):
    pairs = []
    for key, values in _request_param_lists(request):
        if key.lower() in TENANT_PARAM_NAMES:
            continue
        if values == []:
            pairs.append((key, ""))
        for value in values:
            pairs.append((key, "" if value is None else str(value)))
    return pairs


def _first_param(request, name):
    for key, values in _request_param_lists(request):
        if key == name and values:
            return "" if values[0] is None else str(values[0])
    return ""


def _all_params(request, name):
    values = []
    for key, param_values in _request_param_lists(request):
        if key == name:
            values.extend("" if value is None else str(value) for value in param_values)
    return values


def _parse_time(value, field_name):
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        pass

    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValidationError(
            {field_name: "Must be a Unix timestamp or RFC3339 time."}
        ) from exc

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _parse_step_seconds(value):
    if value == "":
        return None

    match = PROM_DURATION_RE.match(value)
    if not match:
        raise ValidationError(
            {"step": "Must be a duration such as 15s, 1m, 1h, or a number of seconds."}
        )

    number = float(match.group("value"))
    unit = match.group("unit") or "s"
    multipliers = {
        "ms": 0.001,
        "s": 1,
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60,
        "y": 365 * 24 * 60 * 60,
    }
    return number * multipliers[unit]


class CortexMetricsProxyView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]
    throttle_classes = [ScopedRateThrottle]
    throttle_scope = "metrics_metadata"
    cortex_path = ""
    allowed_methods = ("GET",)

    def get(self, request, *args, **kwargs):
        return self.proxy(request, *args, **kwargs)

    def post(self, request, *args, **kwargs):
        return self.proxy(request, *args, **kwargs)

    def get_cortex_path(self, **kwargs):
        return self.cortex_path

    def proxy(self, request, *args, **kwargs):
        if request.method not in self.allowed_methods:
            return Response(
                {"status": "error", "error": f"{request.method} is not allowed."},
                status=status.HTTP_405_METHOD_NOT_ALLOWED,
            )

        org_id = self.get_org_id(request)
        self.validate_request(request, **kwargs)

        request_kwargs = {
            "headers": {
                "Accept": "application/json",
                "X-Scope-OrgID": org_id,
            },
            "timeout": _setting_int(
                "DATAMINGLE_METRICS_PROXY_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS
            ),
        }
        param_pairs = self.get_request_param_pairs(request)
        if request.method == "GET":
            request_kwargs["params"] = param_pairs
        else:
            request_kwargs["data"] = param_pairs

        try:
            response = requests.request(
                request.method,
                _cortex_url(self.get_cortex_path(**kwargs)),
                **request_kwargs,
            )
        except requests.RequestException as exc:
            logger.warning("Cortex metrics proxy request failed.", exc_info=True)
            return Response(
                {"status": "error", "error": "Metrics backend is unavailable."},
                status=status.HTTP_502_BAD_GATEWAY,
            )

        return self.build_response(response, request=request)

    def build_response(self, response, request=None):
        try:
            payload = response.json()
        except ValueError:
            payload = {
                "status": "error",
                "error": response.text[:1000]
                or "Metrics backend returned a non-JSON response.",
            }
        if (
            response.status_code >= 500
            and isinstance(payload, dict)
            and "bucket index is too old" in str(payload.get("error") or "").lower()
        ):
            logger.warning("Cortex bucket index is stale.")
            return Response(
                {
                    "status": "error",
                    "error": (
                        "Metrics storage is refreshing after a restart. "
                        "Retry the query shortly."
                    ),
                },
                status=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        return Response(payload, status=response.status_code)

    def get_request_param_pairs(self, request):
        return _request_param_pairs(request)

    def get_org_id(self, request):
        auth = request.auth if isinstance(request.auth, dict) else {}
        org_id = str(
            auth.get("org_id")
            or getattr(request.user, "organization_id", "")
            or getattr(request.user, "workos_claims", {}).get("org_id", "")
        ).strip()
        if not org_id:
            raise PermissionDenied(
                "Authenticated metrics requests require a WorkOS org_id."
            )
        return org_id

    def validate_request(self, request, **kwargs):
        return None

    def validate_query(self, request):
        query = _first_param(request, "query").strip()
        if not query:
            raise ValidationError({"query": "This parameter is required."})
        max_query_length = _setting_int(
            "DATAMINGLE_METRICS_MAX_QUERY_LENGTH",
            DEFAULT_MAX_QUERY_LENGTH,
        )
        if len(query) > max_query_length:
            raise ValidationError(
                {"query": f"Query must be {max_query_length} characters or fewer."}
            )

    def validate_matchers(self, request):
        matchers = _all_params(request, "match[]")
        if not matchers:
            return
        max_matchers = _setting_int(
            "DATAMINGLE_METRICS_MAX_MATCHERS", DEFAULT_MAX_MATCHERS
        )
        if len(matchers) > max_matchers:
            raise ValidationError(
                {"match[]": f"At most {max_matchers} matchers are allowed."}
            )
        max_query_length = _setting_int(
            "DATAMINGLE_METRICS_MAX_QUERY_LENGTH",
            DEFAULT_MAX_QUERY_LENGTH,
        )
        for matcher in matchers:
            if len(matcher) > max_query_length:
                raise ValidationError(
                    {
                        "match[]": f"Each matcher must be {max_query_length} characters or fewer."
                    }
                )

    def validate_timerange(self, request):
        start = _parse_time(_first_param(request, "start"), "start")
        end = _parse_time(_first_param(request, "end"), "end")
        if start is None or end is None:
            return
        if end <= start:
            raise ValidationError({"end": "End time must be after start time."})
        max_range = _setting_int(
            "DATAMINGLE_METRICS_MAX_RANGE_SECONDS",
            DEFAULT_MAX_RANGE_SECONDS,
        )
        if end - start > max_range:
            raise ValidationError(
                {"end": f"Range queries are limited to {max_range} seconds."}
            )


class MetricsLabelsView(CortexMetricsProxyView):
    cortex_path = "/labels"


class MetricsNamesView(CortexMetricsProxyView):
    cortex_path = "/label/__name__/values"

    def validate_request(self, request, **kwargs):
        search = _first_param(request, "search").strip()
        if len(search) > 256:
            raise ValidationError({"search": "Search must be 256 characters or fewer."})
        self.get_limit(request)

    def get_limit(self, request):
        raw_limit = _first_param(request, "limit").strip()
        if not raw_limit:
            return DEFAULT_METRIC_NAME_LIMIT
        try:
            limit = int(raw_limit)
        except ValueError as exc:
            raise ValidationError({"limit": "Must be an integer."}) from exc
        if limit < 1 or limit > DEFAULT_MAX_METRIC_NAME_LIMIT:
            raise ValidationError(
                {"limit": f"Must be between 1 and {DEFAULT_MAX_METRIC_NAME_LIMIT}."}
            )
        return limit

    def get_request_param_pairs(self, request):
        return [
            (key, value)
            for key, value in _request_param_pairs(request)
            if key not in {"search", "limit"}
        ]

    def build_response(self, response, request=None):
        if response.status_code < 200 or response.status_code >= 300:
            return super().build_response(response, request=request)

        try:
            payload = response.json()
        except ValueError:
            return super().build_response(response, request=request)

        names = payload.get("data") or []
        if not isinstance(names, list):
            return Response(payload, status=response.status_code)

        request_search = (
            _first_param(request, "search").strip().lower() if request else ""
        )
        limit = self.get_limit(request) if request else DEFAULT_METRIC_NAME_LIMIT
        filtered_names = []
        for name in names:
            metric_name = str(name)
            if request_search and request_search not in metric_name.lower():
                continue
            filtered_names.append(metric_name)

        if request_search:
            filtered_names.sort(
                key=lambda name: (
                    name.lower() != request_search,
                    not name.lower().startswith(request_search),
                    name.lower().find(request_search),
                    name.lower(),
                )
            )
        else:
            filtered_names.sort(key=str.lower)

        payload["data"] = filtered_names[:limit]
        return Response(payload, status=response.status_code)


class MetricsLabelValuesView(CortexMetricsProxyView):
    def validate_request(self, request, **kwargs):
        self.validate_matchers(request)

    def get_cortex_path(self, **kwargs):
        label_name = kwargs.get("label_name", "")
        if not LABEL_NAME_RE.match(label_name):
            raise ValidationError({"label": "Invalid Prometheus label name."})
        return f"/label/{quote(label_name, safe='')}/values"


class MetricsSeriesView(CortexMetricsProxyView):
    cortex_path = "/series"
    allowed_methods = ("GET", "POST")

    def validate_request(self, request, **kwargs):
        self.validate_matchers(request)
        self.validate_timerange(request)


class MetricsMetadataView(CortexMetricsProxyView):
    cortex_path = "/metadata"

    def validate_request(self, request, **kwargs):
        metric = _first_param(request, "metric").strip()
        if metric and not LABEL_NAME_RE.match(metric):
            raise ValidationError({"metric": "Invalid Prometheus metric name."})
        limit = _first_param(request, "limit").strip()
        if limit:
            try:
                limit_value = int(limit)
            except ValueError as exc:
                raise ValidationError({"limit": "Must be an integer."}) from exc
            if limit_value < 1 or limit_value > 10000:
                raise ValidationError({"limit": "Must be between 1 and 10000."})


class MetricsQueryView(CortexMetricsProxyView):
    cortex_path = "/query"
    allowed_methods = ("GET", "POST")
    throttle_scope = "metrics_query"

    def validate_request(self, request, **kwargs):
        self.validate_query(request)


class MetricsQueryRangeView(CortexMetricsProxyView):
    cortex_path = "/query_range"
    allowed_methods = ("GET", "POST")
    throttle_scope = "metrics_query_range"

    def validate_request(self, request, **kwargs):
        self.validate_query(request)
        start = _parse_time(_first_param(request, "start"), "start")
        end = _parse_time(_first_param(request, "end"), "end")
        step = _parse_step_seconds(_first_param(request, "step"))
        if start is None:
            raise ValidationError({"start": "This parameter is required."})
        if end is None:
            raise ValidationError({"end": "This parameter is required."})
        if step is None:
            raise ValidationError({"step": "This parameter is required."})

        self.validate_timerange(request)

        min_step = _setting_int(
            "DATAMINGLE_METRICS_MIN_STEP_SECONDS",
            DEFAULT_MIN_STEP_SECONDS,
        )
        if step < min_step:
            raise ValidationError(
                {"step": f"Step must be at least {min_step} seconds."}
            )

        max_points = _setting_int(
            "DATAMINGLE_METRICS_MAX_RANGE_POINTS",
            DEFAULT_MAX_RANGE_POINTS,
        )
        if (end - start) / step > max_points:
            raise ValidationError(
                {
                    "step": f"Range query result is limited to {max_points} points per series."
                }
            )


class MetricsFormatQueryView(CortexMetricsProxyView):
    cortex_path = "/format_query"
    allowed_methods = ("GET", "POST")
    throttle_scope = "metrics_query"

    def validate_request(self, request, **kwargs):
        self.validate_query(request)


class MetricsParseQueryView(CortexMetricsProxyView):
    cortex_path = "/parse_query"
    allowed_methods = ("GET", "POST")
    throttle_scope = "metrics_query"

    def validate_request(self, request, **kwargs):
        self.validate_query(request)


def _request_organization_id(request):
    auth = request.auth if isinstance(request.auth, dict) else {}
    organization_id = str(
        auth.get("org_id")
        or getattr(request.user, "organization_id", "")
        or getattr(request.user, "workos_claims", {}).get("org_id", "")
    ).strip()
    if not organization_id:
        raise PermissionDenied(
            "Authenticated dashboard requests require a WorkOS org_id."
        )
    return organization_id


def _dashboard_serializer():
    from api_metrics.serializers import MetricsDashboardSerializer

    return MetricsDashboardSerializer


def _dashboard_revision_data(dashboard):
    return {
        "revision": dashboard.revision,
        "name": dashboard.name,
        "description": dashboard.description,
        "time_range_mode": dashboard.time_range_mode,
        "time_range_seconds": dashboard.time_range_seconds,
        "time_range_start": dashboard.time_range_start,
        "time_range_end": dashboard.time_range_end,
        "refresh_interval_seconds": dashboard.refresh_interval_seconds,
        "variables": dashboard.variables,
        "panels": dashboard.panels,
    }


def _snapshot_dashboard(
    dashboard,
    saved_by,
    *,
    saved_at=None,
    restored_from_revision=None,
):
    from api_metrics.models import MetricsDashboardRevision

    snapshot, _ = MetricsDashboardRevision.objects.get_or_create(
        dashboard=dashboard,
        revision=dashboard.revision,
        defaults={
            **_dashboard_revision_data(dashboard),
            "saved_by": saved_by,
            "saved_at": saved_at or django_timezone.now(),
            "restored_from_revision": restored_from_revision,
        },
    )
    stale_ids = list(
        MetricsDashboardRevision.objects.filter(dashboard=dashboard)
        .order_by("-revision")
        .values_list("id", flat=True)[DASHBOARD_HISTORY_LIMIT:]
    )
    if stale_ids:
        MetricsDashboardRevision.objects.filter(id__in=stale_ids).delete()
    return snapshot


class MetricsDashboardListCreateView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request):
        from api_metrics.models import MetricsDashboard

        organization_id = _request_organization_id(request)
        dashboards = MetricsDashboard.objects.filter(
            organization_id=organization_id
        ).select_related("created_by")
        return success_response(
            data=_dashboard_serializer()(dashboards, many=True).data
        )

    def post(self, request):
        serializer = _dashboard_serializer()(data=request.data)
        serializer.is_valid(raise_exception=True)
        with transaction.atomic():
            dashboard = serializer.save(
                organization_id=_request_organization_id(request),
                created_by=request.user,
            )
            _snapshot_dashboard(dashboard, request.user)
        return success_response(
            data=_dashboard_serializer()(dashboard).data,
            detail="Dashboard created.",
            status_code=status.HTTP_201_CREATED,
        )


class MetricsDashboardDetailView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get_object(self, request, dashboard_id, for_update=False):
        from api_metrics.models import MetricsDashboard

        queryset = MetricsDashboard.objects.select_related("created_by")
        if for_update:
            queryset = queryset.select_for_update()
        return get_object_or_404(
            queryset,
            pk=dashboard_id,
            organization_id=_request_organization_id(request),
        )

    def get(self, request, dashboard_id):
        dashboard = self.get_object(request, dashboard_id)
        return success_response(data=_dashboard_serializer()(dashboard).data)

    def patch(self, request, dashboard_id):
        expected_revision = request.data.get("expected_revision")
        if isinstance(expected_revision, bool) or not isinstance(
            expected_revision, int
        ):
            raise ValidationError(
                {"expected_revision": "This field is required and must be an integer."}
            )

        payload = request.data.copy()
        payload.pop("expected_revision", None)
        with transaction.atomic():
            dashboard = self.get_object(request, dashboard_id, for_update=True)
            if dashboard.revision != expected_revision:
                return success_response(
                    data=_dashboard_serializer()(dashboard).data,
                    detail="Dashboard was changed by another user.",
                    status_code=status.HTTP_409_CONFLICT,
                )
            if not dashboard.history.exists():
                _snapshot_dashboard(
                    dashboard,
                    None,
                    saved_at=dashboard.update_time,
                )
            serializer = _dashboard_serializer()(
                dashboard,
                data=payload,
                partial=True,
            )
            serializer.is_valid(raise_exception=True)
            dashboard = serializer.save(revision=dashboard.revision + 1)
            _snapshot_dashboard(dashboard, request.user)

        return success_response(
            data=_dashboard_serializer()(dashboard).data,
            detail="Dashboard saved.",
        )

    def delete(self, request, dashboard_id):
        dashboard = self.get_object(request, dashboard_id)
        if dashboard.icon:
            dashboard.icon.delete(save=False)
        dashboard.delete()
        return success_response(detail="Dashboard deleted.")


class MetricsDashboardIconView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]
    parser_classes = [MultiPartParser, FormParser]

    def get_object(self, request, dashboard_id):
        from api_metrics.models import MetricsDashboard

        return get_object_or_404(
            MetricsDashboard,
            pk=dashboard_id,
            organization_id=_request_organization_id(request),
        )

    def get(self, request, dashboard_id):
        dashboard = self.get_object(request, dashboard_id)
        if not dashboard.icon:
            return Response(status=status.HTTP_404_NOT_FOUND)
        return FileResponse(
            dashboard.icon.open("rb"),
            content_type="image/png",
            filename=f"dashboard-{dashboard.id}-icon.png",
        )

    def post(self, request, dashboard_id):
        dashboard = self.get_object(request, dashboard_id)
        uploaded = request.FILES.get("icon")
        if uploaded is None:
            raise ValidationError({"icon": "Select an image to upload."})
        if uploaded.size > DASHBOARD_ICON_MAX_BYTES:
            raise ValidationError({"icon": "Dashboard icons must be 2 MB or smaller."})

        try:
            image = Image.open(uploaded)
            if image.format not in DASHBOARD_ICON_FORMATS:
                raise ValidationError({"icon": "Use a PNG, JPEG, WebP, or GIF image."})
            if image.width * image.height > DASHBOARD_ICON_MAX_PIXELS:
                raise ValidationError(
                    {"icon": "Dashboard icon dimensions are too large."}
                )
            image = ImageOps.exif_transpose(image)
            image.seek(0)
            image = image.convert("RGBA")
            image.thumbnail(
                (DASHBOARD_ICON_SIZE, DASHBOARD_ICON_SIZE),
                Image.Resampling.LANCZOS,
            )
            normalized = BytesIO()
            image.save(normalized, format="PNG", optimize=True)
        except (UnidentifiedImageError, OSError, ValueError) as exc:
            raise ValidationError(
                {"icon": "The uploaded file is not a valid image."}
            ) from exc

        old_name = dashboard.icon.name if dashboard.icon else ""
        dashboard.icon.save(
            f"{uuid.uuid4().hex}.png",
            ContentFile(normalized.getvalue()),
            save=True,
        )
        if old_name and old_name != dashboard.icon.name:
            dashboard.icon.storage.delete(old_name)

        return success_response(
            data=_dashboard_serializer()(dashboard).data,
            detail="Dashboard icon updated.",
        )

    def delete(self, request, dashboard_id):
        dashboard = self.get_object(request, dashboard_id)
        if dashboard.icon:
            dashboard.icon.delete(save=True)
        return success_response(
            data=_dashboard_serializer()(dashboard).data,
            detail="Dashboard icon removed.",
        )


class MetricsDashboardRevisionListView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, dashboard_id):
        from api_metrics.models import MetricsDashboard
        from api_metrics.serializers import MetricsDashboardRevisionSummarySerializer

        dashboard = get_object_or_404(
            MetricsDashboard,
            pk=dashboard_id,
            organization_id=_request_organization_id(request),
        )
        revisions = dashboard.history.select_related("saved_by").all()
        return success_response(
            data=MetricsDashboardRevisionSummarySerializer(
                revisions,
                many=True,
            ).data
        )


class MetricsDashboardRevisionDetailView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get_object(self, request, dashboard_id, revision):
        from api_metrics.models import MetricsDashboardRevision

        return get_object_or_404(
            MetricsDashboardRevision.objects.select_related(
                "dashboard",
                "saved_by",
            ),
            dashboard_id=dashboard_id,
            dashboard__organization_id=_request_organization_id(request),
            revision=revision,
        )

    def get(self, request, dashboard_id, revision):
        from api_metrics.serializers import MetricsDashboardRevisionSerializer

        snapshot = self.get_object(request, dashboard_id, revision)
        return success_response(data=MetricsDashboardRevisionSerializer(snapshot).data)


class MetricsDashboardRevisionRestoreView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request, dashboard_id, revision):
        expected_revision = request.data.get("expected_revision")
        if isinstance(expected_revision, bool) or not isinstance(
            expected_revision, int
        ):
            raise ValidationError(
                {"expected_revision": "This field is required and must be an integer."}
            )

        from api_metrics.models import MetricsDashboard, MetricsDashboardRevision

        organization_id = _request_organization_id(request)
        with transaction.atomic():
            dashboard = get_object_or_404(
                MetricsDashboard.objects.select_for_update().select_related(
                    "created_by"
                ),
                pk=dashboard_id,
                organization_id=organization_id,
            )
            if dashboard.revision != expected_revision:
                return success_response(
                    data=_dashboard_serializer()(dashboard).data,
                    detail="Dashboard was changed by another user.",
                    status_code=status.HTTP_409_CONFLICT,
                )
            snapshot = get_object_or_404(
                MetricsDashboardRevision,
                dashboard=dashboard,
                revision=revision,
            )
            for field, value in _dashboard_revision_data(snapshot).items():
                if field != "revision":
                    setattr(dashboard, field, value)
            dashboard.revision += 1
            dashboard.save(
                update_fields=(
                    "name",
                    "description",
                    "time_range_mode",
                    "time_range_seconds",
                    "time_range_start",
                    "time_range_end",
                    "refresh_interval_seconds",
                    "variables",
                    "panels",
                    "revision",
                    "update_time",
                )
            )
            _snapshot_dashboard(
                dashboard,
                request.user,
                restored_from_revision=snapshot.revision,
            )

        return success_response(
            data=_dashboard_serializer()(dashboard).data,
            detail=f"Dashboard restored from revision {snapshot.revision}.",
        )


class MetricsAIAssistantAvailabilityView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request):
        return success_response(
            data={"available": bool(get_openai_config()["api_key"])}
        )


class MetricsAIAssistantView(views.APIView):
    permission_classes = [permissions.IsAuthenticated]
    throttle_classes = [ScopedRateThrottle]
    throttle_scope = "metrics_ai"

    def post(self, request):
        prompt = str(request.data.get("prompt") or "").strip()
        if not prompt:
            raise ValidationError({"prompt": "This field is required."})
        if len(prompt) > 2000:
            raise ValidationError({"prompt": "Must be 2000 characters or fewer."})

        config = get_openai_config()
        api_key = config["api_key"]
        if not api_key:
            return success_response(
                data={"available": False},
                detail="PromQL assistant is not configured.",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

        try:
            context = _promql_ai_context(
                _request_organization_id(request),
                prompt,
            )
        except (requests.RequestException, ValueError):
            logger.warning("Failed to load PromQL assistant context.", exc_info=True)
            return success_response(
                detail="Metrics context is temporarily unavailable.",
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )
        from openai import OpenAI

        client = OpenAI(
            api_key=api_key,
            base_url=config["base_url"] or None,
            timeout=20,
        )
        messages = [
            {
                "role": "system",
                "content": (
                    "You are a PromQL assistant. Return JSON only with keys "
                    "query, explanation, assumptions, warnings. The query must be "
                    "valid PromQL and use only metrics or labels present in the "
                    "provided context unless the user explicitly supplies another. "
                    "Never invent observed metric values."
                ),
            },
            {
                "role": "user",
                "content": f"Request: {prompt}\nContext: {json.dumps(context)}",
            },
        ]
        try:
            completion = client.chat.completions.create(
                model=config["model"],
                messages=messages,
                temperature=0.1,
                response_format={"type": "json_object"},
            )
            content = completion.choices[0].message.content or "{}"
            payload = json.loads(content)
        except Exception:
            logger.warning("PromQL assistant request failed.", exc_info=True)
            return success_response(
                detail="PromQL assistant is unavailable.",
                status_code=status.HTTP_502_BAD_GATEWAY,
            )
        query = str(payload.get("query") or "").strip()
        if not query or len(query) > DEFAULT_MAX_QUERY_LENGTH:
            return success_response(
                detail="PromQL assistant returned an invalid suggestion.",
                status_code=status.HTTP_502_BAD_GATEWAY,
            )
        return success_response(
            data={
                "available": True,
                "query": query,
                "explanation": str(payload.get("explanation") or "")[:4000],
                "assumptions": [
                    str(item)[:500] for item in (payload.get("assumptions") or [])[:10]
                ],
                "warnings": [
                    str(item)[:500] for item in (payload.get("warnings") or [])[:10]
                ],
            }
        )
