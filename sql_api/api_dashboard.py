import logging
from datetime import date, timedelta

from django.contrib.auth.decorators import permission_required
from django.utils.decorators import method_decorator
from drf_spectacular.utils import OpenApiParameter, OpenApiTypes, extend_schema
from rest_framework import permissions, serializers, views

from common.utils.chart_dao import ChartDao
from sql.models import Instance, QueryPrivilegesApply, SqlWorkflow, Users

from .response import success_response

logger = logging.getLogger("default")


class DashboardQuerySerializer(serializers.Serializer):
    start_date = serializers.DateField(required=False)
    end_date = serializers.DateField(required=False)

    def validate(self, attrs):
        start_date = attrs.get("start_date")
        end_date = attrs.get("end_date")
        if start_date and end_date and start_date > end_date:
            raise serializers.ValidationError(
                {"errors": "start_date cannot be greater than end_date."}
            )
        return attrs


class DashboardResponseSerializer(serializers.Serializer):
    start_date = serializers.DateField()
    end_date = serializers.DateField()
    summary = serializers.JSONField()
    charts = serializers.JSONField()


def _to_int(value):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _as_label(value):
    if value is None:
        return "Unknown"
    return str(value)


def _labels_and_values(rows):
    labels = []
    values = []
    for row in rows:
        if len(row) < 2:
            continue
        labels.append(_as_label(row[0]))
        values.append(_to_int(row[1]))
    return labels, values


def _date_values(rows, day_labels):
    value_map = {}
    for row in rows:
        if len(row) < 2:
            continue
        value_map[_as_label(row[0])] = _to_int(row[1])
    return [value_map.get(day, 0) for day in day_labels]


def _stacked_series(rows):
    categories = []
    category_index = {}
    series_names = []
    series_index = {}
    series_values = []

    for row in rows:
        if len(row) < 3:
            continue
        category = _as_label(row[0])
        series_name = _as_label(row[1])
        count = _to_int(row[2])

        if category not in category_index:
            category_index[category] = len(categories)
            categories.append(category)
            for values in series_values:
                values.append(0)

        if series_name not in series_index:
            series_index[series_name] = len(series_names)
            series_names.append(series_name)
            series_values.append([0] * len(categories))

        values = series_values[series_index[series_name]]
        values[category_index[category]] = count

    series = []
    for index, name in enumerate(series_names):
        series.append({"name": name, "values": series_values[index]})

    return {"categories": categories, "series": series}


class DashboardOverview(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Dashboard Overview",
        parameters=[
            OpenApiParameter(
                name="start_date",
                type=OpenApiTypes.DATE,
                location=OpenApiParameter.QUERY,
                description="Start date (YYYY-MM-DD). Defaults to 6 days before today.",
            ),
            OpenApiParameter(
                name="end_date",
                type=OpenApiTypes.DATE,
                location=OpenApiParameter.QUERY,
                description="End date (YYYY-MM-DD). Defaults to today.",
            ),
        ],
        responses={200: DashboardResponseSerializer},
        description="Return dashboard summary and chart series data for SPA rendering.",
    )
    @method_decorator(permission_required("sql.menu_dashboard", raise_exception=True))
    def get(self, request):
        serializer = DashboardQuerySerializer(data=request.query_params)
        serializer.is_valid(raise_exception=True)
        start_date, end_date = self._resolve_date_range(serializer.validated_data)
        payload = self._build_payload(start_date, end_date)
        return success_response(data=payload)

    @staticmethod
    def _resolve_date_range(validated_data):
        today = date.today()
        end_date = validated_data.get("end_date") or today
        start_date = validated_data.get("start_date") or (end_date - timedelta(days=6))
        return start_date, end_date

    @staticmethod
    def _safe_rows(fetcher, query_name):
        try:
            return fetcher().get("rows", [])
        except Exception as exc:
            logger.warning("Dashboard query skipped (%s): %s", query_name, str(exc))
            return []

    def _build_payload(self, start_date, end_date):
        chart_dao = ChartDao()
        day_labels = chart_dao.get_date_list(start_date, end_date)

        query_start = start_date.strftime("%Y-%m-%d")
        query_end = (end_date + timedelta(days=1)).strftime("%Y-%m-%d")

        workflow_by_date_rows = self._safe_rows(
            lambda: chart_dao.workflow_by_date(query_start, query_end),
            "workflow_by_date",
        )
        query_scanned_rows = self._safe_rows(
            lambda: chart_dao.querylog_effect_row_by_date(query_start, query_end),
            "querylog_effect_row_by_date",
        )
        query_count_rows = self._safe_rows(
            lambda: chart_dao.querylog_count_by_date(query_start, query_end),
            "querylog_count_by_date",
        )

        workflow_group_labels, workflow_group_values = _labels_and_values(
            self._safe_rows(
                lambda: chart_dao.workflow_by_group(query_start, query_end),
                "workflow_by_group",
            )
        )
        syntax_labels, syntax_values = _labels_and_values(
            self._safe_rows(
                lambda: chart_dao.syntax_type(query_start, query_end),
                "syntax_type",
            )
        )
        workflow_user_labels, workflow_user_values = _labels_and_values(
            self._safe_rows(
                lambda: chart_dao.workflow_by_user(query_start, query_end),
                "workflow_by_user",
            )
        )
        workflow_status_labels, workflow_status_values = _labels_and_values(
            self._safe_rows(
                lambda: chart_dao.query_sql_prod_bill(query_start, query_end),
                "query_sql_prod_bill",
            )
        )
        query_user_labels, query_user_values = _labels_and_values(
            self._safe_rows(
                lambda: chart_dao.querylog_effect_row_by_user(query_start, query_end),
                "querylog_effect_row_by_user",
            )
        )
        query_db_labels, query_db_values = _labels_and_values(
            self._safe_rows(
                lambda: chart_dao.querylog_effect_row_by_db(query_start, query_end),
                "querylog_effect_row_by_db",
            )
        )
        slow_db_user_labels, slow_db_user_values = _labels_and_values(
            self._safe_rows(
                lambda: chart_dao.slow_query_count_by_db_by_user(
                    query_start, query_end
                ),
                "slow_query_count_by_db_by_user",
            )
        )
        slow_db_labels, slow_db_values = _labels_and_values(
            self._safe_rows(
                lambda: chart_dao.slow_query_count_by_db(query_start, query_end),
                "slow_query_count_by_db",
            )
        )
        instance_type_labels, instance_type_values = _labels_and_values(
            self._safe_rows(
                chart_dao.instance_count_by_type,
                "instance_count_by_type",
            )
        )
        instance_env_rows = self._safe_rows(
            chart_dao.query_instance_env_info,
            "query_instance_env_info",
        )

        return {
            "start_date": start_date,
            "end_date": end_date,
            "summary": {
                "sql_workflow_count": SqlWorkflow.objects.count(),
                "query_workflow_count": QueryPrivilegesApply.objects.count(),
                "active_user_count": Users.objects.filter(is_active=1).count(),
                "instance_count": Instance.objects.count(),
            },
            "charts": {
                "workflow_by_date": {
                    "labels": day_labels,
                    "values": _date_values(workflow_by_date_rows, day_labels),
                },
                "workflow_by_group": {
                    "labels": workflow_group_labels,
                    "values": workflow_group_values,
                },
                "workflow_by_user": {
                    "labels": workflow_user_labels,
                    "values": workflow_user_values,
                },
                "workflow_status": {
                    "labels": workflow_status_labels,
                    "values": workflow_status_values,
                },
                "syntax_type": {
                    "labels": syntax_labels,
                    "values": syntax_values,
                },
                "query_activity": {
                    "labels": day_labels,
                    "scanned_rows": _date_values(query_scanned_rows, day_labels),
                    "query_count": _date_values(query_count_rows, day_labels),
                },
                "query_rows_by_user": {
                    "labels": query_user_labels,
                    "values": query_user_values,
                },
                "query_rows_by_db": {
                    "labels": query_db_labels,
                    "values": query_db_values,
                },
                "slow_query_by_db_user": {
                    "labels": slow_db_user_labels,
                    "values": slow_db_user_values,
                },
                "slow_query_by_db": {
                    "labels": slow_db_labels,
                    "values": slow_db_values,
                },
                "instance_type_distribution": {
                    "labels": instance_type_labels,
                    "values": instance_type_values,
                },
                "instance_env_distribution": _stacked_series(instance_env_rows),
            },
        }
