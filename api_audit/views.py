import datetime

from django.contrib.auth.decorators import permission_required
from django.db.models import Q
from django.utils.decorators import method_decorator
from drf_spectacular.utils import OpenApiParameter, OpenApiTypes, extend_schema
from rest_framework import generics, serializers, views

from api_audit.serializers import (
    GeneralAuditLogSerializer,
    QueryAuditLogSerializer,
    SqlWorkflowAuditSerializer,
    WorkflowOperationLogSerializer,
)
from api_core.pagination import CustomizedPagination
from api_core.response import success_response
from sql.models import AuditEntry, QueryLog, SqlWorkflow, WorkflowAudit, WorkflowLog


def _date_range(start_date, end_date):
    if not start_date or not end_date:
        return None
    try:
        start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.datetime.strptime(end_date, "%Y-%m-%d") + datetime.timedelta(
            days=1
        )
    except ValueError:
        return None
    return start, end


class GeneralAuditLogList(generics.ListAPIView):
    pagination_class = CustomizedPagination
    serializer_class = GeneralAuditLogSerializer

    @extend_schema(
        summary="General Audit Log",
        responses={200: GeneralAuditLogSerializer(many=True)},
        parameters=[
            OpenApiParameter("search", OpenApiTypes.STR, OpenApiParameter.QUERY),
            OpenApiParameter("action", OpenApiTypes.STR, OpenApiParameter.QUERY),
            OpenApiParameter("start_date", OpenApiTypes.DATE, OpenApiParameter.QUERY),
            OpenApiParameter("end_date", OpenApiTypes.DATE, OpenApiParameter.QUERY),
        ],
        description="List general user audit entries.",
    )
    @method_decorator(permission_required("sql.audit_user", raise_exception=True))
    def get(self, request):
        return super().get(request)

    def get_queryset(self):
        queryset = AuditEntry.objects.all().order_by("-action_time")
        search = self.request.query_params.get("search", "").strip()
        action = self.request.query_params.get("action", "").strip()
        start_date = self.request.query_params.get("start_date", "").strip()
        end_date = self.request.query_params.get("end_date", "").strip()

        if action:
            queryset = queryset.filter(action=action)

        action_range = _date_range(start_date, end_date)
        if action_range:
            queryset = queryset.filter(action_time__range=action_range)

        if search:
            queryset = queryset.filter(
                Q(user_name__icontains=search)
                | Q(user_display__icontains=search)
                | Q(action__icontains=search)
                | Q(extra_info__icontains=search)
            )

        return queryset


class QueryAuditLogList(generics.ListAPIView):
    pagination_class = CustomizedPagination
    serializer_class = QueryAuditLogSerializer

    @extend_schema(
        summary="Query Audit Log",
        responses={200: QueryAuditLogSerializer(many=True)},
        parameters=[
            OpenApiParameter("search", OpenApiTypes.STR, OpenApiParameter.QUERY),
            OpenApiParameter("start_date", OpenApiTypes.DATE, OpenApiParameter.QUERY),
            OpenApiParameter("end_date", OpenApiTypes.DATE, OpenApiParameter.QUERY),
            OpenApiParameter("instance_name", OpenApiTypes.STR, OpenApiParameter.QUERY),
            OpenApiParameter("username", OpenApiTypes.STR, OpenApiParameter.QUERY),
        ],
        description="List online query audit entries across users.",
    )
    @method_decorator(permission_required("sql.audit_user", raise_exception=True))
    def get(self, request):
        return super().get(request)

    def get_queryset(self):
        queryset = QueryLog.objects.all().order_by("-id")
        search = self.request.query_params.get("search", "").strip()
        start_date = self.request.query_params.get("start_date", "").strip()
        end_date = self.request.query_params.get("end_date", "").strip()
        instance_name = self.request.query_params.get("instance_name", "").strip()
        username = self.request.query_params.get("username", "").strip()

        if instance_name:
            queryset = queryset.filter(instance_name=instance_name)
        if username:
            queryset = queryset.filter(username=username)

        create_range = _date_range(start_date, end_date)
        if create_range:
            queryset = queryset.filter(create_time__range=create_range)

        if search:
            queryset = queryset.filter(
                Q(sqllog__icontains=search)
                | Q(user_display__icontains=search)
                | Q(username__icontains=search)
                | Q(alias__icontains=search)
                | Q(instance_name__icontains=search)
                | Q(db_name__icontains=search)
            )

        return queryset


class SqlWorkflowAuditLogList(generics.ListAPIView):
    pagination_class = CustomizedPagination
    serializer_class = SqlWorkflowAuditSerializer

    @extend_schema(
        summary="SQL Workflow Audit Log",
        responses={200: SqlWorkflowAuditSerializer(many=True)},
        parameters=[
            OpenApiParameter("search", OpenApiTypes.STR, OpenApiParameter.QUERY),
            OpenApiParameter("status", OpenApiTypes.STR, OpenApiParameter.QUERY),
            OpenApiParameter("syntax_type", OpenApiTypes.INT, OpenApiParameter.QUERY),
            OpenApiParameter("group_id", OpenApiTypes.INT, OpenApiParameter.QUERY),
            OpenApiParameter("instance_id", OpenApiTypes.INT, OpenApiParameter.QUERY),
            OpenApiParameter("start_date", OpenApiTypes.DATE, OpenApiParameter.QUERY),
            OpenApiParameter("end_date", OpenApiTypes.DATE, OpenApiParameter.QUERY),
        ],
        description="List SQL workflow audit entries across users.",
    )
    @method_decorator(permission_required("sql.audit_user", raise_exception=True))
    def get(self, request):
        return super().get(request)

    def get_queryset(self):
        queryset = (
            SqlWorkflow.objects.select_related("instance")
            .all()
            .order_by("-create_time")
        )
        search = self.request.query_params.get("search", "").strip()
        status_value = self.request.query_params.get("status", "").strip()
        syntax_type = self.request.query_params.get("syntax_type", "").strip()
        group_id = self.request.query_params.get("group_id", "").strip()
        instance_id = self.request.query_params.get("instance_id", "").strip()
        start_date = self.request.query_params.get("start_date", "").strip()
        end_date = self.request.query_params.get("end_date", "").strip()

        if status_value:
            queryset = queryset.filter(status=status_value)
        if syntax_type:
            queryset = queryset.filter(syntax_type=syntax_type)
        if group_id:
            queryset = queryset.filter(group_id=group_id)
        if instance_id:
            queryset = queryset.filter(instance_id=instance_id)

        create_range = _date_range(start_date, end_date)
        if create_range:
            queryset = queryset.filter(create_time__range=create_range)

        if search:
            queryset = queryset.filter(
                Q(workflow_name__icontains=search)
                | Q(engineer__icontains=search)
                | Q(engineer_display__icontains=search)
                | Q(group_name__icontains=search)
                | Q(db_name__icontains=search)
                | Q(instance__instance_name__icontains=search)
            )

        return queryset


class WorkflowOperationLogList(views.APIView):
    @extend_schema(
        summary="Workflow Operation Logs",
        responses={200: WorkflowOperationLogSerializer(many=True)},
        parameters=[
            OpenApiParameter("audit_id", OpenApiTypes.INT, OpenApiParameter.QUERY),
            OpenApiParameter("workflow_id", OpenApiTypes.INT, OpenApiParameter.QUERY),
            OpenApiParameter("workflow_type", OpenApiTypes.INT, OpenApiParameter.QUERY),
        ],
        description="List workflow operation logs by audit ID or workflow ID/type.",
    )
    @method_decorator(permission_required("sql.audit_user", raise_exception=True))
    def get(self, request):
        audit_id = request.query_params.get("audit_id", "").strip()
        workflow_id = request.query_params.get("workflow_id", "").strip()
        workflow_type = request.query_params.get("workflow_type", "").strip() or "2"

        if not audit_id and not workflow_id:
            raise serializers.ValidationError(
                {"errors": "audit_id or workflow_id is required."}
            )

        if not audit_id:
            try:
                audit_id = WorkflowAudit.objects.get(
                    workflow_id=workflow_id, workflow_type=workflow_type
                ).audit_id
            except WorkflowAudit.DoesNotExist:
                return success_response(data={"count": 0, "results": []})

        logs = WorkflowLog.objects.filter(audit_id=audit_id).order_by("-id")
        serializer = WorkflowOperationLogSerializer(logs, many=True)
        return success_response(
            data={"count": logs.count(), "results": serializer.data}
        )
