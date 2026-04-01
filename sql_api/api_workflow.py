import datetime
import json
import logging

from django.contrib.auth.models import Group
from django.db import transaction
from django.db.models import Q
from django.shortcuts import get_object_or_404
from django_q.tasks import async_task
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiTypes
from rest_framework import views, generics, status, serializers, permissions
from rest_framework.exceptions import PermissionDenied
from rest_framework.response import Response

from common.config import SysConfig
from common.utils.const import WorkflowStatus, WorkflowType, WorkflowAction
from sql.engines import get_engine
from sql.engines.models import ReviewResult, ReviewSet
from sql.models import (
    Instance,
    ResourceGroup,
    SqlWorkflow,
    SqlWorkflowContent,
    WorkflowAudit,
    WorkflowLog,
)
from sql.notify import notify_for_audit, notify_for_execute
from sql.query_privileges import _query_apply_audit_call_back
from sql.utils.resource_group import (
    user_groups,
    user_instances,
    user_member_groups,
    user_has_group_instance_access,
    user_has_instance_workflow_access,
)
from sql.utils.sql_review import (
    can_cancel,
    can_execute,
    can_rollback,
    can_timingtask,
    can_view,
    on_correct_time_period,
)
from sql.utils.tasks import add_sql_schedule, del_schedule, task_info
from sql.utils.workflow_audit import Audit, AuditV2, get_auditor, AuditException
from .filters import WorkflowAuditFilter
from .pagination import CustomizedPagination
from .response import success_response
from .serializers import (
    ExecuteCheckResultSerializer,
    ExecuteCheckSerializer,
    WorkflowAuditListSerializer,
    AuditWorkflowSerializer,
    ExecuteWorkflowSerializer,
    WorkflowContentSerializer,
    WorkflowLogListSerializer,
)

logger = logging.getLogger("default")

JSON_PARSE_ERROR_MESSAGE = (
    "Json decode failed. Execution result JSON parsing failed. Please contact admin."
)


class WorkflowSummarySerializer(serializers.ModelSerializer):
    instance_id = serializers.IntegerField(read_only=True)
    instance_name = serializers.CharField(
        source="instance.instance_name", read_only=True
    )
    instance_db_type = serializers.CharField(source="instance.db_type", read_only=True)
    status_label = serializers.CharField(source="get_status_display", read_only=True)
    syntax_type_label = serializers.CharField(
        source="get_syntax_type_display", read_only=True
    )

    class Meta:
        model = SqlWorkflow
        fields = (
            "id",
            "workflow_name",
            "demand_url",
            "group_id",
            "group_name",
            "instance_id",
            "instance_name",
            "instance_db_type",
            "db_name",
            "syntax_type",
            "syntax_type_label",
            "status",
            "status_label",
            "is_backup",
            "engineer",
            "engineer_display",
            "run_date_start",
            "run_date_end",
            "create_time",
            "finish_time",
        )


class WorkflowResourceGroupLookupSerializer(serializers.ModelSerializer):
    class Meta:
        model = ResourceGroup
        fields = ("group_id", "group_name")


class WorkflowInstanceLookupSerializer(serializers.ModelSerializer):
    label = serializers.SerializerMethodField()
    resource_groups = serializers.SerializerMethodField()

    def get_label(self, obj):
        return f"{obj.instance_name} | {obj.db_type} | {obj.host}"

    def get_resource_groups(self, obj):
        queryset = obj.resource_group.filter(is_deleted=0).order_by(
            "group_name", "group_id"
        )
        return WorkflowResourceGroupLookupSerializer(queryset, many=True).data

    class Meta:
        model = Instance
        fields = (
            "id",
            "instance_name",
            "db_type",
            "type",
            "host",
            "label",
            "resource_groups",
        )


class WorkflowMetadataSerializer(serializers.Serializer):
    allow_backup_toggle = serializers.BooleanField()
    manual_execution_enabled = serializers.BooleanField()
    resource_groups = WorkflowResourceGroupLookupSerializer(many=True)
    instances = WorkflowInstanceLookupSerializer(many=True)


class WorkflowScheduleSerializer(serializers.Serializer):
    run_date = serializers.DateTimeField(
        input_formats=[
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%dT%H:%M:%S",
            "iso-8601",
        ]
    )


class WorkflowExecutionWindowSerializer(serializers.Serializer):
    run_date_start = serializers.DateTimeField(
        required=False,
        allow_null=True,
        input_formats=[
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%dT%H:%M:%S",
            "iso-8601",
        ],
    )
    run_date_end = serializers.DateTimeField(
        required=False,
        allow_null=True,
        input_formats=[
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%dT%H:%M:%S",
            "iso-8601",
        ],
    )

    def validate(self, attrs):
        start = attrs.get("run_date_start")
        end = attrs.get("run_date_end")
        if start and end and start > end:
            raise serializers.ValidationError(
                {"errors": "run_date_start cannot be later than run_date_end."}
            )
        return attrs


def _normalize_datetime_for_storage(value):
    if value is None:
        return None
    if hasattr(value, "tzinfo") and value.tzinfo is not None:
        return value.astimezone().replace(tzinfo=None)
    return value


def _parse_review_rows(raw_rows, sql_content):
    if not raw_rows:
        return []

    review_result = ReviewSet(full_sql=sql_content)
    try:
        loaded_rows = json.loads(raw_rows)
    except (TypeError, json.JSONDecodeError):
        loaded_rows = None

    if loaded_rows is None:
        review_result.rows.append(
            ReviewResult(id=1, sql=sql_content, errormessage=JSON_PARSE_ERROR_MESSAGE)
        )
        return review_result.to_dict()

    if not loaded_rows:
        return []

    try:
        if isinstance(loaded_rows[-1], list):
            for row in loaded_rows:
                review_result.rows.append(ReviewResult(inception_result=row))
            return review_result.to_dict()
    except IndexError:
        return []

    rows = []
    for row in loaded_rows:
        if isinstance(row, dict):
            rows.append(ReviewResult(**row).__dict__)
        elif isinstance(row, list):
            rows.append(ReviewResult(inception_result=row).__dict__)
    return rows


def _serialize_review_info(workflow):
    review_info = AuditV2(workflow=workflow).get_review_info()
    return [
        {
            "group_name": node.group.name if node.group else "Auto",
            "is_current_node": node.is_current_node,
            "is_passed_node": node.is_passed_node,
        }
        for node in review_info.nodes
    ]


def _serialize_current_reviewers(workflow):
    review_info = AuditV2(workflow=workflow).get_review_info()
    reviewers = []
    seen_usernames = set()
    for node in review_info.nodes:
        if not node.is_current_node or not node.group:
            continue
        for user in node.group.user_set.filter(is_active=1):
            group_names = [group.group_name for group in user_member_groups(user)]
            if (
                workflow.group_name not in group_names
                or user.username in seen_usernames
            ):
                continue
            seen_usernames.add(user.username)
            reviewers.append(
                {
                    "id": user.id,
                    "username": user.username,
                    "display": user.display,
                }
            )
    return reviewers


def _serialize_workflow_detail(workflow, user):
    if not can_view(user, workflow.id):
        raise PermissionDenied("You do not have permission to view this workflow.")

    audit = Audit.detail_by_workflow_id(workflow.id, WorkflowType.SQL_REVIEW)
    logs = []
    last_operation_info = ""
    if audit:
        workflow_logs = WorkflowLog.objects.filter(audit_id=audit.audit_id).order_by(
            "-id"
        )
        logs = [
            {
                "operation_type_desc": log.operation_type_desc,
                "operation_info": log.operation_info,
                "operator_display": log.operator_display,
                "operation_time": log.operation_time,
            }
            for log in workflow_logs
        ]
        if logs:
            last_operation_info = logs[0]["operation_info"]

    if workflow.status == "workflow_autoreviewwrong":
        can_review_now = False
        can_execute_now = False
        can_schedule_now = False
        can_cancel_now = False
        can_rollback_now = False
    else:
        can_review_now = Audit.can_review(user, workflow.id, WorkflowType.SQL_REVIEW)
        can_execute_now = can_execute(user, workflow.id)
        can_schedule_now = can_timingtask(user, workflow.id)
        can_cancel_now = can_cancel(user, workflow.id)
        can_rollback_now = can_rollback(user, workflow.id)

    schedule = task_info(f"sqlreview-timing-{workflow.id}")
    manual_enabled = bool(SysConfig().get("manual"))

    payload = WorkflowSummarySerializer(workflow).data
    payload.update(
        {
            "sql_content": workflow.sqlworkflowcontent.sql_content,
            "review_rows": _parse_review_rows(
                workflow.sqlworkflowcontent.review_content,
                workflow.sqlworkflowcontent.sql_content,
            ),
            "execute_rows": _parse_review_rows(
                workflow.sqlworkflowcontent.execute_result,
                workflow.sqlworkflowcontent.sql_content,
            ),
            "review_info": _serialize_review_info(workflow),
            "current_reviewers": _serialize_current_reviewers(workflow),
            "logs": logs,
            "last_operation_info": last_operation_info,
            "scheduled_run_date": schedule.next_run if schedule else None,
            "is_can_review": can_review_now,
            "is_can_reject": can_review_now,
            "is_can_execute": can_execute_now,
            "is_can_schedule": can_schedule_now,
            "is_can_cancel": can_cancel_now,
            "is_can_abort": can_cancel_now and workflow.engineer == user.username,
            "is_can_rollback": can_rollback_now,
            "is_can_manual_execute": can_execute_now and manual_enabled,
            "is_can_edit_execution_window": can_review_now,
            "manual_execution_enabled": manual_enabled,
        }
    )
    return payload


def _can_access_workflow_module(user):
    return any(
        [
            user.is_superuser,
            user.has_perm("sql.menu_sqlworkflow"),
            user.has_perm("sql.sql_submit"),
            user.has_perm("sql.audit_user"),
            user_instances(user, tag_codes=["can_write"]).exists(),
        ]
    )


class ExecuteCheck(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="SQL Check",
        request=ExecuteCheckSerializer,
        responses={200: ExecuteCheckResultSerializer},
        description="Perform syntax checks for the provided SQL using request body.",
    )
    def post(self, request):
        # Parameter validation
        serializer = ExecuteCheckSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        instance = serializer.get_instance()
        # Run check through engine
        try:
            db_name = serializer.validated_data["db_name"]
            full_sql = serializer.validated_data["full_sql"].strip()
            check_engine = get_engine(instance=instance)
            db_name = check_engine.escape_string(db_name)
            check_result = check_engine.execute_check(db_name=db_name, sql=full_sql)
        except Exception as e:
            raise serializers.ValidationError({"errors": f"{e}"})
        has_group_write_access = user_has_group_instance_access(
            request.user, instance, tag_codes=["can_write"]
        )
        has_temporary_write_access = user_has_instance_workflow_access(
            request.user, instance, check_result.syntax_type
        )
        if not (
            request.user.is_superuser
            or (has_group_write_access and request.user.has_perm("sql.sql_submit"))
            or (has_temporary_write_access and not has_group_write_access)
        ):
            raise serializers.ValidationError(
                {
                    "errors": "You do not have permission to submit SQL for this instance."
                }
            )
        check_result.rows = check_result.to_dict()
        serializer_obj = ExecuteCheckResultSerializer(check_result)
        return success_response(data=serializer_obj.data)


class WorkflowList(generics.ListAPIView):
    """
    List all workflows or submit a new workflow.
    """

    permission_classes = [permissions.IsAuthenticated]

    pagination_class = CustomizedPagination
    serializer_class = WorkflowSummarySerializer

    def get_serializer_class(self):
        if self.request.method == "POST":
            return WorkflowContentSerializer
        return WorkflowSummarySerializer

    def get_queryset(self):
        queryset = SqlWorkflow.objects.select_related("instance").all()
        user = self.request.user

        if user.is_superuser or user.has_perm("sql.audit_user"):
            pass
        elif user.has_perm("sql.sql_review") or user.has_perm(
            "sql.sql_execute_for_resource_group"
        ):
            queryset = queryset.filter(
                group_id__in=[group.group_id for group in user_groups(user)]
            )
        else:
            queryset = queryset.filter(engineer=user.username)

        query_params = self.request.query_params
        search = query_params.get("search", "").strip()
        status_value = query_params.get("status", "").strip()
        syntax_type = query_params.get("syntax_type", "").strip()
        group_id = query_params.get("group_id", "").strip()
        instance_id = query_params.get("instance_id", "").strip()
        engineer = query_params.get("engineer", "").strip()
        start_date = query_params.get("start_date", "").strip()
        end_date = query_params.get("end_date", "").strip()

        if search:
            queryset = queryset.filter(
                Q(workflow_name__icontains=search)
                | Q(engineer_display__icontains=search)
                | Q(instance__instance_name__icontains=search)
                | Q(db_name__icontains=search)
                | Q(group_name__icontains=search)
            )
        if status_value:
            queryset = queryset.filter(status=status_value)
        if syntax_type:
            queryset = queryset.filter(syntax_type=syntax_type)
        if group_id:
            queryset = queryset.filter(group_id=group_id)
        if instance_id:
            queryset = queryset.filter(instance_id=instance_id)
        if engineer:
            queryset = queryset.filter(engineer=engineer)
        if start_date:
            queryset = queryset.filter(create_time__date__gte=start_date)
        if end_date:
            queryset = queryset.filter(create_time__date__lte=end_date)

        return queryset.order_by("-create_time", "-id")

    @extend_schema(
        summary="SQL Release Workflow List",
        responses={200: WorkflowSummarySerializer},
        description="List all SQL release workflows (filtering, pagination).",
    )
    def get(self, request):
        if not _can_access_workflow_module(request.user):
            raise PermissionDenied("You do not have permission to view workflow list.")
        workflows = self.get_queryset()
        page_wf = self.paginate_queryset(queryset=workflows)
        serializer_obj = self.get_serializer(page_wf, many=True)
        return self.get_paginated_response(serializer_obj.data)

    @extend_schema(
        summary="Submit SQL Release Workflow",
        request=WorkflowContentSerializer,
        responses={201: WorkflowContentSerializer},
        description="Submit an SQL release workflow.",
    )
    def post(self, request):
        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        workflow_content = serializer.save()
        sys_config = SysConfig()
        is_notified = (
            "Apply" in sys_config.get("notify_phase_control").split(",")
            if sys_config.get("notify_phase_control")
            else True
        )
        if workflow_content.workflow.status == "workflow_manreviewing" and is_notified:
            # Get audit information
            workflow_audit = Audit.detail_by_workflow_id(
                workflow_id=workflow_content.workflow.id,
                workflow_type=WorkflowType.SQL_REVIEW,
            )
            async_task(
                notify_for_audit,
                workflow_audit=workflow_audit,
                timeout=60,
                task_name=f"sqlreview-submit-{workflow_content.workflow.id}",
            )
        return success_response(
            data=serializer.data, status_code=status.HTTP_201_CREATED
        )


class WorkflowMetadata(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Workflow Submission Metadata",
        responses={200: WorkflowMetadataSerializer},
        description="Return resource groups, writable instances, and config flags used by the SQL workflow SPA.",
    )
    def get(self, request):
        if not _can_access_workflow_module(request.user):
            raise PermissionDenied(
                "You do not have permission to submit SQL workflows."
            )

        payload = {
            "allow_backup_toggle": bool(SysConfig().get("enable_backup_switch")),
            "manual_execution_enabled": bool(SysConfig().get("manual")),
            "resource_groups": user_groups(request.user),
            "instances": user_instances(request.user, tag_codes=["can_write"])
            .prefetch_related("resource_group")
            .order_by("instance_name", "id"),
        }
        serializer = WorkflowMetadataSerializer(payload)
        return success_response(data=serializer.data)


class WorkflowDetail(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Workflow Detail",
        responses={200: OpenApiTypes.OBJECT},
        description="Get SQL workflow detail including approval flow, parsed review and execution rows, logs, and action flags.",
    )
    def get(self, request, workflow_id):
        workflow = get_object_or_404(
            SqlWorkflow.objects.select_related("instance", "sqlworkflowcontent"),
            pk=workflow_id,
        )
        return success_response(data=_serialize_workflow_detail(workflow, request.user))


class WorkflowExecutionWindowUpdate(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Update Execution Window",
        request=WorkflowExecutionWindowSerializer,
        description="Update the executable time window for a waiting SQL workflow.",
    )
    def patch(self, request, workflow_id):
        workflow = get_object_or_404(SqlWorkflow, pk=workflow_id)
        if (
            Audit.can_review(request.user, workflow_id, WorkflowType.SQL_REVIEW)
            is False
        ):
            raise PermissionDenied(
                "You do not have permission to edit this execution window."
            )

        serializer = WorkflowExecutionWindowSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data

        workflow.run_date_start = _normalize_datetime_for_storage(
            data.get("run_date_start")
        )
        workflow.run_date_end = _normalize_datetime_for_storage(
            data.get("run_date_end")
        )
        workflow.save(update_fields=["run_date_start", "run_date_end"])
        return success_response(detail="Execution window updated.")


class WorkflowScheduleCreate(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Schedule Workflow Execution",
        request=WorkflowScheduleSerializer,
        description="Schedule a SQL workflow for execution at a specific time.",
    )
    def post(self, request, workflow_id):
        if not (
            request.user.has_perm("sql.sql_execute")
            or request.user.has_perm("sql.sql_execute_for_resource_group")
        ):
            raise PermissionDenied(
                "You do not have permission to schedule this workflow."
            )

        workflow = get_object_or_404(SqlWorkflow, pk=workflow_id)
        serializer = WorkflowScheduleSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        run_date = _normalize_datetime_for_storage(
            serializer.validated_data["run_date"]
        )

        if run_date < datetime.datetime.now():
            raise serializers.ValidationError(
                {"errors": "run_date cannot be earlier than the current time."}
            )
        if can_timingtask(request.user, workflow_id) is False:
            raise serializers.ValidationError(
                {"errors": "You do not have permission to operate this workflow."}
            )
        if on_correct_time_period(workflow_id, run_date) is False:
            raise serializers.ValidationError(
                {
                    "errors": "Current schedule time is outside the executable window. Update the workflow window first if needed."
                }
            )

        schedule_name = f"sqlreview-timing-{workflow_id}"
        with transaction.atomic():
            workflow.status = "workflow_timingtask"
            workflow.save(update_fields=["status"])
            add_sql_schedule(schedule_name, run_date, workflow_id)
            audit = Audit.detail_by_workflow_id(workflow_id, WorkflowType.SQL_REVIEW)
            Audit.add_log(
                audit_id=audit.audit_id,
                operation_type=WorkflowAction.EXECUTE_SET_TIME,
                operation_type_desc="Scheduled Execution",
                operation_info=f"Scheduled execution time: {run_date}",
                operator=request.user.username,
                operator_display=request.user.display,
            )

        return success_response(detail="Execution scheduled.")


class WorkflowAuditList(generics.ListAPIView):
    """
    List workflows currently waiting for review by the specified user.
    """

    permission_classes = [permissions.IsAuthenticated]

    filterset_class = WorkflowAuditFilter
    pagination_class = CustomizedPagination
    serializer_class = WorkflowAuditListSerializer
    queryset = WorkflowAudit.objects.filter(
        current_status=WorkflowStatus.WAITING
    ).order_by("-audit_id")

    @extend_schema(
        summary="Pending Review List",
        responses={200: WorkflowAuditListSerializer},
        parameters=[
            OpenApiParameter(
                name="workflow_title__icontains",
                type=OpenApiTypes.STR,
                location=OpenApiParameter.QUERY,
                description="Filter by workflow title (contains).",
            ),
            OpenApiParameter(
                name="workflow_type",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                description="Workflow type.",
            ),
            OpenApiParameter(
                name="page",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                description="Page number.",
            ),
            OpenApiParameter(
                name="size",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                description="Page size.",
            ),
        ],
        description="List pending reviews for the authenticated user (filtering, pagination).",
    )
    def get(self, request):
        user = request.user
        group_list = user_member_groups(user)
        group_ids = [group.group_id for group in group_list]

        if user.is_superuser:
            auth_group_ids = [group.id for group in Group.objects.all()]
        else:
            auth_group_ids = [group.id for group in Group.objects.filter(user=user)]

        queryset = self.queryset.filter(
            current_status=WorkflowStatus.WAITING,
            group_id__in=group_ids,
            current_audit__in=auth_group_ids,
        )
        audit = self.filter_queryset(queryset)
        page_audit = self.paginate_queryset(queryset=audit)
        serializer_obj = self.get_serializer(page_audit, many=True)
        return self.get_paginated_response(serializer_obj.data)


class WorkflowReviewCreate(views.APIView):
    """
    Audit workflows, including query privilege applications, SQL release applications, and data archive applications.
    """

    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Audit Workflow",
        request=AuditWorkflowSerializer,
        description="Audit a workflow (approve or terminate).",
    )
    def post(self, request, workflow_id):
        # Parameter validation
        data = request.data.copy()
        data["workflow_id"] = workflow_id
        serializer = AuditWorkflowSerializer(data=data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        data = serializer.validated_data
        # Already validated, record must exist
        workflow_audit = WorkflowAudit.objects.get(
            workflow_id=data["workflow_id"],
            workflow_type=data["workflow_type"],
        )
        sys_config = SysConfig()
        auditor = get_auditor(audit=workflow_audit)
        user = request.user
        if data["audit_type"] == "pass":
            action = WorkflowAction.PASS
            notify_config_key = "Pass"
            success_message = "passed"
        elif data["audit_type"] == "reject":
            notify_config_key = "Cancel"
            success_message = "rejected"
            if Audit.can_review(user, workflow_id, data["workflow_type"]):
                action = WorkflowAction.REJECT
            else:
                raise serializers.ValidationError(
                    {"errors": "User is not allowed to operate this workflow."}
                )
        elif data["audit_type"] == "cancel":
            notify_config_key = "Cancel"
            success_message = "canceled"
            if auditor.audit.create_user == user.username:
                action = WorkflowAction.ABORT
            else:
                raise serializers.ValidationError(
                    {"errors": "User is not allowed to operate this workflow."}
                )
        else:
            raise serializers.ValidationError(
                {"errors": "audit_type can only be pass, reject, or cancel."}
            )

        try:
            workflow_audit_detail = auditor.operate(action, user, data["audit_remark"])
        except AuditException as e:
            raise serializers.ValidationError({"errors": f"Operation failed, {str(e)}"})

        # Finally handle source workflow status
        if auditor.workflow_type == WorkflowType.QUERY:
            _query_apply_audit_call_back(
                auditor.audit.workflow_id,
                auditor.audit.current_status,
            )
        elif auditor.workflow_type == WorkflowType.SQL_REVIEW:
            if auditor.audit.current_status == WorkflowStatus.PASSED:
                auditor.workflow.status = "workflow_review_pass"
                auditor.workflow.save(update_fields=["status"])
            elif auditor.audit.current_status in [
                WorkflowStatus.ABORTED,
                WorkflowStatus.REJECTED,
            ]:
                if auditor.workflow.status == "workflow_timingtask":
                    del_schedule(f"sqlreview-timing-{auditor.workflow.id}")
                    # Mark workflow as manually terminated
                auditor.workflow.status = "workflow_abort"
                auditor.workflow.save(update_fields=["status"])
        elif auditor.workflow_type == WorkflowType.ARCHIVE:
            auditor.workflow.status = auditor.audit.current_status
            if auditor.audit.current_status == WorkflowStatus.PASSED:
                auditor.workflow.state = True
            else:
                auditor.workflow.state = False
            auditor.workflow.save(update_fields=["status", "state"])

        # Send notification
        is_notified = (
            notify_config_key in sys_config.get("notify_phase_control").split(",")
            if sys_config.get("notify_phase_control")
            else True
        )
        if is_notified:
            async_task(
                notify_for_audit,
                workflow_audit=auditor.audit,
                workflow_audit_detail=workflow_audit_detail,
                timeout=60,
                task_name=f"notify-audit-{auditor.audit}-{WorkflowType(auditor.audit.workflow_type).label}",
            )
        return success_response(detail=success_message)


class WorkflowExecutionCreate(views.APIView):
    """
    Execute workflows, including SQL release workflows and data archive workflows.
    """

    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Execute Workflow",
        request=ExecuteWorkflowSerializer,
        description="Execute a workflow.",
    )
    def post(self, request, workflow_id):
        # Parameter validation
        data = request.data.copy()
        data["workflow_id"] = workflow_id
        serializer = ExecuteWorkflowSerializer(data=data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        data = serializer.validated_data

        workflow_type = data["workflow_type"]

        # Execute SQL release workflow
        if workflow_type == 2:
            mode = data["mode"]
            user = request.user

            # Validate multiple permissions
            if not (
                user.has_perm("sql.sql_execute")
                or user.has_perm("sql.sql_execute_for_resource_group")
            ):
                raise serializers.ValidationError(
                    {"errors": "You do not have permission to execute this workflow."}
                )

            if can_execute(user, workflow_id) is False:
                raise serializers.ValidationError(
                    {"errors": "You do not have permission to execute this workflow."}
                )

            if on_correct_time_period(workflow_id) is False:
                raise serializers.ValidationError(
                    {
                        "errors": "Current time is outside the executable window. Please resubmit the workflow if you need to change execution time."
                    }
                )

            # Get audit information
            audit_id = Audit.detail_by_workflow_id(
                workflow_id=workflow_id,
                workflow_type=WorkflowType.SQL_REVIEW,
            ).audit_id

            # Execute by system
            if mode == "auto":
                # Set workflow status to queuing
                SqlWorkflow(id=workflow_id, status="workflow_queuing").save(
                    update_fields=["status"]
                )
                # Delete scheduled execution task
                schedule_name = f"sqlreview-timing-{workflow_id}"
                del_schedule(schedule_name)
                # Add to execution queue
                async_task(
                    "sql.utils.execute_sql.execute",
                    workflow_id,
                    user,
                    hook="sql.utils.execute_sql.execute_callback",
                    timeout=-1,
                    task_name=f"sqlreview-execute-{workflow_id}",
                )
                # Add workflow log
                Audit.add_log(
                    audit_id=audit_id,
                    operation_type=5,
                    operation_type_desc="Execute Workflow",
                    operation_info="Workflow queued for execution",
                    operator=user.username,
                    operator_display=user.display,
                )

            # Manual offline execution
            elif mode == "manual":
                # Set workflow status to finished
                SqlWorkflow(
                    id=workflow_id,
                    status="workflow_finish",
                    finish_time=datetime.datetime.now(),
                ).save(update_fields=["status", "finish_time"])
                del_schedule(f"sqlreview-timing-{workflow_id}")
                # Add workflow log
                Audit.add_log(
                    audit_id=audit_id,
                    operation_type=6,
                    operation_type_desc="Manual Workflow",
                    operation_info="Confirmed manual execution completed",
                    operator=user.username,
                    operator_display=user.display,
                )
                # Send notification only if Execute phase notifications are enabled
                sys_config = SysConfig()
                is_notified = (
                    "Execute" in sys_config.get("notify_phase_control").split(",")
                    if sys_config.get("notify_phase_control")
                    else True
                )
                if is_notified:
                    notify_for_execute(
                        workflow=SqlWorkflow.objects.get(id=workflow_id),
                    )
        # Execute data archive workflow
        elif workflow_type == 3:
            if not request.user.has_perm("sql.archive_mgt"):
                raise serializers.ValidationError(
                    {
                        "errors": "You do not have permission to execute archive workflows."
                    }
                )
            async_task(
                "sql.archiver.archive",
                workflow_id,
                timeout=-1,
                task_name=f"archive-{workflow_id}",
            )

        return success_response(
            detail="Execution started. Please check workflow detail page for results."
        )


class WorkflowLogList(generics.ListAPIView):
    """
    Get logs for a workflow.
    """

    permission_classes = [permissions.IsAuthenticated]

    pagination_class = CustomizedPagination
    serializer_class = WorkflowLogListSerializer
    queryset = WorkflowLog.objects.all()

    @extend_schema(
        summary="Workflow Logs",
        responses={200: WorkflowLogListSerializer},
        parameters=[
            OpenApiParameter(
                name="workflow_id",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Workflow ID.",
            ),
            OpenApiParameter(
                name="workflow_type",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Workflow type: 1, 2, or 3.",
            ),
            OpenApiParameter(
                name="page",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                description="Page number.",
            ),
            OpenApiParameter(
                name="size",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                description="Page size.",
            ),
        ],
        description="Get logs of a workflow (pagination).",
    )
    def get(self, request):
        workflow_id = request.query_params.get("workflow_id")
        workflow_type = request.query_params.get("workflow_type")
        if workflow_id is None or workflow_type is None:
            raise serializers.ValidationError(
                {
                    "errors": "workflow_id and workflow_type are required query parameters."
                }
            )
        try:
            workflow_id = int(workflow_id)
            workflow_type = int(workflow_type)
        except (TypeError, ValueError):
            raise serializers.ValidationError(
                {"errors": "workflow_id and workflow_type must be integers."}
            )
        if workflow_type not in [1, 2, 3]:
            raise serializers.ValidationError(
                {"errors": "workflow_type can only be 1, 2, or 3."}
            )

        try:
            audit_id = WorkflowAudit.objects.get(
                workflow_id=workflow_id,
                workflow_type=workflow_type,
            ).audit_id
        except WorkflowAudit.DoesNotExist:
            raise serializers.ValidationError({"errors": "Workflow does not exist."})
        workflow_logs = self.queryset.filter(audit_id=audit_id).order_by("-id")
        page_log = self.paginate_queryset(queryset=workflow_logs)
        serializer_obj = self.get_serializer(page_log, many=True)
        return self.get_paginated_response(serializer_obj.data)
