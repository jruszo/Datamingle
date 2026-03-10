import datetime
import logging

from django.contrib.auth.models import Group
from django_q.tasks import async_task
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiTypes
from rest_framework import views, generics, status, serializers, permissions
from rest_framework.exceptions import PermissionDenied
from rest_framework.response import Response

from common.config import SysConfig
from common.utils.const import WorkflowStatus, WorkflowType, WorkflowAction
from sql.engines import get_engine
from sql.models import (
    SqlWorkflow,
    SqlWorkflowContent,
    WorkflowAudit,
    WorkflowLog,
)
from sql.notify import notify_for_audit, notify_for_execute
from sql.query_privileges import _query_apply_audit_call_back
from sql.utils.resource_group import (
    user_groups,
    user_member_groups,
    user_has_group_instance_access,
    user_has_instance_workflow_access,
)
from sql.utils.sql_review import can_execute, on_correct_time_period
from sql.utils.tasks import del_schedule
from sql.utils.workflow_audit import Audit, get_auditor, AuditException
from .filters import WorkflowFilter, WorkflowAuditFilter
from .pagination import CustomizedPagination
from .response import success_response
from .serializers import (
    WorkflowContentSerializer,
    ExecuteCheckSerializer,
    ExecuteCheckResultSerializer,
    WorkflowAuditListSerializer,
    WorkflowLogListSerializer,
    AuditWorkflowSerializer,
    ExecuteWorkflowSerializer,
)

logger = logging.getLogger("default")


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

    filterset_class = WorkflowFilter
    pagination_class = CustomizedPagination
    serializer_class = WorkflowContentSerializer

    def get_queryset(self):
        """
        1. Non-admin users with review permission or resource-group-level execution permission can view all workflows in their groups.
        2. Admins and auditors can view all workflows.
        """
        filter_dict = {}
        user = self.request.user
        # Admins and auditors can view all workflows
        if user.is_superuser or user.has_perm("sql.audit_user"):
            pass
        # Non-admin users with review/resource-group execute permission can view group workflows
        elif user.has_perm("sql.sql_review") or user.has_perm(
            "sql.sql_execute_for_resource_group"
        ):
            filter_dict["workflow__group_id__in"] = [
                group.group_id for group in user_groups(user)
            ]
        # Others can only view workflows they submitted
        else:
            filter_dict["workflow__engineer"] = user.username
        return (
            SqlWorkflowContent.objects.filter(**filter_dict)
            .select_related("workflow")
            .order_by("-id")
        )

    @extend_schema(
        summary="SQL Release Workflow List",
        request=WorkflowContentSerializer,
        responses={200: WorkflowContentSerializer},
        description="List all SQL release workflows (filtering, pagination).",
    )
    def get(self, request):
        if not (
            request.user.is_superuser
            or request.user.has_perm("sql.menu_sqlworkflow")
            or request.user.has_perm("sql.audit_user")
        ):
            raise PermissionDenied("You do not have permission to view workflow list.")
        workflows = self.filter_queryset(self.get_queryset())
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
                {"errors": "audit_type can only be pass or cancel."}
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
