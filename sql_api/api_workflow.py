import datetime
import logging
import traceback

from django.contrib.auth.decorators import permission_required
from django.contrib.auth.models import Group
from django.db import transaction
from django.utils.decorators import method_decorator
from django_q.tasks import async_task
from drf_spectacular.utils import extend_schema
from rest_framework import views, generics, status, serializers, permissions
from rest_framework.response import Response

from common.config import SysConfig
from common.utils.const import WorkflowStatus, WorkflowType, WorkflowAction
from sql.engines import get_engine
from sql.models import (
    SqlWorkflow,
    SqlWorkflowContent,
    WorkflowAudit,
    Users,
    WorkflowLog,
    ArchiveConfig,
    QueryPrivilegesApply,
)
from sql.notify import notify_for_audit, notify_for_execute
from sql.query_privileges import _query_apply_audit_call_back
from sql.utils.resource_group import user_groups
from sql.utils.sql_review import can_cancel, can_execute, on_correct_time_period
from sql.utils.tasks import del_schedule
from sql.utils.workflow_audit import Audit, get_auditor, AuditException
from .filters import WorkflowFilter, WorkflowAuditFilter
from .pagination import CustomizedPagination
from .serializers import (
    WorkflowContentSerializer,
    ExecuteCheckSerializer,
    ExecuteCheckResultSerializer,
    WorkflowAuditSerializer,
    WorkflowAuditListSerializer,
    WorkflowLogSerializer,
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
        description="Perform syntax checks for the provided SQL.",
    )
    @method_decorator(permission_required("sql.sql_submit", raise_exception=True))
    def post(self, request):
        # Parameter validation
        serializer = ExecuteCheckSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        instance = serializer.get_instance()
        # Run check through engine
        try:
            db_name = request.data["db_name"]
            check_engine = get_engine(instance=instance)
            db_name = check_engine.escape_string(db_name)
            check_result = check_engine.execute_check(
                db_name=db_name, sql=request.data["full_sql"].strip()
            )
        except Exception as e:
            raise serializers.ValidationError({"errors": f"{e}"})
        check_result.rows = check_result.to_dict()
        serializer_obj = ExecuteCheckResultSerializer(check_result)
        return Response(serializer_obj.data)


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
            filter_dict["group_id__in"] = [
                group.group_id for group in user_groups(user)
            ]
        # Others can only view workflows they submitted
        else:
            filter_dict["engineer"] = user.username
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
        workflows = self.filter_queryset(self.queryset)
        page_wf = self.paginate_queryset(queryset=workflows)
        serializer_obj = self.get_serializer(page_wf, many=True)
        data = {"data": serializer_obj.data}
        return self.get_paginated_response(data)

    @extend_schema(
        summary="Submit SQL Release Workflow",
        request=WorkflowContentSerializer,
        responses={201: WorkflowContentSerializer},
        description="Submit an SQL release workflow.",
    )
    @method_decorator(permission_required("sql.sql_submit", raise_exception=True))
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
        return Response(serializer.data, status=status.HTTP_201_CREATED)


class WorkflowAuditList(generics.ListAPIView):
    """
    List workflows currently waiting for review by the specified user.
    """

    filterset_class = WorkflowAuditFilter
    pagination_class = CustomizedPagination
    serializer_class = WorkflowAuditListSerializer
    queryset = WorkflowAudit.objects.filter(
        current_status=WorkflowStatus.WAITING
    ).order_by("-audit_id")

    @extend_schema(exclude=True)
    def get(self, request):
        return Response({"detail": 'Method "GET" not allowed.'})

    @extend_schema(
        summary="Pending Review List",
        request=WorkflowAuditSerializer,
        responses={200: WorkflowAuditListSerializer},
        description="List pending reviews for the specified user (filtering, pagination).",
    )
    def post(self, request):
        # Parameter validation
        serializer = WorkflowAuditSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        # First get resource groups of the user
        user = Users.objects.get(username=request.data["engineer"])
        group_list = user_groups(user)
        group_ids = [group.group_id for group in group_list]

        # Then get permission groups of the user
        if user.is_superuser:
            auth_group_ids = [group.id for group in Group.objects.all()]
        else:
            auth_group_ids = [group.id for group in Group.objects.filter(user=user)]

        self.queryset = self.queryset.filter(
            current_status=WorkflowStatus.WAITING,
            group_id__in=group_ids,
            current_audit__in=auth_group_ids,
        )
        audit = self.filter_queryset(self.queryset)
        page_audit = self.paginate_queryset(queryset=audit)
        serializer_obj = self.get_serializer(page_audit, many=True)
        data = {"data": serializer_obj.data}
        return self.get_paginated_response(data)


class AuditWorkflow(views.APIView):
    """
    Audit workflows, including query privilege applications, SQL release applications, and data archive applications.
    """

    @extend_schema(
        summary="Audit Workflow",
        request=AuditWorkflowSerializer,
        description="Audit a workflow (approve or terminate).",
    )
    def post(self, request):
        # Parameter validation
        serializer = AuditWorkflowSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        # Already validated, record must exist
        workflow_audit = WorkflowAudit.objects.get(
            workflow_id=serializer.data["workflow_id"],
            workflow_type=serializer.data["workflow_type"],
        )
        sys_config = SysConfig()
        auditor = get_auditor(audit=workflow_audit)
        user = Users.objects.get(username=serializer.data["engineer"])
        if serializer.data["audit_type"] == "pass":
            action = WorkflowAction.PASS
            notify_config_key = "Pass"
            success_message = "passed"
        elif serializer.data["audit_type"] == "cancel":
            notify_config_key = "Cancel"
            success_message = "canceled"
            if auditor.workflow.engineer == serializer.data["engineer"]:
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
            workflow_audit_detail = auditor.operate(
                action, user, serializer.data["audit_remark"]
            )
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
        return Response({"msg": success_message})


class ExecuteWorkflow(views.APIView):
    """
    Execute workflows, including SQL release workflows and data archive workflows.
    """

    @extend_schema(
        summary="Execute Workflow",
        request=ExecuteWorkflowSerializer,
        description="Execute a workflow.",
    )
    def post(self, request):
        # Parameter validation
        serializer = ExecuteWorkflowSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        workflow_type = request.data["workflow_type"]
        workflow_id = request.data["workflow_id"]

        # Execute SQL release workflow
        if workflow_type == 2:
            mode = request.data["mode"]
            engineer = request.data["engineer"]
            user = Users.objects.get(username=engineer)

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
            async_task(
                "sql.archiver.archive",
                workflow_id,
                timeout=-1,
                task_name=f"archive-{workflow_id}",
            )

        return Response(
            {"msg": "Execution started. Please check workflow detail page for results."}
        )


class WorkflowLogList(generics.ListAPIView):
    """
    Get logs for a workflow.
    """

    pagination_class = CustomizedPagination
    serializer_class = WorkflowLogListSerializer
    queryset = WorkflowLog.objects.all()

    @extend_schema(exclude=True)
    def get(self, request):
        return Response({"detail": 'Method "GET" not allowed.'})

    @extend_schema(
        summary="Workflow Logs",
        request=WorkflowLogSerializer,
        responses={200: WorkflowLogListSerializer},
        description="Get logs of a workflow (pagination).",
    )
    def post(self, request):
        # Parameter validation
        serializer = WorkflowLogSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        audit_id = WorkflowAudit.objects.get(
            workflow_id=request.data["workflow_id"],
            workflow_type=request.data["workflow_type"],
        ).audit_id
        workflow_logs = self.queryset.filter(audit_id=audit_id).order_by("-id")
        page_log = self.paginate_queryset(queryset=workflow_logs)
        serializer_obj = self.get_serializer(page_log, many=True)
        data = {"data": serializer_obj.data}
        return self.get_paginated_response(data)
