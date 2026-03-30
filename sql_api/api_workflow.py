import datetime
import json
import logging

from django.contrib.auth.models import Group
from django.db import transaction
from django.db.models import Q
from django.http import HttpResponse
from django.shortcuts import get_object_or_404
from django_q.tasks import async_task
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiTypes
from rest_framework import views, generics, status, serializers, permissions
from rest_framework.exceptions import PermissionDenied
from rest_framework.response import Response

from common.config import SysConfig
from common.utils.const import Const, WorkflowStatus, WorkflowType, WorkflowAction
from sql.engines import get_engine
from sql.engines.models import ReviewResult, ReviewSet
from sql.models import (
    ResourceGroup,
    SqlWorkflow,
    SqlWorkflowContent,
    WorkflowAudit,
    WorkflowLog,
)
from sql.notify import notify_for_audit, notify_for_execute
from sql.query_privileges import _query_apply_audit_call_back
from sql.utils.resource_group import (
    WRITE_ACCESS_LEVELS,
    active_instance_grants,
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
from sql.utils.workflow_audit import Audit, AuditException, get_auditor

from .filters import WorkflowAuditFilter
from .pagination import CustomizedPagination
from .response import success_response
from .serializers import (
    AuditWorkflowSerializer,
    ExecuteCheckResultSerializer,
    ExecuteCheckSerializer,
    ExecuteWorkflowSerializer,
    WorkflowAuditListSerializer,
    WorkflowContentSerializer,
    WorkflowLogListSerializer,
    WorkflowScheduleSerializer,
    WorkflowSummarySerializer,
    WorkflowWindowSerializer,
)

logger = logging.getLogger("default")


def _can_view_workflow_module(user):
    return (
        user.is_superuser
        or user.has_perm("sql.menu_sqlworkflow")
        or user.has_perm("sql.audit_user")
    )


def _pending_review_workflow_ids(user):
    group_ids = [group.group_id for group in user_member_groups(user)]
    if user.is_superuser:
        auth_group_ids = [group.id for group in Group.objects.all()]
    else:
        auth_group_ids = [group.id for group in Group.objects.filter(user=user)]
    return list(
        WorkflowAudit.objects.filter(
            current_status=WorkflowStatus.WAITING,
            workflow_type=WorkflowType.SQL_REVIEW,
            group_id__in=group_ids,
            current_audit__in=auth_group_ids,
        ).values_list("workflow_id", flat=True)
    )


def _visible_workflow_queryset(user):
    queryset = (
        SqlWorkflow.objects.filter(is_offline_export=0, syntax_type__in=[1, 2])
        .select_related("instance")
        .order_by("-id")
    )
    if user.is_superuser or user.has_perm("sql.audit_user"):
        return queryset
    if user.has_perm("sql.sql_review") or user.has_perm(
        "sql.sql_execute_for_resource_group"
    ):
        return queryset.filter(
            group_id__in=[group.group_id for group in user_groups(user)]
        )
    return queryset.filter(engineer=user.username)


def _serialize_review_info(review_info):
    nodes = []
    for node in review_info.nodes:
        if node.is_auto_pass:
            nodes.append(
                {
                    "group_name": "Auto",
                    "is_auto_pass": True,
                    "is_current_node": False,
                    "is_passed_node": True,
                }
            )
            continue
        nodes.append(
            {
                "group_name": node.group.name if node.group else "Auto",
                "is_auto_pass": False,
                "is_current_node": node.is_current_node,
                "is_passed_node": node.is_passed_node,
            }
        )
    return nodes


def _current_reviewers(workflow, review_info):
    current_reviewers = []
    for node in review_info.nodes:
        if not node.is_current_node or not node.group:
            continue
        for reviewer in node.group.user_set.filter(is_active=1):
            group_names = [group.group_name for group in user_member_groups(reviewer)]
            if workflow.group_name in group_names:
                current_reviewers.append(
                    {
                        "username": reviewer.username,
                        "display": reviewer.display,
                    }
                )
    return current_reviewers


def _workflow_logs(workflow):
    audit = Audit.detail_by_workflow_id(
        workflow_id=workflow.id,
        workflow_type=WorkflowType.SQL_REVIEW,
    )
    if not audit:
        return []
    return [
        {
            "operation_type_desc": log.operation_type_desc,
            "operation_info": log.operation_info,
            "operator_display": log.operator_display,
            "operation_time": log.operation_time,
        }
        for log in WorkflowLog.objects.filter(audit_id=audit.audit_id).order_by("-id")
    ]


def _last_operation_info(workflow):
    audit = Audit.detail_by_workflow_id(
        workflow_id=workflow.id,
        workflow_type=WorkflowType.SQL_REVIEW,
    )
    if not audit:
        return ""
    last_log = (
        WorkflowLog.objects.filter(audit_id=audit.audit_id).order_by("-id").first()
    )
    return last_log.operation_info if last_log else ""


def _scheduled_run_date(workflow):
    if workflow.status != "workflow_timingtask":
        return None
    job_id = Const.workflowJobprefix["sqlreview"] + "-" + str(workflow.id)
    job = task_info(job_id)
    return job.next_run if job else None


def _submission_scope(user):
    instances = (
        user_instances(user, tag_codes=["can_write"])
        .prefetch_related("resource_group")
        .order_by("instance_name", "id")
    )
    direct_group_ids = {group.group_id for group in user_groups(user)}
    temporary_groups_by_instance = {}

    for grant in (
        active_instance_grants(user)
        .filter(access_level__in=WRITE_ACCESS_LEVELS, resource_group__is_deleted=0)
        .select_related("resource_group")
    ):
        groups = temporary_groups_by_instance.setdefault(grant.instance_id, {})
        groups[grant.resource_group_id] = grant.resource_group.group_name

    resource_groups = {}
    instance_payload = []
    for instance in instances:
        allowed_groups = {
            group_id: group_name
            for group_id, group_name in instance.resource_group.filter(
                is_deleted=0, group_id__in=direct_group_ids
            ).values_list("group_id", "group_name")
        }
        allowed_groups.update(temporary_groups_by_instance.get(instance.id, {}))
        if not allowed_groups:
            continue

        sorted_groups = sorted(
            allowed_groups.items(), key=lambda item: (item[1], item[0])
        )
        for group_id, group_name in sorted_groups:
            resource_groups[group_id] = group_name
        instance_payload.append(
            {
                "id": instance.id,
                "instance_name": instance.instance_name,
                "db_type": instance.db_type,
                "type": instance.type,
                "group_ids": [group_id for group_id, _ in sorted_groups],
                "group_names": [group_name for _, group_name in sorted_groups],
            }
        )

    resource_group_payload = [
        {
            "group_id": group_id,
            "group_name": group_name,
            "label": group_name,
        }
        for group_id, group_name in sorted(
            resource_groups.items(), key=lambda item: (item[1], item[0])
        )
    ]

    return {
        "resource_groups": resource_group_payload,
        "instances": instance_payload,
    }


def _serialize_workflow_detail(workflow, request_user):
    serializer_data = WorkflowSummarySerializer(workflow).data
    audit_handler = get_auditor(workflow=workflow)
    review_info = audit_handler.get_review_info()

    if workflow.status != "workflow_autoreviewwrong":
        is_can_review = Audit.can_review(
            request_user, workflow.id, WorkflowType.SQL_REVIEW
        )
        is_can_execute = can_execute(request_user, workflow.id)
        is_can_timingtask = can_timingtask(request_user, workflow.id)
        is_can_cancel = can_cancel(request_user, workflow.id)
        is_can_rollback = can_rollback(request_user, workflow.id)
    else:
        is_can_review = False
        is_can_execute = False
        is_can_timingtask = False
        is_can_cancel = False
        is_can_rollback = False

    serializer_data.update(
        {
            "workflow_type": WorkflowType.SQL_REVIEW,
            "sql_content": workflow.sqlworkflowcontent.sql_content,
            "review_info": _serialize_review_info(review_info),
            "current_reviewers": _current_reviewers(workflow, review_info),
            "logs": _workflow_logs(workflow),
            "last_operation_info": _last_operation_info(workflow),
            "run_date": _scheduled_run_date(workflow),
            "manual_execution_enabled": bool(SysConfig().get("manual")),
            "is_can_review": is_can_review,
            "is_can_execute": is_can_execute,
            "is_can_timingtask": is_can_timingtask,
            "is_can_cancel": is_can_cancel,
            "is_can_rollback": is_can_rollback,
            "is_requester": request_user.username == workflow.engineer,
        }
    )
    return serializer_data


def _load_workflow_result_rows(workflow):
    if workflow.status in ["workflow_finish", "workflow_exception"]:
        raw_rows = workflow.sqlworkflowcontent.execute_result
        source = "execution"
    else:
        raw_rows = workflow.sqlworkflowcontent.review_content
        source = "review"

    if not raw_rows:
        return {"source": source, "rows": [], "column_list": []}

    try:
        rows = json.loads(raw_rows)
        if rows and isinstance(rows[-1], list):
            review_set = ReviewSet()
            for row in rows:
                review_set.rows.append(ReviewResult(inception_result=row))
            rows = review_set.to_dict()
    except (json.decoder.JSONDecodeError, IndexError):
        rows = [
            {
                "id": 1,
                "sql": workflow.sqlworkflowcontent.sql_content,
                "errormessage": (
                    "Json decode failed. Execution result JSON parsing failed. "
                    "Please contact admin."
                ),
            }
        ]

    column_list = list(rows[0].keys()) if rows else []
    return {"source": source, "rows": rows, "column_list": column_list}


def _rollback_download_content(rollback_rows):
    return "".join(f"/*{row[0]}*/\n{row[1]}\n" for row in rollback_rows)


class ExecuteCheck(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="SQL Check",
        request=ExecuteCheckSerializer,
        responses={200: ExecuteCheckResultSerializer},
        description="Perform syntax checks for the provided SQL using request body.",
    )
    def post(self, request):
        serializer = ExecuteCheckSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        instance = serializer.get_instance()
        try:
            db_name = serializer.validated_data["db_name"]
            full_sql = serializer.validated_data["full_sql"].strip()
            check_engine = get_engine(instance=instance)
            db_name = check_engine.escape_string(db_name)
            check_result = check_engine.execute_check(db_name=db_name, sql=full_sql)
        except Exception as exc:
            raise serializers.ValidationError({"errors": f"{exc}"})
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


class WorkflowSubmissionMetadata(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Workflow Submission Metadata",
        responses={200: OpenApiTypes.OBJECT},
        description="List resource groups, submit-eligible instances, and workflow config flags for the SPA submission page.",
    )
    def get(self, request):
        submission_scope = _submission_scope(request.user)
        sys_config = SysConfig()
        return success_response(
            data={
                "resource_groups": submission_scope["resource_groups"],
                "instances": submission_scope["instances"],
                "enable_backup_switch": bool(sys_config.get("enable_backup_switch")),
                "manual_execution_enabled": bool(sys_config.get("manual")),
            }
        )


class WorkflowApprovalPreview(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Workflow Approval Preview",
        parameters=[
            OpenApiParameter(
                name="group_id",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Resource group ID.",
            )
        ],
        responses={200: OpenApiTypes.OBJECT},
        description="Resolve the approval chain for a SQL release workflow before submission.",
    )
    def get(self, request):
        group_id = request.query_params.get("group_id")
        if not group_id:
            raise serializers.ValidationError({"errors": "group_id is required."})
        try:
            group_id = int(group_id)
        except (TypeError, ValueError):
            raise serializers.ValidationError(
                {"errors": "group_id must be an integer."}
            )

        submission_scope = _submission_scope(request.user)
        allowed_group_ids = {
            group["group_id"] for group in submission_scope["resource_groups"]
        }
        if not request.user.is_superuser and group_id not in allowed_group_ids:
            raise PermissionDenied("You do not have access to this resource group.")

        resource_group = get_object_or_404(ResourceGroup, pk=group_id, is_deleted=0)
        audit_auth_groups = Audit.settings(group_id, WorkflowType.SQL_REVIEW)
        if audit_auth_groups is None:
            raise serializers.ValidationError(
                {"errors": ("Approval flow is not configured for this resource group.")}
            )
        if audit_auth_groups == "":
            review_info = [
                {
                    "group_name": "Auto",
                    "is_auto_pass": True,
                    "is_current_node": False,
                    "is_passed_node": True,
                }
            ]
            readable = "No approval required"
        else:
            group_names = []
            review_info = []
            for auth_group_id in audit_auth_groups.split(","):
                group = Group.objects.get(id=int(auth_group_id))
                group_names.append(group.name)
                review_info.append(
                    {
                        "group_name": group.name,
                        "is_auto_pass": False,
                        "is_current_node": False,
                        "is_passed_node": False,
                    }
                )
            readable = " -> ".join(group_names)

        return success_response(
            data={
                "group_id": resource_group.group_id,
                "group_name": resource_group.group_name,
                "audit_auth_groups": audit_auth_groups,
                "display": readable,
                "review_info": review_info,
            }
        )


class WorkflowList(generics.GenericAPIView):
    permission_classes = [permissions.IsAuthenticated]
    pagination_class = CustomizedPagination

    def get_serializer_class(self):
        if self.request.method == "POST":
            return WorkflowContentSerializer
        return WorkflowSummarySerializer

    def _filter_queryset(self, queryset):
        params = self.request.query_params
        scope = params.get("scope", "").strip()
        status_value = params.get("status", "").strip()
        syntax_type = params.get("syntax_type", "").strip()
        instance_id = params.get("instance_id", "").strip()
        group_id = params.get("group_id", "").strip()
        start_date = params.get("start_date", "").strip()
        end_date = params.get("end_date", "").strip()
        search = params.get("search", "").strip()

        if scope == "mine":
            queryset = queryset.filter(engineer=self.request.user.username)
        elif scope == "pending_review":
            queryset = queryset.filter(
                id__in=_pending_review_workflow_ids(self.request.user)
            )

        if status_value:
            queryset = queryset.filter(status=status_value)

        if syntax_type:
            try:
                queryset = queryset.filter(syntax_type=int(syntax_type))
            except ValueError:
                raise serializers.ValidationError(
                    {"errors": "syntax_type must be an integer."}
                )

        if instance_id:
            try:
                queryset = queryset.filter(instance_id=int(instance_id))
            except ValueError:
                raise serializers.ValidationError(
                    {"errors": "instance_id must be an integer."}
                )

        if group_id:
            try:
                queryset = queryset.filter(group_id=int(group_id))
            except ValueError:
                raise serializers.ValidationError(
                    {"errors": "group_id must be an integer."}
                )

        if start_date:
            try:
                queryset = queryset.filter(
                    create_time__date__gte=datetime.date.fromisoformat(start_date)
                )
            except ValueError:
                raise serializers.ValidationError(
                    {"errors": "start_date must be in YYYY-MM-DD format."}
                )

        if end_date:
            try:
                queryset = queryset.filter(
                    create_time__date__lte=datetime.date.fromisoformat(end_date)
                )
            except ValueError:
                raise serializers.ValidationError(
                    {"errors": "end_date must be in YYYY-MM-DD format."}
                )

        if search:
            queryset = queryset.filter(
                Q(workflow_name__icontains=search)
                | Q(engineer_display__icontains=search)
                | Q(group_name__icontains=search)
                | Q(instance__instance_name__icontains=search)
                | Q(db_name__icontains=search)
                | Q(demand_url__icontains=search)
            )

        return queryset

    @extend_schema(
        summary="SQL Release Workflow List",
        responses={200: WorkflowSummarySerializer},
        parameters=[
            OpenApiParameter(
                name="scope", type=OpenApiTypes.STR, location=OpenApiParameter.QUERY
            ),
            OpenApiParameter(
                name="search", type=OpenApiTypes.STR, location=OpenApiParameter.QUERY
            ),
            OpenApiParameter(
                name="status", type=OpenApiTypes.STR, location=OpenApiParameter.QUERY
            ),
            OpenApiParameter(
                name="syntax_type",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
            ),
            OpenApiParameter(
                name="instance_id",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
            ),
            OpenApiParameter(
                name="group_id", type=OpenApiTypes.INT, location=OpenApiParameter.QUERY
            ),
            OpenApiParameter(
                name="start_date",
                type=OpenApiTypes.DATE,
                location=OpenApiParameter.QUERY,
            ),
            OpenApiParameter(
                name="end_date", type=OpenApiTypes.DATE, location=OpenApiParameter.QUERY
            ),
            OpenApiParameter(
                name="page", type=OpenApiTypes.INT, location=OpenApiParameter.QUERY
            ),
            OpenApiParameter(
                name="size", type=OpenApiTypes.INT, location=OpenApiParameter.QUERY
            ),
        ],
        description="List DDL/DML workflows visible to the current user for the SPA workflow module.",
    )
    def get(self, request):
        if not _can_view_workflow_module(request.user):
            raise PermissionDenied("You do not have permission to view workflow list.")

        workflows = self._filter_queryset(_visible_workflow_queryset(request.user))
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


class WorkflowDetail(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Workflow Detail",
        responses={200: OpenApiTypes.OBJECT},
        description="Return workflow summary, approval flow, permissions, logs, and action flags for the SPA detail drawer.",
    )
    def get(self, request, workflow_id):
        workflow = get_object_or_404(
            SqlWorkflow.objects.select_related("instance").filter(
                is_offline_export=0, syntax_type__in=[1, 2]
            ),
            pk=workflow_id,
        )
        if not can_view(request.user, workflow_id):
            raise PermissionDenied("You do not have permission to view this workflow.")
        return success_response(data=_serialize_workflow_detail(workflow, request.user))


class WorkflowContentDetail(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Workflow Content Result",
        responses={200: OpenApiTypes.OBJECT},
        description="Return review or execution rows for a workflow detail result table.",
    )
    def get(self, request, workflow_id):
        workflow = get_object_or_404(SqlWorkflow, pk=workflow_id)
        if not can_view(request.user, workflow_id):
            raise PermissionDenied("You do not have permission to view this workflow.")
        return success_response(data=_load_workflow_result_rows(workflow))


class WorkflowRollbackDetail(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Workflow Rollback SQL",
        responses={200: OpenApiTypes.OBJECT},
        description="Return rollback SQL pairs for a completed workflow.",
    )
    def get(self, request, workflow_id):
        if not can_rollback(request.user, workflow_id):
            raise PermissionDenied("You do not have permission to view rollback SQL.")
        workflow = get_object_or_404(
            SqlWorkflow.objects.select_related("instance"), pk=workflow_id
        )
        try:
            query_engine = get_engine(instance=workflow.instance)
            rollback_rows = query_engine.get_rollback(workflow=workflow)
        except Exception as exc:
            logger.error("Failed to load rollback SQL", exc_info=True)
            raise serializers.ValidationError({"errors": str(exc)})

        if request.query_params.get("download") == "true":
            response = HttpResponse(
                _rollback_download_content(rollback_rows),
                content_type="application/sql",
            )
            response["Content-Disposition"] = (
                f'attachment; filename="rollback_{workflow_id}.sql"'
            )
            return response

        return success_response(
            data={
                "rows": rollback_rows,
                "download_content": _rollback_download_content(rollback_rows),
            }
        )


class WorkflowExecutionWindowUpdate(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Update Workflow Execution Window",
        request=WorkflowWindowSerializer,
        responses={200: OpenApiTypes.OBJECT},
        description="Allow a reviewer to change the executable time window for a workflow.",
    )
    def patch(self, request, workflow_id):
        if not Audit.can_review(request.user, workflow_id, WorkflowType.SQL_REVIEW):
            raise PermissionDenied("You are not allowed to operate on this workflow.")

        serializer = WorkflowWindowSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        data = serializer.validated_data
        SqlWorkflow(
            id=workflow_id,
            run_date_start=data.get("run_date_start"),
            run_date_end=data.get("run_date_end"),
        ).save(update_fields=["run_date_start", "run_date_end"])
        return success_response(detail="Execution window updated.")


class WorkflowScheduleCreate(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Schedule Workflow Execution",
        request=WorkflowScheduleSerializer,
        responses={200: OpenApiTypes.OBJECT},
        description="Create or update the scheduled execution time for a workflow.",
    )
    def post(self, request, workflow_id):
        serializer = WorkflowScheduleSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        run_date = serializer.validated_data["run_date"]

        if run_date < datetime.datetime.now(run_date.tzinfo):
            raise serializers.ValidationError(
                {"errors": "Time cannot be earlier than current time."}
            )
        if can_timingtask(request.user, workflow_id) is False:
            raise PermissionDenied("You are not allowed to operate on this workflow.")
        if on_correct_time_period(workflow_id, run_date) is False:
            raise serializers.ValidationError(
                {
                    "errors": (
                        "Current time is outside the executable window. "
                        "Please resubmit the workflow if you need to change execution time."
                    )
                }
            )

        workflow = SqlWorkflow.objects.get(id=workflow_id)
        schedule_name = f"sqlreview-timing-{workflow_id}"
        with transaction.atomic():
            workflow.status = "workflow_timingtask"
            workflow.save(update_fields=["status"])
            add_sql_schedule(schedule_name, run_date, workflow_id)
            audit_id = Audit.detail_by_workflow_id(
                workflow_id=workflow_id,
                workflow_type=WorkflowType.SQL_REVIEW,
            ).audit_id
            Audit.add_log(
                audit_id=audit_id,
                operation_type=4,
                operation_type_desc="Scheduled Execution",
                operation_info="Scheduled execution time: {}".format(run_date),
                operator=request.user.username,
                operator_display=request.user.display,
            )
        return success_response(detail="Workflow scheduled for execution.")


class WorkflowAuditList(generics.ListAPIView):
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
        group_ids = [group.group_id for group in user_member_groups(user)]
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
    @extend_schema(
        summary="Audit Workflow",
        request=AuditWorkflowSerializer,
        description="Audit a workflow (approve or terminate).",
    )
    def post(self, request, workflow_id):
        data = request.data.copy()
        data["workflow_id"] = workflow_id
        serializer = AuditWorkflowSerializer(data=data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        data = serializer.validated_data
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
            elif Audit.can_review(user, workflow_id, WorkflowType.SQL_REVIEW):
                action = WorkflowAction.REJECT
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
        except AuditException as exc:
            raise serializers.ValidationError(
                {"errors": f"Operation failed, {str(exc)}"}
            )

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
                auditor.workflow.status = "workflow_abort"
                auditor.workflow.save(update_fields=["status"])
        elif auditor.workflow_type == WorkflowType.ARCHIVE:
            auditor.workflow.status = auditor.audit.current_status
            auditor.workflow.state = (
                auditor.audit.current_status == WorkflowStatus.PASSED
            )
            auditor.workflow.save(update_fields=["status", "state"])

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
    @extend_schema(
        summary="Execute Workflow",
        request=ExecuteWorkflowSerializer,
        description="Execute a workflow.",
    )
    def post(self, request, workflow_id):
        data = request.data.copy()
        data["workflow_id"] = workflow_id
        serializer = ExecuteWorkflowSerializer(data=data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        data = serializer.validated_data

        workflow_type = data["workflow_type"]
        if workflow_type == 2:
            mode = data["mode"]
            user = request.user
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

            audit_id = Audit.detail_by_workflow_id(
                workflow_id=workflow_id,
                workflow_type=WorkflowType.SQL_REVIEW,
            ).audit_id

            if mode == "auto":
                SqlWorkflow(id=workflow_id, status="workflow_queuing").save(
                    update_fields=["status"]
                )
                del_schedule(f"sqlreview-timing-{workflow_id}")
                async_task(
                    "sql.utils.execute_sql.execute",
                    workflow_id,
                    user,
                    hook="sql.utils.execute_sql.execute_callback",
                    timeout=-1,
                    task_name=f"sqlreview-execute-{workflow_id}",
                )
                Audit.add_log(
                    audit_id=audit_id,
                    operation_type=5,
                    operation_type_desc="Execute Workflow",
                    operation_info="Workflow queued for execution",
                    operator=user.username,
                    operator_display=user.display,
                )
            elif mode == "manual":
                SqlWorkflow(
                    id=workflow_id,
                    status="workflow_finish",
                    finish_time=datetime.datetime.now(),
                ).save(update_fields=["status", "finish_time"])
                Audit.add_log(
                    audit_id=audit_id,
                    operation_type=6,
                    operation_type_desc="Manual Workflow",
                    operation_info="Confirmed manual execution completed",
                    operator=user.username,
                    operator_display=user.display,
                )
                sys_config = SysConfig()
                is_notified = (
                    "Execute" in sys_config.get("notify_phase_control").split(",")
                    if sys_config.get("notify_phase_control")
                    else True
                )
                if is_notified:
                    notify_for_execute(workflow=SqlWorkflow.objects.get(id=workflow_id))
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
