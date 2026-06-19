import datetime
import json
import logging
import re
import ast
import uuid
from types import SimpleNamespace

from django.db import transaction
from django.db.models import Q
from django.shortcuts import get_object_or_404
from common.task_queue import async_task
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiTypes
from rest_framework import views, generics, status, serializers, permissions
from rest_framework.exceptions import PermissionDenied
from rest_framework.response import Response

from common.config import SysConfig
from common.utils.const import Const, WorkflowStatus, WorkflowType, WorkflowAction
from sql.engines.mysql_ddl import MysqlDDLExecutorError
from sql.engines.models import ReviewResult, ReviewSet
from sql.mailbox import (
    emit_execution_finished_notifications,
    resolve_mailbox_items,
    sync_approval_notifications,
    sync_execution_needed_notifications,
)
from sql.offlinedownload import download_export_file
from sql.models import (
    Instance,
    Team,
    SqlWorkflow,
    SqlWorkflowContent,
    WorkflowAudit,
    WorkflowLog,
    TeamPermissionGroup,
)
from sql.notify import notify_for_audit, notify_for_execute
from sql.query_privileges import _query_apply_audit_call_back
from sql.utils.team import (
    DDL_ACCESS_LEVELS,
    READ_ACCESS_LEVELS,
    WRITE_ACCESS_LEVELS,
    active_instance_grants,
    permission_group_label,
    user_groups,
    user_instances,
    user_has_instance_workflow_access,
    teams_for_role,
    resource_role_users,
)
from sql.utils.sql_utils import generate_sql, get_syntax_type
from sql.utils.sql_review import (
    can_cancel,
    can_execute,
    can_timingtask,
    can_view,
    on_correct_time_period,
)
from sql.utils.tasks import add_sql_schedule, del_schedule, task_info
from sql.utils.workflow_audit import (
    Audit,
    AuditV2,
    get_auditor,
    AuditException,
    reviewable_audit_ids,
)
from api_workflows.filters import WorkflowAuditFilter
from api_core.pagination import CustomizedPagination
from api_core.response import success_response
from api_workflows.serializers import (
    ExecuteCheckResultSerializer,
    ExecuteCheckSerializer,
    WorkflowParseResultSerializer,
    WorkflowParseSerializer,
    WorkflowAuditListSerializer,
    AuditWorkflowSerializer,
    ExecuteWorkflowSerializer,
    WorkflowContentSerializer,
    WorkflowLogListSerializer,
)
from api_agents.models import AgentCommandType
from api_agents.services import (
    AgentCommandDispatchError,
    AgentCommandExecutionError,
    command_capable_assignment_for_instance,
    dispatch_sql_workflow_to_agent,
    filter_agent_runnable_instances,
    review_set_from_agent_result,
    run_agent_command_sync,
)

logger = logging.getLogger("default")
LOAD_DATA_PATTERN = re.compile(r"^\s*load\s+data\b", re.IGNORECASE)


def _syntax_types_for_access_level(access_level):
    if access_level in DDL_ACCESS_LEVELS:
        return {1, 2}
    if access_level in WRITE_ACCESS_LEVELS:
        return {2}
    return set()


def _classify_statement_syntax(statement, db_type="mysql"):
    syntax_name = get_syntax_type(statement, parser=True, db_type=db_type)
    if syntax_name not in {"DDL", "DML"}:
        syntax_name = get_syntax_type(statement, parser=False, db_type=db_type)
    if syntax_name == "DDL":
        return 1
    if syntax_name == "DML":
        return 2
    return None


def _ensure_no_load_data_statements(text):
    for row in generate_sql(text):
        if LOAD_DATA_PATTERN.match(row["sql"]):
            raise serializers.ValidationError(
                {
                    "errors": (
                        "LOAD DATA statements are not supported for workflow submission."
                    )
                }
            )


def _detected_workflow_syntax_types(sql_text, db_type="mysql"):
    syntax_types = set()
    for row in generate_sql(sql_text):
        syntax_type = _classify_statement_syntax(row["sql"], db_type=db_type)
        if syntax_type is not None:
            syntax_types.add(syntax_type)
    return syntax_types


def _authorize_workflow_check_dispatch(user, instance, sql_text):
    if user.is_superuser:
        return

    syntax_types = _detected_workflow_syntax_types(
        sql_text, db_type=instance.db_type
    ) or {2}
    if all(
        user_has_instance_workflow_access(user, instance, syntax_type)
        for syntax_type in syntax_types
    ):
        return
    raise serializers.ValidationError(
        {"errors": "You do not have permission to submit SQL for this instance."}
    )


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
    download_available = serializers.SerializerMethodField()

    def get_download_available(self, obj):
        return _workflow_download_available(obj)

    class Meta:
        model = SqlWorkflow
        fields = (
            "id",
            "workflow_name",
            "demand_url",
            "team_id",
            "team_name",
            "instance_id",
            "instance_name",
            "instance_db_type",
            "db_name",
            "schema_name",
            "syntax_type",
            "syntax_type_label",
            "is_offline_export",
            "export_format",
            "file_name",
            "download_available",
            "status",
            "status_label",
            "engineer",
            "engineer_display",
            "run_date_start",
            "run_date_end",
            "create_time",
            "finish_time",
        )


class WorkflowTeamLookupSerializer(serializers.ModelSerializer):
    class Meta:
        model = Team
        fields = ("team_id", "team_name")


class WorkflowInstanceLookupSerializer(serializers.ModelSerializer):
    label = serializers.SerializerMethodField()
    teams = serializers.SerializerMethodField()

    def get_label(self, obj):
        return f"{obj.instance_name} | {obj.db_type} | {obj.host}"

    def get_teams(self, obj):
        groups_by_id = {
            group.team_id: group for group in obj.resource_group.filter(is_deleted=0)
        }
        for group in self.context.get("temporary_instance_groups", {}).get(obj.id, []):
            groups_by_id.setdefault(group.team_id, group)
        queryset = sorted(
            groups_by_id.values(), key=lambda group: (group.team_name, group.team_id)
        )
        return WorkflowTeamLookupSerializer(queryset, many=True).data

    class Meta:
        model = Instance
        fields = (
            "id",
            "instance_name",
            "db_type",
            "type",
            "host",
            "label",
            "teams",
        )


class WorkflowMetadataSerializer(serializers.Serializer):
    manual_execution_enabled = serializers.BooleanField()
    teams = WorkflowTeamLookupSerializer(many=True)
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
    executor = serializers.ChoiceField(
        choices=["direct"],
        required=False,
        allow_null=True,
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
            "team_name": node.group.name if node.group else "Auto",
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
        for user in resource_role_users([node.group.code], workflow.team_id):
            if user.username in seen_usernames:
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


def _workflow_download_available(workflow):
    return bool(
        workflow.is_offline_export
        and workflow.status == "workflow_finish"
        and workflow.file_name
    )


def _is_mysql_ddl_workflow(workflow):
    return (
        workflow.instance.db_type == "mysql"
        and workflow.syntax_type == 1
        and not workflow.is_offline_export
    )


def _parse_schedule_kwargs(raw_kwargs):
    if isinstance(raw_kwargs, dict):
        return raw_kwargs
    if not raw_kwargs:
        return {}
    if isinstance(raw_kwargs, str):
        for parser in (json.loads, ast.literal_eval):
            try:
                parsed = parser(raw_kwargs)
            except Exception:
                continue
            if isinstance(parsed, dict):
                return parsed
    return {}


def _extract_schedule_executor(schedule):
    if not schedule:
        return None
    kwargs = _parse_schedule_kwargs(getattr(schedule, "kwargs", None))
    execution_options = kwargs.get("execution_options")
    if isinstance(execution_options, str):
        execution_options = _parse_schedule_kwargs(execution_options)
    if isinstance(execution_options, dict):
        return execution_options.get("executor")
    if isinstance(kwargs.get("executor"), str):
        return kwargs.get("executor")
    return None


def _get_mysql_ddl_executor_state(workflow):
    if not _is_mysql_ddl_workflow(workflow):
        return [], {}
    if command_capable_assignment_for_instance(workflow.instance_id) is None:
        return [], {
            "direct": "No online command-capable agent is assigned to this MySQL service."
        }
    return [{"id": "direct", "label": "Direct", "kind": "direct"}], {}


def _resolve_mysql_ddl_executor(workflow, executor_id=None, preflight=False):
    if not _is_mysql_ddl_workflow(workflow):
        return None
    if executor_id not in (None, "", "direct"):
        raise MysqlDDLExecutorError(
            "Only direct agent execution is available for MySQL DDL workflows."
        )
    return SimpleNamespace(executor_id="direct", label="Direct", kind="direct")


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
        can_manual_execute_now = False
    else:
        can_review_now = Audit.can_review(user, workflow.id, WorkflowType.SQL_REVIEW)
        can_execute_now = can_execute(user, workflow.id)
        can_schedule_now = can_timingtask(user, workflow.id)
        can_cancel_now = can_cancel(user, workflow.id)
        can_manual_execute_now = can_execute_now

    schedule = task_info(f"sqlreview-timing-{workflow.id}")
    manual_enabled = bool(SysConfig().get("manual"))
    scheduled_executor = _extract_schedule_executor(schedule)
    available_executors, executor_blockers = _get_mysql_ddl_executor_state(workflow)
    if _is_mysql_ddl_workflow(workflow) and not available_executors:
        can_execute_now = False
        can_schedule_now = False

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
            "scheduled_executor": scheduled_executor,
            "available_executors": available_executors,
            "executor_blockers": executor_blockers,
            "is_can_review": can_review_now,
            "is_can_reject": can_review_now,
            "is_can_execute": can_execute_now,
            "is_can_schedule": can_schedule_now,
            "is_can_cancel": can_cancel_now,
            "is_can_abort": can_cancel_now and workflow.engineer == user.username,
            "is_can_manual_execute": can_manual_execute_now and manual_enabled,
            "is_can_edit_execution_window": can_review_now,
            "manual_execution_enabled": manual_enabled,
            "download_available": _workflow_download_available(workflow),
        }
    )
    return payload


def _can_access_workflow_module(user):
    return any(
        [
            user.is_superuser,
            user.has_perm("sql.menu_sqlworkflow"),
            user.has_perm("sql.menu_sqlexportworkflow"),
            user.has_perm("sql.sql_submit"),
            user.has_perm("sql.sqlexport_submit"),
            user.has_perm("sql.offline_download"),
            user.has_perm("sql.audit_user"),
            teams_for_role(user, TeamPermissionGroup.WORKFLOW_REQUESTER).exists(),
            user_instances(user, tag_codes=["can_write"]).exists(),
        ]
    )


def _can_submit_export_workflow(user):
    return (
        user.is_superuser
        or user.has_perm(TeamPermissionGroup.EXPORT_WORKFLOW_REQUESTER)
        or teams_for_role(user, TeamPermissionGroup.EXPORT_WORKFLOW_REQUESTER).exists()
    )


def _pending_review_workflow_ids(user):
    return list(
        WorkflowAudit.objects.filter(
            audit_id__in=reviewable_audit_ids(
                user, workflow_type=WorkflowType.SQL_REVIEW
            )
        ).values_list("workflow_id", flat=True)
    )


def _workflow_metadata_teams(user):
    groups_by_id = {
        group.team_id: group
        for group in teams_for_role(user, TeamPermissionGroup.WORKFLOW_REQUESTER)
    }
    for grant in (
        active_instance_grants(user)
        .filter(
            access_level__in=(WRITE_ACCESS_LEVELS | DDL_ACCESS_LEVELS),
            team__is_deleted=0,
        )
        .select_related("team")
    ):
        groups_by_id.setdefault(grant.team_id, grant.team)
    return sorted(
        groups_by_id.values(), key=lambda group: (group.team_name, group.team_id)
    )


def _workflow_metadata_instance_groups(user):
    groups_by_instance = {}
    for grant in (
        active_instance_grants(user)
        .filter(
            access_level__in=(WRITE_ACCESS_LEVELS | DDL_ACCESS_LEVELS),
            team__is_deleted=0,
        )
        .select_related("team")
    ):
        instance_groups = groups_by_instance.setdefault(grant.instance_id, {})
        instance_groups.setdefault(grant.team_id, grant.team)
    return {
        instance_id: sorted(
            instance_groups.values(),
            key=lambda group: (group.team_name, group.team_id),
        )
        for instance_id, instance_groups in groups_by_instance.items()
    }


def _workflow_submission_scope(user):
    can_submit_directly = (
        user.is_superuser
        or teams_for_role(user, TeamPermissionGroup.WORKFLOW_REQUESTER).exists()
    )
    instances = (
        filter_agent_runnable_instances(user_instances(user))
        .prefetch_related("resource_group")
        .order_by("instance_name", "id")
    )
    direct_group_ids = (
        {
            group.team_id
            for group in teams_for_role(user, TeamPermissionGroup.WORKFLOW_REQUESTER)
            if group.is_deleted == 0
        }
        if can_submit_directly
        else set()
    )
    temporary_groups_by_instance = {}

    for grant in (
        active_instance_grants(user)
        .filter(
            access_level__in=(WRITE_ACCESS_LEVELS | DDL_ACCESS_LEVELS),
            team__is_deleted=0,
        )
        .select_related("team")
    ):
        groups = temporary_groups_by_instance.setdefault(grant.instance_id, {})
        group_info = groups.setdefault(
            grant.team_id,
            {"team_name": grant.team.team_name, "syntax_types": set()},
        )
        group_info["syntax_types"].update(
            _syntax_types_for_access_level(grant.access_level)
        )

    teams = {}
    instance_payload = []
    for instance in instances:
        allowed_groups = {}
        allowed_syntax_types = set()

        if (
            can_submit_directly
            and instance.instance_tag.filter(tag_code="can_write", active=True).exists()
        ):
            direct_groups = {
                team_id: {"team_name": team_name, "syntax_types": {1, 2}}
                for team_id, team_name in instance.resource_group.filter(
                    is_deleted=0, team_id__in=direct_group_ids
                ).values_list("team_id", "team_name")
            }
            allowed_groups.update(direct_groups)
            allowed_syntax_types.update({1, 2})

        for team_id, group_info in temporary_groups_by_instance.get(
            instance.id, {}
        ).items():
            allowed_groups[team_id] = {
                "team_name": group_info["team_name"],
                "syntax_types": set(group_info["syntax_types"]),
            }
            allowed_syntax_types.update(group_info["syntax_types"])

        if not allowed_groups:
            continue

        sorted_groups = sorted(
            allowed_groups.items(), key=lambda item: (item[1]["team_name"], item[0])
        )
        for team_id, group_info in sorted_groups:
            teams[team_id] = group_info["team_name"]
        instance_payload.append(
            {
                "id": instance.id,
                "instance_name": instance.instance_name,
                "db_type": instance.db_type,
                "type": instance.type,
                "team_ids": [team_id for team_id, _ in sorted_groups],
                "team_names": [
                    group_info["team_name"] for _, group_info in sorted_groups
                ],
                "allowed_syntax_types": sorted(allowed_syntax_types),
            }
        )

    return {
        "teams": [
            {
                "team_id": team_id,
                "team_name": team_name,
                "label": team_name,
            }
            for team_id, team_name in sorted(
                teams.items(), key=lambda item: (item[1], item[0])
            )
        ],
        "instances": instance_payload,
    }


def _export_submission_scope(user):
    instances = (
        filter_agent_runnable_instances(user_instances(user, tag_codes=["can_read"]))
        .prefetch_related("resource_group")
        .order_by("instance_name", "id")
    )
    direct_group_ids = (
        {
            group.team_id
            for group in teams_for_role(
                user, TeamPermissionGroup.EXPORT_WORKFLOW_REQUESTER
            )
            if group.is_deleted == 0
        }
        if _can_submit_export_workflow(user)
        else set()
    )
    temporary_groups_by_instance = {}

    for grant in (
        active_instance_grants(user)
        .filter(access_level__in=READ_ACCESS_LEVELS, team__is_deleted=0)
        .select_related("team")
    ):
        groups = temporary_groups_by_instance.setdefault(grant.instance_id, {})
        groups[grant.team_id] = grant.team

    teams = {}
    instance_payload = []
    for instance in instances:
        allowed_groups = {}

        if (
            _can_submit_export_workflow(user)
            and instance.instance_tag.filter(tag_code="can_read", active=True).exists()
        ):
            direct_groups = {
                team_id: team_name
                for team_id, team_name in instance.resource_group.filter(
                    is_deleted=0, team_id__in=direct_group_ids
                ).values_list("team_id", "team_name")
            }
            allowed_groups.update(direct_groups)

        for team_id, group in temporary_groups_by_instance.get(instance.id, {}).items():
            allowed_groups[team_id] = group.team_name

        if not allowed_groups:
            continue

        sorted_groups = sorted(
            allowed_groups.items(), key=lambda item: (item[1], item[0])
        )
        for team_id, team_name in sorted_groups:
            teams[team_id] = team_name
        instance_payload.append(
            {
                "id": instance.id,
                "instance_name": instance.instance_name,
                "db_type": instance.db_type,
                "type": instance.type,
                "team_ids": [team_id for team_id, _ in sorted_groups],
                "team_names": [team_name for _, team_name in sorted_groups],
                "allowed_syntax_types": [3],
            }
        )

    return {
        "teams": [
            {
                "team_id": team_id,
                "team_name": team_name,
                "label": team_name,
            }
            for team_id, team_name in sorted(
                teams.items(), key=lambda item: (item[1], item[0])
            )
        ],
        "instances": instance_payload,
    }


def _scheduled_run_date(workflow):
    if workflow.status != "workflow_timingtask":
        return None
    job_id = f'{Const.workflowJobprefix["sqlreview"]}-timing-{workflow.id}'
    job = task_info(job_id)
    return job.next_run if job else None


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
                "errormessage": JSON_PARSE_ERROR_MESSAGE,
            }
        ]

    column_list = list(rows[0].keys()) if rows else []
    return {"source": source, "rows": rows, "column_list": column_list}


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
        try:
            db_name = serializer.validated_data["db_name"]
            full_sql = serializer.validated_data["full_sql"].strip()
            _ensure_no_load_data_statements(full_sql)
            _authorize_workflow_check_dispatch(request.user, instance, full_sql)
            command = run_agent_command_sync(
                instance=instance,
                command_type=AgentCommandType.WORKFLOW_CHECK,
                workflow_type="workflow.check",
                workflow_id=f"{request.user.username}:{uuid.uuid4().hex}",
                payload={
                    "db_name": db_name,
                    "schema_name": serializer.validated_data.get("schema_name") or "",
                    "sql": full_sql,
                    "submitted_by": request.user.username,
                },
                timeout_seconds=int(SysConfig().get("max_execution_time", 60)),
            )
            check_result = review_set_from_agent_result(full_sql, command.result)
        except serializers.ValidationError:
            raise
        except (AgentCommandDispatchError, AgentCommandExecutionError) as exc:
            raise serializers.ValidationError({"errors": str(exc)})
        except Exception:
            logger.exception(
                "Workflow SQL check failed for instance_id=%s", instance.id
            )
            raise serializers.ValidationError({"errors": "Internal Server Error"})
        has_temporary_write_access = user_has_instance_workflow_access(
            request.user, instance, check_result.syntax_type
        )
        if not (request.user.is_superuser or has_temporary_write_access):
            raise serializers.ValidationError(
                {
                    "errors": "You do not have permission to submit SQL for this instance."
                }
            )
        check_result.rows = check_result.to_dict()
        serializer_obj = ExecuteCheckResultSerializer(check_result)
        return success_response(data=serializer_obj.data)


class WorkflowExportCheck(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Export SQL Check",
        request=ExecuteCheckSerializer,
        responses={200: ExecuteCheckResultSerializer},
        description="Validate export SQL, count result rows, and enforce export thresholds.",
    )
    def post(self, request):
        if not _can_submit_export_workflow(request.user):
            raise PermissionDenied(
                "You do not have permission to submit export workflows."
            )

        serializer = ExecuteCheckSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        instance = serializer.get_instance()
        scoped_instance_ids = {
            item["id"] for item in _export_submission_scope(request.user)["instances"]
        }
        if instance.id not in scoped_instance_ids:
            raise PermissionDenied(
                "You do not have permission to submit export workflows for this instance."
            )

        try:
            db_name = serializer.validated_data["db_name"]
            schema_name = serializer.validated_data.get("schema_name") or None
            full_sql = serializer.validated_data["full_sql"].strip()
            command = run_agent_command_sync(
                instance=instance,
                command_type=AgentCommandType.EXPORT_CHECK,
                workflow_type="export.check",
                workflow_id=f"{request.user.username}:{uuid.uuid4().hex}",
                payload={
                    "db_name": db_name,
                    "schema_name": schema_name or "",
                    "sql": full_sql,
                    "max_export_rows": int(
                        SysConfig().get("max_export_rows", "10000") or 10000
                    ),
                    "submitted_by": request.user.username,
                },
                timeout_seconds=int(SysConfig().get("max_execution_time", 60)),
            )
            check_result = review_set_from_agent_result(full_sql, command.result)
        except serializers.ValidationError:
            raise
        except (AgentCommandDispatchError, AgentCommandExecutionError) as exc:
            raise serializers.ValidationError({"errors": str(exc)})
        except Exception:
            logger.exception(
                "Workflow export SQL check failed for instance_id=%s", instance.id
            )
            raise serializers.ValidationError({"errors": "Internal Server Error"})
        check_result.rows = check_result.to_dict()
        serializer_obj = ExecuteCheckResultSerializer(check_result)
        return success_response(data=serializer_obj.data)


class WorkflowParse(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Parse Workflow SQL Text",
        request=WorkflowParseSerializer,
        responses={200: WorkflowParseResultSerializer},
        description="Split SQL text into statements and classify each statement for SPA workflow uploads.",
    )
    def post(self, request):
        serializer = WorkflowParseSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        text = serializer.validated_data["text"]
        db_type = serializer.validated_data.get("db_type") or "mysql"
        _ensure_no_load_data_statements(text)
        rows = generate_sql(text)

        detected_syntax_types = set()
        has_unknown_syntax = False
        parsed_rows = []
        for row in rows:
            syntax_type = _classify_statement_syntax(row["sql"], db_type=db_type)
            if syntax_type is None:
                has_unknown_syntax = True
            else:
                detected_syntax_types.add(syntax_type)
            parsed_rows.append(
                {
                    "sql_id": row["sql_id"],
                    "sql": row["sql"],
                    "syntax_type": syntax_type,
                }
            )

        has_mixed_syntax = len(detected_syntax_types) > 1
        summary_syntax_type = (
            next(iter(detected_syntax_types))
            if len(detected_syntax_types) == 1 and not has_unknown_syntax
            else None
        )

        return success_response(
            data={
                "total": len(parsed_rows),
                "rows": parsed_rows,
                "summary": {
                    "syntax_type": summary_syntax_type,
                    "has_mixed_syntax": has_mixed_syntax,
                    "has_unknown_syntax": has_unknown_syntax,
                },
            }
        )


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
        elif teams_for_role(
            user, TeamPermissionGroup.WORKFLOW_APPROVER
        ).exists() or user.has_perm("sql.sql_execute_for_team"):
            scoped_groups = teams_for_role(user, TeamPermissionGroup.WORKFLOW_APPROVER)
            if user.has_perm("sql.sql_execute_for_team"):
                scoped_groups = user_groups(user)
            queryset = queryset.filter(
                team_id__in=[group.team_id for group in scoped_groups]
            )
        else:
            queryset = queryset.filter(engineer=user.username)

        query_params = self.request.query_params
        scope = query_params.get("scope", "").strip()
        search = query_params.get("search", "").strip()
        status_value = query_params.get("status", "").strip()
        syntax_type = query_params.get("syntax_type", "").strip()
        team_id = query_params.get("team_id", "").strip()
        instance_id = query_params.get("instance_id", "").strip()
        engineer = query_params.get("engineer", "").strip()
        start_date = query_params.get("start_date", "").strip()
        end_date = query_params.get("end_date", "").strip()

        if scope == "mine":
            queryset = queryset.filter(engineer=user.username)
        elif scope == "pending_review":
            queryset = queryset.filter(id__in=_pending_review_workflow_ids(user))

        if search:
            queryset = queryset.filter(
                Q(workflow_name__icontains=search)
                | Q(engineer_display__icontains=search)
                | Q(instance__instance_name__icontains=search)
                | Q(db_name__icontains=search)
                | Q(team_name__icontains=search)
                | Q(demand_url__icontains=search)
            )
        if status_value:
            queryset = queryset.filter(status=status_value)
        if syntax_type:
            queryset = queryset.filter(syntax_type=syntax_type)
        if team_id:
            queryset = queryset.filter(team_id=team_id)
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
        scope = request.query_params.get("scope", "").strip()
        can_view_own_scope = scope == "mine"
        if not can_view_own_scope and not _can_access_workflow_module(request.user):
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
        sync_approval_notifications(workflow_content.workflow)
        sync_execution_needed_notifications(workflow_content.workflow)
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
        description="Return teams, writable instances, and config flags used by the SQL workflow SPA.",
    )
    def get(self, request):
        if not _can_access_workflow_module(request.user):
            raise PermissionDenied(
                "You do not have permission to submit SQL workflows."
            )

        temporary_instance_groups = _workflow_metadata_instance_groups(request.user)
        payload = {
            "manual_execution_enabled": bool(SysConfig().get("manual")),
            "teams": _workflow_metadata_teams(request.user),
            "instances": filter_agent_runnable_instances(
                user_instances(request.user, tag_codes=["can_write"])
            )
            .prefetch_related("resource_group")
            .order_by("instance_name", "id"),
        }
        serializer = WorkflowMetadataSerializer(
            payload, context={"temporary_instance_groups": temporary_instance_groups}
        )
        return success_response(data=serializer.data)


class WorkflowSubmissionMetadata(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Workflow Submission Metadata",
        responses={200: OpenApiTypes.OBJECT},
        description="Return submission-scope teams, instances, and config flags used by the workflow submission SPA.",
    )
    def get(self, request):
        if not _can_access_workflow_module(request.user):
            raise PermissionDenied(
                "You do not have permission to submit SQL workflows."
            )

        submission_scope = _workflow_submission_scope(request.user)
        return success_response(
            data={
                "teams": submission_scope["teams"],
                "instances": submission_scope["instances"],
                "manual_execution_enabled": bool(SysConfig().get("manual")),
            }
        )


class WorkflowExportSubmissionMetadata(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Workflow Export Submission Metadata",
        responses={200: OpenApiTypes.OBJECT},
        description="Return teams and readable instances available for export workflow submission.",
    )
    def get(self, request):
        if not _can_submit_export_workflow(request.user):
            raise PermissionDenied(
                "You do not have permission to submit export workflows."
            )

        submission_scope = _export_submission_scope(request.user)
        return success_response(
            data={
                "teams": submission_scope["teams"],
                "instances": submission_scope["instances"],
                "manual_execution_enabled": bool(SysConfig().get("manual")),
            }
        )


class WorkflowApprovalPreview(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Workflow Approval Preview",
        parameters=[
            OpenApiParameter(
                name="team_id",
                type=OpenApiTypes.INT,
                location=OpenApiParameter.QUERY,
                required=True,
                description="Team ID.",
            )
        ],
        responses={200: OpenApiTypes.OBJECT},
        description="Resolve the approval chain for a SQL workflow before submission.",
    )
    def get(self, request):
        if not _can_access_workflow_module(request.user):
            raise PermissionDenied(
                "You do not have permission to submit SQL workflows."
            )

        team_id = request.query_params.get("team_id")
        if not team_id:
            raise serializers.ValidationError({"errors": "team_id is required."})
        try:
            team_id = int(team_id)
        except (TypeError, ValueError):
            raise serializers.ValidationError({"errors": "team_id must be an integer."})

        allowed_group_ids = {
            group["team_id"]
            for group in _workflow_submission_scope(request.user)["teams"]
        } | {
            group["team_id"]
            for group in _export_submission_scope(request.user)["teams"]
        }
        if not request.user.is_superuser and team_id not in allowed_group_ids:
            raise PermissionDenied("You do not have access to this team.")

        team = get_object_or_404(Team, pk=team_id, is_deleted=0)
        audit_auth_groups = Audit.settings(team_id, WorkflowType.SQL_REVIEW)
        if audit_auth_groups is None:
            raise serializers.ValidationError(
                {"errors": "Approval flow is not configured for this team."}
            )

        if audit_auth_groups == "":
            readable = "No approval required"
            review_info = [
                {
                    "team_name": "Auto",
                    "is_auto_pass": True,
                    "is_current_node": False,
                    "is_passed_node": True,
                }
            ]
        else:
            readable_groups = []
            review_info = []
            for role in audit_auth_groups.split(","):
                role_label = permission_group_label(role)
                readable_groups.append(role_label)
                review_info.append(
                    {
                        "team_name": role_label,
                        "is_auto_pass": False,
                        "is_current_node": False,
                        "is_passed_node": False,
                    }
                )
            readable = " -> ".join(readable_groups)

        return success_response(
            data={
                "team_id": team.team_id,
                "team_name": team.team_name,
                "audit_auth_groups": audit_auth_groups,
                "display": readable,
                "review_info": review_info,
            }
        )


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


class WorkflowContentDetail(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Workflow Content Result",
        responses={200: OpenApiTypes.OBJECT},
        description="Return review or execution rows for a workflow detail result table.",
    )
    def get(self, request, workflow_id):
        workflow = get_object_or_404(
            SqlWorkflow.objects.select_related("sqlworkflowcontent"),
            pk=workflow_id,
        )
        if not can_view(request.user, workflow_id):
            raise PermissionDenied("You do not have permission to view this workflow.")
        return success_response(data=_load_workflow_result_rows(workflow))


class WorkflowDownload(views.APIView):
    permission_classes = [permissions.IsAuthenticated]

    @extend_schema(
        summary="Workflow Export Download",
        responses={200: OpenApiTypes.BINARY},
        description="Download a finished export workflow artifact or return a storage redirect URL.",
    )
    def get(self, request, workflow_id):
        workflow = get_object_or_404(SqlWorkflow, pk=workflow_id)
        if not can_view(request.user, workflow_id):
            raise PermissionDenied("You do not have permission to view this workflow.")
        if not (
            request.user.is_superuser or request.user.has_perm("sql.offline_download")
        ):
            raise PermissionDenied(
                "You do not have permission to download export files."
            )
        if not workflow.is_offline_export:
            raise serializers.ValidationError(
                {"errors": "This workflow does not have an export artifact."}
            )
        if workflow.status != "workflow_finish" or not workflow.file_name:
            raise serializers.ValidationError(
                {"errors": "The export artifact is not available yet."}
            )
        return download_export_file(request, workflow.file_name, workflow.id)


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
            or request.user.has_perm("sql.sql_execute_for_team")
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
        selected_executor = None

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

        if _is_mysql_ddl_workflow(workflow):
            try:
                resolved_executor = _resolve_mysql_ddl_executor(
                    workflow=workflow,
                    executor_id=serializer.validated_data.get("executor"),
                    preflight=True,
                )
            except MysqlDDLExecutorError as exc:
                logger.warning("Failed to resolve MySQL DDL executor", exc_info=True)
                raise serializers.ValidationError({"errors": str(exc)}) from None
            selected_executor = resolved_executor.executor_id

        schedule_name = f"sqlreview-timing-{workflow_id}"
        with transaction.atomic():
            workflow.status = "workflow_timingtask"
            workflow.save(update_fields=["status"])
            add_sql_schedule(
                schedule_name,
                run_date,
                workflow_id,
                execution_options=(
                    {"executor": selected_executor} if selected_executor else None
                ),
            )
            audit = Audit.detail_by_workflow_id(workflow_id, WorkflowType.SQL_REVIEW)
            operation_info = f"Scheduled execution time: {run_date}"
            if selected_executor:
                operation_info = f"{operation_info} (executor: {selected_executor})"
            Audit.add_log(
                audit_id=audit.audit_id,
                operation_type=WorkflowAction.EXECUTE_SET_TIME,
                operation_type_desc="Scheduled Execution",
                operation_info=operation_info,
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
        queryset = self.queryset.filter(
            current_status=WorkflowStatus.WAITING,
            audit_id__in=reviewable_audit_ids(user),
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
            logger.warning(
                "Workflow audit operation failed for user=%s action=%s",
                getattr(user, "username", user),
                action,
                exc_info=True,
            )
            raise serializers.ValidationError({"errors": "Operation failed."}) from None

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
            sync_approval_notifications(auditor.workflow)
            sync_execution_needed_notifications(auditor.workflow)
        elif auditor.workflow_type == WorkflowType.ARCHIVE:
            auditor.workflow.status = auditor.audit.current_status
            if auditor.audit.current_status == WorkflowStatus.PASSED:
                auditor.workflow.state = True
            else:
                auditor.workflow.state = False
            auditor.workflow.save(update_fields=["status", "state"])
            sync_approval_notifications(auditor.workflow)
            sync_execution_needed_notifications(auditor.workflow)

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
            workflow = get_object_or_404(
                SqlWorkflow.objects.select_related("instance", "sqlworkflowcontent"),
                pk=workflow_id,
            )

            # Validate multiple permissions
            if not (
                user.has_perm("sql.sql_execute")
                or user.has_perm("sql.sql_execute_for_team")
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
            selected_executor = None

            # Execute by system
            if mode == "auto":
                if _is_mysql_ddl_workflow(workflow):
                    try:
                        resolved_executor = _resolve_mysql_ddl_executor(
                            workflow=workflow,
                            executor_id=data.get("executor"),
                            preflight=True,
                        )
                    except MysqlDDLExecutorError as exc:
                        logger.warning(
                            "Failed to resolve MySQL DDL executor for execution",
                            exc_info=True,
                        )
                        raise serializers.ValidationError(
                            {"errors": str(exc)}
                        ) from None
                    selected_executor = resolved_executor.executor_id
                # Set workflow status to queuing
                try:
                    agent_command = dispatch_sql_workflow_to_agent(
                        workflow,
                        user=user,
                        executor=selected_executor,
                    )
                except ValueError as exc:
                    logger.warning(
                        "Failed to dispatch SQL workflow to agent",
                        exc_info=True,
                    )
                    error_detail = str(exc) or exc.__class__.__name__
                    raise serializers.ValidationError(
                        {
                            "errors": (
                                "Unable to dispatch workflow to agent: "
                                f"{error_detail}"
                            )
                        }
                    ) from None
                except Exception as exc:
                    logger.exception("Failed to dispatch SQL workflow to agent")
                    error_detail = str(exc) or exc.__class__.__name__
                    raise serializers.ValidationError(
                        {
                            "errors": (
                                "Unable to dispatch workflow to agent: "
                                f"{error_detail}"
                            )
                        }
                    ) from None
                del_schedule(f"sqlreview-timing-{workflow_id}")
                # Add workflow log
                operation_info = "Workflow dispatched to agent"
                if selected_executor:
                    operation_info = f"{operation_info} (executor: {selected_executor})"
                operation_info = (
                    f"{operation_info} (agent: {agent_command.agent_id}, "
                    f"command: {agent_command.id})"
                )
                Audit.add_log(
                    audit_id=audit_id,
                    operation_type=5,
                    operation_type_desc="Execute Workflow",
                    operation_info=operation_info,
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
                workflow = SqlWorkflow.objects.select_related("instance").get(
                    id=workflow_id
                )
                resolve_mailbox_items(workflow, category="execution_needed")
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
                        workflow=workflow,
                    )
                emit_execution_finished_notifications(
                    workflow,
                    outcome="success",
                    actor=user,
                    dedupe_suffix=(
                        f"manual-{workflow.finish_time.strftime('%Y%m%d%H%M%S%f')}"
                        if workflow.finish_time
                        else f"manual-{workflow.id}"
                    ),
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
