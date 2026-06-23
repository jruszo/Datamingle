import logging
import re
import traceback
import uuid

from django.db import transaction
from rest_framework import serializers

from common.config import SysConfig
from common.utils.const import WorkflowStatus, WorkflowType
from sql.models import (
    Instance,
    TeamPermissionGroup,
    Team,
    SqlWorkflow,
    SqlWorkflowContent,
    WorkflowAudit,
    WorkflowLog,
    Users,
)
from sql.utils.team import (
    user_has_instance_query_access,
    user_has_instance_workflow_access,
    user_has_resource_role,
)
from sql.utils.sql_review import can_cancel, can_execute, can_timingtask
from sql.utils.sql_utils import generate_sql, get_syntax_type
from sql.utils.tasks import task_info
from sql.utils.workflow_audit import get_auditor
from api_agents.models import AgentCommandType
from api_agents.services import (
    AgentCommandDispatchError,
    AgentCommandExecutionError,
    review_set_from_agent_result,
    run_agent_command_sync,
)

logger = logging.getLogger("default")
LOAD_DATA_PATTERN = re.compile(r"^\s*load\s+data\b", re.IGNORECASE)
EXPORT_FORMAT_CHOICES = {"csv", "tsv", "sql", "xlsx"}
DDL_EXECUTOR_CHOICES = ("direct",)


def _sysconfig_int(name, default):
    try:
        return int(SysConfig().get(name, str(default)) or default)
    except (TypeError, ValueError):
        return default


def _classify_statement_syntax(statement, db_type="mysql"):
    syntax_name = get_syntax_type(statement, parser=True, db_type=db_type)
    if syntax_name not in {"DDL", "DML"}:
        syntax_name = get_syntax_type(statement, parser=False, db_type=db_type)
    if syntax_name == "DDL":
        return 1
    if syntax_name == "DML":
        return 2
    return None


def _detected_workflow_syntax_types(sql_text, db_type="mysql"):
    syntax_types = set()
    for row in generate_sql(sql_text):
        syntax_type = _classify_statement_syntax(row["sql"], db_type=db_type)
        if syntax_type is not None:
            syntax_types.add(syntax_type)
    return syntax_types


def _authorize_workflow_check_dispatch(actor, instance, sql_text):
    if actor.is_superuser:
        return
    syntax_types = _detected_workflow_syntax_types(
        sql_text, db_type=instance.db_type
    ) or {2}
    if all(
        user_has_instance_workflow_access(actor, instance, syntax_type)
        for syntax_type in syntax_types
    ):
        return
    raise serializers.ValidationError(
        {"errors": "You do not have permission to submit SQL for this instance."}
    )


class ExecuteCheckSerializer(serializers.Serializer):
    instance_id = serializers.IntegerField(label="Instance ID")
    db_name = serializers.CharField(label="Database name")
    schema_name = serializers.CharField(
        required=False, allow_blank=True, label="Schema name"
    )
    full_sql = serializers.CharField(label="SQL content")

    def validate_instance_id(self, instance_id):
        try:
            Instance.objects.get(pk=instance_id)
        except Instance.DoesNotExist:
            raise serializers.ValidationError(
                {"errors": f"Instance does not exist: {instance_id}"}
            )
        return instance_id

    def get_instance(self):
        return Instance.objects.get(pk=self.validated_data["instance_id"])


class ExecuteCheckResultSerializer(serializers.Serializer):
    is_execute = serializers.BooleanField(read_only=True, default=False)
    checked = serializers.CharField(read_only=True)
    warning = serializers.CharField(read_only=True)
    error = serializers.CharField(read_only=True)
    warning_count = serializers.IntegerField(read_only=True)
    error_count = serializers.IntegerField(read_only=True)
    is_critical = serializers.BooleanField(read_only=True, default=False)
    syntax_type = serializers.IntegerField(read_only=True)
    rows = serializers.JSONField(read_only=True)
    column_list = serializers.JSONField(read_only=True)
    status = serializers.CharField(read_only=True)
    affected_rows = serializers.IntegerField(read_only=True)


class WorkflowParseSerializer(serializers.Serializer):
    text = serializers.CharField(label="SQL text")
    db_type = serializers.CharField(
        label="Database type", required=False, allow_blank=True, default=""
    )

    def validate_text(self, value):
        return value.strip()

    def validate_db_type(self, value):
        return value.strip().lower()


class WorkflowParsedStatementSerializer(serializers.Serializer):
    sql_id = serializers.JSONField(read_only=True)
    sql = serializers.CharField(read_only=True)
    syntax_type = serializers.IntegerField(read_only=True, allow_null=True)


class WorkflowParseSummarySerializer(serializers.Serializer):
    syntax_type = serializers.IntegerField(read_only=True, allow_null=True)
    has_mixed_syntax = serializers.BooleanField(read_only=True)
    has_unknown_syntax = serializers.BooleanField(read_only=True)


class WorkflowParseResultSerializer(serializers.Serializer):
    total = serializers.IntegerField(read_only=True)
    rows = WorkflowParsedStatementSerializer(many=True, read_only=True)
    summary = WorkflowParseSummarySerializer(read_only=True)


class WorkflowSerializer(serializers.ModelSerializer):
    def to_internal_value(self, data):
        data = data.copy()
        data.pop("is_backup", None)
        if data.get("run_date_start") == "":
            data["run_date_start"] = None
        if data.get("run_date_end") == "":
            data["run_date_end"] = None
        return super().to_internal_value(data)

    @staticmethod
    def validate_group_id(team_id):
        try:
            Team.objects.get(pk=team_id)
        except Team.DoesNotExist:
            raise serializers.ValidationError(
                {"errors": f"Team does not exist: {team_id}"}
            )
        return team_id

    class Meta:
        model = SqlWorkflow
        fields = "__all__"
        read_only_fields = [
            "status",
            "syntax_type",
            "audit_auth_groups",
            "engineer_display",
            "team_name",
            "finish_time",
            "is_manual",
        ]
        extra_kwargs = {
            "demand_url": {"required": False},
            "engineer": {"required": False},
        }


class WorkflowContentSerializer(serializers.ModelSerializer):
    workflow = WorkflowSerializer()

    @staticmethod
    def _validate_group_for_instance(instance, team_id):
        try:
            group = Team.objects.get(pk=team_id)
        except Team.DoesNotExist:
            raise serializers.ValidationError(
                {"errors": f"Team does not exist: {team_id}"}
            )

        if not instance.resource_group.filter(pk=group.pk).exists():
            raise serializers.ValidationError(
                {"errors": "Selected team does not belong to this instance."}
            )

        return group

    def create(self, validated_data):
        actor = self.context["request"].user
        workflow_data = validated_data.pop("workflow")
        instance = workflow_data["instance"]
        is_offline_export = bool(workflow_data.get("is_offline_export"))
        sql_content = validated_data["sql_content"].strip()
        for row in generate_sql(sql_content):
            if LOAD_DATA_PATTERN.match(row["sql"]):
                raise serializers.ValidationError(
                    {
                        "errors": (
                            "LOAD DATA statements are not supported for workflow submission."
                        )
                    }
                )
        group = self._validate_group_for_instance(instance, workflow_data["team_id"])
        engineer = workflow_data.get("engineer")

        if actor.is_superuser and engineer:
            try:
                user = Users.objects.get(username=engineer)
            except Users.DoesNotExist:
                raise serializers.ValidationError(
                    {"errors": f"User does not exist: {engineer}"}
                )
        else:
            user = self.context["request"].user

        required_permission = (
            TeamPermissionGroup.EXPORT_WORKFLOW_REQUESTER
            if is_offline_export
            else TeamPermissionGroup.WORKFLOW_REQUESTER
        )
        has_group_request_access = user_has_resource_role(
            actor, group, required_permission
        )
        has_temporary_read_access = user_has_instance_query_access(actor, instance)

        if is_offline_export:
            if not (actor.is_superuser or has_group_request_access):
                raise serializers.ValidationError(
                    {"errors": "You do not have permission to submit export workflows."}
                )
            if not (
                actor.is_superuser
                or has_group_request_access
                or has_temporary_read_access
            ):
                raise serializers.ValidationError(
                    {
                        "errors": (
                            "You do not have permission to submit export workflows for this instance."
                        )
                    }
                )
        elif actor.is_superuser or has_group_request_access:
            pass
        else:
            _authorize_workflow_check_dispatch(
                actor,
                instance,
                sql_content,
            )

        try:
            timeout_seconds = _sysconfig_int("max_execution_time", 60)
            if is_offline_export:
                export_format = (
                    (workflow_data.get("export_format") or "").lower().strip()
                )
                if export_format not in EXPORT_FORMAT_CHOICES:
                    raise serializers.ValidationError(
                        {
                            "errors": (
                                "Export format must be one of: csv, tsv, sql, xlsx."
                            )
                        }
                    )
                workflow_data["export_format"] = export_format
                command = run_agent_command_sync(
                    instance=instance,
                    command_type=AgentCommandType.EXPORT_CHECK,
                    workflow_type="export.check",
                    workflow_id=f"{actor.username}:{uuid.uuid4().hex}",
                    payload={
                        "db_name": workflow_data["db_name"],
                        "schema_name": workflow_data.get("schema_name") or "",
                        "sql": sql_content,
                        "export_format": export_format,
                        "max_export_rows": _sysconfig_int("max_export_rows", 10000),
                        "submitted_by": actor.username,
                    },
                    timeout_seconds=timeout_seconds,
                )
            else:
                workflow_data["export_format"] = None
                command = run_agent_command_sync(
                    instance=instance,
                    command_type=AgentCommandType.WORKFLOW_CHECK,
                    workflow_type="workflow.check",
                    workflow_id=f"{actor.username}:{uuid.uuid4().hex}",
                    payload={
                        "db_name": workflow_data["db_name"],
                        "schema_name": workflow_data.get("schema_name") or "",
                        "sql": sql_content,
                        "submitted_by": actor.username,
                    },
                    timeout_seconds=timeout_seconds,
                )
            check_result = review_set_from_agent_result(sql_content, command.result)
        except serializers.ValidationError:
            raise
        except (AgentCommandDispatchError, AgentCommandExecutionError) as exc:
            raise serializers.ValidationError({"errors": str(exc)})
        except Exception:
            logger.exception("Unexpected error while validating workflow submission.")
            raise serializers.ValidationError(
                {"errors": "An internal validation error occurred."}
            )

        has_temporary_write_access = user_has_instance_workflow_access(
            actor, instance, check_result.syntax_type
        )
        if not (
            actor.is_superuser or has_group_request_access or has_temporary_write_access
        ):
            raise serializers.ValidationError(
                {
                    "errors": "You do not have permission to submit SQL for this instance."
                }
            )

        workflow_data.update(
            status="workflow_manreviewing",
            is_backup=False,
            is_manual=0,
            syntax_type=check_result.syntax_type,
            engineer=user.username,
            engineer_display=user.display,
            team_name=group.team_name,
            audit_auth_groups="",
        )
        try:
            with transaction.atomic():
                workflow = SqlWorkflow(**workflow_data)
                validated_data["review_content"] = check_result.json()
                workflow.save()
                workflow_content = SqlWorkflowContent.objects.create(
                    workflow=workflow, **validated_data
                )
                auditor = get_auditor(workflow=workflow)
                auditor.create_audit()
                if auditor.audit.current_status == WorkflowStatus.REJECTED:
                    auditor.workflow.status = "workflow_autoreviewwrong"
                elif auditor.audit.current_status == WorkflowStatus.PASSED:
                    auditor.workflow.status = "workflow_review_pass"
                auditor.workflow.save()
        except Exception:
            logger.error("Error submitting workflow: %s", traceback.format_exc())
            raise serializers.ValidationError(
                {"errors": "An internal validation error occurred."}
            )
        return workflow_content

    class Meta:
        model = SqlWorkflowContent
        fields = (
            "id",
            "workflow_id",
            "workflow",
            "sql_content",
            "review_content",
            "execute_result",
        )
        read_only_fields = ["review_content", "execute_result"]


class WorkflowScheduleSerializer(serializers.Serializer):
    run_date = serializers.DateTimeField(
        input_formats=["%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "iso-8601"]
    )


class WorkflowWindowSerializer(serializers.Serializer):
    run_date_start = serializers.DateTimeField(
        input_formats=["%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "iso-8601"],
        required=False,
        allow_null=True,
    )
    run_date_end = serializers.DateTimeField(
        input_formats=["%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "iso-8601"],
        required=False,
        allow_null=True,
    )

    def validate(self, attrs):
        start = attrs.get("run_date_start")
        end = attrs.get("run_date_end")
        if start and end and start > end:
            raise serializers.ValidationError(
                {"errors": "run_date_start cannot be later than run_date_end."}
            )
        return attrs


class AuditWorkflowSerializer(serializers.Serializer):
    workflow_id = serializers.IntegerField(label="Workflow ID")
    audit_remark = serializers.CharField(label="Approval remark")
    workflow_type = serializers.ChoiceField(
        choices=WorkflowType.choices,
        label="Workflow type: 1-query privilege apply, 2-SQL release apply, 3-data archive apply",
    )
    audit_type = serializers.ChoiceField(
        choices=["pass", "reject", "cancel"], label="Audit type"
    )

    def validate(self, attrs):
        workflow_id = attrs.get("workflow_id")
        workflow_type = attrs.get("workflow_type")

        try:
            WorkflowAudit.objects.get(
                workflow_id=workflow_id, workflow_type=workflow_type
            )
        except WorkflowAudit.DoesNotExist:
            raise serializers.ValidationError({"errors": "Workflow does not exist."})

        return attrs


class WorkflowAuditListSerializer(serializers.ModelSerializer):
    class Meta:
        model = WorkflowAudit
        exclude = [
            "team_id",
            "workflow_id",
            "workflow_remark",
            "next_audit",
            "create_user",
            "sys_time",
        ]


class WorkflowLogListSerializer(serializers.ModelSerializer):
    class Meta:
        model = WorkflowLog
        fields = [
            "operation_type_desc",
            "operation_info",
            "operator_display",
            "operation_time",
        ]


class ExecuteWorkflowSerializer(serializers.Serializer):
    workflow_id = serializers.IntegerField(label="Workflow ID")
    workflow_type = serializers.ChoiceField(
        choices=[2, 3],
        label="Workflow type: 1-query privilege apply, 2-SQL release apply, 3-data archive apply",
    )
    mode = serializers.ChoiceField(
        choices=["auto", "manual"],
        label="Execution mode: auto-online execution, manual-already executed manually",
        required=False,
    )
    executor = serializers.ChoiceField(
        choices=DDL_EXECUTOR_CHOICES,
        required=False,
        allow_null=True,
    )

    def validate(self, attrs):
        workflow_id = attrs.get("workflow_id")
        workflow_type = attrs.get("workflow_type")
        mode = attrs.get("mode")

        if workflow_type == 2 and not mode:
            raise serializers.ValidationError({"errors": "Missing mode."})

        try:
            WorkflowAudit.objects.get(
                workflow_id=workflow_id, workflow_type=workflow_type
            )
        except WorkflowAudit.DoesNotExist:
            raise serializers.ValidationError({"errors": "Workflow does not exist."})

        return attrs
