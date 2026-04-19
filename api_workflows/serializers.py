import logging
import re
import traceback

from django.db import transaction
from rest_framework import serializers

from common.utils.const import WorkflowStatus, WorkflowType
from sql.engines import get_engine
from sql.models import (
    Instance,
    ResourceGroup,
    SqlWorkflow,
    SqlWorkflowContent,
    WorkflowAudit,
    WorkflowLog,
    Users,
)
from sql.offlinedownload import OffLineDownLoad
from sql.utils.resource_group import (
    user_has_group_instance_access,
    user_has_instance_query_access,
    user_has_instance_workflow_access,
)
from sql.utils.sql_review import can_cancel, can_execute, can_rollback, can_timingtask
from sql.utils.sql_utils import generate_sql
from sql.utils.tasks import task_info
from sql.utils.workflow_audit import get_auditor

logger = logging.getLogger("default")
LOAD_DATA_PATTERN = re.compile(r"^\s*load\s+data\b", re.IGNORECASE)
EXPORT_FORMAT_CHOICES = {"csv", "tsv", "sql", "xlsx"}
DDL_EXECUTOR_CHOICES = ("direct", "gh-ost", "pt-osc")


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
        if data.get("run_date_start") == "":
            data["run_date_start"] = None
        if data.get("run_date_end") == "":
            data["run_date_end"] = None
        return super().to_internal_value(data)

    @staticmethod
    def validate_group_id(group_id):
        try:
            ResourceGroup.objects.get(pk=group_id)
        except ResourceGroup.DoesNotExist:
            raise serializers.ValidationError(
                {"errors": f"Resource group does not exist: {group_id}"}
            )
        return group_id

    class Meta:
        model = SqlWorkflow
        fields = "__all__"
        read_only_fields = [
            "status",
            "syntax_type",
            "audit_auth_groups",
            "engineer_display",
            "group_name",
            "finish_time",
            "is_manual",
        ]
        extra_kwargs = {
            "demand_url": {"required": False},
            "is_backup": {"required": False},
            "engineer": {"required": False},
        }


class WorkflowContentSerializer(serializers.ModelSerializer):
    workflow = WorkflowSerializer()

    @staticmethod
    def _validate_group_for_instance(instance, group_id):
        try:
            group = ResourceGroup.objects.get(pk=group_id)
        except ResourceGroup.DoesNotExist:
            raise serializers.ValidationError(
                {"errors": f"Resource group does not exist: {group_id}"}
            )

        if not instance.resource_group.filter(pk=group.pk).exists():
            raise serializers.ValidationError(
                {"errors": "Selected resource group does not belong to this instance."}
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
        group = self._validate_group_for_instance(instance, workflow_data["group_id"])
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

        try:
            check_engine = get_engine(instance=instance)
            sql_export = OffLineDownLoad()
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
                instance.sql_content = sql_content
                instance.db_name = workflow_data["db_name"]
                instance.schema_name = workflow_data.get("schema_name") or ""
                instance.export_format = export_format
                check_result = sql_export.pre_count_check(workflow=instance)
            else:
                workflow_data["export_format"] = None
                check_result = check_engine.execute_check(
                    db_name=workflow_data["db_name"], sql=sql_content
                )
        except serializers.ValidationError:
            raise
        except Exception:
            logger.exception("Unexpected error while validating workflow submission.")
            raise serializers.ValidationError(
                {"errors": "An internal validation error occurred."}
            )

        has_group_write_access = user_has_group_instance_access(
            actor, instance, tag_codes=["can_write"]
        )
        has_group_read_access = user_has_group_instance_access(
            actor, instance, tag_codes=["can_read"]
        )
        has_temporary_write_access = user_has_instance_workflow_access(
            actor, instance, check_result.syntax_type
        )
        has_temporary_read_access = user_has_instance_query_access(actor, instance)
        if is_offline_export:
            if not (actor.is_superuser or actor.has_perm("sql.sqlexport_submit")):
                raise serializers.ValidationError(
                    {"errors": "You do not have permission to submit export workflows."}
                )
            if not (has_group_read_access or has_temporary_read_access):
                raise serializers.ValidationError(
                    {
                        "errors": (
                            "You do not have permission to submit export workflows for this instance."
                        )
                    }
                )
        elif not (
            actor.is_superuser
            or (has_group_write_access and actor.has_perm("sql.sql_submit"))
            or has_temporary_write_access
        ):
            raise serializers.ValidationError(
                {
                    "errors": "You do not have permission to submit SQL for this instance."
                }
            )

        if "is_backup" in workflow_data:
            is_backup = workflow_data["is_backup"]
        else:
            is_backup = SqlWorkflow._meta.get_field("is_backup").get_default()
        if is_offline_export:
            is_backup = False

        workflow_data.update(
            status="workflow_manreviewing",
            is_backup=is_backup,
            is_manual=0,
            syntax_type=check_result.syntax_type,
            engineer=user.username,
            engineer_display=user.display,
            group_name=group.group_name,
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
            "group_id",
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
