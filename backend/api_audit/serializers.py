from rest_framework import serializers

from sql.models import AuditEntry, QueryLog, SqlWorkflow, WorkflowLog


class GeneralAuditLogSerializer(serializers.ModelSerializer):
    class Meta:
        model = AuditEntry
        fields = (
            "user_id",
            "user_name",
            "user_display",
            "action",
            "extra_info",
            "action_time",
        )


class QueryAuditLogSerializer(serializers.ModelSerializer):
    class Meta:
        model = QueryLog
        fields = (
            "id",
            "instance_name",
            "db_name",
            "sqllog",
            "effect_row",
            "cost_time",
            "username",
            "user_display",
            "priv_check",
            "hit_rule",
            "masking",
            "favorite",
            "alias",
            "create_time",
        )


class SqlWorkflowAuditSerializer(serializers.ModelSerializer):
    instance_name = serializers.CharField(
        source="instance.instance_name", read_only=True
    )
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
            "db_name",
            "schema_name",
            "syntax_type",
            "syntax_type_label",
            "is_backup",
            "engineer",
            "engineer_display",
            "status",
            "status_label",
            "run_date_start",
            "run_date_end",
            "create_time",
            "finish_time",
            "is_offline_export",
            "export_format",
        )


class WorkflowOperationLogSerializer(serializers.ModelSerializer):
    class Meta:
        model = WorkflowLog
        fields = (
            "id",
            "audit_id",
            "operation_type",
            "operation_type_desc",
            "operation_info",
            "operator",
            "operator_display",
            "operation_time",
        )
