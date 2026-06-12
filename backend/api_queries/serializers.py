from rest_framework import serializers

from sql.models import Instance, QueryLog, QueryPrivileges, QueryPrivilegesApply


class QueryPrivilegesApplySerializer(serializers.ModelSerializer):
    class Meta:
        model = QueryPrivilegesApply
        fields = (
            "apply_id",
            "team_id",
            "team_name",
            "title",
            "user_name",
            "user_display",
            "instance",
            "db_list",
            "table_list",
            "valid_date",
            "limit_num",
            "priv_type",
            "status",
            "audit_auth_groups",
            "create_time",
        )


class QueryExecuteSerializer(serializers.Serializer):
    instance_name = serializers.CharField(label="Instance name", max_length=50)
    sql_content = serializers.CharField(label="SQL content")
    db_name = serializers.CharField(label="Database name", max_length=64)
    tb_name = serializers.CharField(
        required=False, allow_blank=True, label="Table name", max_length=64
    )
    schema_name = serializers.CharField(
        required=False, allow_blank=True, label="Schema name", max_length=128
    )
    limit_num = serializers.IntegerField(required=False, min_value=0, default=0)


class QueryExecuteResponseSerializer(serializers.Serializer):
    detail = serializers.CharField()
    data = serializers.JSONField()


class QueryInstanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Instance
        fields = ["id", "instance_name", "db_type", "type"]


class QueryLogSerializer(serializers.ModelSerializer):
    class Meta:
        model = QueryLog
        fields = [
            "id",
            "instance_name",
            "db_name",
            "sqllog",
            "effect_row",
            "cost_time",
            "user_display",
            "favorite",
            "alias",
            "create_time",
        ]


class QueryFavoriteListSerializer(serializers.ModelSerializer):
    class Meta:
        model = QueryLog
        fields = [
            "id",
            "alias",
            "instance_name",
            "db_name",
            "sqllog",
            "create_time",
        ]


class QueryFavoriteSerializer(serializers.Serializer):
    query_log_id = serializers.IntegerField(label="Query log ID")
    star = serializers.BooleanField(label="Favorite status")
    alias = serializers.CharField(
        required=False,
        allow_blank=True,
        label="Query alias",
        default="",
        max_length=64,
    )


class QueryDescribeSerializer(serializers.Serializer):
    instance_id = serializers.IntegerField(label="Instance ID")
    db_name = serializers.CharField(label="Database name", max_length=64)
    schema_name = serializers.CharField(
        required=False, allow_blank=True, label="Schema name", max_length=128
    )
    tb_name = serializers.CharField(label="Table name", max_length=64)


class QueryDescribeResponseSerializer(serializers.Serializer):
    detail = serializers.CharField()
    data = serializers.JSONField()


class QueryPrivilegesApplyListSerializer(serializers.ModelSerializer):
    instance_name = serializers.CharField(
        source="instance.instance_name", read_only=True
    )

    class Meta:
        model = QueryPrivilegesApply
        fields = [
            "apply_id",
            "title",
            "instance_name",
            "db_list",
            "priv_type",
            "table_list",
            "limit_num",
            "valid_date",
            "user_display",
            "status",
            "create_time",
            "team_name",
        ]


class QueryPrivilegesApplyCreateSerializer(serializers.Serializer):
    title = serializers.CharField(label="Request title", max_length=50)
    instance_name = serializers.CharField(label="Instance name", max_length=50)
    team_name = serializers.CharField(label="Team name", max_length=100)
    priv_type = serializers.ChoiceField(choices=[1, 2], label="Privilege type")
    db_name = serializers.CharField(
        required=False, allow_blank=True, label="Database name", max_length=64
    )
    db_list = serializers.ListField(
        child=serializers.CharField(max_length=64),
        required=False,
        label="Database list",
    )
    table_list = serializers.ListField(
        child=serializers.CharField(max_length=64),
        required=False,
        label="Table list",
    )
    valid_date = serializers.DateField(label="Privilege valid date")
    limit_num = serializers.IntegerField(min_value=1, label="Limit rows")

    def validate(self, attrs):
        priv_type = attrs["priv_type"]
        db_list = attrs.get("db_list") or []
        db_name = attrs.get("db_name") or ""
        table_list = attrs.get("table_list") or []

        if priv_type == 1 and not db_list:
            raise serializers.ValidationError(
                {"errors": "db_list is required for database privileges."}
            )
        if priv_type == 2:
            if not db_name:
                raise serializers.ValidationError(
                    {"errors": "db_name is required for table privileges."}
                )
            if not table_list:
                raise serializers.ValidationError(
                    {"errors": "table_list is required for table privileges."}
                )
        return attrs


class QueryPrivilegesListSerializer(serializers.ModelSerializer):
    instance_name = serializers.CharField(
        source="instance.instance_name", read_only=True
    )

    class Meta:
        model = QueryPrivileges
        fields = [
            "privilege_id",
            "user_display",
            "instance_name",
            "db_name",
            "priv_type",
            "table_name",
            "limit_num",
            "valid_date",
        ]


class QueryPrivilegesModifySerializer(serializers.Serializer):
    privilege_id = serializers.IntegerField(label="Privilege ID")
    type = serializers.ChoiceField(choices=[1, 2], label="1-delete, 2-update")
    valid_date = serializers.DateField(required=False)
    limit_num = serializers.IntegerField(required=False, min_value=1)

    def validate(self, attrs):
        if attrs["type"] == 2 and (
            "valid_date" not in attrs or "limit_num" not in attrs
        ):
            raise serializers.ValidationError(
                {"errors": "valid_date and limit_num are required when type is 2."}
            )
        return attrs


class QueryPrivilegesAuditSerializer(serializers.Serializer):
    apply_id = serializers.IntegerField(label="Application ID")
    audit_status = serializers.IntegerField(label="Audit action")
    audit_remark = serializers.CharField(
        required=False, allow_blank=True, label="Audit remark", default=""
    )
