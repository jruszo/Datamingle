from rest_framework import serializers

from sql.models import Instance, QueryLog, QueryPrivileges, QueryPrivilegesApply


class QueryPrivilegesApplySerializer(serializers.ModelSerializer):
    class Meta:
        model = QueryPrivilegesApply
        fields = "__all__"


class QueryExecuteSerializer(serializers.Serializer):
    instance_name = serializers.CharField(label="Instance name")
    sql_content = serializers.CharField(label="SQL content")
    db_name = serializers.CharField(label="Database name")
    tb_name = serializers.CharField(
        required=False, allow_blank=True, label="Table name"
    )
    schema_name = serializers.CharField(
        required=False, allow_blank=True, label="Schema name"
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
        required=False, allow_blank=True, label="Query alias", default=""
    )


class QueryDescribeSerializer(serializers.Serializer):
    instance_id = serializers.IntegerField(label="Instance ID")
    db_name = serializers.CharField(label="Database name")
    schema_name = serializers.CharField(
        required=False, allow_blank=True, label="Schema name"
    )
    tb_name = serializers.CharField(label="Table name")


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
            "group_name",
        ]


class QueryPrivilegesApplyCreateSerializer(serializers.Serializer):
    title = serializers.CharField(label="Request title")
    instance_name = serializers.CharField(label="Instance name")
    group_name = serializers.CharField(label="Resource group name")
    priv_type = serializers.ChoiceField(choices=[1, 2], label="Privilege type")
    db_name = serializers.CharField(
        required=False, allow_blank=True, label="Database name"
    )
    db_list = serializers.ListField(
        child=serializers.CharField(), required=False, label="Database list"
    )
    table_list = serializers.ListField(
        child=serializers.CharField(), required=False, label="Table list"
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
