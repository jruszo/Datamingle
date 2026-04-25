from django.db import transaction
from rest_framework import serializers

from sql.models import (
    Instance,
    InstanceDatabase,
    InstanceTag,
    ParamHistory,
    QueryPrivilegesApply,
    ResourceGroup,
)


class ChoiceOptionSerializer(serializers.Serializer):
    value = serializers.CharField()
    label = serializers.CharField()


class DataDictionaryInstanceSerializer(serializers.ModelSerializer):
    label = serializers.SerializerMethodField()

    def get_label(self, obj):
        return f"{obj.instance_name} ({obj.db_type})"

    class Meta:
        model = Instance
        fields = ("id", "instance_name", "db_type", "label")


class DataDictionaryDatabaseListSerializer(serializers.Serializer):
    count = serializers.IntegerField()
    result = serializers.ListField(child=serializers.CharField())


class DataDictionaryTableGroupSerializer(serializers.Serializer):
    group = serializers.CharField()
    tables = serializers.ListField(child=serializers.ListField())


class DataDictionaryTableGroupListSerializer(serializers.Serializer):
    count = serializers.IntegerField()
    result = DataDictionaryTableGroupSerializer(many=True)


class DataDictionaryResultSetSerializer(serializers.Serializer):
    column_list = serializers.ListField(child=serializers.CharField(), required=False)
    rows = serializers.JSONField()


class DataDictionaryTableDetailSerializer(serializers.Serializer):
    meta_data = DataDictionaryResultSetSerializer()
    desc = DataDictionaryResultSetSerializer()
    index = DataDictionaryResultSetSerializer()
    create_sql = serializers.JSONField(required=False)


class InstanceDatabaseRecordSerializer(serializers.Serializer):
    id = serializers.IntegerField(required=False)
    db_name = serializers.CharField()
    owner = serializers.CharField(required=False, allow_blank=True)
    owner_display = serializers.CharField(required=False, allow_blank=True)
    remark = serializers.CharField(required=False, allow_blank=True)
    saved = serializers.BooleanField(default=False)
    sys_time = serializers.DateTimeField(required=False)
    table_rows = serializers.JSONField(required=False)
    data_length = serializers.JSONField(required=False)
    index_length = serializers.JSONField(required=False)
    data_total = serializers.JSONField(required=False)


class InstanceDatabaseListSerializer(serializers.Serializer):
    count = serializers.IntegerField()
    results = InstanceDatabaseRecordSerializer(many=True)


class InstanceDatabasePayloadSerializer(serializers.Serializer):
    instance_id = serializers.IntegerField()
    db_name = serializers.CharField(max_length=128)
    owner = serializers.CharField(max_length=50, allow_blank=True, required=False)
    remark = serializers.CharField(max_length=255, allow_blank=True, required=False)

    def validate_db_name(self, value):
        db_name = value.strip()
        if not db_name:
            raise serializers.ValidationError("Database name cannot be blank.")
        return db_name

    def validate_owner(self, value):
        return value.strip()

    def validate_remark(self, value):
        return value.strip()


class InstanceDatabaseMetadataSerializer(serializers.ModelSerializer):
    class Meta:
        model = InstanceDatabase
        fields = ("id", "db_name", "owner", "owner_display", "remark", "sys_time")


class InstanceAccountRecordSerializer(serializers.Serializer):
    id = serializers.IntegerField(required=False)
    user = serializers.CharField()
    host = serializers.CharField(required=False, allow_blank=True)
    db_name = serializers.CharField(required=False, allow_blank=True)
    user_host = serializers.CharField(required=False, allow_blank=True)
    db_name_user = serializers.CharField(required=False, allow_blank=True)
    roles = serializers.JSONField(required=False)
    privileges = serializers.JSONField(required=False)
    is_locked = serializers.CharField(required=False, allow_blank=True, allow_null=True)
    remark = serializers.CharField(required=False, allow_blank=True)
    saved = serializers.BooleanField(default=False)
    sys_time = serializers.DateTimeField(required=False)


class InstanceAccountListSerializer(serializers.Serializer):
    count = serializers.IntegerField()
    results = InstanceAccountRecordSerializer(many=True)


class InstanceAccountPayloadSerializer(serializers.Serializer):
    instance_id = serializers.IntegerField()
    db_name = serializers.CharField(max_length=128, allow_blank=True, required=False)
    user = serializers.CharField(max_length=128)
    host = serializers.CharField(max_length=64, allow_blank=True, required=False)
    password = serializers.CharField(
        max_length=128, allow_blank=True, required=False, write_only=True
    )
    remark = serializers.CharField(max_length=255, allow_blank=True, required=False)

    def validate_user(self, value):
        user = value.strip()
        if not user:
            raise serializers.ValidationError("User cannot be blank.")
        return user

    def validate_host(self, value):
        return value.strip()

    def validate_db_name(self, value):
        return value.strip()

    def validate_password(self, value):
        if not value or not value.strip():
            raise serializers.ValidationError("Password cannot be blank.")
        return value

    def validate_remark(self, value):
        return value.strip()


class InstanceAccountPasswordSerializer(serializers.Serializer):
    instance_id = serializers.IntegerField()
    db_name = serializers.CharField(max_length=128, allow_blank=True, required=False)
    db_name_user = serializers.CharField(
        max_length=260, allow_blank=True, required=False
    )
    user_host = serializers.CharField(max_length=260, allow_blank=True, required=False)
    user = serializers.CharField(max_length=128)
    host = serializers.CharField(max_length=64, allow_blank=True, required=False)
    password = serializers.CharField(max_length=128, write_only=True)


class InstanceAccountLockSerializer(serializers.Serializer):
    instance_id = serializers.IntegerField()
    user_host = serializers.CharField(max_length=260)
    locked = serializers.BooleanField()


class InstanceAccountDeleteSerializer(serializers.Serializer):
    instance_id = serializers.IntegerField()
    db_name = serializers.CharField(max_length=128, allow_blank=True, required=False)
    db_name_user = serializers.CharField(
        max_length=260, allow_blank=True, required=False
    )
    user_host = serializers.CharField(max_length=260, allow_blank=True, required=False)
    user = serializers.CharField(max_length=128)
    host = serializers.CharField(max_length=64, allow_blank=True, required=False)


class InstanceAccountGrantSerializer(serializers.Serializer):
    instance_id = serializers.IntegerField()
    user_host = serializers.CharField(max_length=260, allow_blank=True, required=False)
    db_name_user = serializers.CharField(
        max_length=260, allow_blank=True, required=False
    )
    op_type = serializers.ChoiceField(choices=[0, 1], required=False)
    priv_type = serializers.ChoiceField(choices=[0, 1, 2, 3], required=False)
    privs = serializers.JSONField(required=False)
    db_name = serializers.CharField(max_length=128, allow_blank=True, required=False)
    db_names = serializers.ListField(
        child=serializers.CharField(max_length=128), required=False
    )
    tb_name = serializers.CharField(max_length=128, allow_blank=True, required=False)
    tb_names = serializers.ListField(
        child=serializers.CharField(max_length=128), required=False
    )
    col_names = serializers.ListField(
        child=serializers.CharField(max_length=128), required=False
    )
    roles = serializers.ListField(child=serializers.JSONField(), required=False)


class InstanceParamRecordSerializer(serializers.Serializer):
    id = serializers.IntegerField(required=False)
    variable_name = serializers.CharField()
    runtime_value = serializers.CharField(allow_blank=True, allow_null=True)
    default_value = serializers.CharField(required=False, allow_blank=True)
    valid_values = serializers.CharField(required=False, allow_blank=True)
    description = serializers.CharField(required=False, allow_blank=True)
    editable = serializers.BooleanField(default=False)
    configured = serializers.BooleanField(default=False)


class InstanceParamListSerializer(serializers.Serializer):
    count = serializers.IntegerField()
    results = InstanceParamRecordSerializer(many=True)


class InstanceParamHistorySerializer(serializers.ModelSerializer):
    instance_name = serializers.CharField(
        source="instance.instance_name", read_only=True
    )

    class Meta:
        model = ParamHistory
        fields = (
            "instance_name",
            "variable_name",
            "old_var",
            "new_var",
            "set_sql",
            "user_name",
            "user_display",
            "create_time",
        )


class InstanceParamHistoryListSerializer(serializers.Serializer):
    count = serializers.IntegerField()
    results = InstanceParamHistorySerializer(many=True)


class InstanceParamEditSerializer(serializers.Serializer):
    instance_id = serializers.IntegerField()
    variable_name = serializers.CharField(max_length=64)
    runtime_value = serializers.CharField(max_length=1024)

    def validate_variable_name(self, value):
        variable_name = value.strip()
        if not variable_name:
            raise serializers.ValidationError("Parameter name cannot be blank.")
        return variable_name

    def validate_runtime_value(self, value):
        return value.strip()


class InstanceDiagnosticListSerializer(serializers.Serializer):
    count = serializers.IntegerField()
    results = serializers.ListField(child=serializers.JSONField())


class InstanceDiagnosticKillPreviewSerializer(serializers.Serializer):
    instance_id = serializers.IntegerField()
    thread_ids = serializers.ListField(child=serializers.IntegerField(), min_length=1)


class InstanceDiagnosticKillResultSerializer(serializers.Serializer):
    kill_sql = serializers.CharField(allow_blank=True)


class InstanceListSerializer(serializers.ModelSerializer):
    resource_group_ids = serializers.SerializerMethodField()
    instance_tag_ids = serializers.SerializerMethodField()

    def get_resource_group_ids(self, obj):
        return list(
            obj.resource_group.values_list("group_id", flat=True).order_by("group_id")
        )

    def get_instance_tag_ids(self, obj):
        return list(obj.instance_tag.values_list("id", flat=True).order_by("id"))

    class Meta:
        model = Instance
        fields = (
            "id",
            "instance_name",
            "type",
            "db_type",
            "host",
            "port",
            "user",
            "is_ssl",
            "verify_ssl",
            "db_name",
            "charset",
            "service_name",
            "sid",
            "resource_group_ids",
            "instance_tag_ids",
        )


class InstanceEditorSerializer(serializers.ModelSerializer):
    resource_group_ids = serializers.SerializerMethodField()
    instance_tag_ids = serializers.SerializerMethodField()

    def get_resource_group_ids(self, obj):
        return list(
            obj.resource_group.values_list("group_id", flat=True).order_by("group_id")
        )

    def get_instance_tag_ids(self, obj):
        return list(obj.instance_tag.values_list("id", flat=True).order_by("id"))

    class Meta:
        model = Instance
        fields = (
            "id",
            "instance_name",
            "type",
            "db_type",
            "host",
            "port",
            "user",
            "is_ssl",
            "verify_ssl",
            "db_name",
            "show_db_name_regex",
            "denied_db_name_regex",
            "charset",
            "service_name",
            "sid",
            "resource_group_ids",
            "instance_tag_ids",
        )


class InstanceCreateSerializer(serializers.ModelSerializer):
    resource_group_ids = serializers.PrimaryKeyRelatedField(
        source="resource_group",
        queryset=ResourceGroup.objects.filter(is_deleted=0),
        many=True,
        required=False,
    )
    instance_tag_ids = serializers.PrimaryKeyRelatedField(
        source="instance_tag",
        queryset=InstanceTag.objects.filter(active=True),
        many=True,
        required=False,
    )

    def validate_instance_name(self, value):
        instance_name = value.strip()
        if not instance_name:
            raise serializers.ValidationError("Instance name cannot be blank.")
        return instance_name

    def validate_host(self, value):
        host = value.strip()
        if not host:
            raise serializers.ValidationError("Host cannot be blank.")
        return host

    def validate_user(self, value):
        return value.strip()

    def validate_db_name(self, value):
        return value.strip()

    def validate_show_db_name_regex(self, value):
        return value.strip()

    def validate_denied_db_name_regex(self, value):
        return value.strip()

    def validate_charset(self, value):
        return value.strip()

    def validate_service_name(self, value):
        if value is None:
            return value
        return value.strip()

    def validate_sid(self, value):
        if value is None:
            return value
        return value.strip()

    def create(self, validated_data):
        resource_groups = validated_data.pop("resource_group", [])
        instance_tags = validated_data.pop("instance_tag", [])
        with transaction.atomic():
            instance = Instance.objects.create(**validated_data)
            instance.resource_group.set(resource_groups)
            instance.instance_tag.set(instance_tags)
        return instance

    class Meta:
        model = Instance
        fields = (
            "instance_name",
            "type",
            "db_type",
            "host",
            "port",
            "user",
            "password",
            "is_ssl",
            "verify_ssl",
            "db_name",
            "show_db_name_regex",
            "denied_db_name_regex",
            "charset",
            "service_name",
            "sid",
            "resource_group_ids",
            "instance_tag_ids",
        )
        extra_kwargs = {"password": {"write_only": True, "required": False}}


class InstanceConnectionTestRequestSerializer(serializers.Serializer):
    instance_name = serializers.CharField(
        max_length=50, required=False, allow_blank=True
    )
    type = serializers.ChoiceField(
        choices=Instance._meta.get_field("type").choices,
        required=False,
        default="master",
    )
    db_type = serializers.ChoiceField(
        choices=Instance._meta.get_field("db_type").choices
    )
    host = serializers.CharField(max_length=200)
    port = serializers.IntegerField(min_value=1)
    user = serializers.CharField(max_length=200, required=False, allow_blank=True)
    password = serializers.CharField(
        max_length=300, required=False, allow_blank=True, write_only=True
    )
    is_ssl = serializers.BooleanField(required=False, default=False)
    verify_ssl = serializers.BooleanField(required=False, default=True)
    db_name = serializers.CharField(max_length=64, required=False, allow_blank=True)
    show_db_name_regex = serializers.CharField(
        max_length=1024, required=False, allow_blank=True
    )
    denied_db_name_regex = serializers.CharField(
        max_length=1024, required=False, allow_blank=True
    )
    charset = serializers.CharField(max_length=20, required=False, allow_blank=True)
    service_name = serializers.CharField(
        max_length=50, required=False, allow_blank=True, allow_null=True
    )
    sid = serializers.CharField(
        max_length=50, required=False, allow_blank=True, allow_null=True
    )

    def validate_instance_name(self, value):
        return value.strip()

    def validate_host(self, value):
        host = value.strip()
        if not host:
            raise serializers.ValidationError("Host cannot be blank.")
        return host

    def validate_user(self, value):
        return value.strip()

    def validate_db_name(self, value):
        return value.strip()

    def validate_show_db_name_regex(self, value):
        return value.strip()

    def validate_denied_db_name_regex(self, value):
        return value.strip()

    def validate_charset(self, value):
        return value.strip()

    def validate_service_name(self, value):
        if value is None:
            return value
        return value.strip()

    def validate_sid(self, value):
        if value is None:
            return value
        return value.strip()

    def build_instance(self):
        validated_data = self.validated_data.copy()
        return Instance(
            instance_name=validated_data.get("instance_name", ""),
            type=validated_data.get("type", "master"),
            db_type=validated_data["db_type"],
            host=validated_data["host"],
            port=validated_data["port"],
            user=validated_data.get("user", ""),
            password=validated_data.get("password", ""),
            is_ssl=validated_data.get("is_ssl", False),
            verify_ssl=validated_data.get("verify_ssl", True),
            db_name=validated_data.get("db_name", ""),
            show_db_name_regex=validated_data.get("show_db_name_regex", ""),
            denied_db_name_regex=validated_data.get("denied_db_name_regex", ""),
            charset=validated_data.get("charset", ""),
            service_name=validated_data.get("service_name"),
            sid=validated_data.get("sid"),
        )


class InstanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Instance
        fields = "__all__"
        extra_kwargs = {"password": {"write_only": True}}


class InstanceDetailSerializer(serializers.ModelSerializer):
    resource_group_ids = serializers.PrimaryKeyRelatedField(
        source="resource_group",
        queryset=ResourceGroup.objects.filter(is_deleted=0),
        many=True,
        required=False,
    )
    instance_tag_ids = serializers.PrimaryKeyRelatedField(
        source="instance_tag",
        queryset=InstanceTag.objects.filter(active=True),
        many=True,
        required=False,
    )

    def validate_instance_name(self, value):
        instance_name = value.strip()
        if not instance_name:
            raise serializers.ValidationError("Instance name cannot be blank.")
        return instance_name

    def validate_host(self, value):
        host = value.strip()
        if not host:
            raise serializers.ValidationError("Host cannot be blank.")
        return host

    def validate_user(self, value):
        return value.strip()

    def validate_db_name(self, value):
        return value.strip()

    def validate_show_db_name_regex(self, value):
        return value.strip()

    def validate_denied_db_name_regex(self, value):
        return value.strip()

    def validate_charset(self, value):
        return value.strip()

    def validate_service_name(self, value):
        if value is None:
            return value
        return value.strip()

    def validate_sid(self, value):
        if value is None:
            return value
        return value.strip()

    def update(self, instance, validated_data):
        resource_groups = validated_data.pop("resource_group", None)
        instance_tags = validated_data.pop("instance_tag", None)
        password = validated_data.pop("password", None)

        with transaction.atomic():
            for field, value in validated_data.items():
                setattr(instance, field, value)

            if password not in (None, ""):
                instance.password = password

            instance.save()

            if resource_groups is not None:
                instance.resource_group.set(resource_groups)

            if instance_tags is not None:
                instance.instance_tag.set(instance_tags)

        return instance

    class Meta:
        model = Instance
        fields = (
            "instance_name",
            "type",
            "db_type",
            "host",
            "port",
            "user",
            "password",
            "is_ssl",
            "verify_ssl",
            "db_name",
            "show_db_name_regex",
            "denied_db_name_regex",
            "charset",
            "service_name",
            "sid",
            "resource_group_ids",
            "instance_tag_ids",
        )
        extra_kwargs = {
            "password": {"write_only": True},
            "instance_name": {"required": False},
            "type": {"required": False},
            "db_type": {"required": False},
            "host": {"required": False},
            "port": {"required": False},
            "user": {"required": False},
            "is_ssl": {"required": False},
            "verify_ssl": {"required": False},
            "db_name": {"required": False},
            "show_db_name_regex": {"required": False},
            "denied_db_name_regex": {"required": False},
            "charset": {"required": False},
            "service_name": {"required": False},
            "sid": {"required": False},
        }


class QueryPrivilegesApplySerializer(serializers.ModelSerializer):
    class Meta:
        model = QueryPrivilegesApply
        fields = "__all__"


class InstanceTagLookupSerializer(serializers.ModelSerializer):
    label = serializers.SerializerMethodField()

    def get_label(self, obj):
        return obj.tag_name

    class Meta:
        model = InstanceTag
        fields = ("id", "tag_name", "label")


class InstanceTagManagementSerializer(serializers.ModelSerializer):
    usage_count = serializers.SerializerMethodField()

    def get_usage_count(self, obj):
        return obj.instance_set.count()

    class Meta:
        model = InstanceTag
        fields = ("id", "tag_code", "tag_name", "active", "usage_count")


class InstanceTagCreateSerializer(serializers.ModelSerializer):
    def validate_tag_code(self, value):
        tag_code = value.strip()
        if not tag_code:
            raise serializers.ValidationError("Tag code cannot be blank.")
        return tag_code

    def validate_tag_name(self, value):
        tag_name = value.strip()
        if not tag_name:
            raise serializers.ValidationError("Tag name cannot be blank.")
        return tag_name

    class Meta:
        model = InstanceTag
        fields = ("tag_code", "tag_name", "active")


class InstanceTagUpdateSerializer(serializers.ModelSerializer):
    def validate_tag_name(self, value):
        tag_name = value.strip()
        if not tag_name:
            raise serializers.ValidationError("Tag name cannot be blank.")
        return tag_name

    class Meta:
        model = InstanceTag
        fields = ("tag_name", "active")


class ResourceGroupLookupSerializer(serializers.ModelSerializer):
    label = serializers.SerializerMethodField()

    def get_label(self, obj):
        return obj.group_name

    class Meta:
        model = ResourceGroup
        fields = ("group_id", "group_name", "label")


class InstanceMetadataSerializer(serializers.Serializer):
    instance_types = ChoiceOptionSerializer(many=True)
    db_types = ChoiceOptionSerializer(many=True)
    tags = InstanceTagLookupSerializer(many=True)
    resource_groups = ResourceGroupLookupSerializer(many=True)


class InstanceConnectionTestResultSerializer(serializers.Serializer):
    success = serializers.BooleanField()
    message = serializers.CharField()


class InstanceResourceSerializer(serializers.Serializer):
    instance_id = serializers.IntegerField(label="Instance ID")
    resource_type = serializers.ChoiceField(
        choices=["database", "schema", "table", "column"], label="Resource type"
    )
    db_name = serializers.CharField(
        required=False, allow_blank=True, label="Database name"
    )
    schema_name = serializers.CharField(
        required=False, allow_blank=True, label="Schema name"
    )
    tb_name = serializers.CharField(
        required=False, allow_blank=True, label="Table name"
    )

    def validate(self, attrs):
        return attrs


class InstanceResourceListSerializer(serializers.Serializer):
    count = serializers.IntegerField()
    result = serializers.ListField()
