from rest_framework import serializers
from sql.models import (
    Users,
    Instance,
    InstanceTag,
    Tunnel,
    AliyunRdsConfig,
    CloudAccessKey,
    SqlWorkflow,
    SqlWorkflowContent,
    ResourceGroup,
    WorkflowAudit,
    WorkflowLog,
    QueryPrivilegesApply,
    QueryPrivileges,
    QueryLog,
    ArchiveConfig,
)
from django.contrib.auth.models import Group, Permission
from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError
from django.db import transaction
from sql.engines import get_engine
from sql.utils.workflow_audit import Audit, get_auditor
from sql.utils.resource_group import user_instances
from common.utils.const import WorkflowType, WorkflowStatus
from common.config import SysConfig
import traceback
import logging
from sql.offlinedownload import OffLineDownLoad

logger = logging.getLogger("default")


class UserSerializer(serializers.ModelSerializer):
    def create(self, validated_data):
        with transaction.atomic():
            extra_data = dict()
            for field in ("groups", "user_permissions", "resource_group"):
                if field in validated_data.keys():
                    extra_data[field] = validated_data.pop(field)
            user = Users(**validated_data)
            user.set_password(validated_data["password"])
            user.save()
            for field in extra_data.keys():
                getattr(user, field).set(extra_data[field])
            return user

    def validate_password(self, password):
        try:
            validate_password(password)
        except ValidationError as msg:
            raise serializers.ValidationError(msg)
        return password

    class Meta:
        model = Users
        fields = "__all__"
        extra_kwargs = {"password": {"write_only": True}, "display": {"required": True}}


class UserDetailSerializer(serializers.ModelSerializer):
    def update(self, instance, validated_data):
        for attr, value in validated_data.items():
            if attr == "password":
                instance.set_password(value)
            elif attr in ("groups", "user_permissions", "resource_group"):
                getattr(instance, attr).set(value)
            else:
                setattr(instance, attr, value)
        instance.save()
        return instance

    def validate_password(self, password):
        try:
            validate_password(password)
        except ValidationError as msg:
            raise serializers.ValidationError(msg)
        return password

    class Meta:
        model = Users
        fields = "__all__"
        extra_kwargs = {
            "password": {"write_only": True, "required": False},
            "username": {"required": False},
        }


class GroupSerializer(serializers.ModelSerializer):
    class Meta:
        model = Group
        fields = "__all__"


class PermissionSerializer(serializers.ModelSerializer):
    app_label = serializers.CharField(source="content_type.app_label", read_only=True)
    model = serializers.CharField(source="content_type.model", read_only=True)

    class Meta:
        model = Permission
        fields = ("id", "name", "codename", "app_label", "model")


class ResourceGroupListSerializer(serializers.ModelSerializer):
    user_count = serializers.SerializerMethodField()
    instance_count = serializers.SerializerMethodField()

    def get_user_count(self, obj):
        return obj.users_set.count()

    def get_instance_count(self, obj):
        return obj.instance_set.count()

    class Meta:
        model = ResourceGroup
        fields = ("group_id", "group_name", "user_count", "instance_count")


class ResourceGroupDetailSerializer(serializers.ModelSerializer):
    user_ids = serializers.PrimaryKeyRelatedField(
        source="users_set", queryset=Users.objects.all(), many=True, required=False
    )
    instance_ids = serializers.PrimaryKeyRelatedField(
        source="instance_set",
        queryset=Instance.objects.all(),
        many=True,
        required=False,
    )
    user_count = serializers.SerializerMethodField()
    instance_count = serializers.SerializerMethodField()

    def validate_group_name(self, value):
        group_name = value.strip()
        if not group_name:
            raise serializers.ValidationError("Group name cannot be blank.")
        return group_name

    def get_user_count(self, obj):
        return obj.users_set.count()

    def get_instance_count(self, obj):
        return obj.instance_set.count()

    def create(self, validated_data):
        users = validated_data.pop("users_set", [])
        instances = validated_data.pop("instance_set", [])
        with transaction.atomic():
            group = ResourceGroup.objects.create(**validated_data)
            group.users_set.set(users)
            group.instance_set.set(instances)
        return group

    def update(self, instance, validated_data):
        users = validated_data.pop("users_set", None)
        instances = validated_data.pop("instance_set", None)
        for attr, value in validated_data.items():
            setattr(instance, attr, value)
        with transaction.atomic():
            instance.save()
            if users is not None:
                instance.users_set.set(users)
            if instances is not None:
                instance.instance_set.set(instances)
        return instance

    class Meta:
        model = ResourceGroup
        fields = (
            "group_id",
            "group_name",
            "user_ids",
            "instance_ids",
            "user_count",
            "instance_count",
        )


class ResourceGroupUserLookupSerializer(serializers.ModelSerializer):
    label = serializers.SerializerMethodField()

    def get_label(self, obj):
        return obj.display or obj.username

    class Meta:
        model = Users
        fields = ("id", "username", "display", "label")


class ResourceGroupInstanceLookupSerializer(serializers.ModelSerializer):
    label = serializers.SerializerMethodField()

    def get_label(self, obj):
        return f"{obj.instance_name} | {obj.db_type} | {obj.host}"

    class Meta:
        model = Instance
        fields = ("id", "instance_name", "db_type", "host", "label")


class UserAuthSerializer(serializers.Serializer):
    password = serializers.CharField(label="Password")


class CurrentUserGroupSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    name = serializers.CharField()


class CurrentUserResourceGroupSerializer(serializers.Serializer):
    group_id = serializers.IntegerField()
    group_name = serializers.CharField()


class CurrentUserSerializer(serializers.Serializer):
    id = serializers.IntegerField()
    username = serializers.CharField()
    display = serializers.CharField(allow_blank=True)
    email = serializers.CharField(allow_blank=True)
    is_superuser = serializers.BooleanField()
    is_staff = serializers.BooleanField()
    is_active = serializers.BooleanField()
    groups = CurrentUserGroupSerializer(many=True)
    resource_groups = CurrentUserResourceGroupSerializer(many=True)
    permissions = serializers.ListField(child=serializers.CharField())
    two_factor_auth_types = serializers.ListField(child=serializers.CharField())


class CurrentUserProfileUpdateSerializer(serializers.Serializer):
    display = serializers.CharField(max_length=50)

    def validate_display(self, value):
        display = value.strip()
        if not display:
            raise serializers.ValidationError("Display name cannot be blank.")
        return display


class CurrentUserPasswordChangeSerializer(serializers.Serializer):
    current_password = serializers.CharField(write_only=True)
    new_password = serializers.CharField(write_only=True)
    new_password_confirm = serializers.CharField(write_only=True)

    def validate_current_password(self, value):
        user = self.context["request"].user
        if not user.check_password(value):
            raise serializers.ValidationError("Incorrect current password.")
        return value

    def validate(self, attrs):
        if attrs["new_password"] != attrs["new_password_confirm"]:
            raise serializers.ValidationError(
                {"new_password_confirm": "Passwords do not match."}
            )

        if attrs["current_password"] == attrs["new_password"]:
            raise serializers.ValidationError(
                {
                    "new_password": (
                        "New password must be different from the current password."
                    )
                }
            )

        try:
            validate_password(attrs["new_password"], user=self.context["request"].user)
        except ValidationError as msg:
            raise serializers.ValidationError({"new_password": msg.messages})

        return attrs


class TwoFASerializer(serializers.Serializer):
    enable = serializers.ChoiceField(
        choices=["true", "false"], label="Enable or disable"
    )
    phone = serializers.CharField(required=False, label="Phone number")
    auth_type = serializers.ChoiceField(
        choices=["totp", "sms"],
        label="Verification type: totp-Google Authenticator, sms-SMS code",
    )

    def validate(self, attrs):
        auth_type = attrs.get("auth_type")
        enable = attrs.get("enable")

        if auth_type == "sms" and enable == "true":
            if not attrs.get("phone"):
                raise serializers.ValidationError({"errors": "Missing phone."})

        return attrs


class TwoFAStateSerializer(serializers.Serializer):
    pass


class TwoFASaveSerializer(serializers.Serializer):
    key = serializers.CharField(required=False, label="Secret key")
    phone = serializers.CharField(required=False, label="Phone number")
    auth_type = serializers.ChoiceField(
        choices=["disabled", "totp", "sms"],
        label="Verification type: disabled-off, totp-Google Authenticator, sms-SMS code",
    )

    def validate(self, attrs):
        auth_type = attrs.get("auth_type")
        key = attrs.get("key")
        phone = attrs.get("phone")

        if auth_type == "sms":
            if not phone:
                raise serializers.ValidationError({"errors": "Missing phone."})

        if auth_type == "totp":
            if not key:
                raise serializers.ValidationError({"errors": "Missing key."})

        return attrs


class TwoFAVerifySerializer(serializers.Serializer):
    otp = serializers.IntegerField(label="One-time password / code")
    key = serializers.CharField(required=False, label="Secret key")
    phone = serializers.CharField(required=False, label="Phone number")
    auth_type = serializers.CharField(label="Verification method")

    def validate(self, attrs):
        auth_type = attrs.get("auth_type")

        if auth_type == "sms":
            if not attrs.get("phone"):
                raise serializers.ValidationError({"errors": "Missing phone."})

        return attrs


class InstanceListSerializer(serializers.ModelSerializer):
    tunnel_id = serializers.IntegerField(read_only=True)
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
            "tunnel_id",
            "resource_group_ids",
            "instance_tag_ids",
        )


class InstanceEditorSerializer(serializers.ModelSerializer):
    tunnel_id = serializers.IntegerField(read_only=True)
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
            "tunnel_id",
            "resource_group_ids",
            "instance_tag_ids",
        )


class InstanceCreateSerializer(serializers.ModelSerializer):
    tunnel_id = serializers.PrimaryKeyRelatedField(
        source="tunnel",
        queryset=Tunnel.objects.all(),
        allow_null=True,
        required=False,
    )
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
            "tunnel_id",
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
    tunnel_id = serializers.PrimaryKeyRelatedField(
        source="tunnel",
        queryset=Tunnel.objects.all(),
        allow_null=True,
        required=False,
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
            tunnel=validated_data.get("tunnel"),
        )


class InstanceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Instance
        fields = "__all__"
        extra_kwargs = {"password": {"write_only": True}}


class InstanceDetailSerializer(serializers.ModelSerializer):
    class Meta:
        model = Instance
        fields = "__all__"
        extra_kwargs = {
            "password": {"write_only": True},
            "instance_name": {"required": False},
            "type": {"required": False},
            "db_type": {"required": False},
            "host": {"required": False},
        }


class TunnelSerializer(serializers.ModelSerializer):
    class Meta:
        model = Tunnel
        fields = "__all__"
        write_only_fields = ["password", "pkey", "pkey_password"]


class CloudAccessKeySerializer(serializers.ModelSerializer):
    class Meta:
        model = CloudAccessKey
        fields = "__all__"


class AliyunRdsSerializer(serializers.ModelSerializer):
    ak = CloudAccessKeySerializer()

    def create(self, validated_data):
        """Create an Aliyun RDS instance including an access key."""
        rds_data = validated_data.pop("ak")

        try:
            with transaction.atomic():
                ak = CloudAccessKey.objects.create(**rds_data)
                rds = AliyunRdsConfig.objects.create(ak=ak, **validated_data)
        except Exception as e:
            logger.error(f"Error creating AliyunRds: {traceback.format_exc()}")
            raise serializers.ValidationError({"errors": str(e)})
        else:
            return rds

    class Meta:
        model = AliyunRdsConfig
        fields = ("id", "rds_dbinstanceid", "is_enable", "instance", "ak")


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


class ChoiceOptionSerializer(serializers.Serializer):
    value = serializers.CharField()
    label = serializers.CharField()


class InstanceTagLookupSerializer(serializers.ModelSerializer):
    label = serializers.SerializerMethodField()

    def get_label(self, obj):
        return obj.tag_name

    class Meta:
        model = InstanceTag
        fields = ("id", "tag_name", "label")


class TunnelLookupSerializer(serializers.ModelSerializer):
    label = serializers.SerializerMethodField()

    def get_label(self, obj):
        return f"{obj.tunnel_name} | {obj.host}:{obj.port}"

    class Meta:
        model = Tunnel
        fields = ("id", "tunnel_name", "host", "port", "label")


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
    tunnels = TunnelLookupSerializer(many=True)
    resource_groups = ResourceGroupLookupSerializer(many=True)


class InstanceConnectionTestResultSerializer(serializers.Serializer):
    success = serializers.BooleanField()
    message = serializers.CharField()


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


class ArchiveConfigSerializer(serializers.ModelSerializer):
    class Meta:
        model = ArchiveConfig
        fields = "__all__"


class InstanceResourceSerializer(serializers.Serializer):
    instance_id = serializers.IntegerField(label="Instance ID")
    resource_type = serializers.ChoiceField(
        choices=["database", "schema", "table", "column"], label="Resource type"
    )
    db_name = serializers.CharField(required=False, label="Database name")
    schema_name = serializers.CharField(required=False, label="Schema name")
    tb_name = serializers.CharField(required=False, label="Table name")

    def validate(self, attrs):
        instance_id = attrs.get("instance_id")

        try:
            Instance.objects.get(id=instance_id)
        except Instance.DoesNotExist:
            raise serializers.ValidationError({"errors": "Instance does not exist."})

        return attrs


class InstanceResourceListSerializer(serializers.Serializer):
    count = serializers.IntegerField()
    result = serializers.ListField()


class ExecuteCheckSerializer(serializers.Serializer):
    instance_id = serializers.IntegerField(label="Instance ID")
    db_name = serializers.CharField(label="Database name")
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

    def create(self, validated_data):
        """Create workflow using the original submit flow."""
        workflow_data = validated_data.pop("workflow")
        instance = workflow_data["instance"]
        sql_content = validated_data["sql_content"].strip()
        group = ResourceGroup.objects.get(pk=workflow_data["group_id"])
        engineer = workflow_data.get("engineer")

        # Admins can specify submitter info
        if self.context["request"].user.is_superuser and engineer:
            try:
                user = Users.objects.get(username=engineer)
            except Users.DoesNotExist:
                raise serializers.ValidationError(
                    {"errors": f"User does not exist: {engineer}"}
                )
        # Submitter can only be self for non-admins
        else:
            user = self.context["request"].user

        # Validate group permissions of submitting user
        try:
            user_instances(user, tag_codes=["can_write"]).get(id=instance.id)
        except instance.DoesNotExist:
            if workflow_data["is_offline_export"]:
                pass
            else:
                raise serializers.ValidationError(
                    {"errors": "The instance is not associated with your group."}
                )

        # Run engine check again to prevent bypass
        try:
            check_engine = get_engine(instance=instance)
            sql_export = OffLineDownLoad()
            if workflow_data["is_offline_export"]:
                instance.sql_content = sql_content
                instance.db_name = workflow_data["db_name"]
                check_result = sql_export.pre_count_check(workflow=instance)
            else:
                check_result = check_engine.execute_check(
                    db_name=workflow_data["db_name"], sql=sql_content
                )
        except Exception as e:
            raise serializers.ValidationError({"errors": str(e)})

        # If backup switch is off but engine supports backup, force backup on
        is_backup = (
            workflow_data["is_backup"] if "is_backup" in workflow_data.keys() else False
        )
        sys_config = SysConfig()
        if not sys_config.get("enable_backup_switch") and check_engine.auto_backup:
            if workflow_data["is_offline_export"]:
                pass
            else:
                is_backup = True

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
                # Auto-create workflow audit chain
                auditor = get_auditor(workflow=workflow)
                auditor.create_audit()
        except Exception as e:
            logger.error(f"Error submitting workflow: {traceback.format_exc()}")
            raise serializers.ValidationError({"errors": str(e)})
        # In some cases auto-approval happens on submit; rewrite workflow status here
        if auditor.audit.current_status == WorkflowStatus.REJECTED:
            auditor.workflow.status = "workflow_autoreviewwrong"
        elif auditor.audit.current_status == WorkflowStatus.PASSED:
            auditor.workflow.status = "workflow_review_pass"
        auditor.workflow.save()
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


class AuditWorkflowSerializer(serializers.Serializer):
    workflow_id = serializers.IntegerField(label="Workflow ID")
    audit_remark = serializers.CharField(label="Approval remark")
    workflow_type = serializers.ChoiceField(
        choices=WorkflowType.choices,
        label="Workflow type: 1-query privilege apply, 2-SQL release apply, 3-data archive apply",
    )
    audit_type = serializers.ChoiceField(choices=["pass", "cancel"], label="Audit type")

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

    def validate(self, attrs):
        workflow_id = attrs.get("workflow_id")
        workflow_type = attrs.get("workflow_type")
        mode = attrs.get("mode")

        # mode is required for SQL release workflows
        if workflow_type == 2:
            if not mode:
                raise serializers.ValidationError({"errors": "Missing mode."})

        try:
            WorkflowAudit.objects.get(
                workflow_id=workflow_id, workflow_type=workflow_type
            )
        except WorkflowAudit.DoesNotExist:
            raise serializers.ValidationError({"errors": "Workflow does not exist."})

        return attrs
