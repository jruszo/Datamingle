from rest_framework import serializers
from sql.models import (
    Users,
    Instance,
    Tunnel,
    AliyunRdsConfig,
    CloudAccessKey,
    SqlWorkflow,
    SqlWorkflowContent,
    ResourceGroup,
    WorkflowAudit,
    WorkflowLog,
    QueryPrivilegesApply,
    ArchiveConfig,
)
from django.contrib.auth.models import Group
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


class ResourceGroupSerializer(serializers.ModelSerializer):
    class Meta:
        model = ResourceGroup
        fields = "__all__"


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
