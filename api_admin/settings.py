import logging

from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.db import transaction
from drf_spectacular.utils import extend_schema
from rest_framework import serializers, status, views
from rest_framework.response import Response

from common.check import (
    validate_email_payload,
    validate_file_storage_payload,
    validate_go_inception_payload,
)
from common.config import SysConfig
from sql.inventory import (
    INVENTORY_REFRESH_INTERVAL_CHOICES,
    ensure_inventory_refresh_schedule,
)
from sql.engines.mysql_ddl import validate_binary_path
from sql.models import InstanceTag, ResourceGroup

from api_core.permissions import IsStaffOrSuperuser
from api_core.response import success_response

User = get_user_model()
logger = logging.getLogger("default")

DEFAULT_CHAT_MODEL = "gpt-3.5-turbo"
DEFAULT_QUERY_TEMPLATE = (
    "You are an engineer familiar with {{db_type}}. I will give you basic "
    "information and requirements. Generate one query for me. Do not return "
    "comments or numbering. Return only the query: {{table_schema}} \n "
    "{{user_input}}"
)
NOTIFY_PHASE_OPTIONS = ("Apply", "Pass", "Execute", "Cancel")
AUTO_REVIEW_DB_TYPES = (
    "mysql",
    "oracle",
    "mongo",
    "clickhouse",
    "redis",
    "doris",
)
STORAGE_TYPE_OPTIONS = ("local", "sftp", "s3c", "azure")
SMS_PROVIDER_OPTIONS = ("disabled", "aliyun", "tencent")
TASK_BACKEND_OPTIONS = ("django_q", "celery")
INVENTORY_REFRESH_INTERVAL_OPTIONS = INVENTORY_REFRESH_INTERVAL_CHOICES

SYSTEM_SETTINGS_SCHEMA = (
    {"name": "go_inception_host", "kind": "string", "default": ""},
    {"name": "go_inception_port", "kind": "int", "default": None},
    {"name": "go_inception_user", "kind": "string", "default": ""},
    {"name": "go_inception_password", "kind": "string", "default": ""},
    {"name": "inception_remote_backup_host", "kind": "string", "default": ""},
    {"name": "inception_remote_backup_port", "kind": "int", "default": None},
    {"name": "inception_remote_backup_user", "kind": "string", "default": ""},
    {"name": "inception_remote_backup_password", "kind": "string", "default": ""},
    {"name": "critical_ddl_regex", "kind": "string", "default": ""},
    {"name": "auto_review_wrong", "kind": "int", "default": None},
    {"name": "enable_backup_switch", "kind": "bool", "default": False},
    {"name": "auto_review", "kind": "bool", "default": False},
    {"name": "auto_review_tag", "kind": "list_string", "default": []},
    {
        "name": "auto_review_db_type",
        "kind": "list_choice",
        "choices": AUTO_REVIEW_DB_TYPES,
        "default": [],
    },
    {"name": "auto_review_regex", "kind": "string", "default": ""},
    {"name": "auto_review_max_update_rows", "kind": "int", "default": None},
    {"name": "manual", "kind": "bool", "default": False},
    {"name": "ddl_dml_separation", "kind": "bool", "default": False},
    {"name": "ban_self_audit", "kind": "bool", "default": False},
    {"name": "real_row_count", "kind": "bool", "default": False},
    {"name": "data_masking", "kind": "bool", "default": False},
    {"name": "query_check", "kind": "bool", "default": False},
    {"name": "disable_star", "kind": "bool", "default": False},
    {"name": "max_execution_time", "kind": "int", "default": None},
    {"name": "admin_query_limit", "kind": "int", "default": None},
    {"name": "max_export_rows", "kind": "int", "default": None},
    {
        "name": "task_backend",
        "kind": "choice",
        "choices": TASK_BACKEND_OPTIONS,
        "default": "django_q",
    },
    {
        "name": "inventory_refresh_interval",
        "kind": "choice",
        "choices": INVENTORY_REFRESH_INTERVAL_OPTIONS,
        "default": "24h",
    },
    {"name": "celery_broker_url", "kind": "string", "default": ""},
    {"name": "celery_result_backend", "kind": "string", "default": ""},
    {
        "name": "celery_task_default_queue",
        "kind": "string",
        "default": "default",
    },
    {"name": "celery_task_soft_time_limit", "kind": "int", "default": None},
    {"name": "celery_task_time_limit", "kind": "int", "default": None},
    {
        "name": "storage_type",
        "kind": "choice",
        "choices": STORAGE_TYPE_OPTIONS,
        "default": "local",
    },
    {"name": "sftp_host", "kind": "string", "default": ""},
    {"name": "sftp_port", "kind": "int", "default": None},
    {"name": "sftp_user", "kind": "string", "default": ""},
    {"name": "sftp_password", "kind": "string", "default": ""},
    {"name": "sftp_path", "kind": "string", "default": ""},
    {"name": "sftp_custom_params", "kind": "string", "default": ""},
    {"name": "s3c_access_key_id", "kind": "string", "default": ""},
    {"name": "s3c_access_key_secret", "kind": "string", "default": ""},
    {"name": "s3c_endpoint", "kind": "string", "default": ""},
    {"name": "s3c_region", "kind": "string", "default": ""},
    {"name": "s3c_bucket_name", "kind": "string", "default": ""},
    {"name": "s3c_path", "kind": "string", "default": ""},
    {"name": "s3c_custom_params", "kind": "string", "default": ""},
    {"name": "azure_container", "kind": "string", "default": ""},
    {"name": "azure_account_name", "kind": "string", "default": ""},
    {"name": "azure_account_key", "kind": "string", "default": ""},
    {"name": "azure_path", "kind": "string", "default": ""},
    {"name": "azure_custom_params", "kind": "string", "default": ""},
    {"name": "gh_ost", "kind": "string", "default": ""},
    {"name": "pt_osc", "kind": "string", "default": ""},
    {"name": "archery_base_url", "kind": "string", "default": ""},
    {"name": "ddl_notify_auth_group", "kind": "list_string", "default": []},
    {
        "name": "notify_phase_control",
        "kind": "list_choice",
        "choices": NOTIFY_PHASE_OPTIONS,
        "default": list(NOTIFY_PHASE_OPTIONS),
    },
    {"name": "mail", "kind": "bool", "default": False},
    {"name": "mail_ssl", "kind": "bool", "default": False},
    {"name": "mail_smtp_server", "kind": "string", "default": ""},
    {"name": "mail_smtp_port", "kind": "int", "default": None},
    {"name": "mail_smtp_user", "kind": "string", "default": ""},
    {"name": "mail_smtp_password", "kind": "string", "default": ""},
    {"name": "wx", "kind": "bool", "default": False},
    {"name": "wx_corpid", "kind": "string", "default": ""},
    {"name": "wx_agent_id", "kind": "string", "default": ""},
    {"name": "wx_app_secret", "kind": "string", "default": ""},
    {"name": "qywx_webhook", "kind": "bool", "default": False},
    {"name": "feishu_webhook", "kind": "bool", "default": False},
    {"name": "feishu", "kind": "bool", "default": False},
    {"name": "feishu_appid", "kind": "string", "default": ""},
    {"name": "feishu_app_secret", "kind": "string", "default": ""},
    {"name": "generic_webhook_url", "kind": "string", "default": ""},
    {
        "name": "sms_provider",
        "kind": "choice",
        "choices": SMS_PROVIDER_OPTIONS,
        "default": "disabled",
    },
    {"name": "aliyun_access_key_id", "kind": "string", "default": ""},
    {"name": "aliyun_access_key_secret", "kind": "string", "default": ""},
    {"name": "aliyun_sign_name", "kind": "string", "default": ""},
    {"name": "aliyun_template_code", "kind": "string", "default": ""},
    {"name": "aliyun_variable_name", "kind": "string", "default": ""},
    {"name": "tencent_secret_id", "kind": "string", "default": ""},
    {"name": "tencent_secret_key", "kind": "string", "default": ""},
    {"name": "tencent_sign_name", "kind": "string", "default": ""},
    {"name": "tencent_template_id", "kind": "string", "default": ""},
    {"name": "tencent_sdk_appid", "kind": "string", "default": ""},
    {"name": "openai_base_url", "kind": "string", "default": ""},
    {"name": "openai_api_key", "kind": "string", "default": ""},
    {"name": "default_chat_model", "kind": "string", "default": DEFAULT_CHAT_MODEL},
    {
        "name": "default_query_template",
        "kind": "string",
        "default": DEFAULT_QUERY_TEMPLATE,
    },
    {"name": "index_path_url", "kind": "string", "default": ""},
    {"name": "default_auth_group", "kind": "list_string", "default": []},
    {"name": "default_resource_group", "kind": "list_string", "default": []},
    {"name": "api_user_whitelist", "kind": "list_int", "default": []},
    {"name": "lock_time_threshold", "kind": "int", "default": None},
    {"name": "lock_cnt_threshold", "kind": "int", "default": None},
    {"name": "sign_up_enabled", "kind": "bool", "default": False},
    {"name": "watermark_enabled", "kind": "bool", "default": False},
    {"name": "enforce_2fa", "kind": "bool", "default": False},
    {"name": "announcement_content_enabled", "kind": "bool", "default": False},
    {"name": "announcement_content", "kind": "string", "default": ""},
    {"name": "custom_title_suffix", "kind": "string", "default": ""},
)

SYSTEM_SETTINGS_FIELD_MAP = {
    definition["name"]: definition for definition in SYSTEM_SETTINGS_SCHEMA
}


def _split_csv(value):
    if value in (None, ""):
        return []
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [item.strip() for item in str(value).split(",") if item.strip()]


def _get_config_value(config, definition):
    name = definition["name"]
    default = definition.get("default")
    kind = definition["kind"]
    value = config.get(name, default)

    if kind == "bool":
        return bool(value)
    if kind == "int":
        if value in (None, ""):
            return default
        return int(value)
    if kind == "list_int":
        return [int(item) for item in _split_csv(value)]
    if kind in {"list_string", "list_choice"}:
        if value in (None, "") and default is not None:
            return list(default)
        return _split_csv(value)
    if value is None:
        return default
    return value


def load_system_settings():
    config = SysConfig()
    config.get_all_config()
    return {
        definition["name"]: _get_config_value(config, definition)
        for definition in SYSTEM_SETTINGS_SCHEMA
    }


def save_system_settings(settings):
    config = SysConfig()
    with transaction.atomic():
        for definition in SYSTEM_SETTINGS_SCHEMA:
            name = definition["name"]
            value = settings.get(name, definition.get("default"))
            if definition["kind"] in {"list_int", "list_string", "list_choice"}:
                stored_value = ",".join(str(item) for item in value)
            elif definition["kind"] == "int":
                stored_value = "" if value in (None, "") else value
            else:
                stored_value = value
            config.set(name, stored_value)


def build_system_settings_options():
    return {
        "instance_tags": [
            {"value": tag.tag_code, "label": tag.tag_name}
            for tag in InstanceTag.objects.order_by("tag_name")
        ],
        "auth_groups": [
            {"value": group.name, "label": group.name}
            for group in Group.objects.order_by("name")
        ],
        "resource_groups": [
            {"value": group.group_name, "label": group.group_name}
            for group in ResourceGroup.objects.order_by("group_name")
        ],
        "users": [
            {
                "value": user.id,
                "label": f"{user.display or user.username} ({user.username})",
            }
            for user in User.objects.order_by("username")
        ],
        "notify_phases": [
            {"value": phase, "label": phase} for phase in NOTIFY_PHASE_OPTIONS
        ],
        "auto_review_db_types": [
            {"value": db_type, "label": db_type} for db_type in AUTO_REVIEW_DB_TYPES
        ],
        "storage_types": [
            (
                {"value": storage_type, "label": storage_type.upper()}
                if storage_type != "local"
                else {"value": storage_type, "label": "Local"}
            )
            for storage_type in STORAGE_TYPE_OPTIONS
        ],
        "sms_providers": [
            {"value": "disabled", "label": "Disabled"},
            {"value": "aliyun", "label": "Aliyun"},
            {"value": "tencent", "label": "Tencent Cloud"},
        ],
        "task_backends": [
            {
                "value": backend,
                "label": "Django Q" if backend == "django_q" else "Celery",
            }
            for backend in TASK_BACKEND_OPTIONS
        ],
        "inventory_refresh_intervals": [
            {"value": interval, "label": interval}
            for interval in INVENTORY_REFRESH_INTERVAL_OPTIONS
        ],
    }


def sync_inventory_refresh_schedule(force=False):
    try:
        ensure_inventory_refresh_schedule(force=force)
        return True
    except Exception as exc:
        logger.exception("Failed to synchronize the inventory refresh schedule.")
        return False


class SystemSettingsSerializer(serializers.Serializer):
    def get_fields(self):
        fields = {}
        for definition in SYSTEM_SETTINGS_SCHEMA:
            kind = definition["kind"]
            name = definition["name"]
            default = definition.get("default")
            if kind == "bool":
                fields[name] = serializers.BooleanField(required=False, default=default)
            elif kind == "int":
                fields[name] = serializers.IntegerField(
                    required=False, allow_null=True, default=default
                )
            elif kind == "choice":
                fields[name] = serializers.ChoiceField(
                    choices=definition["choices"], required=False, default=default
                )
            elif kind == "list_choice":
                fields[name] = serializers.ListField(
                    child=serializers.ChoiceField(choices=definition["choices"]),
                    required=False,
                    default=list(default),
                )
            elif kind == "list_int":
                fields[name] = serializers.ListField(
                    child=serializers.IntegerField(),
                    required=False,
                    default=list(default),
                )
            elif kind == "list_string":
                fields[name] = serializers.ListField(
                    child=serializers.CharField(),
                    required=False,
                    default=list(default),
                )
            else:
                fields[name] = serializers.CharField(
                    required=False, allow_blank=True, default=default
                )
        return fields

    def validate_auto_review_tag(self, value):
        valid_tags = set(InstanceTag.objects.values_list("tag_code", flat=True))
        invalid_tags = [tag for tag in value if tag not in valid_tags]
        if invalid_tags:
            raise serializers.ValidationError("Unknown instance tags were provided.")
        return value

    def validate_ddl_notify_auth_group(self, value):
        return self._validate_group_names(value)

    def validate_default_auth_group(self, value):
        return self._validate_group_names(value)

    def validate_default_resource_group(self, value):
        valid_groups = set(ResourceGroup.objects.values_list("group_name", flat=True))
        invalid_groups = [
            group_name for group_name in value if group_name not in valid_groups
        ]
        if invalid_groups:
            raise serializers.ValidationError("Unknown resource groups were provided.")
        return value

    def validate_api_user_whitelist(self, value):
        valid_user_ids = set(User.objects.values_list("id", flat=True))
        invalid_user_ids = [
            user_id for user_id in value if user_id not in valid_user_ids
        ]
        if invalid_user_ids:
            raise serializers.ValidationError("Unknown users were provided.")
        return value

    def validate_gh_ost(self, value):
        try:
            return validate_binary_path(value, "gh-ost")
        except ValueError as exc:
            raise serializers.ValidationError(str(exc)) from None

    def validate_pt_osc(self, value):
        try:
            return validate_binary_path(value, "pt-online-schema-change")
        except ValueError as exc:
            raise serializers.ValidationError(str(exc)) from None

    def validate(self, attrs):
        attrs = super().validate(attrs)
        if attrs.get("task_backend") == "celery":
            errors = {}
            broker_url = (attrs.get("celery_broker_url") or "").strip()
            if not broker_url:
                errors["celery_broker_url"] = (
                    "Celery broker URL is required when Celery is enabled."
                )

            soft_limit = attrs.get("celery_task_soft_time_limit")
            hard_limit = attrs.get("celery_task_time_limit")

            if soft_limit is not None and soft_limit <= 0:
                errors["celery_task_soft_time_limit"] = (
                    "Celery soft time limit must be a positive integer."
                )
            if hard_limit is not None and hard_limit <= 0:
                errors["celery_task_time_limit"] = (
                    "Celery hard time limit must be a positive integer."
                )
            if (
                soft_limit is not None
                and hard_limit is not None
                and soft_limit > 0
                and hard_limit > 0
                and soft_limit >= hard_limit
            ):
                errors["celery_task_soft_time_limit"] = (
                    "Celery soft time limit must be less than the hard time limit."
                )

            if errors:
                raise serializers.ValidationError(errors)
        return attrs

    @staticmethod
    def _validate_group_names(value):
        valid_groups = set(Group.objects.values_list("name", flat=True))
        invalid_groups = [
            group_name for group_name in value if group_name not in valid_groups
        ]
        if invalid_groups:
            raise serializers.ValidationError(
                "Unknown permission groups were provided."
            )
        return value


class GoInceptionConnectionTestSerializer(serializers.Serializer):
    go_inception_host = serializers.CharField(required=False, allow_blank=True)
    go_inception_port = serializers.IntegerField(required=False, allow_null=True)
    go_inception_user = serializers.CharField(required=False, allow_blank=True)
    go_inception_password = serializers.CharField(required=False, allow_blank=True)
    inception_remote_backup_host = serializers.CharField(
        required=False, allow_blank=True
    )
    inception_remote_backup_port = serializers.IntegerField(
        required=False, allow_null=True
    )
    inception_remote_backup_user = serializers.CharField(
        required=False, allow_blank=True
    )
    inception_remote_backup_password = serializers.CharField(
        required=False, allow_blank=True
    )


class EmailConnectionTestSerializer(serializers.Serializer):
    mail = serializers.BooleanField(required=False, default=False)
    mail_ssl = serializers.BooleanField(required=False, default=False)
    mail_smtp_server = serializers.CharField(required=False, allow_blank=True)
    mail_smtp_port = serializers.IntegerField(required=False, allow_null=True)
    mail_smtp_user = serializers.CharField(required=False, allow_blank=True)
    mail_smtp_password = serializers.CharField(required=False, allow_blank=True)


class StorageConnectionTestSerializer(serializers.Serializer):
    storage_type = serializers.ChoiceField(
        choices=STORAGE_TYPE_OPTIONS, required=False, default="local"
    )
    max_export_rows = serializers.IntegerField(required=False, allow_null=True)
    sftp_host = serializers.CharField(required=False, allow_blank=True)
    sftp_port = serializers.IntegerField(required=False, allow_null=True)
    sftp_user = serializers.CharField(required=False, allow_blank=True)
    sftp_password = serializers.CharField(required=False, allow_blank=True)
    sftp_path = serializers.CharField(required=False, allow_blank=True)
    sftp_custom_params = serializers.CharField(required=False, allow_blank=True)
    s3c_access_key_id = serializers.CharField(required=False, allow_blank=True)
    s3c_access_key_secret = serializers.CharField(required=False, allow_blank=True)
    s3c_endpoint = serializers.CharField(required=False, allow_blank=True)
    s3c_region = serializers.CharField(required=False, allow_blank=True)
    s3c_bucket_name = serializers.CharField(required=False, allow_blank=True)
    s3c_path = serializers.CharField(required=False, allow_blank=True)
    s3c_custom_params = serializers.CharField(required=False, allow_blank=True)
    azure_account_name = serializers.CharField(required=False, allow_blank=True)
    azure_account_key = serializers.CharField(required=False, allow_blank=True)
    azure_container = serializers.CharField(required=False, allow_blank=True)
    azure_path = serializers.CharField(required=False, allow_blank=True)
    azure_custom_params = serializers.CharField(required=False, allow_blank=True)


class SystemSettingsView(views.APIView):
    permission_classes = [IsStaffOrSuperuser]

    @extend_schema(
        summary="Get Datamingle System Settings",
        responses={200: SystemSettingsSerializer},
        description="Get runtime Datamingle system settings and lookup options for the SPA settings screen.",
    )
    def get(self, request):
        serializer = SystemSettingsSerializer(instance=load_system_settings())
        sync_inventory_refresh_schedule()
        return success_response(
            data={
                "settings": serializer.data,
                "options": build_system_settings_options(),
            }
        )

    @extend_schema(
        summary="Update Datamingle System Settings",
        request=SystemSettingsSerializer,
        responses={200: SystemSettingsSerializer},
        description="Update runtime Datamingle system settings from the SPA settings screen.",
    )
    def put(self, request):
        serializer = SystemSettingsSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        save_system_settings(serializer.validated_data)
        schedule_synced = sync_inventory_refresh_schedule(force=True)
        response_serializer = SystemSettingsSerializer(instance=load_system_settings())
        detail = "System settings updated successfully."
        if not schedule_synced:
            detail = (
                "System settings updated, but the inventory refresh schedule "
                "could not be synchronized. Check the task backend and try again."
            )
        return success_response(
            detail=detail,
            data={
                "settings": response_serializer.data,
                "options": build_system_settings_options(),
                "inventory_refresh_schedule_synced": schedule_synced,
            },
        )


class SystemSettingsGoInceptionTestView(views.APIView):
    permission_classes = [IsStaffOrSuperuser]

    @extend_schema(
        summary="Test goInception Configuration",
        request=GoInceptionConnectionTestSerializer,
        description="Test the configured goInception and backup database connection settings.",
    )
    def post(self, request):
        serializer = GoInceptionConnectionTestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        result = validate_go_inception_payload(serializer.validated_data)
        if result["status"] != 0:
            return Response(
                {"errors": result["msg"]}, status=status.HTTP_400_BAD_REQUEST
            )
        return success_response(
            detail="goInception and backup database connection test succeeded."
        )


class SystemSettingsEmailTestView(views.APIView):
    permission_classes = [IsStaffOrSuperuser]

    @extend_schema(
        summary="Test Email Configuration",
        request=EmailConnectionTestSerializer,
        description="Send a test email using the provided Datamingle mail settings.",
    )
    def post(self, request):
        serializer = EmailConnectionTestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        result = validate_email_payload(
            serializer.validated_data, request.user.email or ""
        )
        if result["status"] != 0:
            return Response(
                {"errors": result["msg"]}, status=status.HTTP_400_BAD_REQUEST
            )
        return success_response(
            detail="A test email has been sent to the current user."
        )


class SystemSettingsStorageTestView(views.APIView):
    permission_classes = [IsStaffOrSuperuser]

    @extend_schema(
        summary="Test File Storage Configuration",
        request=StorageConnectionTestSerializer,
        description="Test the configured Datamingle export storage backend connection.",
    )
    def post(self, request):
        serializer = StorageConnectionTestSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        result = validate_file_storage_payload(serializer.validated_data)
        if result["status"] != 0:
            return Response(
                {"errors": result["msg"]}, status=status.HTTP_400_BAD_REQUEST
            )
        return success_response(detail="Storage connection test succeeded.")
