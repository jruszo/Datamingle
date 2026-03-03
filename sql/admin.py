# -*- coding: UTF-8 -*-
from django.contrib import admin
from django.contrib.auth.admin import UserAdmin

# Register your models here.
from django.forms import PasswordInput

from .models import (
    Users,
    Instance,
    SqlWorkflow,
    SqlWorkflowContent,
    QueryLog,
    DataMaskingColumns,
    DataMaskingRules,
    AliyunRdsConfig,
    CloudAccessKey,
    ResourceGroup,
    QueryPrivilegesApply,
    QueryPrivileges,
    InstanceAccount,
    InstanceDatabase,
    ArchiveConfig,
    WorkflowAudit,
    WorkflowLog,
    ParamTemplate,
    ParamHistory,
    InstanceTag,
    Tunnel,
    AuditEntry,
    TwoFactorAuthConfig,
)

from sql.form import TunnelForm, InstanceForm


# User management
@admin.register(Users)
class UsersAdmin(UserAdmin):
    list_display = (
        "id",
        "username",
        "display",
        "email",
        "is_superuser",
        "is_staff",
        "is_active",
    )
    search_fields = ("id", "username", "display", "email")
    list_display_links = (
        "id",
        "username",
    )
    ordering = ("id",)
    # Fields shown on edit page
    fieldsets = (
        ("Authentication", {"fields": ("username", "password")}),
        (
            "Personal Info",
            {
                "fields": (
                    "display",
                    "email",
                    "ding_user_id",
                    "wx_user_id",
                    "feishu_open_id",
                )
            },
        ),
        (
            "Permissions",
            {
                "fields": (
                    "is_superuser",
                    "is_active",
                    "is_staff",
                    "groups",
                    "user_permissions",
                )
            },
        ),
        ("Resource Groups", {"fields": ("resource_group",)}),
        ("Other Info", {"fields": ("date_joined", "failed_login_count")}),
    )
    # Fields shown on add page
    add_fieldsets = (
        ("Authentication", {"fields": ("username", "password1", "password2")}),
        (
            "Personal Info",
            {
                "fields": (
                    "display",
                    "email",
                    "ding_user_id",
                    "wx_user_id",
                    "feishu_open_id",
                )
            },
        ),
        (
            "Permissions",
            {
                "fields": (
                    "is_superuser",
                    "is_active",
                    "is_staff",
                    "groups",
                    "user_permissions",
                )
            },
        ),
        ("Resource Groups", {"fields": ("resource_group",)}),
    )
    filter_horizontal = ("groups", "user_permissions", "resource_group")
    list_filter = ("is_staff", "is_superuser", "is_active", "groups", "resource_group")


# User 2FA management
@admin.register(TwoFactorAuthConfig)
class TwoFactorAuthConfigAdmin(admin.ModelAdmin):
    list_display = ("id", "username", "auth_type", "phone", "secret_key", "user_id")


# Resource group management
@admin.register(ResourceGroup)
class ResourceGroupAdmin(admin.ModelAdmin):
    list_display = (
        "group_id",
        "group_name",
        "ding_webhook",
        "feishu_webhook",
        "qywx_webhook",
        "is_deleted",
    )
    exclude = (
        "group_parent_id",
        "group_sort",
        "group_level",
    )


# Instance tag configuration
@admin.register(InstanceTag)
class InstanceTagAdmin(admin.ModelAdmin):
    list_display = ("id", "tag_code", "tag_name", "active", "create_time")
    list_display_links = (
        "id",
        "tag_code",
    )
    fieldsets = (
        (
            None,
            {
                "fields": ("tag_code", "tag_name", "active"),
            },
        ),
    )

    # Tag code is read-only after creation.
    def get_readonly_fields(self, request, obj=None):
        return ("tag_code",) if obj else ()


# Instance management
@admin.register(Instance)
class InstanceAdmin(admin.ModelAdmin):
    form = InstanceForm
    list_display = (
        "id",
        "instance_name",
        "db_type",
        "type",
        "host",
        "port",
        "user",
        "create_time",
    )
    search_fields = ["instance_name", "host", "port", "user"]
    list_filter = ("db_type", "type", "instance_tag")

    def formfield_for_dbfield(self, db_field, **kwargs):
        if db_field.name == "password":
            kwargs["widget"] = PasswordInput(render_value=True)
        return super(InstanceAdmin, self).formfield_for_dbfield(db_field, **kwargs)

    # Aliyun instance relation config
    class AliRdsConfigInline(admin.TabularInline):
        model = AliyunRdsConfig

    # Instance-resource-group relation config
    filter_horizontal = (
        "resource_group",
        "instance_tag",
    )

    inlines = [AliRdsConfigInline]


# SSH tunnel
@admin.register(Tunnel)
class TunnelAdmin(admin.ModelAdmin):
    list_display = ("id", "tunnel_name", "host", "port", "create_time")
    list_display_links = (
        "id",
        "tunnel_name",
    )
    search_fields = ("id", "tunnel_name")
    fieldsets = (
        (
            None,
            {
                "fields": (
                    "tunnel_name",
                    "host",
                    "port",
                    "user",
                    "password",
                    "pkey_path",
                    "pkey_password",
                    "pkey",
                ),
            },
        ),
    )
    ordering = ("id",)
    # Fields shown on add page
    add_fieldsets = (
        ("Tunnel Info", {"fields": ("tunnel_name", "host", "port")}),
        (
            "Connection Info",
            {"fields": ("user", "password", "pkey_path", "pkey_password", "pkey")},
        ),
    )
    form = TunnelForm

    def formfield_for_dbfield(self, db_field, **kwargs):
        if db_field.name in ["password", "pkey_password"]:
            kwargs["widget"] = PasswordInput(render_value=True)
        return super(TunnelAdmin, self).formfield_for_dbfield(db_field, **kwargs)

    # ID is read-only after creation.
    def get_readonly_fields(self, request, obj=None):
        return ("id",) if obj else ()


# SQL workflow content
class SqlWorkflowContentInline(admin.TabularInline):
    model = SqlWorkflowContent


# SQL workflow
@admin.register(SqlWorkflow)
class SqlWorkflowAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "workflow_name",
        "group_name",
        "instance",
        "engineer_display",
        "create_time",
        "status",
        "is_backup",
    )
    search_fields = [
        "id",
        "workflow_name",
        "engineer_display",
        "sqlworkflowcontent__sql_content",
    ]
    list_filter = (
        "group_name",
        "instance__instance_name",
        "status",
        "syntax_type",
    )
    list_display_links = (
        "id",
        "workflow_name",
    )
    inlines = [SqlWorkflowContentInline]


# SQL query logs
@admin.register(QueryLog)
class QueryLogAdmin(admin.ModelAdmin):
    list_display = (
        "instance_name",
        "db_name",
        "sqllog",
        "effect_row",
        "cost_time",
        "user_display",
        "create_time",
    )
    search_fields = ["sqllog", "user_display"]
    list_filter = (
        "instance_name",
        "db_name",
        "user_display",
        "priv_check",
        "hit_rule",
        "masking",
    )


# Query permission list
@admin.register(QueryPrivileges)
class QueryPrivilegesAdmin(admin.ModelAdmin):
    list_display = (
        "privilege_id",
        "user_display",
        "instance",
        "db_name",
        "table_name",
        "valid_date",
        "limit_num",
        "create_time",
    )
    search_fields = ["user_display", "instance__instance_name"]
    list_filter = (
        "user_display",
        "instance",
        "db_name",
        "table_name",
    )


# Query permission request records
@admin.register(QueryPrivilegesApply)
class QueryPrivilegesApplyAdmin(admin.ModelAdmin):
    list_display = (
        "apply_id",
        "user_display",
        "group_name",
        "instance",
        "valid_date",
        "limit_num",
        "create_time",
    )
    search_fields = ["user_display", "instance__instance_name", "db_list", "table_list"]
    list_filter = ("user_display", "group_name", "instance")


# Data masking columns admin
@admin.register(DataMaskingColumns)
class DataMaskingColumnsAdmin(admin.ModelAdmin):
    list_display = (
        "column_id",
        "rule_type",
        "active",
        "instance",
        "table_schema",
        "table_name",
        "column_name",
        "column_comment",
        "create_time",
    )
    search_fields = ["table_name", "column_name"]
    list_filter = ("rule_type", "active", "instance__instance_name")


# Data masking rules admin
@admin.register(DataMaskingRules)
class DataMaskingRulesAdmin(admin.ModelAdmin):
    list_display = (
        "rule_type",
        "rule_regex",
        "hide_group",
        "rule_desc",
        "sys_time",
    )


# Workflow approval list
@admin.register(WorkflowAudit)
class WorkflowAuditAdmin(admin.ModelAdmin):
    list_display = (
        "workflow_title",
        "group_name",
        "workflow_type",
        "current_status",
        "create_user_display",
        "create_time",
    )
    search_fields = ["workflow_title", "create_user_display"]
    list_filter = (
        "create_user_display",
        "group_name",
        "workflow_type",
        "current_status",
    )


# Workflow log table
@admin.register(WorkflowLog)
class WorkflowLogAdmin(admin.ModelAdmin):
    list_display = (
        "operation_type_desc",
        "operation_info",
        "operator_display",
        "operation_time",
    )
    list_filter = ("operation_type_desc", "operator_display")


# Instance database list
@admin.register(InstanceDatabase)
class InstanceDatabaseAdmin(admin.ModelAdmin):
    list_display = ("db_name", "owner_display", "instance", "remark")
    search_fields = ("db_name",)
    list_filter = ("instance", "owner_display")
    list_display_links = ("db_name",)

    # Only remark can be edited.
    def get_readonly_fields(self, request, obj=None):
        return ("instance", "owner", "owner_display") if obj else ()


# Instance account list
@admin.register(InstanceAccount)
class InstanceAccountAdmin(admin.ModelAdmin):
    list_display = ("user", "host", "password", "instance", "remark")
    search_fields = ("user", "host")
    list_filter = ("instance", "host")
    list_display_links = ("user",)

    # Only remark can be edited.
    def get_readonly_fields(self, request, obj=None):
        return (
            (
                "user",
                "host",
                "instance",
            )
            if obj
            else ()
        )


# Instance parameter template
@admin.register(ParamTemplate)
class ParamTemplateAdmin(admin.ModelAdmin):
    list_display = (
        "variable_name",
        "db_type",
        "default_value",
        "editable",
        "valid_values",
    )
    search_fields = ("variable_name",)
    list_filter = ("db_type", "editable")
    list_display_links = ("variable_name",)


# Instance parameter change history
@admin.register(ParamHistory)
class ParamHistoryAdmin(admin.ModelAdmin):
    list_display = (
        "variable_name",
        "instance",
        "old_var",
        "new_var",
        "user_display",
        "create_time",
    )
    search_fields = ("variable_name",)
    list_filter = ("instance", "user_display")


# Archive configuration
@admin.register(ArchiveConfig)
class ArchiveConfigAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "title",
        "src_instance",
        "src_db_name",
        "src_table_name",
        "dest_instance",
        "dest_db_name",
        "dest_table_name",
        "mode",
        "no_delete",
        "status",
        "state",
        "user_display",
        "create_time",
        "resource_group",
    )
    search_fields = ("title", "src_table_name")
    list_display_links = ("id", "title")
    list_filter = ("src_instance", "src_db_name", "mode", "no_delete", "state")
    # Fields shown on edit page
    fields = (
        "title",
        "resource_group",
        "src_instance",
        "src_db_name",
        "src_table_name",
        "dest_instance",
        "dest_db_name",
        "dest_table_name",
        "mode",
        "condition",
        "sleep",
        "no_delete",
        "state",
        "user_name",
        "user_display",
    )


# Cloud access key configuration
@admin.register(CloudAccessKey)
class CloudAccessKeyAdmin(admin.ModelAdmin):
    list_display = ("type", "key_id", "key_secret", "remark")


# Login audit log
@admin.register(AuditEntry)
class AuditEntryAdmin(admin.ModelAdmin):
    list_display = (
        "user_id",
        "user_name",
        "user_display",
        "action",
        "extra_info",
        "action_time",
    )
    list_filter = ("user_id", "user_name", "user_display", "action", "extra_info")
