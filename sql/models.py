# -*- coding: UTF-8 -*-
import importlib
import logging
from typing import Optional

from django.db import models
from django.contrib.auth.models import AbstractUser
from mirage import fields
from django.utils.translation import gettext as _
from django.conf import settings
from mirage.crypto import Crypto

from common.utils.const import WorkflowStatus, WorkflowType, WorkflowAction

logger = logging.getLogger("default")
file, _class = settings.PASSWORD_MIXIN_PATH.split(":")

try:
    password_module = importlib.import_module(file)
    PasswordMixin = getattr(password_module, _class)
except (ImportError, AttributeError) as e:
    logger.error(
        f"failed to import password minxin {settings.PASSWORD_MIXIN_PATH}, {str(e)}"
    )
    logger.error(f"falling back to dummy mixin")
    from sql.plugins.password import DummyMixin

    PasswordMixin = DummyMixin


class ResourceGroup(models.Model):
    """
    Resource group.
    """

    group_id = models.AutoField("Group ID", primary_key=True)
    group_name = models.CharField("Group Name", max_length=100, unique=True)
    group_parent_id = models.BigIntegerField("Parent ID", default=0)
    group_sort = models.IntegerField("Sort Order", default=1)
    group_level = models.IntegerField("Level", default=1)
    ding_webhook = models.CharField("DingTalk webhook URL", max_length=255, blank=True)
    feishu_webhook = models.CharField("Feishu webhook URL", max_length=255, blank=True)
    qywx_webhook = models.CharField("WeCom webhook URL", max_length=255, blank=True)
    is_deleted = models.IntegerField(
        "Is Deleted", choices=((0, "No"), (1, "Yes")), default=0
    )
    create_time = models.DateTimeField(auto_now_add=True)
    sys_time = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.group_name

    class Meta:
        managed = True
        db_table = "resource_group"
        verbose_name = "Resource Group Management"
        verbose_name_plural = "Resource Group Management"


class Users(AbstractUser):
    """
    Extended user profile.
    """

    display = models.CharField("Display Name", max_length=50, default="")
    ding_user_id = models.CharField("DingTalk User ID", max_length=64, blank=True)
    wx_user_id = models.CharField("WeCom User ID", max_length=64, blank=True)
    feishu_open_id = models.CharField("Feishu Open ID", max_length=64, blank=True)
    failed_login_count = models.IntegerField("Failed Login Count", default=0)
    last_login_failed_at = models.DateTimeField(
        "Last Failed Login Time", blank=True, null=True
    )
    resource_group = models.ManyToManyField(
        ResourceGroup, verbose_name="Resource Group", blank=True
    )

    def save(self, *args, **kwargs):
        self.failed_login_count = min(127, self.failed_login_count)
        self.failed_login_count = max(0, self.failed_login_count)
        super(Users, self).save(*args, **kwargs)

    def __str__(self):
        if self.display:
            return self.display
        return self.username

    class Meta:
        managed = True
        db_table = "sql_users"
        verbose_name = "User Management"
        verbose_name_plural = "User Management"


class TwoFactorAuthConfig(models.Model):
    """
    2FA configuration.
    """

    auth_type_choice = (
        ("totp", "Google Authenticator"),
        ("sms", "SMS Verification Code"),
    )

    username = fields.EncryptedCharField(verbose_name="Username", max_length=200)
    auth_type = fields.EncryptedCharField(
        verbose_name="Authentication Type", max_length=128, choices=auth_type_choice
    )
    phone = fields.EncryptedCharField(
        verbose_name="Phone Number", max_length=64, null=True, default=""
    )
    secret_key = fields.EncryptedCharField(
        verbose_name="User Secret", max_length=256, null=True
    )
    user = models.ForeignKey(Users, on_delete=models.CASCADE)

    class Meta:
        managed = True
        db_table = "2fa_config"
        verbose_name = "2FA Configuration"
        verbose_name_plural = "2FA Configuration"
        unique_together = ("user", "auth_type")


class InstanceTag(models.Model):
    """Instance tag configuration."""

    tag_code = models.CharField("Tag Code", max_length=20, unique=True)
    tag_name = models.CharField("Tag Name", max_length=20, unique=True)
    active = models.BooleanField("Active Status", default=True)
    create_time = models.DateTimeField("Created Time", auto_now_add=True)

    def __str__(self):
        return self.tag_name

    class Meta:
        managed = True
        db_table = "sql_instance_tag"
        verbose_name = "Instance Tag"
        verbose_name_plural = "Instance Tag"


DB_TYPE_CHOICES = (
    ("mysql", "MySQL"),
    ("mssql", "MsSQL"),
    ("redis", "Redis"),
    ("pgsql", "PgSQL"),
    ("oracle", "Oracle"),
    ("mongo", "Mongo"),
    ("phoenix", "Phoenix"),
    ("odps", "ODPS"),
    ("clickhouse", "ClickHouse"),
    ("goinception", "goInception"),
    ("cassandra", "Cassandra"),
    ("doris", "Doris"),
    ("elasticsearch", "Elasticsearch"),
    ("opensearch", "OpenSearch"),
    ("memcached", "Memcached"),
)


class Tunnel(models.Model):
    """
    SSH tunnel configuration.
    """

    tunnel_name = models.CharField("Tunnel Name", max_length=50, unique=True)
    host = models.CharField("Tunnel Host", max_length=200)
    port = models.IntegerField("Port", default=0)
    user = fields.EncryptedCharField(
        verbose_name="Username", max_length=200, default="", blank=True, null=True
    )
    password = fields.EncryptedCharField(
        verbose_name="Password", max_length=300, default="", blank=True, null=True
    )
    pkey = fields.EncryptedTextField(verbose_name="Private Key", blank=True, null=True)
    pkey_path = models.FileField(
        verbose_name="Key File Path", blank=True, null=True, upload_to="keys/"
    )
    pkey_password = fields.EncryptedCharField(
        verbose_name="Key Passphrase", max_length=300, default="", blank=True, null=True
    )
    create_time = models.DateTimeField("Created Time", auto_now_add=True)
    update_time = models.DateTimeField("Updated Time", auto_now=True)

    def __str__(self):
        return self.tunnel_name

    def short_pkey(self):
        if len(str(self.pkey)) > 20:
            return "{}...".format(str(self.pkey)[0:19])
        else:
            return str(self.pkey)

    class Meta:
        managed = True
        db_table = "ssh_tunnel"
        verbose_name = "Tunnel Configuration"
        verbose_name_plural = "Tunnel Configuration"


class Instance(models.Model, PasswordMixin):
    """
    Production instance configuration.
    """

    instance_name = models.CharField("Instance Name", max_length=50, unique=True)
    type = models.CharField(
        "Instance Type", max_length=6, choices=(("master", "Primary"), ("slave", "Replica"))
    )
    db_type = models.CharField("Database Type", max_length=20, choices=DB_TYPE_CHOICES)
    mode = models.CharField(
        "Run Mode",
        max_length=10,
        default="",
        blank=True,
        choices=(("standalone", "Standalone"), ("cluster", "Cluster")),
    )
    host = models.CharField("Instance Host", max_length=200)
    port = models.IntegerField("Port", default=0)
    user = fields.EncryptedCharField(
        verbose_name="Username", max_length=200, default="", blank=True
    )
    password = fields.EncryptedCharField(
        verbose_name="Password", max_length=300, default="", blank=True
    )
    is_ssl = models.BooleanField("Enable SSL", default=False)
    verify_ssl = models.BooleanField("Verify Server SSL Certificate", default=True)
    db_name = models.CharField("Database", max_length=64, default="", blank=True)
    show_db_name_regex = models.CharField(
        "Visible Database Regex",
        max_length=1024,
        default="",
        blank=True,
        help_text="Regex expression. Example: ^(test_db|dmp_db|za.*)$. Redis example: ^(0|4|6|11|12|13)$",
    )
    denied_db_name_regex = models.CharField(
        "Hidden Database Regex",
        max_length=1024,
        default="",
        blank=True,
        help_text="Regex expression. Hidden rules override visible rules.",
    )

    charset = models.CharField("Charset", max_length=20, default="", blank=True)
    service_name = models.CharField(
        "Oracle service name", max_length=50, null=True, blank=True
    )
    sid = models.CharField("Oracle sid", max_length=50, null=True, blank=True)
    resource_group = models.ManyToManyField(
        ResourceGroup, verbose_name="Resource Group", blank=True
    )
    instance_tag = models.ManyToManyField(
        InstanceTag, verbose_name="Instance Tag", blank=True
    )
    tunnel = models.ForeignKey(
        Tunnel,
        verbose_name="Connection Tunnel",
        blank=True,
        null=True,
        on_delete=models.CASCADE,
        default=None,
    )
    create_time = models.DateTimeField("Created Time", auto_now_add=True)
    update_time = models.DateTimeField("Updated Time", auto_now=True)

    def __str__(self):
        return self.instance_name

    class Meta:
        managed = True
        db_table = "sql_instance"
        verbose_name = "Instance Configuration"
        verbose_name_plural = "Instance Configuration"


SQL_WORKFLOW_CHOICES = (
    ("workflow_finish", _("workflow_finish")),
    ("workflow_abort", _("workflow_abort")),
    ("workflow_manreviewing", _("workflow_manreviewing")),
    ("workflow_review_pass", _("workflow_review_pass")),
    ("workflow_timingtask", _("workflow_timingtask")),
    ("workflow_queuing", _("workflow_queuing")),
    ("workflow_executing", _("workflow_executing")),
    ("workflow_autoreviewwrong", _("workflow_autoreviewwrong")),
    ("workflow_exception", _("workflow_exception")),
)


class WorkflowAuditMixin:
    @property
    def workflow_type(self):
        if isinstance(self, SqlWorkflow):
            return WorkflowType.SQL_REVIEW
        elif isinstance(self, ArchiveConfig):
            return WorkflowType.ARCHIVE
        elif isinstance(self, QueryPrivilegesApply):
            return WorkflowType.QUERY

    @property
    def workflow_pk_field(self):
        if isinstance(self, SqlWorkflow):
            return "id"
        elif isinstance(self, ArchiveConfig):
            return "id"
        elif isinstance(self, QueryPrivilegesApply):
            return "apply_id"

    def get_audit(self) -> Optional["WorkflowAudit"]:
        try:
            return WorkflowAudit.objects.get(
                workflow_type=self.workflow_type,
                workflow_id=getattr(self, self.workflow_pk_field),
            )
        except WorkflowAudit.DoesNotExist:
            return None


class SqlWorkflow(models.Model, WorkflowAuditMixin):
    """
    Stores base data for SQL deployment workflows.
    """

    workflow_name = models.CharField("Workflow Name", max_length=50)
    demand_url = models.CharField("Demand URL", max_length=500, blank=True)
    group_id = models.IntegerField("Group ID")
    group_name = models.CharField("Group Name", max_length=100)
    instance = models.ForeignKey(Instance, on_delete=models.CASCADE)
    db_name = models.CharField("Database", max_length=64)
    syntax_type = models.IntegerField(
        "Workflow Type (0=Unknown, 1=DDL, 2=DML, 3=Offline Export)",
        choices=((0, "Other"), (1, "DDL"), (2, "DML"), (3, "Offline Export")),
        default=0,
    )
    is_backup = models.BooleanField(
        "Backup Required",
        choices=(
            (False, "No"),
            (True, "Yes"),
        ),
        default=True,
    )
    engineer = models.CharField("Submitter", max_length=30)
    engineer_display = models.CharField("Submitter Display Name", max_length=50, default="")
    status = models.CharField(max_length=50, choices=SQL_WORKFLOW_CHOICES)
    audit_auth_groups = models.CharField("Audit Authorization Groups", max_length=255)
    run_date_start = models.DateTimeField("Execution Start Time", null=True, blank=True)
    run_date_end = models.DateTimeField("Execution End Time", null=True, blank=True)
    create_time = models.DateTimeField("Created Time", auto_now_add=True)
    finish_time = models.DateTimeField("Finished Time", null=True, blank=True)
    is_manual = models.IntegerField(
        "Manual Execution", choices=((0, "No"), (1, "Yes")), default=0
    )
    is_offline_export = models.IntegerField(
        "Offline Export Workflow",
        choices=(
            (0, "No"),
            (1, "Yes"),
        ),
        default=0,
    )

    # Export format
    export_format = models.CharField(
        "Export Format",
        max_length=10,
        choices=(
            ("csv", "CSV"),
            ("xlsx", "Excel"),
            ("sql", "SQL"),
            ("json", "JSON"),
            ("xml", "XML"),
        ),
        # default="csv",
        null=True,
        blank=True,
    )

    file_name = models.CharField(
        "File Name",
        max_length=255,  # Reasonable max length.
        null=True,  # Allow null.
        blank=True,  # Allow empty string.
    )

    def __str__(self):
        return self.workflow_name

    class Meta:
        managed = True
        db_table = "sql_workflow"
        verbose_name = "SQL Workflow"
        verbose_name_plural = "SQL Workflow"


class SqlWorkflowContent(models.Model):
    """
    Stores SQL, review, and execution content for SQL deployment workflows.
    Historical data can be archived or cleaned regularly, and table-level
    compression can also be used where applicable.
    """

    workflow = models.OneToOneField(SqlWorkflow, on_delete=models.CASCADE)
    sql_content = models.TextField("SQL Content")
    review_content = models.TextField("Auto Review Result (JSON)")
    execute_result = models.TextField("Execution Result (JSON)", blank=True)

    def __str__(self):
        return self.workflow.workflow_name

    class Meta:
        managed = True
        db_table = "sql_workflow_content"
        verbose_name = "SQL Workflow Content"
        verbose_name_plural = "SQL Workflow Content"


class WorkflowAudit(models.Model):
    """
    Workflow audit status table.
    """

    audit_id = models.AutoField(primary_key=True)
    group_id = models.IntegerField("Group ID")
    group_name = models.CharField("Group Name", max_length=100)
    workflow_id = models.BigIntegerField("Related Workflow ID")
    workflow_type = models.IntegerField("Request Type", choices=WorkflowType.choices)
    workflow_title = models.CharField("Request Title", max_length=50)
    workflow_remark = models.CharField(
        "Request Remark", default="", max_length=140, blank=True
    )
    audit_auth_groups = models.CharField("Audit Authorization Groups", max_length=255)
    current_audit = models.CharField("Current Audit Group", max_length=20)
    next_audit = models.CharField("Next Audit Group", max_length=20)
    current_status = models.IntegerField("Audit Status", choices=WorkflowStatus.choices)
    create_user = models.CharField("Requester", max_length=30)
    create_user_display = models.CharField("Requester Display Name", max_length=50, default="")
    create_time = models.DateTimeField("Request Time", auto_now_add=True)
    sys_time = models.DateTimeField("System Time", auto_now=True)

    def get_workflow(self):
        """Try to resolve workflow object from audit record."""
        if self.workflow_type == WorkflowType.QUERY:
            return QueryPrivilegesApply.objects.get(apply_id=self.workflow_id)
        elif self.workflow_type == WorkflowType.SQL_REVIEW:
            return SqlWorkflow.objects.get(id=self.workflow_id)
        elif self.workflow_type == WorkflowType.ARCHIVE:
            return ArchiveConfig.objects.get(id=self.workflow_id)
        raise ValueError("Unable to resolve related workflow")

    def __int__(self):
        return self.audit_id

    class Meta:
        managed = True
        db_table = "workflow_audit"
        unique_together = ("workflow_id", "workflow_type")
        verbose_name = "Workflow Audit List"
        verbose_name_plural = "Workflow Audit List"


class WorkflowAuditDetail(models.Model):
    """
    Audit detail table.
    TODO
    Some fields overlap with WorkflowLog and could be merged.
    """

    audit_detail_id = models.AutoField(primary_key=True)
    audit_id = models.IntegerField("Audit Record ID")
    audit_user = models.CharField("Auditor", max_length=30)
    audit_time = models.DateTimeField("Audit Time")
    audit_status = models.IntegerField("Audit Status", choices=WorkflowStatus.choices)
    remark = models.CharField("Audit Remark", default="", max_length=1000)
    sys_time = models.DateTimeField("System Time", auto_now=True)

    def __int__(self):
        return self.audit_detail_id

    class Meta:
        managed = True
        db_table = "workflow_audit_detail"
        verbose_name = "Workflow Audit Detail"
        verbose_name_plural = "Workflow Audit Detail"


class WorkflowAuditSetting(models.Model):
    """
    Audit setting table.
    """

    audit_setting_id = models.AutoField(primary_key=True)
    group_id = models.IntegerField("Group ID")
    group_name = models.CharField("Group Name", max_length=100)
    workflow_type = models.IntegerField("Audit Type", choices=WorkflowType.choices)
    audit_auth_groups = models.CharField("Audit Authorization Groups", max_length=255)
    create_time = models.DateTimeField(auto_now_add=True)
    sys_time = models.DateTimeField(auto_now=True)

    def __int__(self):
        return self.audit_setting_id

    class Meta:
        managed = True
        db_table = "workflow_audit_setting"
        unique_together = ("group_id", "workflow_type")
        verbose_name = "Audit Flow Configuration"
        verbose_name_plural = "Audit Flow Configuration"


class WorkflowLog(models.Model):
    """
    Workflow log table.
    """

    id = models.AutoField(primary_key=True)
    audit_id = models.IntegerField("Workflow Audit ID", db_index=True)
    operation_type = models.SmallIntegerField(
        "Operation Type", choices=WorkflowAction.choices
    )
    # operation_type_desc is kept for backward compatibility.
    operation_type_desc = models.CharField("Operation Type Description", max_length=10)
    operation_info = models.CharField("Operation Info", max_length=1000)
    operator = models.CharField("Operator", max_length=30)
    operator_display = models.CharField("Operator Display Name", max_length=50, default="")
    operation_time = models.DateTimeField(auto_now_add=True)

    def __int__(self):
        return self.audit_id

    class Meta:
        managed = True
        db_table = "workflow_log"
        verbose_name = "Workflow Log"
        verbose_name_plural = "Workflow Log"


class QueryPrivilegesApply(models.Model, WorkflowAuditMixin):
    """
    Query privilege application records.
    """

    apply_id = models.AutoField(primary_key=True)
    group_id = models.IntegerField("Group ID")
    group_name = models.CharField("Group Name", max_length=100)
    title = models.CharField("Request Title", max_length=50)
    # TODO: Convert user_name and user_display to a foreign key.
    user_name = models.CharField("Requester", max_length=30)
    user_display = models.CharField("Requester Display Name", max_length=50, default="")
    instance = models.ForeignKey(Instance, on_delete=models.CASCADE)
    db_list = models.TextField("Database", default="")  # Comma-separated database list.
    table_list = models.TextField("Table", default="")  # Comma-separated table list.
    valid_date = models.DateField("Valid Until")
    limit_num = models.IntegerField("Row Limit", default=100)
    priv_type = models.IntegerField(
        "Privilege Type",
        choices=(
            (1, "DATABASE"),
            (2, "TABLE"),
        ),
        default=0,
    )
    status = models.IntegerField("Audit Status", choices=WorkflowStatus.choices)
    audit_auth_groups = models.CharField("Audit Authorization Groups", max_length=255)
    create_time = models.DateTimeField(auto_now_add=True)
    sys_time = models.DateTimeField(auto_now=True)

    def __int__(self):
        return self.apply_id

    class Meta:
        managed = True
        db_table = "query_privileges_apply"
        verbose_name = "Query Privilege Application"
        verbose_name_plural = "Query Privilege Application"


class QueryPrivileges(models.Model):
    """
    User privilege relation table.
    """

    privilege_id = models.AutoField(primary_key=True)
    user_name = models.CharField("Username", max_length=30)
    user_display = models.CharField("Requester Display Name", max_length=50, default="")
    instance = models.ForeignKey(Instance, on_delete=models.CASCADE)
    db_name = models.CharField("Database", max_length=64, default="")
    table_name = models.CharField("Table", max_length=64, default="")
    valid_date = models.DateField("Valid Until")
    limit_num = models.IntegerField("Row Limit", default=100)
    priv_type = models.IntegerField(
        "Privilege Type",
        choices=(
            (1, "DATABASE"),
            (2, "TABLE"),
        ),
        default=0,
    )
    is_deleted = models.IntegerField("Is Deleted", default=0)
    create_time = models.DateTimeField(auto_now_add=True)
    sys_time = models.DateTimeField(auto_now=True)

    def __int__(self):
        return self.privilege_id

    class Meta:
        managed = True
        db_table = "query_privileges"
        index_together = ["user_name", "instance", "db_name", "valid_date"]
        verbose_name = "Query Privilege Record"
        verbose_name_plural = "Query Privilege Record"


class QueryLog(models.Model):
    """
    Logs for online SQL queries.
    """

    # TODO: Convert to instance foreign key.
    instance_name = models.CharField("Instance Name", max_length=50)
    db_name = models.CharField("Database Name", max_length=64)
    sqllog = models.TextField("Executed Query")
    effect_row = models.BigIntegerField("Returned Rows")
    cost_time = models.CharField("Execution Time", max_length=10, default="")
    # TODO: Convert to user foreign key.
    username = models.CharField("Operator", max_length=30)
    user_display = models.CharField("Operator Display Name", max_length=50, default="")
    priv_check = models.BooleanField(
        "Privilege Check Status",
        choices=(
            (False, "Skipped"),
            (True, "Normal"),
        ),
        default=False,
    )
    hit_rule = models.BooleanField(
        "Masking Rule Matched",
        choices=((False, "Not Matched/Unknown"), (True, "Matched")),
        default=False,
    )
    masking = models.BooleanField(
        "Masking Applied Correctly",
        choices=(
            (False, "No"),
            (True, "Yes"),
        ),
        default=False,
    )
    favorite = models.BooleanField(
        "Favorite",
        choices=(
            (False, "No"),
            (True, "Yes"),
        ),
        default=False,
    )
    alias = models.CharField("Statement Alias", max_length=64, default="", blank=True)
    create_time = models.DateTimeField("Operation Time", auto_now_add=True)
    sys_time = models.DateTimeField(auto_now=True)

    class Meta:
        managed = True
        db_table = "query_log"
        verbose_name = "Query Log"
        verbose_name_plural = "Query Log"


rule_type_choices = (
    (1, "Phone Number"),
    (2, "ID Number"),
    (3, "Bank Card"),
    (4, "Email"),
    (5, "Amount"),
    (6, "Other"),
    (100, "Three-Segment Generic Masking Rule"),
)


class DataMaskingColumns(models.Model):
    """
    Data masking column configuration.
    """

    column_id = models.AutoField("Column ID", primary_key=True)
    rule_type = models.IntegerField(
        "Rule Type",
        choices=rule_type_choices,
        help_text="Three-segment generic masking rule: split by length and mask middle segment.",
    )
    active = models.BooleanField(
        "Active Status", choices=((False, "Inactive"), (True, "Active"))
    )
    instance = models.ForeignKey(Instance, on_delete=models.CASCADE)
    table_schema = models.CharField("Schema Name", max_length=64)
    table_name = models.CharField("Table Name", max_length=64)
    column_name = models.CharField("Column Name", max_length=64)
    column_comment = models.CharField(
        "Column Description", max_length=1024, default="", blank=True
    )
    create_time = models.DateTimeField(auto_now_add=True)
    sys_time = models.DateTimeField(auto_now=True)

    class Meta:
        managed = True
        db_table = "data_masking_columns"
        verbose_name = "Data Masking Column Configuration"
        verbose_name_plural = "Data Masking Column Configuration"


class DataMaskingRules(models.Model):
    """
    Data masking rule configuration.
    """

    rule_type = models.IntegerField("Rule Type", choices=rule_type_choices, unique=True)
    rule_regex = models.CharField(
        "Masking regex. Expression must have groups; masked group is replaced by ****.",
        max_length=255,
    )
    hide_group = models.IntegerField("Group To Hide")
    rule_desc = models.CharField("Rule Description", max_length=100, default="", blank=True)
    sys_time = models.DateTimeField(auto_now=True)

    class Meta:
        managed = True
        db_table = "data_masking_rules"
        verbose_name = "Data Masking Rule Configuration"
        verbose_name_plural = "Data Masking Rule Configuration"


class InstanceAccount(models.Model):
    """
    Instance account list.
    """

    instance = models.ForeignKey(Instance, on_delete=models.CASCADE)
    user = fields.EncryptedCharField(verbose_name="Account", max_length=128)
    host = models.CharField(
        verbose_name="Host", max_length=64
    )  # MySQL stores host info here.
    db_name = models.CharField(
        verbose_name="Database Name", max_length=128
    )  # MongoDB stores database name here.
    password = fields.EncryptedCharField(
        verbose_name="Password", max_length=128, default="", blank=True
    )
    remark = models.CharField("Remark", max_length=255)
    sys_time = models.DateTimeField("System Modified Time", auto_now=True)

    class Meta:
        managed = True
        db_table = "instance_account"
        unique_together = ("instance", "user", "host", "db_name")
        verbose_name = "Instance Account List"
        verbose_name_plural = "Instance Account List"


class InstanceDatabase(models.Model):
    """
    Instance database list.
    """

    instance = models.ForeignKey(Instance, on_delete=models.CASCADE)
    db_name = models.CharField("Database Name", max_length=128)
    owner = models.CharField("Owner", max_length=50, default="", blank=True)
    owner_display = models.CharField(
        "Owner Display Name", max_length=50, default="", blank=True
    )
    remark = models.CharField("Remark", max_length=255, default="", blank=True)
    sys_time = models.DateTimeField("System Modified Time", auto_now=True)

    class Meta:
        managed = True
        db_table = "instance_database"
        unique_together = ("instance", "db_name")
        verbose_name = "Instance Database"
        verbose_name_plural = "Instance Database List"


class ParamTemplate(models.Model):
    """
    Instance parameter template configuration.
    """

    db_type = models.CharField("Database Type", max_length=20, choices=DB_TYPE_CHOICES)
    variable_name = models.CharField("Parameter Name", max_length=64)
    default_value = models.CharField("Default Value", max_length=1024)
    editable = models.BooleanField("Editable", default=False)
    valid_values = models.CharField(
        "Valid Values, e.g. range [1-65535] or enum [ON|OFF]", max_length=1024, blank=True
    )
    description = models.CharField("Parameter Description", max_length=1024, blank=True)
    create_time = models.DateTimeField("Created Time", auto_now_add=True)
    sys_time = models.DateTimeField("System Modified Time", auto_now=True)

    class Meta:
        managed = True
        db_table = "param_template"
        unique_together = ("db_type", "variable_name")
        verbose_name = "Instance Parameter Template Configuration"
        verbose_name_plural = "Instance Parameter Template Configuration"


class ParamHistory(models.Model):
    """
    History for dynamic parameters modified online.
    """

    instance = models.ForeignKey(Instance, on_delete=models.CASCADE)
    variable_name = models.CharField("Parameter Name", max_length=64)
    old_var = models.CharField("Old Value", max_length=1024)
    new_var = models.CharField("New Value", max_length=1024)
    set_sql = models.CharField("Executed SQL for Parameter Change", max_length=1024)
    user_name = models.CharField("Modified By", max_length=30)
    user_display = models.CharField("Modifier Display Name", max_length=50)
    create_time = models.DateTimeField("Parameter Modified Time", auto_now_add=True)

    class Meta:
        managed = True
        ordering = ["-create_time"]
        db_table = "param_history"
        verbose_name = "Instance Parameter Change History"
        verbose_name_plural = "Instance Parameter Change History"


class ArchiveConfig(models.Model, WorkflowAuditMixin):
    """
    Archive configuration table.
    """

    title = models.CharField("Archive Configuration Title", max_length=50)
    resource_group = models.ForeignKey(ResourceGroup, on_delete=models.CASCADE)
    audit_auth_groups = models.CharField("Audit Authorization Groups", max_length=255, blank=True)
    src_instance = models.ForeignKey(
        Instance, related_name="src_instance", on_delete=models.CASCADE
    )
    src_db_name = models.CharField("Source Database", max_length=64)
    src_table_name = models.CharField("Source Table", max_length=64)
    dest_instance = models.ForeignKey(
        Instance,
        related_name="dest_instance",
        on_delete=models.CASCADE,
        blank=True,
        null=True,
    )
    dest_db_name = models.CharField("Destination Database", max_length=64, blank=True, null=True)
    dest_table_name = models.CharField("Destination Table", max_length=64, blank=True, null=True)
    condition = models.CharField("Archive Condition (WHERE clause)", max_length=1000)
    mode = models.CharField(
        "Archive Mode",
        max_length=10,
        choices=(("file", "File"), ("dest", "Other Instance"), ("purge", "Direct Delete")),
    )
    no_delete = models.BooleanField("Retain Source Data")
    sleep = models.IntegerField("Sleep Seconds After Each Limited Batch", default=1)
    status = models.IntegerField(
        "Audit Status", choices=WorkflowStatus.choices, blank=True, default=1
    )
    state = models.BooleanField("Archive Enabled", default=True)
    user_name = models.CharField("Requester", max_length=30, blank=True, default="")
    user_display = models.CharField(
        "Requester Display Name", max_length=50, blank=True, default=""
    )
    create_time = models.DateTimeField("Created Time", auto_now_add=True)
    last_archive_time = models.DateTimeField("Last Archive Time", blank=True, null=True)
    sys_time = models.DateTimeField("System Modified Time", auto_now=True)

    class Meta:
        managed = True
        db_table = "archive_config"
        verbose_name = "Archive Configuration"
        verbose_name_plural = "Archive Configuration"


class ArchiveLog(models.Model):
    """
    Archive log table.
    """

    archive = models.ForeignKey(ArchiveConfig, on_delete=models.CASCADE)
    cmd = models.CharField("Archive Command", max_length=2000)
    condition = models.CharField("Archive Condition (WHERE clause)", max_length=1000)
    mode = models.CharField(
        "Archive Mode",
        max_length=10,
        choices=(("file", "File"), ("dest", "Other Instance"), ("purge", "Direct Delete")),
    )
    no_delete = models.BooleanField("Retain Source Data")
    sleep = models.IntegerField("Sleep Seconds After Each Limited Batch", default=0)
    select_cnt = models.IntegerField("Selected Rows")
    insert_cnt = models.IntegerField("Inserted Rows")
    delete_cnt = models.IntegerField("Deleted Rows")
    statistics = models.TextField("Archive Statistics Log")
    success = models.BooleanField("Archive Succeeded")
    error_info = models.TextField("Error Info")
    start_time = models.DateTimeField("Start Time")
    end_time = models.DateTimeField("End Time")
    sys_time = models.DateTimeField("System Modified Time", auto_now=True)

    class Meta:
        managed = True
        db_table = "archive_log"
        verbose_name = "Archive Log"
        verbose_name_plural = "Archive Log"


class Config(models.Model):
    """
    Configuration table.
    """

    item = models.CharField("Config Item", max_length=100, unique=True)
    value = fields.EncryptedCharField(verbose_name="Config Value", max_length=500)
    description = models.CharField("Description", max_length=200, default="", blank=True)

    class Meta:
        managed = True
        db_table = "sql_config"
        verbose_name = "System Configuration"
        verbose_name_plural = "System Configuration"


# Cloud service credential configuration
class CloudAccessKey(models.Model):
    cloud_type_choices = (("aliyun", "aliyun"),)

    type = models.CharField(max_length=20, default="", choices=cloud_type_choices)
    key_id = models.CharField(max_length=200)
    key_secret = models.CharField(max_length=200)
    remark = models.CharField(max_length=50, default="", blank=True)

    def __init__(self, *args, **kwargs):
        self.c = Crypto()
        super().__init__(*args, **kwargs)

    @property
    def raw_key_id(self):
        """Return key ID in plaintext."""
        return self.c.decrypt(self.key_id)

    @property
    def raw_key_secret(self):
        """Return key secret in plaintext."""
        return self.c.decrypt(self.key_secret)

    def save(self, *args, **kwargs):
        self.key_id = self.c.encrypt(self.key_id)
        self.key_secret = self.c.encrypt(self.key_secret)
        super(CloudAccessKey, self).save(*args, **kwargs)

    def __str__(self):
        return f"{self.type}({self.remark})"

    class Meta:
        managed = True
        db_table = "cloud_access_key"
        verbose_name = "Cloud Credential Configuration"
        verbose_name_plural = "Cloud Credential Configuration"


class AliyunRdsConfig(models.Model):
    """
    Alibaba Cloud RDS configuration.
    """

    instance = models.OneToOneField(Instance, on_delete=models.CASCADE)
    rds_dbinstanceid = models.CharField("Alibaba Cloud RDS Instance ID", max_length=100)
    ak = models.ForeignKey(
        CloudAccessKey, verbose_name="RDS Access Key Configuration", on_delete=models.CASCADE
    )
    is_enable = models.BooleanField("Enabled", default=False)

    def __int__(self):
        return self.rds_dbinstanceid

    class Meta:
        managed = True
        db_table = "aliyun_rds_config"
        verbose_name = "Alibaba Cloud RDS Configuration"
        verbose_name_plural = "Alibaba Cloud RDS Configuration"


class Permission(models.Model):
    """
    Custom business permissions.
    """

    class Meta:
        managed = True
        permissions = (
            ("menu_dashboard", "Menu Dashboard"),
            ("menu_sqlcheck", "Menu SQL Review"),
            ("menu_sqlworkflow", "Menu SQL Deployment"),
            ("menu_sqlanalyze", "Menu SQL Analysis"),
            ("menu_query", "Menu SQL Query"),
            ("menu_sqlquery", "Menu Online Query"),
            ("menu_queryapplylist", "Menu Privilege Management"),
            ("menu_sqloptimize", "Menu SQL Optimization"),
            ("menu_sqladvisor", "Menu Optimization Tools"),
            ("menu_slowquery", "Menu Slow Query Log"),
            ("menu_instance", "Menu Instance Management"),
            ("menu_instance_list", "Menu Instance List"),
            ("menu_dbdiagnostic", "Menu Session Management"),
            ("menu_database", "Menu Database Management"),
            ("menu_instance_account", "Menu Instance Account Management"),
            ("menu_param", "Menu Parameter Configuration"),
            ("menu_data_dictionary", "Menu Data Dictionary"),
            ("menu_tools", "Menu Tool Plugins"),
            ("menu_archive", "Menu Data Archive"),
            ("menu_my2sql", "Menu My2SQL"),
            ("menu_schemasync", "Menu SchemaSync"),
            ("menu_system", "Menu System Management"),
            ("menu_document", "Menu Related Documentation"),
            ("menu_openapi", "Menu OpenAPI"),
            ("sql_submit", "Submit SQL Deployment Workflow"),
            ("sql_review", "Review SQL Deployment Workflow"),
            ("sql_execute_for_resource_group", "Execute SQL Deployment Workflow (Resource Group Scope)"),
            ("sql_execute", "Execute SQL Deployment Workflow (Own Submissions Only)"),
            ("sql_analyze", "Execute SQL Analysis"),
            ("optimize_sqladvisor", "Execute SQLAdvisor"),
            ("optimize_sqltuning", "Execute SQLTuning"),
            ("optimize_soar", "Execute SOAR"),
            ("query_applypriv", "Apply Query Privileges"),
            ("query_mgtpriv", "Manage Query Privileges"),
            ("query_review", "Review Query Privileges"),
            ("query_submit", "Submit SQL Query"),
            ("query_all_instances", "Query All Instances"),
            ("query_resource_group_instance", "Query All Instances in Resource Group"),
            ("process_view", "View Sessions"),
            ("process_kill", "Kill Sessions"),
            ("tablespace_view", "View Tablespaces"),
            ("trx_view", "View Transaction Info"),
            ("trxandlocks_view", "View Lock Info"),
            ("instance_account_manage", "Manage Instance Accounts"),
            ("param_view", "View Instance Parameters"),
            ("param_edit", "Edit Instance Parameters"),
            ("data_dictionary_export", "Export Data Dictionary"),
            ("archive_apply", "Submit Archive Request"),
            ("archive_review", "Review Archive Request"),
            ("archive_mgt", "Manage Archive Request"),
            ("audit_user", "Audit Permission"),
            ("query_download", "Online Query Download Permission"),
            ("offline_download", "Offline Download Permission"),
            ("menu_sqlexportworkflow", "Menu Data Export"),
            ("sqlexport_submit", "Submit Data Export"),
        )


class SlowQuery(models.Model):
    """
    SlowQuery
    """

    checksum = models.CharField(max_length=32, primary_key=True)
    fingerprint = models.TextField()
    sample = models.TextField()
    first_seen = models.DateTimeField(blank=True, null=True)
    last_seen = models.DateTimeField(blank=True, null=True, db_index=True)
    reviewed_by = models.CharField(max_length=20, blank=True, null=True)
    reviewed_on = models.DateTimeField(blank=True, null=True)
    comments = models.TextField(blank=True, null=True)

    class Meta:
        managed = False
        db_table = "mysql_slow_query_review"
        verbose_name = "Slow Query Statistics"
        verbose_name_plural = "Slow Query Statistics"


class SlowQueryHistory(models.Model):
    """
    SlowQueryHistory
    """

    hostname_max = models.CharField(max_length=64, null=False)
    client_max = models.CharField(max_length=64, null=True)
    user_max = models.CharField(max_length=64, null=False)
    db_max = models.CharField(max_length=64, null=True, default=None)
    bytes_max = models.CharField(max_length=64, null=True)
    checksum = models.ForeignKey(
        SlowQuery,
        db_constraint=False,
        to_field="checksum",
        db_column="checksum",
        on_delete=models.CASCADE,
    )
    sample = models.TextField()
    ts_min = models.DateTimeField(db_index=True)
    ts_max = models.DateTimeField()
    ts_cnt = models.FloatField(blank=True, null=True)
    query_time_sum = models.FloatField(
        db_column="Query_time_sum", blank=True, null=True
    )
    query_time_min = models.FloatField(
        db_column="Query_time_min", blank=True, null=True
    )
    query_time_max = models.FloatField(
        db_column="Query_time_max", blank=True, null=True
    )
    query_time_pct_95 = models.FloatField(
        db_column="Query_time_pct_95", blank=True, null=True
    )
    query_time_stddev = models.FloatField(
        db_column="Query_time_stddev", blank=True, null=True
    )
    query_time_median = models.FloatField(
        db_column="Query_time_median", blank=True, null=True
    )
    lock_time_sum = models.FloatField(db_column="Lock_time_sum", blank=True, null=True)
    lock_time_min = models.FloatField(db_column="Lock_time_min", blank=True, null=True)
    lock_time_max = models.FloatField(db_column="Lock_time_max", blank=True, null=True)
    lock_time_pct_95 = models.FloatField(
        db_column="Lock_time_pct_95", blank=True, null=True
    )
    lock_time_stddev = models.FloatField(
        db_column="Lock_time_stddev", blank=True, null=True
    )
    lock_time_median = models.FloatField(
        db_column="Lock_time_median", blank=True, null=True
    )
    rows_sent_sum = models.FloatField(db_column="Rows_sent_sum", blank=True, null=True)
    rows_sent_min = models.FloatField(db_column="Rows_sent_min", blank=True, null=True)
    rows_sent_max = models.FloatField(db_column="Rows_sent_max", blank=True, null=True)
    rows_sent_pct_95 = models.FloatField(
        db_column="Rows_sent_pct_95", blank=True, null=True
    )
    rows_sent_stddev = models.FloatField(
        db_column="Rows_sent_stddev", blank=True, null=True
    )
    rows_sent_median = models.FloatField(
        db_column="Rows_sent_median", blank=True, null=True
    )
    rows_examined_sum = models.FloatField(
        db_column="Rows_examined_sum", blank=True, null=True
    )
    rows_examined_min = models.FloatField(
        db_column="Rows_examined_min", blank=True, null=True
    )
    rows_examined_max = models.FloatField(
        db_column="Rows_examined_max", blank=True, null=True
    )
    rows_examined_pct_95 = models.FloatField(
        db_column="Rows_examined_pct_95", blank=True, null=True
    )
    rows_examined_stddev = models.FloatField(
        db_column="Rows_examined_stddev", blank=True, null=True
    )
    rows_examined_median = models.FloatField(
        db_column="Rows_examined_median", blank=True, null=True
    )
    rows_affected_sum = models.FloatField(
        db_column="Rows_affected_sum", blank=True, null=True
    )
    rows_affected_min = models.FloatField(
        db_column="Rows_affected_min", blank=True, null=True
    )
    rows_affected_max = models.FloatField(
        db_column="Rows_affected_max", blank=True, null=True
    )
    rows_affected_pct_95 = models.FloatField(
        db_column="Rows_affected_pct_95", blank=True, null=True
    )
    rows_affected_stddev = models.FloatField(
        db_column="Rows_affected_stddev", blank=True, null=True
    )
    rows_affected_median = models.FloatField(
        db_column="Rows_affected_median", blank=True, null=True
    )
    rows_read_sum = models.FloatField(db_column="Rows_read_sum", blank=True, null=True)
    rows_read_min = models.FloatField(db_column="Rows_read_min", blank=True, null=True)
    rows_read_max = models.FloatField(db_column="Rows_read_max", blank=True, null=True)
    rows_read_pct_95 = models.FloatField(
        db_column="Rows_read_pct_95", blank=True, null=True
    )
    rows_read_stddev = models.FloatField(
        db_column="Rows_read_stddev", blank=True, null=True
    )
    rows_read_median = models.FloatField(
        db_column="Rows_read_median", blank=True, null=True
    )
    merge_passes_sum = models.FloatField(
        db_column="Merge_passes_sum", blank=True, null=True
    )
    merge_passes_min = models.FloatField(
        db_column="Merge_passes_min", blank=True, null=True
    )
    merge_passes_max = models.FloatField(
        db_column="Merge_passes_max", blank=True, null=True
    )
    merge_passes_pct_95 = models.FloatField(
        db_column="Merge_passes_pct_95", blank=True, null=True
    )
    merge_passes_stddev = models.FloatField(
        db_column="Merge_passes_stddev", blank=True, null=True
    )
    merge_passes_median = models.FloatField(
        db_column="Merge_passes_median", blank=True, null=True
    )
    innodb_io_r_ops_min = models.FloatField(
        db_column="InnoDB_IO_r_ops_min", blank=True, null=True
    )
    innodb_io_r_ops_max = models.FloatField(
        db_column="InnoDB_IO_r_ops_max", blank=True, null=True
    )
    innodb_io_r_ops_pct_95 = models.FloatField(
        db_column="InnoDB_IO_r_ops_pct_95", blank=True, null=True
    )
    innodb_io_r_ops_stddev = models.FloatField(
        db_column="InnoDB_IO_r_ops_stddev", blank=True, null=True
    )
    innodb_io_r_ops_median = models.FloatField(
        db_column="InnoDB_IO_r_ops_median", blank=True, null=True
    )
    innodb_io_r_bytes_min = models.FloatField(
        db_column="InnoDB_IO_r_bytes_min", blank=True, null=True
    )
    innodb_io_r_bytes_max = models.FloatField(
        db_column="InnoDB_IO_r_bytes_max", blank=True, null=True
    )
    innodb_io_r_bytes_pct_95 = models.FloatField(
        db_column="InnoDB_IO_r_bytes_pct_95", blank=True, null=True
    )
    innodb_io_r_bytes_stddev = models.FloatField(
        db_column="InnoDB_IO_r_bytes_stddev", blank=True, null=True
    )
    innodb_io_r_bytes_median = models.FloatField(
        db_column="InnoDB_IO_r_bytes_median", blank=True, null=True
    )
    innodb_io_r_wait_min = models.FloatField(
        db_column="InnoDB_IO_r_wait_min", blank=True, null=True
    )
    innodb_io_r_wait_max = models.FloatField(
        db_column="InnoDB_IO_r_wait_max", blank=True, null=True
    )
    innodb_io_r_wait_pct_95 = models.FloatField(
        db_column="InnoDB_IO_r_wait_pct_95", blank=True, null=True
    )
    innodb_io_r_wait_stddev = models.FloatField(
        db_column="InnoDB_IO_r_wait_stddev", blank=True, null=True
    )
    innodb_io_r_wait_median = models.FloatField(
        db_column="InnoDB_IO_r_wait_median", blank=True, null=True
    )
    innodb_rec_lock_wait_min = models.FloatField(
        db_column="InnoDB_rec_lock_wait_min", blank=True, null=True
    )
    innodb_rec_lock_wait_max = models.FloatField(
        db_column="InnoDB_rec_lock_wait_max", blank=True, null=True
    )
    innodb_rec_lock_wait_pct_95 = models.FloatField(
        db_column="InnoDB_rec_lock_wait_pct_95", blank=True, null=True
    )
    innodb_rec_lock_wait_stddev = models.FloatField(
        db_column="InnoDB_rec_lock_wait_stddev", blank=True, null=True
    )
    innodb_rec_lock_wait_median = models.FloatField(
        db_column="InnoDB_rec_lock_wait_median", blank=True, null=True
    )
    innodb_queue_wait_min = models.FloatField(
        db_column="InnoDB_queue_wait_min", blank=True, null=True
    )
    innodb_queue_wait_max = models.FloatField(
        db_column="InnoDB_queue_wait_max", blank=True, null=True
    )
    innodb_queue_wait_pct_95 = models.FloatField(
        db_column="InnoDB_queue_wait_pct_95", blank=True, null=True
    )
    innodb_queue_wait_stddev = models.FloatField(
        db_column="InnoDB_queue_wait_stddev", blank=True, null=True
    )
    innodb_queue_wait_median = models.FloatField(
        db_column="InnoDB_queue_wait_median", blank=True, null=True
    )
    innodb_pages_distinct_min = models.FloatField(
        db_column="InnoDB_pages_distinct_min", blank=True, null=True
    )
    innodb_pages_distinct_max = models.FloatField(
        db_column="InnoDB_pages_distinct_max", blank=True, null=True
    )
    innodb_pages_distinct_pct_95 = models.FloatField(
        db_column="InnoDB_pages_distinct_pct_95", blank=True, null=True
    )
    innodb_pages_distinct_stddev = models.FloatField(
        db_column="InnoDB_pages_distinct_stddev", blank=True, null=True
    )
    innodb_pages_distinct_median = models.FloatField(
        db_column="InnoDB_pages_distinct_median", blank=True, null=True
    )
    qc_hit_cnt = models.FloatField(db_column="QC_Hit_cnt", blank=True, null=True)
    qc_hit_sum = models.FloatField(db_column="QC_Hit_sum", blank=True, null=True)
    full_scan_cnt = models.FloatField(db_column="Full_scan_cnt", blank=True, null=True)
    full_scan_sum = models.FloatField(db_column="Full_scan_sum", blank=True, null=True)
    full_join_cnt = models.FloatField(db_column="Full_join_cnt", blank=True, null=True)
    full_join_sum = models.FloatField(db_column="Full_join_sum", blank=True, null=True)
    tmp_table_cnt = models.FloatField(db_column="Tmp_table_cnt", blank=True, null=True)
    tmp_table_sum = models.FloatField(db_column="Tmp_table_sum", blank=True, null=True)
    tmp_table_on_disk_cnt = models.FloatField(
        db_column="Tmp_table_on_disk_cnt", blank=True, null=True
    )
    tmp_table_on_disk_sum = models.FloatField(
        db_column="Tmp_table_on_disk_sum", blank=True, null=True
    )
    filesort_cnt = models.FloatField(db_column="Filesort_cnt", blank=True, null=True)
    filesort_sum = models.FloatField(db_column="Filesort_sum", blank=True, null=True)
    filesort_on_disk_cnt = models.FloatField(
        db_column="Filesort_on_disk_cnt", blank=True, null=True
    )
    filesort_on_disk_sum = models.FloatField(
        db_column="Filesort_on_disk_sum", blank=True, null=True
    )

    class Meta:
        managed = False
        db_table = "mysql_slow_query_review_history"
        unique_together = ("checksum", "ts_min", "ts_max")
        index_together = ("hostname_max", "ts_min")
        verbose_name = "Slow Query Detail"
        verbose_name_plural = "Slow Query Detail"


class AuditEntry(models.Model):
    """
    Login audit log.
    """

    user_id = models.IntegerField("User ID")
    user_name = models.CharField("Username", max_length=30, null=True)
    user_display = models.CharField("User Display Name", max_length=50, null=True)
    action = models.CharField("Action", max_length=255)
    extra_info = models.TextField("Additional Info", null=True)
    action_time = models.DateTimeField("Action Time", auto_now_add=True)

    class Meta:
        managed = True
        db_table = "audit_log"
        verbose_name = "Audit Log"
        verbose_name_plural = "Audit Log"

    def __unicode__(self):
        return "{0} - {1} - {2} - {3} - {4}".format(
            self.user_id, self.user_name, self.extra_info, self.action, self.action_time
        )

    def __str__(self):
        return "{0} - {1} - {2} - {3} - {4}".format(
            self.user_id, self.user_name, self.extra_info, self.action, self.action_time
        )
