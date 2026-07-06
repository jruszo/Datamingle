# -*- coding: UTF-8 -*-
import importlib
import logging
from typing import Optional

from django.db import models
from django.contrib.auth.models import AbstractUser, Group
from django.utils.translation import gettext as _
from django.conf import settings

from common.fields import EncryptedCharField, EncryptedTextField
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


class Team(models.Model):
    """A permission and resource boundary for users, nodes, and services."""

    team_id = models.AutoField("Group ID", primary_key=True)
    team_name = models.CharField("Group Name", max_length=100, unique=True)
    group_parent_id = models.BigIntegerField("Parent ID", default=0)
    group_sort = models.IntegerField("Sort Order", default=1)
    group_level = models.IntegerField("Level", default=1)
    feishu_webhook = models.CharField("Feishu webhook URL", max_length=255, blank=True)
    qywx_webhook = models.CharField("WeCom webhook URL", max_length=255, blank=True)
    is_deleted = models.IntegerField(
        "Is Deleted", choices=((0, "No"), (1, "Yes")), default=0
    )
    create_time = models.DateTimeField(auto_now_add=True)
    sys_time = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.team_name

    class Meta:
        managed = True
        db_table = "team"
        verbose_name = "Team Management"
        verbose_name_plural = "Team Management"


class TeamPermissionGroup:
    """Team-scoped capability constants used by authorization helpers."""

    QUERY = "sql.query_submit"
    WORKFLOW_REQUESTER = "sql.sql_submit"
    EXPORT_WORKFLOW_REQUESTER = "sql.sqlexport_submit"
    WORKFLOW_APPROVER = "sql.sql_review"
    RESOURCE_OWNER = "sql.change_team"


class Users(AbstractUser):
    """
    Extended user profile.
    """

    display = models.CharField("Display Name", max_length=50, default="")
    workos_user_id = models.CharField(
        "WorkOS User ID",
        max_length=64,
        blank=True,
        null=True,
        unique=True,
        db_index=True,
    )
    avatar_url = models.URLField("Avatar URL", max_length=500, blank=True, default="")
    wx_user_id = models.CharField("WeCom User ID", max_length=64, blank=True)
    feishu_open_id = models.CharField("Feishu Open ID", max_length=64, blank=True)
    failed_login_count = models.IntegerField("Failed Login Count", default=0)
    last_login_failed_at = models.DateTimeField(
        "Last Failed Login Time", blank=True, null=True
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


class TeamMembership(models.Model):
    """Permission-level-bearing membership of a user in a team."""

    user = models.ForeignKey(
        Users, related_name="team_memberships", on_delete=models.CASCADE
    )
    team = models.ForeignKey(Team, related_name="memberships", on_delete=models.CASCADE)
    permission_level = models.ForeignKey(
        "auth.Group",
        related_name="team_memberships",
        on_delete=models.PROTECT,
    )
    create_time = models.DateTimeField(auto_now_add=True)
    sys_time = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.user_id}:{self.team_id}:{self.permission_level_id}"

    class Meta:
        managed = True
        db_table = "team_membership"
        verbose_name = "Team Membership"
        verbose_name_plural = "Team Memberships"
        constraints = [
            models.UniqueConstraint(
                fields=("user", "team"),
                name="team_membership_user_team_uniq",
            )
        ]
        indexes = [
            models.Index(
                fields=("team", "permission_level"),
                name="team_membership_team_level_idx",
            ),
            models.Index(
                fields=("user", "permission_level"),
                name="team_membership_user_level_idx",
            ),
        ]


class WorkflowPolicy(models.Model):
    """Reusable SQL workflow approval policy."""

    name = models.CharField("Policy Name", max_length=100, unique=True)
    description = models.TextField("Description", default="", blank=True)
    is_active = models.BooleanField("Active", default=True)
    created_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        related_name="created_workflow_policies",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    updated_by = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        related_name="updated_workflow_policies",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    create_time = models.DateTimeField("Created Time", auto_now_add=True)
    update_time = models.DateTimeField("Updated Time", auto_now=True)

    def __str__(self):
        return self.name

    @property
    def audit_auth_groups(self):
        return ",".join(str(step.permission_group_id) for step in self.steps.all())

    class Meta:
        managed = True
        db_table = "workflow_policy"
        verbose_name = "Workflow Policy"
        verbose_name_plural = "Workflow Policies"
        ordering = ["name", "id"]


class WorkflowPolicyStep(models.Model):
    """Ordered team-role approval step for a workflow policy."""

    policy = models.ForeignKey(
        WorkflowPolicy,
        related_name="steps",
        on_delete=models.CASCADE,
    )
    order = models.PositiveIntegerField("Order")
    permission_group = models.ForeignKey(
        Group,
        related_name="workflow_policy_steps",
        on_delete=models.PROTECT,
    )
    create_time = models.DateTimeField("Created Time", auto_now_add=True)
    update_time = models.DateTimeField("Updated Time", auto_now=True)

    class Meta:
        managed = True
        db_table = "workflow_policy_step"
        verbose_name = "Workflow Policy Step"
        verbose_name_plural = "Workflow Policy Steps"
        ordering = ["order", "id"]
        constraints = [
            models.UniqueConstraint(
                fields=("policy", "order"),
                name="workflow_policy_step_policy_order_uniq",
            )
        ]


class TwoFactorAuthConfig(models.Model):
    """
    2FA configuration.
    """

    auth_type_choice = (
        ("totp", "Google Authenticator"),
        ("sms", "SMS Verification Code"),
    )

    auth_type = models.CharField(
        verbose_name="Authentication Type", max_length=128, choices=auth_type_choice
    )
    phone = EncryptedCharField(
        verbose_name="Phone Number", max_length=64, null=True, default=""
    )
    secret_key = EncryptedCharField(
        verbose_name="User Secret", max_length=256, null=True
    )
    user = models.ForeignKey(Users, on_delete=models.CASCADE)

    class Meta:
        managed = True
        db_table = "2fa_config"
        verbose_name = "2FA Configuration"
        verbose_name_plural = "2FA Configuration"
        unique_together = ("user", "auth_type")


DEFAULT_NODE_EXPORTER_COLLECTORS = (
    "arp",
    "bcache",
    "bcachefs",
    "bonding",
    "btrfs",
    "conntrack",
    "cpu",
    "cpufreq",
    "diskstats",
    "dmi",
    "edac",
    "entropy",
    "fibrechannel",
    "filefd",
    "filesystem",
    "hwmon",
    "infiniband",
    "ipvs",
    "kernel_hung",
    "loadavg",
    "mdadm",
    "meminfo",
    "netclass",
    "netdev",
    "netstat",
    "nfs",
    "nfsd",
    "nvme",
    "os",
    "powersupplyclass",
    "pressure",
    "rapl",
    "schedstat",
    "selinux",
    "sockstat",
    "softnet",
    "stat",
    "tapestats",
    "textfile",
    "thermal_zone",
    "time",
    "timex",
    "udp_queues",
    "uname",
    "vmstat",
    "watchdog",
    "xfs",
    "zfs",
)

NODE_EXPORTER_COLLECTOR_PROFILES = {
    "high": (
        "cpu",
        "diskstats",
        "filesystem",
        "loadavg",
        "meminfo",
        "netdev",
        "netstat",
        "pressure",
        "stat",
        "time",
        "vmstat",
    ),
    "normal": (
        "arp",
        "bcache",
        "bonding",
        "btrfs",
        "conntrack",
        "cpufreq",
        "entropy",
        "filefd",
        "hwmon",
        "mdadm",
        "netclass",
        "nvme",
        "os",
        "powersupplyclass",
        "rapl",
        "schedstat",
        "sockstat",
        "softnet",
        "textfile",
        "thermal_zone",
        "timex",
        "udp_queues",
        "uname",
    ),
    "low": (
        "bcachefs",
        "dmi",
        "edac",
        "fibrechannel",
        "infiniband",
        "ipvs",
        "kernel_hung",
        "nfs",
        "nfsd",
        "selinux",
        "tapestats",
        "watchdog",
        "xfs",
        "zfs",
    ),
}


def default_node_exporter_collectors():
    return list(DEFAULT_NODE_EXPORTER_COLLECTORS)


MYSQLD_EXPORTER_COLLECTORS = (
    "heartbeat.utc",
    "info_schema.processlist.processes_by_user",
    "info_schema.processlist.processes_by_host",
    "mysql.user.privileges",
    "perf_schema.indexiowaits",
    "perf_schema.tablelocks",
    "perf_schema.eventsstatements",
    "perf_schema.eventsstatementssum",
    "perf_schema.eventswaits",
    "heartbeat",
    "slave_hosts",
    "info_schema.replica_host",
    "info_schema.rocksdb_perf_context",
    "perf_schema.file_events",
    "perf_schema.file_instances",
    "perf_schema.memory_events",
    "perf_schema.replication_group_members",
    "perf_schema.replication_group_member_stats",
    "perf_schema.replication_applier_status_by_worker",
    "sys.user_summary",
    "info_schema.userstats",
    "info_schema.clientstats",
    "info_schema.tablestats",
    "info_schema.schemastats",
    "info_schema.innodb_cmp",
    "info_schema.innodb_cmpmem",
    "info_schema.query_response_time",
    "engine_tokudb_status",
    "engine_innodb_status",
    "global_status",
    "global_variables",
    "slave_status",
    "info_schema.processlist",
    "mysql.user",
    "info_schema.tables",
    "info_schema.innodb_tablespaces",
    "info_schema.innodb_metrics",
    "auto_increment.columns",
    "binlog_size",
    "perf_schema.tableiowaits",
)

DEFAULT_MYSQLD_EXPORTER_COLLECTORS = (
    "global_status",
    "global_variables",
    "slave_status",
)

MYSQLD_EXPORTER_COLLECTOR_PROFILES = {
    "high": (
        "global_status",
        "info_schema.innodb_metrics",
        "slave_status",
    ),
    "normal": (
        "engine_innodb_status",
        "info_schema.innodb_cmp",
        "info_schema.innodb_cmpmem",
        "info_schema.processlist",
        "info_schema.processlist.processes_by_host",
        "info_schema.processlist.processes_by_user",
        "info_schema.query_response_time",
        "perf_schema.eventswaits",
        "perf_schema.file_events",
        "perf_schema.tablelocks",
    ),
    "low": (
        "auto_increment.columns",
        "binlog_size",
        "engine_tokudb_status",
        "global_variables",
        "heartbeat",
        "heartbeat.utc",
        "info_schema.clientstats",
        "info_schema.replica_host",
        "info_schema.innodb_tablespaces",
        "info_schema.rocksdb_perf_context",
        "info_schema.schemastats",
        "info_schema.tables",
        "info_schema.tablestats",
        "info_schema.userstats",
        "mysql.user",
        "mysql.user.privileges",
        "perf_schema.eventsstatements",
        "perf_schema.eventsstatementssum",
        "perf_schema.file_instances",
        "perf_schema.indexiowaits",
        "perf_schema.memory_events",
        "perf_schema.replication_applier_status_by_worker",
        "perf_schema.replication_group_member_stats",
        "perf_schema.replication_group_members",
        "perf_schema.tableiowaits",
        "slave_hosts",
        "sys.user_summary",
    ),
}

POSTGRES_EXPORTER_COLLECTORS = (
    "buffercache_summary",
    "database",
    "database_wraparound",
    "locks",
    "long_running_transactions",
    "postmaster",
    "process_idle",
    "replication",
    "replication_slot",
    "roles",
    "stat_activity_autovacuum",
    "stat_bgwriter",
    "stat_checkpointer",
    "stat_database",
    "stat_progress_vacuum",
    "stat_statements",
    "stat_statements.include_query",
    "stat_user_tables",
    "stat_wal_receiver",
    "statio_user_indexes",
    "statio_user_tables",
    "wal",
    "xlog_location",
)

DEFAULT_POSTGRES_EXPORTER_COLLECTORS = (
    "database",
    "locks",
    "replication",
    "replication_slot",
    "roles",
    "stat_bgwriter",
    "stat_database",
    "stat_progress_vacuum",
    "stat_user_tables",
    "statio_user_tables",
    "wal",
)

POSTGRES_EXPORTER_COLLECTOR_PROFILES = {
    "high": (
        "database",
        "locks",
        "postmaster",
        "replication",
        "replication_slot",
        "stat_activity_autovacuum",
        "stat_bgwriter",
        "stat_checkpointer",
        "stat_database",
        "stat_wal_receiver",
        "wal",
        "xlog_location",
    ),
    "normal": (
        "database_wraparound",
        "long_running_transactions",
        "process_idle",
        "roles",
        "stat_progress_vacuum",
        "stat_user_tables",
        "statio_user_indexes",
        "statio_user_tables",
    ),
    "low": (
        "buffercache_summary",
        "stat_statements",
        "stat_statements.include_query",
    ),
}


def default_service_monitoring_collectors():
    return []


def service_exporter_collectors_for_engine(engine):
    if engine == "mysql":
        return list(MYSQLD_EXPORTER_COLLECTORS)
    if engine == "pgsql":
        return list(POSTGRES_EXPORTER_COLLECTORS)
    return []


def default_service_monitoring_collectors_for_engine(engine):
    if engine == "mysql":
        return list(DEFAULT_MYSQLD_EXPORTER_COLLECTORS)
    if engine == "pgsql":
        return list(DEFAULT_POSTGRES_EXPORTER_COLLECTORS)
    return []


def normalize_service_monitoring_collectors(engine, collectors):
    allowed = service_exporter_collectors_for_engine(engine)
    if collectors is None:
        return default_service_monitoring_collectors_for_engine(engine)
    allowed_set = set(allowed)
    normalized = []
    seen = set()
    for collector in collectors:
        collector = str(collector).strip()
        if collector in allowed_set and collector not in seen:
            normalized.append(collector)
            seen.add(collector)
    return normalized


class InfrastructureNode(models.Model):
    """Server or host that owns one or more database services."""

    name = models.CharField("Node Name", max_length=128, unique=True)
    address = models.CharField(
        "Node Address", max_length=200, blank=True, default="", db_index=True
    )
    description = models.TextField("Description", blank=True, default="")
    metadata = models.JSONField("Metadata", default=dict, blank=True)
    monitoring_enabled = models.BooleanField("Monitoring Enabled", default=True)
    monitoring_collectors = models.JSONField(
        "Monitoring Collectors", default=default_node_exporter_collectors, blank=True
    )
    monitoring_labels = models.JSONField("Monitoring Labels", default=dict, blank=True)
    enabled = models.BooleanField("Enabled", default=True)
    resource_group = models.ManyToManyField(Team, verbose_name="Team", blank=True)
    create_time = models.DateTimeField("Created Time", auto_now_add=True)
    update_time = models.DateTimeField("Updated Time", auto_now=True)

    def __str__(self):
        return self.name

    @property
    def node_name(self):
        return self.name

    @property
    def hostname(self):
        return self.address

    class Meta:
        managed = True
        db_table = "infrastructure_node"
        verbose_name = "Infrastructure Node"
        verbose_name_plural = "Infrastructure Nodes"
        ordering = ("name", "id")
        permissions = (("menu_infrastructure", "Can access Infrastructure menu"),)


class ServiceRecommendation(models.Model):
    STATUS_RECOMMENDED = "recommended"
    STATUS_ACCEPTED = "accepted"
    STATUS_IGNORED = "ignored"
    STATUS_CHOICES = (
        (STATUS_RECOMMENDED, "Recommended"),
        (STATUS_ACCEPTED, "Accepted"),
        (STATUS_IGNORED, "Ignored"),
    )
    ENGINE_CHOICES = (
        ("mysql", "MySQL"),
        ("pgsql", "PostgreSQL"),
    )

    node = models.ForeignKey(
        InfrastructureNode,
        related_name="service_recommendations",
        on_delete=models.CASCADE,
    )
    engine = models.CharField("Service Engine", max_length=20, choices=ENGINE_CHOICES)
    host = models.CharField("Host", max_length=200, blank=True, default="")
    port = models.IntegerField("Port", default=0)
    service_name = models.CharField(
        "Service Name", max_length=128, blank=True, default=""
    )
    source = models.CharField("Discovery Source", max_length=64, blank=True, default="")
    confidence = models.PositiveSmallIntegerField("Confidence", default=50)
    detected_version = models.CharField(
        "Detected Version", max_length=200, blank=True, default=""
    )
    fingerprint = models.CharField("Fingerprint", max_length=128)
    status = models.CharField(
        "Status",
        max_length=20,
        choices=STATUS_CHOICES,
        default=STATUS_RECOMMENDED,
        db_index=True,
    )
    metadata = models.JSONField("Metadata", default=dict, blank=True)
    last_seen_at = models.DateTimeField("Last Seen At", null=True, blank=True)
    create_time = models.DateTimeField("Created Time", auto_now_add=True)
    update_time = models.DateTimeField("Updated Time", auto_now=True)

    class Meta:
        managed = True
        db_table = "service_recommendation"
        verbose_name = "Service Recommendation"
        verbose_name_plural = "Service Recommendations"
        unique_together = ("node", "fingerprint")
        indexes = (
            models.Index(fields=("node", "status"), name="svc_rec_node_status_idx"),
            models.Index(
                fields=("engine", "host", "port"), name="svc_rec_endpoint_idx"
            ),
        )


class MysqlCluster(models.Model):
    STATUS_UNKNOWN = "unknown"
    STATUS_OK = "ok"
    STATUS_MISSING_MASTER = "missing_master"
    STATUS_AMBIGUOUS_MASTER = "ambiguous_master"
    STATUS_DRIFT = "drift"
    STATUS_CHOICES = (
        (STATUS_UNKNOWN, "Unknown"),
        (STATUS_OK, "OK"),
        (STATUS_MISSING_MASTER, "Missing Master"),
        (STATUS_AMBIGUOUS_MASTER, "Ambiguous Master"),
        (STATUS_DRIFT, "Topology Drift"),
    )

    SOURCE_AUTO = "auto"
    SOURCE_MANUAL = "manual"
    SOURCE_CHOICES = (
        (SOURCE_AUTO, "Automatic"),
        (SOURCE_MANUAL, "Manual"),
    )

    name = models.CharField("Cluster Name", max_length=100)
    label_value = models.CharField("Cluster Label Value", max_length=100, unique=True)
    cluster_key = models.CharField("Cluster Key", max_length=255, unique=True)
    topology_status = models.CharField(
        "Topology Status",
        max_length=32,
        choices=STATUS_CHOICES,
        default=STATUS_UNKNOWN,
    )
    primary_instance = models.ForeignKey(
        "Instance",
        verbose_name="Primary Instance",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="mysql_primary_clusters",
    )
    unmanaged_peers = models.JSONField("Unmanaged Peers", default=list, blank=True)
    membership_source = models.CharField(
        "Membership Source",
        max_length=16,
        choices=SOURCE_CHOICES,
        default=SOURCE_AUTO,
    )
    last_seen_at = models.DateTimeField("Last Seen At", null=True, blank=True)
    create_time = models.DateTimeField(auto_now_add=True)
    update_time = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    class Meta:
        managed = True
        db_table = "mysql_cluster"
        verbose_name = "MySQL Cluster"
        verbose_name_plural = "MySQL Clusters"
        indexes = (
            models.Index(fields=("topology_status",), name="mysql_cluster_status_idx"),
        )


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
    ("cassandra", "Cassandra"),
    ("doris", "Doris"),
    ("elasticsearch", "Elasticsearch"),
    ("opensearch", "OpenSearch"),
    ("memcached", "Memcached"),
)


class Instance(models.Model, PasswordMixin):
    """
    Production instance configuration.
    """

    MYSQL_ROLE_UNKNOWN = "unknown"
    MYSQL_ROLE_STANDALONE = "standalone"
    MYSQL_ROLE_PRIMARY = "primary"
    MYSQL_ROLE_REPLICA = "replica"
    MYSQL_ROLE_CHOICES = (
        (MYSQL_ROLE_UNKNOWN, "Unknown"),
        (MYSQL_ROLE_STANDALONE, "Standalone"),
        (MYSQL_ROLE_PRIMARY, "Primary"),
        (MYSQL_ROLE_REPLICA, "Replica"),
    )
    MYSQL_STATUS_UNKNOWN = "unknown"
    MYSQL_STATUS_STANDALONE = "standalone"
    MYSQL_STATUS_CLUSTERED = "clustered"
    MYSQL_STATUS_MISSING_MASTER = "missing_master"
    MYSQL_STATUS_AMBIGUOUS_MASTER = "ambiguous_master"
    MYSQL_STATUS_DRIFT = "drift"
    MYSQL_STATUS_CHOICES = (
        (MYSQL_STATUS_UNKNOWN, "Unknown"),
        (MYSQL_STATUS_STANDALONE, "Standalone"),
        (MYSQL_STATUS_CLUSTERED, "Clustered"),
        (MYSQL_STATUS_MISSING_MASTER, "Missing Master"),
        (MYSQL_STATUS_AMBIGUOUS_MASTER, "Ambiguous Master"),
        (MYSQL_STATUS_DRIFT, "Topology Drift"),
    )

    INVENTORY_STATUS_NEVER = "never"
    INVENTORY_STATUS_OK = "ok"
    INVENTORY_STATUS_STALE = "stale"
    INVENTORY_STATUS_FAILED = "failed"
    INVENTORY_STATUS_CHOICES = (
        (INVENTORY_STATUS_NEVER, "Never"),
        (INVENTORY_STATUS_OK, "OK"),
        (INVENTORY_STATUS_STALE, "Stale"),
        (INVENTORY_STATUS_FAILED, "Failed"),
    )

    instance_name = models.CharField("Instance Name", max_length=50, unique=True)
    type = models.CharField(
        "Instance Type",
        max_length=6,
        choices=(("master", "Primary"), ("slave", "Replica")),
    )
    db_type = models.CharField("Database Type", max_length=20, choices=DB_TYPE_CHOICES)
    mysql_cluster = models.ForeignKey(
        MysqlCluster,
        verbose_name="MySQL Cluster",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="instances",
    )
    mysql_cluster_membership_source = models.CharField(
        "MySQL Cluster Membership Source",
        max_length=16,
        choices=MysqlCluster.SOURCE_CHOICES,
        default=MysqlCluster.SOURCE_AUTO,
    )
    mysql_server_uuid = models.CharField(
        "MySQL Server UUID", max_length=64, default="", blank=True
    )
    mysql_topology_role = models.CharField(
        "MySQL Topology Role",
        max_length=16,
        choices=MYSQL_ROLE_CHOICES,
        default=MYSQL_ROLE_UNKNOWN,
    )
    mysql_topology_status = models.CharField(
        "MySQL Topology Status",
        max_length=32,
        choices=MYSQL_STATUS_CHOICES,
        default=MYSQL_STATUS_UNKNOWN,
    )
    mysql_read_only = models.BooleanField("MySQL Read Only", null=True, blank=True)
    mysql_super_read_only = models.BooleanField(
        "MySQL Super Read Only", null=True, blank=True
    )
    mysql_source_host = models.CharField(
        "MySQL Source Host", max_length=200, default="", blank=True
    )
    mysql_source_port = models.IntegerField("MySQL Source Port", null=True, blank=True)
    mysql_topology_last_seen_at = models.DateTimeField(
        "MySQL Topology Last Seen At", null=True, blank=True
    )
    mysql_topology_details = models.JSONField(
        "MySQL Topology Details", default=dict, blank=True
    )
    mysql_ddl_dml_eligible = models.BooleanField(
        "MySQL DDL/DML Eligible", default=False
    )
    mysql_ddl_dml_block_reason = models.CharField(
        "MySQL DDL/DML Block Reason", max_length=255, default="", blank=True
    )
    mode = models.CharField(
        "Run Mode",
        max_length=10,
        default="",
        blank=True,
        choices=(("standalone", "Standalone"), ("cluster", "Cluster")),
    )
    host = models.CharField("Instance Host", max_length=200)
    port = models.IntegerField("Port", default=0)
    user = EncryptedCharField(
        verbose_name="Username", max_length=200, default="", blank=True
    )
    password = EncryptedCharField(
        verbose_name="Password", max_length=300, default="", blank=True
    )
    monitoring_enabled = models.BooleanField("Monitoring Enabled", default=True)
    queryable = models.BooleanField("Queryable", default=False)
    workflow_enabled = models.BooleanField("Workflow Enabled", default=False)
    workflow_policy = models.ForeignKey(
        WorkflowPolicy,
        verbose_name="Workflow Policy",
        null=True,
        blank=True,
        on_delete=models.PROTECT,
    )
    monitoring_collectors = models.JSONField(
        "Monitoring Collectors",
        default=None,
        blank=True,
        null=True,
    )
    monitoring_labels = models.JSONField("Monitoring Labels", default=dict, blank=True)
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
    resource_group = models.ManyToManyField(Team, verbose_name="Team", blank=True)
    inventory_status = models.CharField(
        "Inventory Refresh Status",
        max_length=20,
        choices=INVENTORY_STATUS_CHOICES,
        default=INVENTORY_STATUS_NEVER,
    )
    inventory_last_attempt_at = models.DateTimeField(
        "Inventory Last Attempt At",
        null=True,
        blank=True,
        default=None,
    )
    inventory_last_success_at = models.DateTimeField(
        "Inventory Last Success At",
        null=True,
        blank=True,
        default=None,
    )
    inventory_detected_hostname = models.CharField(
        "Inventory Detected Hostname",
        max_length=200,
        default="",
        blank=True,
    )
    inventory_detected_version = models.CharField(
        "Inventory Detected Version",
        max_length=200,
        default="",
        blank=True,
    )
    node = models.ForeignKey(
        InfrastructureNode,
        verbose_name="Infrastructure Node",
        related_name="services",
        null=True,
        blank=True,
        default=None,
        on_delete=models.SET_NULL,
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


class MysqlTopologyAlert(models.Model):
    TYPE_DRIFT = "drift"
    TYPE_MISSING_MASTER = "missing_master"
    TYPE_AMBIGUOUS_MASTER = "ambiguous_master"
    TYPE_CHOICES = (
        (TYPE_DRIFT, "Topology Drift"),
        (TYPE_MISSING_MASTER, "Missing Master"),
        (TYPE_AMBIGUOUS_MASTER, "Ambiguous Master"),
    )
    STATUS_ACTIVE = "active"
    STATUS_RESOLVED = "resolved"
    STATUS_CHOICES = (
        (STATUS_ACTIVE, "Active"),
        (STATUS_RESOLVED, "Resolved"),
    )

    cluster = models.ForeignKey(
        MysqlCluster,
        verbose_name="MySQL Cluster",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="alerts",
    )
    instance = models.ForeignKey(
        Instance,
        verbose_name="Instance",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="mysql_topology_alerts",
    )
    alert_type = models.CharField("Alert Type", max_length=32, choices=TYPE_CHOICES)
    status = models.CharField(
        "Alert Status",
        max_length=16,
        choices=STATUS_CHOICES,
        default=STATUS_ACTIVE,
    )
    message = models.CharField("Message", max_length=255)
    metadata = models.JSONField("Metadata", default=dict, blank=True)
    create_time = models.DateTimeField(auto_now_add=True)
    update_time = models.DateTimeField(auto_now=True)

    class Meta:
        managed = True
        db_table = "mysql_topology_alert"
        verbose_name = "MySQL Topology Alert"
        verbose_name_plural = "MySQL Topology Alerts"
        indexes = (
            models.Index(
                fields=("status", "alert_type"), name="mysql_alert_status_idx"
            ),
            models.Index(fields=("cluster", "status"), name="mysql_alert_cluster_idx"),
            models.Index(
                fields=("instance", "status"), name="mysql_alert_instance_idx"
            ),
        )
        constraints = (
            models.UniqueConstraint(
                fields=("instance", "alert_type", "status"),
                name="mysql_alert_instance_type_status_uniq",
            ),
        )


class PermissionRequestTarget(models.TextChoices):
    TEAM = "team", "Team"
    INSTANCE = "instance", "Instance"


class PermissionRequestSubject(models.TextChoices):
    USER = "user", "User"
    TEAM = "team", "Team"


class PermissionRequestDuration(models.TextChoices):
    TEMPORARY = "temporary", "Temporary"
    PERMANENT = "permanent", "Permanent"


class MailboxCategory(models.TextChoices):
    APPROVAL_NEEDED = "approval_needed", "Approval needed"
    EXECUTION_NEEDED = "execution_needed", "Execution needed"
    EXECUTION_FINISHED = "execution_finished", "Execution finished"


class MailboxSourceType(models.TextChoices):
    SQL_WORKFLOW = "sql_workflow", "SQL Workflow"
    ARCHIVE = "archive", "Archive"
    PERMISSION_REQUEST = "permission_request", "Permission Request"


class InstanceAccessLevel(models.TextChoices):
    QUERY = "query", "Query"
    QUERY_DML = "query_dml", "Query + DML"
    QUERY_DML_DDL = "query_dml_ddl", "Query + DML + DDL"


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
        elif isinstance(self, PermissionRequest):
            return WorkflowType.ACCESS_REQUEST

    @property
    def workflow_pk_field(self):
        if isinstance(self, SqlWorkflow):
            return "id"
        elif isinstance(self, ArchiveConfig):
            return "id"
        elif isinstance(self, QueryPrivilegesApply):
            return "apply_id"
        elif isinstance(self, PermissionRequest):
            return "request_id"

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
    team_id = models.IntegerField("Group ID")
    team_name = models.CharField("Group Name", max_length=100)
    instance = models.ForeignKey(Instance, on_delete=models.CASCADE)
    db_name = models.CharField("Database", max_length=64)
    schema_name = models.CharField("Schema", max_length=128, blank=True, default="")
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
    engineer_display = models.CharField(
        "Submitter Display Name", max_length=50, default=""
    )
    status = models.CharField(max_length=50, choices=SQL_WORKFLOW_CHOICES)
    audit_auth_groups = models.CharField("Audit Authorization Groups", max_length=255)
    workflow_policy = models.ForeignKey(
        WorkflowPolicy,
        verbose_name="Workflow Policy",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
    )
    workflow_policy_name = models.CharField(
        "Workflow Policy Name", max_length=100, default="", blank=True
    )
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
            ("tsv", "TSV"),
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
    team_id = models.IntegerField("Group ID")
    team_name = models.CharField("Group Name", max_length=100)
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
    create_user_display = models.CharField(
        "Requester Display Name", max_length=50, default=""
    )
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
        elif self.workflow_type == WorkflowType.ACCESS_REQUEST:
            return PermissionRequest.objects.get(request_id=self.workflow_id)
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
    team_id = models.IntegerField("Group ID")
    team_name = models.CharField("Group Name", max_length=100)
    workflow_type = models.IntegerField("Audit Type", choices=WorkflowType.choices)
    audit_auth_groups = models.CharField("Audit Authorization Groups", max_length=255)
    create_time = models.DateTimeField(auto_now_add=True)
    sys_time = models.DateTimeField(auto_now=True)

    def __int__(self):
        return self.audit_setting_id

    class Meta:
        managed = True
        db_table = "workflow_audit_setting"
        unique_together = ("team_id", "workflow_type")
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
    operation_type_desc = models.CharField("Operation Type Description", max_length=64)
    operation_info = models.CharField("Operation Info", max_length=1000)
    operator = models.CharField("Operator", max_length=30)
    operator_display = models.CharField(
        "Operator Display Name", max_length=50, default=""
    )
    operation_time = models.DateTimeField(auto_now_add=True)

    def __int__(self):
        return self.audit_id

    class Meta:
        managed = True
        db_table = "workflow_log"
        verbose_name = "Workflow Log"
        verbose_name_plural = "Workflow Log"


class MailboxItem(models.Model):
    recipient = models.ForeignKey(
        Users,
        on_delete=models.CASCADE,
        related_name="mailbox_items",
    )
    category = models.CharField(
        "Mailbox Category",
        max_length=32,
        choices=MailboxCategory.choices,
    )
    source_type = models.CharField(
        "Mailbox Source Type",
        max_length=32,
        choices=MailboxSourceType.choices,
    )
    source_id = models.BigIntegerField("Mailbox Source ID")
    title = models.CharField("Mailbox Title", max_length=255)
    body = models.TextField("Mailbox Body", blank=True, default="")
    action_path = models.CharField("Mailbox Action Path", max_length=500, blank=True)
    is_unread = models.BooleanField("Unread", default=True)
    read_at = models.DateTimeField("Read At", blank=True, null=True)
    resolved_at = models.DateTimeField("Resolved At", blank=True, null=True)
    dedupe_key = models.CharField("Dedupe Key", max_length=255)
    metadata = models.JSONField("Metadata", default=dict, blank=True)
    create_time = models.DateTimeField("Created Time", auto_now_add=True)
    sys_time = models.DateTimeField("System Modified Time", auto_now=True)

    class Meta:
        managed = True
        db_table = "mailbox_item"
        verbose_name = "Mailbox Item"
        verbose_name_plural = "Mailbox Items"
        ordering = ["-create_time", "-id"]
        constraints = [
            models.UniqueConstraint(
                fields=["recipient", "dedupe_key"],
                name="mailbox_item_recipient_dedupe_uniq",
            )
        ]
        indexes = [
            models.Index(
                fields=["recipient", "is_unread", "create_time"],
                name="mailbox_item_unread_idx",
            ),
            models.Index(
                fields=["source_type", "source_id", "category"],
                name="mailbox_item_source_idx",
            ),
        ]


class QueryPrivilegesApply(models.Model, WorkflowAuditMixin):
    """
    Query privilege application records.
    """

    apply_id = models.AutoField(primary_key=True)
    team_id = models.IntegerField("Group ID")
    team_name = models.CharField("Group Name", max_length=100)
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
        indexes = [
            models.Index(
                fields=["user_name", "instance", "db_name", "valid_date"],
                name="query_privilege_lookup_idx",
            )
        ]
        verbose_name = "Query Privilege Record"
        verbose_name_plural = "Query Privilege Record"


class PermissionRequest(models.Model, WorkflowAuditMixin):
    request_id = models.AutoField(primary_key=True)
    team = models.ForeignKey(Team, on_delete=models.CASCADE)
    target_type = models.CharField(
        "Target Type",
        max_length=32,
        choices=PermissionRequestTarget.choices,
    )
    instance = models.ForeignKey(
        Instance,
        on_delete=models.CASCADE,
        blank=True,
        null=True,
        default=None,
    )
    access_level = models.CharField(
        "Instance Access Level",
        max_length=32,
        choices=InstanceAccessLevel.choices,
        blank=True,
        default="",
    )
    permission_level = models.ForeignKey(
        "auth.Group",
        related_name="permission_requests",
        on_delete=models.PROTECT,
        blank=True,
        null=True,
        default=None,
    )
    title = models.CharField("Request Title", max_length=50)
    reason = models.CharField("Request Reason", max_length=255, blank=True, default="")
    subject_type = models.CharField(
        "Request Subject Type",
        max_length=32,
        choices=PermissionRequestSubject.choices,
        default=PermissionRequestSubject.USER,
    )
    access_duration = models.CharField(
        "Access Duration",
        max_length=32,
        choices=PermissionRequestDuration.choices,
        default=PermissionRequestDuration.TEMPORARY,
    )
    user_name = models.CharField("Requester", max_length=30)
    user_display = models.CharField("Requester Display Name", max_length=50, default="")
    valid_date = models.DateField("Valid Until")
    status = models.IntegerField("Audit Status", choices=WorkflowStatus.choices)
    audit_auth_groups = models.CharField("Audit Authorization Groups", max_length=255)
    create_time = models.DateTimeField(auto_now_add=True)
    sys_time = models.DateTimeField(auto_now=True)

    @property
    def team_name(self):
        return self.team.team_name

    def __int__(self):
        return self.request_id

    class Meta:
        managed = True
        db_table = "permission_request"
        verbose_name = "Permission Request"
        verbose_name_plural = "Permission Requests"


class TemporaryTeamGrant(models.Model):
    grant_id = models.AutoField(primary_key=True)
    user = models.ForeignKey(Users, on_delete=models.CASCADE)
    team = models.ForeignKey(Team, on_delete=models.CASCADE)
    permission_level = models.ForeignKey(
        "auth.Group",
        related_name="temporary_team_grants",
        on_delete=models.PROTECT,
    )
    source_request = models.ForeignKey(
        PermissionRequest,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        default=None,
    )
    valid_date = models.DateField("Valid Until")
    is_revoked = models.BooleanField("Revoked", default=False)
    create_time = models.DateTimeField(auto_now_add=True)
    sys_time = models.DateTimeField(auto_now=True)

    class Meta:
        managed = True
        db_table = "temporary_team_grant"
        verbose_name = "Temporary Team Grant"
        verbose_name_plural = "Temporary Team Grants"


class TemporaryInstanceGrant(models.Model):
    grant_id = models.AutoField(primary_key=True)
    user = models.ForeignKey(Users, on_delete=models.CASCADE, null=True, blank=True)
    team = models.ForeignKey(Team, on_delete=models.CASCADE)
    instance = models.ForeignKey(Instance, on_delete=models.CASCADE)
    access_level = models.CharField(
        "Instance Access Level",
        max_length=32,
        choices=InstanceAccessLevel.choices,
    )
    source_request = models.ForeignKey(
        PermissionRequest,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        default=None,
    )
    valid_date = models.DateField("Valid Until")
    is_revoked = models.BooleanField("Revoked", default=False)
    create_time = models.DateTimeField(auto_now_add=True)
    sys_time = models.DateTimeField(auto_now=True)

    class Meta:
        managed = True
        db_table = "temporary_instance_grant"
        verbose_name = "Temporary Instance Grant"
        verbose_name_plural = "Temporary Instance Grants"


class PermanentTeamGrant(models.Model):
    grant_id = models.AutoField(primary_key=True)
    user = models.ForeignKey(Users, on_delete=models.CASCADE, null=True, blank=True)
    team = models.ForeignKey(Team, on_delete=models.CASCADE)
    permission_level = models.ForeignKey(
        "auth.Group",
        related_name="permanent_team_grants",
        on_delete=models.PROTECT,
        blank=True,
        null=True,
        default=None,
    )
    instance = models.ForeignKey(
        Instance,
        on_delete=models.CASCADE,
        blank=True,
        null=True,
        default=None,
    )
    source_request = models.ForeignKey(
        PermissionRequest,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        default=None,
    )
    is_revoked = models.BooleanField("Revoked", default=False)
    create_time = models.DateTimeField(auto_now_add=True)
    sys_time = models.DateTimeField(auto_now=True)

    class Meta:
        managed = True
        db_table = "permanent_team_grant"
        constraints = [
            models.CheckConstraint(
                condition=models.Q(user__isnull=False)
                | models.Q(instance__isnull=False),
                name="permanent_team_grant_has_user_or_instance",
            )
        ]
        verbose_name = "Permanent Team Grant"
        verbose_name_plural = "Permanent Team Grants"


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
    rule_desc = models.CharField(
        "Rule Description", max_length=255, default="", blank=True
    )
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
    user = models.CharField(verbose_name="Account", max_length=128)
    host = models.CharField(
        verbose_name="Host", max_length=64
    )  # MySQL stores host info here.
    db_name = models.CharField(
        verbose_name="Database Name", max_length=128
    )  # MongoDB stores database name here.
    password = EncryptedCharField(
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
        "Valid Values, e.g. range [1-65535] or enum [ON|OFF]",
        max_length=1024,
        blank=True,
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

    ARCHIVE_METHOD_CHOICES = (
        ("dml", "Rendered DML Delete"),
        ("pt_archiver", "pt-archiver"),
    )
    EXECUTION_MODE_CHOICES = (
        ("one_time", "One Time"),
        ("scheduled", "Scheduled"),
    )
    SCHEDULE_FREQUENCY_CHOICES = (
        ("daily", "Daily"),
        ("weekly", "Weekly"),
    )
    EXECUTION_STATE_CHOICES = (
        ("idle", "Idle"),
        ("queued", "Queued"),
        ("running", "Running"),
    )

    title = models.CharField("Archive Configuration Title", max_length=50)
    team = models.ForeignKey(Team, on_delete=models.CASCADE)
    audit_auth_groups = models.CharField(
        "Audit Authorization Groups", max_length=255, blank=True
    )
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
    dest_db_name = models.CharField(
        "Destination Database", max_length=64, blank=True, null=True
    )
    dest_table_name = models.CharField(
        "Destination Table", max_length=64, blank=True, null=True
    )
    condition = models.CharField("Archive Condition (WHERE clause)", max_length=1000)
    mode = models.CharField(
        "Archive Mode",
        max_length=10,
        choices=(
            ("file", "File"),
            ("dest", "Other Instance"),
            ("purge", "Direct Delete"),
        ),
    )
    no_delete = models.BooleanField("Retain Source Data")
    sleep = models.IntegerField("Sleep Seconds After Each Limited Batch", default=1)
    archive_method = models.CharField(
        "Archive Method",
        max_length=20,
        choices=ARCHIVE_METHOD_CHOICES,
        default="pt_archiver",
    )
    execution_mode = models.CharField(
        "Execution Mode",
        max_length=20,
        choices=EXECUTION_MODE_CHOICES,
        default="one_time",
    )
    schedule_frequency = models.CharField(
        "Schedule Frequency",
        max_length=20,
        choices=SCHEDULE_FREQUENCY_CHOICES,
        blank=True,
        null=True,
        default=None,
    )
    schedule_time = models.TimeField(
        "Schedule Time",
        blank=True,
        null=True,
        default=None,
    )
    schedule_weekdays = models.CharField(
        "Schedule Weekdays",
        max_length=32,
        blank=True,
        default="",
    )
    next_run_at = models.DateTimeField(
        "Next Run Time",
        blank=True,
        null=True,
        default=None,
    )
    status = models.IntegerField(
        "Audit Status", choices=WorkflowStatus.choices, blank=True, default=1
    )
    state = models.BooleanField("Archive Enabled", default=True)
    execution_state = models.CharField(
        "Execution State",
        max_length=20,
        choices=EXECUTION_STATE_CHOICES,
        default="idle",
    )
    consecutive_failures = models.IntegerField(
        "Consecutive Scheduled Failures",
        default=0,
    )
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
    archive_method = models.CharField(
        "Archive Method",
        max_length=20,
        choices=ArchiveConfig.ARCHIVE_METHOD_CHOICES,
        default="pt_archiver",
    )
    mode = models.CharField(
        "Archive Mode",
        max_length=10,
        choices=(
            ("file", "File"),
            ("dest", "Other Instance"),
            ("purge", "Direct Delete"),
        ),
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
    value = EncryptedCharField(verbose_name="Config Value", max_length=500)
    description = models.CharField(
        "Description", max_length=200, default="", blank=True
    )

    class Meta:
        managed = True
        db_table = "sql_config"
        verbose_name = "System Configuration"
        verbose_name_plural = "System Configuration"


class TaskSchedule(models.Model):
    """Backend-agnostic registry for one-off scheduled tasks."""

    BACKEND_CELERY = "celery"
    BACKEND_CHOICES = ((BACKEND_CELERY, "Celery"),)

    STATUS_SCHEDULED = "scheduled"
    STATUS_RUNNING = "running"
    STATUS_COMPLETED = "completed"
    STATUS_FAILED = "failed"
    STATUS_CANCELLED = "cancelled"
    STATUS_CHOICES = (
        (STATUS_SCHEDULED, "Scheduled"),
        (STATUS_RUNNING, "Running"),
        (STATUS_COMPLETED, "Completed"),
        (STATUS_FAILED, "Failed"),
        (STATUS_CANCELLED, "Cancelled"),
    )

    name = models.CharField("Schedule Name", max_length=200, unique=True)
    backend = models.CharField(
        "Task Backend",
        max_length=20,
        choices=BACKEND_CHOICES,
        default=BACKEND_CELERY,
    )
    task_name = models.CharField("Task Name", max_length=255, default="", blank=True)
    callable_path = models.CharField(
        "Callable Path", max_length=500, default="", blank=True
    )
    payload = models.TextField("Serialized Payload", default="", blank=True)
    run_at = models.DateTimeField("Scheduled Run Time")
    status = models.CharField(
        "Schedule Status",
        max_length=20,
        choices=STATUS_CHOICES,
        default=STATUS_SCHEDULED,
    )
    backend_job_id = models.CharField(
        "Backend Job ID", max_length=255, default="", blank=True
    )
    last_error = models.TextField("Last Error", default="", blank=True)
    completed_at = models.DateTimeField(
        "Completed At", null=True, blank=True, default=None
    )
    cancelled_at = models.DateTimeField(
        "Cancelled At", null=True, blank=True, default=None
    )
    create_time = models.DateTimeField("Created Time", auto_now_add=True)
    sys_time = models.DateTimeField("System Modified Time", auto_now=True)

    @property
    def next_run(self):
        if self.status == self.STATUS_SCHEDULED:
            return self.run_at
        return None

    class Meta:
        managed = True
        db_table = "task_schedule"
        verbose_name = "Scheduled Task"
        verbose_name_plural = "Scheduled Task"
        indexes = [models.Index(fields=["status", "run_at"], name="idx_status_run_at")]


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
            ("menu_query", "Menu SQL Query"),
            ("menu_sqlquery", "Menu Online Query"),
            ("menu_queryapplylist", "Menu Privilege Management"),
            ("menu_instance", "Menu Instance Management"),
            ("menu_instance_list", "Menu Instance List"),
            ("menu_dbdiagnostic", "Menu Session Management"),
            ("menu_database", "Menu Database Management"),
            ("menu_instance_account", "Menu Instance Account Management"),
            ("menu_param", "Menu Parameter Configuration"),
            ("menu_data_dictionary", "Menu Data Dictionary"),
            ("menu_tools", "Menu Tool Plugins"),
            ("menu_archive", "Menu Data Archive"),
            ("menu_system", "Menu System Management"),
            ("menu_openapi", "Menu OpenAPI"),
            ("sql_submit", "Submit SQL Deployment Workflow"),
            ("sql_review", "Review SQL Deployment Workflow"),
            (
                "sql_execute_for_team",
                "Execute SQL Deployment Workflow (Team Scope)",
            ),
            ("sql_execute", "Execute SQL Deployment Workflow (Own Submissions Only)"),
            ("query_applypriv", "Apply Query Privileges"),
            ("query_mgtpriv", "Manage Query Privileges"),
            ("query_review", "Review Query Privileges"),
            ("query_submit", "Submit SQL Query"),
            ("query_all_instances", "Query All Instances"),
            ("query_team_instance", "Query All Instances in Team"),
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
        indexes = [
            models.Index(
                fields=["hostname_max", "ts_min"],
                name="slow_query_hostname_ts_idx",
            )
        ]
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
