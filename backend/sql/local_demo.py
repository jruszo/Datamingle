from collections import OrderedDict

from django.contrib.auth.models import Group, Permission
from django.db import transaction
from django.utils import timezone

from api_agents.models import Agent, AgentStatus, AgentToolArtifact
from common.auth import ensure_superadmin_group
from common.utils.const import WorkflowType
from sql.models import (
    InfrastructureNode,
    Instance,
    InstanceTag,
    ResourceGroup,
    Users,
    WorkflowAuditSetting,
)
from sql.utils.resource_group import normalize_access_role_sequence

DEMO_DB_PASSWORD = "demo123"

AUTH_GROUP_PERMISSION_CODES = OrderedDict(
    {
        "Default": [
            "menu_sqlworkflow",
            "menu_query",
            "menu_sqlquery",
            "menu_queryapplylist",
        ],
        "RD": [
            "menu_dashboard",
            "menu_sqlcheck",
            "menu_sqlworkflow",
            "menu_sqlexportworkflow",
            "menu_query",
            "menu_sqlquery",
            "menu_queryapplylist",
            "menu_data_dictionary",
            "menu_tools",
            "menu_archive",
            "sql_submit",
            "sqlexport_submit",
            "sql_execute",
            "query_applypriv",
            "query_submit",
            "archive_apply",
        ],
        "DBA": [
            "menu_dashboard",
            "menu_sqlcheck",
            "menu_sqlworkflow",
            "menu_sqlexportworkflow",
            "menu_query",
            "menu_sqlquery",
            "menu_queryapplylist",
            "menu_instance",
            "menu_instance_list",
            "menu_dbdiagnostic",
            "menu_database",
            "menu_instance_account",
            "menu_param",
            "menu_data_dictionary",
            "menu_tools",
            "menu_archive",
            "menu_system",
            "menu_openapi",
            "sql_submit",
            "sql_review",
            "sql_execute_for_resource_group",
            "sql_execute",
            "query_applypriv",
            "query_mgtpriv",
            "query_review",
            "query_submit",
            "query_all_instances",
            "query_resource_group_instance",
            "process_view",
            "process_kill",
            "tablespace_view",
            "trx_view",
            "trxandlocks_view",
            "instance_account_manage",
            "param_view",
            "param_edit",
            "data_dictionary_export",
            "offline_download",
            "archive_apply",
            "archive_review",
            "archive_mgt",
        ],
        "PM": [
            "menu_dashboard",
            "menu_sqlcheck",
            "menu_sqlworkflow",
            "menu_query",
            "menu_sqlquery",
            "menu_queryapplylist",
            "menu_data_dictionary",
            "menu_tools",
            "menu_archive",
            "sql_submit",
            "sql_review",
            "sql_execute_for_resource_group",
            "sql_execute",
            "query_applypriv",
            "query_review",
            "query_submit",
            "archive_apply",
            "archive_review",
        ],
        "QA": [
            "menu_dashboard",
            "menu_sqlcheck",
            "menu_sqlworkflow",
            "menu_query",
            "menu_sqlquery",
            "menu_queryapplylist",
            "menu_data_dictionary",
            "sql_submit",
            "sql_execute",
            "query_applypriv",
            "query_submit",
        ],
    }
)

DEMO_RESOURCE_GROUPS = OrderedDict(
    {
        "single_stage": {
            "group_name": "Demo Workflow Single Stage",
            "approval_groups": ["DBA"],
        },
        "multi_stage": {
            "group_name": "Demo Workflow Multi Stage",
            "approval_groups": ["PM", "DBA"],
        },
    }
)

LEGACY_DEMO_USERNAMES = (
    "demo_admin",
    "demo_requester",
    "demo_pm",
    "demo_dba",
)

DEMO_INSTANCES = OrderedDict(
    {
        "mysql": {
            "instance_name": "demo-mysql-workflow",
            "type": "master",
            "db_type": "mysql",
            "host": "mysql_demo",
            "port": 3306,
            "user": "demo_datamingle",
            "password": DEMO_DB_PASSWORD,
            "db_name": "",
            "charset": "utf8mb4",
            "show_db_name_regex": "^(demo_orders|demo_billing)$",
            "denied_db_name_regex": "",
            "resource_groups": ["single_stage", "multi_stage"],
            "tags": ["can_read", "can_write"],
            "databases": ["demo_orders", "demo_billing"],
        },
        "pgsql": {
            "instance_name": "demo-pgsql-workflow",
            "type": "master",
            "db_type": "pgsql",
            "host": "postgres_demo",
            "port": 5432,
            "user": "demo_datamingle",
            "password": DEMO_DB_PASSWORD,
            "db_name": "workflow_pg",
            "charset": "UTF8",
            "show_db_name_regex": "^(workflow_pg|analytics_pg)$",
            "denied_db_name_regex": "",
            "resource_groups": ["single_stage", "multi_stage"],
            "tags": ["can_read", "can_write"],
            "databases": ["workflow_pg", "analytics_pg"],
        },
    }
)

DEMO_INFRASTRUCTURE_NODES = OrderedDict(
    {
        "mysql_node": {
            "name": "demo-mysql-node",
            "address": "mysql_demo",
            "description": "Local demo MySQL database host.",
            "metadata": {"environment": "demo", "provider": "docker-compose"},
            "resource_groups": ["single_stage", "multi_stage"],
            "services": ["mysql"],
            "agent": {
                "name": "demo-mysql-node-agent",
                "display_name": "Demo MySQL Node Agent",
                "status": AgentStatus.OFFLINE,
                "hostname": "mysql_demo",
                "platform": "linux",
                "architecture": "amd64",
                "agent_version": "demo",
            },
        },
        "postgres_node": {
            "name": "demo-postgres-node",
            "address": "postgres_demo",
            "description": "Local demo PostgreSQL database host.",
            "metadata": {"environment": "demo", "provider": "docker-compose"},
            "resource_groups": ["single_stage", "multi_stage"],
            "services": ["pgsql"],
            "agent": None,
        },
    }
)


def managed_demo_usernames():
    return list(LEGACY_DEMO_USERNAMES)


def managed_demo_instance_names():
    return [item["instance_name"] for item in DEMO_INSTANCES.values()]


def managed_demo_node_names():
    return [item["name"] for item in DEMO_INFRASTRUCTURE_NODES.values()]


def managed_demo_resource_group_names():
    return [item["group_name"] for item in DEMO_RESOURCE_GROUPS.values()]


def seed_local_demo(write_line=None):
    def log(message):
        if write_line:
            write_line(message)

    with transaction.atomic():
        auth_groups = _seed_auth_groups(log)
        resource_groups = _seed_resource_groups(log)
        tags = _seed_instance_tags(log)
        _remove_legacy_seeded_users(log)
        instances = _seed_instances(resource_groups, tags, log)
        nodes = _seed_infrastructure_nodes(resource_groups, instances, log)
        _seed_agent_tool_artifacts(log)
        _seed_workflow_settings(auth_groups, resource_groups, log)

    return {
        "auth_groups": list(auth_groups.keys()),
        "resource_groups": [group.group_name for group in resource_groups.values()],
        "users": [],
        "removed_users": managed_demo_usernames(),
        "instances": [instance.instance_name for instance in instances.values()],
        "nodes": [node.name for node in nodes.values()],
    }


def _seed_auth_groups(log):
    auth_groups = {}
    auth_groups["superadmin"] = ensure_superadmin_group()
    log("Auth group updated: superadmin")
    for name, permission_codes in AUTH_GROUP_PERMISSION_CODES.items():
        group, created = Group.objects.get_or_create(name=name)
        permissions = list(Permission.objects.filter(codename__in=permission_codes))
        group.permissions.set(permissions)
        auth_groups[name] = group
        missing_permissions = sorted(
            set(permission_codes) - {permission.codename for permission in permissions}
        )
        state = "created" if created else "updated"
        log(f"Auth group {state}: {name}")
        if missing_permissions:
            log(
                "Missing permissions for {}: {}".format(
                    name, ", ".join(missing_permissions)
                )
            )
    return auth_groups


def _seed_resource_groups(log):
    resource_groups = {}
    for index, (key, config) in enumerate(DEMO_RESOURCE_GROUPS.items(), start=1):
        resource_group, created = ResourceGroup.objects.update_or_create(
            group_name=config["group_name"],
            defaults={
                "group_parent_id": 0,
                "group_sort": index,
                "group_level": 1,
                "is_deleted": 0,
                "feishu_webhook": "",
                "qywx_webhook": "",
            },
        )
        resource_groups[key] = resource_group
        log(
            "Resource group {}: {}".format(
                "created" if created else "updated", resource_group.group_name
            )
        )
    return resource_groups


def _seed_instance_tags(log):
    tags = {}
    for tag_code, tag_name in [
        ("can_write", "Supports release"),
        ("can_read", "Supports query"),
    ]:
        tag, created = InstanceTag.objects.update_or_create(
            tag_code=tag_code,
            defaults={"tag_name": tag_name, "active": True},
        )
        tags[tag_code] = tag
        log("Instance tag {}: {}".format("created" if created else "updated", tag_code))
    return tags


def _remove_legacy_seeded_users(log):
    deleted_count, _ = Users.objects.filter(
        username__in=managed_demo_usernames()
    ).delete()
    if deleted_count:
        log(f"Removed legacy seeded demo users: {deleted_count}")
    else:
        log("No legacy seeded demo users to remove")


def _seed_instances(resource_groups, tags, log):
    instances = {}
    for key, config in DEMO_INSTANCES.items():
        instance, created = Instance.objects.update_or_create(
            instance_name=config["instance_name"],
            defaults={
                "type": config["type"],
                "db_type": config["db_type"],
                "host": config["host"],
                "port": config["port"],
                "user": config["user"],
                "password": config["password"],
                "db_name": config["db_name"],
                "charset": config["charset"],
                "show_db_name_regex": config["show_db_name_regex"],
                "denied_db_name_regex": config["denied_db_name_regex"],
                "mode": "",
                "is_ssl": False,
                "verify_ssl": False,
                "service_name": None,
                "sid": None,
            },
        )
        instance.resource_group.set(
            [resource_groups[name] for name in config["resource_groups"]]
        )
        instance.instance_tag.set([tags[name] for name in config["tags"]])
        instances[key] = instance
        log(
            "Demo instance {}: {}".format(
                "created" if created else "updated", instance.instance_name
            )
        )
    return instances


def _seed_infrastructure_nodes(resource_groups, instances, log):
    nodes = {}
    for key, config in DEMO_INFRASTRUCTURE_NODES.items():
        node, created = InfrastructureNode.objects.update_or_create(
            name=config["name"],
            defaults={
                "address": config["address"],
                "description": config["description"],
                "metadata": config["metadata"],
                "enabled": True,
            },
        )
        node.resource_group.set(
            [resource_groups[name] for name in config["resource_groups"]]
        )
        for service_key in config["services"]:
            instance = instances[service_key]
            if instance.node_id != node.id:
                instance.node = node
                instance.save(update_fields=["node", "update_time"])

        agent_config = config["agent"]
        if agent_config:
            Agent.objects.update_or_create(
                name=agent_config["name"],
                defaults={
                    "display_name": agent_config["display_name"],
                    "status": agent_config["status"],
                    "hostname": agent_config["hostname"],
                    "platform": agent_config["platform"],
                    "architecture": agent_config["architecture"],
                    "agent_version": agent_config["agent_version"],
                    "last_seen_at": timezone.now(),
                    "local_node": node,
                    "enabled": True,
                    "metadata": {"seeded": True},
                },
            )

        nodes[key] = node
        log(
            "Infrastructure node {}: {} ({} services)".format(
                "created" if created else "updated",
                node.name,
                len(config["services"]),
            )
        )
    return nodes


def _seed_agent_tool_artifacts(log):
    artifacts = [
        {
            "tool_name": AgentToolArtifact.TOOL_NODE_EXPORTER,
            "version": "1.11.1",
            "download_url": "https://github.com/prometheus/node_exporter/releases/download/v1.11.1/node_exporter-1.11.1.linux-amd64.tar.gz",
            "sha256": "9f5ea48e5bc7b656f8a91a32e7d7deb89f70f73dabd0d974418aca15f37d6810",
            "notes": "Local demo host metrics exporter.",
        },
        {
            "tool_name": AgentToolArtifact.TOOL_MYSQLD_EXPORTER,
            "version": "0.19.0",
            "download_url": "https://github.com/prometheus/mysqld_exporter/releases/download/v0.19.0/mysqld_exporter-0.19.0.linux-amd64.tar.gz",
            "sha256": "97238be558bd1a6aa6b9a927fa21d91dc5cabe6b9e00678b5cafa2bbb3899e72",
            "notes": "Local demo MySQL service metrics exporter.",
        },
        {
            "tool_name": AgentToolArtifact.TOOL_POSTGRES_EXPORTER,
            "version": "0.19.1",
            "download_url": "https://github.com/prometheus-community/postgres_exporter/releases/download/v0.19.1/postgres_exporter-0.19.1.linux-amd64.tar.gz",
            "sha256": "229096c7988df6ca41fe5b4bf66865089971535e7f0d819c12c920ec64dd2bd0",
            "notes": "Local demo PostgreSQL service metrics exporter.",
        },
    ]
    for config in artifacts:
        artifact, created = AgentToolArtifact.objects.update_or_create(
            tool_name=config["tool_name"],
            version=config["version"],
            platform="linux",
            architecture="amd64",
            defaults={
                "download_url": config["download_url"],
                "sha256": config["sha256"],
                "size_bytes": 0,
                "enabled": True,
                "notes": config["notes"],
            },
        )
        log(
            "Agent tool artifact {}: {} {}".format(
                "created" if created else "updated",
                artifact.tool_name,
                artifact.version,
            )
        )


def _seed_workflow_settings(auth_groups, resource_groups, log):
    for key, config in DEMO_RESOURCE_GROUPS.items():
        resource_group = resource_groups[key]
        audit_auth_groups = ",".join(
            normalize_access_role_sequence(config["approval_groups"])
        )
        _, created = WorkflowAuditSetting.objects.update_or_create(
            group_id=resource_group.group_id,
            workflow_type=WorkflowType.SQL_REVIEW,
            defaults={
                "group_name": resource_group.group_name,
                "audit_auth_groups": audit_auth_groups,
            },
        )
        log(
            "Workflow setting {}: {} -> {}".format(
                "created" if created else "updated",
                resource_group.group_name,
                " -> ".join(config["approval_groups"]),
            )
        )
        _, archive_created = WorkflowAuditSetting.objects.update_or_create(
            group_id=resource_group.group_id,
            workflow_type=WorkflowType.ARCHIVE,
            defaults={
                "group_name": resource_group.group_name,
                "audit_auth_groups": audit_auth_groups,
            },
        )
        log(
            "Archive setting {}: {} -> {}".format(
                "created" if archive_created else "updated",
                resource_group.group_name,
                " -> ".join(config["approval_groups"]),
            )
        )
