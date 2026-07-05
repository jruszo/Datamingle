from collections import OrderedDict

from django.contrib.auth.models import Group, Permission
from django.db import transaction
from django.utils import timezone

from api_agents.models import Agent, AgentStatus, AgentToolArtifact
from api_agents.services import agent_api_key_hash
from common.auth import ensure_superadmin_group
from common.team_permissions import TEAM_PERMISSION_CODES
from common.utils.const import WorkflowType
from sql.models import (
    InfrastructureNode,
    Instance,
    Team,
    Users,
    WorkflowAuditSetting,
    WorkflowPolicy,
    WorkflowPolicyStep,
)
from sql.utils.team import normalize_permission_group_sequence

DEMO_DB_PASSWORD = "demo123"
DEMO_AGENT_API_KEY = "dm_agent_local_demo_key"
LEGACY_DEMO_AGENT_NAMES = ("demo-mysql-node-agent",)

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
            "sql_execute_for_team",
            "sql_execute",
            "query_applypriv",
            "query_mgtpriv",
            "query_review",
            "query_submit",
            "query_all_instances",
            "query_team_instance",
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
            "view_team",
            "change_team",
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
            "sql_execute_for_team",
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

DEMO_TEAMS = OrderedDict(
    {
        "single_stage": {
            "team_name": "Demo Workflow Single Stage",
            "approval_groups": ["DBA"],
        },
        "multi_stage": {
            "team_name": "Demo Workflow Multi Stage",
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
            "teams": ["single_stage", "multi_stage"],
            "workflow_policy": "multi_stage",
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
            "teams": ["single_stage", "multi_stage"],
            "workflow_policy": "multi_stage",
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
            "metadata": {
                "environment": "demo",
                "provider": "docker-compose",
                "agent_service_endpoints": {
                    "demo-mysql-workflow": {
                        "host": "127.0.0.1",
                        "port": 3307,
                    },
                },
            },
            "teams": ["single_stage", "multi_stage"],
            "services": ["mysql"],
            "agent": {
                "name": "notebook-ubuntu",
                "display_name": "Demo MySQL Node Agent",
                "status": AgentStatus.ONLINE,
                "hostname": "mysql_demo",
                "platform": "linux",
                "architecture": "amd64",
                "agent_version": "demo",
                "api_key": DEMO_AGENT_API_KEY,
                "metadata": {
                    "seeded": True,
                    "active_websocket": {"channel_name": "e2e.demo.mysql.agent"},
                },
            },
        },
        "postgres_node": {
            "name": "demo-postgres-node",
            "address": "postgres_demo",
            "description": "Local demo PostgreSQL database host.",
            "metadata": {"environment": "demo", "provider": "docker-compose"},
            "teams": ["single_stage", "multi_stage"],
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


def managed_demo_team_names():
    return [item["team_name"] for item in DEMO_TEAMS.values()]


def seed_local_demo(write_line=None):
    def log(message):
        if write_line:
            write_line(message)

    with transaction.atomic():
        auth_groups = _seed_auth_groups(log)
        teams = _seed_teams(log)
        workflow_policies = _seed_workflow_policies(auth_groups, log)
        _remove_legacy_seeded_users(log)
        instances = _seed_instances(teams, workflow_policies, log)
        nodes = _seed_infrastructure_nodes(teams, instances, log)
        _seed_agent_tool_artifacts(log)
        _seed_workflow_settings(auth_groups, teams, log)

    return {
        "auth_groups": list(auth_groups.keys()),
        "teams": [group.team_name for group in teams.values()],
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
        allowed_codes = set(permission_codes) & TEAM_PERMISSION_CODES
        permissions = list(
            Permission.objects.filter(
                content_type__app_label="sql",
                codename__in=allowed_codes,
            )
        )
        group.permissions.set(permissions)
        auth_groups[name] = group
        missing_permissions = sorted(
            allowed_codes - {permission.codename for permission in permissions}
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


def _seed_teams(log):
    teams = {}
    for index, (key, config) in enumerate(DEMO_TEAMS.items(), start=1):
        team, created = Team.objects.update_or_create(
            team_name=config["team_name"],
            defaults={
                "group_parent_id": 0,
                "group_sort": index,
                "group_level": 1,
                "is_deleted": 0,
                "feishu_webhook": "",
                "qywx_webhook": "",
            },
        )
        teams[key] = team
        log("Team {}: {}".format("created" if created else "updated", team.team_name))
    return teams


def _seed_workflow_policies(auth_groups, log):
    policies = {}
    for key, config in DEMO_TEAMS.items():
        policy, created = WorkflowPolicy.objects.update_or_create(
            name=f"Demo Workflow Policy - {config['team_name']}",
            defaults={
                "description": (
                    "Local demo SQL workflow approval policy for "
                    f"{config['team_name']}."
                ),
                "is_active": True,
            },
        )
        WorkflowPolicyStep.objects.filter(policy=policy).delete()
        for index, group_name in enumerate(config["approval_groups"], start=1):
            WorkflowPolicyStep.objects.create(
                policy=policy,
                order=index,
                permission_group=auth_groups[group_name],
            )
        policies[key] = policy
        log(
            "Workflow policy {}: {} -> {}".format(
                "created" if created else "updated",
                policy.name,
                " -> ".join(config["approval_groups"]),
            )
        )
    return policies


def _remove_legacy_seeded_users(log):
    deleted_count, _ = Users.objects.filter(
        username__in=managed_demo_usernames()
    ).delete()
    if deleted_count:
        log(f"Removed legacy seeded demo users: {deleted_count}")
    else:
        log("No legacy seeded demo users to remove")


def _seed_instances(teams, workflow_policies, log):
    instances = {}
    for key, config in DEMO_INSTANCES.items():
        is_mysql = config["db_type"] == "mysql"
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
                "workflow_enabled": True,
                "workflow_policy": workflow_policies[config["workflow_policy"]],
                "mysql_topology_role": (
                    Instance.MYSQL_ROLE_STANDALONE
                    if is_mysql
                    else Instance.MYSQL_ROLE_UNKNOWN
                ),
                "mysql_topology_status": (
                    Instance.MYSQL_STATUS_STANDALONE
                    if is_mysql
                    else Instance.MYSQL_STATUS_UNKNOWN
                ),
                "mysql_ddl_dml_eligible": is_mysql,
                "mysql_ddl_dml_block_reason": "",
            },
        )
        instance.resource_group.set([teams[name] for name in config["teams"]])
        instances[key] = instance
        log(
            "Demo instance {}: {}".format(
                "created" if created else "updated", instance.instance_name
            )
        )
    return instances


def _seed_infrastructure_nodes(teams, instances, log):
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
        node.resource_group.set([teams[name] for name in config["teams"]])
        for service_key in config["services"]:
            instance = instances[service_key]
            if instance.node_id != node.id:
                instance.node = node
                instance.save(update_fields=["node", "update_time"])

        agent_config = config["agent"]
        if agent_config:
            _migrate_legacy_demo_agent(agent_config["name"], log)
            Agent.objects.update_or_create(
                name=agent_config["name"],
                defaults={
                    "display_name": agent_config["display_name"],
                    "status": agent_config["status"],
                    "hostname": agent_config["hostname"],
                    "platform": agent_config["platform"],
                    "architecture": agent_config["architecture"],
                    "agent_version": agent_config["agent_version"],
                    "api_key_hash": agent_api_key_hash(agent_config["api_key"]),
                    "api_key_prefix": agent_config["api_key"][:16],
                    "workos_api_key_id": "",
                    "last_seen_at": timezone.now(),
                    "local_node": node,
                    "enabled": True,
                    "metadata": agent_config["metadata"],
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


def _migrate_legacy_demo_agent(target_name, log):
    legacy_agents = Agent.objects.filter(name__in=LEGACY_DEMO_AGENT_NAMES)
    if not legacy_agents.exists():
        return

    target_agent = Agent.objects.filter(name=target_name).first()
    if target_agent is None:
        legacy_agent = legacy_agents.order_by("id").first()
        old_name = legacy_agent.name
        legacy_agent.name = target_name
        legacy_agent.save(update_fields=["name", "update_time"])
        legacy_agents.exclude(pk=legacy_agent.pk).delete()
        log(f"Demo agent renamed: {old_name} -> {target_name}")
        return

    stale_count, _ = legacy_agents.exclude(pk=target_agent.pk).delete()
    if stale_count:
        log(f"Removed stale legacy demo agent rows: {stale_count}")


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


def _seed_workflow_settings(auth_groups, teams, log):
    for key, config in DEMO_TEAMS.items():
        team = teams[key]
        audit_auth_groups = ",".join(
            str(group_id)
            for group_id in normalize_permission_group_sequence(
                config["approval_groups"]
            )
        )
        _, created = WorkflowAuditSetting.objects.update_or_create(
            team_id=team.team_id,
            workflow_type=WorkflowType.SQL_REVIEW,
            defaults={
                "team_name": team.team_name,
                "audit_auth_groups": audit_auth_groups,
            },
        )
        log(
            "Workflow setting {}: {} -> {}".format(
                "created" if created else "updated",
                team.team_name,
                " -> ".join(config["approval_groups"]),
            )
        )
        _, archive_created = WorkflowAuditSetting.objects.update_or_create(
            team_id=team.team_id,
            workflow_type=WorkflowType.ARCHIVE,
            defaults={
                "team_name": team.team_name,
                "audit_auth_groups": audit_auth_groups,
            },
        )
        log(
            "Archive setting {}: {} -> {}".format(
                "created" if archive_created else "updated",
                team.team_name,
                " -> ".join(config["approval_groups"]),
            )
        )
