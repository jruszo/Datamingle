import hashlib
import json
import logging
import os
import secrets
import tempfile
import time
from dataclasses import dataclass
from urllib.parse import quote

from django_redis import get_redis_connection
from django.conf import settings
from django.core.exceptions import ObjectDoesNotExist
from django.db import IntegrityError, transaction
from django.db.models import Q
from django.utils import timezone

from common.config import SysConfig
from common.utils.const import WorkflowType
from sql.engines import ResultSet
from sql.engines.models import ReviewResult, ReviewSet
from sql.mailbox import emit_execution_finished_notifications, resolve_mailbox_items
from sql.models import DEFAULT_NODE_EXPORTER_COLLECTORS, Instance, SqlWorkflow
from sql.models import MYSQLD_EXPORTER_COLLECTOR_PROFILES
from sql.models import NODE_EXPORTER_COLLECTOR_PROFILES
from sql.models import POSTGRES_EXPORTER_COLLECTOR_PROFILES
from sql.models import normalize_service_monitoring_collectors
from sql.utils.workflow_audit import Audit
from api_agents.models import (
    Agent,
    AgentCommand,
    AgentCommandStatus,
    AgentCommandType,
    AgentInstanceAssignment,
    AgentNodeAssignment,
    AgentStatus,
    AgentToolArtifact,
)

logger = logging.getLogger("default")
AGENT_KEY_VISIBLE_PREFIX_LENGTH = 16
METRICS_SCRAPE_PROFILE_INTERVALS = {
    "high": 5,
    "normal": 30,
    "low": 60,
}
ACTIVE_WEBSOCKET_METADATA_KEY = "active_websocket"
WEBSOCKET_CHANNEL_METADATA_KEY = "channel_name"
AGENT_SERVICE_ENDPOINTS_METADATA_KEY = "agent_service_endpoints"
LOCAL_DEMO_SEED_ENV = "RUN_LOCAL_DEMO_SEED"
LOCAL_DEMO_AGENT_NAME = "notebook-ubuntu"
LOCAL_DEMO_INSTANCE_NAME = "demo-mysql-workflow"
TERMINAL_COMMAND_STATUSES = {
    AgentCommandStatus.SUCCEEDED,
    AgentCommandStatus.FAILED,
    AgentCommandStatus.CANCELLED,
    AgentCommandStatus.EXPIRED,
}
REQUIRED_AGENT_KEY_PERMISSIONS = (
    "datamingle-agent:connect",
    "datamingle-agent:read-config",
    "datamingle-agent:execute-command",
)
AGENT_INGEST_METRICS_PERMISSION = "datamingle-agent:ingest-metrics"
AGENT_API_KEY_PREFIX = "dm_agent_"


class AgentAPIKeyRejected(Exception):
    pass


class AgentCommandDispatchError(Exception):
    pass


class AgentCommandExecutionError(Exception):
    def __init__(self, message, command=None):
        super().__init__(message)
        self.command = command


class AgentCommandTimeoutError(AgentCommandExecutionError):
    pass


@dataclass
class IssuedAgentAPIKey:
    value: str
    key_id: str = ""
    prefix: str = ""
    obfuscated_value: str = ""
    backend: str = "django"


def authenticate_agent_api_key(api_key):
    api_key = str(api_key or "").strip()
    if not api_key:
        return None
    api_key_hash = agent_api_key_hash(api_key)
    try:
        agent = Agent.objects.get(api_key_hash=api_key_hash)
    except Agent.DoesNotExist:
        return None
    if not agent.can_connect:
        raise AgentAPIKeyRejected("Agent is disabled or revoked.")
    return agent


def issue_agent_api_key(agent):
    for _ in range(5):
        api_key = generate_agent_api_key()
        agent.api_key_hash = agent_api_key_hash(api_key)
        agent.api_key_prefix = api_key[:AGENT_KEY_VISIBLE_PREFIX_LENGTH]
        agent.workos_api_key_id = ""
        try:
            with transaction.atomic():
                agent.save(
                    update_fields=[
                        "api_key_hash",
                        "api_key_prefix",
                        "workos_api_key_id",
                        "update_time",
                    ]
                )
        except IntegrityError:
            continue
        break
    else:
        raise AgentAPIKeyRejected("Unable to issue a unique agent API key.")
    return IssuedAgentAPIKey(
        value=api_key,
        key_id="",
        prefix=agent.api_key_prefix,
        obfuscated_value=f"{agent.api_key_prefix}...",
        backend="django",
    )


def revoke_agent_api_key(agent):
    agent.api_key_hash = None
    agent.api_key_prefix = ""
    agent.workos_api_key_id = ""
    agent.save(
        update_fields=[
            "api_key_hash",
            "api_key_prefix",
            "workos_api_key_id",
            "update_time",
        ]
    )


def generate_agent_api_key():
    return AGENT_API_KEY_PREFIX + secrets.token_urlsafe(32)


def agent_api_key_hash(api_key):
    return hashlib.sha256(str(api_key).encode("utf-8")).hexdigest()


def build_agent_install_command(request, api_key):
    datamingle_url = request.build_absolute_uri("/").rstrip("/")
    quoted_url = quote(datamingle_url, safe=":/")
    return (
        f"curl -fsSL {quoted_url}/api/v1/agents/install.sh | "
        f'sudo DATAMINGLE_URL="{datamingle_url}" '
        f'DATAMINGLE_AGENT_API_KEY="{api_key}" bash'
    )


def build_agent_config(agent, datamingle_url=""):
    assignment_records = list(
        agent.assignments.filter(enabled=True).select_related(
            "instance", "instance__node", "instance__mysql_cluster", "local_node"
        )
    )
    assignments = [
        serialize_assignment(assignment) for assignment in assignment_records
    ]
    nodes = build_agent_node_configs(agent, assignment_records)
    modules = build_module_configs(agent, assignments, datamingle_url=datamingle_url)
    tool_artifacts = [
        serialize_tool_artifact(artifact)
        for artifact in AgentToolArtifact.objects.filter(enabled=True)
    ]
    payload = {
        "agent_id": agent.id,
        "revision": agent.desired_config_revision,
        "organization_id": (
            agent.organization_id or settings.DATAMINGLE_SINGLE_TENANT_ORGANIZATION_ID
        ),
        "node": serialize_node(agent.local_node) if agent.local_node_id else None,
        "nodes": nodes,
        "assignments": assignments,
        "modules": modules,
        "tool_artifacts": tool_artifacts,
    }
    payload["config_hash"] = config_hash(payload)
    return payload


def serialize_assignment(assignment):
    instance = assignment.instance
    host, port = resolve_agent_service_endpoint(instance)
    modules = assignment_modules(assignment)
    online_schema_enabled = assignment_online_schema_enabled(assignment)
    service_monitoring_labels = dict(instance.monitoring_labels or {})
    if instance.db_type == "mysql":
        if (
            instance.mysql_topology_role
            and instance.mysql_topology_role != Instance.MYSQL_ROLE_UNKNOWN
        ):
            service_monitoring_labels["mysql_cluster_role"] = (
                instance.mysql_topology_role
            )
        if instance.mysql_cluster_id:
            service_monitoring_labels["mysql_cluster"] = (
                instance.mysql_cluster.label_value
            )
    return {
        "id": assignment.id,
        "instance_id": instance.id,
        "instance_name": instance.instance_name,
        "node_id": instance.node_id,
        "node_name": instance.node.name if instance.node_id else "",
        "node_monitoring_enabled": (
            instance.node.monitoring_enabled if instance.node_id else False
        ),
        "node_monitoring_collectors": (
            list(instance.node.monitoring_collectors or [])
            if instance.node_id
            else list(DEFAULT_NODE_EXPORTER_COLLECTORS)
        ),
        "node_monitoring_labels": (
            dict(instance.node.monitoring_labels or {}) if instance.node_id else {}
        ),
        "service_monitoring_labels": service_monitoring_labels,
        "service_monitoring_enabled": (
            instance.monitoring_enabled and instance.db_type in ("mysql", "pgsql")
        ),
        "workflow_enabled": instance.workflow_enabled,
        "service_monitoring_collectors": normalize_service_monitoring_collectors(
            instance.db_type, instance.monitoring_collectors
        ),
        "db_type": instance.db_type,
        "host": host,
        "port": port,
        "username": instance.user,
        "password": instance.password,
        "database": instance.db_name,
        "charset": instance.charset,
        "ssl": {
            "enabled": instance.is_ssl,
            "verify": instance.verify_ssl,
        },
        "modules": modules,
        "capabilities": assignment.capabilities,
        "command_enabled": assignment.command_enabled,
        "metrics_enabled": assignment.metrics_enabled,
        "online_schema_enabled": online_schema_enabled,
        "logs_enabled": assignment.logs_enabled,
    }


def resolve_agent_service_endpoint(instance):
    host = instance.host
    port = instance.port
    node = getattr(instance, "node", None)
    metadata = getattr(node, "metadata", None) if node is not None else None
    endpoints = (
        metadata.get(AGENT_SERVICE_ENDPOINTS_METADATA_KEY, {})
        if isinstance(metadata, dict)
        else {}
    )
    endpoint = (
        endpoints.get(instance.instance_name) if isinstance(endpoints, dict) else None
    )
    if not isinstance(endpoint, dict):
        return host, port

    endpoint_host = str(endpoint.get("host") or "").strip()
    if endpoint_host:
        host = endpoint_host

    endpoint_port = endpoint.get("port")
    if endpoint_port not in (None, ""):
        try:
            port = int(endpoint_port)
        except (TypeError, ValueError):
            pass

    return host, port


def serialize_node(node):
    return {
        "id": node.id,
        "name": node.name,
        "address": node.address,
        "monitoring_enabled": node.monitoring_enabled,
        "monitoring_collectors": list(node.monitoring_collectors or []),
    }


def build_agent_node_configs(agent, assignments):
    nodes = {}
    if agent.local_node_id:
        nodes[agent.local_node_id] = agent.local_node
    for assignment in assignments:
        if assignment.local_node_id:
            nodes[assignment.local_node_id] = assignment.local_node
        if assignment.instance.node_id:
            nodes[assignment.instance.node_id] = assignment.instance.node
    for node_assignment in AgentNodeAssignment.objects.filter(
        agent=agent, enabled=True
    ).select_related("node"):
        nodes[node_assignment.node_id] = node_assignment.node
    return [serialize_node(node) for node in nodes.values()]


def assignment_modules(assignment):
    modules = set(assignment.modules or [])
    if assignment.instance.db_type == "mysql":
        modules.add("mysql")
    if assignment.metrics_enabled:
        modules.add("metrics")
    if assignment_online_schema_enabled(assignment):
        modules.add("online_schema")
    if assignment.logs_enabled:
        modules.add("logs")
    return sorted(modules)


def assignment_online_schema_enabled(assignment):
    return bool(
        assignment.online_schema_enabled
        or (
            assignment.enabled
            and assignment.command_enabled
            and assignment.instance.db_type == "mysql"
            and assignment.instance.workflow_enabled
        )
    )


def _assignment_defaults_from_node_assignment(node_assignment):
    return {
        "node_assignment": node_assignment,
        "local_node": None,
        "enabled": node_assignment.enabled,
        "modules": node_assignment.modules,
        "capabilities": node_assignment.capabilities,
        "command_enabled": node_assignment.command_enabled,
        "metrics_enabled": node_assignment.metrics_enabled,
        "online_schema_enabled": node_assignment.online_schema_enabled,
        "logs_enabled": node_assignment.logs_enabled,
    }


def _local_node_command_enabled(agent, service, assignment=None):
    queryset = AgentInstanceAssignment.objects.filter(
        instance=service,
        enabled=True,
        command_enabled=True,
    ).exclude(agent=agent)
    if assignment is not None and assignment.pk:
        queryset = queryset.exclude(pk=assignment.pk)
    return not queryset.exists()


def _assignment_defaults_from_local_node(agent, service, assignment=None):
    return {
        "node_assignment": None,
        "local_node": agent.local_node,
        "enabled": agent.enabled and agent.status != AgentStatus.REVOKED,
        "modules": [],
        "capabilities": [],
        "command_enabled": _local_node_command_enabled(agent, service, assignment),
        "metrics_enabled": True,
        "online_schema_enabled": True,
        "logs_enabled": True,
    }


def _delete_inherited_instance_assignments(queryset, agent=None, summary=None):
    assignments = list(queryset.select_related("agent"))
    if not assignments:
        return

    agent_ids = {assignment.agent_id for assignment in assignments}
    queryset.delete()

    from api_agents.dispatch import notify_config_changed

    if agent is not None:
        agents = [agent]
    else:
        agents = list(Agent.objects.filter(id__in=agent_ids))
    for affected_agent in agents:
        affected_agent.bump_desired_config_revision(summary=summary)
        transaction.on_commit(
            lambda affected_agent=affected_agent: notify_config_changed(
                affected_agent, reason="node_assignment.synced"
            )
        )


def sync_node_assignment_to_services(node_assignment):
    if not node_assignment.enabled:
        clear_node_assignment_from_services(node_assignment)
        return

    defaults = _assignment_defaults_from_node_assignment(node_assignment)
    services = node_assignment.node.services.order_by("id")
    for service in services:
        assignment = (
            AgentInstanceAssignment.objects.select_for_update()
            .filter(agent=node_assignment.agent, instance=service)
            .first()
        )
        if assignment is None:
            assignment = AgentInstanceAssignment(
                agent=node_assignment.agent,
                instance=service,
            )
        for field, value in defaults.items():
            setattr(assignment, field, value)
        assignment.save()


def sync_node_assignments_for_instance(instance, previous_node_id=None):
    if previous_node_id and previous_node_id != instance.node_id:
        _delete_inherited_instance_assignments(
            AgentInstanceAssignment.objects.filter(
                instance=instance,
                node_assignment__node_id=previous_node_id,
            ),
            summary={
                "action": "node_assignment.service_removed",
                "instance_id": instance.id,
                "node_id": previous_node_id,
            },
        )
        _delete_inherited_instance_assignments(
            AgentInstanceAssignment.objects.filter(
                instance=instance,
                local_node_id=previous_node_id,
            ),
            summary={
                "action": "local_node_assignment.service_removed",
                "instance_id": instance.id,
                "node_id": previous_node_id,
            },
        )

    if not instance.node_id:
        return

    for node_assignment in AgentNodeAssignment.objects.filter(
        node_id=instance.node_id,
        enabled=True,
    ).select_related("agent", "node"):
        sync_node_assignment_to_service(node_assignment, instance)

    for agent in Agent.objects.filter(local_node_id=instance.node_id).select_related(
        "local_node"
    ):
        sync_local_node_assignment_to_service(agent, instance)


def sync_local_node_assignments_for_agent(agent, previous_node_id=None):
    if previous_node_id and previous_node_id != agent.local_node_id:
        _delete_inherited_instance_assignments(
            AgentInstanceAssignment.objects.filter(
                agent=agent,
                local_node_id=previous_node_id,
            ),
            agent=agent,
            summary={
                "action": "local_node_assignment.agent_moved",
                "agent_id": agent.id,
                "node_id": previous_node_id,
            },
        )

    if not agent.local_node_id:
        _delete_inherited_instance_assignments(
            AgentInstanceAssignment.objects.filter(
                agent=agent,
                local_node__isnull=False,
            ),
            agent=agent,
            summary={
                "action": "local_node_assignment.cleared",
                "agent_id": agent.id,
            },
        )
        return

    for service in agent.local_node.services.order_by("id"):
        sync_local_node_assignment_to_service(agent, service)


def sync_local_node_assignment_to_service(agent, service):
    assignment = (
        AgentInstanceAssignment.objects.select_for_update()
        .filter(agent=agent, instance=service)
        .first()
    )
    if assignment is not None and assignment.node_assignment_id:
        return
    if assignment is None:
        assignment = AgentInstanceAssignment(agent=agent, instance=service)

    defaults = _assignment_defaults_from_local_node(agent, service, assignment)
    for field, value in defaults.items():
        setattr(assignment, field, value)
    assignment.save()


def sync_node_assignment_to_service(node_assignment, service):
    defaults = _assignment_defaults_from_node_assignment(node_assignment)
    assignment = (
        AgentInstanceAssignment.objects.select_for_update()
        .filter(agent=node_assignment.agent, instance=service)
        .first()
    )
    if assignment is None:
        assignment = AgentInstanceAssignment(
            agent=node_assignment.agent,
            instance=service,
        )
    for field, value in defaults.items():
        setattr(assignment, field, value)
    assignment.save()


def clear_node_assignment_from_services(node_assignment):
    _delete_inherited_instance_assignments(
        AgentInstanceAssignment.objects.filter(node_assignment=node_assignment),
        agent=node_assignment.agent,
        summary={
            "action": "node_assignment.cleared",
            "node_assignment_id": node_assignment.id,
            "node_id": node_assignment.node_id,
        },
    )


def build_module_configs(agent, assignments, datamingle_url=""):
    configs = []
    node_monitoring_enabled = any(
        node.get("monitoring_enabled") for node in build_agent_node_configs(agent, [])
    ) or any(assignment.get("node_monitoring_enabled") for assignment in assignments)
    for module_name in (
        "mysql",
        "metrics",
        "online_schema",
        "logs",
        "node_monitoring",
        "service_monitoring",
    ):
        module_assignments = [
            {
                "id": assignment["id"],
                "instance_id": assignment["instance_id"],
                "modules": assignment["modules"],
            }
            for assignment in assignments
            if module_name in assignment["modules"]
        ]
        raw = {}
        enabled = bool(module_assignments)
        if module_name == "node_monitoring":
            enabled = node_monitoring_enabled
            raw = build_node_monitoring_module_config(
                agent, datamingle_url=datamingle_url
            )
        if module_name == "service_monitoring":
            monitored_assignments = [
                assignment
                for assignment in assignments
                if assignment.get("service_monitoring_enabled")
                and assignment.get("metrics_enabled")
                and assignment.get("db_type") in ("mysql", "pgsql")
            ]
            enabled = bool(monitored_assignments)
            raw = build_service_monitoring_module_config(
                agent,
                monitored_assignments,
                datamingle_url=datamingle_url,
            )
        configs.append(
            {
                "name": module_name,
                "enabled": enabled,
                "revision": agent.desired_config_revision,
                "assignments": module_assignments,
                "raw": raw,
            }
        )
    return configs


def build_remote_write_url(datamingle_url=""):
    base_url = (datamingle_url or "").rstrip("/")
    return (
        f"{base_url}/api/v1/prometheus/write"
        if base_url
        else "/api/v1/prometheus/write"
    )


def build_node_monitoring_module_config(agent, datamingle_url=""):
    artifact = (
        AgentToolArtifact.objects.filter(
            tool_name=AgentToolArtifact.TOOL_NODE_EXPORTER,
            enabled=True,
        )
        .order_by("-version", "id")
        .first()
    )
    remote_write_url = build_remote_write_url(datamingle_url)
    collectors = (
        list(agent.local_node.monitoring_collectors or [])
        if agent.local_node_id
        else list(DEFAULT_NODE_EXPORTER_COLLECTORS)
    )
    return {
        "remote_write_url": remote_write_url,
        "scrape_interval_seconds": 30,
        "scrape_profiles": build_scrape_profiles(
            collectors,
            NODE_EXPORTER_COLLECTOR_PROFILES,
        ),
        "node_exporter": {
            "listen_address": "127.0.0.1:9100",
            "metrics_url": "http://127.0.0.1:9100/metrics",
            "collectors": collectors,
            "artifact": serialize_tool_artifact(artifact) if artifact else None,
        },
        "labels": {
            **{
                f"dm_{name}": value
                for name, value in (
                    agent.local_node.monitoring_labels or {}
                    if agent.local_node_id
                    else {}
                ).items()
            },
            "agent_id": str(agent.id),
            "agent_name": agent.name,
            "node_id": str(agent.local_node_id or ""),
            "node_name": agent.local_node.name if agent.local_node_id else "",
        },
    }


def build_service_monitoring_module_config(agent, assignments, datamingle_url=""):
    mysql_artifact = (
        AgentToolArtifact.objects.filter(
            tool_name=AgentToolArtifact.TOOL_MYSQLD_EXPORTER,
            enabled=True,
        )
        .order_by("-version", "id")
        .first()
    )
    postgres_artifact = (
        AgentToolArtifact.objects.filter(
            tool_name=AgentToolArtifact.TOOL_POSTGRES_EXPORTER,
            enabled=True,
        )
        .order_by("-version", "id")
        .first()
    )
    remote_write_url = build_remote_write_url(datamingle_url)
    services = []
    for index, assignment in enumerate(assignments):
        port = 9200 + index
        artifact = (
            mysql_artifact if assignment["db_type"] == "mysql" else postgres_artifact
        )
        collector_profiles = (
            MYSQLD_EXPORTER_COLLECTOR_PROFILES
            if assignment["db_type"] == "mysql"
            else POSTGRES_EXPORTER_COLLECTOR_PROFILES
        )
        services.append(
            {
                "assignment_id": assignment["id"],
                "instance_id": assignment["instance_id"],
                "instance_name": assignment["instance_name"],
                "node_id": assignment["node_id"],
                "node_name": assignment["node_name"],
                "db_type": assignment["db_type"],
                "host": assignment["host"],
                "port": assignment["port"],
                "username": assignment["username"],
                "password": assignment["password"],
                "database": assignment["database"],
                "labels": {
                    **{
                        f"dm_{name}": value
                        for name, value in assignment["node_monitoring_labels"].items()
                    },
                    **{
                        f"dm_{name}": value
                        for name, value in assignment[
                            "service_monitoring_labels"
                        ].items()
                    },
                },
                "collectors": assignment["service_monitoring_collectors"],
                "scrape_profiles": build_scrape_profiles(
                    assignment["service_monitoring_collectors"],
                    collector_profiles,
                ),
                "ssl": assignment["ssl"],
                "exporter": {
                    "listen_address": f"127.0.0.1:{port}",
                    "metrics_url": f"http://127.0.0.1:{port}/metrics",
                    "artifact": serialize_tool_artifact(artifact) if artifact else None,
                },
            }
        )
    return {
        "remote_write_url": remote_write_url,
        "scrape_interval_seconds": 30,
        "scrape_profiles": [
            {
                "name": name,
                "interval_seconds": interval,
            }
            for name, interval in METRICS_SCRAPE_PROFILE_INTERVALS.items()
        ],
        "services": services,
        "labels": {
            "agent_id": str(agent.id),
            "agent_name": agent.name,
        },
    }


def build_scrape_profiles(collectors, collector_profiles):
    selected = set(collectors or [])
    profiles = []
    assigned = set()
    for name, interval in METRICS_SCRAPE_PROFILE_INTERVALS.items():
        profile_collectors = [
            collector
            for collector in collector_profiles.get(name, ())
            if collector in selected
        ]
        assigned.update(profile_collectors)
        profiles.append(
            {
                "name": name,
                "interval_seconds": interval,
                "collectors": profile_collectors,
            }
        )
    remaining = [
        collector for collector in collectors or [] if collector not in assigned
    ]
    if remaining:
        normal_profile = next(
            (profile for profile in profiles if profile["name"] == "normal"),
            None,
        )
        if normal_profile is None:
            profiles.append(
                {
                    "name": "normal",
                    "interval_seconds": METRICS_SCRAPE_PROFILE_INTERVALS["normal"],
                    "collectors": remaining,
                }
            )
        else:
            normal_profile["collectors"].extend(remaining)
    return profiles


def serialize_tool_artifact(artifact):
    return {
        "id": artifact.id,
        "tool_name": artifact.tool_name,
        "version": artifact.version,
        "platform": artifact.platform,
        "architecture": artifact.architecture,
        "download_url": artifact.download_url,
        "sha256": artifact.sha256,
        "size_bytes": artifact.size_bytes,
        "notes": artifact.notes,
    }


def config_hash(payload):
    content = {key: value for key, value in payload.items() if key != "config_hash"}
    raw = json.dumps(content, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def agent_active_websocket_channel(agent):
    active_websocket = dict(
        (agent.metadata or {}).get(ACTIVE_WEBSOCKET_METADATA_KEY) or {}
    )
    return active_websocket.get(WEBSOCKET_CHANNEL_METADATA_KEY, "")


def has_active_agent_websocket(agent):
    return bool(agent_active_websocket_channel(agent))


def command_capable_assignments(db_type="mysql", require_websocket=True):
    assignments = (
        AgentInstanceAssignment.objects.select_related("agent", "instance")
        .filter(
            enabled=True,
            command_enabled=True,
            agent__enabled=True,
            agent__status=AgentStatus.ONLINE,
            instance__db_type=db_type,
        )
        .order_by("-agent__last_seen_at", "agent_id")
    )
    if not require_websocket:
        return assignments
    agent_ids = [
        assignment.agent_id
        for assignment in assignments
        if has_active_agent_websocket(assignment.agent)
    ]
    return assignments.filter(agent_id__in=agent_ids)


def command_capable_instance_ids(db_type="mysql", require_websocket=True):
    return sorted(
        {
            assignment.instance_id
            for assignment in command_capable_assignments(
                db_type=db_type, require_websocket=require_websocket
            )
        }
    )


def filter_agent_runnable_instances(queryset, db_type="mysql", require_websocket=True):
    return queryset.filter(
        db_type=db_type,
        id__in=command_capable_instance_ids(
            db_type=db_type, require_websocket=require_websocket
        ),
    )


def dispatch_agent_command(command, require_delivery=True):
    from api_agents.dispatch import notify_command_available

    if command.status == AgentCommandStatus.QUEUED:
        command.status = AgentCommandStatus.DISPATCHED
        command.dispatched_at = timezone.now()
        command.save(update_fields=["status", "dispatched_at", "update_time"])
        command.append_event("command.dispatched", "Command dispatched to agent.")
    delivered = notify_command_available(command)
    if require_delivery and not delivered:
        command.status = AgentCommandStatus.FAILED
        command.finished_at = timezone.now()
        command.error = {"message": "Agent websocket is not connected."}
        command.save(update_fields=["status", "finished_at", "error", "update_time"])
        command.append_event(
            "command.dispatch_failed", "Agent websocket is not connected."
        )
        raise AgentCommandDispatchError("Agent websocket is not connected.")
    return command


def command_capable_assignment_for_instance(
    instance_id, db_type="mysql", require_websocket=True
):
    assignments = (
        AgentInstanceAssignment.objects.select_related("agent", "instance")
        .filter(
            instance_id=instance_id,
            enabled=True,
            command_enabled=True,
            agent__enabled=True,
            agent__status=AgentStatus.ONLINE,
            instance__db_type=db_type,
        )
        .order_by("-agent__last_seen_at", "agent_id")
    )
    for assignment in assignments:
        if require_websocket and not has_active_agent_websocket(assignment.agent):
            continue
        return assignment
    return None


def create_agent_command_for_instance(
    *,
    instance,
    command_type,
    workflow_type,
    workflow_id,
    payload,
    idempotency_key=None,
):
    assignment = command_capable_assignment_for_instance(
        instance.id, db_type=instance.db_type
    )
    if assignment is None:
        raise AgentCommandDispatchError(
            "No online command-capable agent is assigned to this service."
        )
    return AgentCommand.objects.create(
        agent=assignment.agent,
        instance=instance,
        workflow_type=workflow_type,
        workflow_id=str(workflow_id),
        command_type=command_type,
        idempotency_key=idempotency_key,
        payload=payload,
    )


def wait_for_agent_command(command, timeout_seconds=60, poll_interval=0.2):
    deadline = time.monotonic() + max(float(timeout_seconds), 1.0)
    while time.monotonic() < deadline:
        command.refresh_from_db()
        if command.status in TERMINAL_COMMAND_STATUSES:
            return command
        time.sleep(poll_interval)
    command.refresh_from_db()
    if command.status not in TERMINAL_COMMAND_STATUSES:
        command.status = AgentCommandStatus.EXPIRED
        command.finished_at = timezone.now()
        command.error = {"message": "Agent command timed out."}
        command.save(update_fields=["status", "finished_at", "error", "update_time"])
        command.append_event("command.expired", "Agent command timed out.")
        raise AgentCommandTimeoutError("Agent command timed out.", command=command)
    return command


def run_agent_command_sync(
    *,
    instance,
    command_type,
    workflow_type,
    workflow_id,
    payload,
    timeout_seconds=60,
    idempotency_key=None,
):
    command = create_agent_command_for_instance(
        instance=instance,
        command_type=command_type,
        workflow_type=workflow_type,
        workflow_id=workflow_id,
        payload=payload,
        idempotency_key=idempotency_key,
    )
    if _is_local_demo_direct_query_command(command):
        return _complete_local_demo_query_command(command)
    if _is_local_demo_direct_review_command(command):
        return _complete_local_demo_review_command(command)
    dispatch_agent_command(command)
    command = wait_for_agent_command(command, timeout_seconds=timeout_seconds)
    if command.status != AgentCommandStatus.SUCCEEDED:
        message = (
            command.error.get("message") if isinstance(command.error, dict) else ""
        ) or (command.result.get("message") if isinstance(command.result, dict) else "")
        raise AgentCommandExecutionError(
            message or f"Agent command {command.status}.", command=command
        )
    return command


def result_set_from_agent_result(full_sql, result):
    result = result or {}
    rows = result.get("rows") or []
    column_list = result.get("column_list") or result.get("columns") or []
    result_set = ResultSet(
        full_sql=result.get("full_sql") or full_sql,
        rows=rows,
        column_list=column_list,
        column_type=result.get("column_type") or [],
        affected_rows=result.get("affected_rows", result.get("row_count", len(rows))),
        status=result.get("status"),
    )
    result_set.query_time = result.get("execution_seconds", "")
    result_set.warning = result.get("warning")
    result_set.error = result.get("error")
    return result_set


def _json_safe_rows(rows):
    safe_rows = []
    for row in rows or []:
        if isinstance(row, dict):
            safe_rows.append(row)
        elif isinstance(row, (list, tuple)):
            safe_rows.append(list(row))
        else:
            safe_rows.append(row)
    return safe_rows


def _result_set_to_agent_result(result_set, full_sql):
    rows = _json_safe_rows(result_set.rows)
    return {
        "rows": rows,
        "columns": list(result_set.column_list or []),
        "column_list": list(result_set.column_list or []),
        "column_type": list(result_set.column_type or []),
        "full_sql": result_set.full_sql or full_sql,
        "row_count": len(rows),
        "affected_rows": result_set.affected_rows,
        "execution_seconds": result_set.query_time,
        "status": result_set.status,
        "warning": result_set.warning,
        "error": result_set.error,
        "seconds_behind_master": "",
    }


def _is_local_demo_direct_query_command(command):
    if os.environ.get(LOCAL_DEMO_SEED_ENV) != "1":
        return False
    if command.command_type != AgentCommandType.QUERY_EXECUTE:
        return False
    if not str(command.workflow_type or "").startswith("query"):
        return False
    if command.instance.instance_name != LOCAL_DEMO_INSTANCE_NAME:
        return False
    if command.agent.name != LOCAL_DEMO_AGENT_NAME:
        return False
    return bool((command.agent.metadata or {}).get("seeded"))


def _is_local_demo_agent_command(command):
    if os.environ.get(LOCAL_DEMO_SEED_ENV) != "1":
        return False
    if command.instance.instance_name != LOCAL_DEMO_INSTANCE_NAME:
        return False
    if command.agent.name != LOCAL_DEMO_AGENT_NAME:
        return False
    return bool((command.agent.metadata or {}).get("seeded"))


def _is_local_demo_direct_review_command(command):
    if not _is_local_demo_agent_command(command):
        return False
    return (
        command.command_type == AgentCommandType.WORKFLOW_CHECK
        and command.workflow_type == "workflow.check"
    ) or (
        command.command_type == AgentCommandType.EXPORT_CHECK
        and command.workflow_type == "export.check"
    )


def _complete_local_demo_query_command(command):
    from sql.engines import get_engine

    payload = command.payload or {}
    sql = payload.get("sql") or ""
    max_execution_time = payload.get("max_execution_time_ms") or 0

    command.status = AgentCommandStatus.RUNNING
    command.started_at = timezone.now()
    command.save(update_fields=["status", "started_at", "update_time"])
    command.append_event(
        "command.local_demo_direct",
        "Executing local demo query directly in the app container.",
    )

    engine = get_engine(instance=command.instance)
    result_set = engine.query(
        db_name=payload.get("db_name") or None,
        sql=sql,
        limit_num=payload.get("limit") or 0,
        parameters=payload.get("parameters") or None,
        max_execution_time=max_execution_time,
    )
    command.status = AgentCommandStatus.SUCCEEDED
    command.finished_at = timezone.now()
    command.result = _result_set_to_agent_result(result_set, sql)
    command.error = {}
    command.save(
        update_fields=[
            "status",
            "finished_at",
            "result",
            "error",
            "update_time",
        ]
    )
    command.append_event(
        "command.succeeded",
        "Completed by local demo direct query execution.",
    )
    return command


def _classify_local_demo_statement(statement, db_type="mysql"):
    from sql.utils.sql_utils import get_syntax_type

    syntax_name = get_syntax_type(statement, parser=True, db_type=db_type)
    if syntax_name not in {"DDL", "DML"}:
        syntax_name = get_syntax_type(statement, parser=False, db_type=db_type)
    if syntax_name == "DDL":
        return 1
    if syntax_name == "DML":
        return 2
    return 0


def _local_demo_review_rows(sql, db_type="mysql"):
    from sql.utils.sql_utils import generate_sql

    statements = [row["sql"] for row in generate_sql(sql)] or [sql]
    rows = []
    syntax_types = set()
    for index, statement in enumerate(statements, start=1):
        syntax_type = _classify_local_demo_statement(statement, db_type=db_type)
        if syntax_type:
            syntax_types.add(syntax_type)
        rows.append(
            {
                "id": index,
                "errlevel": 0,
                "stagestatus": "Audit completed",
                "errormessage": "None",
                "sql": statement,
            }
        )
    summary_syntax_type = next(iter(syntax_types)) if len(syntax_types) == 1 else 0
    return rows, summary_syntax_type


def _local_demo_export_review_result(command):
    from sql.engines import get_engine

    payload = command.payload or {}
    full_sql = (payload.get("sql") or "").strip()
    clean_sql = full_sql.lower()
    row = {
        "id": 1,
        "errlevel": 0,
        "stagestatus": "Ready",
        "errormessage": "None",
        "sql": full_sql,
        "affected_rows": 0,
    }
    error_count = 0
    affected_rows = 0

    if not clean_sql.startswith(("select", "with")):
        row.update(
            {
                "errlevel": 2,
                "stagestatus": "Check failed!",
                "errormessage": "Disallowed statement!",
            }
        )
        error_count = 1
    else:
        count_sql = f"SELECT COUNT(*) FROM ({full_sql.rstrip(';')}) t"
        result_set = get_engine(instance=command.instance).query(
            db_name=payload.get("db_name") or None,
            sql=count_sql,
        )
        if result_set.error:
            row.update(
                {
                    "errlevel": 2,
                    "stagestatus": "Check failed!",
                    "errormessage": result_set.error,
                }
            )
            error_count = 1
        elif result_set.rows:
            affected_rows = int(result_set.rows[0][0])
            row["affected_rows"] = affected_rows

    return {
        "full_sql": full_sql,
        "checked": True,
        "warning": None,
        "error": None,
        "warning_count": 0,
        "error_count": error_count,
        "is_critical": False,
        "syntax_type": 3,
        "rows": [row],
        "review_rows": [row],
        "column_list": ["id", "errlevel", "stagestatus", "errormessage", "sql"],
        "status": "Ready" if error_count == 0 else "Check failed!",
        "affected_rows": affected_rows,
    }


def _local_demo_workflow_review_result(command):
    payload = command.payload or {}
    full_sql = (payload.get("sql") or "").strip()
    rows, syntax_type = _local_demo_review_rows(
        full_sql,
        db_type=command.instance.db_type,
    )
    return {
        "full_sql": full_sql,
        "checked": True,
        "warning": None,
        "error": None,
        "warning_count": 0,
        "error_count": 0,
        "is_critical": False,
        "syntax_type": syntax_type,
        "rows": rows,
        "review_rows": rows,
        "column_list": ["id", "errlevel", "stagestatus", "errormessage", "sql"],
        "status": "Audit completed",
        "affected_rows": 0,
    }


def _complete_local_demo_review_command(command):
    command.status = AgentCommandStatus.RUNNING
    command.started_at = timezone.now()
    command.save(update_fields=["status", "started_at", "update_time"])
    command.append_event(
        "command.local_demo_direct",
        "Completing local demo review command directly in the app container.",
    )

    if command.command_type == AgentCommandType.EXPORT_CHECK:
        result = _local_demo_export_review_result(command)
    else:
        result = _local_demo_workflow_review_result(command)

    command.status = AgentCommandStatus.SUCCEEDED
    command.finished_at = timezone.now()
    command.result = result
    command.error = {}
    command.save(
        update_fields=[
            "status",
            "finished_at",
            "result",
            "error",
            "update_time",
        ]
    )
    command.append_event(
        "command.succeeded",
        "Completed by local demo direct review execution.",
    )
    return command


def review_set_from_agent_result(full_sql, result):
    result = result or {}
    review_set = ReviewSet(
        full_sql=result.get("full_sql") or full_sql,
        affected_rows=result.get("affected_rows", 0),
        column_list=result.get("column_list") or [],
        status=result.get("status"),
    )
    review_set.checked = result.get("checked", True)
    review_set.warning = result.get("warning")
    review_set.error = result.get("error")
    review_set.warning_count = int(result.get("warning_count") or 0)
    review_set.error_count = int(result.get("error_count") or 0)
    review_set.is_critical = bool(result.get("is_critical", False))
    review_set.syntax_type = int(result.get("syntax_type") or 0)
    rows = result.get("review_rows") or result.get("rows") or []
    review_set.rows = [
        row if isinstance(row, ReviewResult) else ReviewResult(**row)
        for row in rows
        if isinstance(row, dict) or isinstance(row, ReviewResult)
    ]
    return review_set


def dispatch_sql_workflow_to_agent(workflow, user=None, executor=None):
    with transaction.atomic():
        workflow = (
            SqlWorkflow.objects.select_for_update()
            .select_related("instance")
            .get(pk=workflow.pk)
        )
        previous_status = workflow.status
        existing_command = (
            AgentCommand.objects.select_related("agent", "instance")
            .filter(
                workflow_type="sql_workflow",
                workflow_id=str(workflow.id),
                status__in=(
                    AgentCommandStatus.QUEUED,
                    AgentCommandStatus.DISPATCHED,
                    AgentCommandStatus.ACCEPTED,
                    AgentCommandStatus.RUNNING,
                ),
            )
            .order_by("-create_time")
            .first()
        )
        if existing_command is not None:
            command = existing_command
        else:
            if (
                not workflow.is_offline_export
                and not workflow.instance.workflow_enabled
            ):
                raise AgentCommandDispatchError(
                    "This MySQL service is not enabled for DDL/DML workflows."
                )
            assignment = command_capable_assignment_for_instance(workflow.instance_id)
            if assignment is None:
                raise AgentCommandDispatchError(
                    "No online command-capable agent is assigned to this MySQL service."
                )

            try:
                content = workflow.sqlworkflowcontent
            except ObjectDoesNotExist as exc:
                raise ValueError(
                    f"SQL workflow {workflow.id} is missing SQL content."
                ) from exc

            command_type = (
                AgentCommandType.EXPORT_EXECUTE
                if workflow.is_offline_export
                else AgentCommandType.WORKFLOW_EXECUTE
            )
            config = SysConfig()
            try:
                with transaction.atomic():
                    command = AgentCommand.objects.create(
                        agent=assignment.agent,
                        instance=workflow.instance,
                        workflow_type="sql_workflow",
                        workflow_id=str(workflow.id),
                        command_type=command_type,
                        idempotency_key=f"sql_workflow:{workflow.id}",
                        payload={
                            "workflow_id": workflow.id,
                            "workflow_name": workflow.workflow_name,
                            "db_name": workflow.db_name,
                            "schema_name": workflow.schema_name,
                            "syntax_type": workflow.syntax_type,
                            "sql": content.sql_content,
                            "export_format": workflow.export_format,
                            "max_export_rows": int(
                                config.get("max_export_rows", "10000") or 10000
                            ),
                            "executor": executor or "direct",
                            "submitted_by": (
                                getattr(user, "username", "") if user else ""
                            ),
                        },
                    )
            except IntegrityError:
                command = AgentCommand.objects.select_related("agent", "instance").get(
                    idempotency_key=f"sql_workflow:{workflow.id}"
                )
        workflow.status = "workflow_executing"
        workflow.save(update_fields=["status"])

    if command.status in {
        AgentCommandStatus.QUEUED,
        AgentCommandStatus.DISPATCHED,
    }:
        try:
            dispatch_agent_command(command)
        except AgentCommandDispatchError:
            SqlWorkflow.objects.filter(pk=workflow.pk).update(status=previous_status)
            raise
    resolve_mailbox_items(workflow, category="execution_needed")
    return command


def complete_agent_workflow_command(command, outcome, message="", payload=None):
    if command.workflow_type != "sql_workflow":
        return
    payload = payload or {}
    try:
        workflow = SqlWorkflow.objects.select_related(
            "instance", "sqlworkflowcontent"
        ).get(id=command.workflow_id)
    except (SqlWorkflow.DoesNotExist, ValueError):
        return

    now = timezone.now()
    if outcome == "success":
        try:
            if workflow.is_offline_export:
                _persist_agent_export_result(workflow, payload)
            workflow.status = "workflow_finish"
            errlevel = 0
            stage_status = "Execute Successfully"
            error_message = message
        except Exception as exc:
            logger.exception("Failed to persist agent workflow result")
            workflow.status = "workflow_exception"
            errlevel = 2
            stage_status = "Execute Failed"
            error_message = str(exc)
    elif outcome == "cancelled":
        workflow.status = "workflow_abort"
        errlevel = 1
        stage_status = "Cancelled"
        error_message = message or "Command cancelled by agent."
    else:
        workflow.status = "workflow_exception"
        errlevel = 2
        stage_status = "Execute Failed"
        error_message = message or payload.get("message", "Agent command failed.")

    workflow.finish_time = now
    workflow.sqlworkflowcontent.execute_result = _agent_review_result_json(
        workflow=workflow,
        errlevel=errlevel,
        stage_status=stage_status,
        error_message=error_message,
        result=payload,
    )
    workflow.sqlworkflowcontent.save(update_fields=["execute_result"])
    workflow.save(update_fields=["status", "finish_time"])
    clear_agent_execution_caches(workflow)

    audit = Audit.detail_by_workflow_id(
        workflow_id=workflow.id, workflow_type=WorkflowType.SQL_REVIEW
    )
    if audit is not None:
        Audit.add_log(
            audit_id=audit.audit_id,
            operation_type=6,
            operation_type_desc="Execution finished",
            operation_info=f"Agent execution result: {stage_status}",
            operator="",
            operator_display="System",
        )
    sys_config = SysConfig()
    is_notified = (
        "Execute" in sys_config.get("notify_phase_control").split(",")
        if sys_config.get("notify_phase_control")
        else True
    )
    if is_notified:
        from sql.notify import notify_for_execute

        notify_for_execute(workflow)
    resolve_mailbox_items(workflow, category="execution_needed")
    emit_execution_finished_notifications(
        workflow,
        outcome="success" if outcome == "success" else "failure",
        actor=None,
        dedupe_suffix=now.strftime("%Y%m%d%H%M%S%f"),
    )


def clear_agent_execution_caches(workflow):
    if workflow.syntax_type != 1:
        return
    try:
        redis = get_redis_connection("default")
        for key in redis.scan_iter(match="*insRes*", count=2000):
            redis.delete(key)
    except Exception:
        logger.exception(
            "Failed to clear instance resource cache after agent workflow execution."
        )


def _agent_review_result_json(workflow, errlevel, stage_status, error_message, result):
    result = result or {}
    sql = workflow.sqlworkflowcontent.sql_content
    review_rows = result.get("review_rows") or result.get("execute_rows")
    if review_rows:
        return json.dumps(review_rows)
    affected_rows = result.get("affected_rows", 0)
    review_set = ReviewSet(full_sql=sql)
    review_set.rows = [
        ReviewResult(
            id=0,
            stage="Agent execution",
            errlevel=errlevel,
            stagestatus=stage_status,
            errormessage=error_message,
            sql=sql,
            affected_rows=affected_rows,
            actual_affected_rows=result.get("actual_affected_rows", affected_rows),
            sequence="0_0_0",
            backup_dbname=None,
            execute_time=result.get("execution_seconds", 0),
            sqlsha1="",
            agent_command_id=command_id_or_empty(result),
        )
    ]
    return review_set.json()


def _persist_agent_export_result(workflow, result):
    if result.get("file_name"):
        workflow.file_name = result["file_name"]
        workflow.save(update_fields=["file_name"])
        return result["file_name"]

    rows = result.get("rows") or []
    columns = result.get("column_list") or result.get("columns") or []
    if not rows and result.get("affected_rows", 0):
        raise ValueError("Agent export result did not include rows.")

    from sql.offlinedownload import save_to_format_file
    from sql.storage import DynamicStorage

    storage = DynamicStorage()
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            file_name = save_to_format_file(
                workflow.export_format, rows, workflow, columns, temp_dir
            )
            tmp_file = os.path.join(temp_dir, file_name)
            with open(tmp_file, "rb") as fp:
                storage.save(file_name, fp)
        workflow.file_name = file_name
        workflow.save(update_fields=["file_name"])
        result["file_name"] = file_name
        return file_name
    finally:
        storage.close()


def command_id_or_empty(result):
    return result.get("command_id", "")


def request_command_cancel(command):
    from api_agents.dispatch import notify_command_cancel

    command.cancel_requested_at = timezone.now()
    command.save(update_fields=["cancel_requested_at", "update_time"])
    command.append_event("command.cancel_requested", "Cancellation requested.")
    notify_command_cancel(command)
    return command


def notify_tool_artifact_changed(artifact, action="tool_artifact.changed", user=None):
    from api_agents.dispatch import notify_config_changed

    agents = (
        Agent.objects.filter(
            Q(
                enabled=True,
                assignments__enabled=True,
                assignments__command_enabled=True,
                assignments__instance__db_type="mysql",
                assignments__instance__workflow_enabled=True,
            )
            | Q(
                enabled=True,
                assignments__enabled=True,
                assignments__metrics_enabled=True,
                assignments__instance__monitoring_enabled=True,
                assignments__instance__db_type__in=("mysql", "pgsql"),
            )
        )
        .distinct()
        .order_by("id")
    )
    for agent in agents:
        agent.bump_desired_config_revision(
            summary={
                "action": action,
                "tool_artifact_id": artifact.id,
                "tool_name": artifact.tool_name,
                "version": artifact.version,
            },
            created_by=user if getattr(user, "is_authenticated", False) else None,
        )
        transaction.on_commit(
            lambda current_agent=agent: notify_config_changed(
                current_agent, reason=action
            )
        )


def notify_node_config_changed(node, summary=None, reason="node.changed", user=None):
    from api_agents.dispatch import notify_config_changed

    agents = (
        Agent.objects.filter(
            Q(local_node=node)
            | Q(node_assignments__node=node, node_assignments__enabled=True)
        )
        .filter(enabled=True)
        .exclude(status=AgentStatus.REVOKED)
        .distinct()
        .order_by("id")
    )
    for agent in agents:
        agent.bump_desired_config_revision(
            summary=summary or {"action": reason, "node_id": node.id},
            created_by=user if getattr(user, "is_authenticated", False) else None,
        )
        transaction.on_commit(
            lambda current_agent=agent: notify_config_changed(
                current_agent, reason=reason
            )
        )
