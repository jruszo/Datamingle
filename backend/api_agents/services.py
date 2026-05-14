import hashlib
import hmac
import json
import logging
import secrets
from dataclasses import dataclass
from urllib.parse import quote
from urllib.parse import urljoin

from django_redis import get_redis_connection
from django.conf import settings
from django.core.exceptions import (
    ImproperlyConfigured,
    ObjectDoesNotExist,
    PermissionDenied,
)
from django.db import IntegrityError, transaction
from django.utils import timezone
import requests

from common.config import SysConfig
from common.authenticate.workos import _dynamic_import_workos
from common.utils.const import WorkflowType
from sql.engines.models import ReviewResult, ReviewSet
from sql.mailbox import emit_execution_finished_notifications, resolve_mailbox_items
from sql.models import SqlWorkflow
from sql.notify import notify_for_execute
from sql.utils.workflow_audit import Audit
from api_agents.models import (
    Agent,
    AgentCommand,
    AgentCommandStatus,
    AgentCommandType,
    AgentInstanceAssignment,
    AgentStatus,
    AgentToolArtifact,
)

logger = logging.getLogger("default")
AGENT_API_KEY_PREFIX = "dma_"
AGENT_KEY_VISIBLE_PREFIX_LENGTH = 16
REQUIRED_AGENT_KEY_PERMISSIONS = (
    "datamingle-agent:connect",
    "datamingle-agent:read-config",
    "datamingle-agent:execute-command",
)


class AgentAPIKeyRejected(Exception):
    pass


@dataclass
class IssuedAgentAPIKey:
    value: str
    key_id: str = ""
    prefix: str = ""
    obfuscated_value: str = ""
    backend: str = "local"


def generate_agent_api_key():
    return f"{AGENT_API_KEY_PREFIX}{secrets.token_urlsafe(32)}"


def hash_agent_api_key(api_key):
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def create_agent_api_key(agent):
    api_key = generate_agent_api_key()
    agent.api_key_hash = hash_agent_api_key(api_key)
    agent.api_key_prefix = api_key[:AGENT_KEY_VISIBLE_PREFIX_LENGTH]
    agent.save(update_fields=["api_key_hash", "api_key_prefix", "update_time"])
    return IssuedAgentAPIKey(
        value=api_key,
        prefix=api_key[:AGENT_KEY_VISIBLE_PREFIX_LENGTH],
        backend="local",
    )


def authenticate_agent_api_key(api_key):
    return get_agent_api_key_provider().authenticate(api_key)


def issue_agent_api_key(agent):
    return get_agent_api_key_provider().issue(agent)


def revoke_agent_api_key(agent):
    return get_agent_api_key_provider().revoke(agent)


def get_agent_api_key_provider():
    backend = settings.DATAMINGLE_AGENT_API_KEY_BACKEND.strip().lower()
    if backend == "local":
        return LocalAgentAPIKeyProvider()
    if backend == "workos":
        return WorkOSAgentAPIKeyProvider()
    raise ImproperlyConfigured(
        f"Unsupported DATAMINGLE_AGENT_API_KEY_BACKEND: {backend}"
    )


class LocalAgentAPIKeyProvider:
    def issue(self, agent):
        return create_agent_api_key(agent)

    def authenticate(self, api_key):
        return authenticate_local_agent_api_key(api_key)

    def revoke(self, agent):
        agent.api_key_hash = None
        agent.api_key_prefix = ""
        agent.save(update_fields=["api_key_hash", "api_key_prefix", "update_time"])


class WorkOSAgentAPIKeyProvider:
    def issue(self, agent):
        if not settings.WORKOS_API_KEY or not settings.WORKOS_ORGANIZATION_ID:
            raise ImproperlyConfigured(
                "WORKOS_API_KEY and WORKOS_ORGANIZATION_ID are required to issue agent API keys."
            )
        api_key = create_workos_organization_api_key(
            name=f"Datamingle Agent: {agent.display_name or agent.name}",
            permissions=list(REQUIRED_AGENT_KEY_PERMISSIONS),
        )
        owner = api_key.get("owner") or {}
        if (
            owner.get("type") != "organization"
            or owner.get("id") != settings.WORKOS_ORGANIZATION_ID
        ):
            raise PermissionDenied(
                "WorkOS returned an API key for the wrong organization."
            )

        value = api_key["value"]
        agent.workos_api_key_id = api_key["id"]
        agent.api_key_hash = None
        agent.api_key_prefix = (
            api_key.get("obfuscated_value") or value[:AGENT_KEY_VISIBLE_PREFIX_LENGTH]
        )
        agent.save(
            update_fields=[
                "workos_api_key_id",
                "api_key_hash",
                "api_key_prefix",
                "update_time",
            ]
        )
        return IssuedAgentAPIKey(
            value=value,
            key_id=api_key["id"],
            prefix=agent.api_key_prefix,
            obfuscated_value=api_key.get("obfuscated_value", ""),
            backend="workos",
        )

    def authenticate(self, api_key):
        workos_key = validate_workos_api_key(api_key)
        if workos_key is None:
            return None
        owner = getattr(workos_key, "owner", None)
        owner_type = getattr(owner, "type", "")
        owner_id = getattr(owner, "id", "")
        if owner_type != "organization" or owner_id != settings.WORKOS_ORGANIZATION_ID:
            raise AgentAPIKeyRejected(
                "Agent API key belongs to the wrong organization."
            )
        permissions = set(getattr(workos_key, "permissions", []) or [])
        missing = set(REQUIRED_AGENT_KEY_PERMISSIONS) - permissions
        if missing:
            raise AgentAPIKeyRejected("Agent API key is missing required permissions.")
        try:
            agent = Agent.objects.get(workos_api_key_id=workos_key.id)
        except Agent.DoesNotExist:
            return None
        if not agent.can_connect:
            raise AgentAPIKeyRejected("Agent is disabled or revoked.")
        return agent

    def revoke(self, agent):
        if not agent.workos_api_key_id:
            return
        workos_client().api_keys.delete_api_key(agent.workos_api_key_id)
        agent.workos_api_key_id = ""
        agent.api_key_prefix = ""
        agent.save(update_fields=["workos_api_key_id", "api_key_prefix", "update_time"])


def authenticate_local_agent_api_key(api_key):
    candidate_hash = hash_agent_api_key(api_key)
    for agent in Agent.objects.filter(api_key_hash=candidate_hash):
        if hmac.compare_digest(agent.api_key_hash, candidate_hash):
            if not agent.can_connect:
                raise AgentAPIKeyRejected("Agent is disabled or revoked.")
            return agent
    return None


def workos_client():
    workos_client_class = _dynamic_import_workos()
    return workos_client_class(
        api_key=settings.WORKOS_API_KEY,
        client_id=settings.WORKOS_CLIENT_ID,
        base_url=settings.WORKOS_BASE_URL,
    )


def validate_workos_api_key(api_key):
    return workos_client().api_keys.validate_api_key(value=api_key)


def create_workos_organization_api_key(name, permissions):
    url = urljoin(
        settings.WORKOS_BASE_URL.rstrip("/") + "/",
        f"organizations/{settings.WORKOS_ORGANIZATION_ID}/api_keys",
    )
    response = requests.post(
        url,
        json={"name": name, "permissions": permissions},
        headers={
            "Authorization": f"Bearer {settings.WORKOS_API_KEY}",
            "Content-Type": "application/json",
        },
        timeout=10,
    )
    response.raise_for_status()
    payload = response.json()
    api_key = payload.get("api_key") or {}
    if not api_key.get("id") or not api_key.get("value"):
        raise PermissionDenied("WorkOS did not return a usable API key.")
    return api_key


def build_agent_install_command(request, api_key):
    datamingle_url = request.build_absolute_uri("/").rstrip("/")
    quoted_url = quote(datamingle_url, safe=":/")
    return (
        f"curl -fsSL {quoted_url}/api/v1/agents/install.sh | "
        f'sudo DATAMINGLE_URL="{datamingle_url}" '
        f'DATAMINGLE_AGENT_API_KEY="{api_key}" bash'
    )


def build_agent_config(agent):
    assignments = [
        serialize_assignment(assignment)
        for assignment in agent.assignments.filter(enabled=True).select_related(
            "instance"
        )
    ]
    modules = build_module_configs(agent, assignments)
    tool_artifacts = [
        serialize_tool_artifact(artifact)
        for artifact in AgentToolArtifact.objects.filter(enabled=True)
    ]
    payload = {
        "agent_id": agent.id,
        "revision": agent.desired_config_revision,
        "organization_id": agent.organization_id or settings.WORKOS_ORGANIZATION_ID,
        "assignments": assignments,
        "modules": modules,
        "tool_artifacts": tool_artifacts,
    }
    payload["config_hash"] = config_hash(payload)
    return payload


def serialize_assignment(assignment):
    instance = assignment.instance
    modules = assignment_modules(assignment)
    return {
        "id": assignment.id,
        "instance_id": instance.id,
        "instance_name": instance.instance_name,
        "db_type": instance.db_type,
        "host": instance.host,
        "port": instance.port,
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
        "online_schema_enabled": assignment.online_schema_enabled,
        "logs_enabled": assignment.logs_enabled,
    }


def assignment_modules(assignment):
    modules = set(assignment.modules or [])
    if assignment.instance.db_type == "mysql":
        modules.add("mysql")
    if assignment.metrics_enabled:
        modules.add("metrics")
    if assignment.online_schema_enabled:
        modules.add("online_schema")
    if assignment.logs_enabled:
        modules.add("logs")
    return sorted(modules)


def build_module_configs(agent, assignments):
    configs = []
    for module_name in ("mysql", "metrics", "online_schema", "logs"):
        module_assignments = [
            {
                "id": assignment["id"],
                "instance_id": assignment["instance_id"],
                "modules": assignment["modules"],
            }
            for assignment in assignments
            if module_name in assignment["modules"]
        ]
        configs.append(
            {
                "name": module_name,
                "enabled": bool(module_assignments),
                "revision": agent.desired_config_revision,
                "assignments": module_assignments,
            }
        )
    return configs


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


def dispatch_agent_command(command):
    from api_agents.dispatch import notify_command_available

    if command.status == AgentCommandStatus.QUEUED:
        command.status = AgentCommandStatus.DISPATCHED
        command.dispatched_at = timezone.now()
        command.save(update_fields=["status", "dispatched_at", "update_time"])
        command.append_event("command.dispatched", "Command dispatched to agent.")
    notify_command_available(command)
    return command


def command_capable_assignment_for_instance(instance_id):
    return (
        AgentInstanceAssignment.objects.select_related("agent", "instance")
        .filter(
            instance_id=instance_id,
            enabled=True,
            command_enabled=True,
            agent__enabled=True,
            agent__status=AgentStatus.ONLINE,
        )
        .order_by("-agent__last_seen_at", "agent_id")
        .first()
    )


def dispatch_sql_workflow_to_agent(workflow, user=None, executor=None):
    with transaction.atomic():
        workflow = (
            SqlWorkflow.objects.select_for_update()
            .select_related("instance")
            .get(pk=workflow.pk)
        )
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
            workflow.status = "workflow_executing"
            workflow.save(update_fields=["status"])
            resolve_mailbox_items(workflow, category="execution_needed")
            command = existing_command
        else:
            assignment = command_capable_assignment_for_instance(workflow.instance_id)
            if assignment is None:
                return None

            try:
                content = workflow.sqlworkflowcontent
            except ObjectDoesNotExist as exc:
                raise ValueError(
                    f"SQL workflow {workflow.id} is missing SQL content."
                ) from exc

            command_type = (
                AgentCommandType.SCHEMA_CHANGE
                if workflow.syntax_type == 1
                else AgentCommandType.QUERY_EXECUTE
            )
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
            resolve_mailbox_items(workflow, category="execution_needed")

    if command.status in {
        AgentCommandStatus.QUEUED,
        AgentCommandStatus.DISPATCHED,
    }:
        dispatch_agent_command(command)
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
        workflow.status = "workflow_finish"
        errlevel = 0
        stage_status = "Execute Successfully"
        error_message = message
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
            enabled=True,
            assignments__enabled=True,
            assignments__online_schema_enabled=True,
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
