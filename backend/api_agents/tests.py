import asyncio
import os
from decimal import Decimal
from datetime import datetime, timedelta, timezone as datetime_timezone
from unittest.mock import patch

from asgiref.sync import async_to_sync, sync_to_async
from channels.testing import WebsocketCommunicator
from django.core.exceptions import ValidationError
from django.test import TransactionTestCase, override_settings
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APIRequestFactory, APITestCase

from api_agents.authentication import AgentAPIKeyAuthentication
from api_agents.dispatch import (
    ACTIVE_WEBSOCKET_METADATA_KEY,
    WEBSOCKET_CHANNEL_METADATA_KEY,
    active_agent_channel_name,
    notify_config_changed,
    send_agent_message,
)
from api_agents.models import (
    Agent,
    AgentCommand,
    AgentCommandStatus,
    AgentCommandType,
    AgentInstanceAssignment,
    AgentStatus,
    AgentToolArtifact,
)
from api_agents.services import (
    AgentCommandDispatchError,
    AgentCommandExecutionError,
    AgentAPIKeyRejected,
    agent_api_key_hash,
    authenticate_agent_api_key,
    dispatch_sql_workflow_to_agent,
    dispatch_agent_command,
    filter_agent_runnable_instances,
    issue_agent_api_key,
    resolve_agent_service_endpoint,
    run_agent_command_sync,
)
from api_agents.time import agent_utc_now
from common.utils.const import WorkflowStatus, WorkflowType
from sql.models import (
    DEFAULT_NODE_EXPORTER_COLLECTORS,
    SqlWorkflow,
    SqlWorkflowContent,
    WorkflowAudit,
)
from sql.engines.models import ResultSet
from sql.models import InfrastructureNode, Instance, Users


def create_instance(name="primary"):
    return Instance.objects.create(
        instance_name=name,
        type="master",
        db_type="mysql",
        host="127.0.0.1",
        port=3306,
        user="root",
        password="secret",
    )


def create_sql_workflow(instance, status_value="workflow_review_pass", syntax_type=2):
    if not instance.workflow_enabled:
        instance.workflow_enabled = True
        instance.save(update_fields=["workflow_enabled", "update_time"])
    workflow = SqlWorkflow.objects.create(
        workflow_name="agent workflow",
        team_id=1,
        team_name="Default",
        engineer="agent-admin",
        engineer_display="Agent Admin",
        audit_auth_groups="Default",
        status=status_value,
        instance=instance,
        db_name="test",
        syntax_type=syntax_type,
    )
    SqlWorkflowContent.objects.create(
        workflow=workflow,
        sql_content="select 1",
        review_content="[]",
    )
    WorkflowAudit.objects.create(
        team_id=1,
        team_name="Default",
        workflow_id=workflow.id,
        workflow_type=WorkflowType.SQL_REVIEW,
        workflow_title=workflow.workflow_name,
        audit_auth_groups="Default",
        current_audit="Default",
        next_audit="",
        current_status=WorkflowStatus.PASSED,
        create_user=workflow.engineer,
        create_user_display=workflow.engineer_display,
    )
    return workflow


def assign_agent_api_key(agent, api_key=None):
    api_key = api_key or f"dm_agent_test_key_{agent.id}"
    agent.api_key_hash = agent_api_key_hash(api_key)
    agent.api_key_prefix = api_key[:16]
    agent.workos_api_key_id = ""
    agent.save(
        update_fields=[
            "api_key_hash",
            "api_key_prefix",
            "workos_api_key_id",
            "update_time",
        ]
    )
    return api_key


def mark_agent_websocket(agent, channel_name="agent.test"):
    agent.status = AgentStatus.ONLINE
    agent.metadata = {
        **(agent.metadata or {}),
        ACTIVE_WEBSOCKET_METADATA_KEY: {
            WEBSOCKET_CHANNEL_METADATA_KEY: channel_name,
        },
    }
    agent.save(update_fields=["status", "metadata", "update_time"])
    return agent


class AgentAuthenticationTests(APITestCase):
    def test_non_agent_authorization_scheme_is_ignored(self):
        request = APIRequestFactory().get(
            "/api/v1/agent/me/config/",
            HTTP_AUTHORIZATION="Basic not-agent-credentials",
        )

        self.assertIsNone(AgentAPIKeyAuthentication().authenticate(request))


class AgentModelTests(APITestCase):
    def test_assignment_save_increments_desired_revision(self):
        agent = Agent.objects.create(name="agent-a")
        instance = create_instance()

        AgentInstanceAssignment.objects.create(
            agent=agent,
            instance=instance,
            command_enabled=True,
        )

        agent.refresh_from_db()
        self.assertEqual(agent.desired_config_revision, 2)
        self.assertEqual(agent.config_revisions.count(), 1)

    @patch(
        "api_agents.models.Agent.bump_desired_config_revision",
        side_effect=RuntimeError("revision failed"),
    )
    def test_assignment_save_rolls_back_when_revision_bump_fails(self, _mock_bump):
        agent = Agent.objects.create(name="agent-a")
        instance = create_instance()

        with self.assertRaises(RuntimeError):
            AgentInstanceAssignment.objects.create(
                agent=agent,
                instance=instance,
                command_enabled=True,
            )

        self.assertFalse(
            AgentInstanceAssignment.objects.filter(
                agent=agent,
                instance=instance,
            ).exists()
        )
        agent.refresh_from_db()
        self.assertEqual(agent.desired_config_revision, 1)

    def test_assignment_delete_rolls_back_when_revision_bump_fails(self):
        agent = Agent.objects.create(name="agent-a")
        instance = create_instance()
        assignment = AgentInstanceAssignment.objects.create(
            agent=agent,
            instance=instance,
            command_enabled=True,
        )

        with patch(
            "api_agents.models.Agent.bump_desired_config_revision",
            side_effect=RuntimeError("revision failed"),
        ):
            with self.assertRaises(RuntimeError):
                assignment.delete()

        self.assertTrue(
            AgentInstanceAssignment.objects.filter(
                agent=agent,
                instance=instance,
            ).exists()
        )

    def test_duplicate_command_assignment_is_rejected(self):
        instance = create_instance()
        first = Agent.objects.create(name="agent-a")
        second = Agent.objects.create(name="agent-b")
        AgentInstanceAssignment.objects.create(
            agent=first,
            instance=instance,
            command_enabled=True,
        )

        duplicate = AgentInstanceAssignment(
            agent=second,
            instance=instance,
            command_enabled=True,
        )
        with self.assertRaises(ValidationError):
            duplicate.full_clean()

    def test_agent_command_without_idempotency_key_gets_unique_key(self):
        agent = Agent.objects.create(name="agent-a")
        instance = create_instance()

        first = AgentCommand.objects.create(
            agent=agent,
            instance=instance,
            workflow_type="test",
            workflow_id="workflow-1",
            command_type=AgentCommandType.CONNECTION_TEST,
        )
        second = AgentCommand.objects.create(
            agent=agent,
            instance=instance,
            workflow_type="test",
            workflow_id="workflow-2",
            command_type=AgentCommandType.CONNECTION_TEST,
        )

        self.assertTrue(first.idempotency_key)
        self.assertTrue(second.idempotency_key)
        self.assertNotEqual(first.idempotency_key, second.idempotency_key)

    def test_mark_seen_refreshes_update_time(self):
        agent = Agent.objects.create(name="agent-a")
        old_update_time = timezone.now() - timedelta(days=1)
        Agent.objects.filter(pk=agent.pk).update(update_time=old_update_time)
        agent.refresh_from_db()

        agent.mark_seen(config_revision=None)

        agent.refresh_from_db()
        self.assertGreater(agent.update_time, old_update_time)

    @patch("api_agents.dispatch.active_agent_channel_name", return_value="agent.test")
    @patch("api_agents.dispatch.get_channel_layer")
    def test_send_agent_message_returns_false_when_channel_send_fails(
        self, mock_get_channel_layer, _mock_active_channel_name
    ):
        class FailingChannelLayer:
            async def send(self, channel_name, message):
                raise RuntimeError("channel send failed")

        mock_get_channel_layer.return_value = FailingChannelLayer()

        with self.assertLogs("default", level="ERROR"):
            self.assertFalse(send_agent_message(1, {"type": "agent.test"}))

    def test_enabled_artifact_requires_sha256(self):
        artifact = AgentToolArtifact(
            tool_name=AgentToolArtifact.TOOL_GHOST,
            version="1.1.6",
            platform="linux",
            architecture="amd64",
            download_url="https://example.com/gh-ost",
            enabled=True,
        )

        with self.assertRaises(ValidationError):
            artifact.full_clean()


class InstanceAssignmentSignalTransactionTests(TransactionTestCase):
    @patch("api_agents.signals.sync_node_assignments_for_instance")
    def test_inventory_only_save_does_not_resync_assignments(self, mock_sync):
        instance = create_instance()
        mock_sync.reset_mock()

        instance.inventory_status = Instance.INVENTORY_STATUS_FAILED
        instance.save(update_fields=["inventory_status", "inventory_last_attempt_at"])

        mock_sync.assert_not_called()

    def test_instance_save_can_sync_assignments_outside_request_transaction(self):
        node = InfrastructureNode.objects.create(name="node-a")
        agent = Agent.objects.create(name="agent-a", local_node=node)
        instance = create_instance()

        instance.node = node
        instance.save(update_fields=["node", "update_time"])

        self.assertTrue(
            AgentInstanceAssignment.objects.filter(
                agent=agent,
                instance=instance,
                local_node=node,
            ).exists()
        )
        agent.refresh_from_db()
        self.assertEqual(agent.desired_config_revision, 2)


class AgentApiTests(APITestCase):
    def setUp(self):
        self.user = Users.objects.create_user(
            username="agent-admin",
            email="agent-admin@example.com",
            is_active=True,
            is_staff=True,
            is_superuser=True,
        )
        self.client.force_authenticate(user=self.user)

    def test_create_and_list_agent(self):
        response = self.client.post(
            "/api/v1/agents/",
            {
                "node_name": "prod-db-node-01",
                "organization_id": "org_evil",
                "monitoring_enabled": False,
                "monitoring_collectors": ["cpu", "meminfo"],
            },
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        payload = response.json()["data"]
        self.assertEqual(payload["name"], "prod-db-node-01")
        self.assertEqual(payload["display_name"], "prod-db-node-01 Agent")
        self.assertTrue(payload["api_key"].startswith("dm_agent_"))
        self.assertEqual(payload["api_key_backend"], "django")
        self.assertIn("DATAMINGLE_AGENT_API_KEY", payload["install_command"])
        agent = Agent.objects.get(name="prod-db-node-01")
        self.assertNotEqual(agent.organization_id, "org_evil")
        self.assertEqual(agent.organization_id, "datamingle")
        self.assertEqual(agent.workos_api_key_id, "")
        self.assertEqual(agent.api_key_prefix, payload["api_key"][:16])
        self.assertEqual(agent.api_key_hash, agent_api_key_hash(payload["api_key"]))
        self.assertNotIn(payload["api_key"], agent.api_key_hash)
        self.assertEqual(agent.local_node.name, "prod-db-node-01")
        self.assertEqual(agent.local_node.address, "")
        self.assertFalse(agent.local_node.monitoring_enabled)
        self.assertEqual(agent.local_node.monitoring_collectors, ["cpu", "meminfo"])
        self.assertEqual(
            agent.local_node.metadata["provisioning_status"],
            "pending_agent_install",
        )

        list_response = self.client.get("/api/v1/agents/")
        self.assertEqual(list_response.status_code, status.HTTP_200_OK)
        self.assertEqual(list_response.json()["data"]["count"], 1)

    def test_issue_install_key_rotates_django_key(self):
        agent = Agent.objects.create(
            name="agent-a",
            organization_id="org_test_123",
            workos_api_key_id="api_key_legacy",
        )
        old_key = assign_agent_api_key(agent, "dm_agent_old_key")

        response = self.client.post(f"/api/v1/agents/{agent.id}/install-key/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response.json()["data"]
        self.assertTrue(payload["api_key"].startswith("dm_agent_"))
        self.assertNotEqual(payload["api_key"], old_key)
        self.assertEqual(payload["api_key_backend"], "django")
        self.assertIn("DATAMINGLE_AGENT_API_KEY", payload["install_command"])
        agent.refresh_from_db()
        self.assertEqual(agent.workos_api_key_id, "")
        self.assertEqual(agent.api_key_hash, agent_api_key_hash(payload["api_key"]))
        self.assertIsNone(authenticate_agent_api_key(old_key))
        self.assertEqual(authenticate_agent_api_key(payload["api_key"]), agent)

    def test_replace_assignments_increments_revision_once(self):
        agent = Agent.objects.create(name="agent-a")
        instance = create_instance()

        response = self.client.put(
            f"/api/v1/agents/{agent.id}/assignments/",
            {
                "assignments": [
                    {
                        "instance": instance.id,
                        "enabled": True,
                        "modules": ["mysql", "metrics"],
                        "command_enabled": True,
                        "metrics_enabled": True,
                    }
                ]
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        agent.refresh_from_db()
        self.assertEqual(agent.desired_config_revision, 2)
        self.assertEqual(agent.assignments.count(), 1)

    def test_browser_can_list_detail_and_cancel_agent_commands(self):
        agent = Agent.objects.create(name="agent-a")
        instance = create_instance()
        command = AgentCommand.objects.create(
            agent=agent,
            instance=instance,
            workflow_type="sql_workflow",
            workflow_id="42",
            command_type=AgentCommandType.QUERY_EXECUTE,
            status=AgentCommandStatus.RUNNING,
            payload={"sql": "select 1"},
        )
        command.append_event("command.started", "started")

        list_response = self.client.get(f"/api/v1/agents/{agent.id}/commands/")
        self.assertEqual(list_response.status_code, status.HTTP_200_OK)
        list_payload = list_response.json()["data"]
        self.assertEqual(list_payload["count"], 1)
        self.assertEqual(list_payload["results"][0]["id"], command.id)

        detail_response = self.client.get(
            f"/api/v1/agents/{agent.id}/commands/{command.id}/"
        )
        self.assertEqual(detail_response.status_code, status.HTTP_200_OK)
        detail_payload = detail_response.json()["data"]
        self.assertEqual(detail_payload["payload"]["sql"], "select 1")
        self.assertEqual(detail_payload["events"][0]["event_type"], "command.started")

        cancel_response = self.client.post(
            f"/api/v1/agents/{agent.id}/commands/{command.id}/cancel/"
        )
        self.assertEqual(cancel_response.status_code, status.HTTP_200_OK)
        command.refresh_from_db()
        self.assertIsNotNone(command.cancel_requested_at)
        self.assertTrue(
            command.events.filter(event_type="command.cancel_requested").exists()
        )

    def test_tool_artifact_change_bumps_workflow_enabled_agent_revision(self):
        agent = Agent.objects.create(name="agent-a")
        instance = create_instance()
        instance.workflow_enabled = True
        instance.save(update_fields=["workflow_enabled", "update_time"])
        AgentInstanceAssignment.objects.create(
            agent=agent,
            instance=instance,
            command_enabled=True,
        )
        agent.refresh_from_db()
        self.assertEqual(agent.desired_config_revision, 2)

        response = self.client.post(
            "/api/v1/agents/tool-artifacts/",
            {
                "tool_name": AgentToolArtifact.TOOL_GHOST,
                "version": "1.1.6",
                "platform": "linux",
                "architecture": "amd64",
                "download_url": "https://example.com/gh-ost",
                "sha256": "d459a6c4b0867e9f665a7db35f4387d11fa7fa79a00a85c2c172ba0fa4295c14",
                "size_bytes": 10,
                "enabled": True,
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        agent.refresh_from_db()
        self.assertEqual(agent.desired_config_revision, 3)

    def test_tool_artifact_change_skips_legacy_online_schema_assignment(self):
        agent = Agent.objects.create(name="agent-a")
        instance = create_instance()
        AgentInstanceAssignment.objects.create(
            agent=agent,
            instance=instance,
            online_schema_enabled=True,
            metrics_enabled=False,
        )
        agent.refresh_from_db()
        self.assertEqual(agent.desired_config_revision, 2)

        response = self.client.post(
            "/api/v1/agents/tool-artifacts/",
            {
                "tool_name": AgentToolArtifact.TOOL_GHOST,
                "version": "1.1.6",
                "platform": "linux",
                "architecture": "amd64",
                "download_url": "https://example.com/gh-ost",
                "sha256": "d459a6c4b0867e9f665a7db35f4387d11fa7fa79a00a85c2c172ba0fa4295c14",
                "size_bytes": 10,
                "enabled": True,
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        agent.refresh_from_db()
        self.assertEqual(agent.desired_config_revision, 2)


class DjangoAgentAPIKeyTests(APITestCase):
    def test_issues_django_agent_api_key(self):
        agent = Agent.objects.create(name="agent-a")

        issued = issue_agent_api_key(agent)

        self.assertTrue(issued.value.startswith("dm_agent_"))
        self.assertEqual(issued.backend, "django")
        agent.refresh_from_db()
        self.assertEqual(agent.workos_api_key_id, "")
        self.assertEqual(agent.api_key_prefix, issued.value[:16])
        self.assertEqual(agent.api_key_hash, agent_api_key_hash(issued.value))

    def test_validates_django_key_hash_against_agent_record(self):
        agent = Agent.objects.create(name="agent-a")
        api_key = assign_agent_api_key(agent, "dm_agent_value")

        authenticated = authenticate_agent_api_key(api_key)

        self.assertEqual(authenticated, agent)

    def test_invalid_key_returns_none(self):
        Agent.objects.create(name="agent-a")

        self.assertIsNone(authenticate_agent_api_key("dm_agent_missing"))

    def test_disabled_agent_key_is_rejected(self):
        agent = Agent.objects.create(name="agent-a", enabled=False)
        api_key = assign_agent_api_key(agent, "dm_agent_disabled")
        with self.assertRaises(AgentAPIKeyRejected):
            authenticate_agent_api_key(api_key)


class AgentFacingApiTests(APITestCase):
    def authenticate_agent(self, agent):
        api_key = assign_agent_api_key(agent)
        self.client.credentials(HTTP_AUTHORIZATION=f"Bearer {api_key}")
        return api_key

    def create_seeded_local_demo_assignment(self):
        agent = Agent.objects.create(
            name="notebook-ubuntu",
            status=AgentStatus.ONLINE,
            metadata={"seeded": True},
        )
        mark_agent_websocket(agent, channel_name="e2e.demo.mysql.agent")
        instance = create_instance("demo-mysql-workflow")
        AgentInstanceAssignment.objects.create(
            agent=agent,
            instance=instance,
            command_enabled=True,
        )
        return instance

    def test_register_binds_install_id_and_marks_agent_online(self):
        node = InfrastructureNode.objects.create(name="db-node-01", address="")
        agent = Agent.objects.create(name="agent-a", local_node=node)
        self.authenticate_agent(agent)
        before = datetime.now(datetime_timezone.utc).replace(tzinfo=None)

        response = self.client.post(
            "/api/v1/agent/register/",
            {
                "install_id": "ins_test_123",
                "name": "agent-a",
                "address": "10.0.0.12",
                "hostname": "db-host-01",
                "platform": "linux",
                "architecture": "amd64",
                "agent_version": "0.1.0",
                "config_revision": 0,
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        after = datetime.now(datetime_timezone.utc).replace(tzinfo=None)
        self.assertEqual(response.json()["agent_id"], agent.id)
        agent.refresh_from_db()
        self.assertEqual(agent.install_id, "ins_test_123")
        self.assertEqual(agent.status, AgentStatus.ONLINE)
        self.assertEqual(agent.hostname, "db-host-01")
        self.assertGreaterEqual(agent.last_seen_at, before - timedelta(seconds=1))
        self.assertLessEqual(agent.last_seen_at, after + timedelta(seconds=1))
        node.refresh_from_db()
        self.assertEqual(node.address, "10.0.0.12")
        self.assertEqual(node.metadata["provisioning_status"], "agent_registered")
        self.assertEqual(node.metadata["agent_host"]["hostname"], "db-host-01")
        self.assertTrue(node.metadata["agent_host"]["last_registered_at"].endswith("Z"))

    def test_register_does_not_clear_existing_optional_metadata(self):
        agent = Agent.objects.create(
            name="agent-a",
            install_id="ins_test_123",
            hostname="db-host-01",
            platform="linux",
            architecture="amd64",
            agent_version="0.1.0",
        )
        self.authenticate_agent(agent)

        response = self.client.post(
            "/api/v1/agent/register/",
            {"install_id": "ins_test_123", "config_revision": 1},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        agent.refresh_from_db()
        self.assertEqual(agent.hostname, "db-host-01")
        self.assertEqual(agent.platform, "linux")
        self.assertEqual(agent.architecture, "amd64")
        self.assertEqual(agent.agent_version, "0.1.0")

    def test_disabled_agent_cannot_authenticate(self):
        agent = Agent.objects.create(name="agent-a", enabled=False)
        self.authenticate_agent(agent)

        response = self.client.get("/api/v1/agent/me/config/")

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_config_includes_only_authenticated_agent_assignments_with_credentials(
        self,
    ):
        node = InfrastructureNode.objects.create(
            name="node-a",
            address="10.0.0.10",
            monitoring_enabled=False,
            metadata={
                "agent_service_endpoints": {
                    "primary": {
                        "host": "127.0.0.1",
                        "port": 3307,
                    },
                },
            },
        )
        agent = Agent.objects.create(name="agent-a", local_node=node)
        other_agent = Agent.objects.create(name="agent-b")
        instance = create_instance("primary")
        instance.host = "mysql_demo"
        instance.port = 3306
        instance.node = node
        instance.workflow_enabled = True
        instance.mysql_topology_role = Instance.MYSQL_ROLE_STANDALONE
        instance.save(
            update_fields=[
                "host",
                "port",
                "node",
                "workflow_enabled",
                "mysql_topology_role",
                "update_time",
            ]
        )
        other_instance = create_instance("secondary")
        assignment = AgentInstanceAssignment.objects.get(agent=agent, instance=instance)
        assignment.command_enabled = True
        assignment.metrics_enabled = True
        assignment.save(update_fields=["command_enabled", "metrics_enabled"])
        AgentInstanceAssignment.objects.create(
            agent=other_agent,
            instance=other_instance,
            command_enabled=False,
            metrics_enabled=True,
        )
        AgentToolArtifact.objects.create(
            tool_name=AgentToolArtifact.TOOL_NODE_EXPORTER,
            version="1.9.1",
            platform="linux",
            architecture="amd64",
            download_url="https://example.com/node_exporter",
            sha256="d459a6c4b0867e9f665a7db35f4387d11fa7fa79a00a85c2c172ba0fa4295c14",
            enabled=True,
        )
        AgentToolArtifact.objects.create(
            tool_name=AgentToolArtifact.TOOL_MYSQLD_EXPORTER,
            version="0.19.0",
            platform="linux",
            architecture="amd64",
            download_url="https://example.com/mysqld_exporter",
            sha256="d459a6c4b0867e9f665a7db35f4387d11fa7fa79a00a85c2c172ba0fa4295c14",
            enabled=True,
        )
        self.authenticate_agent(agent)

        response = self.client.get("/api/v1/agent/me/config/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response.json()
        self.assertEqual(payload["agent_id"], agent.id)
        self.assertEqual(payload["node"]["id"], node.id)
        self.assertFalse(payload["node"]["monitoring_enabled"])
        self.assertEqual(
            payload["node"]["monitoring_collectors"],
            list(DEFAULT_NODE_EXPORTER_COLLECTORS),
        )
        self.assertEqual(payload["nodes"][0]["id"], node.id)
        self.assertFalse(payload["nodes"][0]["monitoring_enabled"])
        self.assertEqual(
            payload["nodes"][0]["monitoring_collectors"],
            list(DEFAULT_NODE_EXPORTER_COLLECTORS),
        )
        self.assertEqual(len(payload["assignments"]), 1)
        assignment = payload["assignments"][0]
        self.assertEqual(assignment["instance_id"], instance.id)
        self.assertEqual(assignment["node_id"], node.id)
        self.assertTrue(assignment["workflow_enabled"])
        self.assertTrue(assignment["online_schema_enabled"])
        self.assertIn("online_schema", assignment["modules"])
        self.assertFalse(assignment["node_monitoring_enabled"])
        self.assertTrue(assignment["service_monitoring_enabled"])
        self.assertEqual(
            assignment["service_monitoring_collectors"],
            ["global_status", "global_variables", "slave_status"],
        )
        self.assertEqual(assignment["host"], "127.0.0.1")
        self.assertEqual(assignment["port"], 3307)
        self.assertEqual(
            assignment["node_monitoring_collectors"],
            list(DEFAULT_NODE_EXPORTER_COLLECTORS),
        )
        self.assertEqual(assignment["username"], "root")
        self.assertEqual(assignment["password"], "secret")
        self.assertNotIn(other_instance.instance_name, str(payload))
        module_names = {module["name"]: module for module in payload["modules"]}
        self.assertTrue(module_names["mysql"]["enabled"])
        self.assertTrue(module_names["metrics"]["enabled"])
        self.assertTrue(module_names["online_schema"]["enabled"])
        self.assertFalse(module_names["node_monitoring"]["enabled"])
        self.assertTrue(module_names["service_monitoring"]["enabled"])
        self.assertEqual(
            module_names["node_monitoring"]["raw"]["remote_write_url"],
            "http://testserver/api/v1/prometheus/write",
        )
        self.assertEqual(
            module_names["node_monitoring"]["raw"]["node_exporter"]["artifact"][
                "tool_name"
            ],
            AgentToolArtifact.TOOL_NODE_EXPORTER,
        )
        self.assertEqual(
            module_names["node_monitoring"]["raw"]["node_exporter"]["collectors"],
            list(DEFAULT_NODE_EXPORTER_COLLECTORS),
        )
        self.assertEqual(
            [
                (profile["name"], profile["interval_seconds"])
                for profile in module_names["node_monitoring"]["raw"]["scrape_profiles"]
            ],
            [("high", 5), ("normal", 30), ("low", 60)],
        )
        self.assertIn(
            "cpu",
            module_names["node_monitoring"]["raw"]["scrape_profiles"][0]["collectors"],
        )
        services = module_names["service_monitoring"]["raw"]["services"]
        self.assertEqual(len(services), 1)
        self.assertEqual(services[0]["db_type"], "mysql")
        self.assertEqual(services[0]["host"], "127.0.0.1")
        self.assertEqual(services[0]["port"], 3307)
        self.assertEqual(services[0]["username"], "root")
        self.assertEqual(services[0]["password"], "secret")
        self.assertEqual(
            services[0]["collectors"],
            ["global_status", "global_variables", "slave_status"],
        )
        self.assertEqual(
            [
                (profile["name"], profile["interval_seconds"])
                for profile in services[0]["scrape_profiles"]
            ],
            [("high", 5), ("normal", 30), ("low", 60)],
        )
        self.assertEqual(
            services[0]["scrape_profiles"][0]["collectors"],
            ["global_status", "slave_status"],
        )
        self.assertEqual(
            services[0]["scrape_profiles"][2]["collectors"],
            ["global_variables"],
        )
        self.assertEqual(services[0]["labels"]["dm_mysql_cluster_role"], "standalone")
        self.assertNotIn("dm_mysql_cluster", services[0]["labels"])
        self.assertEqual(services[0]["exporter"]["listen_address"], "127.0.0.1:9200")
        self.assertEqual(
            services[0]["exporter"]["artifact"]["tool_name"],
            AgentToolArtifact.TOOL_MYSQLD_EXPORTER,
        )

    def test_endpoint_override_ignores_invalid_ports(self):
        node = InfrastructureNode.objects.create(
            name="node-a",
            metadata={
                "agent_service_endpoints": {
                    "primary": {
                        "host": "127.0.0.1",
                        "port": 70000,
                    },
                },
            },
        )
        instance = create_instance("primary")
        instance.host = "mysql_demo"
        instance.port = 3306
        instance.node = node
        instance.save(update_fields=["host", "port", "node", "update_time"])

        self.assertEqual(resolve_agent_service_endpoint(instance), ("127.0.0.1", 3306))

    def test_heartbeat_updates_last_seen_revision_and_module_health(self):
        agent = Agent.objects.create(name="agent-a", install_id="ins_test_123")
        self.authenticate_agent(agent)

        response = self.client.post(
            "/api/v1/agent/me/heartbeat/",
            {
                "install_id": "ins_test_123",
                "status": "online",
                "config_revision": 3,
                "module_health": [
                    {"module": "mysql", "status": "healthy"},
                ],
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.json()["desired_config_revision"], 1)
        agent.refresh_from_db()
        self.assertEqual(agent.last_config_revision, 3)
        self.assertEqual(agent.metadata["module_health"][0]["module"], "mysql")

    @patch("api_agents.dispatch.notify_command_available", return_value=True)
    def test_agent_fetches_and_acks_dispatched_command(self, _mock_notify):
        agent = Agent.objects.create(name="agent-a")
        instance = create_instance()
        command = AgentCommand.objects.create(
            agent=agent,
            instance=instance,
            workflow_type="test",
            workflow_id="workflow-1",
            command_type=AgentCommandType.CONNECTION_TEST,
            payload={"timeout_seconds": 10},
        )
        dispatch_agent_command(command)
        self.authenticate_agent(agent)

        detail_response = self.client.get(f"/api/v1/agent/commands/{command.id}/")
        self.assertEqual(detail_response.status_code, status.HTTP_200_OK)
        self.assertEqual(detail_response.json()["payload"]["timeout_seconds"], 10)

        ack_response = self.client.post(f"/api/v1/agent/commands/{command.id}/ack/")
        self.assertEqual(ack_response.status_code, status.HTTP_200_OK)
        command.refresh_from_db()
        self.assertEqual(command.status, AgentCommandStatus.ACCEPTED)

    def test_agent_reports_command_lifecycle(self):
        agent = Agent.objects.create(name="agent-a")
        instance = create_instance()
        command = AgentCommand.objects.create(
            agent=agent,
            instance=instance,
            workflow_type="test",
            workflow_id="workflow-1",
            command_type=AgentCommandType.CONNECTION_TEST,
            status=AgentCommandStatus.ACCEPTED,
            payload={},
        )
        self.authenticate_agent(agent)

        start_response = self.client.post(
            f"/api/v1/agent/commands/{command.id}/start/",
            {"lease_owner": "worker-1", "lease_seconds": 60},
            format="json",
        )
        self.assertEqual(start_response.status_code, status.HTTP_200_OK)
        command.refresh_from_db()
        self.assertEqual(command.status, AgentCommandStatus.RUNNING)
        self.assertEqual(command.lease_owner, "worker-1")
        self.assertIsNotNone(command.lease_expires_at)

        progress_response = self.client.post(
            f"/api/v1/agent/commands/{command.id}/progress/",
            {
                "lease_owner": "worker-1",
                "message": "connection established",
                "payload": {"phase": "connect"},
            },
            format="json",
        )
        self.assertEqual(progress_response.status_code, status.HTTP_200_OK)
        progress_event = command.events.get(
            event_type="command.progress",
            message="connection established",
        )
        self.assertEqual(progress_event.payload["phase"], "connect")

        finish_response = self.client.post(
            f"/api/v1/agent/commands/{command.id}/finish/",
            {"message": "done", "result": {"ok": True}},
            format="json",
        )
        self.assertEqual(finish_response.status_code, status.HTTP_200_OK)
        command.refresh_from_db()
        self.assertEqual(command.status, AgentCommandStatus.SUCCEEDED)
        self.assertEqual(command.result["ok"], True)
        self.assertEqual(command.lease_owner, "")
        self.assertIsNone(command.lease_expires_at)

    def test_agent_marks_command_cancelled(self):
        agent = Agent.objects.create(name="agent-a")
        instance = create_instance()
        command = AgentCommand.objects.create(
            agent=agent,
            instance=instance,
            workflow_type="test",
            workflow_id="workflow-1",
            command_type=AgentCommandType.CONNECTION_TEST,
            status=AgentCommandStatus.RUNNING,
            payload={},
        )
        self.authenticate_agent(agent)

        response = self.client.post(
            f"/api/v1/agent/commands/{command.id}/cancel/",
            {"message": "cancelled by runner"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        command.refresh_from_db()
        self.assertEqual(command.status, AgentCommandStatus.CANCELLED)
        self.assertTrue(
            command.events.filter(
                event_type="command.cancelled",
                message="cancelled by runner",
            ).exists()
        )

    @patch("api_agents.agent_api.complete_agent_workflow_command")
    def test_terminal_command_finish_is_idempotent(self, mock_complete):
        agent = Agent.objects.create(name="agent-a")
        instance = create_instance()
        command = AgentCommand.objects.create(
            agent=agent,
            instance=instance,
            workflow_type="test",
            workflow_id="workflow-1",
            command_type=AgentCommandType.CONNECTION_TEST,
            status=AgentCommandStatus.SUCCEEDED,
            result={"ok": True},
        )
        self.authenticate_agent(agent)

        response = self.client.post(
            f"/api/v1/agent/commands/{command.id}/finish/",
            {"message": "duplicate", "result": {"ok": False}},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        mock_complete.assert_not_called()
        command.refresh_from_db()
        self.assertEqual(command.result["ok"], True)
        self.assertFalse(command.events.filter(event_type="command.succeeded").exists())

    def test_terminal_command_rejects_progress(self):
        agent = Agent.objects.create(name="agent-a")
        instance = create_instance()
        command = AgentCommand.objects.create(
            agent=agent,
            instance=instance,
            workflow_type="test",
            workflow_id="workflow-1",
            command_type=AgentCommandType.CONNECTION_TEST,
            status=AgentCommandStatus.FAILED,
        )
        self.authenticate_agent(agent)

        response = self.client.post(
            f"/api/v1/agent/commands/{command.id}/progress/",
            {"message": "late"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_409_CONFLICT)
        self.assertFalse(command.events.filter(event_type="command.progress").exists())

    @patch("api_agents.services.emit_execution_finished_notifications")
    @patch("sql.notify.notify_for_execute")
    @patch("api_agents.dispatch.notify_command_available", return_value=True)
    def test_agent_workflow_command_finish_updates_sql_workflow(
        self,
        _mock_notify_command_available,
        _notify_for_execute,
        _emit_execution_finished_notifications,
    ):
        agent = Agent.objects.create(name="agent-a", status=AgentStatus.ONLINE)
        mark_agent_websocket(agent)
        instance = create_instance()
        AgentInstanceAssignment.objects.create(
            agent=agent,
            instance=instance,
            command_enabled=True,
        )
        workflow = create_sql_workflow(instance)

        command = dispatch_sql_workflow_to_agent(workflow)
        self.assertIsNotNone(command)
        workflow.refresh_from_db()
        self.assertEqual(workflow.status, "workflow_executing")

        self.authenticate_agent(agent)
        response = self.client.post(
            f"/api/v1/agent/commands/{command.id}/finish/",
            {"message": "agent finished", "result": {"affected_rows": 1}},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        workflow.refresh_from_db()
        self.assertEqual(workflow.status, "workflow_finish")
        self.assertIn("agent finished", workflow.sqlworkflowcontent.execute_result)

    def test_agent_runnable_instances_require_active_websocket(self):
        offline_instance = create_instance("offline")
        online_instance = create_instance("online")
        offline_agent = Agent.objects.create(
            name="agent-offline", status=AgentStatus.ONLINE
        )
        online_agent = Agent.objects.create(
            name="agent-online", status=AgentStatus.ONLINE
        )
        mark_agent_websocket(online_agent)
        AgentInstanceAssignment.objects.create(
            agent=offline_agent,
            instance=offline_instance,
            command_enabled=True,
        )
        AgentInstanceAssignment.objects.create(
            agent=online_agent,
            instance=online_instance,
            command_enabled=True,
        )

        runnable_ids = set(
            filter_agent_runnable_instances(Instance.objects.all()).values_list(
                "id", flat=True
            )
        )

        self.assertNotIn(offline_instance.id, runnable_ids)
        self.assertIn(online_instance.id, runnable_ids)

    @patch.dict(os.environ, {"RUN_LOCAL_DEMO_SEED": "1"})
    @patch("sql.engines.get_engine")
    @patch("api_agents.services.dispatch_agent_command")
    def test_local_demo_query_command_uses_direct_engine(
        self,
        mock_dispatch_agent_command,
        mock_get_engine,
    ):
        agent = Agent.objects.create(
            name="notebook-ubuntu",
            status=AgentStatus.ONLINE,
            metadata={"seeded": True},
        )
        mark_agent_websocket(agent, channel_name="e2e.demo.mysql.agent")
        instance = create_instance("demo-mysql-workflow")
        AgentInstanceAssignment.objects.create(
            agent=agent,
            instance=instance,
            command_enabled=True,
        )
        mock_get_engine.return_value.query.return_value = ResultSet(
            full_sql="select 1",
            rows=[(1,)],
            column_list=["one"],
            column_type=["LONGLONG"],
            affected_rows=1,
        )

        command = run_agent_command_sync(
            instance=instance,
            command_type=AgentCommandType.QUERY_EXECUTE,
            workflow_type="query",
            workflow_id="local-demo-query",
            payload={
                "db_name": "demo_orders",
                "sql": "select 1",
                "limit": 10,
                "max_execution_time_ms": 5000,
                "submitted_by": "demo_requester",
            },
        )

        self.assertEqual(command.status, AgentCommandStatus.SUCCEEDED)
        self.assertEqual(command.result["rows"], [[1]])
        self.assertEqual(command.result["column_list"], ["one"])
        self.assertEqual(command.result["affected_rows"], 1)
        mock_dispatch_agent_command.assert_not_called()
        mock_get_engine.return_value.query.assert_called_once_with(
            db_name="demo_orders",
            sql="select 1",
            limit_num=10,
            parameters=None,
            max_execution_time=5000,
        )
        mock_get_engine.return_value.close.assert_called_once()

    @patch.dict(os.environ, {"RUN_LOCAL_DEMO_SEED": "1"})
    @patch("sql.engines.get_engine")
    @patch("api_agents.services.dispatch_agent_command")
    def test_local_demo_query_command_serializes_json_unsafe_values(
        self,
        mock_dispatch_agent_command,
        mock_get_engine,
    ):
        instance = self.create_seeded_local_demo_assignment()
        mock_get_engine.return_value.query.return_value = ResultSet(
            full_sql="select created_at, amount, payload",
            rows=[(datetime(2026, 1, 2, 3, 4, 5), Decimal("1.23"), b"hello")],
            column_list=["created_at", "amount", "payload"],
            affected_rows=1,
        )

        command = run_agent_command_sync(
            instance=instance,
            command_type=AgentCommandType.QUERY_EXECUTE,
            workflow_type="query",
            workflow_id="local-demo-query-json",
            payload={
                "db_name": "demo_orders",
                "sql": "select created_at, amount, payload",
                "submitted_by": "demo_requester",
            },
        )

        self.assertEqual(command.status, AgentCommandStatus.SUCCEEDED)
        self.assertEqual(
            command.result["rows"],
            [["2026-01-02T03:04:05", "1.23", "hello"]],
        )
        mock_dispatch_agent_command.assert_not_called()

    @patch.dict(os.environ, {"RUN_LOCAL_DEMO_SEED": "1"})
    @patch("sql.engines.get_engine")
    @patch("api_agents.services.dispatch_agent_command")
    def test_local_demo_query_command_fails_on_result_error(
        self,
        mock_dispatch_agent_command,
        mock_get_engine,
    ):
        instance = self.create_seeded_local_demo_assignment()
        mock_get_engine.return_value.query.return_value = ResultSet(
            full_sql="select broken",
            rows=[],
            column_list=[],
            affected_rows=0,
        )
        mock_get_engine.return_value.query.return_value.error = "query failed"

        with self.assertRaises(AgentCommandExecutionError) as exc_context:
            run_agent_command_sync(
                instance=instance,
                command_type=AgentCommandType.QUERY_EXECUTE,
                workflow_type="query",
                workflow_id="local-demo-query-failed",
                payload={
                    "db_name": "demo_orders",
                    "sql": "select broken",
                    "submitted_by": "demo_requester",
                },
            )

        command = exc_context.exception.command
        self.assertEqual(command.status, AgentCommandStatus.FAILED)
        self.assertEqual(command.error["message"], "query failed")
        self.assertTrue(command.events.filter(event_type="command.failed").exists())
        mock_dispatch_agent_command.assert_not_called()
        mock_get_engine.return_value.close.assert_called_once()

    @patch.dict(os.environ, {"RUN_LOCAL_DEMO_SEED": "1"})
    @patch("sql.engines.get_engine")
    @patch("api_agents.services.dispatch_agent_command")
    def test_local_demo_query_command_fails_on_exception(
        self,
        mock_dispatch_agent_command,
        mock_get_engine,
    ):
        instance = self.create_seeded_local_demo_assignment()
        mock_get_engine.return_value.query.side_effect = RuntimeError(
            "connection failed"
        )

        with self.assertRaises(AgentCommandExecutionError) as exc_context:
            run_agent_command_sync(
                instance=instance,
                command_type=AgentCommandType.QUERY_EXECUTE,
                workflow_type="query",
                workflow_id="local-demo-query-exception",
                payload={
                    "db_name": "demo_orders",
                    "sql": "select 1",
                    "submitted_by": "demo_requester",
                },
            )

        command = exc_context.exception.command
        self.assertEqual(command.status, AgentCommandStatus.FAILED)
        self.assertEqual(command.error["message"], "connection failed")
        self.assertTrue(command.events.filter(event_type="command.failed").exists())
        mock_dispatch_agent_command.assert_not_called()
        mock_get_engine.return_value.close.assert_called_once()

    @patch.dict(os.environ, {"RUN_LOCAL_DEMO_SEED": "1"})
    @patch("api_agents.services.dispatch_agent_command")
    def test_local_demo_workflow_check_command_completes_directly(
        self,
        mock_dispatch_agent_command,
    ):
        agent = Agent.objects.create(
            name="notebook-ubuntu",
            status=AgentStatus.ONLINE,
            metadata={"seeded": True},
        )
        mark_agent_websocket(agent, channel_name="e2e.demo.mysql.agent")
        instance = create_instance("demo-mysql-workflow")
        AgentInstanceAssignment.objects.create(
            agent=agent,
            instance=instance,
            command_enabled=True,
        )

        command = run_agent_command_sync(
            instance=instance,
            command_type=AgentCommandType.WORKFLOW_CHECK,
            workflow_type="workflow.check",
            workflow_id="local-demo-workflow-check",
            payload={
                "db_name": "demo_orders",
                "sql": "ALTER TABLE customers ADD COLUMN demo_col varchar(16);",
                "submitted_by": "demo_requester",
            },
        )

        self.assertEqual(command.status, AgentCommandStatus.SUCCEEDED)
        self.assertEqual(command.result["syntax_type"], 1)
        self.assertEqual(command.result["error_count"], 0)
        self.assertEqual(command.result["rows"][0]["stagestatus"], "Audit completed")
        mock_dispatch_agent_command.assert_not_called()

    @patch.dict(os.environ, {"RUN_LOCAL_DEMO_SEED": "1"})
    @patch("sql.engines.get_engine")
    @patch(
        "api_agents.services.dispatch_agent_command",
        side_effect=AgentCommandDispatchError("agent unavailable"),
    )
    def test_local_demo_export_check_requires_agent_dispatch(
        self,
        mock_dispatch_agent_command,
        mock_get_engine,
    ):
        agent = Agent.objects.create(
            name="notebook-ubuntu",
            status=AgentStatus.ONLINE,
            metadata={"seeded": True},
        )
        mark_agent_websocket(agent, channel_name="e2e.demo.mysql.agent")
        instance = create_instance("demo-mysql-workflow")
        AgentInstanceAssignment.objects.create(
            agent=agent,
            instance=instance,
            command_enabled=True,
        )
        with self.assertRaises(AgentCommandDispatchError):
            run_agent_command_sync(
                instance=instance,
                command_type=AgentCommandType.EXPORT_CHECK,
                workflow_type="export.check",
                workflow_id="local-demo-export-check",
                payload={
                    "db_name": "demo_billing",
                    "sql": "SELECT invoice_number FROM invoices;",
                    "submitted_by": "demo_requester",
                },
            )
        mock_dispatch_agent_command.assert_called_once()
        mock_get_engine.assert_not_called()

    @patch.dict(os.environ, {"RUN_LOCAL_DEMO_SEED": "1"})
    @patch(
        "api_agents.services._local_demo_workflow_review_result",
        side_effect=RuntimeError("review failed"),
    )
    @patch("api_agents.services.dispatch_agent_command")
    def test_local_demo_workflow_check_command_fails_terminally_on_review_exception(
        self,
        mock_dispatch_agent_command,
        _mock_review_result,
    ):
        instance = self.create_seeded_local_demo_assignment()

        with self.assertRaises(AgentCommandExecutionError) as exc_context:
            run_agent_command_sync(
                instance=instance,
                command_type=AgentCommandType.WORKFLOW_CHECK,
                workflow_type="workflow.check",
                workflow_id="local-demo-workflow-check-failed",
                payload={
                    "db_name": "demo_orders",
                    "sql": "ALTER TABLE customers ADD COLUMN demo_col varchar(16);",
                    "submitted_by": "demo_requester",
                },
            )

        command = exc_context.exception.command
        self.assertEqual(command.status, AgentCommandStatus.FAILED)
        self.assertEqual(command.error["message"], "review failed")
        self.assertTrue(command.events.filter(event_type="command.failed").exists())
        mock_dispatch_agent_command.assert_not_called()

    def test_dispatch_sql_workflow_requires_active_websocket(self):
        agent = Agent.objects.create(name="agent-a", status=AgentStatus.ONLINE)
        instance = create_instance()
        AgentInstanceAssignment.objects.create(
            agent=agent,
            instance=instance,
            command_enabled=True,
        )
        workflow = create_sql_workflow(instance)

        with self.assertRaises(AgentCommandDispatchError):
            dispatch_sql_workflow_to_agent(workflow)


@override_settings(
    CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}},
)
class AgentWebsocketTests(TransactionTestCase):
    reset_sequences = True

    def authenticate_agent(self, agent):
        return assign_agent_api_key(agent)

    def test_websocket_receives_config_changed_notification(self):
        agent = Agent.objects.create(name="agent-a", desired_config_revision=4)
        api_key = self.authenticate_agent(agent)
        async_to_sync(self._websocket_receives_config_changed_notification)(
            agent, api_key
        )

    async def _websocket_receives_config_changed_notification(self, agent, api_key):
        from archery.asgi import application

        communicator = WebsocketCommunicator(
            application,
            "/api/ws/agent/",
            headers=[(b"authorization", f"Bearer {api_key}".encode("utf-8"))],
        )

        connected, _ = await communicator.connect()
        self.assertTrue(connected)
        hello = await communicator.receive_json_from()
        self.assertEqual(hello["type"], "hello.ack")
        channel_name = await sync_to_async(active_agent_channel_name)(agent.id)
        self.assertTrue(channel_name)

        await sync_to_async(notify_config_changed)(agent, reason="assignment.updated")
        message = await communicator.receive_json_from()
        self.assertEqual(message["type"], "config.changed")
        self.assertEqual(message["revision"], 4)
        self.assertEqual(message["reason"], "assignment.updated")

        await communicator.disconnect()
        await sync_to_async(agent.refresh_from_db)()
        self.assertEqual(agent.status, AgentStatus.OFFLINE)
        self.assertNotIn(ACTIVE_WEBSOCKET_METADATA_KEY, agent.metadata)

    def test_websocket_pong_updates_heartbeat_time(self):
        agent = Agent.objects.create(name="agent-a")
        api_key = self.authenticate_agent(agent)
        async_to_sync(self._websocket_pong_updates_heartbeat_time)(agent, api_key)

    async def _websocket_pong_updates_heartbeat_time(self, agent, api_key):
        from archery.asgi import application

        communicator = WebsocketCommunicator(
            application,
            "/api/ws/agent/",
            headers=[(b"authorization", f"Bearer {api_key}".encode("utf-8"))],
        )

        connected, _ = await communicator.connect()
        self.assertTrue(connected)
        hello = await communicator.receive_json_from()
        self.assertEqual(hello["type"], "hello.ack")
        self.assertEqual(hello["heartbeat_interval"], 30)

        before = agent_utc_now()
        await communicator.send_json_to(
            {"type": "pong", "sent_at": "2026-06-03T20:00:00Z"}
        )
        after = agent_utc_now()

        for _ in range(20):
            await sync_to_async(agent.refresh_from_db)()
            if agent.last_websocket_pong_at is not None:
                break
            await asyncio.sleep(0.05)
        self.assertGreaterEqual(
            agent.last_websocket_pong_at, before - timedelta(seconds=1)
        )
        self.assertLessEqual(agent.last_websocket_pong_at, after + timedelta(seconds=1))
        self.assertEqual(agent.metadata["last_pong"]["sent_at"], "2026-06-03T20:00:00Z")
        self.assertTrue(agent.metadata["last_pong"]["received_at"].endswith("Z"))
        self.assertEqual(agent.last_seen_at, agent.last_websocket_pong_at)

        await communicator.disconnect()

    def test_websocket_receives_command_available_notification(self):
        agent = Agent.objects.create(name="agent-a")
        api_key = self.authenticate_agent(agent)
        instance = create_instance()
        command = AgentCommand.objects.create(
            agent=agent,
            instance=instance,
            workflow_type="test",
            workflow_id="workflow-1",
            command_type=AgentCommandType.CONNECTION_TEST,
            payload={},
        )
        async_to_sync(self._websocket_receives_command_available_notification)(
            api_key, command
        )

    async def _websocket_receives_command_available_notification(
        self, api_key, command
    ):
        from archery.asgi import application

        communicator = WebsocketCommunicator(
            application,
            "/api/ws/agent/",
            headers=[(b"authorization", f"Bearer {api_key}".encode("utf-8"))],
        )

        connected, _ = await communicator.connect()
        self.assertTrue(connected)
        await communicator.receive_json_from()

        await sync_to_async(dispatch_agent_command)(command)
        message = await communicator.receive_json_from()
        self.assertEqual(message["type"], "command.available")
        self.assertEqual(message["command_id"], command.id)
        self.assertEqual(message["command_type"], AgentCommandType.CONNECTION_TEST)

        await communicator.disconnect()

    def test_websocket_dispatch_targets_only_the_agent_channel(self):
        agent_a = Agent.objects.create(name="agent-a", desired_config_revision=7)
        agent_b = Agent.objects.create(name="agent-b", desired_config_revision=8)
        api_key_a = self.authenticate_agent(agent_a)
        api_key_b = self.authenticate_agent(agent_b)
        async_to_sync(self._websocket_dispatch_targets_only_the_agent_channel)(
            agent_a,
            agent_b,
            api_key_a,
            api_key_b,
        )

    async def _websocket_dispatch_targets_only_the_agent_channel(
        self, agent_a, agent_b, api_key_a, api_key_b
    ):
        from archery.asgi import application

        communicator_a = WebsocketCommunicator(
            application,
            "/api/ws/agent/",
            headers=[(b"authorization", f"Bearer {api_key_a}".encode("utf-8"))],
        )
        communicator_b = WebsocketCommunicator(
            application,
            "/api/ws/agent/",
            headers=[(b"authorization", f"Bearer {api_key_b}".encode("utf-8"))],
        )

        connected_a, _ = await communicator_a.connect()
        connected_b, _ = await communicator_b.connect()
        self.assertTrue(connected_a)
        self.assertTrue(connected_b)
        await communicator_a.receive_json_from()
        await communicator_b.receive_json_from()

        channel_a = await sync_to_async(active_agent_channel_name)(agent_a.id)
        channel_b = await sync_to_async(active_agent_channel_name)(agent_b.id)
        self.assertTrue(channel_a)
        self.assertTrue(channel_b)
        self.assertNotEqual(channel_a, channel_b)

        await sync_to_async(notify_config_changed)(agent_a, reason="agent-a-only")
        message = await communicator_a.receive_json_from()
        self.assertEqual(message["type"], "config.changed")
        self.assertEqual(message["revision"], 7)
        self.assertEqual(message["reason"], "agent-a-only")
        self.assertTrue(await communicator_b.receive_nothing(timeout=0.05))

        await communicator_a.disconnect()
        await communicator_b.disconnect()
