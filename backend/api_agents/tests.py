from datetime import timedelta
from types import SimpleNamespace
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
    REQUIRED_AGENT_KEY_PERMISSIONS,
    AgentAPIKeyRejected,
    authenticate_agent_api_key,
    create_agent_api_key,
    dispatch_sql_workflow_to_agent,
    dispatch_agent_command,
    issue_agent_api_key,
)
from common.utils.const import WorkflowStatus, WorkflowType
from sql.models import SqlWorkflow, SqlWorkflowContent, WorkflowAudit
from sql.models import Instance, Users


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
    workflow = SqlWorkflow.objects.create(
        workflow_name="agent workflow",
        group_id=1,
        group_name="Default",
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
        group_id=1,
        group_name="Default",
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
        agent.refresh_from_db()
        self.assertEqual(agent.desired_config_revision, 2)

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


@override_settings(DATAMINGLE_AGENT_API_KEY_BACKEND="local")
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
                "name": "prod-agent-01",
                "display_name": "Production Agent",
                "organization_id": "org_evil",
            },
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        payload = response.json()["data"]
        self.assertEqual(payload["name"], "prod-agent-01")
        self.assertTrue(payload["api_key"].startswith("dma_"))
        self.assertEqual(payload["api_key_backend"], "local")
        self.assertIn("DATAMINGLE_AGENT_API_KEY", payload["install_command"])
        agent = Agent.objects.get(name="prod-agent-01")
        self.assertNotEqual(agent.organization_id, "org_evil")
        self.assertEqual(agent.api_key_prefix, payload["api_key"][:16])
        self.assertNotEqual(agent.api_key_hash, payload["api_key"])

        list_response = self.client.get("/api/v1/agents/")
        self.assertEqual(list_response.status_code, status.HTTP_200_OK)
        self.assertEqual(list_response.json()["data"]["count"], 1)

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

    def test_tool_artifact_change_bumps_online_schema_agent_revision(self):
        agent = Agent.objects.create(name="agent-a")
        instance = create_instance()
        AgentInstanceAssignment.objects.create(
            agent=agent,
            instance=instance,
            online_schema_enabled=True,
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


@override_settings(
    DATAMINGLE_AGENT_API_KEY_BACKEND="workos",
    WORKOS_API_KEY="sk_test_123",
    WORKOS_CLIENT_ID="client_test_123",
    WORKOS_ORGANIZATION_ID="org_test_123",
    WORKOS_BASE_URL="https://api.workos.test/",
)
class WorkOSAgentAPIKeyTests(APITestCase):
    @patch("api_agents.services.requests.post")
    def test_issues_workos_organization_api_key(self, mock_post):
        mock_post.return_value.json.return_value = {
            "api_key": {
                "object": "api_key",
                "id": "api_key_123",
                "owner": {"type": "organization", "id": "org_test_123"},
                "name": "Datamingle Agent: agent-a",
                "value": "sk_agent_created_once",
                "obfuscated_value": "sk_...once",
                "permissions": list(REQUIRED_AGENT_KEY_PERMISSIONS),
            }
        }
        mock_post.return_value.raise_for_status.return_value = None
        agent = Agent.objects.create(name="agent-a")

        issued = issue_agent_api_key(agent)

        self.assertEqual(issued.value, "sk_agent_created_once")
        self.assertEqual(issued.backend, "workos")
        agent.refresh_from_db()
        self.assertEqual(agent.workos_api_key_id, "api_key_123")
        self.assertEqual(agent.api_key_prefix, "sk_...once")
        self.assertIsNone(agent.api_key_hash)
        mock_post.assert_called_once()
        self.assertEqual(
            mock_post.call_args.kwargs["json"]["permissions"],
            list(REQUIRED_AGENT_KEY_PERMISSIONS),
        )

    @patch("api_agents.services.validate_workos_api_key")
    def test_validates_workos_key_owner_permissions_and_agent_record(
        self, mock_validate
    ):
        agent = Agent.objects.create(
            name="agent-a",
            workos_api_key_id="api_key_123",
        )
        mock_validate.return_value = SimpleNamespace(
            id="api_key_123",
            owner=SimpleNamespace(type="organization", id="org_test_123"),
            permissions=list(REQUIRED_AGENT_KEY_PERMISSIONS),
        )

        authenticated = authenticate_agent_api_key("sk_agent_value")

        self.assertEqual(authenticated, agent)

    @patch("api_agents.services.validate_workos_api_key")
    def test_rejects_workos_key_missing_agent_permissions(self, mock_validate):
        mock_validate.return_value = SimpleNamespace(
            id="api_key_123",
            owner=SimpleNamespace(type="organization", id="org_test_123"),
            permissions=["datamingle-agent:connect"],
        )

        with self.assertRaises(AgentAPIKeyRejected):
            authenticate_agent_api_key("sk_agent_value")


@override_settings(DATAMINGLE_AGENT_API_KEY_BACKEND="local")
class AgentFacingApiTests(APITestCase):
    def authenticate_agent(self, agent):
        issued_key = create_agent_api_key(agent)
        self.client.credentials(HTTP_AUTHORIZATION=f"Bearer {issued_key.value}")
        return issued_key.value

    def test_register_binds_install_id_and_marks_agent_online(self):
        agent = Agent.objects.create(name="agent-a")
        self.authenticate_agent(agent)

        response = self.client.post(
            "/api/v1/agent/register/",
            {
                "install_id": "ins_test_123",
                "name": "agent-a",
                "hostname": "db-host-01",
                "platform": "linux",
                "architecture": "amd64",
                "agent_version": "0.1.0",
                "config_revision": 0,
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.json()["agent_id"], agent.id)
        agent.refresh_from_db()
        self.assertEqual(agent.install_id, "ins_test_123")
        self.assertEqual(agent.status, AgentStatus.ONLINE)
        self.assertEqual(agent.hostname, "db-host-01")

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
        agent = Agent.objects.create(name="agent-a")
        other_agent = Agent.objects.create(name="agent-b")
        instance = create_instance("primary")
        other_instance = create_instance("secondary")
        AgentInstanceAssignment.objects.create(
            agent=agent,
            instance=instance,
            command_enabled=True,
            metrics_enabled=True,
        )
        AgentInstanceAssignment.objects.create(
            agent=other_agent,
            instance=other_instance,
            command_enabled=False,
            metrics_enabled=True,
        )
        self.authenticate_agent(agent)

        response = self.client.get("/api/v1/agent/me/config/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response.json()
        self.assertEqual(payload["agent_id"], agent.id)
        self.assertEqual(len(payload["assignments"]), 1)
        assignment = payload["assignments"][0]
        self.assertEqual(assignment["instance_id"], instance.id)
        self.assertEqual(assignment["username"], "root")
        self.assertEqual(assignment["password"], "secret")
        self.assertNotIn(other_instance.instance_name, str(payload))
        module_names = {module["name"]: module for module in payload["modules"]}
        self.assertTrue(module_names["mysql"]["enabled"])
        self.assertTrue(module_names["metrics"]["enabled"])

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

    def test_agent_fetches_and_acks_dispatched_command(self):
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
    @patch("api_agents.services.notify_for_execute")
    def test_agent_workflow_command_finish_updates_sql_workflow(
        self, _notify_for_execute, _emit_execution_finished_notifications
    ):
        agent = Agent.objects.create(name="agent-a", status=AgentStatus.ONLINE)
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


@override_settings(
    DATAMINGLE_AGENT_API_KEY_BACKEND="local",
    CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}},
)
class AgentWebsocketTests(TransactionTestCase):
    reset_sequences = True

    def authenticate_agent(self, agent):
        issued_key = create_agent_api_key(agent)
        return issued_key.value

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
