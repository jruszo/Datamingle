from django.test import override_settings
from django.contrib.auth.models import Permission
from rest_framework import status
from rest_framework.test import APITestCase

from api_agents.models import (
    Agent,
    AgentInstanceAssignment,
    AgentNodeAssignment,
    AgentStatus,
)
from api_agents.services import build_agent_config, dispatch_sql_workflow_to_agent
from common.utils.const import WorkflowStatus, WorkflowType
from sql.models import (
    InfrastructureNode,
    Instance,
    ResourceGroup,
    SqlWorkflow,
    SqlWorkflowContent,
    Users,
    WorkflowAudit,
)


def create_node(name="db-node-01", hostname="db-node-01"):
    return InfrastructureNode.objects.create(
        node_name=name,
        hostname=hostname,
        environment="test",
        provider="manual",
    )


def create_instance(name="primary", node=None):
    return Instance.objects.create(
        node=node,
        instance_name=name,
        type="master",
        db_type="mysql",
        host=node.hostname if node else "127.0.0.1",
        port=3306,
        user="root",
        password="secret",
    )


def create_resource_group(name):
    return ResourceGroup.objects.create(
        group_name=name,
        group_parent_id=0,
        group_sort=1,
        group_level=1,
        is_deleted=0,
    )


def create_sql_workflow(instance):
    workflow = SqlWorkflow.objects.create(
        workflow_name="node managed workflow",
        group_id=1,
        group_name="Default",
        engineer="infra-admin",
        engineer_display="Infra Admin",
        audit_auth_groups="Default",
        status="workflow_review_pass",
        instance=instance,
        db_name="test",
        syntax_type=2,
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


@override_settings(DATAMINGLE_AGENT_API_KEY_BACKEND="local")
class InfrastructureNodeApiTests(APITestCase):
    def setUp(self):
        self.user = Users.objects.create_user(
            username="infra-admin",
            email="infra-admin@example.com",
            is_active=True,
            is_staff=True,
            is_superuser=True,
        )
        self.client.force_authenticate(user=self.user)

    def test_list_includes_empty_nodes_and_child_services(self):
        empty_node = create_node("empty-node", "empty-host")
        service_node = create_node("service-node", "service-host")
        create_instance("service-mysql", node=service_node)

        response = self.client.get("/api/v1/infrastructure/nodes/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response.json()["data"]
        nodes = {node["node_name"]: node for node in payload["results"]}
        self.assertEqual(nodes[empty_node.node_name]["service_count"], 0)
        self.assertEqual(nodes[empty_node.node_name]["services"], [])
        self.assertEqual(nodes[service_node.node_name]["service_count"], 1)
        self.assertEqual(
            nodes[service_node.node_name]["services"][0]["instance_name"],
            "service-mysql",
        )

    def test_list_scopes_child_services_to_user_resource_groups(self):
        visible_group = create_resource_group("visible services")
        hidden_group = create_resource_group("hidden services")
        visible_node = create_node("visible-node", "visible-host")
        hidden_node = create_node("hidden-node", "hidden-host")
        mixed_node = create_node("mixed-node", "mixed-host")
        visible_service = create_instance("visible-mysql", node=visible_node)
        hidden_service = create_instance("hidden-mysql", node=hidden_node)
        mixed_visible_service = create_instance("mixed-visible", node=mixed_node)
        mixed_hidden_service = create_instance("mixed-hidden", node=mixed_node)
        visible_service.resource_group.set([visible_group])
        hidden_service.resource_group.set([hidden_group])
        mixed_visible_service.resource_group.set([visible_group])
        mixed_hidden_service.resource_group.set([hidden_group])

        limited_user = Users.objects.create_user(
            username="limited-user",
            email="limited@example.com",
            is_active=True,
        )
        limited_user.resource_group.set([visible_group])
        limited_user.user_permissions.add(
            Permission.objects.get(codename="menu_instance_list")
        )
        self.client.force_authenticate(user=limited_user)

        response = self.client.get("/api/v1/infrastructure/nodes/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        nodes = {node["node_name"]: node for node in response.json()["data"]["results"]}
        self.assertIn("visible-node", nodes)
        self.assertIn("mixed-node", nodes)
        self.assertNotIn("hidden-node", nodes)
        self.assertEqual(nodes["visible-node"]["service_count"], 1)
        self.assertEqual(
            [service["instance_name"] for service in nodes["mixed-node"]["services"]],
            ["mixed-visible"],
        )

    def test_create_local_agent_for_node_surfaces_as_local_agent(self):
        node = create_node()
        service = create_instance("local-service", node=node)

        create_response = self.client.post(
            "/api/v1/agents/",
            {
                "name": "local-agent-01",
                "display_name": "Local Agent",
                "local_node": node.id,
            },
            format="json",
        )

        self.assertEqual(create_response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(create_response.json()["data"]["local_node"], node.id)

        list_response = self.client.get("/api/v1/infrastructure/nodes/")
        self.assertEqual(list_response.status_code, status.HTTP_200_OK)
        node_payload = list_response.json()["data"]["results"][0]
        self.assertEqual(node_payload["local_agent"]["name"], "local-agent-01")
        self.assertEqual(node_payload["local_agent_count"], 1)
        agent = Agent.objects.get(name="local-agent-01")
        assignment = AgentInstanceAssignment.objects.get(agent=agent, instance=service)
        self.assertEqual(assignment.local_node_id, node.id)
        self.assertTrue(assignment.command_enabled)
        config = build_agent_config(agent)
        self.assertEqual(config["assignments"][0]["instance_id"], service.id)

        future_service = create_instance("future-local-service", node=node)
        self.assertTrue(
            AgentInstanceAssignment.objects.filter(
                agent=agent,
                instance=future_service,
                local_node=node,
            ).exists()
        )

    def test_assignment_replace_preserves_inherited_local_node_assignments(self):
        node = create_node()
        service = create_instance("local-service", node=node)
        agent = Agent.objects.create(name="local-agent-01", local_node=node)
        inherited_assignment = AgentInstanceAssignment.objects.get(
            agent=agent,
            instance=service,
            local_node=node,
        )

        response = self.client.put(
            f"/api/v1/agents/{agent.id}/assignments/",
            {"assignments": []},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(
            AgentInstanceAssignment.objects.filter(pk=inherited_assignment.pk).exists()
        )

    def test_instance_update_allows_current_disabled_node(self):
        node = create_node()
        service = create_instance("disabled-node-service", node=node)
        node.enabled = False
        node.save(update_fields=["enabled", "update_time"])

        response = self.client.put(
            f"/api/v1/instance/{service.id}/",
            {"node": node.id},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        service.refresh_from_db()
        self.assertEqual(service.node_id, node.id)

    def test_agent_update_allows_current_disabled_local_node(self):
        node = create_node()
        agent = Agent.objects.create(name="local-agent-01", local_node=node)
        node.enabled = False
        node.save(update_fields=["enabled", "update_time"])

        response = self.client.patch(
            f"/api/v1/agents/{agent.id}/",
            {"local_node": node.id},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        agent.refresh_from_db()
        self.assertEqual(agent.local_node_id, node.id)

    def test_remote_manager_assignment_syncs_existing_and_future_services(self):
        node = create_node()
        first_service = create_instance("primary", node=node)
        second_service = create_instance("reporting", node=node)
        agent = Agent.objects.create(
            name="remote-agent-01",
            status=AgentStatus.ONLINE,
            hostname="agent-host",
        )

        response = self.client.put(
            f"/api/v1/infrastructure/nodes/{node.id}/remote-manager/",
            {
                "agent": agent.id,
                "command_enabled": True,
                "metrics_enabled": True,
                "online_schema_enabled": True,
                "logs_enabled": False,
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        node_assignment = AgentNodeAssignment.objects.get(node=node, agent=agent)
        self.assertEqual(
            AgentInstanceAssignment.objects.filter(
                node_assignment=node_assignment,
                agent=agent,
                instance__in=[first_service, second_service],
                command_enabled=True,
                metrics_enabled=True,
                online_schema_enabled=True,
            ).count(),
            2,
        )

        future_service = create_instance("future", node=node)
        self.assertTrue(
            AgentInstanceAssignment.objects.filter(
                node_assignment=node_assignment,
                agent=agent,
                instance=future_service,
                command_enabled=True,
            ).exists()
        )

        list_response = self.client.get("/api/v1/infrastructure/nodes/")
        self.assertEqual(list_response.status_code, status.HTTP_200_OK)
        node_payload = list_response.json()["data"]["results"][0]
        self.assertEqual(node_payload["remote_manager"]["name"], agent.name)
        self.assertTrue(node_payload["remote_manager"]["command_enabled"])

    def test_node_remote_manager_allows_workflow_dispatch_for_service(self):
        node = create_node()
        service = create_instance("primary", node=node)
        agent = Agent.objects.create(
            name="remote-agent-01",
            status=AgentStatus.ONLINE,
            hostname="agent-host",
        )
        AgentNodeAssignment.objects.create(
            node=node,
            agent=agent,
            command_enabled=True,
            metrics_enabled=True,
        )
        workflow = create_sql_workflow(service)

        command = dispatch_sql_workflow_to_agent(workflow)

        self.assertIsNotNone(command)
        self.assertEqual(command.agent_id, agent.id)
        self.assertEqual(command.instance_id, service.id)
