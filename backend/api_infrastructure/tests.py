from types import SimpleNamespace
from unittest.mock import patch

from django.test import override_settings
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APITestCase

from api_agents.models import (
    Agent,
    AgentCommandStatus,
    AgentInstanceAssignment,
    AgentStatus,
)
from api_agents.services import build_agent_config
from sql.models import (
    InfrastructureNode,
    Instance,
    ResourceGroup,
    ServiceRecommendation,
    Users,
)


def create_resource_group(name):
    return ResourceGroup.objects.create(
        group_name=name,
        group_parent_id=0,
        group_sort=1,
        group_level=1,
        is_deleted=0,
    )


def create_node(name="db-node-01", address="10.0.0.10"):
    return InfrastructureNode.objects.create(
        name=name,
        address=address,
        description="Database host",
    )


def create_instance(name="primary", node=None):
    return Instance.objects.create(
        node=node,
        instance_name=name,
        type="master",
        db_type="mysql",
        host=node.address if node else "127.0.0.1",
        port=3306,
        user="root",
        password="secret",
    )


@override_settings(
    DATAMINGLE_AGENT_API_KEY_BACKEND="workos",
    WORKOS_API_KEY="sk_test_123",
    WORKOS_CLIENT_ID="client_test_123",
    WORKOS_ORGANIZATION_ID="org_test_123",
    WORKOS_BASE_URL="https://api.workos.test/",
)
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

    def test_node_list_includes_services_and_recommendations(self):
        group = create_resource_group("primary services")
        node = create_node()
        node.resource_group.set([group])
        agent = Agent.objects.create(
            name="db-node-agent",
            display_name="DB Node Agent",
            local_node=node,
            status=AgentStatus.ONLINE,
            hostname="db-host-01",
            platform="linux",
            architecture="amd64",
            agent_version="0.1.0",
            last_config_revision=2,
            desired_config_revision=3,
        )
        service = create_instance("orders-primary", node=node)
        service.resource_group.set([group])
        ServiceRecommendation.objects.create(
            node=node,
            engine="mysql",
            host=node.address,
            port=3307,
            service_name="orders-replica",
            source="agent",
            confidence=90,
            fingerprint="mysql-10.0.0.10-3307",
            last_seen_at=timezone.now(),
        )

        response = self.client.get("/api/v1/infrastructure/nodes/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response.json()["data"]["results"][0]
        self.assertEqual(payload["name"], node.name)
        self.assertEqual(payload["address"], node.address)
        self.assertEqual(payload["resource_group_ids"], [group.group_id])
        self.assertEqual(payload["agent_id"], agent.id)
        self.assertEqual(payload["agent_status"], AgentStatus.ONLINE)
        self.assertEqual(payload["agent"]["hostname"], "db-host-01")
        self.assertEqual(payload["agent"]["platform"], "linux")
        self.assertEqual(payload["agent"]["architecture"], "amd64")
        self.assertEqual(payload["agent"]["agent_version"], "0.1.0")
        self.assertEqual(payload["agent"]["last_config_revision"], 2)
        agent.refresh_from_db()
        self.assertEqual(
            payload["agent"]["desired_config_revision"],
            agent.desired_config_revision,
        )
        self.assertEqual(payload["service_count"], 1)
        self.assertEqual(payload["recommendation_count"], 1)
        self.assertEqual(payload["services"][0]["service_name"], service.instance_name)
        self.assertEqual(
            payload["recommendations"][0]["service_name"], "orders-replica"
        )

    @patch("api_agents.services.requests.post")
    def test_create_service_under_node_syncs_local_agent_assignment(self, mock_post):
        mock_post.return_value.json.return_value = {
            "object": "api_key",
            "id": "api_key_123",
            "owner": {"type": "organization", "id": "org_test_123"},
            "name": "Datamingle Agent",
            "value": "sk_agent_created_once",
            "obfuscated_value": "sk_...once",
            "permissions": ["datamingle-agent:connect"],
        }
        mock_post.return_value.raise_for_status.return_value = None
        node = create_node()

        create_agent_response = self.client.post(
            "/api/v1/agents/",
            {
                "name": "local-agent-01",
                "display_name": "Local Agent",
                "local_node": node.id,
            },
            format="json",
        )
        self.assertEqual(create_agent_response.status_code, status.HTTP_201_CREATED)

        create_service_response = self.client.post(
            "/api/v1/infrastructure/services/",
            {
                "node_id": node.id,
                "service_name": "orders-primary",
                "role": "master",
                "engine": "mysql",
                "host": node.address,
                "port": 3306,
                "user": "root",
                "password": "secret",
                "is_ssl": False,
                "verify_ssl": True,
                "db_name": "",
                "show_db_name_regex": "",
                "denied_db_name_regex": "",
                "charset": "utf8mb4",
                "resource_group_ids": [],
                "service_tag_ids": [],
            },
            format="json",
        )

        self.assertEqual(create_service_response.status_code, status.HTTP_201_CREATED)
        agent = Agent.objects.get(name="local-agent-01")
        service = Instance.objects.get(instance_name="orders-primary")
        assignment = AgentInstanceAssignment.objects.get(agent=agent, instance=service)
        self.assertEqual(assignment.local_node_id, node.id)
        self.assertTrue(assignment.command_enabled)
        config = build_agent_config(agent)
        self.assertEqual(config["assignments"][0]["instance_id"], service.id)
        self.assertEqual(config["assignments"][0]["node_id"], node.id)

    def test_create_service_accepts_recommendation(self):
        node = create_node()
        recommendation = ServiceRecommendation.objects.create(
            node=node,
            engine="mysql",
            host=node.address,
            port=3306,
            service_name="recommended-primary",
            source="agent",
            confidence=80,
            fingerprint="recommended-primary",
        )

        response = self.client.post(
            "/api/v1/infrastructure/services/",
            {
                "node_id": node.id,
                "service_name": "recommended-primary",
                "role": "master",
                "engine": "mysql",
                "host": node.address,
                "port": 3306,
                "user": "root",
                "password": "secret",
                "is_ssl": False,
                "verify_ssl": True,
                "db_name": "",
                "show_db_name_regex": "",
                "denied_db_name_regex": "",
                "charset": "",
                "resource_group_ids": [],
                "service_tag_ids": [],
                "recommendation_id": recommendation.id,
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        recommendation.refresh_from_db()
        self.assertEqual(recommendation.status, ServiceRecommendation.STATUS_ACCEPTED)

    def test_service_connection_test_dispatches_agent_command(self):
        node = create_node()
        service = create_instance("orders-primary", node=node)
        command = SimpleNamespace(
            status=AgentCommandStatus.SUCCEEDED,
            result={"message": "Connection successful from agent."},
        )

        with patch(
            "api_infrastructure.views.run_agent_command_sync", return_value=command
        ) as run_command:
            response = self.client.post(
                f"/api/v1/infrastructure/services/{service.id}/test/",
                {},
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.json()["data"]["message"], "Connection successful from agent."
        )
        self.assertEqual(run_command.call_args.kwargs["instance"], service)
        self.assertEqual(
            run_command.call_args.kwargs["command_type"], "connection.test"
        )
