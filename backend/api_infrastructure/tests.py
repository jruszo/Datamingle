from datetime import datetime
from unittest.mock import patch

from django.contrib.auth.models import Group
from django.contrib.auth.models import Permission
from django.utils import timezone
from rest_framework import status
from rest_framework.test import APITestCase

from api_agents.models import (
    Agent,
    AgentInstanceAssignment,
    AgentStatus,
)
from api_agents.services import build_agent_config
from sql.models import (
    DEFAULT_NODE_EXPORTER_COLLECTORS,
    InfrastructureNode,
    Instance,
    MysqlCluster,
    MysqlTopologyAlert,
    Team,
    TeamMembership,
    ServiceRecommendation,
    Users,
    WorkflowPolicy,
)


def create_team(name):
    return Team.objects.create(
        team_name=name,
        group_parent_id=0,
        group_sort=1,
        group_level=1,
        is_deleted=0,
    )


def create_node(
    name="db-node-01",
    address="10.0.0.10",
    monitoring_enabled=True,
    monitoring_collectors=None,
):
    return InfrastructureNode.objects.create(
        name=name,
        address=address,
        description="Database host",
        monitoring_enabled=monitoring_enabled,
        monitoring_collectors=(
            monitoring_collectors
            if monitoring_collectors is not None
            else list(DEFAULT_NODE_EXPORTER_COLLECTORS)
        ),
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


def create_workflow_policy(name="Default SQL Policy", user=None):
    role, _ = Group.objects.get_or_create(name=f"{name} DBA")
    policy = WorkflowPolicy.objects.create(
        name=name,
        description="Default SQL approval flow",
        created_by=user,
        updated_by=user,
    )
    policy.steps.create(order=1, permission_group=role)
    return policy


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
        group = create_team("primary services")
        node = create_node(monitoring_enabled=False)
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
            last_seen_at=datetime(2026, 6, 3, 20, 7, 0),
            last_websocket_pong_at=datetime(2026, 6, 3, 20, 8, 0),
            last_connected_at=datetime(2026, 6, 3, 20, 6, 0),
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
        self.assertFalse(payload["monitoring_enabled"])
        self.assertEqual(
            payload["monitoring_collectors"], list(DEFAULT_NODE_EXPORTER_COLLECTORS)
        )
        self.assertEqual(payload["team_ids"], [group.team_id])
        self.assertEqual(payload["agent_id"], agent.id)
        self.assertEqual(payload["agent_status"], AgentStatus.ONLINE)
        self.assertEqual(payload["agent"]["hostname"], "db-host-01")
        self.assertEqual(payload["agent"]["platform"], "linux")
        self.assertEqual(payload["agent"]["architecture"], "amd64")
        self.assertEqual(payload["agent"]["agent_version"], "0.1.0")
        self.assertEqual(payload["agent"]["last_seen_at"], "2026-06-03T20:07:00Z")
        self.assertEqual(
            payload["agent"]["last_websocket_pong_at"], "2026-06-03T20:08:00Z"
        )
        self.assertEqual(payload["agent"]["last_connected_at"], "2026-06-03T20:06:00Z")
        self.assertEqual(payload["agent"]["last_config_revision"], 2)
        agent.refresh_from_db()
        self.assertEqual(
            payload["agent"]["desired_config_revision"],
            agent.desired_config_revision,
        )
        self.assertEqual(payload["service_count"], 1)
        self.assertEqual(payload["recommendation_count"], 1)
        self.assertEqual(payload["services"][0]["service_name"], service.instance_name)
        self.assertTrue(payload["services"][0]["monitoring_enabled"])
        self.assertEqual(
            payload["services"][0]["monitoring_collectors"],
            ["global_status", "global_variables", "slave_status"],
        )
        self.assertEqual(
            payload["recommendations"][0]["service_name"], "orders-replica"
        )

    def test_mysql_cluster_list_is_scoped_to_visible_services(self):
        user = Users.objects.create_user(
            username="infra-viewer",
            email="infra-viewer@example.com",
            is_active=True,
        )
        user.user_permissions.add(
            Permission.objects.get(codename="menu_infrastructure")
        )
        permission_level = Group.objects.create(name="Visible Infra Role")
        visible_team = create_team("visible services")
        hidden_team = create_team("hidden services")
        TeamMembership.objects.create(
            user=user, team=visible_team, permission_level=permission_level
        )
        visible_cluster = MysqlCluster.objects.create(
            name="visible-cluster",
            label_value="visible_cluster",
            cluster_key="mysql:endpoint:10.0.0.10:3306",
        )
        hidden_cluster = MysqlCluster.objects.create(
            name="hidden-cluster",
            label_value="hidden_cluster",
            cluster_key="mysql:endpoint:10.0.0.20:3306",
        )
        visible_service = create_instance(
            "visible-primary", node=create_node("visible-node", "10.0.0.10")
        )
        visible_service.mysql_cluster = visible_cluster
        visible_service.save(update_fields=["mysql_cluster", "update_time"])
        visible_service.resource_group.set([visible_team])
        MysqlTopologyAlert.objects.create(
            cluster=visible_cluster,
            alert_type=MysqlTopologyAlert.TYPE_MISSING_MASTER,
            status=MysqlTopologyAlert.STATUS_ACTIVE,
            message="Cluster master is missing.",
        )
        hidden_service = create_instance(
            "hidden-primary", node=create_node("hidden-node", "10.0.0.20")
        )
        hidden_service.mysql_cluster = hidden_cluster
        hidden_service.save(update_fields=["mysql_cluster", "update_time"])
        hidden_service.resource_group.set([hidden_team])
        MysqlTopologyAlert.objects.create(
            cluster=hidden_cluster,
            alert_type=MysqlTopologyAlert.TYPE_MISSING_MASTER,
            status=MysqlTopologyAlert.STATUS_ACTIVE,
            message="Hidden cluster master is missing.",
        )
        self.client.force_authenticate(user=user)

        response = self.client.get("/api/v1/infrastructure/mysql-clusters/")
        hidden_response = self.client.get(
            f"/api/v1/infrastructure/mysql-clusters/{hidden_cluster.id}/"
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        names = {row["name"] for row in response.json()["data"]["results"]}
        self.assertEqual(names, {"visible-cluster"})
        visible_payload = response.json()["data"]["results"][0]
        self.assertEqual(visible_payload["active_alert_count"], 1)
        self.assertEqual(
            visible_payload["active_alerts"][0]["alert_type"],
            MysqlTopologyAlert.TYPE_MISSING_MASTER,
        )
        self.assertEqual(hidden_response.status_code, status.HTTP_404_NOT_FOUND)

    def test_create_service_under_node_syncs_local_agent_assignment(self):
        node = create_node()
        policy = create_workflow_policy(user=self.user)

        create_agent_response = self.client.post(
            "/api/v1/agents/",
            {
                "name": "local-agent-01",
                "display_name": "Local Agent",
                "local_node": node.id,
                "monitoring_enabled": False,
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
                "monitoring_enabled": True,
                "workflow_enabled": True,
                "workflow_policy": policy.id,
                "monitoring_collectors": ["global_status", "binlog_size"],
                "is_ssl": False,
                "verify_ssl": True,
                "db_name": "",
                "show_db_name_regex": "",
                "denied_db_name_regex": "",
                "charset": "utf8mb4",
                "team_ids": [],
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
        self.assertTrue(service.workflow_enabled)
        self.assertEqual(service.workflow_policy_id, policy.id)
        self.assertTrue(service.monitoring_enabled)
        self.assertEqual(
            service.monitoring_collectors, ["global_status", "binlog_size"]
        )
        agent.local_node.refresh_from_db()
        self.assertFalse(agent.local_node.monitoring_enabled)
        config = build_agent_config(agent)
        self.assertFalse(config["node"]["monitoring_enabled"])
        self.assertEqual(config["assignments"][0]["instance_id"], service.id)
        self.assertEqual(config["assignments"][0]["node_id"], node.id)
        self.assertTrue(config["assignments"][0]["workflow_enabled"])
        self.assertTrue(config["assignments"][0]["online_schema_enabled"])
        self.assertIn("online_schema", config["assignments"][0]["modules"])
        self.assertTrue(config["assignments"][0]["service_monitoring_enabled"])
        self.assertEqual(
            config["assignments"][0]["service_monitoring_collectors"],
            ["global_status", "binlog_size"],
        )
        service_monitoring = {module["name"]: module for module in config["modules"]}[
            "service_monitoring"
        ]
        self.assertTrue(service_monitoring["enabled"])
        self.assertEqual(
            service_monitoring["raw"]["services"][0]["username"],
            "root",
        )
        self.assertEqual(
            service_monitoring["raw"]["services"][0]["collectors"],
            ["global_status", "binlog_size"],
        )

    def test_create_queryable_service_allows_missing_workflow_policy(self):
        node = create_node()

        response = self.client.post(
            "/api/v1/infrastructure/services/",
            {
                "node_id": node.id,
                "service_name": "orders-query",
                "role": "master",
                "engine": "mysql",
                "host": node.address,
                "port": 3306,
                "user": "root",
                "password": "secret",
                "monitoring_enabled": True,
                "queryable": True,
                "workflow_enabled": False,
                "monitoring_collectors": ["global_status"],
                "is_ssl": False,
                "verify_ssl": True,
                "db_name": "",
                "show_db_name_regex": "",
                "denied_db_name_regex": "",
                "charset": "utf8mb4",
                "team_ids": [],
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        payload = response.json()["data"]
        self.assertTrue(payload["queryable"])
        self.assertFalse(payload["workflow_enabled"])
        self.assertIsNone(payload["workflow_policy"])

    def test_create_workflow_enabled_service_requires_workflow_policy(self):
        node = create_node()

        response = self.client.post(
            "/api/v1/infrastructure/services/",
            {
                "node_id": node.id,
                "service_name": "orders-workflow",
                "role": "master",
                "engine": "mysql",
                "host": node.address,
                "port": 3306,
                "user": "root",
                "password": "secret",
                "monitoring_enabled": True,
                "queryable": True,
                "workflow_enabled": True,
                "monitoring_collectors": ["global_status"],
                "is_ssl": False,
                "verify_ssl": True,
                "db_name": "",
                "show_db_name_regex": "",
                "denied_db_name_regex": "",
                "charset": "utf8mb4",
                "team_ids": [],
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("workflow_policy", response.json())

    def test_service_detail_serializes_workflow_policy(self):
        group = create_team("primary services")
        node = create_node()
        policy = create_workflow_policy(user=self.user)
        service = create_instance("orders-primary", node=node)
        service.queryable = True
        service.workflow_policy = policy
        service.save(update_fields=["queryable", "workflow_policy"])
        service.resource_group.set([group])

        response = self.client.get(f"/api/v1/infrastructure/nodes/{node.id}/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response.json()["data"]
        self.assertEqual(payload["services"][0]["workflow_policy"], policy.id)
        self.assertEqual(
            payload["services"][0]["workflow_policy_name"], "Default SQL Policy"
        )

    def test_update_node_monitoring_bumps_agent_config_revision(self):
        node = create_node()
        agent = Agent.objects.create(name="agent-a", local_node=node)

        response = self.client.patch(
            f"/api/v1/infrastructure/nodes/{node.id}/",
            {
                "name": node.name,
                "address": node.address,
                "description": node.description,
                "metadata": node.metadata,
                "monitoring_enabled": False,
                "monitoring_collectors": ["cpu", "meminfo", "filesystem"],
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response.json()["data"]
        self.assertFalse(payload["monitoring_enabled"])
        self.assertEqual(
            payload["monitoring_collectors"], ["cpu", "meminfo", "filesystem"]
        )
        node.refresh_from_db()
        agent.refresh_from_db()
        self.assertFalse(node.monitoring_enabled)
        self.assertEqual(node.monitoring_collectors, ["cpu", "meminfo", "filesystem"])
        self.assertEqual(agent.desired_config_revision, 2)
        config = build_agent_config(agent)
        self.assertFalse(config["node"]["monitoring_enabled"])
        modules = {module["name"]: module for module in config["modules"]}
        self.assertEqual(
            modules["node_monitoring"]["raw"]["node_exporter"]["collectors"],
            ["cpu", "meminfo", "filesystem"],
        )

    def test_monitoring_labels_are_inherited_and_service_values_override_node(self):
        node = create_node()
        node.monitoring_labels = {"environment": "prod", "team": "platform"}
        node.save(update_fields=["monitoring_labels"])
        agent = Agent.objects.create(name="agent-a", local_node=node)
        service = create_instance("orders-primary", node=node)
        service.monitoring_labels = {"team": "payments"}
        service.save(update_fields=["monitoring_labels"])

        response = self.client.get(f"/api/v1/infrastructure/nodes/{node.id}/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response.json()["data"]
        self.assertEqual(payload["monitoring_labels"], node.monitoring_labels)
        self.assertEqual(
            payload["services"][0]["effective_monitoring_labels"],
            {"environment": "prod", "team": "payments"},
        )
        config = build_agent_config(agent)
        modules = {module["name"]: module for module in config["modules"]}
        self.assertEqual(
            modules["node_monitoring"]["raw"]["labels"]["dm_environment"], "prod"
        )
        self.assertEqual(
            modules["service_monitoring"]["raw"]["services"][0]["labels"],
            {"dm_environment": "prod", "dm_team": "payments"},
        )

    def test_service_detail_serializes_workflow_enabled(self):
        node = create_node()
        service = create_instance("orders-primary", node=node)
        service.workflow_enabled = True
        service.save(update_fields=["workflow_enabled"])

        response = self.client.get(f"/api/v1/infrastructure/nodes/{node.id}/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response.json()["data"]
        self.assertTrue(payload["services"][0]["workflow_enabled"])

    def test_update_node_saves_monitoring_labels(self):
        node = create_node()

        response = self.client.patch(
            f"/api/v1/infrastructure/nodes/{node.id}/",
            {"monitoring_labels": {"environment": "prod", "team": "platform"}},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.json()["data"]["monitoring_labels"],
            {"environment": "prod", "team": "platform"},
        )
        node.refresh_from_db()
        self.assertEqual(
            node.monitoring_labels,
            {"environment": "prod", "team": "platform"},
        )

    def test_node_list_filters_monitoring_labels(self):
        prod_platform = create_node(name="prod-platform", address="10.0.0.11")
        prod_platform.monitoring_labels = {
            "environment": "prod",
            "team": "platform",
        }
        prod_platform.save(update_fields=["monitoring_labels"])
        prod_payments = create_node(name="prod-payments", address="10.0.0.12")
        prod_payments.monitoring_labels = {
            "environment": "prod",
            "team": "payments",
        }
        prod_payments.save(update_fields=["monitoring_labels"])
        create_node(name="unlabelled", address="10.0.0.13")

        response = self.client.get(
            "/api/v1/infrastructure/nodes/",
            {
                "lf.environment": "prod",
                "lx.team": "payments",
            },
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            [node["name"] for node in response.json()["data"]["results"]],
            ["prod-platform"],
        )

        exclude_response = self.client.get(
            "/api/v1/infrastructure/nodes/",
            {"lx.team": "payments"},
        )
        self.assertEqual(exclude_response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            [node["name"] for node in exclude_response.json()["data"]["results"]],
            ["prod-platform", "unlabelled"],
        )

    def test_node_label_filter_supports_multiple_values(self):
        for suffix, environment in (
            ("prod", "prod"),
            ("stage", "stage"),
            ("dev", "dev"),
        ):
            node = create_node(name=f"node-{suffix}", address=f"10.0.1.{len(suffix)}")
            node.monitoring_labels = {"environment": environment}
            node.save(update_fields=["monitoring_labels"])

        response = self.client.get(
            "/api/v1/infrastructure/nodes/",
            [("lf.environment", "prod"), ("lf.environment", "stage")],
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            [node["name"] for node in response.json()["data"]["results"]],
            ["node-prod", "node-stage"],
        )

    def test_node_label_names_and_values_are_available_for_autocomplete(self):
        first = create_node(name="first", address="10.0.2.1")
        first.monitoring_labels = {"environment": "prod", "team": "platform"}
        first.save(update_fields=["monitoring_labels"])
        second = create_node(name="second", address="10.0.2.2")
        second.monitoring_labels = {"environment": "stage"}
        second.save(update_fields=["monitoring_labels"])

        names_response = self.client.get("/api/v1/infrastructure/nodes/labels/")
        values_response = self.client.get(
            "/api/v1/infrastructure/nodes/label/environment/values/"
        )

        self.assertEqual(names_response.status_code, status.HTTP_200_OK)
        self.assertEqual(names_response.json()["data"], ["environment", "team"])
        self.assertEqual(values_response.status_code, status.HTTP_200_OK)
        self.assertEqual(values_response.json()["data"], ["prod", "stage"])

    def test_rejects_invalid_monitoring_label_names(self):
        node = create_node()

        response = self.client.patch(
            f"/api/v1/infrastructure/nodes/{node.id}/",
            {"monitoring_labels": {"not-valid": "prod"}},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("monitoring_labels", response.json())

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
                "team_ids": [],
                "service_tag_ids": [],
                "recommendation_id": recommendation.id,
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        recommendation.refresh_from_db()
        self.assertEqual(recommendation.status, ServiceRecommendation.STATUS_ACCEPTED)

    def test_service_connection_test_refreshes_inventory_through_agent(self):
        node = create_node()
        service = create_instance("orders-primary", node=node)

        with patch(
            "api_infrastructure.views.refresh_instance_inventory_snapshot",
            return_value={"success": True, "status": "ok"},
        ) as refresh_inventory:
            response = self.client.post(
                f"/api/v1/infrastructure/services/{service.id}/test/",
                {},
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.json()["data"]["message"],
            "Connection successful and inventory refreshed.",
        )
        self.assertEqual(refresh_inventory.call_args.kwargs["instance"], service)
