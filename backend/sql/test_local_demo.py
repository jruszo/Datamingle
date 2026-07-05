from django.contrib.auth.models import Group, Permission
from django.core.management import call_command
from django.test import TestCase

from api_agents.models import Agent, AgentStatus
from api_agents.services import agent_api_key_hash, authenticate_agent_api_key
from common.utils.const import WorkflowType
from sql.local_demo import (
    DEMO_AGENT_API_KEY,
    managed_demo_instance_names,
    managed_demo_node_names,
    managed_demo_team_names,
    managed_demo_usernames,
)
from sql.models import (
    InfrastructureNode,
    Instance,
    Team,
    Users,
    WorkflowAuditSetting,
)


class TestLocalDemoSeed(TestCase):
    def test_seed_local_demo_is_idempotent(self):
        Users.objects.create_user(username="demo_admin", email="demo_admin@example.com")
        Agent.objects.create(name="demo-mysql-node-agent")

        call_command("seed_local_demo")
        call_command("seed_local_demo")

        self.assertEqual(
            Users.objects.filter(username__in=managed_demo_usernames()).count(),
            0,
        )
        self.assertEqual(
            Instance.objects.filter(
                instance_name__in=managed_demo_instance_names()
            ).count(),
            len(managed_demo_instance_names()),
        )
        self.assertEqual(
            InfrastructureNode.objects.filter(
                name__in=managed_demo_node_names(), enabled=True
            ).count(),
            len(managed_demo_node_names()),
        )
        self.assertEqual(
            Team.objects.filter(
                team_name__in=managed_demo_team_names(), is_deleted=0
            ).count(),
            len(managed_demo_team_names()),
        )
        self.assertEqual(
            WorkflowAuditSetting.objects.filter(
                workflow_type=WorkflowType.SQL_REVIEW
            ).count(),
            len(managed_demo_team_names()),
        )
        self.assertEqual(
            WorkflowAuditSetting.objects.filter(
                workflow_type=WorkflowType.ARCHIVE
            ).count(),
            len(managed_demo_team_names()),
        )
        superadmin_group = Group.objects.get(name="superadmin")
        self.assertEqual(
            superadmin_group.permissions.count(),
            Permission.objects.count(),
        )

        mysql_instance = Instance.objects.get(instance_name="demo-mysql-workflow")
        self.assertEqual(mysql_instance.host, "mysql_demo")
        self.assertEqual(mysql_instance.port, 3306)
        self.assertEqual(mysql_instance.user, "demo_datamingle")
        self.assertEqual(mysql_instance.db_type, "mysql")
        self.assertEqual(mysql_instance.node.name, "demo-mysql-node")
        self.assertEqual(
            mysql_instance.node.metadata.get("agent_service_endpoints", {}).get(
                "demo-mysql-workflow"
            ),
            {"host": "127.0.0.1", "port": 3307},
        )
        self.assertTrue(mysql_instance.workflow_enabled)
        self.assertTrue(mysql_instance.mysql_ddl_dml_eligible)
        self.assertEqual(mysql_instance.mysql_ddl_dml_block_reason, "")
        self.assertIsNotNone(mysql_instance.workflow_policy)
        self.assertTrue(mysql_instance.workflow_policy.is_active)
        self.assertEqual(
            list(
                mysql_instance.workflow_policy.steps.order_by("order").values_list(
                    "permission_group__name", flat=True
                )
            ),
            ["PM", "DBA"],
        )
        agent = Agent.objects.get(name="notebook-ubuntu")
        self.assertFalse(Agent.objects.filter(name="demo-mysql-node-agent").exists())
        self.assertEqual(agent.status, AgentStatus.ONLINE)
        self.assertEqual(agent.api_key_prefix, DEMO_AGENT_API_KEY[:16])
        self.assertEqual(agent.api_key_hash, agent_api_key_hash(DEMO_AGENT_API_KEY))
        self.assertEqual(authenticate_agent_api_key(DEMO_AGENT_API_KEY), agent)
        self.assertEqual(
            agent.metadata.get("active_websocket", {}).get("channel_name"),
            "e2e.demo.mysql.agent",
        )

        pg_instance = Instance.objects.get(instance_name="demo-pgsql-workflow")
        self.assertEqual(pg_instance.host, "postgres_demo")
        self.assertEqual(pg_instance.port, 5432)
        self.assertEqual(pg_instance.user, "demo_datamingle")
        self.assertEqual(pg_instance.db_type, "pgsql")
        self.assertEqual(pg_instance.node.name, "demo-postgres-node")
