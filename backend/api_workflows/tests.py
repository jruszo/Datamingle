from types import SimpleNamespace
from unittest.mock import patch

from django.contrib.auth.models import Group
from rest_framework import status
from rest_framework.test import APITestCase

from api_agents.dispatch import (
    ACTIVE_WEBSOCKET_METADATA_KEY,
    WEBSOCKET_CHANNEL_METADATA_KEY,
)
from api_agents.models import Agent, AgentInstanceAssignment, AgentStatus
from sql.models import Instance, Team, Users, WorkflowAuditSetting, WorkflowPolicy
from common.utils.const import WorkflowType

from api_core.legacy_tests import TestWorkflow


class WorkflowSubmissionMetadataTests(APITestCase):
    def setUp(self):
        self.user = Users.objects.create_user(
            username="workflow-admin",
            email="workflow-admin@example.com",
            is_active=True,
            is_staff=True,
            is_superuser=True,
        )
        self.client.force_authenticate(user=self.user)

    def _team(self):
        return Team.objects.create(
            team_name="workflow team",
            group_parent_id=0,
            group_sort=1,
            group_level=1,
            is_deleted=0,
        )

    def _policy(self, name="Default SQL Policy"):
        role = Group.objects.create(name=f"{name} DBA")
        policy = WorkflowPolicy.objects.create(
            name=name,
            description="Default approval flow",
            created_by=self.user,
            updated_by=self.user,
        )
        policy.steps.create(order=1, permission_group=role)
        return policy

    def _instance(self, name, workflow_enabled, queryable=False, policy=None):
        instance = Instance.objects.create(
            instance_name=name,
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
            password="secret",
            queryable=queryable,
            workflow_enabled=workflow_enabled,
            workflow_policy=policy,
        )
        instance.resource_group.set([self.team])
        return instance

    def _agent_for(self, instance):
        agent = Agent.objects.create(
            name=f"{instance.instance_name}-agent",
            status=AgentStatus.ONLINE,
            metadata={
                ACTIVE_WEBSOCKET_METADATA_KEY: {
                    WEBSOCKET_CHANNEL_METADATA_KEY: "agent.test"
                }
            },
        )
        AgentInstanceAssignment.objects.create(
            agent=agent,
            instance=instance,
            enabled=True,
            command_enabled=True,
        )
        return agent

    def test_submission_metadata_only_lists_workflow_enabled_instances(self):
        self.team = self._team()
        policy = self._policy()
        enabled = self._instance(
            "workflow-enabled", workflow_enabled=True, policy=policy
        )
        disabled = self._instance("workflow-disabled", workflow_enabled=False)
        self._agent_for(enabled)
        self._agent_for(disabled)

        response = self.client.get("/api/v1/workflow/submission-metadata/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        names = {
            instance["instance_name"]
            for instance in response.json()["data"]["instances"]
        }
        self.assertEqual(names, {"workflow-enabled"})
        listed = response.json()["data"]["instances"][0]
        self.assertEqual(listed["workflow_policy_id"], policy.id)
        self.assertEqual(listed["workflow_policy_name"], policy.name)

    def test_submission_metadata_excludes_workflow_enabled_instances_without_policy(
        self,
    ):
        self.team = self._team()
        instance = self._instance("missing-policy", workflow_enabled=True)
        self._agent_for(instance)

        response = self.client.get("/api/v1/workflow/submission-metadata/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.json()["data"]["instances"], [])

    def test_export_submission_metadata_allows_queryable_instances_without_policy(
        self,
    ):
        self.team = self._team()
        policy = self._policy()
        with_policy = self._instance(
            "queryable-with-policy",
            workflow_enabled=False,
            queryable=True,
            policy=policy,
        )
        without_policy = self._instance(
            "queryable-without-policy",
            workflow_enabled=False,
            queryable=True,
        )
        self._agent_for(with_policy)
        self._agent_for(without_policy)

        response = self.client.get("/api/v1/workflow/export/submission-metadata/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        names = {
            instance["instance_name"]
            for instance in response.json()["data"]["instances"]
        }
        self.assertEqual(names, {"queryable-with-policy", "queryable-without-policy"})

    def test_approval_preview_uses_instance_policy(self):
        self.team = self._team()
        policy = self._policy("Production DDL")
        instance = self._instance(
            "workflow-enabled", workflow_enabled=True, policy=policy
        )
        self._agent_for(instance)

        response = self.client.get(
            f"/api/v1/workflow/approval-preview/?team_id={self.team.team_id}&instance_id={instance.id}"
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response.json()["data"]
        self.assertEqual(payload["workflow_policy_id"], policy.id)
        self.assertEqual(payload["workflow_policy_name"], "Production DDL")
        self.assertEqual(payload["review_info"][0]["team_name"], "Production DDL DBA")

    def test_approval_preview_for_policy_free_export_uses_team_setting(self):
        self.team = self._team()
        fallback_role = Group.objects.create(name="Fallback Export DBA")
        WorkflowAuditSetting.objects.create(
            team_id=self.team.team_id,
            team_name=self.team.team_name,
            workflow_type=WorkflowType.SQL_REVIEW,
            audit_auth_groups=str(fallback_role.id),
        )
        instance = self._instance(
            "queryable-without-policy",
            workflow_enabled=False,
            queryable=True,
        )
        self._agent_for(instance)

        response = self.client.get(
            f"/api/v1/workflow/approval-preview/?team_id={self.team.team_id}&instance_id={instance.id}"
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response.json()["data"]
        self.assertIsNone(payload["workflow_policy_id"])
        self.assertEqual(payload["workflow_policy_name"], "")
        self.assertEqual(payload["audit_auth_groups"], str(fallback_role.id))
        self.assertEqual(payload["review_info"][0]["team_name"], "Fallback Export DBA")

    @patch("api_workflows.serializers.run_agent_command_sync")
    def test_submission_creates_audit_from_service_policy(self, run_command):
        self.team = self._team()
        policy_role = Group.objects.create(name="Policy DBA")
        legacy_role = Group.objects.create(name="Legacy DBA")
        policy = WorkflowPolicy.objects.create(
            name="Service SQL Policy",
            created_by=self.user,
            updated_by=self.user,
        )
        policy.steps.create(order=1, permission_group=policy_role)
        WorkflowAuditSetting.objects.create(
            team_id=self.team.team_id,
            team_name=self.team.team_name,
            workflow_type=WorkflowType.SQL_REVIEW,
            audit_auth_groups=str(legacy_role.id),
        )
        instance = self._instance(
            "workflow-enabled", workflow_enabled=True, policy=policy
        )
        self._agent_for(instance)
        run_command.return_value = SimpleNamespace(
            result={
                "syntax_type": 2,
                "review_rows": [
                    {
                        "id": 1,
                        "stage": "checked",
                        "errlevel": 0,
                        "stagestatus": "Audit completed",
                        "errormessage": "",
                        "sql": "update users set active = 1",
                        "affected_rows": 1,
                    }
                ],
            }
        )

        response = self.client.post(
            "/api/v1/workflow/",
            {
                "workflow": {
                    "workflow_name": "Policy-backed DML",
                    "demand_url": "",
                    "team_id": self.team.team_id,
                    "db_name": "app",
                    "instance": instance.id,
                    "is_offline_export": 0,
                },
                "sql_content": "update users set active = 1;",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        workflow_id = response.json()["data"]["workflow_id"]
        workflow = Instance.objects.get(pk=instance.id).sqlworkflow_set.get(
            pk=workflow_id
        )
        self.assertEqual(workflow.workflow_policy_id, policy.id)
        self.assertEqual(workflow.workflow_policy_name, policy.name)
        self.assertEqual(workflow.audit_auth_groups, str(policy_role.id))

    @patch("api_workflows.serializers.run_agent_command_sync")
    def test_export_submission_allows_queryable_instance_without_policy(
        self, run_command
    ):
        self.team = self._team()
        fallback_role = Group.objects.create(name="Fallback Export DBA")
        WorkflowAuditSetting.objects.create(
            team_id=self.team.team_id,
            team_name=self.team.team_name,
            workflow_type=WorkflowType.SQL_REVIEW,
            audit_auth_groups=str(fallback_role.id),
        )
        instance = self._instance(
            "export-only",
            workflow_enabled=False,
            queryable=True,
        )
        self._agent_for(instance)
        run_command.return_value = SimpleNamespace(
            result={
                "syntax_type": 3,
                "review_rows": [
                    {
                        "id": 1,
                        "stage": "Export review",
                        "errlevel": 0,
                        "stagestatus": "Audit completed",
                        "errormessage": "",
                        "sql": "select * from users",
                        "affected_rows": 42,
                    }
                ],
            }
        )

        response = self.client.post(
            "/api/v1/workflow/",
            {
                "workflow": {
                    "workflow_name": "Policy-free export",
                    "demand_url": "",
                    "team_id": self.team.team_id,
                    "db_name": "app",
                    "instance": instance.id,
                    "is_offline_export": 1,
                    "export_format": "csv",
                },
                "sql_content": "select * from users;",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        workflow_id = response.json()["data"]["workflow_id"]
        workflow = Instance.objects.get(pk=instance.id).sqlworkflow_set.get(
            pk=workflow_id
        )
        self.assertIsNone(workflow.workflow_policy_id)
        self.assertEqual(workflow.workflow_policy_name, "")


class WorkflowPolicyApiTests(APITestCase):
    def setUp(self):
        self.creator = Users.objects.create_user(
            username="policy-creator",
            email="policy-creator@example.com",
            is_active=True,
        )
        self.other_user = Users.objects.create_user(
            username="policy-other",
            email="policy-other@example.com",
            is_active=True,
        )
        self.admin = Users.objects.create_user(
            username="policy-admin",
            email="policy-admin@example.com",
            is_active=True,
            is_staff=True,
        )
        self.role, _ = Group.objects.get_or_create(name="DBA")

    def test_authenticated_user_can_create_and_list_global_policy(self):
        self.client.force_authenticate(user=self.creator)

        create_response = self.client.post(
            "/api/v1/workflow/policies/",
            {
                "name": "Production SQL",
                "description": "DBA approval",
                "is_active": True,
                "steps": [{"order": 1, "permission_group": self.role.id}],
            },
            format="json",
        )

        self.assertEqual(create_response.status_code, status.HTTP_201_CREATED)
        payload = create_response.json()["data"]
        self.assertEqual(payload["name"], "Production SQL")
        self.assertEqual(payload["created_by"], self.creator.username)
        self.assertEqual(payload["steps"][0]["permission_group_name"], "DBA")

        list_response = self.client.get("/api/v1/workflow/policies/")

        self.assertEqual(list_response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            list_response.json()["data"]["results"][0]["name"], "Production SQL"
        )

    def test_only_creator_or_admin_can_edit_policy(self):
        policy = WorkflowPolicy.objects.create(
            name="Shared SQL",
            created_by=self.creator,
            updated_by=self.creator,
        )
        policy.steps.create(order=1, permission_group=self.role)

        self.client.force_authenticate(user=self.other_user)
        forbidden = self.client.patch(
            f"/api/v1/workflow/policies/{policy.id}/",
            {"name": "Changed by other"},
            format="json",
        )
        self.assertEqual(forbidden.status_code, status.HTTP_403_FORBIDDEN)

        self.client.force_authenticate(user=self.creator)
        allowed = self.client.patch(
            f"/api/v1/workflow/policies/{policy.id}/",
            {"name": "Changed by creator"},
            format="json",
        )
        self.assertEqual(allowed.status_code, status.HTTP_200_OK)

        self.client.force_authenticate(user=self.admin)
        admin_allowed = self.client.patch(
            f"/api/v1/workflow/policies/{policy.id}/",
            {"description": "Admin update"},
            format="json",
        )
        self.assertEqual(admin_allowed.status_code, status.HTTP_200_OK)

    def test_delete_policy_used_by_instance_returns_validation_error(self):
        policy = WorkflowPolicy.objects.create(
            name="Protected SQL",
            created_by=self.creator,
            updated_by=self.creator,
        )
        policy.steps.create(order=1, permission_group=self.role)
        Instance.objects.create(
            instance_name="protected-policy-instance",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            workflow_policy=policy,
        )
        self.client.force_authenticate(user=self.creator)

        response = self.client.delete(f"/api/v1/workflow/policies/{policy.id}/")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("assigned", response.json()["errors"])
        self.assertTrue(WorkflowPolicy.objects.filter(pk=policy.id).exists())
