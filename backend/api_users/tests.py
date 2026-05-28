from django.test import TestCase
from django.urls import Resolver404, resolve
from rest_framework.test import APIClient

from common.utils.const import WorkflowStatus, WorkflowType
from sql.models import (
    Instance,
    InstanceTag,
    ResourceAccessRole,
    ResourceGroup,
    ResourceGroupMembership,
    SqlWorkflow,
    WorkflowAudit,
    Users,
)
from sql.utils.resource_group import (
    user_has_instance_workflow_access,
    user_has_resource_role,
)
from sql.utils.workflow_audit import Audit


class ResourceAccessRoleTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.owner = Users.objects.create_user(
            username="owner", display="Owner", is_active=True
        )
        self.approver = Users.objects.create_user(
            username="approver", display="Approver", is_active=True
        )
        self.requester = Users.objects.create_user(
            username="requester", display="Requester", is_active=True
        )
        self.query_user = Users.objects.create_user(
            username="query", display="Query", is_active=True
        )
        self.superuser = Users.objects.create_superuser(
            username="admin", password="password", email="admin@example.com"
        )
        self.group_a = ResourceGroup.objects.create(group_name="Group A")
        self.group_b = ResourceGroup.objects.create(group_name="Group B")
        self.group_c = ResourceGroup.objects.create(group_name="Group C")

        ResourceGroupMembership.objects.create(
            user=self.owner,
            resource_group=self.group_a,
            access_role=ResourceAccessRole.RESOURCE_OWNER,
        )
        ResourceGroupMembership.objects.create(
            user=self.approver,
            resource_group=self.group_b,
            access_role=ResourceAccessRole.WORKFLOW_APPROVER,
        )
        ResourceGroupMembership.objects.create(
            user=self.requester,
            resource_group=self.group_b,
            access_role=ResourceAccessRole.WORKFLOW_REQUESTER,
        )
        ResourceGroupMembership.objects.create(
            user=self.query_user,
            resource_group=self.group_c,
            access_role=ResourceAccessRole.QUERY,
        )
        ResourceGroupMembership.objects.create(
            user=self.query_user,
            resource_group=self.group_b,
            access_role=ResourceAccessRole.QUERY,
        )

        self.can_write = InstanceTag.objects.create(
            tag_code="can_write", tag_name="Can Write", active=True
        )
        self.instance_b = Instance.objects.create(
            instance_name="instance-b",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
            password="password",
        )
        self.instance_b.resource_group.add(self.group_b)
        self.instance_b.instance_tag.add(self.can_write)

    def _create_waiting_sql_audit(self, group, suffix):
        workflow = SqlWorkflow.objects.create(
            workflow_name=f"Workflow {suffix}",
            group_id=group.group_id,
            group_name=group.group_name,
            engineer="submitter",
            engineer_display="Submitter",
            audit_auth_groups=(
                f"{ResourceAccessRole.WORKFLOW_APPROVER},"
                f"{ResourceAccessRole.RESOURCE_OWNER}"
            ),
            status="workflow_manreviewing",
            is_backup=True,
            instance=self.instance_b,
            db_name="test",
            syntax_type=2,
        )
        WorkflowAudit.objects.create(
            group_id=group.group_id,
            group_name=group.group_name,
            workflow_id=workflow.id,
            workflow_type=WorkflowType.SQL_REVIEW,
            workflow_title=workflow.workflow_name,
            workflow_remark="",
            audit_auth_groups=workflow.audit_auth_groups,
            current_audit=ResourceAccessRole.WORKFLOW_APPROVER,
            next_audit=ResourceAccessRole.RESOURCE_OWNER,
            current_status=WorkflowStatus.WAITING,
            create_user="submitter",
            create_user_display="Submitter",
        )
        return workflow

    def test_users_can_hold_different_roles_per_resource_group(self):
        self.assertTrue(
            user_has_resource_role(
                self.owner, self.group_a, ResourceAccessRole.RESOURCE_OWNER
            )
        )
        self.assertFalse(
            user_has_resource_role(
                self.owner, self.group_b, ResourceAccessRole.WORKFLOW_APPROVER
            )
        )
        self.assertTrue(
            user_has_resource_role(
                self.approver, self.group_b, ResourceAccessRole.WORKFLOW_REQUESTER
            )
        )
        self.assertFalse(
            user_has_resource_role(
                self.query_user,
                self.group_c,
                ResourceAccessRole.WORKFLOW_REQUESTER,
            )
        )

    def test_requester_can_submit_but_query_user_cannot_submit(self):
        self.assertTrue(
            user_has_instance_workflow_access(self.requester, self.instance_b, 2)
        )
        self.assertFalse(
            user_has_instance_workflow_access(self.query_user, self.instance_b, 2)
        )

    def test_approver_can_review_only_matching_resource_group(self):
        matching_workflow = self._create_waiting_sql_audit(self.group_b, "B")
        other_workflow = self._create_waiting_sql_audit(self.group_a, "A")

        self.assertFalse(
            Audit.can_review(
                self.requester, matching_workflow.id, WorkflowType.SQL_REVIEW
            )
        )
        self.assertTrue(
            Audit.can_review(
                self.approver, matching_workflow.id, WorkflowType.SQL_REVIEW
            )
        )
        self.assertFalse(
            Audit.can_review(self.approver, other_workflow.id, WorkflowType.SQL_REVIEW)
        )

    def test_resource_owner_can_manage_only_owned_resource_group(self):
        self.client.force_authenticate(self.owner)

        owned_response = self.client.put(
            f"/api/v1/user/resourcegroup/{self.group_a.group_id}/",
            {
                "group_name": "Group A",
                "user_access": [
                    {
                        "user_id": self.owner.id,
                        "access_role": ResourceAccessRole.RESOURCE_OWNER,
                    }
                ],
                "instance_ids": [],
            },
            format="json",
        )
        self.assertEqual(owned_response.status_code, 200)

        other_response = self.client.put(
            f"/api/v1/user/resourcegroup/{self.group_b.group_id}/",
            {
                "group_name": "Group B",
                "user_access": [],
                "instance_ids": [],
            },
            format="json",
        )
        self.assertEqual(other_response.status_code, 403)

    def test_role_catalog_is_read_only_and_group_crud_routes_are_removed(self):
        self.client.force_authenticate(self.superuser)

        catalog_response = self.client.get("/api/v1/user/access-roles/")
        self.assertEqual(catalog_response.status_code, 200)
        role_codes = {row["code"] for row in catalog_response.json()["data"]}
        self.assertEqual(
            role_codes,
            {
                ResourceAccessRole.QUERY,
                ResourceAccessRole.WORKFLOW_REQUESTER,
                ResourceAccessRole.WORKFLOW_APPROVER,
                ResourceAccessRole.RESOURCE_OWNER,
            },
        )

        with self.assertRaises(Resolver404):
            resolve("/api/v1/user/group/")
        with self.assertRaises(Resolver404):
            resolve("/api/v1/user/permission/")
