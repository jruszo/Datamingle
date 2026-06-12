from django.contrib.auth.models import Group, Permission
from django.test import TestCase
from rest_framework.test import APIClient

from sql.models import Instance, InstanceTag, Team, TeamMembership, Users
from sql.utils.team import (
    user_has_instance_workflow_access,
    user_has_resource_role,
)


class TeamPermissionGroupTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.owner = Users.objects.create_user(username="owner", is_active=True)
        self.requester = Users.objects.create_user(username="requester", is_active=True)
        self.query_user = Users.objects.create_user(username="query", is_active=True)
        self.superuser = Users.objects.create_superuser(
            username="admin", password="password", email="admin@example.com"
        )

        self.dba = self._group(
            "DBA", "change_team", "sql_review", "sql_submit", "query_submit"
        )
        self.pm = self._group("PM", "sql_review")
        self.rd = self._group("RD", "sql_submit", "query_submit")
        self.qa = self._group("QA", "query_submit")

        self.team_a = Team.objects.create(team_name="Team A")
        self.team_b = Team.objects.create(team_name="Team B")
        TeamMembership.objects.create(
            user=self.owner, team=self.team_a, permission_group=self.dba
        )
        TeamMembership.objects.create(
            user=self.owner, team=self.team_b, permission_group=self.qa
        )
        TeamMembership.objects.create(
            user=self.requester, team=self.team_b, permission_group=self.rd
        )
        TeamMembership.objects.create(
            user=self.query_user, team=self.team_b, permission_group=self.qa
        )

        self.can_write = InstanceTag.objects.create(
            tag_code="can_write", tag_name="Can Write", active=True
        )
        self.service = Instance.objects.create(
            instance_name="service-b",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
            password="password",
        )
        self.service.resource_group.add(self.team_b)
        self.service.instance_tag.add(self.can_write)

    def _group(self, name, *codenames):
        group, _ = Group.objects.get_or_create(name=name)
        group.permissions.set(Permission.objects.filter(codename__in=codenames))
        return group

    def test_users_can_have_different_permission_groups_per_team(self):
        self.assertTrue(
            user_has_resource_role(self.owner, self.team_a, "sql.change_team")
        )
        self.assertFalse(
            user_has_resource_role(self.owner, self.team_b, "sql.change_team")
        )

    def test_requester_can_submit_but_query_user_cannot(self):
        self.assertTrue(
            user_has_instance_workflow_access(self.requester, self.service, 2)
        )
        self.assertFalse(
            user_has_instance_workflow_access(self.query_user, self.service, 2)
        )

    def test_owner_can_manage_only_owned_team(self):
        self.client.force_authenticate(self.owner)
        owned = self.client.put(
            f"/api/v1/teams/{self.team_a.team_id}/",
            {
                "team_name": "Team A",
                "user_access": [
                    {
                        "user_id": self.owner.id,
                        "permission_group_id": self.dba.id,
                    }
                ],
                "node_ids": [],
                "service_ids": [],
            },
            format="json",
        )
        self.assertEqual(owned.status_code, 200)

        other = self.client.put(
            f"/api/v1/teams/{self.team_b.team_id}/",
            {
                "team_name": "Team B",
                "user_access": [],
                "node_ids": [],
                "service_ids": [],
            },
            format="json",
        )
        self.assertEqual(other.status_code, 403)

    def test_permission_group_catalog_excludes_superadmin(self):
        Group.objects.get_or_create(name="superadmin")
        self.client.force_authenticate(self.superuser)
        response = self.client.get("/api/v1/teams/permission-groups/")
        self.assertEqual(response.status_code, 200)
        names = {row["name"] for row in response.json()["data"]}
        self.assertEqual(names, {"DBA", "PM", "RD", "QA"})

    def test_team_detail_manages_nodes_and_services_independently(self):
        self.client.force_authenticate(self.superuser)
        response = self.client.put(
            f"/api/v1/teams/{self.team_a.team_id}/",
            {
                "team_name": "Team A",
                "user_access": [],
                "node_ids": [],
                "service_ids": [self.service.id],
            },
            format="json",
        )
        self.assertEqual(response.status_code, 200)
        self.assertTrue(self.service.resource_group.filter(pk=self.team_a.pk).exists())
