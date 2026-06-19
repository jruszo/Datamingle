import datetime

from django.contrib.auth.models import Group, Permission
from django.test import TestCase
from rest_framework.test import APIClient

from sql.models import (
    Instance,
    InstanceTag,
    Team,
    TeamMembership,
    TemporaryTeamGrant,
    Users,
)
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
            user=self.owner, team=self.team_a, permission_level=self.dba
        )
        TeamMembership.objects.create(
            user=self.owner, team=self.team_b, permission_level=self.qa
        )
        TeamMembership.objects.create(
            user=self.requester, team=self.team_b, permission_level=self.rd
        )
        TeamMembership.objects.create(
            user=self.query_user, team=self.team_b, permission_level=self.qa
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

    def test_users_can_have_different_permission_levels_per_team(self):
        self.assertTrue(
            user_has_resource_role(self.owner, self.team_a, "sql.change_team")
        )
        self.assertFalse(
            user_has_resource_role(self.owner, self.team_b, "sql.change_team")
        )

    def test_membership_permissions_are_unioned_across_teams(self):
        self.assertTrue(self.owner.has_perm("sql.change_team"))
        self.assertTrue(self.owner.has_perm("sql.query_submit"))
        self.assertFalse(self.owner.has_perm("sql.sqlexport_submit"))

    def test_active_temporary_team_grant_includes_permission_level(self):
        temporary_user = Users.objects.create_user(username="temporary", is_active=True)
        TemporaryTeamGrant.objects.create(
            user=temporary_user,
            team=self.team_a,
            permission_level=self.qa,
            valid_date=datetime.date.today(),
        )

        self.assertTrue(temporary_user.has_perm("sql.query_submit"))
        self.assertTrue(
            user_has_resource_role(
                temporary_user,
                self.team_a,
                "sql.query_submit",
            )
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
                        "permission_level_id": self.dba.id,
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

    def test_permission_level_catalog_excludes_superadmin(self):
        Group.objects.get_or_create(name="superadmin")
        self.client.force_authenticate(self.superuser)
        response = self.client.get("/api/v1/permission-levels/")
        self.assertEqual(response.status_code, 200)
        names = {row["name"] for row in response.json()["data"]}
        self.assertEqual(names, {"DBA", "PM", "RD", "QA"})

    def test_superuser_can_create_and_update_permission_level(self):
        self.client.force_authenticate(self.superuser)
        created = self.client.post(
            "/api/v1/permission-levels/",
            {
                "name": "Developer",
                "permission_codes": ["sql.query_submit", "sql.sql_submit"],
            },
            format="json",
        )
        self.assertEqual(created.status_code, 201)
        level_id = created.json()["data"]["id"]
        self.assertEqual(
            created.json()["data"]["permissions"],
            ["sql.query_submit", "sql.sql_submit"],
        )

        updated = self.client.put(
            f"/api/v1/permission-levels/{level_id}/",
            {
                "name": "Read Only",
                "permission_codes": ["sql.query_submit"],
            },
            format="json",
        )
        self.assertEqual(updated.status_code, 200)
        self.assertEqual(updated.json()["data"]["name"], "Read Only")
        self.assertEqual(updated.json()["data"]["permissions"], ["sql.query_submit"])

    def test_permission_level_rejects_unsafe_permissions(self):
        self.client.force_authenticate(self.superuser)
        response = self.client.post(
            "/api/v1/permission-levels/",
            {
                "name": "Unsafe",
                "permission_codes": ["sql.menu_system"],
            },
            format="json",
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("permission_codes", response.json())

    def test_permission_level_delete_is_blocked_while_in_use(self):
        self.client.force_authenticate(self.superuser)
        response = self.client.delete(f"/api/v1/permission-levels/{self.qa.id}/")

        self.assertEqual(response.status_code, 400)
        self.assertTrue(Group.objects.filter(pk=self.qa.id).exists())

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
