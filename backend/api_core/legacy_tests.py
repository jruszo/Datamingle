from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch, Mock
import re

from django.conf import settings
from django.contrib.auth import BACKEND_SESSION_KEY, HASH_SESSION_KEY, SESSION_KEY
from django.test import TestCase
from django.core.cache import cache
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group, Permission
from django.http import HttpResponse
from django.urls import Resolver404, resolve
from importlib import import_module
from rest_framework.test import APITestCase, APIClient
from rest_framework import status
from allauth.headless.tokens.strategies.jwt import internal as allauth_jwt
from common.config import SysConfig
from sql.utils.workflow_audit import AuditException, AuditSetting
from sql.engines import ReviewSet
from sql.engines.models import ReviewResult, ResultSet
from api_admin.settings import (
    INVENTORY_REFRESH_INTERVAL_OPTIONS,
    NOTIFY_PHASE_OPTIONS,
)
from api_agents.dispatch import (
    ACTIVE_WEBSOCKET_METADATA_KEY,
    WEBSOCKET_CHANNEL_METADATA_KEY,
)
from api_agents.models import Agent, AgentInstanceAssignment, AgentStatus
from sql.models import (
    Team,
    Instance,
    InstanceAccessLevel,
    SqlWorkflow,
    SqlWorkflowContent,
    WorkflowAudit,
    WorkflowLog,
    WorkflowAuditSetting,
    WorkflowPolicy,
    QueryLog,
    QueryPrivileges,
    QueryPrivilegesApply,
    PermanentTeamGrant,
    PermissionRequest,
    TemporaryInstanceGrant,
    TemporaryTeamGrant,
    TeamMembership,
    ArchiveConfig,
    ArchiveLog,
)
from common.utils.const import WorkflowAction, WorkflowStatus, WorkflowType
from sql.utils.team import user_groups, user_instances
import json

User = get_user_model()


def response_data(response):
    payload = response.json()
    return payload.get("data", payload)


def assert_success_envelope(testcase, response):
    payload = response.json()
    testcase.assertIn("detail", payload)
    testcase.assertIn("data", payload)
    return payload["data"]


def token_pair_for_user(user):
    SessionStore = import_module(settings.SESSION_ENGINE).SessionStore
    session = SessionStore()
    session[SESSION_KEY] = str(user.pk)
    session[BACKEND_SESSION_KEY] = "common.auth_backends.TeamPermissionBackend"
    session[HASH_SESSION_KEY] = user.get_session_auth_hash()
    session.save()
    access = allauth_jwt.create_access_token(user, session, {})
    refresh = allauth_jwt.create_refresh_token(user, session)
    session.save()
    return {"refresh": refresh, "access": access}


def authenticate_client(client, user):
    token_pair = token_pair_for_user(user)
    client.force_authenticate(user=user)
    return token_pair


def assign_user_to_team(user, team):
    permission_level = user.groups.exclude(name="superadmin").order_by("id").first()
    if permission_level is None:
        permission_level, _ = Group.objects.get_or_create(
            name=f"test-team-permissions-{user.pk}"
        )
    permission_ids = set(permission_level.permissions.values_list("id", flat=True))
    permission_ids.update(user.user_permissions.values_list("id", flat=True))
    permission_level.permissions.set(permission_ids)
    return TeamMembership.objects.update_or_create(
        user=user,
        team=team,
        defaults={"permission_level": permission_level},
    )[0]


def remove_team_permission(user, team, codename):
    membership = TeamMembership.objects.get(user=user, team=team)
    membership.permission_level.permissions.remove(
        Permission.objects.get(codename=codename)
    )


class CacheIsolatedAPITestCase(APITestCase):
    """Reset shared cache state so API throttles do not leak across tests."""

    def _pre_setup(self):
        cache.clear()
        super()._pre_setup()

    def _post_teardown(self):
        try:
            cache.clear()
        finally:
            super()._post_teardown()


class InfoTest(TestCase):
    def setUp(self) -> None:
        self.superuser = User.objects.create(username="super", is_superuser=True)
        self.client.force_login(self.superuser)

    def tearDown(self) -> None:
        self.superuser.delete()

    def test_info_api(self):
        r = self.client.get("/api/info")
        r_json = r.json()
        self.assertIsInstance(r_json["archery"]["version"], str)

    def test_debug_api(self):
        r = self.client.get("/api/debug")
        r_json = r.json()
        self.assertIsInstance(r_json["archery"]["version"], str)


class SystemSettingsTaskBackendTest(TestCase):
    def setUp(self):
        self.superuser = User.objects.create(
            username="settings_admin", is_superuser=True
        )
        self.client.force_login(self.superuser)
        self.sys_config = SysConfig()

    def tearDown(self):
        self.sys_config.replace(json.dumps({}))
        self.superuser.delete()

    def test_get_system_settings_includes_background_job_options(self):
        response = self.client.get("/api/v1/system-settings/")
        payload = response.json()

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertNotIn("task_backend", payload["data"]["settings"])
        self.assertNotIn("task_backends", payload["data"]["options"])
        self.assertEqual(
            payload["data"]["settings"]["inventory_refresh_interval"], "1h"
        )
        self.assertEqual(
            payload["data"]["options"]["inventory_refresh_intervals"],
            [
                {"value": interval, "label": interval}
                for interval in INVENTORY_REFRESH_INTERVAL_OPTIONS
            ],
        )

    def test_put_system_settings_saves_inventory_refresh_interval(self):
        response = self.client.put(
            "/api/v1/system-settings/",
            data=json.dumps(
                {
                    "inventory_refresh_interval": "6h",
                    "celery_broker_url": "redis://example:6379/5",
                    "celery_result_backend": "redis://example:6379/6",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.json()["data"]["settings"]["inventory_refresh_interval"], "6h"
        )
        self.assertTrue(response.json()["data"]["inventory_refresh_schedule_synced"])
        self.assertEqual(self.sys_config.get("inventory_refresh_interval"), "6h")

    @patch("api_admin.settings.sync_inventory_refresh_schedule", return_value=False)
    def test_put_system_settings_surfaces_inventory_schedule_sync_warning(
        self, _mock_sync
    ):
        response = self.client.put(
            "/api/v1/system-settings/",
            data=json.dumps(
                {
                    "inventory_refresh_interval": "12h",
                    "celery_broker_url": "redis://example:6379/5",
                    "celery_result_backend": "redis://example:6379/6",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            response.json()["detail"],
            "System settings updated, but the inventory refresh schedule could not be synchronized. Check the task backend and try again.",
        )
        self.assertFalse(response.json()["data"]["inventory_refresh_schedule_synced"])
        self.assertEqual(self.sys_config.get("inventory_refresh_interval"), "12h")

    def test_put_system_settings_requires_broker_url(self):
        response = self.client.put(
            "/api/v1/system-settings/",
            data=json.dumps(
                {
                    "celery_broker_url": "",
                    "celery_result_backend": "redis://example:6379/6",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("celery_broker_url", response.json())

    def test_put_system_settings_rejects_blank_broker_url(self):
        response = self.client.put(
            "/api/v1/system-settings/",
            data=json.dumps(
                {
                    "celery_broker_url": "   ",
                    "celery_result_backend": "redis://example:6379/6",
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("celery_broker_url", response.json())

    def test_put_system_settings_rejects_invalid_celery_time_limits(self):
        response = self.client.put(
            "/api/v1/system-settings/",
            data=json.dumps(
                {
                    "celery_broker_url": "redis://example:6379/5",
                    "celery_result_backend": "redis://example:6379/6",
                    "celery_task_soft_time_limit": 60,
                    "celery_task_time_limit": 60,
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("celery_task_soft_time_limit", response.json())

    def test_put_system_settings_rejects_non_positive_celery_time_limits(self):
        response = self.client.put(
            "/api/v1/system-settings/",
            data=json.dumps(
                {
                    "celery_broker_url": "redis://example:6379/5",
                    "celery_result_backend": "redis://example:6379/6",
                    "celery_task_soft_time_limit": 0,
                    "celery_task_time_limit": -1,
                }
            ),
            content_type="application/json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("celery_task_soft_time_limit", response.json())
        self.assertIn("celery_task_time_limit", response.json())

    def test_put_system_settings_saves_celery_config(self):
        response = self.client.put(
            "/api/v1/system-settings/",
            data=json.dumps(
                {
                    "celery_broker_url": "redis://example:6379/5",
                    "celery_result_backend": "redis://example:6379/6",
                    "celery_task_default_queue": "workers",
                    "celery_task_soft_time_limit": 30,
                    "celery_task_time_limit": 60,
                }
            ),
            content_type="application/json",
        )
        payload = response.json()

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertNotIn("task_backend", payload["data"]["settings"])
        self.assertEqual(
            payload["data"]["settings"]["celery_broker_url"],
            "redis://example:6379/5",
        )
        self.assertEqual(
            self.sys_config.get("celery_task_default_queue"),
            "workers",
        )


class TestUser(CacheIsolatedAPITestCase):
    """Test user-related APIs."""

    def setUp(self):
        self.user = User(
            username="test_user",
            display="Test User",
            is_active=True,
            is_superuser=True,
        )
        self.user.set_password("test_password")
        self.user.save()
        self.member_user = User.objects.create(
            username="group_member", display="Group Member", is_active=True
        )
        self.group = Group.objects.get(name="DBA")
        self.res_group = Team.objects.create(team_id=1, team_name="test")
        self.instance = Instance.objects.create(
            instance_name="test_instance",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
            password="pwd",
        )
        self.view_group_permission = Permission.objects.get(codename="view_group")
        self.menu_system_permission = Permission.objects.get(codename="menu_system")
        self.change_team_permission = Permission.objects.get(
            content_type__app_label="sql",
            codename="change_team",
        )
        self.token = authenticate_client(self.client, self.user)["access"]

    def tearDown(self):
        Instance.objects.all().delete()
        Team.objects.all().delete()
        Group.objects.all().delete()
        User.objects.all().delete()
        SysConfig().purge()

    def test_user_list_not_gated_by_whitelist(self):
        """API access is no longer gated by api_user_whitelist."""
        SysConfig().set("api_user_whitelist", "")
        r = self.client.get("/api/v1/user/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)

    def test_get_user_list(self):
        """Test getting user list."""
        r = self.client.get("/api/v1/user/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["count"], 2)
        self.assertEqual(
            set(payload["results"][0].keys()),
            {
                "id",
                "username",
                "display",
                "email",
                "is_active",
                "is_superuser",
                "is_staff",
                "teams",
                "team_ids",
            },
        )

    def test_get_user_list_with_search(self):
        """User list search matches username, display, email, and id."""
        self.member_user.email = "group.member@datamingle.test"
        self.member_user.save(update_fields=["email"])

        r = self.client.get("/api/v1/user/?search=member", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["results"][0]["username"], self.member_user.username)

    def test_get_user_list_with_ordering(self):
        """User list ordering supports username sorting."""
        User.objects.create(username="aaa_user", display="AAA User", is_active=True)

        r = self.client.get("/api/v1/user/?ordering=username", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["results"][0]["username"], "aaa_user")

    def test_get_current_user_context(self):
        """Test SPA bootstrap current-user endpoint."""
        r = self.client.get("/api/v1/me/", format="json")
        r_data = response_data(r)
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(r_data["username"], self.user.username)
        self.assertIn("permissions", r_data)
        self.assertIn("groups", r_data)
        self.assertIn("teams", r_data)

    def test_get_current_user_context_includes_workflow_menu_for_temporary_write_access(
        self,
    ):
        TemporaryInstanceGrant.objects.create(
            user=self.member_user,
            team=self.res_group,
            instance=self.instance,
            access_level="query_dml_ddl",
            valid_date=datetime.now().date() + timedelta(days=30),
        )

        self.client.credentials()
        authenticate_client(self.client, self.member_user)

        response = self.client.get("/api/v1/me/", format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        permissions = response_data(response)["permissions"]
        self.assertIn("sql.menu_sqlworkflow", permissions)
        self.assertIn("sql.sql_submit", permissions)

    def test_success_envelope_shape_for_paginated_and_detail_endpoints(self):
        """Success responses should use unified envelope for list and detail."""
        r1 = self.client.get("/api/v1/user/", format="json")
        self.assertEqual(r1.status_code, status.HTTP_200_OK)
        list_data = assert_success_envelope(self, r1)
        self.assertEqual(
            set(list_data.keys()), {"count", "next", "previous", "results"}
        )

        r2 = self.client.get("/api/v1/me/", format="json")
        self.assertEqual(r2.status_code, status.HTTP_200_OK)
        me_data = assert_success_envelope(self, r2)
        self.assertEqual(me_data["username"], self.user.username)

    def test_user_management_requires_superuser(self):
        """Delegated permissions do not grant access to user management."""
        User.objects.filter(id=self.user.id).update(is_superuser=0)
        self.user = User.objects.get(id=self.user.id)
        self.user.user_permissions.clear()
        self.client.credentials()
        authenticate_client(self.client, self.user)

        r1 = self.client.get("/api/v1/user/", format="json")
        self.assertEqual(r1.status_code, status.HTTP_403_FORBIDDEN)

        delegated_permission = Permission.objects.get(codename="view_users")
        self.user.user_permissions.add(delegated_permission)
        self.client.credentials()
        authenticate_client(self.client, self.user)
        r2 = self.client.get("/api/v1/user/", format="json")
        self.assertEqual(r2.status_code, status.HTTP_403_FORBIDDEN)

    def test_get_user_detail(self):
        """Test getting a single managed user."""
        assign_user_to_team(self.member_user, self.res_group)
        r = self.client.get(f"/api/v1/user/{self.member_user.id}/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["id"], self.member_user.id)
        self.assertEqual(payload["team_ids"], [self.res_group.team_id])
        self.assertEqual(payload["teams"][0]["team_name"], self.res_group.team_name)

    def test_update_user(self):
        """Superusers can update status but not identity or membership fields."""
        self.member_user.set_password("member_password")
        self.member_user.save(update_fields=["password"])
        json_data = {
            "display": "Updated Display Name",
            "email": "updated@datamingle.test",
            "team_ids": [self.res_group.team_id],
            "password": "",
        }
        r = self.client.put(
            f"/api/v1/user/{self.member_user.id}/", json_data, format="json"
        )
        user = User.objects.get(pk=self.member_user.id)
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(user.display, "Group Member")
        self.assertEqual(user.email, "")
        self.assertTrue(user.check_password("member_password"))
        self.assertFalse(user.team_memberships.exists())

    def test_deactivate_and_reactivate_user(self):
        """Managed users can be deactivated and reactivated."""
        deactivate_response = self.client.put(
            f"/api/v1/user/{self.member_user.id}/",
            {"is_active": False},
            format="json",
        )
        self.assertEqual(deactivate_response.status_code, status.HTTP_200_OK)
        self.member_user.refresh_from_db()
        self.assertFalse(self.member_user.is_active)

        reactivate_response = self.client.put(
            f"/api/v1/user/{self.member_user.id}/",
            {"is_active": True},
            format="json",
        )
        self.assertEqual(reactivate_response.status_code, status.HTTP_200_OK)
        self.member_user.refresh_from_db()
        self.assertTrue(self.member_user.is_active)

    def test_delete_user(self):
        """Superusers can delete existing local access records."""
        managed_user = User.objects.create(
            username="test_user2",
            display="Test User 2",
            workos_user_id="workos_test_user2",
        )
        r2 = self.client.delete(f"/api/v1/user/{managed_user.id}/", format="json")
        self.assertEqual(r2.status_code, status.HTTP_200_OK)
        self.assertEqual(User.objects.filter(username="test_user2").count(), 0)

    def test_delete_self_is_blocked(self):
        """Superusers cannot delete themselves."""
        r = self.client.delete(f"/api/v1/user/{self.user.id}/", format="json")
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("cannot delete your own account", json.dumps(r.json()).lower())

    def test_deactivate_self_is_blocked(self):
        """Superusers cannot deactivate themselves."""
        r = self.client.put(
            f"/api/v1/user/{self.user.id}/", {"is_active": False}, format="json"
        )
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn(
            "cannot deactivate your own account", json.dumps(r.json()).lower()
        )

    def test_get_permission_level_list(self):
        """Test getting permission levels."""
        r = self.client.get("/api/v1/permission-levels/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertIn("DBA", {level["name"] for level in response_data(r)})

    def test_create_permission_level(self):
        """Test creating a permission level."""
        json_data = {
            "name": "Test Developer",
            "permission_codes": ["sql.change_team"],
        }
        r = self.client.post("/api/v1/permission-levels/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response_data(r)["name"], "Test Developer")
        self.assertEqual(response_data(r)["permissions"], ["sql.change_team"])

    def test_get_permission_level_detail(self):
        """Test getting a single permission level."""
        self.group.permissions.add(self.change_team_permission)
        r = self.client.get(
            f"/api/v1/permission-levels/{self.group.id}/",
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(response_data(r)["id"], self.group.id)
        self.assertIn("sql.change_team", response_data(r)["permissions"])

    def test_update_permission_level(self):
        """Test updating a permission level."""
        json_data = {
            "name": "Updated Group Name",
            "permission_codes": ["sql.change_team"],
        }
        r = self.client.put(
            f"/api/v1/permission-levels/{self.group.id}/",
            json_data,
            format="json",
        )
        group = Group.objects.get(pk=self.group.id)
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(group.name, "Updated Group Name")
        self.assertEqual(
            list(group.permissions.values_list("id", flat=True)),
            [self.change_team_permission.id],
        )

    def test_delete_permission_level(self):
        """Test deleting an unused permission level."""
        level = Group.objects.create(name="Unused")
        r = self.client.delete(
            f"/api/v1/permission-levels/{level.id}/",
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertFalse(Group.objects.filter(pk=level.id).exists())

    def test_get_permission_catalog(self):
        """Test getting assignable permission catalog."""
        r = self.client.get(
            "/api/v1/permission-levels/available-permissions/",
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        permissions = [
            permission
            for category in response_data(r)
            for permission in category["permissions"]
        ]
        matching_permission = next(
            permission
            for permission in permissions
            if permission["code"] == "sql.change_team"
        )
        self.assertEqual(matching_permission["codename"], "change_team")

    def test_get_team_list(self):
        """Test getting team list."""
        assign_user_to_team(self.member_user, self.res_group)
        self.res_group.instance_set.add(self.instance)
        r = self.client.get("/api/v1/teams/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["results"][0]["team_name"], "test")
        self.assertEqual(payload["results"][0]["user_count"], 1)
        self.assertEqual(payload["results"][0]["service_count"], 1)

    def test_get_team_list_with_search_and_deleted_filter(self):
        """Search should match name or ID and skip deleted groups."""
        deleted_group = Team.objects.create(team_name="hidden", is_deleted=1)
        visible_group = Team.objects.create(team_name="analytics")
        r = self.client.get(
            f"/api/v1/teams/?search={visible_group.team_id}",
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["results"][0]["team_name"], "analytics")
        self.assertEqual(Team.objects.filter(team_id=deleted_group.team_id).count(), 1)

    def test_get_team_list_with_ordering(self):
        """Ordering supports membership counts."""
        busy_group = Team.objects.create(team_name="busy")
        assign_user_to_team(self.user, busy_group)
        assign_user_to_team(self.member_user, busy_group)
        idle_group = Team.objects.create(team_name="idle")
        idle_group.instance_set.add(self.instance)

        r = self.client.get(
            "/api/v1/teams/?ordering=-user_count",
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["results"][0]["team_name"], "busy")

    def test_get_team_detail(self):
        """Test getting a single team with memberships."""
        membership = assign_user_to_team(self.member_user, self.res_group)
        self.res_group.instance_set.add(self.instance)

        r = self.client.get(f"/api/v1/teams/{self.res_group.team_id}/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["team_id"], self.res_group.team_id)
        self.assertEqual(payload["user_access"][0]["user_id"], self.member_user.id)
        self.assertEqual(
            payload["user_access"][0]["permission_level_id"],
            membership.permission_level_id,
        )
        self.assertEqual(payload["service_ids"], [self.instance.id])
        self.assertEqual(payload["user_count"], 1)
        self.assertEqual(payload["service_count"], 1)

    def test_create_team(self):
        """Test creating team."""
        json_data = {
            "team_name": "prod",
            "user_access": [
                {
                    "user_id": self.member_user.id,
                    "permission_level_id": self.group.id,
                }
            ],
            "node_ids": [],
            "service_ids": [self.instance.id],
        }
        r = self.client.post("/api/v1/teams/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        payload = response_data(r)
        self.assertEqual(payload["team_name"], "prod")
        self.assertEqual(payload["user_access"][0]["user_id"], self.member_user.id)
        self.assertEqual(payload["service_ids"], [self.instance.id])

    def test_update_team(self):
        """Test updating team."""
        json_data = {
            "team_name": "Updated Team Name",
            "user_access": [
                {
                    "user_id": self.member_user.id,
                    "permission_level_id": self.group.id,
                }
            ],
            "node_ids": [],
            "service_ids": [self.instance.id],
        }
        r = self.client.put(
            f"/api/v1/teams/{self.res_group.team_id}/",
            json_data,
            format="json",
        )
        group = Team.objects.get(pk=self.res_group.team_id)
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(group.team_name, "Updated Team Name")
        self.assertEqual(
            list(group.memberships.values_list("user_id", flat=True)),
            [self.member_user.id],
        )
        self.assertEqual(
            list(group.instance_set.values_list("id", flat=True)), [self.instance.id]
        )

    def test_delete_team(self):
        """Test deleting team."""
        r = self.client.delete(
            f"/api/v1/teams/{self.res_group.team_id}/", format="json"
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(Team.objects.filter(team_id=self.res_group.team_id).count(), 0)

    def test_team_user_lookup(self):
        """Lookup returns lightweight user records."""
        r = self.client.get(
            "/api/v1/teams/users/lookup/?search=group",
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["id"], self.member_user.id)
        self.assertEqual(payload[0]["label"], "Group Member")

    def test_team_instance_lookup(self):
        """Lookup returns lightweight instance records."""
        r = self.client.get(
            "/api/v1/teams/services/lookup/?search=test_instance",
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["id"], self.instance.id)
        self.assertIn("test_instance", payload[0]["label"])


class TestSPATokenHelpers(CacheIsolatedAPITestCase):
    """Test allauth headless token refresh for the SPA."""

    def setUp(self):
        self.user = User(
            username="token_2fa_user",
            display="Token 2FA User",
            is_active=True,
        )
        self.user.set_password("test_password")
        self.user.save()

    def tearDown(self):
        self.user.delete()
        SysConfig().purge()

    def test_token_refresh_success_envelope(self):
        refresh_token = token_pair_for_user(self.user)["refresh"]

        r = self.client.post(
            "/api/_allauth/app/v1/tokens/refresh",
            {"refresh_token": refresh_token},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertIn("access_token", r.json()["data"])

    def test_token_refresh_invalid_token_returns_error_contract(self):
        r = self.client.post(
            "/api/_allauth/app/v1/tokens/refresh",
            {"refresh_token": "invalid.refresh.token"},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)


class TestQueryAPI(CacheIsolatedAPITestCase):
    """Test query/query-privilege API endpoints."""

    def setUp(self):
        self.user = User(username="query_user", display="Query User", is_active=True)
        self.user.set_password("test_password")
        self.user.save()

        permissions = Permission.objects.filter(
            codename__in=[
                "query_submit",
                "menu_sqlquery",
                "menu_queryapplylist",
                "query_applypriv",
                "query_mgtpriv",
                "query_review",
            ]
        )
        self.user.user_permissions.add(*permissions)

        self.group = Group.objects.create(name="Query Group")
        self.user.groups.add(self.group)
        self.res_group = Team.objects.create(team_name="query_rg")
        assign_user_to_team(self.user, self.res_group)

        self.ins = Instance.objects.create(
            instance_name="query_instance",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
            password="pwd",
        )
        self.ins.resource_group.add(self.res_group)

        self.token = authenticate_client(self.client, self.user)["access"]

    def tearDown(self):
        self.user.delete()
        QueryLog.objects.all().delete()
        QueryPrivileges.objects.all().delete()
        QueryPrivilegesApply.objects.all().delete()
        Instance.objects.all().delete()
        Team.objects.all().delete()
        Group.objects.all().delete()
        SysConfig().purge()

    @patch("api_queries.views.query_priv_check")
    @patch("api_queries.views.get_engine")
    def test_query_execute_success(self, mock_get_engine, mock_query_priv_check):
        SysConfig().set("data_masking", False)
        SysConfig().set("disable_star", False)

        mock_query_priv_check.return_value = {
            "status": 0,
            "msg": "ok",
            "data": {"priv_check": True, "limit_num": 100},
        }
        mock_engine = Mock()
        mock_engine.query_check.return_value = {
            "bad_query": False,
            "has_star": False,
            "filtered_sql": "select 1",
        }
        mock_engine.filter_sql.return_value = "select 1"
        mock_engine.thread_id = None
        mock_engine.get_connection.return_value = None
        mock_engine.seconds_behind_master = 0
        result = ResultSet(rows=[(1,)], column_list=["v"], affected_rows=1)
        result.error = None
        mock_engine.query.return_value = result
        mock_get_engine.return_value = mock_engine

        r = self.client.post(
            "/api/v1/query/",
            {
                "instance_name": self.ins.instance_name,
                "sql_content": "select 1",
                "db_name": "mysql",
                "limit_num": 10,
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(r.json()["detail"], "ok")
        self.assertEqual(r.json()["data"]["rows"][0]["v"], 1)
        self.assertEqual(QueryLog.objects.count(), 1)

    def test_query_instance_list_returns_can_read_instances(self):
        hidden_instance = Instance.objects.create(
            instance_name="hidden_instance",
            type="master",
            db_type="pgsql",
            host="127.0.0.1",
            port=5432,
            user="postgres",
            password="pwd",
        )
        hidden_instance.resource_group.add(self.res_group)

        r = self.client.get("/api/v1/query/instance/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        data = assert_success_envelope(self, r)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["instance_name"], self.ins.instance_name)

    @patch("api_queries.views.get_engine")
    def test_query_describe_success(self, mock_get_engine):
        mock_engine = Mock()
        mock_engine.escape_string.side_effect = lambda value: value
        mock_result = ResultSet(
            full_sql="show create table `users`;",
            rows=[("users", "CREATE TABLE `users` (`id` bigint);")],
            column_list=["Table", "Create Table"],
        )
        mock_result.error = None
        mock_engine.describe_table.return_value = mock_result
        mock_get_engine.return_value = mock_engine

        r = self.client.post(
            "/api/v1/query/describe/",
            {
                "instance_id": self.ins.id,
                "db_name": "archery",
                "tb_name": "users",
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        data = assert_success_envelope(self, r)
        self.assertEqual(data["display_mode"], "ddl")
        self.assertEqual(data["rows"][0]["Table"], "users")

    def test_query_describe_rejects_unrelated_instance(self):
        other_group = Team.objects.create(team_name="other_rg")
        other_instance = Instance.objects.create(
            instance_name="other_instance",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
            password="pwd",
        )
        other_instance.resource_group.add(other_group)

        r = self.client.post(
            "/api/v1/query/describe/",
            {
                "instance_id": other_instance.id,
                "db_name": "archery",
                "tb_name": "users",
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)

    def test_query_log_and_favorite(self):
        QueryLog.objects.create(
            username=self.user.username,
            user_display=self.user.display,
            db_name="db1",
            instance_name=self.ins.instance_name,
            sqllog="select 1",
            effect_row=1,
            cost_time="0.1",
        )
        QueryLog.objects.create(
            username="other",
            user_display="Other",
            db_name="db1",
            instance_name=self.ins.instance_name,
            sqllog="select 2",
            effect_row=1,
            cost_time="0.1",
        )

        r1 = self.client.get("/api/v1/query/log/", format="json")
        self.assertEqual(r1.status_code, status.HTTP_200_OK)
        self.assertEqual(response_data(r1)["count"], 1)

        query_log_id = response_data(r1)["results"][0]["id"]
        r2 = self.client.post(
            "/api/v1/query/favorite/",
            {"query_log_id": query_log_id, "star": True, "alias": "fav1"},
            format="json",
        )
        self.assertEqual(r2.status_code, status.HTTP_200_OK)
        self.assertEqual(r2.json()["detail"], "ok")
        log_obj = QueryLog.objects.get(id=query_log_id)
        self.assertEqual(log_obj.favorite, True)
        self.assertEqual(log_obj.alias, "fav1")

        r3 = self.client.get("/api/v1/query/favorite/", format="json")
        self.assertEqual(r3.status_code, status.HTTP_200_OK)
        favorite_data = assert_success_envelope(self, r3)
        self.assertEqual(len(favorite_data), 1)
        self.assertEqual(favorite_data[0]["alias"], "fav1")

    def test_query_log_list_is_owner_scoped_for_superuser(self):
        self.user.is_superuser = True
        self.user.save(update_fields=["is_superuser"])

        QueryLog.objects.create(
            username=self.user.username,
            user_display=self.user.display,
            db_name="db1",
            instance_name=self.ins.instance_name,
            sqllog="select 1",
            effect_row=1,
            cost_time="0.1",
        )
        QueryLog.objects.create(
            username="other",
            user_display="Other",
            db_name="db2",
            instance_name=self.ins.instance_name,
            sqllog="select 2",
            effect_row=1,
            cost_time="0.2",
        )

        response = self.client.get("/api/v1/query/log/", format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = assert_success_envelope(self, response)
        self.assertEqual(data["count"], 1)
        self.assertEqual(data["results"][0]["sqllog"], "select 1")

    def test_query_favorite_rejects_other_users_log(self):
        foreign_log = QueryLog.objects.create(
            username="other",
            user_display="Other",
            db_name="db1",
            instance_name=self.ins.instance_name,
            sqllog="select 2",
            effect_row=1,
            cost_time="0.1",
        )

        response = self.client.post(
            "/api/v1/query/favorite/",
            {"query_log_id": foreign_log.id, "star": True, "alias": "fav-other"},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(response.json()["errors"], "Query log does not exist.")

    def test_query_log_filters_unstarred(self):
        QueryLog.objects.create(
            username=self.user.username,
            user_display=self.user.display,
            db_name="db1",
            instance_name=self.ins.instance_name,
            sqllog="select 1",
            effect_row=1,
            cost_time="0.1",
            favorite=True,
            alias="fav1",
        )
        QueryLog.objects.create(
            username=self.user.username,
            user_display=self.user.display,
            db_name="db2",
            instance_name=self.ins.instance_name,
            sqllog="select 2",
            effect_row=1,
            cost_time="0.2",
            favorite=False,
        )

        r = self.client.get("/api/v1/query/log/", {"star": "false"}, format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        data = assert_success_envelope(self, r)
        self.assertEqual(data["count"], 1)
        self.assertEqual(data["results"][0]["db_name"], "db2")

    def test_query_log_audit_requires_audit_permission(self):
        r = self.client.get("/api/v1/query/log/audit/", format="json")
        self.assertEqual(r.status_code, status.HTTP_403_FORBIDDEN)

    def test_query_log_audit_list_for_auditor(self):
        audit_user_perm = Permission.objects.get(codename="audit_user")
        self.user.user_permissions.add(audit_user_perm)

        QueryLog.objects.create(
            username=self.user.username,
            user_display=self.user.display,
            db_name="db1",
            instance_name=self.ins.instance_name,
            sqllog="select 1",
            effect_row=1,
            cost_time="0.1",
        )
        QueryLog.objects.create(
            username="another_user",
            user_display="Another",
            db_name="db1",
            instance_name=self.ins.instance_name,
            sqllog="select 2",
            effect_row=1,
            cost_time="0.1",
        )

        r = self.client.get("/api/v1/query/log/audit/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        audit_data = assert_success_envelope(self, r)
        self.assertEqual(audit_data["count"], 2)
        self.assertEqual(len(audit_data["results"]), 2)

    @patch("api_queries.views.async_task")
    @patch("api_queries.views._query_apply_audit_call_back")
    @patch("api_queries.views.get_auditor")
    def test_query_privilege_apply_create(
        self, mock_get_auditor, mock_callback, mock_async_task
    ):
        mock_handler = Mock()
        mock_handler.workflow.apply_id = 123
        mock_handler.audit.current_status = WorkflowStatus.WAITING
        mock_get_auditor.return_value = mock_handler

        r = self.client.post(
            "/api/v1/query/privilege/apply/",
            {
                "title": "apply db read",
                "instance_name": self.ins.instance_name,
                "team_name": self.res_group.team_name,
                "priv_type": 1,
                "db_list": ["db1"],
                "valid_date": "2099-12-31",
                "limit_num": 100,
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        self.assertEqual(r.json()["detail"], "ok")
        self.assertEqual(r.json()["data"]["apply_id"], 123)
        mock_callback.assert_called_once()
        mock_async_task.assert_called_once()

    def test_query_privilege_list_and_modify(self):
        QueryPrivilegesApply.objects.create(
            team_id=self.res_group.team_id,
            team_name=self.res_group.team_name,
            title="history apply",
            user_name=self.user.username,
            user_display=self.user.display,
            instance=self.ins,
            db_list="db1",
            table_list="",
            valid_date=datetime.now().date() + timedelta(days=3),
            limit_num=10,
            priv_type=1,
            status=WorkflowStatus.WAITING,
            audit_auth_groups="1",
        )
        priv = QueryPrivileges.objects.create(
            user_name=self.user.username,
            user_display=self.user.display,
            instance=self.ins,
            db_name="db1",
            table_name="",
            valid_date=datetime.now().date() + timedelta(days=3),
            limit_num=10,
            priv_type=1,
            is_deleted=0,
        )

        r1 = self.client.get("/api/v1/query/privilege/", format="json")
        self.assertEqual(r1.status_code, status.HTTP_200_OK)
        self.assertEqual(response_data(r1)["count"], 1)

        r2 = self.client.patch(
            f"/api/v1/query/privilege/{priv.privilege_id}/",
            {"valid_date": "2099-12-31", "limit_num": 200},
            format="json",
        )
        self.assertEqual(r2.status_code, status.HTTP_200_OK)
        priv.refresh_from_db()
        self.assertEqual(priv.limit_num, 200)

        r3 = self.client.delete(f"/api/v1/query/privilege/{priv.privilege_id}/")
        self.assertEqual(r3.status_code, status.HTTP_200_OK)
        priv.refresh_from_db()
        self.assertEqual(priv.is_deleted, 1)

    def test_query_privilege_patch_requires_manage_permission(self):
        priv = QueryPrivileges.objects.create(
            user_name=self.user.username,
            user_display=self.user.display,
            instance=self.ins,
            db_name="db1",
            table_name="",
            valid_date=datetime.now().date() + timedelta(days=3),
            limit_num=10,
            priv_type=1,
            is_deleted=0,
        )
        self.user.user_permissions.remove(
            Permission.objects.get(codename="query_mgtpriv")
        )

        r = self.client.patch(
            f"/api/v1/query/privilege/{priv.privilege_id}/",
            {"valid_date": "2099-12-31", "limit_num": 200},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_403_FORBIDDEN)

    def test_query_privilege_patch_requires_valid_date_and_limit_num(self):
        priv = QueryPrivileges.objects.create(
            user_name=self.user.username,
            user_display=self.user.display,
            instance=self.ins,
            db_name="db1",
            table_name="",
            valid_date=datetime.now().date() + timedelta(days=3),
            limit_num=10,
            priv_type=1,
            is_deleted=0,
        )

        r = self.client.patch(
            f"/api/v1/query/privilege/{priv.privilege_id}/",
            {"valid_date": "2099-12-31"},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        error_msg = r.json()["errors"]
        if isinstance(error_msg, list):
            error_msg = error_msg[0]
        self.assertEqual(
            error_msg, "valid_date and limit_num are required when type is 2."
        )

    def test_query_privilege_review_requires_review_permission(self):
        apply_obj = QueryPrivilegesApply.objects.create(
            team_id=self.res_group.team_id,
            team_name=self.res_group.team_name,
            title="apply one",
            user_name=self.user.username,
            user_display=self.user.display,
            instance=self.ins,
            db_list="db1",
            table_list="",
            valid_date=datetime.now().date() + timedelta(days=3),
            limit_num=10,
            priv_type=1,
            status=WorkflowStatus.WAITING,
            audit_auth_groups="1",
        )
        self.user.user_permissions.remove(
            Permission.objects.get(codename="query_review")
        )

        r = self.client.post(
            f"/api/v1/query/privilege/apply/{apply_obj.apply_id}/reviews/",
            {"audit_status": WorkflowAction.PASS, "audit_remark": "ok"},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_403_FORBIDDEN)

    def test_query_privilege_review_rejects_invalid_audit_status(self):
        r = self.client.post(
            "/api/v1/query/privilege/apply/999/reviews/",
            {"audit_status": 999, "audit_remark": "invalid"},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("Invalid audit_status parameter", r.json()["errors"])

    @patch("api_queries.views.async_task")
    @patch("api_queries.views._query_apply_audit_call_back")
    @patch("api_queries.views.get_auditor")
    def test_query_privilege_audit(
        self, mock_get_auditor, mock_callback, mock_async_task
    ):
        apply_obj = QueryPrivilegesApply.objects.create(
            team_id=self.res_group.team_id,
            team_name=self.res_group.team_name,
            title="apply one",
            user_name=self.user.username,
            user_display=self.user.display,
            instance=self.ins,
            db_list="db1",
            table_list="",
            valid_date=datetime.now().date() + timedelta(days=3),
            limit_num=10,
            priv_type=1,
            status=WorkflowStatus.WAITING,
            audit_auth_groups="1",
        )

        mock_handler = Mock()
        mock_handler.audit.workflow_id = apply_obj.apply_id
        mock_handler.audit.current_status = WorkflowStatus.PASSED
        mock_handler.operate.return_value = Mock()
        mock_get_auditor.return_value = mock_handler

        r = self.client.post(
            f"/api/v1/query/privilege/apply/{apply_obj.apply_id}/reviews/",
            {"audit_status": WorkflowAction.PASS, "audit_remark": "ok"},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(r.json()["detail"], "ok")
        mock_callback.assert_called_once()
        mock_async_task.assert_called_once()


class TestInstance(CacheIsolatedAPITestCase):
    """Test instance-related APIs."""

    def setUp(self):
        self.user = User(username="test_user", display="Test User", is_active=True)
        self.user.set_password("test_password")
        self.user.save()
        menu_instance_list = Permission.objects.get(codename="menu_instance_list")
        menu_instance = Permission.objects.get(codename="menu_instance")
        self.user.user_permissions.add(menu_instance_list, menu_instance)
        self.ins = Instance.objects.create(
            instance_name="some_ins",
            type="slave",
            db_type="mysql",
            host="some_host",
            port=3306,
            user="ins_user",
            password="some_str",
        )
        self.token = authenticate_client(self.client, self.user)["access"]

    def tearDown(self):
        User.objects.all().delete()
        Instance.objects.all().delete()
        Team.objects.all().delete()
        SysConfig().purge()

    def test_get_instance_list(self):
        """Test getting instance list."""
        r = self.client.get("/api/v1/instance/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(response_data(r)["count"], 1)

    def test_get_instance_list_includes_inventory_snapshot_fields(self):
        self.ins.inventory_status = Instance.INVENTORY_STATUS_OK
        self.ins.inventory_detected_hostname = "detected-host"
        self.ins.inventory_detected_version = "8.0.36"
        self.ins.inventory_last_success_at = datetime.now()
        self.ins.save(
            update_fields=[
                "inventory_status",
                "inventory_detected_hostname",
                "inventory_detected_version",
                "inventory_last_success_at",
            ]
        )

        r = self.client.get("/api/v1/instance/", format="json")

        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)["results"][0]
        self.assertEqual(payload["inventory_status"], "ok")
        self.assertEqual(payload["inventory_detected_hostname"], "detected-host")
        self.assertEqual(payload["inventory_detected_version"], "8.0.36")
        self.assertIsNotNone(payload["inventory_last_refresh_at"])

    def test_get_instance_list_with_search_and_filters(self):
        """Search and filters should match legacy inventory behavior."""
        other_instance = Instance.objects.create(
            instance_name="analytics",
            type="master",
            db_type="pgsql",
            host="analytics-db",
            port=5432,
            user="reader",
            password="secret",
        )

        r = self.client.get(
            "/api/v1/instance/",
            {
                "search": "some",
                "type": "slave",
                "db_type": "mysql",
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["results"][0]["instance_name"], "some_ins")

    def test_get_instance_list_with_ordering(self):
        """Ordering should support the SPA table headers."""
        Instance.objects.create(
            instance_name="aaa_ins",
            type="master",
            db_type="mysql",
            host="db-a",
            port=3307,
            user="z_user",
            password="secret",
        )

        r = self.client.get("/api/v1/instance/?ordering=instance_name", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["results"][0]["instance_name"], "aaa_ins")

    def test_get_instance_metadata(self):
        """Metadata should return list and create-form dependencies."""
        visible_group = Team.objects.create(team_name="Visible Group")
        Team.objects.create(team_name="Deleted Group", is_deleted=1)

        r = self.client.get("/api/v1/instance/metadata/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertTrue(
            any(item["value"] == "master" for item in payload["instance_types"])
        )
        self.assertTrue(any(item["value"] == "mysql" for item in payload["db_types"]))
        self.assertEqual(payload["teams"][0]["team_id"], visible_group.team_id)

    def test_create_instance(self):
        """Test creating instance."""
        json_data = {
            "instance_name": "test_ins",
            "type": "master",
            "db_type": "mysql",
            "host": "some_host",
            "port": 3306,
        }
        r = self.client.post("/api/v1/instance/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response_data(r)["instance_name"], "test_ins")

    def test_get_instance_detail(self):
        """Detail should expose the fields needed by the SPA edit form."""
        team = Team.objects.create(team_name="Detail Group")
        self.ins.resource_group.add(team)
        self.ins.show_db_name_regex = "^detail_.*$"
        self.ins.denied_db_name_regex = "^mysql$"
        self.ins.charset = "utf8mb4"
        self.ins.save(
            update_fields=[
                "show_db_name_regex",
                "denied_db_name_regex",
                "charset",
            ]
        )

        r = self.client.get(f"/api/v1/instance/{self.ins.id}/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)

        payload = response_data(r)
        self.assertEqual(payload["id"], self.ins.id)
        self.assertEqual(payload["instance_name"], "some_ins")
        self.assertEqual(payload["show_db_name_regex"], "^detail_.*$")
        self.assertEqual(payload["denied_db_name_regex"], "^mysql$")
        self.assertEqual(payload["charset"], "utf8mb4")
        self.assertEqual(payload["team_ids"], [team.team_id])

    def test_create_instance_with_relationships(self):
        """Create should accept SPA relationship IDs and optional fields."""
        team = Team.objects.create(team_name="Inventory Group")
        json_data = {
            "instance_name": "inventory_ins",
            "type": "master",
            "db_type": "mysql",
            "host": "inventory-host",
            "port": 3306,
            "user": "inventory_user",
            "password": "secret",
            "is_ssl": True,
            "verify_ssl": False,
            "db_name": "inventory_db",
            "charset": "utf8mb4",
            "show_db_name_regex": "^inventory_.*$",
            "denied_db_name_regex": "^mysql$",
            "team_ids": [team.team_id],
        }
        r = self.client.post("/api/v1/instance/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)

        payload = response_data(r)
        self.assertEqual(payload["instance_name"], "inventory_ins")
        self.assertEqual(payload["team_ids"], [team.team_id])

        instance = Instance.objects.get(instance_name="inventory_ins")
        self.assertEqual(
            list(instance.resource_group.values_list("team_id", flat=True)),
            [team.team_id],
        )

    def test_test_draft_instance_connection_requires_saved_agent_assignment(self):
        self.client.force_authenticate(user=self.user)
        payload = {
            "instance_name": "draft_mysql",
            "type": "master",
            "db_type": "mysql",
            "host": "draft-host",
            "port": 3306,
            "user": "draft_user",
            "password": "draft_password",
            "is_ssl": True,
            "verify_ssl": False,
            "db_name": "draft_db",
            "charset": "utf8mb4",
            "show_db_name_regex": "^draft_.*$",
            "denied_db_name_regex": "^mysql$",
        }
        r = self.client.post(
            "/api/v1/instance/test-connection/",
            payload,
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            Instance.objects.filter(instance_name="draft_mysql").count(), 0
        )
        self.assertEqual(
            r.json()["errors"],
            "Save the service and assign it to an online agent before testing the connection.",
        )

    @patch("sql.inventory.collect_inventory_snapshot")
    def test_test_saved_instance_connection_refreshes_agent_inventory(
        self, collect_inventory
    ):
        self.user.is_superuser = True
        self.user.save(update_fields=["is_superuser"])
        self.client.force_authenticate(user=self.user)
        collect_inventory.return_value = {
            "hostname": "detected-host",
            "version": "8.0.36",
        }

        response = self.client.post(
            f"/api/v1/instance/{self.ins.id}/test-connection/",
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.ins.refresh_from_db()
        self.assertEqual(self.ins.inventory_status, "ok")
        self.assertEqual(self.ins.inventory_detected_hostname, "detected-host")
        self.assertEqual(self.ins.inventory_detected_version, "8.0.36")

    def test_update_instance(self):
        """Test updating instance."""
        json_data = {"instance_name": "Updated Instance Name"}
        r = self.client.put(
            f"/api/v1/instance/{self.ins.id}/", json_data, format="json"
        )
        ins = Instance.objects.get(pk=self.ins.id)
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(ins.instance_name, "Updated Instance Name")

    def test_update_instance_with_relationships(self):
        """Update should persist SPA relationship IDs for groups."""
        team = Team.objects.create(team_name="Updated Group")
        json_data = {
            "instance_name": "Updated Instance Name",
            "type": "master",
            "db_type": "mysql",
            "host": "updated-host",
            "port": 3307,
            "user": "updated-user",
            "password": "",
            "is_ssl": True,
            "verify_ssl": False,
            "db_name": "updated_db",
            "show_db_name_regex": "^updated_.*$",
            "denied_db_name_regex": "^mysql$",
            "charset": "utf8mb4",
            "service_name": "",
            "sid": "",
            "team_ids": [team.team_id],
        }
        r = self.client.put(
            f"/api/v1/instance/{self.ins.id}/", json_data, format="json"
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)

        self.ins.refresh_from_db()
        payload = response_data(r)
        self.assertEqual(self.ins.instance_name, "Updated Instance Name")
        self.assertEqual(self.ins.host, "updated-host")
        self.assertEqual(self.ins.port, 3307)
        self.assertEqual(
            list(self.ins.resource_group.values_list("team_id", flat=True)),
            [team.team_id],
        )
        self.assertEqual(payload["team_ids"], [team.team_id])

    def test_delete_instance(self):
        """Test deleting instance."""
        r = self.client.delete(f"/api/v1/instance/{self.ins.id}/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(Instance.objects.filter(instance_name="some_ins").count(), 0)


class TestPermissionRequestAPI_Legacy(CacheIsolatedAPITestCase):
    def setUp(self):
        self.review_group = Group.objects.create(name="Permission Approvers")
        self.team = Team.objects.create(team_name="permission-rg")
        self.instance = Instance.objects.create(
            instance_name="permission-instance",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
            password="pwd",
        )
        self.instance.resource_group.add(self.team)

        self.requester = User(
            username="permission_requester",
            display="Permission Requester",
            is_active=True,
        )
        self.requester.set_password("test_password")
        self.requester.save()
        self.requester.user_permissions.add(
            Permission.objects.get(codename="query_applypriv"),
            Permission.objects.get(codename="menu_queryapplylist"),
        )

        self.reviewer = User(
            username="permission_reviewer",
            display="Permission Reviewer",
            is_active=True,
        )
        self.reviewer.set_password("test_password")
        self.reviewer.save()
        self.reviewer.user_permissions.add(
            Permission.objects.get(codename="query_review"),
            Permission.objects.get(codename="query_mgtpriv"),
            Permission.objects.get(codename="menu_queryapplylist"),
        )
        self.reviewer.groups.add(self.review_group)
        assign_user_to_team(self.reviewer, self.team)

        self.query_user = User(
            username="permission_query_user",
            display="Permission Query User",
            is_active=True,
        )
        self.query_user.set_password("test_password")
        self.query_user.save()
        self.query_user.user_permissions.add(
            Permission.objects.get(codename="menu_queryapplylist"),
        )

        WorkflowAuditSetting.objects.create(
            team_id=self.team.team_id,
            team_name=self.team.team_name,
            workflow_type=WorkflowType.ACCESS_REQUEST,
            audit_auth_groups=str(self.review_group.id),
        )

    def tearDown(self):
        TemporaryInstanceGrant.objects.all().delete()
        TemporaryTeamGrant.objects.all().delete()
        PermanentTeamGrant.objects.all().delete()
        PermissionRequest.objects.all().delete()
        WorkflowAudit.objects.filter(workflow_type=WorkflowType.ACCESS_REQUEST).delete()
        WorkflowLog.objects.all().delete()
        WorkflowAuditSetting.objects.filter(
            workflow_type=WorkflowType.ACCESS_REQUEST
        ).delete()
        Instance.objects.filter(id=self.instance.id).delete()
        Team.objects.filter(team_id=self.team.team_id).delete()
        Group.objects.filter(id=self.review_group.id).delete()
        User.objects.filter(
            id__in=[self.requester.id, self.reviewer.id, self.query_user.id]
        ).delete()

    def _login(self, user):
        authenticate_client(self.client, user)

    @patch("api_access.views.async_task")
    def test_create_instance_request(self, _async_task):
        self._login(self.requester)

        r = self.client.post(
            "/api/v1/access/request/",
            {
                "title": "Need DML on one instance",
                "reason": "Investigation",
                "target_type": "instance",
                "team_id": self.team.team_id,
                "instance_id": self.instance.id,
                "access_level": "query_dml",
                "valid_date": "2099-12-31",
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        request_obj = PermissionRequest.objects.get(
            request_id=response_data(r)["request_id"]
        )
        self.assertEqual(request_obj.target_type, "instance")
        self.assertEqual(request_obj.access_level, "query_dml")
        self.assertEqual(request_obj.team_id, self.team.team_id)
        self.assertEqual(request_obj.instance_id, self.instance.id)

    @patch("api_access.views.async_task")
    def test_reviewer_sees_pending_request(self, _async_task):
        self._login(self.requester)
        create_response = self.client.post(
            "/api/v1/access/request/",
            {
                "title": "Need query access",
                "target_type": "instance",
                "team_id": self.team.team_id,
                "instance_id": self.instance.id,
                "access_level": "query",
                "valid_date": "2099-12-31",
            },
            format="json",
        )
        self.assertEqual(create_response.status_code, status.HTTP_201_CREATED)

        self._login(self.reviewer)
        list_response = self.client.get("/api/v1/access/request/", format="json")
        self.assertEqual(list_response.status_code, status.HTTP_200_OK)
        self.assertEqual(response_data(list_response)["count"], 1)

    @patch("api_access.views.async_task")
    def test_approving_instance_request_creates_instance_grant(self, _async_task):
        self._login(self.requester)
        create_response = self.client.post(
            "/api/v1/access/request/",
            {
                "title": "Need query access",
                "target_type": "instance",
                "team_id": self.team.team_id,
                "instance_id": self.instance.id,
                "access_level": "query",
                "valid_date": "2099-12-31",
            },
            format="json",
        )
        request_id = response_data(create_response)["request_id"]

        self._login(self.reviewer)
        review_response = self.client.post(
            f"/api/v1/access/request/{request_id}/reviews/",
            {"audit_status": WorkflowAction.PASS, "audit_remark": "approved"},
            format="json",
        )
        self.assertEqual(review_response.status_code, status.HTTP_200_OK)
        self.assertTrue(
            TemporaryInstanceGrant.objects.filter(source_request_id=request_id).exists()
        )
        self.assertIn(self.instance, list(user_instances(self.requester)))

    @patch("api_access.views.async_task")
    def test_approving_group_request_creates_group_grant(self, _async_task):
        self._login(self.requester)
        create_response = self.client.post(
            "/api/v1/access/request/",
            {
                "title": "Need group access",
                "target_type": "team",
                "team_id": self.team.team_id,
                "valid_date": "2099-12-31",
            },
            format="json",
        )
        request_id = response_data(create_response)["request_id"]

        self._login(self.reviewer)
        review_response = self.client.post(
            f"/api/v1/access/request/{request_id}/reviews/",
            {"audit_status": WorkflowAction.PASS, "audit_remark": "approved"},
            format="json",
        )
        self.assertEqual(review_response.status_code, status.HTTP_200_OK)
        self.assertTrue(
            TemporaryTeamGrant.objects.filter(source_request_id=request_id).exists()
        )
        self.assertIn(self.team, user_groups(self.requester))
        self.assertIn(self.instance, list(user_instances(self.requester)))

    @patch("api_access.views.async_task")
    def test_approving_team_subject_instance_request_grants_group_members(
        self, _async_task
    ):
        group_member = User(
            username="permission_team_member",
            display="Permission Team Member",
            is_active=True,
        )
        group_member.set_password("test_password")
        group_member.save()
        assign_user_to_team(self.requester, self.team)
        assign_user_to_team(group_member, self.team)
        group_instance = Instance.objects.create(
            instance_name="permission-group-instance",
            type="master",
            db_type="mysql",
            host="127.0.0.2",
            port=3306,
            user="root",
            password="pwd",
        )

        self._login(self.requester)
        create_response = self.client.post(
            "/api/v1/access/request/",
            {
                "title": "Need instance access for team",
                "target_type": "instance",
                "subject_type": "team",
                "access_duration": "temporary",
                "team_id": self.team.team_id,
                "instance_id": group_instance.id,
                "access_level": "query",
                "valid_date": "2099-12-31",
            },
            format="json",
        )
        request_id = response_data(create_response)["request_id"]

        self._login(self.reviewer)
        review_response = self.client.post(
            f"/api/v1/access/request/{request_id}/reviews/",
            {"audit_status": WorkflowAction.PASS, "audit_remark": "approved"},
            format="json",
        )

        self.assertEqual(review_response.status_code, status.HTTP_200_OK)
        self.assertTrue(
            TemporaryInstanceGrant.objects.filter(
                source_request_id=request_id,
                user__isnull=True,
                team=self.team,
                instance=group_instance,
            ).exists()
        )
        self.assertIn(group_instance, list(user_instances(group_member)))

        group_instance.delete()
        group_member.delete()

    @patch("api_access.views.async_task")
    def test_approving_permanent_self_request_adds_direct_assignment(self, _async_task):
        self._login(self.requester)
        create_response = self.client.post(
            "/api/v1/access/request/",
            {
                "title": "Need permanent resource access",
                "target_type": "team",
                "subject_type": "user",
                "access_duration": "permanent",
                "team_id": self.team.team_id,
                "valid_date": "2099-12-31",
            },
            format="json",
        )
        request_id = response_data(create_response)["request_id"]

        self._login(self.reviewer)
        review_response = self.client.post(
            f"/api/v1/access/request/{request_id}/reviews/",
            {"audit_status": WorkflowAction.PASS, "audit_remark": "approved"},
            format="json",
        )

        self.assertEqual(review_response.status_code, status.HTTP_200_OK)
        self.assertTrue(
            PermanentTeamGrant.objects.filter(
                source_request_id=request_id, user=self.requester
            ).exists()
        )
        self.assertIn(self.team, user_groups(self.requester))
        self.assertIn(self.team, user_groups(self.requester))

    @patch("api_access.views.async_task")
    def test_approving_permanent_team_request_adds_instance_to_group(self, _async_task):
        group_member = User(
            username="permanent_group_member",
            display="Permanent Group Member",
            is_active=True,
        )
        group_member.set_password("test_password")
        group_member.save()
        assign_user_to_team(self.requester, self.team)
        assign_user_to_team(group_member, self.team)
        group_instance = Instance.objects.create(
            instance_name="permission-permanent-group-instance",
            type="master",
            db_type="mysql",
            host="127.0.0.3",
            port=3306,
            user="root",
            password="pwd",
        )

        self._login(self.requester)
        create_response = self.client.post(
            "/api/v1/access/request/",
            {
                "title": "Need permanent group access",
                "target_type": "instance",
                "subject_type": "team",
                "access_duration": "permanent",
                "team_id": self.team.team_id,
                "instance_id": group_instance.id,
                "access_level": "query",
                "valid_date": "2099-12-31",
            },
            format="json",
        )
        request_id = response_data(create_response)["request_id"]

        self._login(self.reviewer)
        review_response = self.client.post(
            f"/api/v1/access/request/{request_id}/reviews/",
            {"audit_status": WorkflowAction.PASS, "audit_remark": "approved"},
            format="json",
        )

        self.assertEqual(review_response.status_code, status.HTTP_200_OK)
        self.assertTrue(
            PermanentTeamGrant.objects.filter(
                source_request_id=request_id,
                user__isnull=True,
                team=self.team,
                instance=group_instance,
            ).exists()
        )
        self.assertIn(self.team, group_instance.resource_group.all())
        self.assertIn(self.team, user_groups(group_member))
        self.assertIn(group_instance, list(user_instances(group_member)))

        group_instance.delete()
        group_member.delete()

    def test_active_grant_list_and_revoke(self):
        grant = TemporaryInstanceGrant.objects.create(
            user=self.requester,
            team=self.team,
            instance=self.instance,
            access_level="query",
            valid_date=datetime.now().date() + timedelta(days=30),
        )

        self._login(self.reviewer)
        list_response = self.client.get("/api/v1/access/grant/", format="json")
        self.assertEqual(list_response.status_code, status.HTTP_200_OK)
        self.assertEqual(response_data(list_response)["count"], 1)

        delete_response = self.client.delete(
            f"/api/v1/access/grant/instance/{grant.grant_id}/", format="json"
        )
        self.assertEqual(delete_response.status_code, status.HTTP_200_OK)
        grant.refresh_from_db()
        self.assertEqual(grant.is_revoked, True)

    def test_query_instance_list_includes_temporary_instance_grant(self):
        TemporaryInstanceGrant.objects.create(
            user=self.query_user,
            team=self.team,
            instance=self.instance,
            access_level="query",
            valid_date=datetime.now().date() + timedelta(days=30),
        )
        self._login(self.query_user)

        r = self.client.get("/api/v1/query/instance/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["instance_name"], self.instance.instance_name)

    def test_direct_team_assignment_still_controls_instance_access_for_directory_user(
        self,
    ):
        self.query_user.workos_directory_managed = True
        self.query_user.workos_directory_id = "directory_123"
        self.query_user.workos_directory_user_id = "directory_user_query"
        self.query_user.save(
            update_fields=[
                "workos_directory_managed",
                "workos_directory_id",
                "workos_directory_user_id",
            ]
        )
        self.query_user.user_permissions.add(
            Permission.objects.get(codename="menu_sqlquery")
        )
        assign_user_to_team(self.query_user, self.team)
        self._login(self.query_user)

        r = self.client.get("/api/v1/query/instance/", format="json")

        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(len(payload), 1)
        self.assertEqual(payload[0]["instance_name"], self.instance.instance_name)

    def test_test_instance_connection_requires_superuser(self):
        """Connection testing stays restricted to superusers."""
        self._login(self.requester)
        r = self.client.post(
            f"/api/v1/instance/{self.instance.id}/test-connection/",
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_403_FORBIDDEN)

    @patch("api_instances.views.get_engine")
    def test_get_instance_resource(self, mock_get_engine):
        """Test querying instance resources."""
        group = Team.objects.create(team_name="instance_resource_test")
        assign_user_to_team(self.query_user, group)
        self.instance.resource_group.add(group)
        self._login(self.query_user)

        mock_engine = Mock()
        mock_engine.escape_string.side_effect = lambda x: x
        mock_engine.instance = Mock(show_db_name_regex="", denied_db_name_regex="")
        mock_resource = Mock()
        mock_resource.rows = ["db1"]
        mock_resource.error = ""
        mock_engine.get_all_databases.return_value = mock_resource
        mock_get_engine.return_value = mock_engine

        r = self.client.get(
            "/api/v1/instance/resource/",
            {"instance_id": self.instance.id, "resource_type": "database"},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(response_data(r)["count"], 1)


class TestWorkflow(CacheIsolatedAPITestCase):
    """Test workflow-related APIs."""

    def setUp(self):
        self.now = datetime.now()
        self.group, _ = Group.objects.get_or_create(name="DBA")
        self.policy_group, _ = Group.objects.get_or_create(name="Workflow Policy DBA")
        self.res_group = Team.objects.create(team_id=1, team_name="test")
        self.wfs = WorkflowAuditSetting.objects.create(
            team_id=self.res_group.team_id,
            workflow_type=2,
            audit_auth_groups=str(self.group.id),
        )
        self.workflow_policy = WorkflowPolicy.objects.create(
            name="Legacy Test SQL Policy",
            description="Policy fixture for SQL workflow tests.",
        )
        self.workflow_policy.steps.create(order=1, permission_group=self.policy_group)
        can_submit = Permission.objects.get(codename="sql_submit")
        can_export_submit = Permission.objects.get(codename="sqlexport_submit")
        can_export_download = Permission.objects.get(codename="offline_download")
        menu_sqlexportworkflow_permission = Permission.objects.get(
            codename="menu_sqlexportworkflow"
        )
        can_execute_permission = Permission.objects.get(codename="sql_execute")
        can_execute_resource_permission = Permission.objects.get(
            codename="sql_execute_for_team"
        )
        can_review_permission = Permission.objects.get(codename="sql_review")
        menu_sqlworkflow_permission = Permission.objects.get(
            codename="menu_sqlworkflow"
        )
        self.group.permissions.add(
            can_submit,
            can_export_submit,
            can_export_download,
            can_execute_permission,
            can_execute_resource_permission,
            can_review_permission,
            menu_sqlworkflow_permission,
            menu_sqlexportworkflow_permission,
        )
        self.user = User(username="test_user", display="Test User", is_active=True)
        self.user.set_password("test_password")
        self.user.save()
        self.user.user_permissions.add(
            can_submit,
            can_export_submit,
            can_export_download,
            can_execute_permission,
            can_execute_resource_permission,
            can_review_permission,
            menu_sqlworkflow_permission,
            menu_sqlexportworkflow_permission,
        )
        self.user.groups.add(self.group.id)
        assign_user_to_team(self.user, self.res_group)
        self.ins = Instance.objects.create(
            instance_name="some_ins",
            type="slave",
            db_type="mysql",
            host="some_host",
            port=3306,
            user="ins_user",
            password="some_str",
            queryable=True,
            workflow_enabled=True,
            workflow_policy=self.workflow_policy,
        )
        self.ins.resource_group.add(self.res_group.team_id)
        self._create_agent_assignment(self.ins)
        self.wf1 = SqlWorkflow.objects.create(
            workflow_name="some_name",
            team_id=self.res_group.team_id,
            team_name="g1",
            engineer=self.user.username,
            engineer_display=self.user.display,
            audit_auth_groups=str(self.group.id),
            create_time=self.now - timedelta(days=1),
            status="workflow_manreviewing",
            is_backup=False,
            instance=self.ins,
            db_name="some_db",
            syntax_type=1,
        )
        self.wfc1 = SqlWorkflowContent.objects.create(
            workflow=self.wf1,
            sql_content="some_sql",
            execute_result=json.dumps([{"id": 1, "sql": "some_content"}]),
        )
        self.audit1 = WorkflowAudit.objects.create(
            team_id=self.res_group.team_id,
            team_name="some_group",
            workflow_id=self.wf1.id,
            workflow_type=2,
            workflow_title="Apply Title",
            workflow_remark="Apply Remark",
            audit_auth_groups=str(self.group.id),
            current_audit=str(self.group.id),
            next_audit="-1",
            current_status=0,
            create_user=self.user.username,
            create_user_display=self.user.display,
        )
        self.wl = WorkflowLog.objects.create(
            audit_id=self.audit1.audit_id, operation_type=1
        )
        self.token = authenticate_client(self.client, self.user)["access"]
        self.notify_patcher = patch("sql.notify.auto_notify")
        self.notify_patcher.start()
        self.workflow_agent_check_patcher = patch(
            "api_workflows.views.run_agent_command_sync",
            side_effect=self._agent_check_command,
        )
        self.workflow_serializer_agent_check_patcher = patch(
            "api_workflows.serializers.run_agent_command_sync",
            side_effect=self._agent_check_command,
        )
        self.mock_workflow_agent_check = self.workflow_agent_check_patcher.start()
        self.mock_workflow_serializer_agent_check = (
            self.workflow_serializer_agent_check_patcher.start()
        )

    def tearDown(self):
        self.workflow_serializer_agent_check_patcher.stop()
        self.workflow_agent_check_patcher.stop()
        self.user.delete()
        self.res_group.delete()
        SqlWorkflowContent.objects.all().delete()
        SqlWorkflow.objects.all().delete()
        WorkflowAudit.objects.all().delete()
        WorkflowLog.objects.all().delete()
        User.objects.filter(
            username__in=[
                "temp_workflow_submitter",
                "temp_workflow_submitter_ddl",
                "temp_preview_submitter",
                "self_service_submitter",
                "export_only_submitter",
                "export_only_temp_submitter",
                "export_download_blocked_user",
            ]
        ).delete()
        self.notify_patcher.stop()

    def _create_agent_assignment(self, instance):
        agent = Agent.objects.create(
            name=f"agent-{instance.id}",
            status=AgentStatus.ONLINE,
            metadata={
                ACTIVE_WEBSOCKET_METADATA_KEY: {
                    WEBSOCKET_CHANNEL_METADATA_KEY: f"agent.test.{instance.id}",
                }
            },
        )
        AgentInstanceAssignment.objects.create(
            agent=agent,
            instance=instance,
            command_enabled=True,
        )
        return agent

    def _agent_check_command(self, **kwargs):
        payload = kwargs.get("payload") or {}
        sql = payload.get("sql") or ""
        command_type = str(kwargs.get("command_type") or "")
        if command_type == "export.check":
            syntax_type = 3
            affected_rows = 42
            stage = "Export review"
        elif re.match(r"^\s*(alter|create|drop|rename|truncate)\b", sql, re.I):
            syntax_type = 1
            affected_rows = 0
            stage = "SQL review"
        else:
            syntax_type = 2
            affected_rows = 0
            stage = "SQL review"
        return SimpleNamespace(
            result={
                "full_sql": sql,
                "syntax_type": syntax_type,
                "affected_rows": affected_rows,
                "warning_count": 0,
                "error_count": 0,
                "review_rows": [
                    {
                        "id": 1,
                        "stage": stage,
                        "errlevel": 0,
                        "stagestatus": "Audit completed",
                        "errormessage": "",
                        "sql": sql,
                        "affected_rows": affected_rows,
                        "sequence": "0_0_00000001",
                        "backup_dbname": "",
                        "execute_time": "0",
                        "sqlsha1": "",
                        "backup_time": "",
                        "actual_affected_rows": affected_rows,
                    }
                ],
                "column_list": [
                    "id",
                    "stage",
                    "errlevel",
                    "stagestatus",
                    "errormessage",
                    "sql",
                    "affected_rows",
                    "sequence",
                    "backup_dbname",
                    "execute_time",
                    "sqlsha1",
                    "backup_time",
                    "actual_affected_rows",
                ],
            }
        )

    def _login_as_user(self, username, password="test_password"):
        user = User.objects.get(username=username)
        return authenticate_client(self.client, user)

    def _create_mysql_workflow(self, status="workflow_review_pass"):
        mysql_instance = Instance.objects.create(
            instance_name="mysql_ins",
            type="master",
            db_type="mysql",
            host="mysql_host",
            port=3306,
            user="mysql_user",
            password="mysql_password",
        )
        mysql_instance.resource_group.add(self.res_group.team_id)
        self._create_agent_assignment(mysql_instance)
        workflow = SqlWorkflow.objects.create(
            workflow_name="mysql_release",
            team_id=self.res_group.team_id,
            team_name=self.res_group.team_name,
            engineer=self.user.username,
            engineer_display=self.user.display,
            audit_auth_groups=str(self.group.id),
            create_time=self.now - timedelta(days=1),
            status=status,
            is_backup=False,
            instance=mysql_instance,
            db_name="app",
            syntax_type=1,
        )
        workflow_content = SqlWorkflowContent.objects.create(
            workflow=workflow,
            sql_content="ALTER TABLE demo ADD COLUMN note VARCHAR(64);",
            review_content=json.dumps(
                [{"id": 1, "sql": "ALTER TABLE demo ADD COLUMN note VARCHAR(64);"}]
            ),
        )
        audit = WorkflowAudit.objects.create(
            team_id=self.res_group.team_id,
            team_name=self.res_group.team_name,
            workflow_id=workflow.id,
            workflow_type=2,
            workflow_title="MySQL Apply",
            workflow_remark="MySQL Apply",
            audit_auth_groups=str(self.group.id),
            current_audit="-1",
            next_audit="-1",
            current_status=WorkflowStatus.PASSED,
            create_user=self.user.username,
            create_user_display=self.user.display,
        )
        return mysql_instance, workflow, workflow_content, audit

    def test_get_sql_workflow_list(self):
        """Test getting SQL release workflow list."""
        r = self.client.get("/api/v1/workflow/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(response_data(r)["count"], 1)

    def test_workflow_list_uses_unified_success_envelope(self):
        r = self.client.get("/api/v1/workflow/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        list_data = assert_success_envelope(self, r)
        self.assertEqual(
            set(list_data.keys()), {"count", "next", "previous", "results"}
        )

    def test_workflow_list_supports_pending_review_scope(self):
        r = self.client.get("/api/v1/workflow/?scope=pending_review", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["results"][0]["id"], self.wf1.id)

    def test_workflow_list_allows_self_service_scope_without_menu_permission(self):
        submitter = User.objects.create(
            username="self_service_submitter",
            display="Self Service Submitter",
            is_active=True,
        )
        submitter.set_password("test_password")
        submitter.save()
        own_workflow = SqlWorkflow.objects.create(
            workflow_name="own_workflow",
            team_id=self.res_group.team_id,
            team_name=self.res_group.team_name,
            engineer=submitter.username,
            engineer_display=submitter.display,
            audit_auth_groups="1",
            status="workflow_review_pass",
            is_backup=False,
            instance=self.ins,
            db_name="some_db",
            syntax_type=2,
        )
        authenticate_client(self.client, submitter)

        r = self.client.get("/api/v1/workflow/?scope=mine", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["results"][0]["id"], own_workflow.id)

    def test_workflow_submission_metadata(self):
        r = self.client.get("/api/v1/workflow/submission-metadata/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(len(payload["teams"]), 1)
        self.assertEqual(payload["teams"][0]["team_id"], self.res_group.team_id)
        self.assertEqual(len(payload["instances"]), 1)
        self.assertEqual(payload["instances"][0]["id"], self.ins.id)
        self.assertEqual(payload["instances"][0]["allowed_syntax_types"], [1, 2])

    def test_workflow_submission_metadata_excludes_direct_group_access_without_submit_permission(
        self,
    ):
        self.user.user_permissions.remove(Permission.objects.get(codename="sql_submit"))
        remove_team_permission(self.user, self.res_group, "sql_submit")

        r = self.client.get("/api/v1/workflow/submission-metadata/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["teams"], [])
        self.assertEqual(payload["instances"], [])

    def test_workflow_submission_metadata_includes_temporary_instance_grant_group(self):
        temp_user = User.objects.create(
            username="temp_workflow_submitter",
            display="Temp Workflow Submitter",
            is_active=True,
        )
        temp_user.set_password("test_password")
        temp_user.save()
        TemporaryInstanceGrant.objects.create(
            user=temp_user,
            team=self.res_group,
            instance=self.ins,
            access_level=InstanceAccessLevel.QUERY_DML,
            valid_date=datetime.now().date() + timedelta(days=1),
        )
        authenticate_client(self.client, temp_user)

        r = self.client.get("/api/v1/workflow/submission-metadata/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(len(payload["teams"]), 1)
        self.assertEqual(payload["teams"][0]["team_id"], self.res_group.team_id)
        self.assertEqual(payload["instances"][0]["team_ids"], [self.res_group.team_id])
        self.assertEqual(
            payload["instances"][0]["team_names"], [self.res_group.team_name]
        )
        self.assertEqual(payload["instances"][0]["allowed_syntax_types"], [2])

    def test_workflow_submission_metadata_includes_ddl_temporary_instance_grant(self):
        temp_user = User.objects.create(
            username="temp_workflow_submitter_ddl",
            display="Temp Workflow Submitter DDL",
            is_active=True,
        )
        temp_user.set_password("test_password")
        temp_user.save()
        TemporaryInstanceGrant.objects.create(
            user=temp_user,
            team=self.res_group,
            instance=self.ins,
            access_level=InstanceAccessLevel.QUERY_DML_DDL,
            valid_date=datetime.now().date() + timedelta(days=1),
        )
        authenticate_client(self.client, temp_user)

        r = self.client.get("/api/v1/workflow/submission-metadata/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(len(payload["instances"]), 1)
        self.assertEqual(payload["instances"][0]["allowed_syntax_types"], [1, 2])

    def test_workflow_export_submission_metadata(self):
        r = self.client.get(
            "/api/v1/workflow/export/submission-metadata/", format="json"
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(len(payload["teams"]), 1)
        self.assertEqual(payload["teams"][0]["team_id"], self.res_group.team_id)
        self.assertEqual(len(payload["instances"]), 1)
        self.assertEqual(payload["instances"][0]["allowed_syntax_types"], [3])

    def test_workflow_export_submission_metadata_requires_export_submit_permission(
        self,
    ):
        self.user.user_permissions.remove(
            Permission.objects.get(codename="sqlexport_submit")
        )
        remove_team_permission(self.user, self.res_group, "sqlexport_submit")

        r = self.client.get(
            "/api/v1/workflow/export/submission-metadata/", format="json"
        )
        self.assertEqual(r.status_code, status.HTTP_403_FORBIDDEN)

    def test_workflow_export_submission_metadata_allows_export_only_submitter(self):
        export_user = User.objects.create(
            username="export_only_submitter",
            display="Export Only Submitter",
            is_active=True,
        )
        export_user.set_password("test_password")
        export_user.save()
        export_user.user_permissions.add(
            Permission.objects.get(codename="sqlexport_submit")
        )
        assign_user_to_team(export_user, self.res_group)

        self._login_as_user(export_user.username)

        r = self.client.get(
            "/api/v1/workflow/export/submission-metadata/", format="json"
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["instances"][0]["id"], self.ins.id)
        self.assertEqual(payload["instances"][0]["allowed_syntax_types"], [3])

    def test_workflow_export_submission_metadata_allows_temporary_read_grant(self):
        export_user = User.objects.create(
            username="export_only_temp_submitter",
            display="Export Temp Submitter",
            is_active=True,
        )
        export_user.set_password("test_password")
        export_user.save()
        export_user.user_permissions.add(
            Permission.objects.get(codename="sqlexport_submit")
        )
        TemporaryInstanceGrant.objects.create(
            user=export_user,
            team=self.res_group,
            instance=self.ins,
            access_level=InstanceAccessLevel.QUERY,
            valid_date=datetime.now().date() + timedelta(days=1),
        )

        self._login_as_user(export_user.username)

        r = self.client.get(
            "/api/v1/workflow/export/submission-metadata/", format="json"
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["instances"][0]["team_ids"], [self.res_group.team_id])
        self.assertEqual(payload["instances"][0]["allowed_syntax_types"], [3])

    def test_workflow_parse_sql_returns_dml_summary(self):
        r = self.client.post(
            "/api/v1/workflow/parse/",
            {
                "text": "insert into demo values (1);\nupdate demo set id = 2 where id = 1;",
                "db_type": "mysql",
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["total"], 2)
        self.assertEqual(payload["summary"]["syntax_type"], 2)
        self.assertFalse(payload["summary"]["has_mixed_syntax"])
        self.assertFalse(payload["summary"]["has_unknown_syntax"])
        self.assertEqual(payload["rows"][0]["syntax_type"], 2)

    def test_workflow_parse_sql_reports_mixed_syntax(self):
        r = self.client.post(
            "/api/v1/workflow/parse/",
            {
                "text": "insert into demo values (1);\ncreate table demo_two(id int);",
                "db_type": "mysql",
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertIsNone(payload["summary"]["syntax_type"])
        self.assertTrue(payload["summary"]["has_mixed_syntax"])
        self.assertFalse(payload["summary"]["has_unknown_syntax"])

    def test_workflow_parse_sql_rejects_load_data(self):
        r = self.client.post(
            "/api/v1/workflow/parse/",
            {
                "text": "load data infile '/tmp/demo.csv' into table demo fields terminated by ',';",
                "db_type": "mysql",
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            r.json()["errors"],
            "LOAD DATA statements are not supported for workflow submission.",
        )

    def test_workflow_approval_preview(self):
        r = self.client.get(
            f"/api/v1/workflow/approval-preview/?team_id={self.res_group.team_id}",
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["team_id"], self.res_group.team_id)
        self.assertEqual(payload["display"], self.group.name)
        self.assertEqual(payload["review_info"][0]["team_name"], self.group.name)

    def test_workflow_approval_preview_allows_temporary_instance_grant_submitter(self):
        temp_user = User.objects.create(
            username="temp_preview_submitter",
            display="Temp Preview Submitter",
            is_active=True,
        )
        temp_user.set_password("test_password")
        temp_user.save()
        TemporaryInstanceGrant.objects.create(
            user=temp_user,
            team=self.res_group,
            instance=self.ins,
            access_level=InstanceAccessLevel.QUERY_DML,
            valid_date=datetime.now().date() + timedelta(days=1),
        )
        authenticate_client(self.client, temp_user)

        r = self.client.get(
            f"/api/v1/workflow/approval-preview/?team_id={self.res_group.team_id}",
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["display"], self.group.name)

    def test_workflow_approval_preview_allows_export_only_submitter(self):
        export_user = User.objects.create(
            username="export_only_submitter",
            display="Export Only Submitter",
            is_active=True,
        )
        export_user.set_password("test_password")
        export_user.save()
        export_user.user_permissions.add(
            Permission.objects.get(codename="sqlexport_submit")
        )
        assign_user_to_team(export_user, self.res_group)

        self._login_as_user(export_user.username)

        r = self.client.get(
            f"/api/v1/workflow/approval-preview/?team_id={self.res_group.team_id}",
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["team_id"], self.res_group.team_id)
        self.assertEqual(payload["display"], self.group.name)

    def test_workflow_approval_preview_reports_missing_configuration(self):
        WorkflowAuditSetting.objects.filter(
            team_id=self.res_group.team_id, workflow_type=WorkflowType.SQL_REVIEW
        ).delete()

        r = self.client.get(
            f"/api/v1/workflow/approval-preview/?team_id={self.res_group.team_id}",
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            r.json()["errors"],
            "Approval flow is not configured for this team.",
        )

    def test_workflow_approval_preview_supports_explicit_auto_pass(self):
        WorkflowAuditSetting.objects.filter(
            team_id=self.res_group.team_id, workflow_type=WorkflowType.SQL_REVIEW
        ).update(audit_auth_groups="")

        r = self.client.get(
            f"/api/v1/workflow/approval-preview/?team_id={self.res_group.team_id}",
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["audit_auth_groups"], "")
        self.assertEqual(payload["display"], "No approval required")
        self.assertEqual(payload["review_info"][0]["team_name"], "Auto")
        self.assertTrue(payload["review_info"][0]["is_auto_pass"])

    def test_workflow_detail(self):
        self.wfc1.review_content = json.dumps([{"id": 1, "sql": "select 1"}])
        self.wfc1.save(update_fields=["review_content"])
        r = self.client.get(f"/api/v1/workflow/{self.wf1.id}/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["id"], self.wf1.id)
        self.assertEqual(payload["sql_content"], self.wfc1.sql_content)
        self.assertTrue(payload["is_can_review"])
        self.assertEqual(payload["review_info"][0]["team_name"], self.group.name)

    @patch("api_workflows.views._get_mysql_ddl_executor_state")
    def test_mysql_workflow_detail_includes_executor_options(self, mock_executor_state):
        _, workflow, _, _ = self._create_mysql_workflow()
        mock_executor_state.return_value = (
            [
                {"id": "direct", "label": "Direct", "kind": "direct"},
                {"id": "gh-ost", "label": "gh-ost", "kind": "online"},
            ],
            {"pt-osc": "pt-online-schema-change is not configured."},
        )

        r = self.client.get(f"/api/v1/workflow/{workflow.id}/", format="json")

        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["available_executors"][0]["id"], "direct")
        self.assertEqual(
            payload["executor_blockers"]["pt-osc"],
            "pt-online-schema-change is not configured.",
        )

    def test_workflow_content_detail(self):
        self.wfc1.review_content = json.dumps([{"id": 1, "sql": "select 1"}])
        self.wfc1.save(update_fields=["review_content"])
        r = self.client.get(f"/api/v1/workflow/{self.wf1.id}/content/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["source"], "review")
        self.assertEqual(payload["rows"][0]["sql"], "select 1")

    def test_workflow_rollback_detail_route_removed(self):
        with self.assertRaises(Resolver404):
            resolve(f"/api/v1/workflow/{self.wf1.id}/rollback/")

    def test_get_audit_list(self):
        """Test getting pending audit workflow list."""
        r = self.client.get("/api/v1/workflow/auditlist/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(response_data(r)["count"], 1)

    def test_get_workflow_log_list(self):
        """Test getting workflow logs."""
        r = self.client.get(
            "/api/v1/workflow/log/",
            {
                "workflow_id": self.wf1.id,
                "workflow_type": self.audit1.workflow_type,
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(response_data(r)["count"], 1)

    def test_get_workflow_metadata(self):
        response = self.client.get("/api/v1/workflow/metadata/", format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response_data(response)
        self.assertEqual(len(data["teams"]), 1)
        self.assertEqual(len(data["instances"]), 1)
        self.assertEqual(data["instances"][0]["id"], self.ins.id)
        self.assertNotIn("allow_backup_toggle", data)

    def test_get_workflow_submission_metadata(self):
        response = self.client.get(
            "/api/v1/workflow/submission-metadata/", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response_data(response)
        self.assertEqual(len(data["teams"]), 1)
        self.assertEqual(data["teams"][0]["team_id"], self.res_group.team_id)
        self.assertEqual(len(data["instances"]), 1)
        self.assertEqual(data["instances"][0]["id"], self.ins.id)
        self.assertEqual(data["instances"][0]["team_ids"], [self.res_group.team_id])
        self.assertNotIn("enable_backup_switch", data)

    def test_get_workflow_approval_preview(self):
        response = self.client.get(
            "/api/v1/workflow/approval-preview/",
            {"team_id": self.res_group.team_id},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response_data(response)
        self.assertEqual(data["team_id"], self.res_group.team_id)
        self.assertEqual(data["team_name"], self.res_group.team_name)
        self.assertEqual(data["review_info"][0]["team_name"], self.group.name)
        self.assertFalse(data["review_info"][0]["is_auto_pass"])

    def test_get_workflow_metadata_includes_temporary_instance_grant_group(self):
        temp_user = User.objects.create(
            username="workflow_temp_user",
            display="Workflow Temp User",
            is_active=True,
        )
        temp_user.set_password("test_password")
        temp_user.save()
        TemporaryInstanceGrant.objects.create(
            user=temp_user,
            team=self.res_group,
            instance=self.ins,
            access_level="query_dml",
            valid_date=datetime.now().date() + timedelta(days=1),
        )

        authenticate_client(self.client, temp_user)

        response = self.client.get("/api/v1/workflow/metadata/", format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response_data(response)
        self.assertEqual(
            [(group["team_id"], group["team_name"]) for group in data["teams"]],
            [(self.res_group.team_id, self.res_group.team_name)],
        )
        self.assertEqual(len(data["instances"]), 1)
        self.assertEqual(
            data["instances"][0]["teams"][0]["team_id"],
            self.res_group.team_id,
        )

        temp_user.delete()

    def test_get_workflow_detail(self):
        self.wfc1.review_content = json.dumps(
            [
                {
                    "id": 1,
                    "stage": "CHECKED",
                    "errlevel": 0,
                    "stagestatus": "Audit completed",
                    "errormessage": "",
                    "sql": "alter table demo add column note varchar(64)",
                    "affected_rows": 0,
                    "sequence": "0_0_00000001",
                    "backup_dbname": "",
                    "execute_time": "0",
                    "sqlsha1": "",
                    "backup_time": "",
                    "actual_affected_rows": "",
                }
            ]
        )
        self.wfc1.save(update_fields=["review_content"])

        response = self.client.get(f"/api/v1/workflow/{self.wf1.id}/", format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response_data(response)
        self.assertEqual(data["id"], self.wf1.id)
        self.assertEqual(data["syntax_type"], 1)
        self.assertEqual(
            data["review_rows"][0]["sql"],
            "alter table demo add column note varchar(64)",
        )
        self.assertEqual(
            data["logs"][0]["operation_type_desc"], self.wl.operation_type_desc
        )
        self.assertTrue(data["is_can_review"])
        self.assertNotIn("is_can_rollback", data)

    def test_get_workflow_detail_allows_audit_user(self):
        audit_user = User.objects.create(
            username="workflow_audit_user",
            display="Workflow Audit User",
            is_active=True,
        )
        audit_user.set_password("test_password")
        audit_user.save()
        audit_user.user_permissions.add(Permission.objects.get(codename="audit_user"))

        authenticate_client(self.client, audit_user)

        response = self.client.get(f"/api/v1/workflow/{self.wf1.id}/", format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response_data(response)["id"], self.wf1.id)

        audit_user.delete()

    def test_get_workflow_log_list_missing_params(self):
        """workflow_id and workflow_type are required query params."""
        r = self.client.get("/api/v1/workflow/log/", format="json")
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            r.json()["errors"],
            "workflow_id and workflow_type are required query parameters.",
        )

    def test_get_workflow_log_list_invalid_params(self):
        """workflow_id and workflow_type must be integers."""
        r = self.client.get(
            "/api/v1/workflow/log/",
            {"workflow_id": "abc", "workflow_type": "2"},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            r.json()["errors"], "workflow_id and workflow_type must be integers."
        )

    def test_check_param_is_None(self):
        """Test workflow SQL check with empty parameters."""
        json_data = {
            "full_sql": "",
            "db_name": "test_db",
            "instance_id": self.ins.id,
        }
        r = self.client.post("/api/v1/workflow/sqlcheck/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)

    @patch(
        "api_workflows.views.run_agent_command_sync",
        side_effect=Exception("RuntimeError"),
    )
    @patch("api_workflows.views.logger")
    def test_check_inception_Exception(self, mock_logger, _run_agent_command_sync):
        """Test workflow SQL check when inception raises an error."""
        json_data = {
            "full_sql": "use mysql",
            "db_name": "test_db",
            "instance_id": self.ins.id,
        }
        r = self.client.post("/api/v1/workflow/sqlcheck/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertDictEqual(json.loads(r.content), {"errors": "Internal Server Error"})
        mock_logger.exception.assert_called_once()

    @patch("api_workflows.views.get_engine", create=True)
    def test_check(self, _get_engine):
        """Test workflow SQL check with normal return."""
        json_data = {
            "full_sql": "use mysql",
            "db_name": "test_db",
            "instance_id": self.ins.id,
        }
        column_list = [
            "id",
            "stage",
            "errlevel",
            "stagestatus",
            "errormessage",
            "sql",
            "affected_rows",
            "sequence",
            "backup_dbname",
            "execute_time",
            "sqlsha1",
            "backup_time",
            "actual_affected_rows",
        ]

        rows = [
            ReviewResult(
                id=1,
                stage="CHECKED",
                errlevel=0,
                stagestatus="Audit Completed",
                errormessage="",
                sql="use `archer`",
                affected_rows=0,
                actual_affected_rows=0,
                sequence="0_0_00000000",
                backup_dbname="",
                execute_time="0",
                sqlsha1="",
            )
        ]
        _get_engine.return_value.execute_check.return_value = ReviewSet(
            warning_count=0, error_count=0, column_list=column_list, rows=rows
        )
        r = self.client.post("/api/v1/workflow/sqlcheck/", json_data, format="json")
        sqlcheck_data = response_data(r)
        self.assertListEqual(
            list(sqlcheck_data.keys()),
            [
                "is_execute",
                "checked",
                "warning",
                "error",
                "warning_count",
                "error_count",
                "is_critical",
                "syntax_type",
                "rows",
                "column_list",
                "status",
                "affected_rows",
            ],
        )
        self.assertListEqual(list(sqlcheck_data["rows"][0].keys()), column_list)

    @patch("api_workflows.views.get_engine", create=True)
    def test_sqlcheck_uses_unified_success_envelope(self, _get_engine):
        json_data = {
            "full_sql": "use mysql",
            "db_name": "test_db",
            "instance_id": self.ins.id,
        }
        _get_engine.return_value.execute_check.return_value = ReviewSet(
            warning_count=0,
            error_count=0,
            column_list=[],
            rows=[],
        )
        r = self.client.post("/api/v1/workflow/sqlcheck/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        data = assert_success_envelope(self, r)
        self.assertIn("rows", data)

    def test_sqlcheck_sends_schema_name_to_agent_check(self):
        json_data = {
            "full_sql": "select 1",
            "db_name": "test_db",
            "schema_name": "public",
            "instance_id": self.ins.id,
        }

        r = self.client.post("/api/v1/workflow/sqlcheck/", json_data, format="json")

        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = self.mock_workflow_agent_check.call_args.kwargs["payload"]
        self.assertEqual(payload["db_name"], "test_db")
        self.assertEqual(payload["schema_name"], "public")
        self.assertEqual(payload["sql"], "select 1")

    def test_export_sqlcheck(self):
        r = self.client.post(
            "/api/v1/workflow/export/sqlcheck/",
            {
                "full_sql": "select * from demo",
                "db_name": "test_db",
                "schema_name": "analytics",
                "instance_id": self.ins.id,
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["syntax_type"], 3)
        self.assertEqual(payload["affected_rows"], 42)
        command_payload = self.mock_workflow_agent_check.call_args.kwargs["payload"]
        self.assertEqual(command_payload["db_name"], "test_db")
        self.assertEqual(command_payload["schema_name"], "analytics")

    @patch(
        "api_workflows.views.run_agent_command_sync",
        side_effect=Exception("COUNT(*) failed"),
    )
    @patch("api_workflows.views.logger")
    def test_export_sqlcheck_returns_validation_error_for_count_failures(
        self, mock_logger, _run_agent_command_sync
    ):
        r = self.client.post(
            "/api/v1/workflow/export/sqlcheck/",
            {
                "full_sql": "select * from demo",
                "db_name": "test_db",
                "instance_id": self.ins.id,
            },
            format="json",
        )

        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(r.json()["errors"], "Internal Server Error")
        mock_logger.exception.assert_called_once()

    def test_export_sqlcheck_requires_export_submit_permission(self):
        self.user.user_permissions.remove(
            Permission.objects.get(codename="sqlexport_submit")
        )
        remove_team_permission(self.user, self.res_group, "sqlexport_submit")

        r = self.client.post(
            "/api/v1/workflow/export/sqlcheck/",
            {
                "full_sql": "select * from demo",
                "db_name": "test_db",
                "instance_id": self.ins.id,
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_403_FORBIDDEN)

    def test_export_sqlcheck_requires_read_access_to_instance(self):
        isolated_group = Team.objects.create(team_name="isolated_group")
        isolated_user = User.objects.create(
            username="export_only_submitter",
            display="Export Only Submitter",
            is_active=True,
        )
        isolated_user.set_password("test_password")
        isolated_user.save()
        isolated_user.user_permissions.add(
            Permission.objects.get(codename="sqlexport_submit")
        )
        assign_user_to_team(isolated_user, isolated_group)

        self._login_as_user(isolated_user.username)

        r = self.client.post(
            "/api/v1/workflow/export/sqlcheck/",
            {
                "full_sql": "select * from demo",
                "db_name": "test_db",
                "instance_id": self.ins.id,
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_403_FORBIDDEN)
        self.mock_workflow_agent_check.assert_not_called()

    def test_submit_workflow(self):
        """Test submitting SQL release workflow."""
        json_data = {
            "workflow": {
                "workflow_name": "Release Workflow 1",
                "demand_url": "test",
                "team_id": 1,
                "db_name": "test_db",
                "instance": self.ins.id,
                "is_offline_export": 0,
            },
            "sql_content": "alter table abc add column note varchar(64);",
        }
        r = self.client.post("/api/v1/workflow/", json_data, format="json")
        r_data = response_data(r)
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        self.assertEqual(r_data["workflow"]["workflow_name"], "Release Workflow 1")
        self.assertEqual(r_data["workflow"]["engineer"], self.user.username)
        self.assertEqual(r_data["workflow"]["engineer_display"], self.user.display)
        workflow = SqlWorkflow.objects.get(id=r_data["workflow"]["id"])
        audit = WorkflowAudit.objects.get(
            workflow_id=workflow.id, workflow_type=WorkflowType.SQL_REVIEW
        )
        self.assertEqual(workflow.audit_auth_groups, str(self.policy_group.id))
        self.assertEqual(audit.current_audit, str(self.policy_group.id))

    @patch("api_workflows.serializers.get_engine", create=True)
    def test_submit_workflow_defaults_backup_to_false(self, mock_get_engine):
        review_set = ReviewSet(
            rows=[
                ReviewResult(
                    errlevel=0,
                    stagestatus="Audit completed",
                    errormessage="None",
                    sql="alter table abc add column note varchar(64);",
                )
            ]
        )
        review_set.syntax_type = 2
        review_set.error_count = 0
        review_set.warning_count = 0
        mock_get_engine.return_value.auto_backup = True
        mock_get_engine.return_value.execute_check.return_value = review_set

        json_data = {
            "workflow": {
                "workflow_name": "Release Workflow Default Backup",
                "demand_url": "test",
                "team_id": 1,
                "db_name": "test_db",
                "instance": self.ins.id,
                "is_offline_export": 0,
            },
            "sql_content": "alter table abc add column note varchar(64);",
        }
        r = self.client.post("/api/v1/workflow/", json_data, format="json")

        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        workflow_id = response_data(r)["workflow"]["id"]
        workflow = SqlWorkflow.objects.get(id=workflow_id)
        self.assertFalse(workflow.is_backup)

    @patch("api_workflows.serializers.get_engine", create=True)
    def test_submit_workflow_ignores_submitted_backup_flag(self, mock_get_engine):
        review_set = ReviewSet(
            rows=[
                ReviewResult(
                    errlevel=0,
                    stagestatus="Audit completed",
                    errormessage="None",
                    sql="alter table abc add column note varchar(64);",
                )
            ]
        )
        review_set.syntax_type = 2
        review_set.error_count = 0
        review_set.warning_count = 0
        mock_get_engine.return_value.auto_backup = True
        mock_get_engine.return_value.execute_check.return_value = review_set

        json_data = {
            "workflow": {
                "workflow_name": "Release Workflow Ignored Backup",
                "demand_url": "test",
                "team_id": 1,
                "db_name": "test_db",
                "instance": self.ins.id,
                "is_backup": True,
                "is_offline_export": 0,
            },
            "sql_content": "alter table abc add column note varchar(64);",
        }
        r = self.client.post("/api/v1/workflow/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        workflow_id = response_data(r)["workflow"]["id"]
        workflow = SqlWorkflow.objects.get(id=workflow_id)
        self.assertFalse(workflow.is_backup)

    def test_submit_export_workflow(self):
        json_data = {
            "workflow": {
                "workflow_name": "Export Workflow 1",
                "team_id": 1,
                "db_name": "test_db",
                "schema_name": "analytics",
                "instance": self.ins.id,
                "is_offline_export": 1,
                "export_format": "tsv",
            },
            "sql_content": "select * from demo;",
        }
        r = self.client.post("/api/v1/workflow/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        command_payload = self.mock_workflow_serializer_agent_check.call_args.kwargs[
            "payload"
        ]
        self.assertEqual(command_payload["export_format"], "tsv")
        workflow_id = response_data(r)["workflow"]["id"]
        workflow = SqlWorkflow.objects.get(id=workflow_id)
        self.assertEqual(workflow.syntax_type, 3)
        self.assertEqual(workflow.is_offline_export, 1)
        self.assertEqual(workflow.export_format, "tsv")
        self.assertEqual(workflow.schema_name, "analytics")
        self.assertFalse(workflow.is_backup)

    @patch("api_workflows.serializers.get_engine", create=True)
    def test_submit_workflow_rejects_team_not_attached_to_instance(
        self, mock_get_engine
    ):
        review_set = ReviewSet(
            rows=[
                ReviewResult(
                    errlevel=0,
                    stagestatus="Audit completed",
                    errormessage="None",
                    sql="alter table abc add column note varchar(64);",
                )
            ]
        )
        review_set.syntax_type = 2
        review_set.error_count = 0
        review_set.warning_count = 0
        mock_get_engine.return_value.auto_backup = True
        mock_get_engine.return_value.execute_check.return_value = review_set
        other_group = Team.objects.create(team_name="other-group")

        json_data = {
            "workflow": {
                "workflow_name": "Release Workflow Wrong Group",
                "team_id": other_group.team_id,
                "db_name": "test_db",
                "instance": self.ins.id,
                "is_offline_export": 0,
            },
            "sql_content": "alter table abc add column note varchar(64);",
        }
        r = self.client.post("/api/v1/workflow/", json_data, format="json")

        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            r.json()["errors"],
            "Selected team does not belong to this instance.",
        )

    @patch("api_workflows.serializers.get_engine", create=True)
    @patch("api_workflows.serializers.user_has_instance_workflow_access")
    def test_submit_workflow_allows_temporary_write_access_even_with_group_access(
        self, mock_temporary_access, mock_get_engine
    ):
        review_set = ReviewSet(
            rows=[
                ReviewResult(
                    errlevel=0,
                    stagestatus="Audit completed",
                    errormessage="None",
                    sql="alter table abc add column note varchar(64);",
                )
            ]
        )
        review_set.syntax_type = 2
        review_set.error_count = 0
        review_set.warning_count = 0
        mock_get_engine.return_value.auto_backup = True
        mock_get_engine.return_value.execute_check.return_value = review_set
        mock_temporary_access.return_value = True

        limited_user = User.objects.create(
            username="temporary_submitter",
            display="Temporary Submitter",
            is_active=True,
        )
        limited_user.set_password("test_password")
        limited_user.save()
        assign_user_to_team(limited_user, self.res_group)
        self._login_as_user(limited_user.username)

        json_data = {
            "workflow": {
                "workflow_name": "Release Workflow Temporary Access",
                "team_id": 1,
                "db_name": "test_db",
                "instance": self.ins.id,
                "is_offline_export": 0,
            },
            "sql_content": "alter table abc add column note varchar(64);",
        }
        r = self.client.post("/api/v1/workflow/", json_data, format="json")

        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        workflow = SqlWorkflow.objects.get(id=response_data(r)["workflow"]["id"])
        self.assertEqual(workflow.engineer, limited_user.username)

    @patch(
        "api_workflows.serializers.run_agent_command_sync",
        side_effect=Exception("sensitive agent failure"),
    )
    def test_submit_workflow_hides_agent_exception_details(
        self, _run_agent_command_sync
    ):
        json_data = {
            "workflow": {
                "workflow_name": "Release Workflow Engine Failure",
                "team_id": 1,
                "db_name": "test_db",
                "instance": self.ins.id,
                "is_offline_export": 0,
            },
            "sql_content": "alter table abc add column note varchar(64);",
        }
        r = self.client.post("/api/v1/workflow/", json_data, format="json")

        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(r.json()["errors"], "An internal validation error occurred.")

    @patch("api_workflows.serializers.get_auditor")
    @patch("api_workflows.serializers.get_engine", create=True)
    def test_submit_workflow_hides_save_exception_details(
        self, mock_get_engine, mock_get_auditor
    ):
        review_set = ReviewSet(
            rows=[
                ReviewResult(
                    errlevel=0,
                    stagestatus="Audit completed",
                    errormessage="None",
                    sql="alter table abc add column note varchar(64);",
                )
            ]
        )
        review_set.syntax_type = 2
        review_set.error_count = 0
        review_set.warning_count = 0
        mock_get_engine.return_value.auto_backup = True
        mock_get_engine.return_value.execute_check.return_value = review_set

        class BrokenAuditor:
            def create_audit(self):
                raise Exception("hidden audit failure")

        mock_get_auditor.return_value = BrokenAuditor()

        json_data = {
            "workflow": {
                "workflow_name": "Release Workflow Save Failure",
                "team_id": 1,
                "db_name": "test_db",
                "instance": self.ins.id,
                "is_offline_export": 0,
            },
            "sql_content": "alter table abc add column note varchar(64);",
        }
        r = self.client.post("/api/v1/workflow/", json_data, format="json")

        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(r.json()["errors"], "An internal validation error occurred.")

    @patch("api_workflows.serializers.get_engine", create=True)
    def test_submit_workflow_rolls_back_when_status_update_save_fails(
        self, mock_get_engine
    ):
        review_set = ReviewSet(
            rows=[
                ReviewResult(
                    errlevel=0,
                    stagestatus="Audit completed",
                    errormessage="None",
                    sql="alter table abc add column note varchar(64);",
                )
            ]
        )
        review_set.syntax_type = 2
        review_set.error_count = 0
        review_set.warning_count = 0
        mock_get_engine.return_value.auto_backup = True
        mock_get_engine.return_value.execute_check.return_value = review_set

        original_save = SqlWorkflow.save
        save_calls = {"count": 0}

        def flaky_save(instance, *args, **kwargs):
            save_calls["count"] += 1
            if save_calls["count"] == 2:
                raise RuntimeError("status save failed")
            return original_save(instance, *args, **kwargs)

        json_data = {
            "workflow": {
                "workflow_name": "Release Workflow Atomic Save",
                "team_id": 1,
                "db_name": "test_db",
                "instance": self.ins.id,
                "is_offline_export": 0,
            },
            "sql_content": "alter table abc add column note varchar(64);",
        }

        baseline_workflow_count = SqlWorkflow.objects.count()
        baseline_audit_count = WorkflowAudit.objects.count()

        with patch.object(SqlWorkflow, "save", autospec=True, side_effect=flaky_save):
            r = self.client.post("/api/v1/workflow/", json_data, format="json")

        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(r.json()["errors"], "An internal validation error occurred.")
        self.assertEqual(SqlWorkflow.objects.count(), baseline_workflow_count)
        self.assertEqual(WorkflowAudit.objects.count(), baseline_audit_count)
        self.assertFalse(
            SqlWorkflow.objects.filter(
                workflow_name="Release Workflow Atomic Save"
            ).exists()
        )

    @patch("api_workflows.serializers.get_engine", create=True)
    def test_submit_export_workflow_rejects_invalid_format(self, mock_get_engine):
        mock_get_engine.return_value.auto_backup = False
        json_data = {
            "workflow": {
                "workflow_name": "Export Workflow 1",
                "team_id": 1,
                "db_name": "test_db",
                "instance": self.ins.id,
                "is_offline_export": 1,
                "export_format": "xls",
            },
            "sql_content": "select * from demo;",
        }
        r = self.client.post("/api/v1/workflow/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn(
            "not a valid choice.",
            json.dumps(r.json()),
        )

    def test_submit_export_workflow_requires_export_submit_permission(self):
        self.user.user_permissions.remove(
            Permission.objects.get(codename="sqlexport_submit")
        )
        remove_team_permission(self.user, self.res_group, "sqlexport_submit")
        json_data = {
            "workflow": {
                "workflow_name": "Export Workflow 1",
                "team_id": 1,
                "db_name": "test_db",
                "instance": self.ins.id,
                "is_offline_export": 1,
                "export_format": "csv",
            },
            "sql_content": "select * from demo;",
        }
        r = self.client.post("/api/v1/workflow/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            r.json()["errors"],
            "You do not have permission to submit export workflows.",
        )

    def test_workflow_list_includes_export_metadata(self):
        export_workflow = SqlWorkflow.objects.create(
            workflow_name="export_listed",
            team_id=self.res_group.team_id,
            team_name=self.res_group.team_name,
            engineer=self.user.username,
            engineer_display=self.user.display,
            audit_auth_groups="1",
            status="workflow_finish",
            is_backup=False,
            instance=self.ins,
            db_name="some_db",
            schema_name="analytics",
            syntax_type=3,
            is_offline_export=1,
            export_format="csv",
            file_name="demo.csv",
        )
        SqlWorkflowContent.objects.create(
            workflow=export_workflow,
            sql_content="select * from demo",
        )

        r = self.client.get("/api/v1/workflow/?syntax_type=3", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertEqual(payload["count"], 1)
        row = payload["results"][0]
        self.assertEqual(row["id"], export_workflow.id)
        self.assertTrue(row["is_offline_export"])
        self.assertEqual(row["export_format"], "csv")
        self.assertEqual(row["file_name"], "demo.csv")
        self.assertEqual(row["schema_name"], "analytics")
        self.assertTrue(row["download_available"])

    def test_workflow_detail_includes_export_metadata(self):
        export_workflow = SqlWorkflow.objects.create(
            workflow_name="export_detail",
            team_id=self.res_group.team_id,
            team_name=self.res_group.team_name,
            engineer=self.user.username,
            engineer_display=self.user.display,
            audit_auth_groups="1",
            status="workflow_finish",
            is_backup=False,
            instance=self.ins,
            db_name="some_db",
            schema_name="analytics",
            syntax_type=3,
            is_offline_export=1,
            export_format="sql",
            file_name="demo.sql",
        )
        SqlWorkflowContent.objects.create(
            workflow=export_workflow,
            sql_content="select * from demo",
            execute_result=json.dumps([{"stagestatus": "Execution succeeded"}]),
        )
        WorkflowAudit.objects.create(
            team_id=self.res_group.team_id,
            team_name=self.res_group.team_name,
            workflow_id=export_workflow.id,
            workflow_type=2,
            workflow_title="Export Apply",
            workflow_remark="Export",
            audit_auth_groups="1",
            current_audit="-1",
            next_audit="-1",
            current_status=1,
            create_user=self.user.username,
            create_user_display=self.user.display,
        )

        r = self.client.get(f"/api/v1/workflow/{export_workflow.id}/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        payload = response_data(r)
        self.assertTrue(payload["is_offline_export"])
        self.assertEqual(payload["export_format"], "sql")
        self.assertEqual(payload["file_name"], "demo.sql")
        self.assertEqual(payload["schema_name"], "analytics")
        self.assertTrue(payload["download_available"])

    @patch("api_workflows.views.download_export_file")
    def test_download_export_workflow(self, mock_download_export_file):
        export_workflow = SqlWorkflow.objects.create(
            workflow_name="export_ready",
            team_id=self.res_group.team_id,
            team_name=self.res_group.team_name,
            engineer=self.user.username,
            engineer_display=self.user.display,
            audit_auth_groups="1",
            status="workflow_finish",
            is_backup=False,
            instance=self.ins,
            db_name="some_db",
            syntax_type=3,
            is_offline_export=1,
            export_format="csv",
            file_name="demo.csv",
        )
        SqlWorkflowContent.objects.create(
            workflow=export_workflow,
            sql_content="select * from demo",
        )
        WorkflowAudit.objects.create(
            team_id=self.res_group.team_id,
            team_name=self.res_group.team_name,
            workflow_id=export_workflow.id,
            workflow_type=2,
            workflow_title="Export Apply",
            workflow_remark="Export",
            audit_auth_groups="1",
            current_audit="-1",
            next_audit="-1",
            current_status=1,
            create_user=self.user.username,
            create_user_display=self.user.display,
        )
        mock_download_export_file.return_value = HttpResponse("ok")

        r = self.client.get(
            f"/api/v1/workflow/{export_workflow.id}/download/", format="json"
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        mock_download_export_file.assert_called_once()
        self.assertEqual(
            mock_download_export_file.call_args.args[1:],
            ("demo.csv", export_workflow.id),
        )

    def test_download_export_workflow_requires_download_permission(self):
        export_workflow = SqlWorkflow.objects.create(
            workflow_name="export_ready",
            team_id=self.res_group.team_id,
            team_name=self.res_group.team_name,
            engineer=self.user.username,
            engineer_display=self.user.display,
            audit_auth_groups="1",
            status="workflow_finish",
            is_backup=False,
            instance=self.ins,
            db_name="some_db",
            syntax_type=3,
            is_offline_export=1,
            export_format="csv",
            file_name="demo.csv",
        )
        SqlWorkflowContent.objects.create(
            workflow=export_workflow,
            sql_content="select * from demo",
        )
        WorkflowAudit.objects.create(
            team_id=self.res_group.team_id,
            team_name=self.res_group.team_name,
            workflow_id=export_workflow.id,
            workflow_type=2,
            workflow_title="Export Apply",
            workflow_remark="Export",
            audit_auth_groups="1",
            current_audit="-1",
            next_audit="-1",
            current_status=1,
            create_user=self.user.username,
            create_user_display=self.user.display,
        )

        blocked_user = User.objects.create(
            username="export_download_blocked_user",
            display="Export Download Blocked",
            is_active=True,
        )
        blocked_user.set_password("test_password")
        blocked_user.save()
        blocked_user.user_permissions.add(
            Permission.objects.get(codename="menu_sqlexportworkflow")
        )
        blocked_user.user_permissions.remove(
            Permission.objects.get(codename="offline_download")
        )
        assign_user_to_team(blocked_user, self.res_group)
        export_workflow.engineer = blocked_user.username
        export_workflow.engineer_display = blocked_user.display
        export_workflow.save(update_fields=["engineer", "engineer_display"])
        self._login_as_user(blocked_user.username)

        r = self.client.get(
            f"/api/v1/workflow/{export_workflow.id}/download/", format="json"
        )
        self.assertEqual(r.status_code, status.HTTP_403_FORBIDDEN)

    def test_download_export_workflow_rejects_unfinished_artifact(self):
        export_workflow = SqlWorkflow.objects.create(
            workflow_name="export_pending",
            team_id=self.res_group.team_id,
            team_name=self.res_group.team_name,
            engineer=self.user.username,
            engineer_display=self.user.display,
            audit_auth_groups="1",
            status="workflow_manreviewing",
            is_backup=False,
            instance=self.ins,
            db_name="some_db",
            syntax_type=3,
            is_offline_export=1,
            export_format="csv",
            file_name=None,
        )
        SqlWorkflowContent.objects.create(
            workflow=export_workflow,
            sql_content="select * from demo",
        )
        WorkflowAudit.objects.create(
            team_id=self.res_group.team_id,
            team_name=self.res_group.team_name,
            workflow_id=export_workflow.id,
            workflow_type=2,
            workflow_title="Export Apply",
            workflow_remark="Export",
            audit_auth_groups="1",
            current_audit="1",
            next_audit="-1",
            current_status=0,
            create_user=self.user.username,
            create_user_display=self.user.display,
        )

        r = self.client.get(
            f"/api/v1/workflow/{export_workflow.id}/download/", format="json"
        )
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            r.json()["errors"],
            "The export artifact is not available yet.",
        )

    def test_download_export_workflow_rejects_non_export_workflow(self):
        r = self.client.get(f"/api/v1/workflow/{self.wf1.id}/download/", format="json")
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            r.json()["errors"],
            "This workflow does not have an export artifact.",
        )

    def test_submit_workflow_super(self):
        """Test admin submitting SQL release workflow with specified user."""
        self.user.is_superuser = True
        self.user.save(update_fields=["is_superuser"])
        user2 = User.objects.create(
            username="test_user2", display="Test User 2", is_active=True
        )
        user2.groups.add(self.group.id)
        assign_user_to_team(user2, self.res_group)
        json_data = {
            "workflow": {
                "workflow_name": "Release Workflow 1",
                "demand_url": "test",
                "team_id": 1,
                "db_name": "test_db",
                "engineer": "test_user2",
                "instance": self.ins.id,
                "is_offline_export": 0,
            },
            "sql_content": "alter table abc add column note varchar(64);",
        }
        r = self.client.post("/api/v1/workflow/", json_data, format="json")
        r_data = response_data(r)
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        self.assertEqual(r_data["workflow"]["workflow_name"], "Release Workflow 1")
        self.assertEqual(r_data["workflow"]["engineer"], user2.username)
        self.assertEqual(r_data["workflow"]["engineer_display"], user2.display)

    @patch("sql.utils.workflow_audit.AuditV2.generate_audit_setting")
    def test_submit_workflow_auto_pass(self, mock_generate_settings):
        json_data = {
            "workflow": {
                "workflow_name": "Release Workflow 1",
                "demand_url": "test",
                "team_id": 1,
                "db_name": "test_db",
                "instance": self.ins.id,
                "is_offline_export": 0,
            },
            "sql_content": "alter table abc add column note varchar(64);",
        }
        mock_generate_settings.return_value = AuditSetting(auto_pass=True)
        r = self.client.post("/api/v1/workflow/", json_data, format="json")
        return_data = response_data(r)
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        workflow_in_db = SqlWorkflow.objects.get(id=return_data["workflow"]["id"])
        assert workflow_in_db.status == "workflow_review_pass"

    def test_submit_param_is_None(self):
        """Test SQL submit with empty parameters."""
        json_data = {
            "workflow": {
                "workflow_name": "Release Workflow 1",
                "demand_url": "test",
                "team_id": 1,
                "db_name": "test_db",
                "instance": self.ins.id,
            },
            "sql_content": "",
        }
        r = self.client.post("/api/v1/workflow/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)

    def test_sqlcheck_rejects_load_data(self):
        json_data = {
            "full_sql": "load data infile '/tmp/demo.csv' into table demo fields terminated by ',';",
            "db_name": "test_db",
            "instance_id": self.ins.id,
        }
        r = self.client.post("/api/v1/workflow/sqlcheck/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            r.json()["errors"],
            "LOAD DATA statements are not supported for workflow submission.",
        )

    def test_submit_workflow_rejects_load_data(self):
        json_data = {
            "workflow": {
                "workflow_name": "Release Workflow 1",
                "demand_url": "test",
                "team_id": 1,
                "db_name": "test_db",
                "instance": self.ins.id,
                "is_offline_export": 0,
            },
            "sql_content": "load data infile '/tmp/demo.csv' into table demo fields terminated by ',';",
        }
        r = self.client.post("/api/v1/workflow/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            r.json()["errors"],
            "LOAD DATA statements are not supported for workflow submission.",
        )

    def test_audit_workflow(self):
        """Test auditing workflow."""
        json_data = {
            "audit_remark": "cancel",
            "workflow_type": self.audit1.workflow_type,
            "audit_type": "cancel",
        }
        r = self.client.post(
            f"/api/v1/workflow/{self.wf1.id}/reviews/", json_data, format="json"
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(r.json()["detail"], "canceled")

    def test_audit_workflow_reject(self):
        r = self.client.post(
            f"/api/v1/workflow/{self.wf1.id}/reviews/",
            {
                "audit_remark": "reject",
                "workflow_type": self.audit1.workflow_type,
                "audit_type": "reject",
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(r.json()["detail"], "rejected")

    def test_audit_cancel_denies_non_owner(self):
        user2 = User.objects.create(
            username="workflow_user2",
            display="Workflow User 2",
            is_active=True,
        )
        user2.set_password("test_password")
        user2.save()
        authenticate_client(self.client, user2)
        r = self.client.post(
            f"/api/v1/workflow/{self.wf1.id}/reviews/",
            {
                "audit_remark": "cancel by non-owner",
                "workflow_type": self.audit1.workflow_type,
                "audit_type": "cancel",
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            r.json()["errors"], "User is not allowed to operate this workflow."
        )
        user2.delete()

    def test_audit_pass_updates_workflow_status(self):
        r = self.client.post(
            f"/api/v1/workflow/{self.wf1.id}/reviews/",
            {
                "audit_remark": "approved",
                "workflow_type": self.audit1.workflow_type,
                "audit_type": "pass",
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.wf1.refresh_from_db()
        self.assertEqual(self.wf1.status, "workflow_review_pass")

    def test_audit_reject_updates_workflow_status(self):
        r = self.client.post(
            f"/api/v1/workflow/{self.wf1.id}/reviews/",
            {
                "audit_remark": "rejected",
                "workflow_type": self.audit1.workflow_type,
                "audit_type": "reject",
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(r.json()["detail"], "rejected")
        self.wf1.refresh_from_db()
        self.assertEqual(self.wf1.status, "workflow_abort")

    def test_execute_workflow(self):
        """Test executing workflow."""
        # Audit first
        audit_data = {
            "audit_remark": "approved",
            "workflow_type": self.audit1.workflow_type,
            "audit_type": "pass",
        }
        self.client.post(
            f"/api/v1/workflow/{self.wf1.id}/reviews/", audit_data, format="json"
        )
        # Then execute
        execute_data = {
            "workflow_type": self.audit1.workflow_type,
            "mode": "manual",
        }
        r = self.client.post(
            f"/api/v1/workflow/{self.wf1.id}/executions/", execute_data, format="json"
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(
            r.json()["detail"],
            "Execution started. Please check workflow detail page for results.",
        )

    @patch("api_workflows.views.dispatch_sql_workflow_to_agent")
    @patch("api_workflows.views._resolve_mysql_ddl_executor")
    def test_execute_workflow_auto_passes_selected_executor(
        self, mock_resolve_executor, mock_dispatch_sql_workflow_to_agent
    ):
        _, workflow, _, _ = self._create_mysql_workflow()
        mock_resolve_executor.return_value = Mock(executor_id="direct")
        mock_dispatch_sql_workflow_to_agent.return_value = Mock(agent_id=1, id=2)

        r = self.client.post(
            f"/api/v1/workflow/{workflow.id}/executions/",
            {"workflow_type": 2, "mode": "auto", "executor": "direct"},
            format="json",
        )

        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(
            mock_dispatch_sql_workflow_to_agent.call_args.kwargs["executor"], "direct"
        )

    def test_execute_workflow_rejects_unsupported_executor(self):
        _, workflow, _, _ = self._create_mysql_workflow()
        workflow.instance.workflow_enabled = True
        workflow.instance.save(update_fields=["workflow_enabled"])

        r = self.client.post(
            f"/api/v1/workflow/{workflow.id}/executions/",
            {"workflow_type": 2, "mode": "auto", "executor": "gh-ost"},
            format="json",
        )

        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        workflow.refresh_from_db()
        self.assertEqual(workflow.status, "workflow_review_pass")
        self.assertIn("artifact is not configured", json.dumps(r.json()))

    @patch(
        "api_workflows.views.dispatch_sql_workflow_to_agent",
        side_effect=ValueError("SQL workflow is missing SQL content."),
    )
    @patch("api_workflows.views._resolve_mysql_ddl_executor")
    def test_execute_workflow_returns_validation_error_on_agent_dispatch_failure(
        self, mock_resolve_executor, _mock_dispatch
    ):
        _, workflow, _, _ = self._create_mysql_workflow()
        mock_resolve_executor.return_value = Mock(executor_id="direct")

        r = self.client.post(
            f"/api/v1/workflow/{workflow.id}/executions/",
            {"workflow_type": 2, "mode": "auto", "executor": "direct"},
            format="json",
        )

        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        workflow.refresh_from_db()
        self.assertEqual(workflow.status, "workflow_review_pass")
        self.assertIn("Unable to dispatch workflow to agent", r.json()["errors"])
        self.assertIn("missing SQL content", r.json()["errors"])

    def test_execute_workflow_requires_execute_permission(self):
        self.user.user_permissions.remove(
            Permission.objects.get(codename="sql_execute")
        )
        self.user.user_permissions.remove(
            Permission.objects.get(codename="sql_execute_for_team")
        )
        r = self.client.post(
            f"/api/v1/workflow/{self.wf1.id}/executions/",
            {"workflow_type": self.audit1.workflow_type, "mode": "manual"},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            r.json()["errors"], "You do not have permission to execute this workflow."
        )

    @patch("api_workflows.views.can_execute", return_value=False)
    def test_execute_workflow_denied_by_resource_scope(self, _can_execute):
        r = self.client.post(
            f"/api/v1/workflow/{self.wf1.id}/executions/",
            {"workflow_type": self.audit1.workflow_type, "mode": "manual"},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            r.json()["errors"], "You do not have permission to execute this workflow."
        )

    def test_execute_workflow_requires_mode_for_sql_review(self):
        r = self.client.post(
            f"/api/v1/workflow/{self.wf1.id}/executions/",
            {"workflow_type": self.audit1.workflow_type},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        error_msg = r.json()["errors"]
        if isinstance(error_msg, list):
            error_msg = error_msg[0]
        self.assertEqual(error_msg, "Missing mode.")

    def test_execute_manual_updates_workflow_status_and_log(self):
        self.client.post(
            f"/api/v1/workflow/{self.wf1.id}/reviews/",
            {
                "audit_remark": "approved",
                "workflow_type": self.audit1.workflow_type,
                "audit_type": "pass",
            },
            format="json",
        )
        r = self.client.post(
            f"/api/v1/workflow/{self.wf1.id}/executions/",
            {"workflow_type": self.audit1.workflow_type, "mode": "manual"},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.wf1.refresh_from_db()
        self.assertEqual(self.wf1.status, "workflow_finish")
        self.assertIsNotNone(self.wf1.finish_time)
        self.assertTrue(
            WorkflowLog.objects.filter(
                audit_id=self.audit1.audit_id, operation_type=6
            ).exists()
        )

    def test_update_workflow_execution_window(self):
        response = self.client.patch(
            f"/api/v1/workflow/{self.wf1.id}/window/",
            {
                "run_date_start": "2030-01-02T03:04",
                "run_date_end": "2030-01-02T05:04",
            },
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.wf1.refresh_from_db()
        self.assertEqual(
            self.wf1.run_date_start.strftime("%Y-%m-%d %H:%M"), "2030-01-02 03:04"
        )
        self.assertEqual(
            self.wf1.run_date_end.strftime("%Y-%m-%d %H:%M"), "2030-01-02 05:04"
        )

    def test_update_workflow_execution_window_alias(self):
        response = self.client.patch(
            f"/api/v1/workflow/{self.wf1.id}/execution-window/",
            {
                "run_date_start": "2030-01-02T03:04",
                "run_date_end": "2030-01-02T05:04",
            },
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.wf1.refresh_from_db()
        self.assertEqual(
            self.wf1.run_date_start.strftime("%Y-%m-%d %H:%M"), "2030-01-02 03:04"
        )
        self.assertEqual(
            self.wf1.run_date_end.strftime("%Y-%m-%d %H:%M"), "2030-01-02 05:04"
        )

    @patch("api_workflows.views.add_sql_schedule")
    def test_schedule_workflow(self, mock_add_schedule):
        self.client.post(
            f"/api/v1/workflow/{self.wf1.id}/reviews/",
            {
                "audit_remark": "approved",
                "workflow_type": self.audit1.workflow_type,
                "audit_type": "pass",
            },
            format="json",
        )

        response = self.client.post(
            f"/api/v1/workflow/{self.wf1.id}/schedule/",
            {
                "run_date": (datetime.now() + timedelta(hours=1)).strftime(
                    "%Y-%m-%dT%H:%M"
                ),
            },
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.wf1.refresh_from_db()
        self.assertEqual(self.wf1.status, "workflow_timingtask")
        mock_add_schedule.assert_called_once()
        self.assertTrue(
            WorkflowLog.objects.filter(
                audit_id=self.audit1.audit_id,
                operation_type=WorkflowAction.EXECUTE_SET_TIME,
            ).exists()
        )

    @patch("api_workflows.views.add_sql_schedule")
    @patch("api_workflows.views._resolve_mysql_ddl_executor")
    def test_schedule_mysql_workflow_persists_executor(
        self, mock_resolve_executor, mock_add_schedule
    ):
        _, workflow, _, _ = self._create_mysql_workflow()
        mock_resolve_executor.return_value = Mock(executor_id="direct")

        response = self.client.post(
            f"/api/v1/workflow/{workflow.id}/schedule/",
            {
                "run_date": (datetime.now() + timedelta(hours=1)).strftime(
                    "%Y-%m-%dT%H:%M"
                ),
                "executor": "direct",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            mock_add_schedule.call_args.kwargs["execution_options"],
            {"executor": "direct"},
        )


class TestPermissionRequestAPI(CacheIsolatedAPITestCase):
    def setUp(self):
        self.user = User(
            username="permission_user", display="Permission User", is_active=True
        )
        self.user.set_password("test_password")
        self.user.save()

        self.reviewer = User(
            username="permission_reviewer",
            display="Permission Reviewer",
            is_active=True,
        )
        self.reviewer.set_password("test_password")
        self.reviewer.save()

        permissions = Permission.objects.filter(
            codename__in=[
                "menu_queryapplylist",
                "query_applypriv",
                "query_review",
                "query_mgtpriv",
            ]
        )
        self.user.user_permissions.add(*permissions)
        self.reviewer.user_permissions.add(*permissions)

        self.review_group = Group.objects.create(name="Permission Reviewers")
        self.reviewer.groups.add(self.review_group)

        self.res_group = Team.objects.create(team_name="permission_rg")
        assign_user_to_team(self.reviewer, self.res_group)

        self.instance = Instance.objects.create(
            instance_name="permission_instance",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
            password="pwd",
        )
        self.instance.resource_group.add(self.res_group)

        authenticate_client(self.client, self.user)

    def _login_as(self, username, password="test_password"):
        authenticate_client(self.client, User.objects.get(username=username))

    def tearDown(self):
        TemporaryInstanceGrant.objects.all().delete()
        TemporaryTeamGrant.objects.all().delete()
        PermanentTeamGrant.objects.all().delete()
        PermissionRequest.objects.all().delete()
        WorkflowLog.objects.all().delete()
        WorkflowAudit.objects.all().delete()
        Instance.objects.all().delete()
        Team.objects.all().delete()
        Group.objects.all().delete()
        User.objects.filter(
            username__in=[
                "permission_user",
                "permission_reviewer",
                "other_requester",
                "temporary_permission_reviewer",
            ]
        ).delete()

    @patch("api_access.views.async_task")
    @patch("api_access.views._permission_request_audit_callback")
    @patch("api_access.views.get_auditor")
    def test_create_instance_permission_request(
        self, mock_get_auditor, mock_callback, mock_async_task
    ):
        mock_handler = Mock()
        mock_handler.workflow.request_id = 123
        mock_handler.audit.current_status = WorkflowStatus.WAITING
        mock_get_auditor.return_value = mock_handler

        with self.captureOnCommitCallbacks(execute=True):
            response = self.client.post(
                "/api/v1/access/request/",
                {
                    "title": "Need DML access",
                    "target_type": "instance",
                    "team_id": self.res_group.team_id,
                    "instance_id": self.instance.id,
                    "access_level": "query_dml",
                    "valid_date": "2099-12-31",
                },
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response_data(response)["request_id"], 123)
        mock_callback.assert_called_once()
        mock_async_task.assert_called_once()

    def test_request_list_only_shows_own_requests(self):
        PermissionRequest.objects.create(
            team=self.res_group,
            target_type="team",
            title="My request",
            user_name=self.user.username,
            user_display=self.user.display,
            valid_date=datetime.now().date() + timedelta(days=1),
            status=WorkflowStatus.WAITING,
            audit_auth_groups="",
        )
        other_user = User.objects.create(
            username="other_requester", display="Other Requester", is_active=True
        )
        PermissionRequest.objects.create(
            team=self.res_group,
            target_type="team",
            title="Other request",
            user_name=other_user.username,
            user_display=other_user.display,
            valid_date=datetime.now().date() + timedelta(days=1),
            status=WorkflowStatus.WAITING,
            audit_auth_groups="",
        )

        response = self.client.get("/api/v1/access/request/", format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response_data(response)
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["results"][0]["title"], "My request")

    def test_request_detail_returns_logs(self):
        permission_request = PermissionRequest.objects.create(
            team=self.res_group,
            target_type="team",
            title="Detail request",
            user_name=self.user.username,
            user_display=self.user.display,
            valid_date=datetime.now().date() + timedelta(days=1),
            status=WorkflowStatus.WAITING,
            audit_auth_groups=str(self.review_group.id),
        )
        audit = WorkflowAudit.objects.create(
            team_id=self.res_group.team_id,
            team_name=self.res_group.team_name,
            workflow_id=permission_request.request_id,
            workflow_type=WorkflowType.ACCESS_REQUEST,
            workflow_title=permission_request.title,
            audit_auth_groups=str(self.review_group.id),
            current_audit=str(self.review_group.id),
            next_audit="-1",
            current_status=WorkflowStatus.WAITING,
            create_user=self.user.username,
            create_user_display=self.user.display,
        )
        WorkflowLog.objects.create(
            audit_id=audit.audit_id,
            operation_type=WorkflowAction.SUBMIT,
            operation_type_desc="Submit",
            operation_info="Waiting for approval",
            operator=self.user.username,
            operator_display=self.user.display,
        )

        response = self.client.get(
            f"/api/v1/access/request/{permission_request.request_id}/",
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response_data(response)
        self.assertEqual(payload["title"], "Detail request")
        self.assertEqual(len(payload["logs"]), 1)

    def test_reviewer_can_see_pending_request_for_direct_member_group(self):
        permission_request = PermissionRequest.objects.create(
            team=self.res_group,
            target_type="team",
            title="Needs approval",
            user_name=self.user.username,
            user_display=self.user.display,
            valid_date=datetime.now().date() + timedelta(days=1),
            status=WorkflowStatus.WAITING,
            audit_auth_groups=str(self.review_group.id),
        )
        WorkflowAudit.objects.create(
            team_id=self.res_group.team_id,
            team_name=self.res_group.team_name,
            workflow_id=permission_request.request_id,
            workflow_type=WorkflowType.ACCESS_REQUEST,
            workflow_title=permission_request.title,
            audit_auth_groups=str(self.review_group.id),
            current_audit=str(self.review_group.id),
            next_audit="-1",
            current_status=WorkflowStatus.WAITING,
            create_user=self.user.username,
            create_user_display=self.user.display,
        )

        self._login_as(self.reviewer.username)
        response = self.client.get("/api/v1/access/request/", format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response_data(response)
        self.assertEqual(payload["count"], 1)
        self.assertEqual(
            payload["results"][0]["request_id"], permission_request.request_id
        )

    def test_temporary_group_access_does_not_expose_pending_approvals(self):
        temporary_reviewer = User.objects.create(
            username="temporary_permission_reviewer",
            display="Temporary Permission Reviewer",
            is_active=True,
        )
        temporary_reviewer.set_password("test_password")
        temporary_reviewer.save()
        temporary_reviewer.user_permissions.add(
            *Permission.objects.filter(
                codename__in=["menu_queryapplylist", "query_review"]
            )
        )
        temporary_reviewer.groups.add(self.review_group)

        permission_request = PermissionRequest.objects.create(
            team=self.res_group,
            target_type="team",
            title="Restricted approval",
            user_name=self.user.username,
            user_display=self.user.display,
            valid_date=datetime.now().date() + timedelta(days=1),
            status=WorkflowStatus.WAITING,
            audit_auth_groups=str(self.review_group.id),
        )
        WorkflowAudit.objects.create(
            team_id=self.res_group.team_id,
            team_name=self.res_group.team_name,
            workflow_id=permission_request.request_id,
            workflow_type=WorkflowType.ACCESS_REQUEST,
            workflow_title=permission_request.title,
            audit_auth_groups=str(self.review_group.id),
            current_audit=str(self.review_group.id),
            next_audit="-1",
            current_status=WorkflowStatus.WAITING,
            create_user=self.user.username,
            create_user_display=self.user.display,
        )
        TemporaryTeamGrant.objects.create(
            user=temporary_reviewer,
            team=self.res_group,
            permission_level=self.review_group,
            valid_date=datetime.now().date() + timedelta(days=1),
        )

        self._login_as(temporary_reviewer.username)
        response = self.client.get("/api/v1/access/request/", format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response_data(response)
        self.assertEqual(payload["count"], 0)

    def test_active_grant_list_and_revoke(self):
        TemporaryTeamGrant.objects.create(
            user=self.user,
            team=self.res_group,
            permission_level=self.review_group,
            valid_date=datetime.now().date() + timedelta(days=1),
        )
        instance_grant = TemporaryInstanceGrant.objects.create(
            user=self.user,
            team=self.res_group,
            instance=self.instance,
            access_level="query_dml",
            valid_date=datetime.now().date() + timedelta(days=1),
        )

        response = self.client.get("/api/v1/access/grant/", format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response_data(response)
        self.assertEqual(payload["count"], 2)

        revoke = self.client.delete(
            f"/api/v1/access/grant/instance/{instance_grant.grant_id}/",
            format="json",
        )
        self.assertEqual(revoke.status_code, status.HTTP_200_OK)
        instance_grant.refresh_from_db()
        self.assertEqual(instance_grant.is_revoked, True)


class TestDashboardAPI(CacheIsolatedAPITestCase):
    def setUp(self):
        self.user = User(
            username="dashboard_user",
            display="Dashboard User",
            is_active=True,
            is_superuser=True,
        )
        self.user.set_password("test_password")
        self.user.save()

        self.ins = Instance.objects.create(
            instance_name="dashboard_instance",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
            password="pwd",
        )
        self.workflow = SqlWorkflow.objects.create(
            workflow_name="dashboard-wf",
            demand_url="",
            team_id=1,
            team_name="DBA",
            instance=self.ins,
            db_name="mysql",
            syntax_type=2,
            is_backup=False,
            engineer=self.user.username,
            engineer_display=self.user.display,
            status="workflow_finish",
            audit_auth_groups="1",
        )
        QueryPrivilegesApply.objects.create(
            team_id=1,
            team_name="DBA",
            title="query-apply",
            user_name=self.user.username,
            user_display=self.user.display,
            instance=self.ins,
            db_list="mysql",
            table_list="",
            valid_date=(datetime.now() + timedelta(days=30)).date(),
            limit_num=100,
            priv_type=1,
            status=WorkflowStatus.WAITING,
            audit_auth_groups="1",
        )
        QueryLog.objects.create(
            username=self.user.username,
            user_display=self.user.display,
            db_name="mysql",
            instance_name=self.ins.instance_name,
            sqllog="select 1",
            effect_row=10,
            cost_time="0.1",
        )

        self.token = authenticate_client(self.client, self.user)["access"]

    def tearDown(self):
        QueryLog.objects.all().delete()
        QueryPrivilegesApply.objects.all().delete()
        SqlWorkflow.objects.all().delete()
        Instance.objects.all().delete()
        User.objects.filter(
            username__in=["dashboard_user", "dashboard_no_perm"]
        ).delete()

    def test_dashboard_overview_success(self):
        start_date = (datetime.now() - timedelta(days=6)).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")
        response = self.client.get(
            "/api/v1/dashboard/",
            {"start_date": start_date, "end_date": end_date},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        data = response_data(response)
        self.assertIn("summary", data)
        self.assertIn("charts", data)
        self.assertEqual(data["summary"]["sql_workflow_count"], 1)
        self.assertEqual(data["summary"]["query_workflow_count"], 1)
        self.assertEqual(data["summary"]["instance_count"], 1)

        charts = data["charts"]
        self.assertIn("query_activity", charts)
        self.assertIn("workflow_by_date", charts)
        self.assertIn("instance_type_distribution", charts)
        self.assertEqual(
            len(charts["workflow_by_date"]["labels"]),
            len(charts["workflow_by_date"]["values"]),
        )
        self.assertEqual(
            len(charts["query_activity"]["labels"]),
            len(charts["query_activity"]["query_count"]),
        )
        self.assertEqual(
            len(charts["query_activity"]["labels"]),
            len(charts["query_activity"]["scanned_rows"]),
        )

    def test_dashboard_overview_default_date_range(self):
        response = self.client.get("/api/v1/dashboard/", format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        data = response_data(response)
        self.assertIn("start_date", data)
        self.assertIn("end_date", data)

    def test_dashboard_overview_invalid_date_range(self):
        response = self.client.get(
            "/api/v1/dashboard/",
            {"start_date": "2026-03-05", "end_date": "2026-03-01"},
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("errors", response.json())

    def test_dashboard_overview_requires_permission(self):
        self.client.credentials()
        no_perm_user = User(
            username="dashboard_no_perm",
            display="No Perm",
            is_active=True,
            is_superuser=False,
        )
        no_perm_user.set_password("test_password")
        no_perm_user.save()

        authenticate_client(self.client, no_perm_user)

        response = self.client.get("/api/v1/dashboard/", format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)


class TestSystemSettings(CacheIsolatedAPITestCase):
    """Test SPA system settings APIs."""

    def setUp(self):
        self.staff_user = User.objects.create(
            username="staff_user",
            display="Staff User",
            email="staff@datamingle.test",
            is_active=True,
            is_staff=True,
        )
        self.staff_user.set_password("staff_password")
        self.staff_user.save(update_fields=["password"])
        self.regular_user = User.objects.create(
            username="regular_user",
            display="Regular User",
            email="regular@datamingle.test",
            is_active=True,
            is_staff=False,
        )
        self.regular_user.set_password("regular_password")
        self.regular_user.save(update_fields=["password"])
        self.group = Group.objects.create(name="Ops")
        self.team = Team.objects.create(team_name="Core Systems")

    def tearDown(self):
        SysConfig().purge()
        Team.objects.all().delete()
        Group.objects.all().delete()
        User.objects.all().delete()

    def authenticate(self, username, password):
        authenticate_client(self.client, User.objects.get(username=username))

    def test_staff_can_get_system_settings(self):
        self.authenticate("staff_user", "staff_password")

        response = self.client.get("/api/v1/system-settings/", format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response_data(response)
        self.assertNotIn("openai_api_key", payload["settings"])
        self.assertNotIn("openai_base_url", payload["settings"])
        self.assertNotIn("default_chat_model", payload["settings"])
        self.assertEqual(
            payload["settings"]["notify_phase_control"],
            list(NOTIFY_PHASE_OPTIONS),
        )
        self.assertEqual(payload["settings"]["storage_type"], "local")
        self.assertNotIn("enable_backup_switch", payload["settings"])
        self.assertNotIn("inception_remote_backup_host", payload["settings"])
        self.assertNotIn("go_inception_host", payload["settings"])
        self.assertNotIn("go_inception_port", payload["settings"])
        self.assertNotIn("go_inception_user", payload["settings"])
        self.assertNotIn("go_inception_password", payload["settings"])
        auth_group_values = {
            option["value"] for option in payload["options"]["auth_groups"]
        }
        self.assertIn(self.group.name, auth_group_values)

    def test_staff_gets_default_storage_type_when_blank_config_is_stored(self):
        SysConfig().set("storage_type", "")
        self.authenticate("staff_user", "staff_password")

        response = self.client.get("/api/v1/system-settings/", format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response_data(response)
        self.assertEqual(payload["settings"]["storage_type"], "local")

    def test_non_staff_users_cannot_access_system_settings(self):
        self.authenticate("regular_user", "regular_password")

        response = self.client.get("/api/v1/system-settings/", format="json")

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_staff_can_update_system_settings(self):
        self.authenticate("staff_user", "staff_password")
        current_settings = response_data(
            self.client.get("/api/v1/system-settings/", format="json")
        )["settings"]
        current_settings.update(
            {
                "auto_review": True,
                "auto_review_tag": [],
                "notify_phase_control": ["Apply", "Execute"],
                "storage_type": "sftp",
                "sftp_host": "sftp.internal",
                "sftp_port": 2222,
                "api_user_whitelist": [self.regular_user.id],
                "gh_ost": "/bin/echo",
                "pt_osc": "/bin/echo",
            }
        )

        response = self.client.put(
            "/api/v1/system-settings/", current_settings, format="json"
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response_data(response)["settings"]
        self.assertTrue(payload["auto_review"])
        self.assertEqual(payload["auto_review_tag"], [])
        self.assertEqual(payload["notify_phase_control"], ["Apply", "Execute"])
        self.assertEqual(payload["api_user_whitelist"], [self.regular_user.id])

        config = SysConfig()
        self.assertTrue(config.get("auto_review"))
        self.assertIn(config.get("auto_review_tag"), [None, ""])
        self.assertEqual(config.get("notify_phase_control"), "Apply,Execute")
        self.assertEqual(config.get("storage_type"), "sftp")
        self.assertEqual(config.get("sftp_port"), "2222")
        self.assertEqual(config.get("api_user_whitelist"), str(self.regular_user.id))
        self.assertEqual(config.get("gh_ost"), "/bin/echo")
        self.assertEqual(config.get("pt_osc"), "/bin/echo")

    def test_staff_rejects_invalid_osc_binary_path(self):
        self.authenticate("staff_user", "staff_password")
        current_settings = response_data(
            self.client.get("/api/v1/system-settings/", format="json")
        )["settings"]
        current_settings["gh_ost"] = "/path/that/does/not/exist"

        response = self.client.put(
            "/api/v1/system-settings/", current_settings, format="json"
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("gh-ost binary", response.json()["gh_ost"][0])

    def test_go_inception_connection_test_endpoint_is_removed(self):
        with self.assertRaises(Resolver404):
            resolve("/api/v1/system-settings/tests/go-inception/")

    @patch("api_admin.settings.validate_email_payload")
    def test_staff_can_run_email_test(self, validate_payload):
        validate_payload.return_value = {"status": 0, "msg": "ok", "data": []}
        self.authenticate("staff_user", "staff_password")

        response = self.client.post(
            "/api/v1/system-settings/tests/email/",
            {
                "mail": True,
                "mail_ssl": False,
                "mail_smtp_server": "smtp.datamingle.test",
                "mail_smtp_port": 587,
                "mail_smtp_user": "mailer",
                "mail_smtp_password": "secret",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        validate_payload.assert_called_once()

    @patch("api_admin.settings.validate_file_storage_payload")
    def test_staff_can_run_storage_test(self, validate_payload):
        validate_payload.return_value = {"status": 0, "msg": "ok", "data": []}
        self.authenticate("staff_user", "staff_password")

        response = self.client.post(
            "/api/v1/system-settings/tests/storage/",
            {"storage_type": "local", "max_export_rows": 10000},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        validate_payload.assert_called_once()


class ArchiveApiTests(CacheIsolatedAPITestCase):
    def setUp(self):
        self.password = "archive_password"

        self.team = Team.objects.create(team_name="Archive Group")
        self.other_team = Team.objects.create(team_name="Other Archive Group")

        self.mysql_instance = Instance.objects.create(
            instance_name="archive-mysql",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
            password="pwd",
        )
        self.mysql_instance.resource_group.add(self.team)
        self.mysql_instance.resource_group.add(self.other_team)

        self.pg_instance = Instance.objects.create(
            instance_name="archive-pg",
            type="master",
            db_type="pgsql",
            host="127.0.0.1",
            port=5432,
            user="postgres",
            password="pwd",
            db_name="workflow_pg",
        )
        self.pg_instance.resource_group.add(self.team)

        self.reviewer_auth_group = Group.objects.create(name="Archive DBA")

        self.requester = self._create_user(
            "archive_requester",
            "Archive Requester",
            permissions=("menu_archive", "archive_apply"),
            teams=(self.team,),
        )
        self.reviewer = self._create_user(
            "archive_reviewer",
            "Archive Reviewer",
            permissions=("menu_archive", "archive_review", "archive_mgt"),
            auth_groups=(self.reviewer_auth_group,),
            teams=(self.team,),
        )
        self.outsider = self._create_user(
            "archive_outsider",
            "Archive Outsider",
        )

        WorkflowAuditSetting.objects.create(
            workflow_type=WorkflowType.ARCHIVE,
            team_id=self.team.team_id,
            team_name=self.team.team_name,
            audit_auth_groups=str(self.reviewer_auth_group.id),
        )
        WorkflowAuditSetting.objects.create(
            workflow_type=WorkflowType.ARCHIVE,
            team_id=self.other_team.team_id,
            team_name=self.other_team.team_name,
            audit_auth_groups=str(self.reviewer_auth_group.id),
        )

    def _create_user(
        self,
        username,
        display,
        permissions=(),
        auth_groups=(),
        teams=(),
    ):
        user = User.objects.create(
            username=username,
            display=display,
            is_active=True,
        )
        user.set_password(self.password)
        user.save()
        for permission in permissions:
            user.user_permissions.add(Permission.objects.get(codename=permission))
        for auth_group in auth_groups:
            user.groups.add(auth_group)
        for team in teams:
            assign_user_to_team(user, team)
        return user

    def authenticate(self, user):
        return authenticate_client(self.client, user)["access"]

    def archive_payload(self, **overrides):
        payload = {
            "title": "Delete expired rows",
            "team_id": self.team.team_id,
            "instance_id": self.mysql_instance.id,
            "db_name": "demo_orders",
            "table_name": "orders",
            "condition": "created_at < {{ today }}",
            "archive_method": "dml",
            "execution_mode": "one_time",
        }
        payload.update(overrides)
        return payload

    def create_pending_archive(self, **overrides):
        self.authenticate(self.requester)
        with patch("api_archives.views.async_task"):
            response = self.client.post(
                "/api/v1/archive/",
                self.archive_payload(**overrides),
                format="json",
            )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        archive_id = assert_success_envelope(self, response)["id"]
        return ArchiveConfig.objects.get(id=archive_id)

    def test_archive_metadata_requires_archive_permission(self):
        self.authenticate(self.outsider)

        response = self.client.get("/api/v1/archive/metadata/", format="json")

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_archive_metadata_lists_archive_methods_by_engine(self):
        self.authenticate(self.requester)

        response = self.client.get("/api/v1/archive/metadata/", format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = assert_success_envelope(self, response)
        mysql_record = next(
            item
            for item in payload["instances"]
            if item["id"] == self.mysql_instance.id
        )
        pg_record = next(
            item for item in payload["instances"] if item["id"] == self.pg_instance.id
        )
        self.assertEqual(
            mysql_record["available_archive_methods"], ["dml", "pt_archiver"]
        )
        self.assertEqual(pg_record["available_archive_methods"], ["dml"])

    def test_archive_metadata_hides_unowned_groups_on_shared_instances(self):
        self.authenticate(self.requester)

        response = self.client.get("/api/v1/archive/metadata/", format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = assert_success_envelope(self, response)
        mysql_record = next(
            item
            for item in payload["instances"]
            if item["id"] == self.mysql_instance.id
        )
        team_ids = {group["team_id"] for group in payload["teams"]}
        self.assertEqual(mysql_record["team_ids"], [self.team.team_id])
        self.assertEqual(team_ids, {self.team.team_id})

    def test_archive_approval_preview_checks_group_access(self):
        self.authenticate(self.requester)

        response = self.client.get(
            f"/api/v1/archive/approval-preview/?team_id={self.team.team_id}",
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = assert_success_envelope(self, response)
        self.assertEqual(payload["display"], "Archive DBA")

        forbidden_response = self.client.get(
            f"/api/v1/archive/approval-preview/?team_id={self.other_team.team_id}",
            format="json",
        )
        self.assertEqual(forbidden_response.status_code, status.HTTP_403_FORBIDDEN)

    def test_create_one_time_archive_request(self):
        self.authenticate(self.requester)

        with patch("api_archives.views.async_task"):
            response = self.client.post(
                "/api/v1/archive/",
                self.archive_payload(),
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        archive_id = assert_success_envelope(self, response)["id"]
        archive = ArchiveConfig.objects.get(id=archive_id)
        self.assertEqual(archive.mode, "purge")
        self.assertFalse(archive.no_delete)
        self.assertEqual(archive.archive_method, "dml")
        self.assertEqual(archive.execution_mode, "one_time")
        self.assertEqual(archive.status, WorkflowStatus.WAITING)
        self.assertFalse(archive.state)

    @patch("api_archives.views.get_auditor")
    def test_create_archive_hides_internal_audit_errors(self, get_auditor_mock):
        self.authenticate(self.requester)
        audit_handler = Mock()
        audit_handler.create_audit.side_effect = AuditException("db connection failed")
        get_auditor_mock.return_value = audit_handler

        response = self.client.post(
            "/api/v1/archive/",
            self.archive_payload(),
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.json()["errors"],
            "Failed to create approval flow. Contact admin.",
        )

    def test_create_archive_rejects_unowned_group_on_shared_instance(self):
        self.authenticate(self.requester)

        with patch("api_archives.views.async_task"):
            response = self.client.post(
                "/api/v1/archive/",
                self.archive_payload(team_id=self.other_team.team_id),
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_create_scheduled_daily_archive_request(self):
        self.authenticate(self.requester)

        with patch("api_archives.views.async_task"):
            response = self.client.post(
                "/api/v1/archive/",
                self.archive_payload(
                    execution_mode="scheduled",
                    schedule_frequency="daily",
                    schedule_time="02:15",
                ),
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        archive = ArchiveConfig.objects.get(
            id=assert_success_envelope(self, response)["id"]
        )
        self.assertEqual(archive.execution_mode, "scheduled")
        self.assertEqual(archive.schedule_frequency, "daily")
        self.assertEqual(archive.schedule_time.strftime("%H:%M"), "02:15")
        self.assertEqual(archive.schedule_weekdays, "")

    def test_create_scheduled_weekly_archive_request(self):
        self.authenticate(self.requester)

        with patch("api_archives.views.async_task"):
            response = self.client.post(
                "/api/v1/archive/",
                self.archive_payload(
                    execution_mode="scheduled",
                    schedule_frequency="weekly",
                    schedule_time="03:30",
                    schedule_weekdays=["mon", "fri"],
                ),
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        archive = ArchiveConfig.objects.get(
            id=assert_success_envelope(self, response)["id"]
        )
        self.assertEqual(archive.schedule_frequency, "weekly")
        self.assertEqual(archive.schedule_time.strftime("%H:%M"), "03:30")
        self.assertEqual(archive.schedule_weekdays, "mon,fri")

    def test_create_archive_rejects_pt_archiver_for_non_mysql(self):
        self.authenticate(self.requester)

        with patch("api_archives.views.async_task"):
            response = self.client.post(
                "/api/v1/archive/",
                self.archive_payload(
                    instance_id=self.pg_instance.id,
                    archive_method="pt_archiver",
                ),
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("pt-archiver", " ".join(response.json()["errors"]))

    def test_create_archive_requires_schedule_fields(self):
        self.authenticate(self.requester)

        with patch("api_archives.views.async_task"):
            response = self.client.post(
                "/api/v1/archive/",
                self.archive_payload(
                    execution_mode="scheduled",
                    schedule_frequency="weekly",
                    schedule_weekdays=[],
                ),
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("schedule_frequency", " ".join(response.json()["errors"]))

    def test_review_pass_for_scheduled_archive_arms_next_run(self):
        archive = self.create_pending_archive(
            execution_mode="scheduled",
            schedule_frequency="daily",
            schedule_time="02:00",
        )
        self.authenticate(self.reviewer)
        next_run = datetime.now() + timedelta(days=1)

        with patch(
            "api_archives.views.calculate_next_archive_run", return_value=next_run
        ), patch("api_archives.views.schedule_archive") as schedule_archive_mock, patch(
            "api_archives.views.async_task"
        ):
            response = self.client.post(
                f"/api/v1/archive/{archive.id}/reviews/",
                {"audit_type": "pass", "audit_remark": "Looks good"},
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        archive.refresh_from_db()
        self.assertEqual(archive.status, WorkflowStatus.PASSED)
        self.assertTrue(archive.state)
        self.assertEqual(
            archive.next_run_at.replace(tzinfo=None), next_run.replace(tzinfo=None)
        )
        schedule_archive_mock.assert_called_once()

    def test_review_reject_and_cancel_archive_workflows(self):
        reject_archive = self.create_pending_archive(
            title="Reject me",
            execution_mode="scheduled",
            schedule_frequency="daily",
            schedule_time="01:00",
        )
        self.authenticate(self.reviewer)

        with patch("api_archives.views.cancel_archive_schedule") as cancel_mock, patch(
            "api_archives.views.async_task"
        ):
            reject_response = self.client.post(
                f"/api/v1/archive/{reject_archive.id}/reviews/",
                {"audit_type": "reject", "audit_remark": "No"},
                format="json",
            )

        self.assertEqual(reject_response.status_code, status.HTTP_200_OK)
        reject_archive.refresh_from_db()
        self.assertEqual(reject_archive.status, WorkflowStatus.REJECTED)
        self.assertFalse(reject_archive.state)
        self.assertIsNone(reject_archive.next_run_at)
        cancel_mock.assert_called_once_with(reject_archive.id)

        cancel_archive = self.create_pending_archive(title="Cancel me")
        self.authenticate(self.requester)
        with patch("api_archives.views.cancel_archive_schedule") as cancel_mock, patch(
            "api_archives.views.async_task"
        ):
            cancel_response = self.client.post(
                f"/api/v1/archive/{cancel_archive.id}/reviews/",
                {"audit_type": "cancel", "audit_remark": "Stop this"},
                format="json",
            )

        self.assertEqual(cancel_response.status_code, status.HTTP_200_OK)
        cancel_archive.refresh_from_db()
        self.assertEqual(cancel_archive.status, WorkflowStatus.ABORTED)
        self.assertFalse(cancel_archive.state)
        self.assertIsNone(cancel_archive.next_run_at)
        cancel_mock.assert_called_once_with(cancel_archive.id)

    def test_run_now_queues_execution_for_approved_archive(self):
        archive = self.create_pending_archive()
        archive.status = WorkflowStatus.PASSED
        archive.state = True
        archive.save(update_fields=["status", "state"])
        audit = WorkflowAudit.objects.get(
            workflow_type=WorkflowType.ARCHIVE,
            workflow_id=archive.id,
        )
        audit.current_status = WorkflowStatus.PASSED
        audit.current_audit = ""
        audit.next_audit = ""
        audit.save(update_fields=["current_status", "current_audit", "next_audit"])

        self.authenticate(self.reviewer)

        with patch("api_archives.views.async_task") as async_task_mock:
            response = self.client.post(
                f"/api/v1/archive/{archive.id}/run/",
                {},
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        archive.refresh_from_db()
        self.assertEqual(archive.execution_state, "queued")
        async_task_mock.assert_called_once_with(
            "sql.archiver.archive",
            archive.id,
            "manual",
            hook="sql.archiver.archive_task_callback",
            timeout=-1,
            task_name=f"archive-{archive.id}",
        )

    def test_run_now_rejects_already_queued_archive(self):
        archive = self.create_pending_archive()
        archive.status = WorkflowStatus.PASSED
        archive.state = True
        archive.execution_state = "queued"
        archive.save(update_fields=["status", "state", "execution_state"])
        self.authenticate(self.reviewer)

        with patch("api_archives.views.async_task") as async_task_mock:
            response = self.client.post(
                f"/api/v1/archive/{archive.id}/run/",
                {},
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("already queued or running", response.json()["errors"])
        async_task_mock.assert_not_called()

    def test_run_now_requires_manager_group_scope(self):
        archive = ArchiveConfig.objects.create(
            title="Other group archive",
            team=self.other_team,
            audit_auth_groups="",
            src_instance=self.mysql_instance,
            src_db_name="demo_orders",
            src_table_name="orders",
            condition="id = 1",
            mode="purge",
            no_delete=False,
            sleep=1,
            archive_method="dml",
            execution_mode="one_time",
            status=WorkflowStatus.PASSED,
            state=True,
            user_name=self.requester.username,
            user_display=self.requester.display,
        )
        self.authenticate(self.reviewer)

        with patch("api_archives.views.async_task") as async_task_mock:
            response = self.client.post(
                f"/api/v1/archive/{archive.id}/run/",
                {},
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        async_task_mock.assert_not_called()

    def test_disable_scheduled_archive_cancels_current_schedule(self):
        archive = self.create_pending_archive(
            execution_mode="scheduled",
            schedule_frequency="daily",
            schedule_time="04:00",
        )
        archive.status = WorkflowStatus.PASSED
        archive.state = True
        archive.next_run_at = datetime.now() + timedelta(days=1)
        archive.save(update_fields=["status", "state", "next_run_at"])

        self.authenticate(self.reviewer)

        with patch("api_archives.views.cancel_archive_schedule") as cancel_mock:
            response = self.client.post(
                f"/api/v1/archive/{archive.id}/state/",
                {"enabled": False},
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        archive.refresh_from_db()
        self.assertFalse(archive.state)
        self.assertIsNone(archive.next_run_at)
        cancel_mock.assert_called_once_with(archive.id)

    def test_state_update_requires_manager_group_scope(self):
        archive = ArchiveConfig.objects.create(
            title="Other group scheduled archive",
            team=self.other_team,
            audit_auth_groups="",
            src_instance=self.mysql_instance,
            src_db_name="demo_orders",
            src_table_name="orders",
            condition="id = 1",
            mode="purge",
            no_delete=False,
            sleep=1,
            archive_method="dml",
            execution_mode="scheduled",
            schedule_frequency="daily",
            schedule_time=datetime.now().time().replace(second=0, microsecond=0),
            schedule_weekdays="",
            next_run_at=datetime.now() + timedelta(days=1),
            status=WorkflowStatus.PASSED,
            state=True,
            user_name=self.requester.username,
            user_display=self.requester.display,
        )
        self.authenticate(self.reviewer)

        with patch("api_archives.views.cancel_archive_schedule") as cancel_mock:
            response = self.client.post(
                f"/api/v1/archive/{archive.id}/state/",
                {"enabled": False},
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        cancel_mock.assert_not_called()

    def test_archive_detail_flags_and_log_endpoint(self):
        archive = self.create_pending_archive(
            execution_mode="scheduled",
            schedule_frequency="weekly",
            schedule_time="05:30",
            schedule_weekdays=["mon"],
        )
        archive.status = WorkflowStatus.PASSED
        archive.state = True
        archive.next_run_at = datetime.now() + timedelta(days=3)
        archive.save(update_fields=["status", "state", "next_run_at"])

        audit = WorkflowAudit.objects.get(
            workflow_type=WorkflowType.ARCHIVE,
            workflow_id=archive.id,
        )
        audit.current_status = WorkflowStatus.PASSED
        audit.current_audit = ""
        audit.next_audit = ""
        audit.save(update_fields=["current_status", "current_audit", "next_audit"])

        WorkflowLog.objects.create(
            audit_id=audit.audit_id,
            operation_type=WorkflowAction.SUBMIT,
            operation_type_desc="Archive Submitted",
            operation_info="Archive workflow created",
            operator=self.requester.username,
            operator_display=self.requester.display,
        )
        ArchiveLog.objects.create(
            archive=archive,
            cmd="DELETE FROM orders WHERE created_at < '2026-04-18'",
            condition="created_at < '2026-04-18'",
            archive_method="dml",
            mode="purge",
            no_delete=False,
            sleep=1,
            select_cnt=5,
            insert_cnt=0,
            delete_cnt=5,
            statistics="deleted=5",
            success=True,
            error_info="",
            start_time=datetime.now(),
            end_time=datetime.now(),
        )

        self.authenticate(self.reviewer)

        detail_response = self.client.get(
            f"/api/v1/archive/{archive.id}/",
            format="json",
        )
        self.assertEqual(detail_response.status_code, status.HTTP_200_OK)
        detail_payload = assert_success_envelope(self, detail_response)
        self.assertTrue(detail_payload["is_can_run_now"])
        self.assertFalse(detail_payload["is_can_enable"])
        self.assertTrue(detail_payload["is_can_disable"])
        self.assertEqual(len(detail_payload["archive_logs"]), 1)
        self.assertEqual(
            detail_payload["logs"][0]["operation_type_desc"], "Archive Submitted"
        )

        log_response = self.client.get(
            f"/api/v1/archive/{archive.id}/logs/",
            format="json",
        )
        self.assertEqual(log_response.status_code, status.HTTP_200_OK)
        log_payload = assert_success_envelope(self, log_response)
        self.assertEqual(log_payload["count"], 1)
        self.assertEqual(log_payload["results"][0]["delete_cnt"], 5)
