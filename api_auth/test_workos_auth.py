import json
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse
from unittest.mock import patch

from django.contrib.auth.models import Group
from django.test import override_settings
from rest_framework import status
from rest_framework.test import APITestCase

from common.config import SysConfig
from sql.models import ResourceGroup, Users


class FakeRedis:
    def __init__(self):
        self.values = {}

    def set(self, key, value, ex=None):
        self.values[key] = value

    def get(self, key):
        return self.values.get(key)

    def delete(self, key):
        self.values.pop(key, None)


@override_settings(
    AUTH_MODE="workos",
    ENABLE_WORKOS_AUTH=True,
    WORKOS_API_KEY="sk_test_123",
    WORKOS_CLIENT_ID="client_test_123",
    WORKOS_ORGANIZATION_ID="org_test_123",
    WORKOS_REDIRECT_URI="https://tenant.datamingle.dev/api/auth/workos/callback/",
    WORKOS_LOGOUT_REDIRECT_URI="https://tenant.datamingle.dev/login",
    WORKOS_STAFF_EMAILS=["staff@datamingle.dev"],
    WORKOS_SUPERUSER_EMAILS=["admin@datamingle.dev"],
)
class WorkOSAuthApiTests(APITestCase):
    def setUp(self):
        self.redis = FakeRedis()
        self.auth_group = Group.objects.create(name="DBA")
        self.resource_group = ResourceGroup.objects.create(group_name="Primary Team")
        self.sys_config = SysConfig()
        self.sys_config.set("default_auth_group", self.auth_group.name)
        self.sys_config.set("default_resource_group", self.resource_group.group_name)

    def tearDown(self):
        self.sys_config.purge()
        Users.objects.all().delete()
        ResourceGroup.objects.all().delete()
        Group.objects.all().delete()

    @patch("api_auth.views.get_redis_connection")
    @patch("api_auth.views.WorkOSAuthClient")
    def test_authorize_redirects_to_workos(
        self, mock_client_class, mock_redis_connection
    ):
        mock_redis_connection.return_value = self.redis
        mock_client_class.return_value.get_authorization_url.return_value = (
            "https://workos.example/authorize"
        )

        response = self.client.get("/api/auth/workos/authorize/")

        self.assertEqual(response.status_code, status.HTTP_302_FOUND)
        self.assertEqual(response.url, "https://workos.example/authorize")
        self.assertIn("datamingle_workos_state", response.cookies)

    @patch("api_auth.views.get_redis_connection")
    @patch("api_auth.views.WorkOSAuthClient")
    def test_callback_jit_provisions_user_and_exchange_returns_local_tokens(
        self, mock_client_class, mock_redis_connection
    ):
        mock_redis_connection.return_value = self.redis
        mock_client = mock_client_class.return_value
        mock_client.authenticate_with_code.return_value = SimpleNamespace(
            user_id="user_123",
            email="admin@datamingle.dev",
            first_name="Admin",
            last_name="User",
            profile_picture_url="https://images.workos.dev/avatar.png",
            organization_id="org_test_123",
            session_id="session_123",
            display_name="Admin User",
        )

        self.client.cookies["datamingle_workos_state"] = "state_123"
        callback_response = self.client.get(
            "/api/auth/workos/callback/",
            {"code": "code_123", "state": "state_123"},
        )

        self.assertEqual(callback_response.status_code, status.HTTP_302_FOUND)
        self.assertTrue(callback_response.url.startswith("/login/callback?code="))
        self.assertIn("datamingle_workos_session_id", callback_response.cookies)

        created_user = Users.objects.get(workos_user_id="user_123")
        self.assertEqual(created_user.username, "admin@datamingle.dev")
        self.assertEqual(created_user.email, "admin@datamingle.dev")
        self.assertEqual(
            created_user.avatar_url, "https://images.workos.dev/avatar.png"
        )
        self.assertTrue(created_user.is_staff)
        self.assertTrue(created_user.is_superuser)
        self.assertEqual(
            list(created_user.groups.values_list("name", flat=True)),
            [self.auth_group.name],
        )
        self.assertEqual(
            list(created_user.resource_group.values_list("group_name", flat=True)),
            [self.resource_group.group_name],
        )

        exchange_code = parse_qs(urlparse(callback_response.url).query)["code"][0]
        exchange_response = self.client.post(
            "/api/auth/workos/exchange/",
            {"code": exchange_code},
            format="json",
        )

        self.assertEqual(exchange_response.status_code, status.HTTP_200_OK)
        payload = exchange_response.json()["data"]
        self.assertIn("access", payload)
        self.assertIn("refresh", payload)
        self.assertIsNone(self.redis.get(f"workos-exchange-code:{exchange_code}"))

    @patch("api_auth.views.get_redis_connection")
    @patch("api_auth.views.WorkOSAuthClient")
    def test_exchange_rejects_invalid_code(
        self, mock_client_class, mock_redis_connection
    ):
        mock_redis_connection.return_value = self.redis
        mock_client_class.return_value.get_authorization_url.return_value = ""

        response = self.client.post(
            "/api/auth/workos/exchange/",
            {"code": "missing"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.json()["errors"],
            "The WorkOS login exchange code is invalid or expired.",
        )

    def test_password_login_is_disabled_in_workos_mode(self):
        user = Users.objects.create_user(
            username="builtin-user",
            password="test-password",
            display="Builtin User",
        )
        try:
            response = self.client.post(
                "/api/auth/token/",
                {"username": user.username, "password": "test-password"},
                format="json",
            )
            self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
            self.assertEqual(
                response.json()["errors"],
                "Password login is disabled while WorkOS authentication is active.",
            )
        finally:
            user.delete()

    @patch("api_auth.views.WorkOSAuthClient")
    def test_logout_redirects_through_workos_when_session_cookie_present(
        self, mock_client_class
    ):
        mock_client_class.return_value.get_logout_url.return_value = (
            "https://workos.example/logout"
        )
        self.client.cookies["datamingle_workos_session_id"] = "session_123"

        response = self.client.get("/api/auth/workos/logout/")

        self.assertEqual(response.status_code, status.HTTP_302_FOUND)
        self.assertEqual(response.url, "https://workos.example/logout")
        self.assertEqual(response.cookies["datamingle_workos_session_id"].value, "")

    def test_auth_config_reports_workos_mode(self):
        response = self.client.get("/api/auth/config/")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.json()["data"], {"mode": "workos"})

    def test_current_user_context_marks_workos_managed_users(self):
        user = Users.objects.create_user(
            username="managed@datamingle.dev",
            email="managed@datamingle.dev",
            display="Managed User",
            avatar_url="https://images.workos.dev/avatar.png",
            workos_user_id="user_123",
            is_active=True,
        )
        self.client.force_authenticate(user=user)

        response = self.client.get("/api/v1/me/", format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.json()["data"]["is_workos_managed"])
        self.assertEqual(
            response.json()["data"]["avatar_url"],
            "https://images.workos.dev/avatar.png",
        )

    def test_current_user_profile_update_is_blocked_for_workos_managed_users(self):
        user = Users.objects.create_user(
            username="managed@datamingle.dev",
            email="managed@datamingle.dev",
            display="Managed User",
            workos_user_id="user_123",
            is_active=True,
        )
        self.client.force_authenticate(user=user)

        response = self.client.patch(
            "/api/v1/me/",
            {"display": "Updated Name"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.json()[0],
            "Profile fields synced from WorkOS cannot be edited in Datamingle.",
        )
        user.refresh_from_db()
        self.assertEqual(user.display, "Managed User")

    def test_superuser_can_change_groups_but_not_identity_fields_for_workos_users(self):
        superuser = Users.objects.create_user(
            username="superuser@datamingle.dev",
            email="superuser@datamingle.dev",
            display="Super User",
            is_active=True,
            is_superuser=True,
            is_staff=True,
        )
        workos_user = Users.objects.create_user(
            username="managed@datamingle.dev",
            email="managed@datamingle.dev",
            display="Managed User",
            workos_user_id="user_123",
            is_active=True,
        )
        new_group = Group.objects.create(name="Ops")
        self.client.force_authenticate(user=superuser)

        reject_response = self.client.put(
            f"/api/v1/user/{workos_user.id}/",
            {
                "display": "Updated Name",
                "email": "updated@datamingle.dev",
                "group_ids": [new_group.id],
                "is_active": True,
            },
            format="json",
        )
        self.assertEqual(reject_response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("display", reject_response.json())

        allow_response = self.client.put(
            f"/api/v1/user/{workos_user.id}/",
            {
                "group_ids": [new_group.id],
                "is_active": False,
            },
            format="json",
        )
        self.assertEqual(allow_response.status_code, status.HTTP_200_OK)
        workos_user.refresh_from_db()
        self.assertFalse(workos_user.is_active)
        self.assertEqual(
            list(workos_user.groups.values_list("name", flat=True)),
            ["Ops"],
        )

    @patch("api_auth.views.get_redis_connection")
    @patch("api_auth.views.WorkOSAuthClient")
    def test_callback_rejects_unexpected_organization(
        self, mock_client_class, mock_redis_connection
    ):
        mock_redis_connection.return_value = self.redis
        mock_client = mock_client_class.return_value
        mock_client.authenticate_with_code.return_value = SimpleNamespace(
            user_id="user_123",
            email="staff@datamingle.dev",
            first_name="Staff",
            last_name="User",
            profile_picture_url="",
            organization_id="org_other",
            session_id="session_123",
            display_name="Staff User",
        )

        self.client.cookies["datamingle_workos_state"] = "state_123"
        response = self.client.get(
            "/api/auth/workos/callback/",
            {"code": "code_123", "state": "state_123"},
        )

        self.assertEqual(response.status_code, status.HTTP_302_FOUND)
        self.assertIn(
            "unexpected+organization",
            response.url,
        )
