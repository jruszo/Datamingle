import base64
import json
from datetime import datetime, timedelta, timezone as datetime_timezone
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse
from unittest.mock import patch

from django.contrib.auth.models import Group
from django.core.exceptions import ImproperlyConfigured
from django.test import override_settings
from rest_framework import status
from rest_framework.exceptions import AuthenticationFailed
from rest_framework.test import APIRequestFactory
from rest_framework.test import APITestCase

from common.config import SysConfig
from common.auth import SUPERADMIN_GROUP_NAME
from common.authenticate.workos import WorkOSAuthClient
from common.authenticate.workos_jwt import WorkOSJWTAuthentication
from sql.models import (
    Team,
    Users,
)


def _jwt(payload):
    def encode(segment):
        raw = json.dumps(segment).encode("utf-8")
        return base64.urlsafe_b64encode(raw).decode("utf-8").rstrip("=")

    return f"{encode({'alg': 'none'})}.{encode(payload)}."


class WorkOSAuthClientConfigTests(APITestCase):
    @override_settings(
        WORKOS_API_KEY="",
        WORKOS_CLIENT_ID="",
        WORKOS_ORGANIZATION_ID="",
    )
    def test_client_reports_missing_workos_settings_when_auth_is_used(self):
        with self.assertRaisesMessage(
            ImproperlyConfigured,
            "Missing required WorkOS settings: "
            "WORKOS_API_KEY, WORKOS_CLIENT_ID, WORKOS_ORGANIZATION_ID",
        ):
            WorkOSAuthClient()

    def test_authenticate_with_code_extracts_role_claims_from_access_token(self):
        client = WorkOSAuthClient.__new__(WorkOSAuthClient)
        client.client = SimpleNamespace(
            user_management=SimpleNamespace(
                authenticate_with_code=lambda **kwargs: SimpleNamespace(
                    user=SimpleNamespace(
                        id="user_123",
                        email="Admin@DataMingle.dev",
                        first_name="Admin",
                        last_name="User",
                        profile_picture_url="",
                    ),
                    organization_id="org_test_123",
                    access_token=_jwt(
                        {
                            "sid": "session_123",
                            "role": "admin",
                            "roles": ["admin"],
                        }
                    ),
                    refresh_token="workos_refresh_123",
                )
            )
        )

        auth_result = client.authenticate_with_code(
            code="code_123",
            ip_address="127.0.0.1",
            user_agent="test-agent",
        )

        self.assertEqual(auth_result.email, "admin@datamingle.dev")
        self.assertEqual(
            auth_result.role_slugs,
            ("admin",),
        )
        self.assertTrue(auth_result.access_token)
        self.assertEqual(auth_result.refresh_token, "workos_refresh_123")

    def test_authenticate_with_code_refreshes_organization_membership_roles(self):
        client = WorkOSAuthClient.__new__(WorkOSAuthClient)
        calls = []

        def list_organization_memberships(**kwargs):
            calls.append(kwargs)
            return SimpleNamespace(
                data=[
                    SimpleNamespace(
                        status="active",
                        role={"slug": "admin"},
                        roles=[{"slug": "member"}],
                    )
                ],
                list_metadata=SimpleNamespace(after=None),
            )

        client.client = SimpleNamespace(
            user_management=SimpleNamespace(
                authenticate_with_code=lambda **kwargs: SimpleNamespace(
                    user=SimpleNamespace(
                        id="user_123",
                        email="Admin@DataMingle.dev",
                        first_name="Admin",
                        last_name="User",
                        profile_picture_url="",
                    ),
                    organization_id="org_test_123",
                    access_token=_jwt({"sid": "session_123"}),
                    refresh_token="workos_refresh_123",
                ),
                list_organization_memberships=list_organization_memberships,
            )
        )

        auth_result = client.authenticate_with_code(
            code="code_123",
            ip_address="127.0.0.1",
            user_agent="test-agent",
        )

        self.assertEqual(auth_result.role_slugs, ("admin", "member"))
        self.assertEqual(
            calls,
            [
                {
                    "user_id": "user_123",
                    "organization_id": "org_test_123",
                    "statuses": ["active"],
                    "limit": 100,
                    "order": "asc",
                }
            ],
        )


class WorkOSJWTAuthenticationTests(APITestCase):
    def setUp(self):
        self.factory = APIRequestFactory()
        self.user = Users.objects.create_user(
            username="jwt@datamingle.dev",
            email="jwt@datamingle.dev",
            display="JWT User",
            is_active=True,
            workos_user_id="user_jwt_123",
        )

    @patch("common.authenticate.workos_jwt.WorkOSJWTVerifier.verify")
    def test_authenticates_linked_workos_user(self, mock_verify):
        mock_verify.return_value = {
            "sub": "user_jwt_123",
            "user_id": "user_jwt_123",
            "org_id": "org_test_123",
        }
        request = self.factory.get(
            "/api/v1/user/current/",
            HTTP_AUTHORIZATION="Bearer workos_access_123",
        )

        user, payload = WorkOSJWTAuthentication().authenticate(request)

        self.assertEqual(user.pk, self.user.pk)
        self.assertEqual(payload["org_id"], "org_test_123")
        self.assertEqual(user.organization_id, "org_test_123")

    @patch("common.authenticate.workos_jwt.WorkOSJWTVerifier.verify")
    def test_rejects_unlinked_workos_user(self, mock_verify):
        mock_verify.return_value = {
            "sub": "user_missing",
            "user_id": "user_missing",
            "org_id": "org_test_123",
        }
        request = self.factory.get(
            "/api/v1/user/current/",
            HTTP_AUTHORIZATION="Bearer workos_access_123",
        )

        with self.assertRaises(AuthenticationFailed):
            WorkOSJWTAuthentication().authenticate(request)


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
    WORKOS_API_KEY="sk_test_123",
    WORKOS_CLIENT_ID="client_test_123",
    WORKOS_ORGANIZATION_ID="org_test_123",
    WORKOS_STAFF_EMAILS=["staff@datamingle.dev"],
    WORKOS_SUPERUSER_EMAILS=["admin@datamingle.dev"],
)
class WorkOSAuthApiTests(APITestCase):
    def setUp(self):
        self.redis = FakeRedis()
        self.auth_group, _ = Group.objects.get_or_create(name=SUPERADMIN_GROUP_NAME)
        self.permission_level, _ = Group.objects.get_or_create(name="QA")
        self.team = Team.objects.create(team_name="Primary Team")
        self.sys_config = SysConfig()

    def tearDown(self):
        self.sys_config.purge()
        Users.objects.all().delete()
        Team.objects.all().delete()
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
        mock_client_class.return_value.get_authorization_url.assert_called_once_with(
            state=response.cookies["datamingle_workos_state"].value,
            redirect_uri="http://testserver/api/auth/workos/callback/",
        )

    @patch("api_auth.views.get_redis_connection")
    @patch("api_auth.views.WorkOSAuthClient")
    def test_callback_jit_provisions_user_and_exchange_returns_workos_tokens(
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
            access_token="workos_access_123",
            refresh_token="workos_refresh_123",
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
            set(created_user.groups.values_list("name", flat=True)),
            {SUPERADMIN_GROUP_NAME},
        )

        exchange_code = parse_qs(urlparse(callback_response.url).query)["code"][0]
        exchange_response = self.client.post(
            "/api/auth/workos/exchange/",
            {"code": exchange_code},
            format="json",
        )

        self.assertEqual(exchange_response.status_code, status.HTTP_200_OK)
        payload = exchange_response.json()["data"]
        self.assertEqual(payload["access"], "workos_access_123")
        self.assertEqual(payload["refresh"], "workos_refresh_123")
        self.assertIsNone(self.redis.get(f"workos-exchange-code:{exchange_code}"))

    @patch("api_auth.views.get_redis_connection")
    @patch("api_auth.views.WorkOSAuthClient")
    def test_callback_refreshes_workos_superadmin_role_membership(
        self, mock_client_class, mock_redis_connection
    ):
        mock_redis_connection.return_value = self.redis
        user = Users.objects.create_user(
            username="temporary.admin@datamingle.dev",
            email="temporary.admin@datamingle.dev",
            display="Temporary Admin",
            is_active=True,
            workos_user_id="user_temp_admin",
        )
        mock_client = mock_client_class.return_value
        mock_client.authenticate_with_code.return_value = SimpleNamespace(
            user_id="user_temp_admin",
            email="temporary.admin@datamingle.dev",
            first_name="Temporary",
            last_name="Admin",
            profile_picture_url="",
            organization_id="org_test_123",
            session_id="session_123",
            access_token="workos_access_123",
            refresh_token="workos_refresh_123",
            display_name="Temporary Admin",
            role_slugs=("admin",),
        )

        self.client.cookies["datamingle_workos_state"] = "state_123"
        first_response = self.client.get(
            "/api/auth/workos/callback/",
            {"code": "code_123", "state": "state_123"},
        )

        self.assertEqual(first_response.status_code, status.HTTP_302_FOUND)
        user.refresh_from_db()
        self.assertTrue(user.is_superuser)
        self.assertTrue(user.is_staff)
        self.assertTrue(user.groups.filter(name=SUPERADMIN_GROUP_NAME).exists())

        mock_client.authenticate_with_code.return_value = SimpleNamespace(
            user_id="user_temp_admin",
            email="temporary.admin@datamingle.dev",
            first_name="Temporary",
            last_name="Admin",
            profile_picture_url="",
            organization_id="org_test_123",
            session_id="session_456",
            access_token="workos_access_456",
            refresh_token="workos_refresh_456",
            display_name="Temporary Admin",
            role_slugs=("member",),
        )
        self.client.cookies["datamingle_workos_state"] = "state_456"
        second_response = self.client.get(
            "/api/auth/workos/callback/",
            {"code": "code_456", "state": "state_456"},
        )

        self.assertEqual(second_response.status_code, status.HTTP_302_FOUND)
        user.refresh_from_db()
        self.assertFalse(user.is_superuser)
        self.assertFalse(user.is_staff)
        self.assertFalse(user.groups.filter(name=SUPERADMIN_GROUP_NAME).exists())

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

    @patch("api_auth.views.WorkOSAuthClient")
    def test_token_refresh_returns_workos_tokens(self, mock_client_class):
        mock_client_class.return_value.authenticate_with_refresh_token.return_value = {
            "access": "workos_access_refreshed",
            "refresh": "workos_refresh_rotated",
        }

        response = self.client.post(
            "/api/auth/token/refresh/",
            {"refresh": "workos_refresh_old"},
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response.json()["data"]
        self.assertEqual(payload["access"], "workos_access_refreshed")
        self.assertEqual(payload["refresh"], "workos_refresh_rotated")

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
        mock_client_class.return_value.get_logout_url.assert_called_once_with(
            session_id="session_123",
            return_to="http://testserver/login",
        )

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

    @patch("api_auth.views.WorkOSAuthClient")
    def test_workos_profile_can_be_loaded_and_updated(self, mock_client_class):
        user = Users.objects.create_user(
            username="managed@datamingle.dev",
            email="managed@datamingle.dev",
            display="Managed User",
            avatar_url="",
            workos_user_id="user_123",
            is_active=True,
        )
        mock_client = mock_client_class.return_value
        mock_client.get_user.return_value = SimpleNamespace(
            id="user_123",
            email="managed@datamingle.dev",
            first_name="Managed",
            last_name="User",
            profile_picture_url="https://images.workos.dev/avatar.png",
        )
        mock_client.update_user_profile.return_value = SimpleNamespace(
            id="user_123",
            email="managed@datamingle.dev",
            first_name="Updated",
            last_name="User",
            profile_picture_url="https://images.workos.dev/avatar.png",
        )
        self.client.force_authenticate(user=user)

        get_response = self.client.get("/api/auth/workos/profile/", format="json")

        self.assertEqual(get_response.status_code, status.HTTP_200_OK)
        self.assertEqual(get_response.json()["data"]["display_name"], "Managed User")

        patch_response = self.client.patch(
            "/api/auth/workos/profile/",
            {"first_name": "Updated", "last_name": "User"},
            format="json",
        )

        self.assertEqual(patch_response.status_code, status.HTTP_200_OK)
        self.assertEqual(patch_response.json()["data"]["display_name"], "Updated User")
        mock_client.update_user_profile.assert_called_once_with(
            user_id="user_123",
            first_name="Updated",
            last_name="User",
        )
        user.refresh_from_db()
        self.assertEqual(user.display, "Updated User")
        self.assertEqual(user.avatar_url, "https://images.workos.dev/avatar.png")

    @patch("api_auth.views.WorkOSAuthClient")
    def test_workos_sessions_can_be_listed_and_revoked(self, mock_client_class):
        user = Users.objects.create_user(
            username="managed@datamingle.dev",
            email="managed@datamingle.dev",
            display="Managed User",
            workos_user_id="user_123",
            is_active=True,
        )
        mock_client = mock_client_class.return_value
        mock_client.list_sessions.return_value = SimpleNamespace(
            data=[
                SimpleNamespace(
                    id="session_current",
                    status="active",
                    auth_method="sso",
                    ip_address="127.0.0.1",
                    user_agent="Current Browser",
                    expires_at="2026-05-18T12:00:00Z",
                    ended_at=None,
                    created_at="2026-05-17T12:00:00Z",
                    updated_at="2026-05-17T12:00:00Z",
                ),
                SimpleNamespace(
                    id="session_other",
                    status="active",
                    auth_method="sso",
                    ip_address="192.0.2.1",
                    user_agent="Other Browser",
                    expires_at="2026-05-18T12:00:00Z",
                    ended_at=None,
                    created_at="2026-05-17T12:00:00Z",
                    updated_at="2026-05-17T12:00:00Z",
                ),
            ]
        )
        self.client.force_authenticate(user=user)
        self.client.cookies["datamingle_workos_session_id"] = "session_current"

        list_response = self.client.get("/api/auth/workos/sessions/", format="json")

        self.assertEqual(list_response.status_code, status.HTTP_200_OK)
        sessions = list_response.json()["data"]
        self.assertTrue(sessions[0]["is_current"])
        self.assertFalse(sessions[1]["is_current"])

        revoke_response = self.client.post(
            "/api/auth/workos/sessions/session_other/revoke/",
            {},
            format="json",
        )

        self.assertEqual(revoke_response.status_code, status.HTTP_200_OK)
        mock_client.revoke_session.assert_called_once_with(session_id="session_other")

        current_response = self.client.post(
            "/api/auth/workos/sessions/session_current/revoke/",
            {},
            format="json",
        )

        self.assertEqual(current_response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_superuser_can_change_status_but_identity_and_groups_are_ignored(self):
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
        self.client.force_authenticate(user=superuser)

        update_response = self.client.put(
            f"/api/v1/user/{workos_user.id}/",
            {
                "display": "Updated Name",
                "email": "updated@datamingle.dev",
                "is_active": True,
            },
            format="json",
        )
        self.assertEqual(update_response.status_code, status.HTTP_200_OK)
        workos_user.refresh_from_db()
        self.assertEqual(workos_user.display, "Managed User")
        self.assertEqual(workos_user.email, "managed@datamingle.dev")
        self.assertFalse(workos_user.groups.exists())

        allow_response = self.client.put(
            f"/api/v1/user/{workos_user.id}/",
            {
                "is_active": False,
            },
            format="json",
        )
        self.assertEqual(allow_response.status_code, status.HTTP_200_OK)
        workos_user.refresh_from_db()
        self.assertFalse(workos_user.is_active)
        self.assertFalse(workos_user.groups.exists())

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

    @patch("api_auth.views.get_redis_connection")
    @patch("api_auth.views.WorkOSAuthClient")
    def test_callback_rejects_missing_organization(
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
            organization_id="",
            session_id="session_123",
            display_name="Staff User",
        )

        self.client.cookies["datamingle_workos_state"] = "state_123"
        response = self.client.get(
            "/api/auth/workos/callback/",
            {"code": "code_123", "state": "state_123"},
        )

        self.assertEqual(response.status_code, status.HTTP_302_FOUND)
        self.assertIn("unexpected+organization", response.url)

    @patch("api_users.views.WorkOSAuthClient")
    def test_superuser_can_invite_workos_user_and_create_local_record(
        self, mock_client_class
    ):
        superuser = Users.objects.create_user(
            username="superuser@datamingle.dev",
            email="superuser@datamingle.dev",
            display="Super User",
            is_active=True,
            is_superuser=True,
            is_staff=True,
            workos_user_id="user_super",
        )
        mock_client_class.return_value.send_invitation.return_value = SimpleNamespace(
            id="invitation_123",
            email="new.user@datamingle.dev",
            state="pending",
            organization_id="org_test_123",
            expires_at="2026-05-24T12:00:00Z",
        )
        self.client.force_authenticate(user=superuser)

        response = self.client.post(
            "/api/v1/user/invitations/",
            {
                "email": "New.User@DataMingle.dev",
                "display": "New User",
            },
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        payload = response.json()["data"]
        self.assertEqual(payload["invitation"]["id"], "invitation_123")
        self.assertEqual(payload["user"]["email"], "new.user@datamingle.dev")
        mock_client_class.return_value.send_invitation.assert_called_once_with(
            email="new.user@datamingle.dev",
            inviter_user_id="user_super",
        )

        invited_user = Users.objects.get(email="new.user@datamingle.dev")
        self.assertEqual(invited_user.username, "new.user@datamingle.dev")
        self.assertEqual(invited_user.display, "New User")
        self.assertFalse(bool(invited_user.workos_user_id))
        self.assertFalse(invited_user.groups.exists())
        self.assertFalse(invited_user.team_memberships.exists())
