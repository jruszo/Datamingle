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
from common.authenticate.workos_directory import process_directory_event
from sql.models import (
    ResourceGroup,
    ResourceAccessRole,
    ResourceGroupMembership,
    ResourceGroupMembershipSource,
    Users,
    WorkOSDirectoryGroup,
    WorkOSDirectoryGroupMembership,
    WorkOSDirectorySyncEvent,
)
from sql.utils.resource_group import sync_user_legacy_resource_groups


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

    def test_directory_list_methods_follow_workos_pagination(self):
        client = WorkOSAuthClient.__new__(WorkOSAuthClient)
        directory_sync = SimpleNamespace()
        page_one_user = SimpleNamespace(id="directory_user_1")
        page_two_user = SimpleNamespace(id="directory_user_2")
        pages = [
            SimpleNamespace(
                data=[page_one_user],
                list_metadata=SimpleNamespace(after="directory_user_1"),
            ),
            SimpleNamespace(
                data=[page_two_user],
                list_metadata=SimpleNamespace(after=None),
            ),
        ]
        calls = []

        def list_users(**kwargs):
            calls.append(kwargs)
            return pages.pop(0)

        directory_sync.list_users = list_users
        client.client = SimpleNamespace(directory_sync=directory_sync)

        users = client.list_directory_users(directory_id="directory_123")

        self.assertEqual(users, [page_one_user, page_two_user])
        self.assertEqual(
            calls,
            [
                {"directory_id": "directory_123", "limit": 100, "order": "asc"},
                {
                    "directory_id": "directory_123",
                    "limit": 100,
                    "order": "asc",
                    "after": "directory_user_1",
                },
            ],
        )

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
        self.auth_group = Group.objects.create(name="DBA")
        self.resource_group = ResourceGroup.objects.create(group_name="Primary Team")
        self.sys_config = SysConfig()
        self.sys_config.set("default_auth_group", self.auth_group.name)

    def tearDown(self):
        self.sys_config.purge()
        WorkOSDirectoryGroupMembership.objects.all().delete()
        WorkOSDirectoryGroup.objects.all().delete()
        WorkOSDirectorySyncEvent.objects.all().delete()
        Users.objects.all().delete()
        ResourceGroup.objects.all().delete()
        Group.objects.all().delete()

    def _directory_user_payload(self, **overrides):
        payload = {
            "id": "directory_user_123",
            "directory_id": "directory_123",
            "organization_id": "org_test_123",
            "email": "directory.user@datamingle.dev",
            "first_name": "Directory",
            "last_name": "User",
            "state": "active",
            "updated_at": "2026-05-18T12:00:00Z",
        }
        payload.update(overrides)
        return payload

    def _directory_group_payload(self, **overrides):
        payload = {
            "id": "directory_group_123",
            "directory_id": "directory_123",
            "organization_id": "org_test_123",
            "idp_id": "idp_group_123",
            "name": "Developers",
            "updated_at": "2026-05-18T12:00:00Z",
        }
        payload.update(overrides)
        return payload

    def _post_workos_directory_event(self, event_type, data, event_id="event_123"):
        with patch(
            "api_auth.views.process_workos_webhook_task.delay",
            side_effect=process_directory_event,
        ):
            return self.client.post(
                "/api/auth/workos/webhook/",
                {"id": event_id, "event": event_type, "data": data},
                format="json",
            )

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
            {self.auth_group.name, SUPERADMIN_GROUP_NAME},
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

    def test_workos_directory_group_membership_webhook_creates_resource_group(self):
        response = self._post_workos_directory_event(
            "dsync.group.user_added",
            {
                "directory_id": "directory_123",
                "user": self._directory_user_payload(),
                "group": self._directory_group_payload(),
            },
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        payload = response.json()["data"]
        self.assertTrue(payload["processed"])

        user = Users.objects.get(email="directory.user@datamingle.dev")
        self.assertTrue(user.workos_directory_managed)
        self.assertEqual(user.workos_directory_user_id, "directory_user_123")
        self.assertEqual(user.workos_directory_id, "directory_123")
        self.assertEqual(
            list(user.resource_group.values_list("group_name", flat=True)),
            ["Developers"],
        )

        self.assertFalse(Group.objects.filter(name="Developers").exists())
        resource_group = ResourceGroup.objects.get(group_name="Developers")
        mapping = WorkOSDirectoryGroup.objects.get(
            workos_group_id="directory_group_123"
        )
        self.assertEqual(mapping.resource_group, resource_group)
        self.assertTrue(
            WorkOSDirectoryGroupMembership.objects.filter(
                user=user, directory_group=mapping
            ).exists()
        )

        duplicate_response = self._post_workos_directory_event(
            "dsync.group.user_added",
            {
                "directory_id": "directory_123",
                "user": self._directory_user_payload(),
                "group": self._directory_group_payload(),
            },
        )

        self.assertEqual(duplicate_response.status_code, status.HTTP_200_OK)
        self.assertTrue(duplicate_response.json()["data"]["duplicate"])
        self.assertEqual(WorkOSDirectorySyncEvent.objects.count(), 1)

    def test_workos_directory_group_removed_webhook_clears_membership(self):
        self._post_workos_directory_event(
            "dsync.group.user_added",
            {
                "directory_id": "directory_123",
                "user": self._directory_user_payload(),
                "group": self._directory_group_payload(),
            },
            event_id="event_added",
        )

        response = self._post_workos_directory_event(
            "dsync.group.user_removed",
            {
                "directory_id": "directory_123",
                "user": self._directory_user_payload(),
                "group": self._directory_group_payload(),
            },
            event_id="event_removed",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        user = Users.objects.get(email="directory.user@datamingle.dev")
        self.assertEqual(
            list(user.resource_group.values_list("group_name", flat=True)), []
        )
        self.assertFalse(
            WorkOSDirectoryGroupMembership.objects.filter(user=user).exists()
        )

    def test_workos_directory_group_removed_preserves_direct_resource_group(self):
        self._post_workos_directory_event(
            "dsync.group.user_added",
            {
                "directory_id": "directory_123",
                "user": self._directory_user_payload(),
                "group": self._directory_group_payload(),
            },
            event_id="event_added",
        )
        user = Users.objects.get(email="directory.user@datamingle.dev")
        direct_resource_group = ResourceGroup.objects.create(group_name="Direct Grant")
        ResourceGroupMembership.objects.create(
            user=user,
            resource_group=direct_resource_group,
            access_role=ResourceAccessRole.QUERY,
            membership_source=ResourceGroupMembershipSource.DATAMINGLE,
        )
        sync_user_legacy_resource_groups(user)

        response = self._post_workos_directory_event(
            "dsync.group.user_removed",
            {
                "directory_id": "directory_123",
                "user": self._directory_user_payload(),
                "group": self._directory_group_payload(),
            },
            event_id="event_removed",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        user.refresh_from_db()
        self.assertEqual(
            list(
                user.resource_group.order_by("group_name").values_list(
                    "group_name", flat=True
                )
            ),
            ["Direct Grant"],
        )

    def test_directory_managed_user_rejects_manual_resource_group_updates(self):
        superuser = Users.objects.create_user(
            username="superuser@datamingle.dev",
            email="superuser@datamingle.dev",
            display="Super User",
            is_active=True,
            is_superuser=True,
            is_staff=True,
        )
        directory_user = Users.objects.create_user(
            username="directory.user@datamingle.dev",
            email="directory.user@datamingle.dev",
            display="Directory User",
            is_active=True,
            workos_directory_user_id="directory_user_123",
            workos_directory_id="directory_123",
            workos_directory_managed=True,
        )
        directory_user.groups.add(self.auth_group)
        directory_user.resource_group.add(self.resource_group)
        new_resource_group = ResourceGroup.objects.create(group_name="Ops")
        self.client.force_authenticate(user=superuser)

        group_response = self.client.put(
            f"/api/v1/user/{directory_user.id}/",
            {"resource_group_ids": [new_resource_group.group_id], "is_active": True},
            format="json",
        )

        self.assertEqual(group_response.status_code, status.HTTP_400_BAD_REQUEST)
        directory_user.refresh_from_db()
        self.assertEqual(
            list(directory_user.resource_group.values_list("group_name", flat=True)),
            [self.resource_group.group_name],
        )

        status_response = self.client.put(
            f"/api/v1/user/{directory_user.id}/",
            {"is_active": False},
            format="json",
        )

        self.assertEqual(status_response.status_code, status.HTTP_200_OK)
        directory_user.refresh_from_db()
        self.assertFalse(directory_user.is_active)

    def test_workos_webhook_rejects_stale_event_timestamps(self):
        stale_timestamp = (
            datetime.now(datetime_timezone.utc) - timedelta(seconds=301)
        ).isoformat()

        with patch("api_auth.views.process_workos_webhook_task.delay") as task_delay:
            response = self.client.post(
                "/api/auth/workos/webhook/",
                {
                    "id": "event_stale",
                    "event": "dsync.group.created",
                    "created_at": stale_timestamp,
                    "data": self._directory_group_payload(),
                },
                format="json",
            )

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.json()["errors"], "WorkOS webhook timestamp is too old."
        )
        task_delay.assert_not_called()

    def test_soft_deleting_resource_group_deletes_workos_mapping(self):
        resource_group = ResourceGroup.objects.create(group_name="Directory Team")
        mapping = WorkOSDirectoryGroup.objects.create(
            workos_group_id="directory_group_soft_delete",
            directory_id="directory_123",
            organization_id="org_test_123",
            idp_id="idp_group_soft_delete",
            name="Directory Team",
            resource_group=resource_group,
        )

        resource_group.is_deleted = 1
        resource_group.save(update_fields=["is_deleted"])

        mapping.refresh_from_db()
        self.assertTrue(mapping.is_deleted)

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

    def test_superuser_can_change_groups_but_identity_fields_are_ignored(self):
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

        update_response = self.client.put(
            f"/api/v1/user/{workos_user.id}/",
            {
                "display": "Updated Name",
                "email": "updated@datamingle.dev",
                "group_ids": [new_group.id],
                "is_active": True,
            },
            format="json",
        )
        self.assertEqual(update_response.status_code, status.HTTP_200_OK)
        workos_user.refresh_from_db()
        self.assertEqual(workos_user.display, "Managed User")
        self.assertEqual(workos_user.email, "managed@datamingle.dev")
        self.assertEqual(
            list(workos_user.groups.values_list("name", flat=True)),
            ["Ops"],
        )

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
                "group_ids": [self.auth_group.id],
                "resource_group_ids": [self.resource_group.group_id],
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
        self.assertEqual(
            list(invited_user.groups.values_list("id", flat=True)),
            [self.auth_group.id],
        )
        self.assertEqual(
            list(invited_user.resource_group.values_list("group_id", flat=True)),
            [self.resource_group.group_id],
        )
