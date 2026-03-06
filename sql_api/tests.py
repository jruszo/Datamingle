from datetime import datetime, timedelta
from unittest.mock import patch, Mock

from django.test import TestCase
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group, Permission
from rest_framework.test import APITestCase
from rest_framework import status
from common.config import SysConfig
from sql.utils.workflow_audit import AuditSetting
from sql.engines import ReviewSet
from sql.engines.models import ReviewResult, ResultSet
from sql.models import (
    ResourceGroup,
    Instance,
    AliyunRdsConfig,
    CloudAccessKey,
    Tunnel,
    SqlWorkflow,
    SqlWorkflowContent,
    WorkflowAudit,
    WorkflowLog,
    InstanceTag,
    WorkflowAuditSetting,
    TwoFactorAuthConfig,
    QueryLog,
    QueryPrivileges,
    QueryPrivilegesApply,
)
from common.utils.const import WorkflowAction, WorkflowStatus
import json
import pyotp

User = get_user_model()


def response_data(response):
    payload = response.json()
    return payload.get("data", payload)


def assert_success_envelope(testcase, response):
    payload = response.json()
    testcase.assertIn("detail", payload)
    testcase.assertIn("data", payload)
    return payload["data"]


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


class TestUser(APITestCase):
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
        self.group = Group.objects.create(id=1, name="DBA")
        self.res_group = ResourceGroup.objects.create(group_id=1, group_name="test")
        r = self.client.post(
            "/api/auth/token/",
            {"username": "test_user", "password": "test_password"},
            format="json",
        )
        self.token = response_data(r)["access"]
        self.client.credentials(HTTP_AUTHORIZATION="Bearer " + self.token)

    def tearDown(self):
        self.user.delete()
        self.group.delete()
        self.res_group.delete()
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
        self.assertEqual(response_data(r)["count"], 1)

    def test_get_current_user_context(self):
        """Test SPA bootstrap current-user endpoint."""
        r = self.client.get("/api/v1/me/", format="json")
        r_data = response_data(r)
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(r_data["username"], self.user.username)
        self.assertIn("permissions", r_data)
        self.assertIn("groups", r_data)
        self.assertIn("resource_groups", r_data)

    def test_update_current_user_profile(self):
        """Authenticated users can update their own display name."""
        r = self.client.patch("/api/v1/me/", {"display": "Updated Self"}, format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.user.refresh_from_db()
        self.assertEqual(self.user.display, "Updated Self")
        self.assertEqual(assert_success_envelope(self, r)["display"], "Updated Self")

    def test_change_current_user_password(self):
        """Authenticated users can change their own password."""
        new_password = "StrongerPass123!"
        r = self.client.post(
            "/api/v1/me/password/",
            {
                "current_password": "test_password",
                "new_password": new_password,
                "new_password_confirm": new_password,
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.user.refresh_from_db()
        self.assertTrue(self.user.check_password(new_password))

        self.client.credentials()
        login_response = self.client.post(
            "/api/auth/token/",
            {"username": self.user.username, "password": new_password},
            format="json",
        )
        self.assertEqual(login_response.status_code, status.HTTP_200_OK)
        self.assertIn("access", response_data(login_response))

    def test_change_current_user_password_rejects_wrong_current_password(self):
        """Password change requires the existing password."""
        r = self.client.post(
            "/api/v1/me/password/",
            {
                "current_password": "wrong-password",
                "new_password": "StrongerPass123!",
                "new_password_confirm": "StrongerPass123!",
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("current_password", r.json())

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

    def test_get_user_list_with_delegated_permission(self):
        """Non-superuser can access with explicit delegated permission."""
        User.objects.filter(id=self.user.id).update(is_superuser=0)
        self.user = User.objects.get(id=self.user.id)
        self.user.user_permissions.clear()

        r1 = self.client.get("/api/v1/user/", format="json")
        self.assertEqual(r1.status_code, status.HTTP_403_FORBIDDEN)

        delegated_permission = Permission.objects.get(codename="view_users")
        self.user.user_permissions.add(delegated_permission)
        r2 = self.client.get("/api/v1/user/", format="json")
        self.assertEqual(r2.status_code, status.HTTP_200_OK)

    def test_create_user(self):
        """Test creating user."""
        json_data = {
            "username": "test_user2",
            "password": "test_password2",
            "display": "Test User 2",
        }
        r = self.client.post("/api/v1/user/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response_data(r)["username"], "test_user2")

    def test_update_user(self):
        """Test updating user."""
        json_data = {"display": "Updated Display Name"}
        r = self.client.put(f"/api/v1/user/{self.user.id}/", json_data, format="json")
        user = User.objects.get(pk=self.user.id)
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(user.display, "Updated Display Name")

    def test_delete_user(self):
        """Test deleting user."""
        json_data = {
            "username": "test_user2",
            "password": "test_password2",
            "display": "Test User 2",
        }
        r1 = self.client.post("/api/v1/user/", json_data, format="json")
        r2 = self.client.delete(
            f'/api/v1/user/{response_data(r1)["id"]}/', format="json"
        )
        self.assertEqual(r2.status_code, status.HTTP_200_OK)
        self.assertEqual(User.objects.filter(username="test_user2").count(), 0)

    def test_get_user_group_list(self):
        """Test getting user group list."""
        r = self.client.get("/api/v1/user/group/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(response_data(r)["count"], 1)

    def test_create_user_group(self):
        """Test creating user group."""
        json_data = {"name": "RD"}
        r = self.client.post("/api/v1/user/group/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response_data(r)["name"], "RD")

    def test_update_user_group(self):
        """Test updating user group."""
        json_data = {"name": "Updated Group Name"}
        r = self.client.put(
            f"/api/v1/user/group/{self.group.id}/", json_data, format="json"
        )
        group = Group.objects.get(pk=self.group.id)
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(group.name, "Updated Group Name")

    def test_delete_user_group(self):
        """Test deleting user group."""
        r = self.client.delete(f"/api/v1/user/group/{self.group.id}/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(Group.objects.filter(name="DBA").count(), 0)

    def test_get_resource_group_list(self):
        """Test getting resource group list."""
        r = self.client.get("/api/v1/user/resourcegroup/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(response_data(r)["count"], 1)

    def test_create_resource_group(self):
        """Test creating resource group."""
        json_data = {
            "group_name": "prod",
            "ding_webhook": "https://oapi.dingtalk.com/robot/send?access_token=123",
        }
        r = self.client.post("/api/v1/user/resourcegroup/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response_data(r)["group_name"], "prod")

    def test_update_resource_group(self):
        """Test updating resource group."""
        json_data = {"group_name": "Updated Resource Group Name"}
        r = self.client.put(
            f"/api/v1/user/resourcegroup/{self.res_group.group_id}/",
            json_data,
            format="json",
        )
        group = ResourceGroup.objects.get(pk=self.res_group.group_id)
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(group.group_name, "Updated Resource Group Name")

    def test_delete_resource_group(self):
        """Test deleting resource group."""
        r = self.client.delete(
            f"/api/v1/user/resourcegroup/{self.res_group.group_id}/", format="json"
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(Group.objects.filter(name="test").count(), 0)

    def test_user_auth(self):
        """Test user authentication check."""
        json_data = {"password": "test_password"}
        r = self.client.post(f"/api/v1/user/auth/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(r.json()["detail"], "Authentication successful.")

    def test_2fa_config(self):
        """Test user 2FA configuration."""
        json_data = {"auth_type": "totp", "enable": "false"}
        r = self.client.post(f"/api/v1/user/2fa/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(TwoFactorAuthConfig.objects.count(), 0)

    def test_2fa_save(self):
        """Test saving user 2FA configuration."""
        json_data = {
            "auth_type": "totp",
            "key": "ZUGRIJZP6H7LIOAL4LH5JA4GSXXT3WOK",
        }
        r = self.client.post(f"/api/v1/user/2fa/save/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(TwoFactorAuthConfig.objects.count(), 1)

    def test_2fa_state(self):
        """Test querying user 2FA status."""
        r = self.client.get(f"/api/v1/user/2fa/state/", format="json")
        r_data = response_data(r)
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(r_data["totp"], "disabled")
        self.assertEqual(r_data["sms"], "disabled")

    def test_2fa_verify(self):
        """Test 2FA code verification."""
        json_data = {
            "otp": 123456,
            "key": "ZUGRIJZP6H7LIOAL4LH5JA4GSXXT3WOK",
            "auth_type": "totp",
        }
        r = self.client.post(f"/api/v1/user/2fa/verify/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(r.json()["errors"], "Invalid verification code.")


class TestTokenAuth2FA(APITestCase):
    """Test token auth with stateless 2FA checks."""

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
        TwoFactorAuthConfig.objects.all().delete()
        SysConfig().purge()

    def test_token_requires_2fa_when_user_has_totp(self):
        secret = pyotp.random_base32(32)
        TwoFactorAuthConfig.objects.create(
            username=self.user.username,
            auth_type="totp",
            secret_key=secret,
            user=self.user,
        )
        r = self.client.post(
            "/api/auth/token/",
            {"username": self.user.username, "password": "test_password"},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(r.json()["code"][0], "2fa_required")
        self.assertEqual(r.json()["available_auth_types"], ["totp"])

    def test_token_totp_success(self):
        secret = pyotp.random_base32(32)
        TwoFactorAuthConfig.objects.create(
            username=self.user.username,
            auth_type="totp",
            secret_key=secret,
            user=self.user,
        )
        otp = pyotp.TOTP(secret).now()
        r = self.client.post(
            "/api/auth/token/",
            {
                "username": self.user.username,
                "password": "test_password",
                "auth_type": "totp",
                "otp": otp,
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertIn("access", response_data(r))
        self.assertIn("refresh", response_data(r))

    def test_token_refresh_success_envelope(self):
        login_resp = self.client.post(
            "/api/auth/token/",
            {"username": self.user.username, "password": "test_password"},
            format="json",
        )
        self.assertEqual(login_resp.status_code, status.HTTP_200_OK)
        refresh_token = response_data(login_resp)["refresh"]

        r = self.client.post(
            "/api/auth/token/refresh/",
            {"refresh": refresh_token},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        refreshed = assert_success_envelope(self, r)
        self.assertIn("access", refreshed)

    def test_token_verify_success_envelope(self):
        login_resp = self.client.post(
            "/api/auth/token/",
            {"username": self.user.username, "password": "test_password"},
            format="json",
        )
        self.assertEqual(login_resp.status_code, status.HTTP_200_OK)
        access_token = response_data(login_resp)["access"]

        r = self.client.post(
            "/api/auth/token/verify/",
            {"token": access_token},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(assert_success_envelope(self, r), {})

    def test_token_refresh_invalid_token_returns_error_contract(self):
        r = self.client.post(
            "/api/auth/token/refresh/",
            {"refresh": "invalid.refresh.token"},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertIn("detail", r.json())
        self.assertIn("code", r.json())
        self.assertNotIn("data", r.json())

    def test_token_verify_invalid_token_returns_error_contract(self):
        r = self.client.post(
            "/api/auth/token/verify/",
            {"token": "invalid.access.token"},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_401_UNAUTHORIZED)
        self.assertIn("detail", r.json())
        self.assertIn("code", r.json())
        self.assertNotIn("data", r.json())

    def test_token_enforce_2fa_requires_setup(self):
        SysConfig().set("enforce_2fa", True)
        r = self.client.post(
            "/api/auth/token/",
            {"username": self.user.username, "password": "test_password"},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(r.json()["code"][0], "2fa_setup_required")

    def test_request_sms_login_otp_requires_sms_config(self):
        r = self.client.post(
            "/api/auth/token/sms/",
            {"username": self.user.username, "password": "test_password"},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            r.json()["errors"], "SMS 2FA is not configured for this account."
        )

    @patch("sql_api.api_auth.get_redis_connection")
    @patch("sql_api.api_auth.get_authenticator")
    def test_request_sms_login_otp_success(
        self, mock_get_authenticator, mock_get_redis
    ):
        TwoFactorAuthConfig.objects.create(
            username=self.user.username,
            auth_type="sms",
            phone="13800138000",
            user=self.user,
        )
        mock_auth = Mock()
        mock_auth.get_captcha.return_value = {"status": 0, "msg": "ok"}
        mock_get_authenticator.return_value = mock_auth
        mock_redis = Mock()
        mock_get_redis.return_value = mock_redis

        r = self.client.post(
            "/api/auth/token/sms/",
            {"username": self.user.username, "password": "test_password"},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(r.json()["detail"], "ok")
        mock_redis.set.assert_called_once()


class TestQueryAPI(APITestCase):
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
        self.res_group = ResourceGroup.objects.create(group_name="query_rg")
        self.user.resource_group.add(self.res_group)

        self.read_tag = InstanceTag.objects.create(
            tag_code="can_read", tag_name="Can Read", active=1
        )
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
        self.ins.instance_tag.add(self.read_tag)

        r = self.client.post(
            "/api/auth/token/",
            {"username": "query_user", "password": "test_password"},
            format="json",
        )
        self.token = response_data(r)["access"]
        self.client.credentials(HTTP_AUTHORIZATION="Bearer " + self.token)

    def tearDown(self):
        self.user.delete()
        QueryLog.objects.all().delete()
        QueryPrivileges.objects.all().delete()
        QueryPrivilegesApply.objects.all().delete()
        InstanceTag.objects.all().delete()
        Instance.objects.all().delete()
        ResourceGroup.objects.all().delete()
        Group.objects.all().delete()
        SysConfig().purge()

    @patch("sql_api.api_query.query_priv_check")
    @patch("sql_api.api_query.get_engine")
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
        other_tag = InstanceTag.objects.create(
            tag_code="can_write", tag_name="Can Write", active=1
        )
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
        hidden_instance.instance_tag.add(other_tag)

        r = self.client.get("/api/v1/query/instance/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        data = assert_success_envelope(self, r)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["instance_name"], self.ins.instance_name)

    @patch("sql_api.api_query.get_engine")
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
        other_group = ResourceGroup.objects.create(group_name="other_rg")
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
        other_instance.instance_tag.add(self.read_tag)

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

    @patch("sql_api.api_query.async_task")
    @patch("sql_api.api_query._query_apply_audit_call_back")
    @patch("sql_api.api_query.get_auditor")
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
                "group_name": self.res_group.group_name,
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
            group_id=self.res_group.group_id,
            group_name=self.res_group.group_name,
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
            group_id=self.res_group.group_id,
            group_name=self.res_group.group_name,
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

    @patch("sql_api.api_query.async_task")
    @patch("sql_api.api_query._query_apply_audit_call_back")
    @patch("sql_api.api_query.get_auditor")
    def test_query_privilege_audit(
        self, mock_get_auditor, mock_callback, mock_async_task
    ):
        apply_obj = QueryPrivilegesApply.objects.create(
            group_id=self.res_group.group_id,
            group_name=self.res_group.group_name,
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


class TestInstance(APITestCase):
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
        self.ak = CloudAccessKey.objects.create(
            type="aliyun", key_id="abc", key_secret="abc"
        )
        self.rds = AliyunRdsConfig.objects.create(
            rds_dbinstanceid="abc", ak_id=self.ak.id, instance=self.ins
        )
        self.tunnel = Tunnel.objects.create(
            tunnel_name="one_tunnel", host="one_host", port=22
        )
        r = self.client.post(
            "/api/auth/token/",
            {"username": "test_user", "password": "test_password"},
            format="json",
        )
        self.token = response_data(r)["access"]
        self.client.credentials(HTTP_AUTHORIZATION="Bearer " + self.token)

    def tearDown(self):
        self.user.delete()
        Instance.objects.all().delete()
        AliyunRdsConfig.objects.all().delete()
        CloudAccessKey.objects.all().delete()
        Tunnel.objects.all().delete()
        SysConfig().purge()

    def test_get_instance_list(self):
        """Test getting instance list."""
        r = self.client.get("/api/v1/instance/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(response_data(r)["count"], 1)

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

    def test_update_instance(self):
        """Test updating instance."""
        json_data = {"instance_name": "Updated Instance Name"}
        r = self.client.put(
            f"/api/v1/instance/{self.ins.id}/", json_data, format="json"
        )
        ins = Instance.objects.get(pk=self.ins.id)
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(ins.instance_name, "Updated Instance Name")

    def test_delete_instance(self):
        """Test deleting instance."""
        r = self.client.delete(f"/api/v1/instance/{self.ins.id}/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(Instance.objects.filter(instance_name="some_ins").count(), 0)

    def test_get_aliyunrds_list(self):
        """Test getting Aliyun RDS list."""
        r = self.client.get("/api/v1/instance/rds/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(response_data(r)["count"], 1)

    def test_create_aliyunrds(self):
        """Test creating Aliyun RDS config."""
        ins = Instance.objects.create(
            instance_name="another_ins",
            type="slave",
            db_type="mysql",
            host="another_host",
            port=3306,
        )
        json_data = {
            "rds_dbinstanceid": "bbc",
            "is_enable": True,
            "instance": ins.id,
            "ak": {
                "type": "aliyun",
                "key_id": "bbc",
                "key_secret": "bbc",
                "remark": "bbc",
            },
        }
        r = self.client.post("/api/v1/instance/rds/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response_data(r)["rds_dbinstanceid"], "bbc")

    def test_get_tunnel_list(self):
        """Test getting tunnel list."""
        r = self.client.get("/api/v1/instance/tunnel/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(response_data(r)["count"], 1)

    def test_create_tunnel(self):
        """Test creating tunnel."""
        json_data = {"tunnel_name": "tunnel_test", "host": "one_host", "port": 22}
        r = self.client.post("/api/v1/instance/tunnel/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response_data(r)["tunnel_name"], "tunnel_test")

    @patch("sql_api.api_instance.get_engine")
    def test_get_instance_resource(self, mock_get_engine):
        """Test querying instance resources."""
        group = ResourceGroup.objects.create(group_name="instance_resource_test")
        self.user.resource_group.add(group)
        self.ins.resource_group.add(group)

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
            {"instance_id": self.ins.id, "resource_type": "database"},
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(response_data(r)["count"], 1)


class TestWorkflow(APITestCase):
    """Test workflow-related APIs."""

    def setUp(self):
        self.now = datetime.now()
        self.group = Group.objects.create(id=1, name="DBA")
        self.res_group = ResourceGroup.objects.create(group_id=1, group_name="test")
        self.ins_tag = InstanceTag.objects.create(tag_code="can_write", active=1)
        self.wfs = WorkflowAuditSetting.objects.create(
            group_id=self.res_group.group_id,
            workflow_type=2,
            audit_auth_groups=self.group.id,
        )
        can_submit = Permission.objects.get(codename="sql_submit")
        can_execute_permission = Permission.objects.get(codename="sql_execute")
        can_execute_resource_permission = Permission.objects.get(
            codename="sql_execute_for_resource_group"
        )
        can_review_permission = Permission.objects.get(codename="sql_review")
        menu_sqlworkflow_permission = Permission.objects.get(
            codename="menu_sqlworkflow"
        )
        self.user = User(username="test_user", display="Test User", is_active=True)
        self.user.set_password("test_password")
        self.user.save()
        self.user.user_permissions.add(
            can_submit,
            can_execute_permission,
            can_execute_resource_permission,
            can_review_permission,
            menu_sqlworkflow_permission,
        )
        self.user.groups.add(self.group.id)
        self.user.resource_group.add(self.res_group.group_id)
        self.ins = Instance.objects.create(
            instance_name="some_ins",
            type="slave",
            db_type="redis",
            host="some_host",
            port=6379,
            user="ins_user",
            password="some_str",
        )
        self.ins.resource_group.add(self.res_group.group_id)
        self.ins.instance_tag.add(self.ins_tag.id)
        self.wf1 = SqlWorkflow.objects.create(
            workflow_name="some_name",
            group_id=1,
            group_name="g1",
            engineer=self.user.username,
            engineer_display=self.user.display,
            audit_auth_groups="1",
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
            group_id=1,
            group_name="some_group",
            workflow_id=self.wf1.id,
            workflow_type=2,
            workflow_title="Apply Title",
            workflow_remark="Apply Remark",
            audit_auth_groups="1",
            current_audit="1",
            next_audit="-1",
            current_status=0,
            create_user=self.user.username,
            create_user_display=self.user.display,
        )
        self.wl = WorkflowLog.objects.create(
            audit_id=self.audit1.audit_id, operation_type=1
        )
        r = self.client.post(
            "/api/auth/token/",
            {"username": "test_user", "password": "test_password"},
            format="json",
        )
        self.token = response_data(r)["access"]
        self.client.credentials(HTTP_AUTHORIZATION="Bearer " + self.token)
        self.notify_patcher = patch("sql.notify.auto_notify")
        self.notify_patcher.start()

    def tearDown(self):
        self.user.delete()
        self.group.delete()
        self.res_group.delete()
        SqlWorkflowContent.objects.all().delete()
        SqlWorkflow.objects.all().delete()
        WorkflowAudit.objects.all().delete()
        WorkflowLog.objects.all().delete()
        self.notify_patcher.stop()

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

    @patch("sql_api.api_workflow.get_engine")
    def test_check_inception_Exception(self, _get_engine):
        """Test workflow SQL check when inception raises an error."""
        json_data = {
            "full_sql": "use mysql",
            "db_name": "test_db",
            "instance_id": self.ins.id,
        }
        _get_engine.side_effect = RuntimeError("RuntimeError")
        r = self.client.post("/api/v1/workflow/sqlcheck/", json_data, format="json")
        print(json.loads(r.content))
        self.assertDictEqual(json.loads(r.content), {"errors": "RuntimeError"})

    @patch("sql_api.api_workflow.get_engine")
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

    @patch("sql_api.api_workflow.get_engine")
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

    def test_submit_workflow(self):
        """Test submitting SQL release workflow."""
        json_data = {
            "workflow": {
                "workflow_name": "Release Workflow 1",
                "demand_url": "test",
                "group_id": 1,
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

    def test_submit_workflow_super(self):
        """Test admin submitting SQL release workflow with specified user."""
        User.objects.filter(id=self.user.id).update(is_superuser=1)
        user2 = User.objects.create(
            username="test_user2", display="Test User 2", is_active=True
        )
        user2.groups.add(self.group.id)
        user2.resource_group.add(self.res_group.group_id)
        json_data = {
            "workflow": {
                "workflow_name": "Release Workflow 1",
                "demand_url": "test",
                "group_id": 1,
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
                "group_id": 1,
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
                "group_id": 1,
                "db_name": "test_db",
                "instance": self.ins.id,
            },
            "sql_content": "",
        }
        r = self.client.post("/api/v1/workflow/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)

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

    def test_audit_workflow_invalid_audit_type(self):
        r = self.client.post(
            f"/api/v1/workflow/{self.wf1.id}/reviews/",
            {
                "audit_remark": "noop",
                "workflow_type": self.audit1.workflow_type,
                "audit_type": "reject",
            },
            format="json",
        )
        self.assertEqual(r.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("audit_type", r.json())

    def test_audit_cancel_denies_non_owner(self):
        user2 = User.objects.create(
            username="workflow_user2",
            display="Workflow User 2",
            is_active=True,
        )
        user2.set_password("test_password")
        user2.save()
        r_login = self.client.post(
            "/api/auth/token/",
            {"username": "workflow_user2", "password": "test_password"},
            format="json",
        )
        self.client.credentials(
            HTTP_AUTHORIZATION="Bearer " + response_data(r_login)["access"]
        )
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

    def test_execute_workflow_requires_execute_permission(self):
        self.user.user_permissions.remove(
            Permission.objects.get(codename="sql_execute")
        )
        self.user.user_permissions.remove(
            Permission.objects.get(codename="sql_execute_for_resource_group")
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

    @patch("sql_api.api_workflow.can_execute", return_value=False)
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


class TestDashboardAPI(APITestCase):
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
            group_id=1,
            group_name="DBA",
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
            group_id=1,
            group_name="DBA",
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

        login_response = self.client.post(
            "/api/auth/token/",
            {"username": "dashboard_user", "password": "test_password"},
            format="json",
        )
        self.token = response_data(login_response)["access"]
        self.client.credentials(HTTP_AUTHORIZATION="Bearer " + self.token)

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

        login_response = self.client.post(
            "/api/auth/token/",
            {"username": "dashboard_no_perm", "password": "test_password"},
            format="json",
        )
        token = response_data(login_response)["access"]
        self.client.credentials(HTTP_AUTHORIZATION="Bearer " + token)

        response = self.client.get("/api/v1/dashboard/", format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
