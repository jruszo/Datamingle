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
from sql.engines.models import ReviewResult
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
)
import json
import pyotp

User = get_user_model()


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
        self.token = r.data["access"]
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
        self.assertEqual(r.json()["count"], 1)

    def test_get_current_user_context(self):
        """Test SPA bootstrap current-user endpoint."""
        r = self.client.get("/api/v1/me/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(r.json()["username"], self.user.username)
        self.assertIn("permissions", r.json())
        self.assertIn("groups", r.json())
        self.assertIn("resource_groups", r.json())

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
        self.assertEqual(r.json()["username"], "test_user2")

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
        r2 = self.client.delete(f'/api/v1/user/{r1.json()["id"]}/', format="json")
        self.assertEqual(r2.status_code, status.HTTP_204_NO_CONTENT)
        self.assertEqual(User.objects.filter(username="test_user2").count(), 0)

    def test_get_user_group_list(self):
        """Test getting user group list."""
        r = self.client.get("/api/v1/user/group/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(r.json()["count"], 1)

    def test_create_user_group(self):
        """Test creating user group."""
        json_data = {"name": "RD"}
        r = self.client.post("/api/v1/user/group/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        self.assertEqual(r.json()["name"], "RD")

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
        self.assertEqual(r.status_code, status.HTTP_204_NO_CONTENT)
        self.assertEqual(Group.objects.filter(name="DBA").count(), 0)

    def test_get_resource_group_list(self):
        """Test getting resource group list."""
        r = self.client.get("/api/v1/user/resourcegroup/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(r.json()["count"], 1)

    def test_create_resource_group(self):
        """Test creating resource group."""
        json_data = {
            "group_name": "prod",
            "ding_webhook": "https://oapi.dingtalk.com/robot/send?access_token=123",
        }
        r = self.client.post("/api/v1/user/resourcegroup/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        self.assertEqual(r.json()["group_name"], "prod")

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
        self.assertEqual(r.status_code, status.HTTP_204_NO_CONTENT)
        self.assertEqual(Group.objects.filter(name="test").count(), 0)

    def test_user_auth(self):
        """Test user authentication check."""
        json_data = {"password": "test_password"}
        r = self.client.post(f"/api/v1/user/auth/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(r.json(), {"detail": "Authentication successful."})

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
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(r.json()["data"]["totp"], "disabled")
        self.assertEqual(r.json()["data"]["sms"], "disabled")

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
        self.assertIn("access", r.json())
        self.assertIn("refresh", r.json())

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
        self.token = r.data["access"]
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
        self.assertEqual(r.json()["count"], 1)

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
        self.assertEqual(r.json()["instance_name"], "test_ins")

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
        self.assertEqual(r.status_code, status.HTTP_204_NO_CONTENT)
        self.assertEqual(Instance.objects.filter(instance_name="some_ins").count(), 0)

    def test_get_aliyunrds_list(self):
        """Test getting Aliyun RDS list."""
        r = self.client.get("/api/v1/instance/rds/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(r.json()["count"], 1)

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
        self.assertEqual(r.json()["rds_dbinstanceid"], "bbc")

    def test_get_tunnel_list(self):
        """Test getting tunnel list."""
        r = self.client.get("/api/v1/instance/tunnel/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(r.json()["count"], 1)

    def test_create_tunnel(self):
        """Test creating tunnel."""
        json_data = {"tunnel_name": "tunnel_test", "host": "one_host", "port": 22}
        r = self.client.post("/api/v1/instance/tunnel/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        self.assertEqual(r.json()["tunnel_name"], "tunnel_test")

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
        self.assertEqual(r.json()["count"], 1)


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
        self.token = r.data["access"]
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
        self.assertEqual(r.json()["count"], 1)

    def test_get_audit_list(self):
        """Test getting pending audit workflow list."""
        r = self.client.get("/api/v1/workflow/auditlist/", format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(r.json()["count"], 1)

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
        self.assertEqual(r.json()["count"], 1)

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
        self.assertListEqual(
            list(json.loads(r.content).keys()),
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
        self.assertListEqual(list(json.loads(r.content)["rows"][0].keys()), column_list)

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
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        self.assertEqual(r.json()["workflow"]["workflow_name"], "Release Workflow 1")
        self.assertEqual(r.json()["workflow"]["engineer"], self.user.username)
        self.assertEqual(r.json()["workflow"]["engineer_display"], self.user.display)

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
        self.assertEqual(r.status_code, status.HTTP_201_CREATED)
        self.assertEqual(r.json()["workflow"]["workflow_name"], "Release Workflow 1")
        self.assertEqual(r.json()["workflow"]["engineer"], user2.username)
        self.assertEqual(r.json()["workflow"]["engineer_display"], user2.display)

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
        return_data = r.json()
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
            "workflow_id": self.wf1.id,
            "audit_remark": "cancel",
            "workflow_type": self.audit1.workflow_type,
            "audit_type": "cancel",
        }
        r = self.client.post("/api/v1/workflow/audit/", json_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(r.json(), {"detail": "canceled"})

    def test_execute_workflow(self):
        """Test executing workflow."""
        # Audit first
        audit_data = {
            "workflow_id": self.wf1.id,
            "audit_remark": "approved",
            "workflow_type": self.audit1.workflow_type,
            "audit_type": "pass",
        }
        self.client.post("/api/v1/workflow/audit/", audit_data, format="json")
        # Then execute
        execute_data = {
            "workflow_id": self.wf1.id,
            "workflow_type": self.audit1.workflow_type,
            "mode": "manual",
        }
        r = self.client.post("/api/v1/workflow/execute/", execute_data, format="json")
        self.assertEqual(r.status_code, status.HTTP_200_OK)
        self.assertEqual(
            r.json(),
            {
                "detail": "Execution started. Please check workflow detail page for results."
            },
        )
