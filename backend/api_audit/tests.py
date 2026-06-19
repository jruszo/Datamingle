from django.contrib.auth import get_user_model
from django.contrib.auth.models import Permission
from django.test import TestCase

from common.utils.const import WorkflowType
from sql.models import (
    AuditEntry,
    Instance,
    QueryLog,
    SqlWorkflow,
    WorkflowAudit,
    WorkflowLog,
)


class AuditApiTests(TestCase):
    def setUp(self):
        self.user = get_user_model().objects.create_user(
            username="audit_user", password="test", display="Audit User"
        )
        self.client.force_login(self.user)

    def add_audit_permission(self):
        self.user.user_permissions.add(Permission.objects.get(codename="audit_user"))

    def test_general_audit_requires_audit_permission(self):
        response = self.client.get("/api/v1/audit/general/")

        self.assertEqual(response.status_code, 403)

    def test_general_audit_filters_by_search_and_action(self):
        self.add_audit_permission()
        AuditEntry.objects.create(
            user_id=self.user.id,
            user_name=self.user.username,
            user_display=self.user.display,
            action="Login",
            extra_info="127.0.0.1",
        )
        AuditEntry.objects.create(
            user_id=self.user.id,
            user_name=self.user.username,
            user_display=self.user.display,
            action="Logout",
            extra_info="10.0.0.1",
        )

        response = self.client.get(
            "/api/v1/audit/general/",
            {"action": "Login", "search": "127.0.0.1"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()["data"]
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["results"][0]["action"], "Login")

    def test_general_audit_ignores_invalid_date_filter(self):
        self.add_audit_permission()
        AuditEntry.objects.create(
            user_id=self.user.id,
            user_name=self.user.username,
            user_display=self.user.display,
            action="Login",
            extra_info="invalid-date-filter-target",
        )

        response = self.client.get(
            "/api/v1/audit/general/",
            {
                "search": "invalid-date-filter-target",
                "start_date": "bad-date",
                "end_date": "9999-12-31",
            },
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["data"]["count"], 1)

    def test_query_audit_lists_queries_across_users(self):
        self.add_audit_permission()
        QueryLog.objects.create(
            instance_name="mysql-a",
            db_name="appdb",
            sqllog="select * from accounts",
            effect_row=2,
            cost_time="0.01",
            username="other_user",
            user_display="Other User",
        )

        response = self.client.get("/api/v1/audit/query/", {"search": "accounts"})

        self.assertEqual(response.status_code, 200)
        payload = response.json()["data"]
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["results"][0]["username"], "other_user")

    def test_sql_workflow_audit_filters_workflows(self):
        self.add_audit_permission()
        instance = Instance.objects.create(
            instance_name="workflow-mysql",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
        )
        SqlWorkflow.objects.create(
            workflow_name="Create accounts table",
            team_id=1,
            team_name="DBA",
            instance=instance,
            db_name="appdb",
            syntax_type=1,
            engineer="requester",
            engineer_display="Requester",
            status="workflow_finish",
            audit_auth_groups="1",
        )

        response = self.client.get(
            "/api/v1/audit/sql-workflow/",
            {"status": "workflow_finish", "search": "accounts"},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()["data"]
        self.assertEqual(payload["count"], 1)
        self.assertEqual(
            payload["results"][0]["workflow_name"], "Create accounts table"
        )

    def test_workflow_operation_logs_resolve_by_workflow_id(self):
        self.add_audit_permission()
        workflow_audit = WorkflowAudit.objects.create(
            team_id=1,
            team_name="DBA",
            workflow_id=42,
            workflow_type=WorkflowType.SQL_REVIEW,
            workflow_title="Workflow",
            audit_auth_groups="1",
            current_audit="1",
            next_audit="",
            current_status=2,
            create_user="requester",
        )
        WorkflowLog.objects.create(
            audit_id=workflow_audit.audit_id,
            operation_type=1,
            operation_type_desc="Approve",
            operation_info="Approved by DBA",
            operator="audit_user",
            operator_display="Audit User",
        )

        response = self.client.get(
            "/api/v1/audit/workflow-log/",
            {"workflow_id": 42, "workflow_type": WorkflowType.SQL_REVIEW},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()["data"]
        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["results"][0]["operation_info"], "Approved by DBA")

    def test_workflow_operation_logs_reject_invalid_audit_id(self):
        self.add_audit_permission()

        response = self.client.get(
            "/api/v1/audit/workflow-log/",
            {"audit_id": "not-a-number"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("audit_id", response.json())
