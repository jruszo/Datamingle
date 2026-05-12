import json
from datetime import datetime, timedelta
from unittest.mock import ANY, MagicMock, Mock, patch

from django.test import TestCase

from sql.models import Instance, SqlWorkflow, SqlWorkflowContent, Users
from sql.utils.execute_sql import execute_callback

User = Users


class PickableMock(Mock):
    def __reduce__(self):
        return (Mock, ())


class TestUser(TestCase):
    def setUp(self):
        self.u1 = User(username="test_user", display="Display Name", is_active=True)
        self.u1.set_password("test_password")
        self.u1.save()

    def tearDown(self):
        self.u1.delete()

    def test_out_ranged_failed_login_count(self):
        self.u1.failed_login_count = 64
        self.u1.save()
        self.u1.refresh_from_db()
        self.assertEqual(64, self.u1.failed_login_count)

        self.u1.failed_login_count = 256
        self.u1.save()
        self.u1.refresh_from_db()
        self.assertEqual(127, self.u1.failed_login_count)

        self.u1.failed_login_count = -1
        self.u1.save()
        self.u1.refresh_from_db()
        self.assertEqual(0, self.u1.failed_login_count)


class TestAsync(TestCase):
    def setUp(self):
        self.now = datetime.now()
        self.u1 = User(username="some_user", display="User 1")
        self.u1.save()
        self.master1 = Instance.objects.create(
            instance_name="test_master_instance",
            type="master",
            db_type="mysql",
            host="testhost",
            port=3306,
            user="mysql_user",
            password="mysql_password",
        )
        self.wf1 = SqlWorkflow.objects.create(
            workflow_name="some_name2",
            group_id=1,
            group_name="g1",
            engineer=self.u1.username,
            engineer_display=self.u1.display,
            audit_auth_groups="some_group",
            create_time=self.now - timedelta(days=1),
            status="workflow_executing",
            is_backup=True,
            instance=self.master1,
            db_name="some_db",
            syntax_type=1,
        )
        self.wfc1 = SqlWorkflowContent.objects.create(
            workflow=self.wf1, sql_content="some_sql", execute_result=""
        )
        self.task_result = MagicMock()
        self.task_result.args = [self.wf1.id]
        self.task_result.success = True
        self.task_result.stopped = self.now
        self.task_result.result.json.return_value = json.dumps(
            [{"id": 1, "sql": "some_content"}]
        )
        self.task_result.result.warning = ""
        self.task_result.result.error = ""

    def tearDown(self):
        self.wf1.delete()
        self.u1.delete()
        self.task_result = None
        self.master1.delete()

    @patch("sql.utils.execute_sql.notify_for_execute")
    @patch("sql.utils.execute_sql.Audit")
    def test_call_back(self, mock_audit, mock_notify):
        mock_audit.detail_by_workflow_id.return_value.audit_id = 123
        mock_audit.add_log.return_value = "any thing"
        execute_callback(self.task_result)
        mock_audit.detail_by_workflow_id.assert_called_with(
            workflow_id=self.wf1.id, workflow_type=ANY
        )
        mock_audit.add_log.assert_called_with(
            audit_id=123,
            operation_type=ANY,
            operation_type_desc=ANY,
            operation_info="Execution result: finished successfully",
            operator=ANY,
            operator_display=ANY,
        )
        mock_notify.assert_called_once()
