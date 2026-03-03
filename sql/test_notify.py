import json
from datetime import datetime, timedelta, date
from unittest.mock import patch, Mock, ANY
import pytest
from pytest_mock import MockFixture

from django.contrib.auth.models import Group
from django.contrib.auth import get_user_model
from django.test import TestCase

from common.config import SysConfig
from sql.models import (
    Instance,
    SqlWorkflow,
    SqlWorkflowContent,
    QueryPrivilegesApply,
    WorkflowAudit,
    WorkflowAuditDetail,
    ResourceGroup,
    ArchiveConfig,
)
from sql.notify import (
    auto_notify,
    EventType,
    LegacyRender,
    GenericWebhookNotifier,
    My2SqlResult,
    DingdingWebhookNotifier,
    DingdingPersonNotifier,
    FeishuPersonNotifier,
    FeishuWebhookNotifier,
    QywxWebhookNotifier,
    QywxToUserNotifier,
    LegacyMessage,
    Notifier,
    notify_for_execute,
    notify_for_audit,
    notify_for_my2sql,
    MailNotifier,
)

User = get_user_model()


class TestNotify(TestCase):
    """
    Notification tests.
    """

    def setUp(self):
        self.sys_config = SysConfig()
        self.aug = Group.objects.create(id=1, name="auth_group")
        self.user = User.objects.create(
            username="test_user", display="Display Name", is_active=True
        )
        self.su = User.objects.create(
            username="s_user",
            display="Display Name",
            is_active=True,
            is_superuser=True,
        )
        self.su.groups.add(self.aug)

        tomorrow = date.today() + timedelta(days=1)
        self.ins = Instance.objects.create(
            instance_name="some_ins",
            type="slave",
            db_type="mysql",
            host="some_host",
            port=3306,
            user="ins_user",
            password="some_str",
        )
        self.wf = SqlWorkflow.objects.create(
            workflow_name="some_name",
            group_id=1,
            group_name="g1",
            engineer=self.user.username,
            engineer_display=self.user.display,
            audit_auth_groups="some_audit_group",
            create_time=datetime.now(),
            status="workflow_timingtask",
            is_backup=True,
            instance=self.ins,
            db_name="some_db",
            syntax_type=1,
        )
        SqlWorkflowContent.objects.create(
            workflow=self.wf, sql_content="some_sql", execute_result=""
        )
        self.query_apply_1 = QueryPrivilegesApply.objects.create(
            group_id=1,
            group_name="some_name",
            title="some_title1",
            user_name="some_user",
            instance=self.ins,
            db_list="some_db,some_db2",
            limit_num=100,
            valid_date=tomorrow,
            priv_type=1,
            status=0,
            audit_auth_groups="some_audit_group",
        )
        # Required objects:
        # WorkflowAudit: one record per workflow.
        # WorkflowAuditDetail: one record per audit step linked to WorkflowAudit.
        self.audit_wf = WorkflowAudit.objects.create(
            group_id=1,
            group_name="some_group",
            workflow_id=self.wf.id,
            workflow_type=2,
            workflow_title="Request title",
            workflow_remark="Request note",
            audit_auth_groups="1",
            current_audit="1",
            next_audit="2",
            current_status=0,
            create_user=self.user.username,
        )
        self.audit_wf_detail = WorkflowAuditDetail.objects.create(
            audit_id=self.audit_wf.audit_id,
            audit_user=self.user.display,
            audit_time=datetime.now(),
            audit_status=1,
            remark="Test note",
        )
        self.audit_query = WorkflowAudit.objects.create(
            group_id=1,
            group_name="some_group",
            workflow_id=self.query_apply_1.apply_id,
            workflow_type=1,
            workflow_title="Request title",
            workflow_remark="Request note",
            audit_auth_groups=",".join([str(self.aug.id)]),
            current_audit=str(self.aug.id),
            next_audit="-1",
            current_status=0,
        )
        self.audit_query_detail = WorkflowAuditDetail.objects.create(
            audit_id=self.audit_query.audit_id,
            audit_user=self.user.display,
            audit_time=datetime.now(),
            audit_status=1,
            remark="Test query note",
        )

        self.rs = ResourceGroup.objects.create(group_id=1, ding_webhook="url")

        self.archive_apply = ArchiveConfig.objects.create(
            title="Test archive",
            resource_group=self.rs,
            src_instance=self.ins,
            src_db_name="foo",
            src_table_name="bar",
            dest_db_name="foo-dest",
            dest_table_name="bar-dest",
            mode="purge",
            no_delete=False,
            status=0,
            user_name=self.user.username,
            user_display=self.user.display,
        )
        self.archive_apply_audit = WorkflowAudit.objects.create(
            group_id=1,
            group_name="some_group",
            workflow_id=self.archive_apply.id,
            workflow_type=3,
            workflow_title=self.archive_apply.title,
            workflow_remark="Request note",
            audit_auth_groups=",".join([str(self.aug.id)]),
            current_audit=str(self.aug.id),
            next_audit="-1",
            current_status=0,
        )

    def tearDown(self):
        self.sys_config.purge()
        User.objects.all().delete()
        SqlWorkflow.objects.all().delete()
        SqlWorkflowContent.objects.all().delete()
        WorkflowAudit.objects.all().delete()
        WorkflowAuditDetail.objects.all().delete()
        ArchiveConfig.objects.all().delete()
        ResourceGroup.objects.all().delete()

    def test_empty_notifiers(self):
        with self.settings(ENABLED_NOTIFIERS=()):
            auto_notify(
                workflow=self.wf,
                event_type=EventType.EXECUTE,
                sys_config=self.sys_config,
            )

    def test_base_notifier(self):
        self.sys_config.set("foo", "bar")
        n = Notifier(workflow=self.wf, sys_config=self.sys_config)
        n.sys_config_key = "foo"
        self.assertTrue(n.should_run())
        with self.assertRaises(NotImplementedError):
            n.run()
        n.send = Mock()
        n.render = Mock()
        n.run()
        n.sys_config_key = "not-foo"
        self.assertFalse(n.should_run())

    def test_no_workflow_and_audit(self):
        with self.assertRaises(ValueError):
            Notifier(workflow=None, audit=None)

    @patch("sql.notify.FeishuWebhookNotifier.run")
    def test_auto_notify(self, mock_run):
        with self.settings(ENABLED_NOTIFIERS=("sql.notify:FeishuWebhookNotifier",)):
            auto_notify(self.sys_config, event_type=EventType.EXECUTE, workflow=self.wf)
            mock_run.assert_called_once()

    @patch("sql.notify.auto_notify")
    def test_notify_for_execute(self, mock_auto_notify: Mock):
        """Test adapter."""
        notify_for_execute(self.wf)
        mock_auto_notify.assert_called_once_with(
            workflow=self.wf, sys_config=ANY, event_type=EventType.EXECUTE
        )

    @patch("sql.notify.auto_notify")
    def test_notify_for_audit(self, mock_auto_notify: Mock):
        """Test adapter."""
        notify_for_audit(
            workflow_audit=self.audit_wf, workflow_audit_detail=self.audit_wf_detail
        )
        mock_auto_notify.assert_called_once_with(
            workflow=None,
            event_type=EventType.AUDIT,
            sys_config=ANY,
            audit=self.audit_wf,
            audit_detail=self.audit_wf_detail,
        )

    @patch("sql.notify.auto_notify")
    def test_notify_for_m2sql(self, mock_auto_notify: Mock):
        """Test adapter."""
        task = Mock()
        task.success = True
        task.kwargs = {"user": "foo"}
        task.result = ["", "/foo"]
        expect_workflow = My2SqlResult(success=True, submitter="foo", file_path="/foo")
        notify_for_my2sql(task)
        mock_auto_notify.assert_called_once_with(
            workflow=expect_workflow, sys_config=ANY, event_type=EventType.M2SQL
        )
        mock_auto_notify.reset_mock()
        # Test failure scenario.
        task.success = False
        task.result = "Traceback blahblah"
        expect_workflow = My2SqlResult(
            success=False, submitter="foo", error=task.result
        )
        notify_for_my2sql(task)
        mock_auto_notify.assert_called_once_with(
            workflow=expect_workflow, sys_config=ANY, event_type=EventType.M2SQL
        )

    # The tests below focus on notifier render() and send().
    def test_legacy_render_execution(self):
        notifier = LegacyRender(
            workflow=self.wf, event_type=EventType.EXECUTE, sys_config=self.sys_config
        )
        notifier.render()
        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("Workflow", notifier.messages[0].msg_title)
        with self.assertRaises(NotImplementedError):
            notifier.send()

    def test_legacy_render_execution_ddl(self):
        """DDL has one extra DBA notification compared to normal workflow."""
        self.wf.syntax_type = 1
        self.wf.status = "workflow_finish"
        self.wf.save()
        self.sys_config.set("ddl_notify_auth_group", self.aug.name)
        notifier = LegacyRender(
            workflow=self.wf, event_type=EventType.EXECUTE, sys_config=self.sys_config
        )
        notifier.render()
        self.assertEqual(len(notifier.messages), 2)
        self.assertIn(
            "A new DDL statement finished execution", notifier.messages[1].msg_title
        )

    def test_legacy_render_audit(self):
        notifier = LegacyRender(
            workflow=self.wf,
            event_type=EventType.AUDIT,
            audit=self.audit_wf,
            audit_detail=self.audit_wf_detail,
            sys_config=self.sys_config,
        )
        notifier.render()
        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("New Workflow Request", notifier.messages[0].msg_title)
        # Test without providing workflow.
        notifier = LegacyRender(
            event_type=EventType.AUDIT,
            workflow=None,
            audit=self.audit_wf,
            audit_detail=self.audit_wf_detail,
            sys_config=self.sys_config,
        )
        notifier.render()
        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("New Workflow Request", notifier.messages[0].msg_title)

    def test_legacy_render_query_audit(self):
        # Default is database-level permission.
        notifier = LegacyRender(
            workflow=self.query_apply_1,
            event_type=EventType.AUDIT,
            audit=self.audit_query,
            audit_detail=self.audit_query_detail,
            sys_config=self.sys_config,
        )
        notifier.render()
        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("Database List", notifier.messages[0].msg_content)

        # Table-level permission request.
        self.query_apply_1.priv_type = 2
        self.query_apply_1.table_list = "foo,bar"
        self.query_apply_1.save()
        notifier = LegacyRender(
            workflow=self.query_apply_1,
            event_type=EventType.AUDIT,
            audit=self.audit_query,
            audit_detail=self.audit_query_detail,
            sys_config=self.sys_config,
        )
        notifier.render()
        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("Table List", notifier.messages[0].msg_content)
        self.assertIn("foo,bar", notifier.messages[0].msg_content)

    def test_legacy_render_archive_audit(self):
        notifier = LegacyRender(
            workflow=self.archive_apply,
            event_type=EventType.AUDIT,
            audit=self.archive_apply_audit,
            sys_config=self.sys_config,
        )
        notifier.render()
        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("Archive Table", notifier.messages[0].msg_content)

    def test_legacy_render_audit_success(self):
        """Approved audit message."""
        # Only test SQL deployment workflow here.
        self.audit_wf.current_status = 1
        self.audit_wf.save()
        notifier = LegacyRender(
            workflow=self.wf,
            event_type=EventType.AUDIT,
            audit=self.audit_wf,
            sys_config=self.sys_config,
        )
        notifier.render()
        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("Workflow Approved", notifier.messages[0].msg_title)

    def test_legacy_render_audit_reject(self):
        self.audit_wf.current_status = 2
        self.audit_wf.save()
        self.audit_wf_detail.remark = "Rejected foo-bar"
        self.audit_wf_detail.save()
        notifier = LegacyRender(
            workflow=self.wf,
            event_type=EventType.AUDIT,
            audit=self.audit_wf,
            audit_detail=self.audit_wf_detail,
            sys_config=self.sys_config,
        )
        notifier.render()
        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("Workflow Rejected", notifier.messages[0].msg_title)
        self.assertIn("Rejected foo-bar", notifier.messages[0].msg_content)

    def test_legacy_render_audit_abort(self):
        self.audit_wf.current_status = 3
        self.audit_wf.save()
        self.audit_wf_detail.remark = "Withdrawn foo-bar"
        self.audit_wf_detail.save()
        notifier = LegacyRender(
            workflow=self.wf,
            event_type=EventType.AUDIT,
            audit=self.audit_wf,
            audit_detail=self.audit_wf_detail,
            sys_config=self.sys_config,
        )
        notifier.render()
        self.assertEqual(len(notifier.messages), 1)
        self.assertIn("Workflow Cancelled by Submitter", notifier.messages[0].msg_title)
        self.assertIn("Withdrawn foo-bar", notifier.messages[0].msg_content)

    def test_legacy_render_m2sql(self):
        successful_workflow = My2SqlResult(
            submitter=self.user.username, success=True, file_path="/foo/bar"
        )
        notifier = LegacyRender(
            workflow=successful_workflow,
            sys_config=self.sys_config,
            event_type=EventType.M2SQL,
        )
        notifier.render()
        self.assertEqual(len(notifier.messages), 1)
        self.assertEqual(
            notifier.messages[0].msg_title,
            "[Archery Notification] My2SQL execution finished",
        )
        # Failure.
        failed_workflow = My2SqlResult(
            submitter=self.user.username, success=False, error="Traceback blahblah"
        )
        notifier = LegacyRender(
            workflow=failed_workflow,
            sys_config=self.sys_config,
            event_type=EventType.M2SQL,
        )
        notifier.render()
        self.assertEqual(len(notifier.messages), 1)
        self.assertEqual(
            notifier.messages[0].msg_title,
            "[Archery Notification] My2SQL execution failed",
        )

    def test_general_webhook(self):
        # SQL deployment workflow
        notifier = GenericWebhookNotifier(
            workflow=self.wf,
            event_type=EventType.AUDIT,
            audit=self.audit_wf,
            audit_detail=self.audit_wf_detail,
            sys_config=self.sys_config,
        )
        notifier.render()
        self.assertIsNotNone(notifier.request_data)
        print(json.dumps(notifier.request_data))
        self.assertDictEqual(
            notifier.request_data["audit"],
            {
                "audit_id": self.audit_wf.audit_id,
                "group_name": "some_group",
                "workflow_type": 2,
                "create_user_display": "",
                "workflow_title": "Request title",
                "audit_auth_groups": self.audit_wf.audit_auth_groups,
                "current_audit": "1",
                "current_status": 0,
                "create_time": self.audit_wf.create_time.isoformat(),
            },
        )
        self.assertDictEqual(
            notifier.request_data["workflow_content"]["workflow"],
            {
                "id": self.wf.id,
                "workflow_name": "some_name",
                "demand_url": "",
                "group_id": 1,
                "group_name": "g1",
                "db_name": "some_db",
                "syntax_type": 1,
                "is_backup": True,
                "engineer": "test_user",
                "engineer_display": "Display Name",
                "status": "workflow_timingtask",
                "audit_auth_groups": "some_audit_group",
                "run_date_start": None,
                "run_date_end": None,
                "finish_time": None,
                "is_manual": 0,
                "instance": self.ins.id,
                "create_time": self.wf.create_time.isoformat(),
                "is_offline_export": 0,
                "export_format": None,
                "file_name": None,
            },
        )
        self.assertEqual(
            notifier.request_data["workflow_content"]["sql_content"], "some_sql"
        )
        self.assertEqual(
            notifier.request_data["instance"]["instance_name"], self.ins.instance_name
        )
        # SQL query privilege workflow
        notifier = GenericWebhookNotifier(
            workflow=self.query_apply_1,
            event_type=EventType.AUDIT,
            audit=self.audit_query,
            audit_detail=self.audit_query_detail,
            sys_config=self.sys_config,
        )
        notifier.render()
        self.assertIsNotNone(notifier.request_data)
        self.assertEqual(
            notifier.request_data["workflow_content"]["title"], self.query_apply_1.title
        )


@pytest.mark.parametrize(
    "notifier_to_test,method_assert_called",
    [
        (DingdingWebhookNotifier, "send_ding"),
        (DingdingPersonNotifier, "send_ding2user"),
        (FeishuWebhookNotifier, "send_feishu_webhook"),
        (FeishuPersonNotifier, "send_feishu_user"),
        (QywxWebhookNotifier, "send_qywx_webhook"),
        (QywxToUserNotifier, "send_wx2user"),
        (MailNotifier, "send_email"),
    ],
)
def test_notify_send(
    mocker: MockFixture,
    create_audit_workflow,
    notifier_to_test: Notifier.__class__,
    method_assert_called: str,
):
    """Test notifier send().

    Initialize ``notifier_to_test``, call ``send()``, and assert the corresponding
    ``MsgSender`` method was called. If a notifier does not use ``MsgSender``,
    it should be tested separately.
    """
    mock_send_method = Mock()
    mock_msg_sender = mocker.patch("sql.notify.MsgSender")
    mocker.patch("sql.models.WorkflowAudit.get_workflow")
    setattr(mock_msg_sender.return_value, method_assert_called, mock_send_method)
    notifier = notifier_to_test(
        workflow=None, audit=create_audit_workflow, sys_config=SysConfig()
    )
    notifier.messages = [
        LegacyMessage(msg_to=[Mock()], msg_title="test", msg_content="test")
    ]
    notifier.send()
    mock_send_method.assert_called_once()


def test_override_sys_key():
    """Ensure dataclass inheritance can override class-level defaults."""

    class OverrideNotifier(Notifier):
        sys_config_key = "test"

    n = OverrideNotifier(workflow=Mock())
    assert n.sys_config_key == "test"
