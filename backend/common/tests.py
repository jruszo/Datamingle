import json
import smtplib
import psycopg2
from unittest.mock import patch, ANY, Mock
import datetime
from dateutil.relativedelta import relativedelta
from django.contrib.auth import get_user_model
from django.test import RequestFactory, TestCase, override_settings

from common.config import SysConfig
from common import task_queue
from common.utils.sendmsg import MsgSender
from common.utils.const import WorkflowType
from common.utils.global_info import global_info
from common.utils.spa import spa_path_for_workflow, spa_url_for_workflow
from sql.engines import EngineBase, ResultSet
from sql import inventory
from sql.models import (
    Config,
    Instance,
    SqlWorkflow,
    SqlWorkflowContent,
    QueryLog,
    ResourceGroup,
    TaskSchedule,
    TwoFactorAuthConfig,
)
from common.utils.chart_dao import ChartDao
from common.auth import init_user
from common.twofa.sms import SMS
from common.twofa.totp import TOTP
from common.utils.extend_json_encoder import ExtendJSONEncoderFTime

User = get_user_model()
TASK_CALLBACK_RESULTS = []


def sample_task(value, suffix=""):
    return f"{value}{suffix}"


def failing_task():
    raise RuntimeError("task boom")


def record_task_callback(task_result):
    TASK_CALLBACK_RESULTS.append(task_result)


def failing_task_callback(task_result):
    TASK_CALLBACK_RESULTS.append(task_result)
    raise RuntimeError("callback boom")


class ConfigOpsTests(TestCase):
    def setUp(self):
        pass

    def test_purge(self):
        archer_config = SysConfig()
        archer_config.set("some_key", "some_value")
        archer_config.purge()
        self.assertEqual({}, archer_config.sys_config)
        archer_config2 = SysConfig()
        self.assertEqual({}, archer_config2.sys_config)

    def test_replace_configs(self):
        archer_config = SysConfig()
        new_config = json.dumps(
            [
                {"key": "numconfig", "value": 1},
                {"key": "strconfig", "value": "strconfig"},
                {"key": "boolconfig", "value": "false"},
            ]
        )
        archer_config.replace(new_config)
        archer_config.get_all_config()
        expected_config = {
            "numconfig": "1",
            "strconfig": "strconfig",
            "boolconfig": False,
        }
        self.assertEqual(archer_config.sys_config, expected_config)

    def test_get_bool_transform(self):
        bool_config = json.dumps([{"key": "boolconfig2", "value": "false"}])
        archer_config = SysConfig()
        archer_config.replace(bool_config)
        self.assertEqual(archer_config.sys_config["boolconfig2"], False)

    def test_set_bool_transform(self):
        archer_config = SysConfig()
        archer_config.set("boolconfig3", False)
        self.assertEqual(archer_config.sys_config["boolconfig3"], False)

    def test_get_other_data(self):
        new_config = json.dumps([{"key": "other_config", "value": "testvalue"}])
        archer_config = SysConfig()
        archer_config.replace(new_config)
        self.assertEqual(archer_config.sys_config["other_config"], "testvalue")

    def test_set_other_data(self):
        archer_config = SysConfig()
        archer_config.set("other_config", "testvalue3")
        self.assertEqual(archer_config.sys_config["other_config"], "testvalue3")


class InventoryRefreshTests(TestCase):
    def setUp(self):
        self.sys_config = SysConfig()
        self.instance = Instance.objects.create(
            instance_name="inventory-test",
            type="master",
            db_type="mysql",
            host="inventory-host",
            port=3306,
            user="inventory-user",
            password="secret",
        )

    def tearDown(self):
        TaskSchedule.objects.all().delete()
        Instance.objects.all().delete()
        self.sys_config.purge()

    @patch("common.task_queue._refresh_celery_runtime_config")
    @patch("common.task_queue._celery_execute_task")
    def test_ensure_inventory_refresh_schedule_creates_single_active_schedule(
        self, mock_celery_execute_task, _mock_refresh
    ):
        mock_celery_execute_task.return_value.apply_async.return_value.id = (
            "inventory-task-id"
        )
        inventory.ensure_inventory_refresh_schedule(force=True)
        inventory.ensure_inventory_refresh_schedule()

        self.assertEqual(
            TaskSchedule.objects.filter(
                name=inventory.INVENTORY_REFRESH_SCHEDULE_NAME
            ).count(),
            1,
        )
        mock_celery_execute_task.return_value.apply_async.assert_called_once()

    @patch("common.task_queue._refresh_celery_runtime_config")
    @patch("common.task_queue._celery_execute_task")
    def test_force_schedule_refresh_replaces_next_run_when_interval_changes(
        self, mock_celery_execute_task, _mock_refresh
    ):
        mock_celery_execute_task.return_value.apply_async.return_value.id = (
            "inventory-task-id"
        )
        self.sys_config.set("inventory_refresh_interval", "24h")
        inventory.ensure_inventory_refresh_schedule(force=True)
        first_run = TaskSchedule.objects.get(
            name=inventory.INVENTORY_REFRESH_SCHEDULE_NAME
        ).run_at

        self.sys_config.set("inventory_refresh_interval", "1h")
        inventory.ensure_inventory_refresh_schedule(force=True)
        second_run = TaskSchedule.objects.get(
            name=inventory.INVENTORY_REFRESH_SCHEDULE_NAME
        ).run_at

        self.assertLess(second_run, first_run)
        self.assertEqual(
            mock_celery_execute_task.return_value.apply_async.call_count,
            2,
        )
        self.assertTrue(
            Config.objects.filter(
                item=inventory.INVENTORY_REFRESH_SCHEDULE_LOCK_NAME
            ).exists()
        )

    @patch("common.task_queue._refresh_celery_runtime_config")
    @patch("common.task_queue._celery_execute_task")
    def test_inventory_refresh_task_callback_rearms_schedule(
        self, mock_celery_execute_task, _mock_refresh
    ):
        mock_celery_execute_task.return_value.apply_async.return_value.id = (
            "inventory-task-id"
        )
        inventory.inventory_refresh_task_callback(Mock(success=True))

        self.assertTrue(
            TaskSchedule.objects.filter(
                name=inventory.INVENTORY_REFRESH_SCHEDULE_NAME
            ).exists()
        )
        mock_celery_execute_task.return_value.apply_async.assert_called_once()

    @patch(
        "sql.inventory.collect_inventory_snapshot",
        return_value={"hostname": "detected-host", "version": "8.0.36"},
    )
    def test_refresh_instance_inventory_snapshot_marks_ok(self, _collect_snapshot):
        result = inventory.refresh_instance_inventory_snapshot(self.instance)

        self.instance.refresh_from_db()
        self.assertEqual(result["status"], Instance.INVENTORY_STATUS_OK)
        self.assertEqual(self.instance.inventory_status, Instance.INVENTORY_STATUS_OK)
        self.assertEqual(self.instance.inventory_detected_hostname, "detected-host")
        self.assertEqual(self.instance.inventory_detected_version, "8.0.36")
        self.assertIsNotNone(self.instance.inventory_last_attempt_at)
        self.assertIsNotNone(self.instance.inventory_last_success_at)

    @patch(
        "sql.inventory.collect_inventory_snapshot",
        side_effect=[
            {"hostname": "detected-host", "version": "8.0.36"},
            RuntimeError("boom"),
        ],
    )
    def test_refresh_instance_inventory_snapshot_keeps_last_good_values_when_stale(
        self, _collect_snapshot
    ):
        inventory.refresh_instance_inventory_snapshot(self.instance)
        inventory.refresh_instance_inventory_snapshot(self.instance)

        self.instance.refresh_from_db()
        self.assertEqual(
            self.instance.inventory_status, Instance.INVENTORY_STATUS_STALE
        )
        self.assertEqual(self.instance.inventory_detected_hostname, "detected-host")
        self.assertEqual(self.instance.inventory_detected_version, "8.0.36")
        self.assertIsNotNone(self.instance.inventory_last_success_at)

    @patch("sql.inventory.collect_inventory_snapshot", side_effect=RuntimeError("boom"))
    def test_refresh_instance_inventory_snapshot_marks_failed_before_first_success(
        self, _collect_snapshot
    ):
        inventory.refresh_instance_inventory_snapshot(self.instance)

        self.instance.refresh_from_db()
        self.assertEqual(
            self.instance.inventory_status, Instance.INVENTORY_STATUS_FAILED
        )
        self.assertEqual(self.instance.inventory_detected_hostname, "")
        self.assertEqual(self.instance.inventory_detected_version, "")
        self.assertIsNone(self.instance.inventory_last_success_at)

    def test_engine_base_inventory_details_without_instance_returns_safe_defaults(self):
        self.assertEqual(
            EngineBase().get_inventory_details(),
            {"hostname": "", "version": ""},
        )

    def test_refresh_inventory_snapshots_maps_status_constants_to_summary_keys(self):
        mock_queryset = Mock()
        mock_queryset.iterator.return_value = [self.instance]
        with patch.object(Instance, "INVENTORY_STATUS_OK", "healthy"):
            with patch("sql.inventory.close_old_connections"):
                with patch(
                    "sql.inventory.Instance.objects.order_by",
                    return_value=mock_queryset,
                ):
                    with patch(
                        "sql.inventory.refresh_instance_inventory_snapshot",
                        return_value={"status": "healthy"},
                    ):
                        summary = inventory.refresh_inventory_snapshots()

        self.assertEqual(summary["total"], 1)
        self.assertEqual(summary["ok"], 1)
        self.assertEqual(summary["stale"], 0)
        self.assertEqual(summary["failed"], 0)

    def test_format_inventory_version_normalizes_lists_and_tuples(self):
        self.assertEqual(
            inventory._format_inventory_version((" 8 ", None, " 0 ", "", " 36 ")),
            "8.0.36",
        )
        self.assertEqual(
            inventory._format_inventory_version(["  2024 ", " 04 ", " 1 "]),
            "2024.04.1",
        )


class SendMessageTest(TestCase):
    """Message sending tests."""

    def setUp(self):
        archer_config = SysConfig()
        self.smtp_server = "test_smtp_server"
        self.smtp_user = "test_smtp_user"
        self.smtp_password = "some_str"
        self.smtp_port = 1234
        self.smtp_ssl = True
        archer_config.set("mail_smtp_server", self.smtp_server)
        archer_config.set("mail_smtp_user", self.smtp_user)
        archer_config.set("mail_smtp_password", self.smtp_password)
        archer_config.set("mail_smtp_port", self.smtp_port)
        archer_config.set("mail_ssl", self.smtp_ssl)

    def testSenderInit(self):
        sender = MsgSender()
        self.assertEqual(sender.MAIL_REVIEW_SMTP_PORT, self.smtp_port)
        archer_config = SysConfig()
        archer_config.set("mail_smtp_port", "")
        sender = MsgSender()
        self.assertEqual(sender.MAIL_REVIEW_SMTP_PORT, 465)
        archer_config.set("mail_ssl", False)
        sender = MsgSender()
        self.assertEqual(sender.MAIL_REVIEW_SMTP_PORT, 25)

    @patch.object(smtplib.SMTP, "__init__", return_value=None)
    @patch.object(smtplib.SMTP, "login")
    @patch.object(smtplib.SMTP, "sendmail")
    @patch.object(smtplib.SMTP, "quit")
    def testNoPasswordSendMail(self, _quit, sendmail, login, _):
        """No-password email test."""
        some_sub = "test_subject"
        some_body = "mail_body"
        some_to = ["mail_to"]
        archer_config = SysConfig()
        archer_config.set("mail_ssl", "")

        archer_config.set("mail_smtp_password", "")
        sender2 = MsgSender()
        sender2.send_email(some_sub, some_body, some_to)
        login.assert_not_called()

    @patch.object(smtplib.SMTP, "__init__", return_value=None)
    @patch.object(smtplib.SMTP, "login")
    @patch.object(smtplib.SMTP, "sendmail")
    @patch.object(smtplib.SMTP, "quit")
    def testSendMail(self, _quit, sendmail, login, _):
        """Password-protected SMTP test."""
        some_sub = "test_subject"
        some_body = "mail_body"
        some_to = ["mail_to"]
        archer_config = SysConfig()
        archer_config.set("mail_ssl", "")
        archer_config.set("mail_smtp_password", self.smtp_password)
        sender = MsgSender()
        sender.send_email(some_sub, some_body, some_to)
        login.assert_called_once()
        sendmail.assert_called_with(self.smtp_user, some_to, ANY)
        _quit.assert_called_once()

    @patch.object(smtplib.SMTP, "__init__", return_value=None)
    @patch.object(smtplib.SMTP, "login")
    @patch.object(smtplib.SMTP, "sendmail")
    @patch.object(smtplib.SMTP, "quit")
    def testSSLSendMail(self, _quit, sendmail, login, _):
        """SSL SMTP test."""
        some_sub = "test_subject"
        some_body = "mail_body"
        some_to = ["mail_to"]
        archer_config = SysConfig()
        archer_config.set("mail_ssl", True)
        sender = MsgSender()
        sender.send_email(some_sub, some_body, some_to)
        sendmail.assert_called_with(self.smtp_user, some_to, ANY)
        _quit.assert_called_once()

    def tearDown(self):
        archer_config = SysConfig()
        archer_config.set("mail_smtp_server", "")
        archer_config.set("mail_smtp_user", "")
        archer_config.set("mail_smtp_password", "")
        archer_config.set("mail_smtp_port", "")
        archer_config.set("mail_ssl", "")


class GlobalInfoTest(TestCase):
    def setUp(self):
        self.factory = RequestFactory()
        self.u1 = User(username="test_user", display="Chinese display", is_active=True)
        self.u1.save()

    @patch("sql.utils.workflow_audit.Audit.todo")
    def testGlobalInfo(self, todo):
        """Test global info context."""
        request = self.factory.get("/missing-page/")
        request.user = type("AnonymousUser", (), {"is_authenticated": False})()
        r = global_info(request)
        todo.assert_not_called()
        self.assertEqual(r["todo"], 0)
        # Authenticated user
        todo.return_value = 3
        request = self.factory.get("/missing-page/")
        request.user = self.u1
        r = global_info(request)
        todo.assert_called_once_with(self.u1)
        self.assertEqual(r["todo"], 3)
        # Exception case
        todo.side_effect = NameError("some exception")
        r = global_info(request)
        self.assertEqual(r["todo"], 0)

    def tearDown(self):
        self.u1.delete()


class SpaRouteHelperTest(TestCase):
    def test_spa_path_for_supported_workflow_types(self):
        self.assertEqual(
            spa_path_for_workflow(WorkflowType.SQL_REVIEW, 12), "/workflows/12"
        )
        self.assertEqual(
            spa_path_for_workflow(WorkflowType.QUERY, 34),
            "/permission-management?requestId=34",
        )
        self.assertEqual(
            spa_path_for_workflow(WorkflowType.ARCHIVE, 56), "/archives/56"
        )

    def test_spa_url_for_supported_workflow_types(self):
        self.assertEqual(
            spa_url_for_workflow(
                "https://app.example.com/", WorkflowType.SQL_REVIEW, 12
            ),
            "https://app.example.com/workflows/12",
        )

    def test_spa_path_rejects_unknown_workflow_type(self):
        with self.assertRaises(ValueError):
            spa_path_for_workflow(WorkflowType.ACCESS_REQUEST, 99)


class ChartTest(TestCase):
    """Dashboard chart tests."""

    @classmethod
    def setUpClass(cls):
        cls.u1 = User(username="some_user", display="user1")
        cls.u1.save()
        cls.u2 = User(username="some_other_user", display="user2")
        cls.u2.save()
        cls.superuser1 = User(username="super1", is_superuser=True)
        cls.superuser1.save()
        cls.now = datetime.datetime.now()
        cls.slave1 = Instance(
            instance_name="test_slave_instance",
            type="slave",
            db_type="mysql",
            host="testhost",
            port=3306,
            user="mysql_user",
            password="mysql_password",
        )
        cls.slave1.save()
        # Bulk create DDL data: u1, group g1, yesterday, 2 rows
        ddl_workflow = [
            SqlWorkflow(
                workflow_name="ddl %s" % i,
                group_id=1,
                group_name="g1",
                engineer=cls.u1.username,
                engineer_display=cls.u1.display,
                audit_auth_groups="some_group",
                create_time=cls.now - datetime.timedelta(days=1),
                status="workflow_finish",
                is_backup=True,
                instance=cls.slave1,
                db_name="some_db",
                syntax_type=1,
            )
            for i in range(2)
        ]
        # Bulk create DML data: u2, group g2, day-before-yesterday, 3 rows
        dml_workflow = [
            SqlWorkflow(
                workflow_name="Test %s" % i,
                group_id=2,
                group_name="g2",
                engineer=cls.u2.username,
                engineer_display=cls.u2.display,
                audit_auth_groups="some_group",
                create_time=cls.now - datetime.timedelta(days=2),
                status="workflow_finish",
                is_backup=True,
                instance=cls.slave1,
                db_name="some_db",
                syntax_type=2,
            )
            for i in range(3)
        ]
        SqlWorkflow.objects.bulk_create(ddl_workflow + dml_workflow)
        # Save workflow content rows
        ddl_workflow_content = [
            SqlWorkflowContent(
                workflow=SqlWorkflow.objects.get(workflow_name="ddl %s" % i),
                sql_content="some_sql",
            )
            for i in range(2)
        ]
        dml_workflow_content = [
            SqlWorkflowContent(
                workflow=SqlWorkflow.objects.get(workflow_name="Test %s" % i),
                sql_content="some_sql",
            )
            for i in range(3)
        ]
        SqlWorkflowContent.objects.bulk_create(
            ddl_workflow_content + dml_workflow_content
        )

    # query_logs = [QueryLog(
    #    instance_name = 'some_instance',
    #
    # ) for i in range(20)]

    @classmethod
    def tearDownClass(cls):
        SqlWorkflowContent.objects.all().delete()
        SqlWorkflow.objects.all().delete()
        QueryLog.objects.all().delete()
        cls.u1.delete()
        cls.u2.delete()
        cls.superuser1.delete()
        cls.slave1.delete()

    def testGetDateList(self):
        dao = ChartDao()
        end = datetime.date.today()
        begin = end - datetime.timedelta(days=3)
        result = dao.get_date_list(begin, end)
        self.assertEqual(len(result), 4)
        self.assertEqual(result[0], begin.strftime("%Y-%m-%d"))
        self.assertEqual(result[-1], end.strftime("%Y-%m-%d"))

    def testSyntaxList(self):
        """Group workflows by syntax type."""
        dao = ChartDao()
        expected_rows = (("DDL", 2), ("DML", 3))
        today = (datetime.date.today() - relativedelta(days=-1)).strftime("%Y-%m-%d")
        one_week_before = (datetime.date.today() - relativedelta(days=+6)).strftime(
            "%Y-%m-%d"
        )
        result = dao.syntax_type(one_week_before, today)
        self.assertEqual(result["rows"], expected_rows)

    def testWorkflowByDate(self):
        """TODO: workflow count grouped by date."""
        dao = ChartDao()
        today = (datetime.date.today() - relativedelta(days=-1)).strftime("%Y-%m-%d")
        one_week_before = (datetime.date.today() - relativedelta(days=+6)).strftime(
            "%Y-%m-%d"
        )
        result = dao.workflow_by_date(one_week_before, today)
        self.assertEqual(len(result["rows"][0]), 2)

    def testWorkflowByGroup(self):
        """Workflow count grouped by group."""
        dao = ChartDao()
        today = (datetime.date.today() - relativedelta(days=-1)).strftime("%Y-%m-%d")
        one_week_before = (datetime.date.today() - relativedelta(days=+6)).strftime(
            "%Y-%m-%d"
        )
        result = dao.workflow_by_group(one_week_before, today)
        expected_rows = (("g2", 3), ("g1", 2))
        self.assertEqual(result["rows"], expected_rows)

    def testWorkflowByUser(self):
        """Workflow count grouped by user."""
        dao = ChartDao()
        today = (datetime.date.today() - relativedelta(days=-1)).strftime("%Y-%m-%d")
        one_week_before = (datetime.date.today() - relativedelta(days=+6)).strftime(
            "%Y-%m-%d"
        )
        result = dao.workflow_by_user(one_week_before, today)
        expected_rows = ((self.u2.display, 3), (self.u1.display, 2))
        self.assertEqual(result["rows"], expected_rows)


class AuthTest(TestCase):
    def setUp(self):
        self.username = "some_user"
        self.password = "some_str"
        self.u1 = User(username=self.username, password=self.password, display="user1")
        self.u1.save()
        self.resource_group1 = ResourceGroup.objects.create(group_name="some_group")
        sys_config = SysConfig()
        sys_config.set("default_resource_group", self.resource_group1.group_name)

    def tearDown(self):
        self.u1.delete()
        self.resource_group1.delete()
        SysConfig().purge()

    def test_init_user(self):
        """User initialization test."""
        init_user(self.u1)
        self.assertEqual(self.u1, self.resource_group1.users_set.get(pk=self.u1.pk))
        # init should be idempotent
        init_user(self.u1)
        self.assertEqual(self.u1, self.resource_group1.users_set.get(pk=self.u1.pk))


class TestTwoFactorAuth(TestCase):
    def setUp(self):
        self.user = User.objects.create(
            username="twofa_user",
            display="TwoFA User",
            is_active=True,
        )

    def tearDown(self):
        TwoFactorAuthConfig.objects.all().delete()
        self.user.delete()

    def test_sms_verify_returns_controlled_error_when_config_missing(self):
        result = SMS(user=self.user).verify("123456")

        self.assertEqual(
            result,
            {"status": 1, "msg": "SMS 2FA is not configured for this account."},
        )

    def test_totp_verify_returns_controlled_error_when_config_missing(self):
        result = TOTP(user=self.user).verify("123456")

        self.assertEqual(
            result,
            {"status": 1, "msg": "TOTP 2FA is not configured for this account."},
        )


class ExtendJSONEncoderFTimeTest(TestCase):
    def setUp(self):
        # Initialize test data/state
        self.datetime1 = datetime.datetime.now()
        self.datetime2 = datetime.datetime.now() - datetime.timedelta(days=1)
        self.tz_range = psycopg2._range.DateTimeTZRange(self.datetime2, self.datetime1)
        self.date_time = self.datetime1

    def test_datetime_tz_range(self):
        # Test DateTimeTZRange
        result = ExtendJSONEncoderFTime().default(self.tz_range)
        assert (
            self.datetime1.strftime("%Y-%m-%d") in result
            and self.datetime2.strftime("%Y-%m-%d") in result
        )

    def test_datetime(self):
        # Test datetime
        result = ExtendJSONEncoderFTime().default(self.date_time)
        assert self.datetime1.strftime("%Y-%m-%d") in result


class TaskQueueTests(TestCase):
    def setUp(self):
        TASK_CALLBACK_RESULTS.clear()
        TaskSchedule.objects.all().delete()
        self.sys_config = SysConfig()
        self.sys_config.purge()

    def tearDown(self):
        TASK_CALLBACK_RESULTS.clear()
        TaskSchedule.objects.all().delete()
        self.sys_config.purge()

    def test_current_task_backend_is_celery(self):
        self.assertEqual(task_queue.current_task_backend(), "celery")

    @override_settings(
        CELERY_BROKER_URL="redis://settings:6379/1",
        CELERY_RESULT_BACKEND="redis://settings:6379/2",
        CELERY_TASK_DEFAULT_QUEUE="settings-default",
        CELERY_TASK_SOFT_TIME_LIMIT=15,
        CELERY_TASK_TIME_LIMIT=30,
    )
    def test_celery_runtime_settings_prefer_db_values(self):
        self.sys_config.set("celery_broker_url", "redis://db:6379/5")
        self.sys_config.set("celery_result_backend", "redis://db:6379/6")
        self.sys_config.set("celery_task_default_queue", "db-queue")
        self.sys_config.set("celery_task_soft_time_limit", 25)
        self.sys_config.set("celery_task_time_limit", 50)

        runtime = task_queue.celery_runtime_settings()

        self.assertEqual(runtime["broker_url"], "redis://db:6379/5")
        self.assertEqual(runtime["result_backend"], "redis://db:6379/6")
        self.assertEqual(runtime["task_default_queue"], "db-queue")
        self.assertEqual(runtime["task_soft_time_limit"], 25)
        self.assertEqual(runtime["task_time_limit"], 50)

    def test_async_task_encodes_payload_and_uses_selected_backend(self):
        backend = Mock()
        backend.enqueue_payload.return_value = "queued-id"

        with patch("common.task_queue.get_task_backend", return_value=backend):
            task_id = task_queue.async_task(
                sample_task,
                "hello",
                suffix="!",
                hook=record_task_callback,
                task_name="demo-task",
                timeout=8,
            )

        self.assertEqual(task_id, "queued-id")
        payload = backend.enqueue_payload.call_args.kwargs["payload"]
        decoded = task_queue._decode_task_payload(payload)
        self.assertEqual(decoded["callable_path"], "common.tests.sample_task")
        self.assertEqual(decoded["args"], ("hello",))
        self.assertEqual(decoded["kwargs"], {"suffix": "!"})
        self.assertEqual(decoded["callback_path"], "common.tests.record_task_callback")
        self.assertEqual(decoded["task_name"], "demo-task")
        self.assertEqual(
            backend.enqueue_payload.call_args.kwargs["task_name"], "demo-task"
        )
        self.assertEqual(backend.enqueue_payload.call_args.kwargs["timeout"], 8)

    def test_task_payload_round_trips_model_references_with_json(self):
        user = User.objects.create(username="task-user")

        payload = task_queue._encode_task_payload(
            sample_task,
            (user,),
            {"suffix": "!"},
            record_task_callback,
            "demo-task",
            "",
        )

        decoded = task_queue._decode_task_payload(payload)

        self.assertTrue(payload.startswith("{"))
        self.assertEqual(decoded["args"][0].pk, user.pk)
        self.assertEqual(decoded["args"][0].username, user.username)
        self.assertEqual(decoded["kwargs"], {"suffix": "!"})

    def test_task_payload_round_trips_marker_dict_without_collision(self):
        payload = task_queue._encode_task_payload(
            sample_task,
            (),
            {"metadata": {"__task_type__": "custom", "value": "ok"}},
            record_task_callback,
            "demo-task",
            "",
        )

        decoded = task_queue._decode_task_payload(payload)

        self.assertEqual(
            decoded["kwargs"]["metadata"],
            {"__task_type__": "custom", "value": "ok"},
        )

    def test_execute_payload_rejects_tampered_signature(self):
        payload = task_queue._encode_task_payload(
            sample_task,
            ("hello",),
            {"suffix": "!"},
            record_task_callback,
            "demo-task",
            "",
        )
        envelope = json.loads(payload)
        envelope["payload"]["task_name"] = "forged-task"
        forged_payload = json.dumps(envelope, separators=(",", ":"), sort_keys=True)

        with self.assertRaises(ValueError):
            task_queue.execute_payload(forged_payload)

    def test_execute_payload_marks_schedule_completed_and_runs_callback(self):
        TaskSchedule.objects.create(
            name="scheduled-task",
            task_name="scheduled-task",
            callable_path="common.tests.sample_task",
            run_at=datetime.datetime.now(),
        )
        payload = task_queue._encode_task_payload(
            sample_task,
            ("hello",),
            {"suffix": "!"},
            record_task_callback,
            "scheduled-task",
            "scheduled-task",
        )

        result = task_queue.execute_payload(payload)

        self.assertEqual(result, "hello!")
        schedule = TaskSchedule.objects.get(name="scheduled-task")
        self.assertEqual(schedule.status, TaskSchedule.STATUS_COMPLETED)
        self.assertEqual(len(TASK_CALLBACK_RESULTS), 1)
        self.assertTrue(TASK_CALLBACK_RESULTS[0].success)
        self.assertEqual(TASK_CALLBACK_RESULTS[0].result, "hello!")

    def test_execute_payload_keeps_success_when_callback_fails(self):
        TaskSchedule.objects.create(
            name="scheduled-task",
            task_name="scheduled-task",
            callable_path="common.tests.sample_task",
            run_at=datetime.datetime.now(),
        )
        payload = task_queue._encode_task_payload(
            sample_task,
            ("hello",),
            {"suffix": "!"},
            failing_task_callback,
            "scheduled-task",
            "scheduled-task",
        )

        result = task_queue.execute_payload(payload)

        self.assertEqual(result, "hello!")
        schedule = TaskSchedule.objects.get(name="scheduled-task")
        self.assertEqual(schedule.status, TaskSchedule.STATUS_COMPLETED)
        self.assertEqual(len(TASK_CALLBACK_RESULTS), 1)
        self.assertTrue(TASK_CALLBACK_RESULTS[0].success)

    def test_execute_payload_marks_schedule_failed_and_runs_callback(self):
        TaskSchedule.objects.create(
            name="failing-task",
            task_name="failing-task",
            callable_path="common.tests.failing_task",
            run_at=datetime.datetime.now(),
        )
        payload = task_queue._encode_task_payload(
            failing_task,
            (),
            {},
            record_task_callback,
            "failing-task",
            "failing-task",
        )

        with self.assertRaises(RuntimeError):
            task_queue.execute_payload(payload)

        schedule = TaskSchedule.objects.get(name="failing-task")
        self.assertEqual(schedule.status, TaskSchedule.STATUS_FAILED)
        self.assertEqual(len(TASK_CALLBACK_RESULTS), 1)
        self.assertFalse(TASK_CALLBACK_RESULTS[0].success)
        self.assertEqual(TASK_CALLBACK_RESULTS[0].error, "task boom")

    @patch("common.task_queue._refresh_celery_runtime_config")
    @patch("common.task_queue._celery_execute_task")
    def test_celery_backend_schedule_payload_creates_registry(
        self, mock_celery_execute_task, mock_refresh
    ):
        backend = task_queue.CeleryTaskBackend()
        mock_refresh.return_value = Mock()
        mock_result = Mock()
        mock_result.id = "celery-task-id"
        mock_celery_execute_task.return_value.apply_async.return_value = mock_result
        self.sys_config.set("celery_task_default_queue", "celery-queue")

        backend.schedule_payload(
            name="celery-scheduled",
            payload="encoded",
            run_at=datetime.datetime.now(),
            task_name="celery-scheduled",
            callable_path="common.tests.sample_task",
            timeout=45,
        )

        saved = TaskSchedule.objects.get(name="celery-scheduled")
        self.assertEqual(saved.backend, TaskSchedule.BACKEND_CELERY)
        self.assertEqual(saved.backend_job_id, "celery-task-id")
        mock_celery_execute_task.return_value.apply_async.assert_called_once()

    @patch("common.task_queue._refresh_celery_runtime_config")
    @patch("common.task_queue._celery_execute_task")
    def test_celery_backend_schedule_payload_marks_failed_when_enqueue_fails(
        self, mock_celery_execute_task, mock_refresh
    ):
        backend = task_queue.CeleryTaskBackend()
        mock_refresh.return_value = Mock()
        mock_celery_execute_task.return_value.apply_async.side_effect = RuntimeError(
            "broker unavailable"
        )

        with self.assertRaises(RuntimeError):
            backend.schedule_payload(
                name="celery-scheduled",
                payload="encoded",
                run_at=datetime.datetime.now(),
                task_name="celery-scheduled",
                callable_path="common.tests.sample_task",
                timeout=45,
            )

        saved = TaskSchedule.objects.get(name="celery-scheduled")
        self.assertEqual(saved.status, TaskSchedule.STATUS_FAILED)
        self.assertIn("broker unavailable", saved.last_error)

    @patch("common.task_queue._celery_app")
    def test_celery_execute_task_uses_explicit_runtime_error(self, mock_celery_app):
        mock_celery_app.side_effect = RuntimeError("Celery unavailable")

        with self.assertRaisesRegex(RuntimeError, "Celery unavailable"):
            task_queue._celery_execute_task()

    @patch("common.task_queue._celery_app")
    def test_celery_backend_cancel_schedule_revokes_task_and_marks_cancelled(
        self, mock_celery_app
    ):
        backend = task_queue.CeleryTaskBackend()
        TaskSchedule.objects.create(
            name="celery-scheduled",
            backend=TaskSchedule.BACKEND_CELERY,
            task_name="celery-scheduled",
            callable_path="common.tests.sample_task",
            backend_job_id="celery-task-id",
            run_at=datetime.datetime.now(),
        )
        mock_celery_app.return_value.control.revoke.return_value = None

        backend.cancel_scheduled("celery-scheduled")

        saved = TaskSchedule.objects.get(name="celery-scheduled")
        self.assertEqual(saved.status, TaskSchedule.STATUS_CANCELLED)
        mock_celery_app.return_value.control.revoke.assert_called_once_with(
            "celery-task-id"
        )

    def test_task_backend_info_reports_schedule_counts(self):
        TaskSchedule.objects.create(
            name="scheduled-task",
            task_name="scheduled-task",
            callable_path="common.tests.sample_task",
            status=TaskSchedule.STATUS_SCHEDULED,
            run_at=datetime.datetime.now(),
        )
        TaskSchedule.objects.create(
            name="running-task",
            task_name="running-task",
            callable_path="common.tests.sample_task",
            status=TaskSchedule.STATUS_RUNNING,
            run_at=datetime.datetime.now(),
        )
        backend = Mock()
        backend.backend_id = "celery"
        backend.health_snapshot.return_value = {"label": "Celery"}

        with patch("common.task_queue.get_task_backend", return_value=backend):
            info = task_queue.task_backend_info(full=True)

        self.assertEqual(info["active"], "celery")
        self.assertEqual(info["scheduled"]["pending"], 1)
        self.assertEqual(info["scheduled"]["running"], 1)
        self.assertEqual(info["config"], {"label": "Celery"})
