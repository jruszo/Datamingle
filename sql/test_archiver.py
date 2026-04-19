import json
from datetime import datetime, time, timedelta
from types import SimpleNamespace
from unittest.mock import patch

from django.conf import settings
from django.contrib.auth.models import Permission
from django.test import TestCase, Client
from django.utils import timezone
from pytest_django.asserts import assertTemplateUsed

from common.config import SysConfig
from common.utils.const import WorkflowStatus, WorkflowType
from sql.utils.workflow_audit import AuditSetting, AuditV2
from sql.archiver import (
    ARCHIVE_EXECUTION_ONE_TIME,
    ARCHIVE_EXECUTION_SCHEDULED,
    ARCHIVE_METHOD_PT_ARCHIVER,
    ARCHIVE_SCHEDULE_DAILY,
    ARCHIVE_SCHEDULE_WEEKLY,
    add_archive_task,
    archive,
    archive_task_callback,
    build_archive_delete_sql,
    calculate_next_archive_run,
    render_archive_condition,
    _build_pt_archiver_args,
)
from sql.models import (
    Instance,
    ResourceGroup,
    ArchiveConfig,
    WorkflowAudit,
    WorkflowAuditSetting,
)
from sql.tests import User


class TestArchiver(TestCase):
    """
    Test Archive.
    """

    def setUp(self):
        self.superuser = User.objects.create(username="super", is_superuser=True)
        self.u1 = User.objects.create(username="u1", is_superuser=False)
        self.u2 = User.objects.create(username="u2", is_superuser=False)
        menu_archive = Permission.objects.get(codename="menu_archive")
        archive_review = Permission.objects.get(codename="archive_review")
        self.u1.user_permissions.add(menu_archive)
        self.u2.user_permissions.add(menu_archive)
        self.u2.user_permissions.add(archive_review)
        # Keep test instance consistent with CI service.
        self.ins = Instance.objects.create(
            instance_name="test_instance",
            type="master",
            db_type="mysql",
            host=settings.DATABASES["default"]["HOST"],
            port=settings.DATABASES["default"]["PORT"],
            user=settings.DATABASES["default"]["USER"],
            password=settings.DATABASES["default"]["PASSWORD"],
        )
        self.res_group = ResourceGroup.objects.create(
            group_id=1, group_name="group_name"
        )
        self.archive_apply = ArchiveConfig.objects.create(
            title="title",
            resource_group=self.res_group,
            audit_auth_groups="some_audit_group",
            src_instance=self.ins,
            src_db_name="src_db_name",
            src_table_name="src_table_name",
            dest_instance=self.ins,
            dest_db_name="src_db_name",
            dest_table_name="src_table_name",
            condition="1=1",
            mode="file",
            no_delete=True,
            sleep=1,
            status=WorkflowStatus.WAITING,
            state=False,
            user_name="some_user",
            user_display="display",
        )
        self.audit_flow = WorkflowAudit.objects.create(
            group_id=1,
            group_name="g1",
            workflow_id=self.archive_apply.id,
            workflow_type=WorkflowType.ARCHIVE,
            workflow_title="123",
            audit_auth_groups="123",
            current_audit="",
            next_audit="",
            current_status=WorkflowStatus.WAITING,
            create_user="",
            create_user_display="",
        )
        self.sys_config = SysConfig()
        self.client = Client()

    def tearDown(self):
        User.objects.all().delete()
        ResourceGroup.objects.all().delete()
        ArchiveConfig.objects.all().delete()
        WorkflowAuditSetting.objects.all().delete()
        self.ins.delete()
        self.sys_config.purge()

    def test_archive_list_super(self):
        """
        Superuser gets archive request list.
        :return:
        """
        data = {"filter_instance_id": self.ins.id, "state": "false", "search": "text"}
        self.client.force_login(self.superuser)
        r = self.client.get(path="/archive/list/", data=data)
        self.assertDictEqual(json.loads(r.content), {"total": 0, "rows": []})

    def test_archive_list_own(self):
        """
        Non-admin non-reviewer gets own archive list.
        :return:
        """
        data = {"filter_instance_id": self.ins.id, "state": "false", "search": "text"}
        self.client.force_login(self.u1)
        r = self.client.get(path="/archive/list/", data=data)
        self.assertDictEqual(json.loads(r.content), {"total": 0, "rows": []})

    def test_archive_list_review(self):
        """
        Reviewer gets archive request list.
        :return:
        """
        data = {"filter_instance_id": self.ins.id, "state": "false", "search": "text"}
        self.client.force_login(self.u2)
        r = self.client.get(path="/archive/list/", data=data)
        self.assertDictEqual(json.loads(r.content), {"total": 0, "rows": []})

    def test_archive_apply_not_param(self):
        """
        Archive apply fails when parameters are incomplete.
        :return:
        """
        data = {
            "group_name": self.res_group.group_name,
            "src_instance_name": self.ins.instance_name,
            "src_db_name": "src_db_name",
            "src_table_name": "src_table_name",
            "mode": "dest",
            "dest_instance_name": self.ins.instance_name,
            "dest_db_name": "dest_db_name",
            "dest_table_name": "dest_table_name",
            "condition": "1=1",
            "no_delete": "true",
            "sleep": 10,
        }
        self.client.force_login(self.superuser)
        r = self.client.post(path="/archive/apply/", data=data)
        self.assertDictEqual(
            json.loads(r.content),
            {"status": 1, "msg": "Please complete all required fields!", "data": {}},
        )

    def test_archive_apply_not_dest_param(self):
        """
        Archive apply fails when destination instance params are incomplete.
        :return:
        """
        data = {
            "title": "title",
            "group_name": self.res_group.group_name,
            "src_instance_name": self.ins.instance_name,
            "src_db_name": "src_db_name",
            "src_table_name": "src_table_name",
            "mode": "dest",
            "condition": "1=1",
            "no_delete": "true",
            "sleep": 10,
        }
        self.client.force_login(self.superuser)
        r = self.client.post(path="/archive/apply/", data=data)
        self.assertDictEqual(
            json.loads(r.content),
            {
                "status": 1,
                "msg": "Destination instance info is required for destination mode!",
                "data": {},
            },
        )

    def test_archive_apply_not_exist_review(self):
        """
        Archive apply fails when approval flow is not configured.
        :return:
        """
        data = {
            "title": "title",
            "group_name": self.res_group.group_name,
            "src_instance_name": self.ins.instance_name,
            "src_db_name": "src_db_name",
            "src_table_name": "src_table_name",
            "mode": "dest",
            "dest_instance_name": self.ins.instance_name,
            "dest_db_name": "dest_db_name",
            "dest_table_name": "dest_table_name",
            "condition": "1=1",
            "no_delete": "true",
            "sleep": 10,
        }
        self.client.force_login(self.superuser)
        r = self.client.post(path="/archive/apply/", data=data)
        self.assertDictEqual(
            json.loads(r.content),
            {
                "data": {},
                "msg": "Failed to create approval flow. Contact admin.",
                "status": 1,
            },
        )

    @patch("sql.archiver.async_task")
    def test_archive_apply(self, _async_task):
        """
        Test archive apply.
        :return:
        """
        WorkflowAuditSetting.objects.create(
            workflow_type=3, group_id=1, audit_auth_groups="1"
        )
        data = {
            "title": "title",
            "group_name": self.res_group.group_name,
            "src_instance_name": self.ins.instance_name,
            "src_db_name": "src_db_name",
            "src_table_name": "src_table_name",
            "mode": "dest",
            "dest_instance_name": self.ins.instance_name,
            "dest_db_name": "dest_db_name",
            "dest_table_name": "dest_table_name",
            "condition": "1=1",
            "no_delete": "true",
            "sleep": 10,
        }
        self.client.force_login(self.superuser)
        r = self.client.post(path="/archive/apply/", data=data)
        self.assertEqual(json.loads(r.content)["status"], 0)

    @patch("sql.utils.workflow_audit.AuditV2.generate_audit_setting")
    def test_archive_apply_auto_pass(self, mock_generate_setting):
        mock_generate_setting.return_value = AuditSetting(
            auto_pass=True,
        )
        data = {
            "title": "title",
            "group_name": self.res_group.group_name,
            "src_instance_name": self.ins.instance_name,
            "src_db_name": "src_db_name",
            "src_table_name": "src_table_name",
            "mode": "dest",
            "dest_instance_name": self.ins.instance_name,
            "dest_db_name": "dest_db_name",
            "dest_table_name": "dest_table_name",
            "condition": "1=1",
            "no_delete": "true",
            "sleep": 10,
        }
        self.client.force_login(self.superuser)
        r = self.client.post(path="/archive/apply/", data=data)
        return_data = r.json()
        self.assertEqual(return_data["status"], 0)
        archive_config = ArchiveConfig.objects.get(id=return_data["data"]["archive_id"])
        assert archive_config.state == True
        assert archive_config.status == WorkflowStatus.PASSED

    @patch("sql.utils.workflow_audit.AuditV2.operate")
    @patch("sql.archiver.async_task")
    def test_archive_audit(self, _async_task, mock_operate):
        """
        Test archive review.
        :return:
        """
        mock_operate.return_value = None
        data = {
            "archive_id": self.archive_apply.id,
            "audit_status": WorkflowStatus.PASSED,
            "audit_remark": "xxxx",
        }
        # operate is patched, force a passed state to run through flow.
        self.audit_flow.current_status = WorkflowStatus.PASSED
        self.audit_flow.save()
        self.client.force_login(self.superuser)
        r = self.client.post(path="/archive/audit/", data=data)
        self.assertRedirects(
            r, f"/archive/{self.archive_apply.id}/", fetch_redirect_response=False
        )
        self.archive_apply.refresh_from_db()
        assert self.archive_apply.state == True
        assert self.archive_apply.status == WorkflowStatus.PASSED

    @patch("sql.archiver.async_task")
    def test_add_archive_task(self, _async_task):
        """
        Test adding async archive tasks.
        :return:
        """
        add_archive_task()

    @patch("sql.archiver.async_task")
    def test_add_archive(self, _async_task):
        """
        Test executing archive task.
        :return:
        """
        with self.assertRaises(Exception):
            archive(self.archive_apply.id)

    @patch("sql.archiver.async_task")
    def test_archive_log(self, _async_task):
        """
        Test fetching archive logs.
        :return:
        """
        data = {
            "archive_id": self.archive_apply.id,
        }
        self.client.force_login(self.superuser)
        r = self.client.post(path="/archive/log/", data=data)
        self.assertDictEqual(json.loads(r.content), {"total": 0, "rows": []})


def test_archive_detail_view(
    archive_apply,
    resource_group,
    admin_client,
    fake_generate_audit_setting,
    create_auth_group,
):
    audit = AuditV2(workflow=archive_apply, resource_group=resource_group.group_name)
    audit.create_audit()
    audit.workflow.save()
    response = admin_client.get(f"/archive/{archive_apply.id}/")
    assert response.status_code == 200
    assertTemplateUsed(response, "archivedetail.html")
    review_info = response.context["review_info"]
    assert len(review_info.nodes) == len(
        fake_generate_audit_setting.return_value.audit_auth_groups
    )
    assert review_info.nodes[0].group.name == create_auth_group.name


class ArchiveExecutionHelpersTest(TestCase):
    def setUp(self):
        self.resource_group = ResourceGroup.objects.create(group_name="Archive Helpers")
        self.instance = Instance.objects.create(
            instance_name="archive-helper-mysql",
            type="master",
            db_type="mysql",
            host="127.0.0.1",
            port=3306,
            user="root",
            password="secret",
        )
        self.archive = ArchiveConfig.objects.create(
            title="Helper archive",
            resource_group=self.resource_group,
            audit_auth_groups="",
            src_instance=self.instance,
            src_db_name="demo_orders",
            src_table_name="orders",
            condition="created_at < {{ today }} AND updated_at >= {{ yesterday }} AND run_at < {{ tomorrow }} AND touched_at <= {{ now }}",
            mode="purge",
            no_delete=False,
            sleep=1,
            archive_method="dml",
            execution_mode="one_time",
            status=WorkflowStatus.PASSED,
            state=True,
            user_name="system",
            user_display="System",
        )

    def test_render_archive_condition_supports_datetime_variables(self):
        rendered = render_archive_condition(
            "d = {{ today }} OR y = {{ yesterday }} OR t = {{ tomorrow }} OR n = {{ now }}",
            now=datetime(2026, 4, 18, 13, 14, 15),
        )

        self.assertEqual(
            rendered,
            "d = '2026-04-18' OR y = '2026-04-17' OR t = '2026-04-19' OR n = '2026-04-18 13:14:15'",
        )

    def test_build_archive_delete_sql_enforces_single_delete_statement(self):
        self.archive.condition = "id > 10"

        delete_sql = build_archive_delete_sql(self.archive)
        self.assertEqual(delete_sql, "DELETE FROM orders WHERE id > 10")

        self.archive.condition = "id > 10; DROP TABLE orders"
        with self.assertRaises(ValueError):
            build_archive_delete_sql(self.archive)

    @patch("sql.archiver.get_engine")
    def test_pt_archiver_args_enforce_delete_only_purge_mode(self, get_engine_mock):
        fake_table = SimpleNamespace(
            options={"charset": SimpleNamespace(value="utf8mb4")}
        )
        fake_database = SimpleNamespace(tables={"orders": fake_table})
        fake_engine = SimpleNamespace(
            schema_object=SimpleNamespace(databases={"demo_orders": fake_database}),
            close=lambda: None,
        )
        get_engine_mock.return_value = fake_engine

        args = _build_pt_archiver_args(self.archive)

        self.assertEqual(
            args["where"], render_archive_condition(self.archive.condition)
        )
        self.assertTrue(args["purge"])
        self.assertNotIn("file", args)
        self.assertNotIn("dest", args)
        self.assertNotIn("no-delete", args)

    def test_calculate_next_archive_run_supports_daily_and_weekly(self):
        daily_archive = SimpleNamespace(
            execution_mode=ARCHIVE_EXECUTION_SCHEDULED,
            schedule_frequency=ARCHIVE_SCHEDULE_DAILY,
            schedule_time=time(2, 30),
            schedule_weekdays="",
        )
        daily_next_run = calculate_next_archive_run(
            daily_archive,
            from_time=datetime(2026, 4, 18, 1, 0, 0),
        )
        self.assertEqual(daily_next_run.strftime("%Y-%m-%d %H:%M"), "2026-04-18 02:30")

        weekly_archive = SimpleNamespace(
            execution_mode=ARCHIVE_EXECUTION_SCHEDULED,
            schedule_frequency=ARCHIVE_SCHEDULE_WEEKLY,
            schedule_time=time(3, 45),
            schedule_weekdays="mon,fri",
        )
        weekly_next_run = calculate_next_archive_run(
            weekly_archive,
            from_time=datetime(2026, 4, 18, 4, 0, 0),
        )
        self.assertEqual(weekly_next_run.strftime("%Y-%m-%d %H:%M"), "2026-04-20 03:45")

    def test_archive_task_callback_disables_completed_one_time_archives(self):
        task = SimpleNamespace(
            args=(self.archive.id,),
            success=True,
            result="done",
            error="",
        )

        archive_task_callback(task)

        self.archive.refresh_from_db()
        self.assertFalse(self.archive.state)
        self.assertIsNone(self.archive.next_run_at)

    @patch("sql.archiver.calculate_next_archive_run")
    def test_archive_task_callback_rearms_scheduled_archives_after_failure(
        self, calculate_next_archive_run_mock
    ):
        next_run = datetime.now() + timedelta(days=2)
        calculate_next_archive_run_mock.return_value = next_run
        self.archive.execution_mode = ARCHIVE_EXECUTION_SCHEDULED
        self.archive.archive_method = ARCHIVE_METHOD_PT_ARCHIVER
        self.archive.schedule_frequency = ARCHIVE_SCHEDULE_DAILY
        self.archive.schedule_time = time(2, 0)
        self.archive.save(
            update_fields=[
                "execution_mode",
                "archive_method",
                "schedule_frequency",
                "schedule_time",
            ]
        )

        def fake_schedule_archive(archive_info, run_at=None):
            ArchiveConfig.objects.filter(id=archive_info.id).update(next_run_at=run_at)

        task = SimpleNamespace(
            args=(self.archive.id,),
            success=False,
            result="",
            error="boom",
        )

        with patch(
            "sql.archiver.schedule_archive", side_effect=fake_schedule_archive
        ) as schedule_archive_mock:
            archive_task_callback(task)

        self.archive.refresh_from_db()
        self.assertEqual(
            self.archive.next_run_at.replace(tzinfo=None),
            next_run.replace(tzinfo=None),
        )
        schedule_archive_mock.assert_called_once()
